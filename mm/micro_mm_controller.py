"""
Micro market-making controller with Hybrid composite-price extensions.

This controller encapsulates reservation price, spread, sizing, and gating
logic for a μMM strategy that mixes local venue signals with a composite
reference price.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


def _bps_to_price(mid: Decimal, bps: float) -> Decimal:
    return Decimal(str(float(mid) * bps / 1e4))


def _round_to_tick(px: Decimal, tick: float) -> Decimal:
    if tick <= 0:
        return px
    return Decimal(str(round(float(px) / tick) * tick))


@dataclass
class MicroMMConfig:
    maker_fee_bps: float = 0.5
    tick_size: float = 0.1
    base_spread_bps: float = 2.0
    min_spread_bps: float = 1.0
    max_spread_bps: float = 20.0
    horizon_s: float = 1.0

    c_sigma: float = 3.0
    c_tox: float = 5.0
    alpha_kappa: float = 0.5
    inv_gamma_bps: float = 3.0

    beta0: float = 0.3
    beta_max: float = 0.6
    staleness_cut_ms: float = 300.0
    sigma_pair_high: float = 0.005
    d_shift_coeff: float = 0.3
    d_shift_cap: float = 0.5

    alpha_th: float = 0.05
    tox_pause: float = 0.8
    tox_defensive: float = 0.5
    d_taker_bps: float = 15.0

    base_size: float = 0.001
    min_size: float = 0.001
    max_size: float = 0.02
    ttl_min_ms: int = 300
    ttl_max_ms: int = 1500


@dataclass
class MicroSignals:
    mid: Decimal
    sigma_micro_local: float
    tox: float
    alpha_micro: float
    inv_ratio: float
    queue_long: bool

    composite_mid: Optional[Decimal] = None
    n_exch: int = 0
    staleness_ms: float = 1e9
    sigma_star: float = 0.0
    sigma_pair: float = 0.0
    d_local: float = 0.0


@dataclass
class QuotePlan:
    bid: Optional[Decimal]
    ask: Optional[Decimal]
    bid_size: float
    ask_size: float
    ttl_ms: int
    mode: str
    spread_bps_used: float
    beta_used: float
    d_local_bps: float
    hedge_intent: int


class MicroMMController:
    def __init__(self, cfg: MicroMMConfig) -> None:
        self.cfg = cfg

    # ----------------------------------------------------------------- Helpers
    def _compute_beta(self, staleness_ms: float, sigma_pair: float, n_exch: int) -> float:
        if n_exch < 2 or staleness_ms > self.cfg.staleness_cut_ms:
            return 0.0
        fresh = max(0.0, 1.0 - staleness_ms / self.cfg.staleness_cut_ms)
        consistent = max(
            0.0, 1.0 - sigma_pair / max(1e-9, self.cfg.sigma_pair_high)
        )
        beta = self.cfg.beta0 * fresh * consistent
        return max(0.0, min(self.cfg.beta_max, beta))

    # ------------------------------------------------------------------ Public
    def plan(self, sig: MicroSignals) -> QuotePlan:
        S = float(sig.mid)
        if S <= 0:
            return QuotePlan(None, None, 0.0, 0.0, self.cfg.ttl_max_ms, "D", 0.0, 0.0, 0.0, 0)

        # Mode selection
        if sig.tox >= self.cfg.tox_pause:
            mode = "D"
        elif sig.tox >= self.cfg.tox_defensive:
            mode = "C"
        elif abs(sig.alpha_micro) <= self.cfg.alpha_th:
            mode = "A"
        else:
            mode = "B"

        beta = self._compute_beta(sig.staleness_ms, sig.sigma_pair, sig.n_exch)
        S_star = float(sig.composite_mid) if sig.composite_mid is not None and beta > 0 else S
        d = sig.d_local
        d_bps = d * 1e4

        sigma_for_spread = max(sig.sigma_star, sig.sigma_micro_local)
        spread_bps = max(self.cfg.base_spread_bps, self.cfg.min_spread_bps)
        spread_bps += self.cfg.c_sigma * sigma_for_spread * 1e4
        spread_bps += self.cfg.c_tox * sig.tox * 1e4
        spread_bps = min(spread_bps, self.cfg.max_spread_bps)
        half_spread = _bps_to_price(sig.mid, spread_bps / 2)

        reservation = Decimal(
            str((1.0 - beta) * S + beta * S_star)
        )
        reservation += _bps_to_price(sig.mid, self.cfg.alpha_kappa * sig.alpha_micro * 1e4)
        reservation -= _bps_to_price(sig.mid, sig.inv_ratio * self.cfg.inv_gamma_bps)

        maker_adj = _bps_to_price(sig.mid, self.cfg.maker_fee_bps)

        d_scale = min(self.cfg.d_shift_cap, 1.0 + self.cfg.d_shift_coeff * abs(d))
        if d >= 0:
            h_bid = half_spread * Decimal(str(d_scale))
            h_ask = half_spread / Decimal(str(d_scale))
        else:
            h_bid = half_spread / Decimal(str(d_scale))
            h_ask = half_spread * Decimal(str(d_scale))

        bid = reservation - h_bid - maker_adj
        ask = reservation + h_ask + maker_adj

        if sig.queue_long:
            bid -= Decimal(str(self.cfg.tick_size))
            ask += Decimal(str(self.cfg.tick_size))

        bid = _round_to_tick(bid, self.cfg.tick_size)
        ask = _round_to_tick(ask, self.cfg.tick_size)

        if mode == "D":
            bid = None
            ask = None

        sigma_norm = min(1.0, sigma_for_spread * 50)
        g = max(0.1, 1.0 - 0.5 * sig.tox) * max(0.2, 1.0 - 0.5 * sigma_norm)
        base = self.cfg.base_size * (1.0 - min(1.0, abs(sig.inv_ratio)))
        size = max(self.cfg.min_size, min(self.cfg.max_size, base * g))

        if sig.alpha_micro > self.cfg.alpha_th:
            bid_size = 0.7 * size
            ask_size = 1.3 * size
        elif sig.alpha_micro < -self.cfg.alpha_th:
            bid_size = 1.3 * size
            ask_size = 0.7 * size
        else:
            bid_size = size
            ask_size = size

        ttl = int(
            self.cfg.ttl_max_ms
            - (self.cfg.ttl_max_ms - self.cfg.ttl_min_ms)
            * min(1.0, 0.6 * sig.tox + 0.4 * sigma_norm)
        )

        hedge_intent = 1 if abs(d_bps) >= self.cfg.d_taker_bps and beta > 0 else 0

        return QuotePlan(
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            ttl_ms=ttl,
            mode=mode,
            spread_bps_used=spread_bps,
            beta_used=beta,
            d_local_bps=d_bps,
            hedge_intent=hedge_intent,
        )


