"""
Hybrid micro market-making controller entrypoint.

This module wires together the composite price aggregator, the μMM controller,
and external dependencies such as order executors, risk managers, and InfluxDB
writers. It is intentionally lightweight so that downstream projects can plug
it into their existing infrastructure without rewriting their order entry
stack.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, Optional, Protocol
import time

from .composite_price import CompositePrice, ExchangeQuote
from .micro_mm_controller import (
    MicroMMConfig,
    MicroMMController,
    MicroSignals,
    QuotePlan,
)


# ---------------------------------------------------------------- Dependency Protocols
class OrderExecutor(Protocol):
    def quote_or_replace(
        self, side: str, price: Decimal, size: float, ttl_ms: int, mode: str
    ) -> None:
        ...

    def cancel_all(self, reason: str) -> None:
        ...


class InfluxWriter(Protocol):
    def write_point(
        self,
        measurement: str,
        tags: Dict[str, str],
        fields: Dict[str, float],
        timestamp: float,
    ) -> None:
        ...


class Logger(Protocol):
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        ...

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        ...

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        ...

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        ...


# ---------------------------------------------------------------- Dataclasses
@dataclass
class HybridMMConfig:
    monitor_only: bool = True
    trade_enabled: bool = False
    min_order_size: float = 0.001
    symbol: str = "BTC"
    composite_cfg: Dict[str, Any] = field(
        default_factory=lambda: {
            "staleness_cut_ms": 300,
            "method": "wmedian",
            "trim_q": 0.1,
        }
    )
    micro_cfg: Dict[str, Any] = field(
        default_factory=lambda: {
            "maker_fee_bps": 0.5,
            "tick_size": 0.1,
            "base_spread_bps": 2.0,
            "min_spread_bps": 1.0,
            "max_spread_bps": 20.0,
            "horizon_s": 1.0,
            "c_sigma": 3.0,
            "c_tox": 5.0,
            "alpha_kappa": 0.5,
            "inv_gamma_bps": 3.0,
            "beta0": 0.3,
            "beta_max": 0.6,
            "staleness_cut_ms": 300.0,
            "sigma_pair_high": 0.005,
            "d_shift_coeff": 0.3,
            "d_shift_cap": 0.5,
            "alpha_th": 0.05,
            "tox_pause": 0.8,
            "tox_defensive": 0.5,
            "d_taker_bps": 15.0,
            "base_size": 0.001,
            "min_size": 0.001,
            "max_size": 0.02,
            "ttl_min_ms": 300,
            "ttl_max_ms": 1500,
        }
    )


@dataclass
class LocalMarketSnapshot:
    mid: Decimal
    best_bid: float
    best_ask: float
    tox: float
    alpha: float
    sigma_local: float
    inv_ratio: float
    queue_long: bool
    timestamp: Optional[float] = None


# ---------------------------------------------------------------- Main Engine
class HybridMicroMarketMaker:
    """
    Glue layer orchestrating composite price aggregation, μMM planning, and
    order execution.
    """

    def __init__(
        self,
        config: HybridMMConfig,
        order_executor: OrderExecutor,
        influx_writer: InfluxWriter,
        logger: Logger,
    ) -> None:
        self.config = config
        self.order_executor = order_executor
        self.influx_writer = influx_writer
        self.logger = logger

        self.composite = CompositePrice(**self.config.composite_cfg)
        self.micro_ctrl = MicroMMController(MicroMMConfig(**self.config.micro_cfg))

    # ------------------------------------------------------------ External Feed
    def on_external_quote(
        self,
        exch: str,
        best_bid: float,
        best_ask: float,
        top_depth: float,
        ts_ms: Optional[int] = None,
    ) -> None:
        ts = ts_ms if ts_ms is not None else int(time.time() * 1000)
        mid = 0.5 * (best_bid + best_ask)
        self.composite.update(
            ExchangeQuote(exch=exch, mid=mid, top_depth=top_depth, ts_ms=ts)
        )

    # -------------------------------------------------------------- Main Tick
    def on_tick(self, snapshot: LocalMarketSnapshot) -> QuotePlan:
        comp = self.composite.compute(S_local=float(snapshot.mid))
        if comp is not None:
            S_star = Decimal(str(comp["S_star"]))
            n_exch = int(comp["n_exch"])
            staleness = float(comp["staleness_ms_min"])
            sigma_star = float(comp["sigma_star"])
            sigma_pair = float(comp.get("sigma_pair", 0.0))
            d_local = float(comp.get("d_local", 0.0))
        else:
            S_star = None
            n_exch = 0
            staleness = 1e9
            sigma_star = 0.0
            sigma_pair = 0.0
            d_local = 0.0

        sig = MicroSignals(
            mid=snapshot.mid,
            sigma_micro_local=snapshot.sigma_local,
            tox=snapshot.tox,
            alpha_micro=snapshot.alpha,
            inv_ratio=snapshot.inv_ratio,
            queue_long=snapshot.queue_long,
            composite_mid=S_star,
            n_exch=n_exch,
            staleness_ms=staleness,
            sigma_star=sigma_star,
            sigma_pair=sigma_pair,
            d_local=d_local,
        )

        plan = self.micro_ctrl.plan(sig)
        self._write_composite_state(snapshot.mid, comp, plan)
        self._write_micro_state(sig, plan)

        if not self.config.trade_enabled or plan.bid is None and plan.ask is None:
            if not self.config.trade_enabled and plan.mode != "D":
                self.logger.info("Monitor-only mode, skipping order placement")
            if plan.bid is None and plan.ask is None:
                self.order_executor.cancel_all("μMM pause")
            return plan

        bid_size = max(plan.bid_size, self.config.min_order_size)
        ask_size = max(plan.ask_size, self.config.min_order_size)

        if plan.bid is not None:
            self.order_executor.quote_or_replace(
                "bid", plan.bid, bid_size, plan.ttl_ms, plan.mode
            )
        if plan.ask is not None:
            self.order_executor.quote_or_replace(
                "ask", plan.ask, ask_size, plan.ttl_ms, plan.mode
            )

        if plan.hedge_intent:
            self.logger.info(
                "Hybrid μMM suggests cross-ex venue hedge: d_local=%.2fbps",
                plan.d_local_bps,
            )

        return plan

    # ----------------------------------------------------------- Influx Writers
    def _write_composite_state(
        self, local_mid: Decimal, comp: Optional[Dict[str, float]], plan: QuotePlan
    ) -> None:
        try:
            fields: Dict[str, float] = {
                "local_mid": float(local_mid),
                "beta_used": float(plan.beta_used),
                "d_local_bps": float(plan.d_local_bps),
            }
            if comp is not None:
                fields.update(
                    {
                        "S_star": float(comp["S_star"]),
                        "n_exch": float(comp["n_exch"]),
                        "staleness_ms_min": float(comp["staleness_ms_min"]),
                        "staleness_ms_p95": float(comp["staleness_ms_p95"]),
                        "sigma_star": float(comp["sigma_star"]),
                        "sigma_pair": float(comp.get("sigma_pair", 0.0)),
                    }
                )
            self.influx_writer.write_point(
                "composite_state",
                tags={"symbol": self.config.symbol},
                fields=fields,
                timestamp=time.time(),
            )
        except Exception as exc:
            self.logger.exception("write composite_state failed: %s", exc)

    def _write_micro_state(self, sig: MicroSignals, plan: QuotePlan) -> None:
        try:
            fields = {
                "mid": float(sig.mid),
                "sigma_star": float(sig.sigma_star),
                "sigma_micro_local": float(sig.sigma_micro_local),
                "sigma_pair": float(sig.sigma_pair),
                "tox": float(sig.tox),
                "alpha_micro": float(sig.alpha_micro),
                "inv_ratio": float(sig.inv_ratio),
                "queue_long": float(1 if sig.queue_long else 0),
                "spread_bps": float(plan.spread_bps_used),
                "bid": float(plan.bid) if plan.bid is not None else float("nan"),
                "ask": float(plan.ask) if plan.ask is not None else float("nan"),
                "bid_size": float(plan.bid_size),
                "ask_size": float(plan.ask_size),
                "ttl_ms": float(plan.ttl_ms),
                "beta_used": float(plan.beta_used),
                "d_local_bps": float(plan.d_local_bps),
                "hedge_intent": float(plan.hedge_intent),
                "trade_enabled": float(1 if self.config.trade_enabled else 0),
            }
            self.influx_writer.write_point(
                "micro_mm_state",
                tags={"symbol": self.config.symbol, "mode": plan.mode},
                fields=fields,
                timestamp=time.time(),
            )
        except Exception as exc:
            self.logger.exception("write micro_mm_state failed: %s", exc)


