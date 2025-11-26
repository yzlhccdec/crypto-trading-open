"""
Composite price aggregator used by the Hybrid micro market-making stack.

This module implements a robust multi-venue mid-price aggregator that can be
fed by external exchange adapters. It computes a composite price S*, basic
staleness metrics, EWMA-based volatility estimates, and the local mispricing
between the composite price and the local exchange quote.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import math
import time


@dataclass
class ExchangeQuote:
    """
    Snapshot of a single exchange mid quote.

    Attributes:
        exch: Exchange identifier (e.g., "binance").
        mid: Mid price computed as (best_bid + best_ask) / 2.
        ts_ms: Timestamp in milliseconds when the quote reached the process.
        top_depth: Optional L1/L3 depth metric used as quote weight.
    """

    exch: str
    mid: float
    ts_ms: int
    top_depth: float = 1.0


class CompositePrice:
    """
    Aggregates multi-venue quotes into a composite price S* with robustness and
    staleness awareness.
    """

    def __init__(
        self,
        staleness_cut_ms: int = 300,
        method: str = "wmedian",
        trim_q: float = 0.1,
        ewma_alpha_sigma: float = 0.2,
        ewma_alpha_pair: float = 0.2,
    ) -> None:
        self.staleness_cut_ms = staleness_cut_ms
        self.method = method
        self.trim_q = trim_q
        self.ewma_alpha_sigma = ewma_alpha_sigma
        self.ewma_alpha_pair = ewma_alpha_pair

        self._quotes: Dict[str, ExchangeQuote] = {}
        self._last_s_star: Optional[float] = None
        self._ewma_sigma_star = 0.0
        self._ewma_sigma_pair = 0.0

    # --------------------------------------------------------------------- API
    def update(self, quote: ExchangeQuote) -> None:
        """Update/insert the latest quote for an exchange."""
        self._quotes[quote.exch] = quote

    def compute(self, s_local: Optional[float] = None) -> Optional[Dict[str, float]]:
        """
        Compute the composite statistics.

        Args:
            s_local: Local mid price used to derive d_local and sigma_pair.

        Returns:
            Dictionary with composite metrics or None if insufficient data.
        """
        act = self._active_quotes()
        if len(act) < 2:
            return None

        s_star = self._compute_s_star(act)
        if math.isnan(s_star) or s_star <= 0:
            return None

        now_ms = self._now_ms()
        stales = [now_ms - q.ts_ms for q in act]
        staleness_min = min(stales)
        staleness_p95 = sorted(stales)[int(0.95 * (len(stales) - 1))]

        # EWMA for sigma_star (normalized absolute delta)
        if self._last_s_star is not None:
            delta = abs(s_star - self._last_s_star) / max(1e-12, s_star)
            self._ewma_sigma_star = (
                (1 - self.ewma_alpha_sigma) * self._ewma_sigma_star
                + self.ewma_alpha_sigma * delta
            )
        self._last_s_star = s_star

        out: Dict[str, float] = {
            "S_star": s_star,
            "n_exch": float(len(act)),
            "staleness_ms_min": float(staleness_min),
            "staleness_ms_p95": float(staleness_p95),
            "sigma_star": float(self._ewma_sigma_star),
        }

        if s_local is not None and s_star > 0:
            d_local = (s_local - s_star) / s_star
            self._ewma_sigma_pair = (
                (1 - self.ewma_alpha_pair) * self._ewma_sigma_pair
                + self.ewma_alpha_pair * abs(d_local)
            )
            out["d_local"] = float(d_local)
            out["sigma_pair"] = float(self._ewma_sigma_pair)

        return out

    # ----------------------------------------------------------------- Helpers
    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _active_quotes(self) -> List[ExchangeQuote]:
        now_ms = self._now_ms()
        return [
            q for q in self._quotes.values() if now_ms - q.ts_ms <= self.staleness_cut_ms
        ]

    def _compute_s_star(self, quotes: List[ExchangeQuote]) -> float:
        if self.method == "wmedian":
            vals_w = [
                (q.mid, math.sqrt(max(1e-9, q.top_depth))) for q in quotes if q.mid > 0
            ]
            if not vals_w:
                return float("nan")
            return self._weighted_median(vals_w)

        values = [q.mid for q in quotes if q.mid > 0]
        return self._trimmed_mean(values, self.trim_q)

    @staticmethod
    def _weighted_median(vals_w: List[Tuple[float, float]]) -> float:
        vals_w = [(v, max(1e-9, w)) for v, w in vals_w]
        vals_w.sort(key=lambda x: x[0])
        total = sum(w for _, w in vals_w)
        acc = 0.0
        for value, weight in vals_w:
            acc += weight
            if acc >= 0.5 * total:
                return value
        return vals_w[-1][0]

    @staticmethod
    def _trimmed_mean(values: List[float], trim_q: float) -> float:
        if not values:
            return float("nan")
        values = sorted(values)
        n = len(values)
        k = int(n * trim_q)
        lo = min(k, n - 1)
        hi = max(n - k, lo + 1)
        trimmed = values[lo:hi]
        return sum(trimmed) / max(1, len(trimmed))


