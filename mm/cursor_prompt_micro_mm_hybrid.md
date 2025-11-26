
# Cursor 指令：在新工程上落地“合成价（Composite）+ μMM”的两阶段方案
**目标**  
1) **阶段一（监控）**：汇聚多个大交易所的合约中价形成**合成价 S\***，与本地小交易所 `S_local` 联合产生监控指标，连续写入 InfluxDB，并提供 Grafana 查询面板；**不下单**。  
2) **阶段二（交易）**：在指标确认有效后，打开交易开关，按 **Hybrid μMM**（合成价驱动 α/σ，本地所驱动 tox/执行）进行做市；保留工程里已有的下单、风控、日志与 Influx 打点能力；**不要重复造轮子**。

> 说明：本方案**不删除**原有 Box/ATR/中观模块，只是将其退出“微观决策”。微观层改用 μMM + 合成价。


---

## 一、目录与文件改动（新增 / 小改）
**新增文件**（最小侵入，尽量复用现有工具与接口）
- `composite_price.py`：合成价聚合器（多所 mid → S\*、staleness、σ\*、σ_pair、d_local）。  
- `micro_mm_controller.py`：μMM 控制器（**含 Hybrid 扩展**：S\* / d_local / σ\* / σ_pair / staleness 驱动报价与 gating）。
- `market_making.py`：
  - **两阶段开关**：`monitor_only=True`、`trade_enabled=False`；
  - 在主循环/行情回调中，采集**本地 L2/Trades**与**合成价**，构造 μ 信号；
  - 阶段一仅打点；阶段二调用控制器产出报价并复用你的下单器（GRVT/Lighter 等）；
  - 写入 Influx：`composite_state`、`micro_mm_state`、`micro_markout`。

> 若工程已有类似“价格聚合/控制器”文件，请**直接合并**本指令的核心逻辑，避免重复模块。


---

## 二、合成价聚合器（新增 `composite_price.py`）
> 设计目标：多所 mid 的**加权稳健聚合（Weighted Median / Trimmed Mean）** + **陈旧性过滤（staleness）** + **波动统计（σ\*）** + **相对波动（σ_pair）** + **本地错价（d_local）**。  
> 不依赖第三方库；对接你工程里已有的数据总线即可。

```python
# composite_price.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Optional, List, Tuple
import time
import math

@dataclass
class ExchangeQuote:
    exch: str            # 交易所名，如 "binance", "okx", "bybit"
    mid: float           # 中价 (best_bid + best_ask)/2
    ts_ms: int           # 报价到达本机/进程的毫秒时间戳
    top_depth: float = 1.0  # 可选：L1 深度或 L1-L3 合计，用于加权；没有就留 1.0

class CompositePrice:
    """
    多所 mid 聚合 → S*；并计算：staleness、sigma_star（合成价短时波动）、
    sigma_pair（S_local 与 S* 的相对波动）与 d_local（本地错价）。
    """
    def __init__(self,
                 staleness_cut_ms: int = 300,
                 method: str = "wmedian",       # "wmedian" or "tmean"
                 trim_q: float = 0.1,           # tmean 修剪比例
                 ewma_alpha_sigma: float = 0.2, # σ* 的 EWMA 系数（0..1）
                 ewma_alpha_pair: float = 0.2   # σ_pair 的 EWMA 系数
                 ):
        self.staleness_cut_ms = staleness_cut_ms
        self.method = method
        self.trim_q = trim_q
        self.ewma_alpha_sigma = ewma_alpha_sigma
        self.ewma_alpha_pair = ewma_alpha_pair

        self._quotes: Dict[str, ExchangeQuote] = {}
        self._last_S_star: Optional[float] = None
        self._ewma_sigma_star: float = 0.0
        self._ewma_sigma_pair: float = 0.0

    def update(self, q: ExchangeQuote) -> None:
        self._quotes[q.exch] = q

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _active_quotes(self) -> List[ExchangeQuote]:
        now_ms = self._now_ms()
        act: List[ExchangeQuote] = []
        for q in self._quotes.values():
            if now_ms - q.ts_ms <= self.staleness_cut_ms:
                act.append(q)
        return act

    @staticmethod
    def _weighted_median(vals_w: List[Tuple[float, float]]) -> float:
        # vals_w: list of (value, weight)
        vals_w = [(v, max(1e-9, w)) for (v, w) in vals_w]
        vals_w.sort(key=lambda x: x[0])
        total = sum(w for _, w in vals_w)
        acc = 0.0
        for v, w in vals_w:
            acc += w
            if acc >= 0.5 * total:
                return v
        return vals_w[-1][0]

    @staticmethod
    def _trimmed_mean(values: List[float], trim_q: float) -> float:
        if not values:
            return float("nan")
        values = sorted(values)
        n = len(values)
        k = int(n * trim_q)
        lo = min(k, n-1)
        hi = max(n - k, lo + 1)
        trimmed = values[lo:hi]
        return sum(trimmed) / max(1, len(trimmed))

    def compute(self, S_local: Optional[float] = None) -> Optional[dict]:
        act = self._active_quotes()
        n = len(act)
        if n < 2:  # 至少两所有效，才可信
            return None

        # 1) S*: 合成价
        if self.method == "wmedian":
            vals_w = [(q.mid, math.sqrt(max(1e-9, q.top_depth))) for q in act]
            S_star = self._weighted_median(vals_w)
        else:  # "tmean"
            S_star = self._trimmed_mean([q.mid for q in act], self.trim_q)

        # 2) staleness 统计
        now_ms = self._now_ms()
        stales = [now_ms - q.ts_ms for q in act]
        staleness_min = min(stales)
        staleness_p95 = sorted(stales)[int(0.95 * (len(stales)-1))]

        # 3) σ*: 合成价短时波动（EWMA |ΔS*|/S*）
        if self._last_S_star is not None and S_star > 0:
            dm = abs(S_star - self._last_S_star) / S_star
            self._ewma_sigma_star = (1 - self.ewma_alpha_sigma) * self._ewma_sigma_star + self.ewma_alpha_sigma * dm
        self._last_S_star = S_star

        out = {
            "S_star": S_star,
            "n_exch": n,
            "staleness_ms_min": staleness_min,
            "staleness_ms_p95": staleness_p95,
            "sigma_star": self._ewma_sigma_star,
        }

        # 4) σ_pair 与 d_local（若给了本地中价）
        if S_local is not None and S_star > 0:
            d_local = (S_local - S_star) / S_star
            # pair 的短时变动（EWMA |Δd|）
            self._ewma_sigma_pair = (1 - self.ewma_alpha_pair) * self._ewma_sigma_pair + self.ewma_alpha_pair * abs(d_local)
            out.update({
                "d_local": d_local,
                "sigma_pair": self._ewma_sigma_pair,
            })

        return out
```

> 对接方式：在你工程现有的**大所行情适配器**各自拿到 mid 与到达时间戳，调用 `CompositePrice.update(ExchangeQuote(...))`；主循环中定期 `compute(S_local=float(local_mid))`。  
> **注意**：若某路行情断流/陈旧（>staleness_cut_ms），会被自动剔除；**n_exch<2 时，Hybrid 功能应自动退化为本地模式**。


---

## 三、μMM 控制器（Hybrid 版，`micro_mm_controller.py`）
> 在 μMM 的基础上，**引入合成价 S\***、**本地错价 d_local**、**σ\*** 与 **σ_pair**，动态决定：  
> - **reservation**：`(1-β)S_local + βS* + κ·α_micro·S_local - γ·inv_ratio·S_local`  
> - **半边 spread**：基于 `σ*` 与本地 `tox`，并对**不利一侧**按 `|d|` 放大  
> - **gating**：staleness 过大、n_exch<2 → β=0；`|d|` 过大且持续 → 产生机会型对冲意图（仅建议，实际对冲在主策略里实现）

```python
# micro_mm_controller.py
from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

def _bps_to_price(mid: Decimal, bps: float) -> Decimal:
    return Decimal(str(float(mid) * bps / 1e4))

def _round_to_tick(px: Decimal, tick: float) -> Decimal:
    return Decimal(str(round(float(px)/tick)*tick))

@dataclass
class MicroMMConfig:
    maker_fee_bps: float = 0.5
    tick_size: float = 0.1
    base_spread_bps: float = 2.0
    min_spread_bps: float = 1.0
    max_spread_bps: float = 20.0

    # μ signals horizon
    horizon_s: float = 1.0

    # coefficients
    c_sigma: float = 3.0      # 合成价 σ* 的系数
    c_tox: float = 5.0        # 本地 tox 的系数
    alpha_kappa: float = 0.5  # 方向项映射到 reservation
    inv_gamma_bps: float = 3.0

    # Hybrid: 合成价权重与错价调整
    beta0: float = 0.3        # 基础 β（S* 权重）
    beta_max: float = 0.6
    staleness_cut_ms: float = 300.0
    sigma_pair_high: float = 0.005   # 当 σ_pair 接近此值 → 降低 β
    d_shift_coeff: float = 0.3       # 半边 spread 随 |d| 的不对称调节强度
    d_shift_cap: float = 0.5         # 不对称调节上限（倍数）

    # gating thresholds
    alpha_th: float = 0.05
    tox_pause: float = 0.8
    tox_defensive: float = 0.5
    d_taker_bps: float = 15.0        # 机会型对冲的错价阈值（bps）；成本保守估计

    # sizing
    base_size: float = 0.001
    min_size: float = 0.001
    max_size: float = 0.02

    # ttl
    ttl_min_ms: int = 300
    ttl_max_ms: int = 1500

@dataclass
class MicroSignals:
    # 本地 μ 信号
    mid: Decimal
    sigma_micro_local: float   # 若已有本地 σ，则传入；否则置 0
    tox: float                 # 0..1
    alpha_micro: float         # -1..1

    inv_ratio: float           # -1..1
    queue_long: bool           # 本地队列是否过长

    # 合成价相关（可选）
    composite_mid: Optional[Decimal] = None
    n_exch: int = 0
    staleness_ms: float = 1e9
    sigma_star: float = 0.0
    sigma_pair: float = 0.0
    d_local: float = 0.0       # (S_local - S*)/S* ，比例（0.001=10bps）

@dataclass
class QuotePlan:
    bid: Optional[Decimal]
    ask: Optional[Decimal]
    bid_size: float
    ask_size: float
    ttl_ms: int
    mode: str                  # "B"|"A"|"C"|"D"
    spread_bps_used: float
    beta_used: float
    d_local_bps: float
    hedge_intent: int          # 0/1 是否建议机会型对冲

class MicroMMController:
    def __init__(self, cfg: MicroMMConfig):
        self.cfg = cfg

    def _compute_beta(self, staleness_ms: float, sigma_pair: float, n_exch: int) -> float:
        if n_exch < 2 or staleness_ms > self.cfg.staleness_cut_ms:
            return 0.0
        fresh = max(0.0, 1.0 - staleness_ms / self.cfg.staleness_cut_ms)  # 0..1
        consistent = max(0.0, 1.0 - sigma_pair / max(1e-9, self.cfg.sigma_pair_high))  # 0..1
        beta = self.cfg.beta0 * fresh * consistent
        return max(0.0, min(self.cfg.beta_max, beta))

    def plan(self, sig: MicroSignals) -> QuotePlan:
        S = float(sig.mid)
        if S <= 0:
            return QuotePlan(None, None, 0.0, 0.0, self.cfg.ttl_max_ms, "D", 0.0, 0.0, 0, 0)

        # 1) 模式选择
        if sig.tox >= self.cfg.tox_pause:
            mode = "D"
        elif sig.tox >= self.cfg.tox_defensive:
            mode = "C"
        elif abs(sig.alpha_micro) <= self.cfg.alpha_th:
            mode = "A"
        else:
            mode = "B"

        # 2) β 与 d
        beta = self._compute_beta(sig.staleness_ms, sig.sigma_pair, sig.n_exch)
        S_star = float(sig.composite_mid) if (sig.composite_mid is not None and beta > 0) else S
        d = sig.d_local
        d_bps = d * 1e4

        # 3) 基础 spread（σ* + tox）
        sigma_for_spread = max(sig.sigma_star, sig.sigma_micro_local)
        spread_bps = max(self.cfg.base_spread_bps, self.cfg.min_spread_bps)
        spread_bps += self.cfg.c_sigma * sigma_for_spread * 1e4
        spread_bps += self.cfg.c_tox   * sig.tox           * 1e4
        spread_bps = min(spread_bps, self.cfg.max_spread_bps)

        h = _bps_to_price(sig.mid, spread_bps/2)

        # 4) reservation（S 与 S* 混合 + α + 库存）
        reservation = (Decimal(str((1.0 - beta) * S)) + Decimal(str(beta * S_star)))
        reservation += _bps_to_price(sig.mid, self.cfg.alpha_kappa * sig.alpha_micro * 1e4)
        reservation -= _bps_to_price(sig.mid, sig.inv_ratio * self.cfg.inv_gamma_bps)

        maker_adj = _bps_to_price(sig.mid, self.cfg.maker_fee_bps)

        # 5) 半边不对称（按 |d| 调不利侧）
        d_scale = min(self.cfg.d_shift_cap, 1.0 + self.cfg.d_shift_coeff * abs(d))
        if d >= 0:  # 本地偏贵：下行风险 → 扩 bid，收 ask
            h_bid = h * Decimal(str(d_scale))
            h_ask = h / Decimal(str(d_scale))
        else:      # 本地偏便宜：上行风险
            h_bid = h / Decimal(str(d_scale))
            h_ask = h * Decimal(str(d_scale))

        bid = reservation - h_bid - maker_adj
        ask = reservation + h_ask + maker_adj

        if sig.queue_long:
            bid -= Decimal(str(self.cfg.tick_size))
            ask += Decimal(str(self.cfg.tick_size))

        bid = _round_to_tick(bid, self.cfg.tick_size)
        ask = _round_to_tick(ask, self.cfg.tick_size)

        if mode == "D":
            bid = None; ask = None

        # 6) size & TTL
        sigma_norm = min(1.0, sigma_for_spread * 50)
        g = max(0.1, 1.0 - 0.5*sig.tox) * max(0.2, 1.0 - 0.5*sigma_norm)
        base = self.cfg.base_size * (1.0 - min(1.0, abs(sig.inv_ratio)))
        size = max(self.cfg.min_size, min(self.cfg.max_size, base * g))

        if sig.alpha_micro > self.cfg.alpha_th:
            bid_size = 0.7*size; ask_size = 1.3*size
        elif sig.alpha_micro < -self.cfg.alpha_th:
            bid_size = 1.3*size; ask_size = 0.7*size
        else:
            bid_size = size; ask_size = size

        ttl = int(self.cfg.ttl_max_ms - (self.cfg.ttl_max_ms-self.cfg.ttl_min_ms)*
                  min(1.0, 0.6*sig.tox + 0.4*sigma_norm))

        hedge_intent = 1 if abs(d_bps) >= self.cfg.d_taker_bps and beta > 0 else 0

        return QuotePlan(
            bid=bid, ask=ask, bid_size=bid_size, ask_size=ask_size,
            ttl_ms=ttl, mode=mode, spread_bps_used=spread_bps,
            beta_used=beta, d_local_bps=d_bps, hedge_intent=hedge_intent
        )
```


---

## 四、对接：在 `as_market_making.py` 集成两阶段逻辑
> 复用工程里已有：下单器、风险/库存管理、Influx 写点、日志。以下为**伪代码/骨架**，请贴入你的主循环并对接已有方法名。

### 1) 配置项
```python
# μMM 两阶段开关
monitor_only: bool = True
trade_enabled: bool = False

# Composite 与 μMM 参数（可并入你的 Config）
composite_cfg = {
    "staleness_cut_ms": 300,
    "method": "wmedian",  # or "tmean"
    "trim_q": 0.1,
}
micro_cfg = {
    "maker_fee_bps": 0.5,
    "tick_size": 0.1,
    "base_spread_bps": 2.0,
    "min_spread_bps": 1.0,
    "max_spread_bps": 20.0,
    "horizon_s": 1.0,
    "c_sigma": 3.0, "c_tox": 5.0,
    "alpha_kappa": 0.5, "inv_gamma_bps": 3.0,
    "beta0": 0.3, "beta_max": 0.6, "staleness_cut_ms": 300.0,
    "sigma_pair_high": 0.005, "d_shift_coeff": 0.3, "d_shift_cap": 0.5,
    "alpha_th": 0.05, "tox_pause": 0.8, "tox_defensive": 0.5,
    "d_taker_bps": 15.0,
    "base_size": 0.001, "min_size": 0.001, "max_size": 0.02,
    "ttl_min_ms": 300, "ttl_max_ms": 1500,
}
```

### 2) 初始化
```python
from composite_price import CompositePrice, ExchangeQuote
from micro_mm_controller import MicroMMController, MicroMMConfig, MicroSignals

self.composite = CompositePrice(**self.config.composite_cfg)
self.micro_ctrl = MicroMMController(MicroMMConfig(**self.config.micro_cfg))
```

### 3) 合成价更新（在各大所回调里调用一次）
```python
def on_ext_quote(self, exch: str, best_bid: float, best_ask: float, top_depth: float, ts_ms: int):
    mid = 0.5*(best_bid + best_ask)
    self.composite.update(ExchangeQuote(exch=exch, mid=mid, top_depth=top_depth, ts_ms=ts_ms))
```

### 4) 主循环：构造 μ 信号 + 打点 + 两阶段控制
```python
def on_tick(self):
    # 本地行情
    local_mid = self._get_mid_price_decimal()  # 现有方法
    best_bid, best_ask = self._get_best_quotes()
    tox = self._calc_local_toxicity()          # 0..1：基于本地 OBI/OFI/主动量
    alpha = self._alpha_micro_from_composite() # 建议用合成价斜率；工程内实现
    sigma_local = self._sigma_micro_local()    # 若已有本地 σ，则传入

    # 合成价
    comp = self.composite.compute(S_local=float(local_mid))  # 可能为 None（不足两所）
    if comp is not None:
        S_star = Decimal(str(comp["S_star"]))
        n_exch = int(comp["n_exch"])
        staleness = float(comp["staleness_ms_min"])
        sigma_star = float(comp["sigma_star"])
        sigma_pair = float(comp.get("sigma_pair", 0.0))
        d_local = float(comp.get("d_local", 0.0))
    else:
        S_star = None; n_exch = 0; staleness = 1e9; sigma_star = 0.0; sigma_pair = 0.0; d_local = 0.0

    inv_ratio = self.risk_manager.get_inventory_ratio(self.net_inventory)
    queue_long = self._queue_is_long()

    sig = MicroSignals(
        mid=local_mid,
        sigma_micro_local=sigma_local,
        tox=tox,
        alpha_micro=alpha,
        inv_ratio=inv_ratio,
        queue_long=queue_long,
        composite_mid=S_star,
        n_exch=n_exch,
        staleness_ms=staleness,
        sigma_star=sigma_star,
        sigma_pair=sigma_pair,
        d_local=d_local,
    )

    plan = self.micro_ctrl.plan(sig)

    # —— 阶段一：监控 ——
    self._write_composite_state_to_influx(local_mid, comp, plan)
    self._write_micro_state_to_influx(sig, plan, int(self.config.trade_enabled))

    if not self.config.trade_enabled:
        return  # 不下单

    # —— 阶段二：交易 ——
    if plan.bid is None and plan.ask is None:
        self._cancel_all_if_needed(reason="μMM pause")
        return

    # 交易所最小下单量保护（结合你工程已有常量）
    bid_sz = max(plan.bid_size, self.config.min_order_size_grvt)
    ask_sz = max(plan.ask_size, self.config.min_order_size_grvt)

    self._quote_or_replace("bid", price=plan.bid, size=bid_sz, ttl_ms=plan.ttl_ms, mode=plan.mode)
    self._quote_or_replace("ask", price=plan.ask, size=ask_sz, ttl_ms=plan.ttl_ms, mode=plan.mode)

    # 机会型对冲意图（仅建议；实际由主策略统一风控执行）
    if plan.hedge_intent:
        self._maybe_trigger_cross_hedge(d_local_bps=plan.d_local_bps)
```

### 5) Influx 写点（新增两个方法）
```python
def _write_composite_state_to_influx(self, local_mid, comp, plan):
    try:
        fields = {
            "local_mid": float(local_mid),
            "beta_used": float(plan.beta_used),
            "d_local_bps": float(plan.d_local_bps),
        }
        if comp is not None:
            fields.update({
                "S_star": float(comp["S_star"]),
                "n_exch": int(comp["n_exch"]),
                "staleness_ms_min": float(comp["staleness_ms_min"]),
                "staleness_ms_p95": float(comp["staleness_ms_p95"]),
                "sigma_star": float(comp["sigma_star"]),
                "sigma_pair": float(comp.get("sigma_pair", 0.0)),
            })
        self._write_influx_point(
            "composite_state",
            tags={"symbol": self.config.ticker},
            fields=fields,
            timestamp=time.time(),
        )
    except Exception as e:
        self.logger.exception("write composite_state failed: %s", e)

def _write_micro_state_to_influx(self, sig, plan, trade_enabled: int):
    try:
        self._write_influx_point(
            "micro_mm_state",
            tags={"symbol": self.config.ticker, "mode": plan.mode},
            fields={
                "mid": float(sig.mid),
                "sigma_star": float(sig.sigma_star),
                "sigma_micro_local": float(sig.sigma_micro_local),
                "sigma_pair": float(sig.sigma_pair),
                "tox": float(sig.tox),
                "alpha_micro": float(sig.alpha_micro),
                "inv_ratio": float(sig.inv_ratio),
                "queue_long": int(1 if sig.queue_long else 0),
                "spread_bps": float(plan.spread_bps_used),
                "bid": float(plan.bid) if plan.bid is not None else 0.0/0.0,
                "ask": float(plan.ask) if plan.ask is not None else 0.0/0.0,
                "bid_size": float(plan.bid_size),
                "ask_size": float(plan.ask_size),
                "ttl_ms": int(plan.ttl_ms),
                "beta_used": float(plan.beta_used),
                "d_local_bps": float(plan.d_local_bps),
                "hedge_intent": int(plan.hedge_intent),
                "trade_enabled": int(trade_enabled),
            },
            timestamp=time.time(),
        )
    except Exception as e:
        self.logger.exception("write micro_mm_state failed: %s", e)
```

### 6) 成交后的短时 markout（补充 d_bin/sigma_pair_bin）
在你已有的 markout 写入中追加：`d_bin`、`sigma_pair_bin`。

```python
def _write_markout(self, horizon_s: float, side: int, mid_at_fill: float, fill_px: float, sig_snapshot: dict):
    mid_now = float(self._get_mid_price_decimal())
    signed_move = (mid_now - fill_px) if side > 0 else (fill_px - mid_now)
    markout_bps = 1e4 * signed_move / max(1e-9, mid_at_fill)

    tox_bin = f"{int(min(9, max(0, sig_snapshot['tox']*10)))}/10"
    sigma_bin = f"{int(min(9, max(0, sig_snapshot['sigma_star']*1000)))}e-3"
    d_bin = f"{int(min(50, max(0, abs(sig_snapshot['d_local']*1e4))))}bps"           # 0..50 bps
    sp_bin = f"{int(min(50, max(0, sig_snapshot['sigma_pair']*1e4)))}bps"           # 0..50 bps

    self._write_influx_point(
        "micro_markout",
        tags={
            "symbol": self.config.ticker,
            "horizon": f"{int(horizon_s)}s",
            "tox_bin": tox_bin,
            "sigma_bin": sigma_bin,
            "d_bin": d_bin,
            "sigma_pair_bin": sp_bin,
        },
        fields={"markout_bps": float(markout_bps), "side": int(side)},
        timestamp=time.time(),
    )
```


---

## 五、Grafana 查询（监控面板）
> 以下 Flux 可直接复制。若 bucket 非 `mm_box_raw`，请替换。所有“空值”统一用 `0.0/0.0` 制造 NaN。

### 1) 本地 vs 合成价（同图两轴或双线）
```flux
from(bucket: "mm_box_raw")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "composite_state" and r.symbol == "BTC")
  |> filter(fn: (r) => r._field == "local_mid" or r._field == "S_star")
  |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)
  |> yield(name: "price_overlay")
```

### 2) 错价与合成价参数（d_local、β、n_exch、staleness、σ\*、σ_pair）
```flux
from(bucket: "mm_box_raw")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "composite_state" and r.symbol == "BTC")
  |> filter(fn: (r) => r._field == "d_local_bps" or r._field == "beta_used" or r._field == "n_exch" or r._field == "staleness_ms_min" or r._field == "sigma_star" or r._field == "sigma_pair")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> yield(name: "composite_params")
```

### 3) μMM 状态（σ\*、σ_local、tox、alpha、inv、queue、spread、TTL、hedge_intent、trade_enabled）
```flux
from(bucket: "mm_box_raw")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "micro_mm_state" and r.symbol == "BTC")
  |> filter(fn: (r) => r._field == "sigma_star" or r._field == "sigma_micro_local" or r._field == "tox" or r._field == "alpha_micro" or r._field == "inv_ratio" or r._field == "queue_long" or r._field == "spread_bps" or r._field == "ttl_ms" or r._field == "hedge_intent" or r._field == "trade_enabled")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> yield(name: "micro_state")
```

### 4) 条件 EV：按 d_bin / sigma_pair_bin 分箱（1s）
```flux
from(bucket: "mm_box_raw")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "micro_markout" and r.symbol == "BTC" and r.horizon == "1s")
  |> filter(fn: (r) => r._field == "markout_bps")
  |> group(columns: ["d_bin", "sigma_pair_bin"])
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> yield(name: "ev_by_d_sigma_pair")
```

### 5) “可赚时间比例”估计：(|d|-τ_taker)_+ 占比（请在 Grafana 变量里设置 `var.tau_bps`）
```flux
import "experimental"

data =
  from(bucket: "mm_box_raw")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "composite_state" and r.symbol == "BTC")
    |> filter(fn: (r) => r._field == "d_local_bps")
    |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)

data
  |> map(fn: (r) => ({
      r with
      excess: if abs(r._value) > float(v.tau_bps) then abs(r._value) - float(v.tau_bps) else 0.0/0.0
  }))
  |> experimental.unpivot()
  |> yield(name: "excess_over_tau")
```


---

## 六、参数与风控建议（起步值）
- β 动态：`beta0=0.3, beta_max=0.6, staleness_cut_ms=300, sigma_pair_high=0.005`  
- 错价不对称：`d_shift_coeff=0.3, d_shift_cap=0.5`  
- 机会型对冲阈值：`d_taker_bps=15`（请按两边 taker 费 + 典型滑点 + 安全垫校正）  
- TTL：`300–1500ms`，合成价**明显领先**本地时（观察 staleness/σ_pair），取下限。  
- 仍保留你工程的**账户级止损**（如“单次突破亏损不超 2%”）与**最小下单量**保护。


---

## 七、验收 checklist
1) **阶段一**：Grafana 可见 `price_overlay / composite_params / micro_state / ev_by_d_sigma_pair / excess_over_tau` 曲线；  
2) **阶段二**：`trade_enabled=True` 后开始下单；`micro_markout` 有 1s/3s 标记收益，条件 EV 在 `d_bin/sigma_pair_bin` 中为非空；  
3) `n_exch<2` 或 `staleness>cut` 时，`beta_used→0`，系统自动退化为**本地-only**。


---

## 八、对接提示（避免重复造轮子）
- **行情接入**：复用工程里已有的大所行情适配器；只需在回调里调用 `CompositePrice.update(...)`。  
- **下单路径**：继续用现有 `_quote_or_replace` / TTL / 撤单逻辑；控制器只回 `价/量/TTL/模式`。  
- **Influx 打点**：复用工程的 `_write_influx_point`；measurement 名称见上文。  
- **风控/库存**：继续用你的 `risk_manager`；本方案只改变“何时、何价、挂多大”。

> 如需我把以上骨架改写成你新工程的**具体方法名**与**类结构**，把相关文件贴上来即可按文件级别给出完整补丁。
