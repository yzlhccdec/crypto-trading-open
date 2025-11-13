# 本金保护管理器 (CapitalProtectionManager) 功能说明

## 📋 概述

`CapitalProtectionManager` 是网格交易系统的**本金保护机制**，用于在价格不利变动时保护初始本金，并在回本后自动重置网格。

**文件位置**: `core/services/grid/capital_protection/capital_protection_manager.py`

---

## 🎯 核心功能

### 1. 记录初始本金
- 在网格启动时记录初始抵押品余额作为本金基准
- 支持重新初始化（网格重置后）

### 2. 监控价格触发
- 当价格移动到网格特定百分比位置时触发本金保护模式
- 默认触发阈值：50%（可配置）

### 3. 检查回本状态
- 持续监控抵押品余额
- 当抵押品 >= 初始本金时，判定为已回本
- 支持 $0.01 容差，避免精度问题

### 4. 自动重置网格
- 回本后自动执行网格重置
- 平仓后重新初始化网格和本金

---

## 🔧 配置参数

### 在 GridConfig 中配置

```python
# 启用本金保护
capital_protection_enabled: bool = True

# 触发阈值（网格进度百分比）
capital_protection_trigger_percent: int = 50  # 默认50%
```

### 配置示例 (YAML)

```yaml
# config/grid/backpack_capital_protection_long_btc.yaml
capital_protection_enabled: true
capital_protection_trigger_percent: 50  # 当价格跌到网格的50%位置时触发
```

---

## 🔄 工作流程

### 阶段 1: 初始化

```
网格启动
  ↓
BalanceMonitor 获取抵押品余额
  ↓
CapitalProtectionManager.initialize_capital(余额)
  ↓
记录初始本金
```

### 阶段 2: 监控价格

```
价格更新（每次价格变化时）
  ↓
GridCoordinator._check_capital_protection_mode()
  ↓
检查 should_trigger(current_price, current_grid_index)
  ↓
如果价格 <= 触发网格位置
  ↓
激活本金保护模式
```

### 阶段 3: 等待回本

```
本金保护已激活
  ↓
持续监控抵押品余额
  ↓
检查 check_capital_recovery(当前余额)
  ↓
如果 当前余额 >= 初始本金（容差±$0.01）
  ↓
触发网格重置
```

### 阶段 4: 重置网格

```
执行 execute_capital_protection_reset()
  ↓
1. 取消所有挂单
  ↓
2. 平仓（市价单）
  ↓
3. 重置网格状态
  ↓
4. 重新初始化本金（使用新的抵押品余额）
  ↓
5. 重新创建网格订单
```

---

## 📊 触发逻辑

### 做多网格（LONG）

```
Grid 1 (最低价) ← 触发位置（如 Grid 100，50%）
  ↓
价格下跌到触发位置
  ↓
激活本金保护
```

**示例**:
- 网格数量: 200格
- 触发阈值: 50%
- 触发网格: Grid 100
- 当价格跌到 Grid 100 或更低时触发

### 做空网格（SHORT）

```
Grid 1 (最高价) ← 触发位置（如 Grid 100，50%）
  ↓
价格上涨到触发位置
  ↓
激活本金保护
```

**示例**:
- 网格数量: 200格
- 触发阈值: 50%
- 触发网格: Grid 100
- 当价格涨到 Grid 100 或更高时触发

---

## 💡 关键方法

### 1. `initialize_capital(initial_capital, is_reinit=False)`

初始化或重新初始化本金

```python
# 首次初始化
manager.initialize_capital(Decimal("1000.00"))

# 网格重置后重新初始化
manager.initialize_capital(Decimal("1050.00"), is_reinit=True)
```

### 2. `should_trigger(current_price, current_grid_index) -> bool`

检查是否应该触发本金保护

```python
if manager.should_trigger(current_price, current_grid_index):
    manager.activate()
```

### 3. `activate()`

激活本金保护模式

```python
manager.activate()
# 设置 _is_active = True
# 记录激活时间
```

### 4. `check_capital_recovery(current_collateral) -> bool`

检查抵押品是否回本

```python
if manager.check_capital_recovery(current_balance):
    # 已回本，执行重置
    await reset_manager.execute_capital_protection_reset()
```

### 5. `get_profit_loss(current_collateral) -> Decimal`

获取盈亏金额

```python
profit_loss = manager.get_profit_loss(current_balance)
# 正数 = 盈利，负数 = 亏损
```

### 6. `get_profit_loss_rate(current_collateral) -> Decimal`

获取盈亏率（百分比）

```python
profit_rate = manager.get_profit_loss_rate(current_balance)
# 例如: 5.5 表示盈利 5.5%
```

### 7. `reset()`

重置管理器状态（不清除初始本金）

```python
manager.reset()
# 清除激活状态，等待新的初始化
```

---

## 🔍 使用示例

### 在 GridCoordinator 中的使用

```python
class GridCoordinator:
    def __init__(self, config: GridConfig):
        # 初始化本金保护管理器
        if config.capital_protection_enabled:
            self.capital_protection_manager = CapitalProtectionManager(config)
    
    async def _check_capital_protection_mode(self, current_price, current_grid_index):
        """检查本金保护模式"""
        if not self.capital_protection_manager:
            return
        
        # 如果已激活，检查是否回本
        if self.capital_protection_manager.is_active():
            if self.capital_protection_manager.check_capital_recovery(
                self.balance_monitor.collateral_balance
            ):
                # 已回本，执行重置
                await self.reset_manager.execute_capital_protection_reset()
        else:
            # 检查是否应该触发
            if self.capital_protection_manager.should_trigger(
                current_price, current_grid_index
            ):
                self.capital_protection_manager.activate()
```

### 在 BalanceMonitor 中的初始化

```python
class BalanceMonitor:
    def _initialize_managers_capital(self):
        """初始化各个管理器的本金"""
        # 获取抵押品余额
        collateral = self._collateral_balance
        
        # 初始化本金保护管理器
        if self.coordinator.capital_protection_manager:
            if self.coordinator.capital_protection_manager.get_initial_capital() == 0:
                self.coordinator.capital_protection_manager.initialize_capital(
                    collateral
                )
```

---

## 📈 实际运行场景

### 场景 1: 正常触发和回本

```
1. 网格启动，初始本金: $1000
2. 价格下跌到 Grid 100（50%位置）
3. 🛡️ 本金保护激活
4. 价格继续下跌，抵押品余额: $950（亏损 $50）
5. 价格反弹，抵押品余额: $1000.01
6. ✅ 检测到回本（$1000.01 >= $1000）
7. 🔄 执行网格重置
8. 重新初始化本金: $1000.01
9. 重新创建网格订单
```

### 场景 2: 容差处理

```
初始本金: $1000.00
当前余额: $999.99（亏损 $0.01）

检查: $999.99 >= ($1000.00 - $0.01) = $999.99
结果: ✅ 已回本（在容差范围内）
```

### 场景 3: 多次触发

```
第1次:
- 初始本金: $1000
- 触发 → 回本 → 重置
- 新本金: $1050

第2次:
- 初始本金: $1050（重新初始化）
- 触发 → 回本 → 重置
- 新本金: $1100
```

---

## ⚙️ 容差机制

### 为什么需要容差？

由于浮点数精度问题，可能出现：
- 初始本金: $1000.0000000001
- 当前余额: $999.9999999999
- 实际差异: < $0.01

### 容差设置

```python
tolerance = Decimal('0.01')  # $0.01 容差
is_recovered = profit_loss >= -tolerance
```

**判断逻辑**:
- 如果 `当前余额 >= (初始本金 - $0.01)`，视为已回本
- 避免因精度问题导致无法回本

---

## 🎨 UI 显示

### 终端 UI 中的显示

```python
# 在 GridTerminalUI 中显示本金保护状态
if capital_protection_enabled:
    if capital_protection_active:
        # 显示激活状态
        status = "🛡️ 本金保护已激活"
        # 显示盈亏信息
        profit_loss = manager.get_profit_loss(current_balance)
        profit_rate = manager.get_profit_loss_rate(current_balance)
    else:
        # 显示待触发状态
        status = "本金保护待触发"
```

---

## 🔗 相关组件

### 1. GridCoordinator
- 调用 `_check_capital_protection_mode()` 检查触发条件
- 在价格更新时自动检查

### 2. BalanceMonitor
- 提供抵押品余额数据
- 初始化本金保护管理器的初始本金

### 3. GridResetManager
- 执行本金保护重置
- 平仓、重置网格、重新初始化

### 4. GridConfig
- 提供配置参数
- `capital_protection_enabled`
- `capital_protection_trigger_percent`

---

## 📝 配置示例

### 完整配置示例

```yaml
# config/grid/backpack_capital_protection_long_btc.yaml
exchange: backpack
symbol: BTC_USDC_PERP
grid_type: long
grid_interval: 50
order_amount: 10
lower_price: 100000
upper_price: 110000
grid_count: 200

# 本金保护配置
capital_protection_enabled: true
capital_protection_trigger_percent: 50  # 50%位置触发

# 其他配置...
leverage: 10
margin_mode: isolated
```

---

## ⚠️ 注意事项

### 1. 触发时机
- 只在价格移动到触发位置时激活
- 激活后不会重复触发（直到重置）

### 2. 回本判断
- 使用抵押品余额（不是持仓盈亏）
- 包含 $0.01 容差

### 3. 重置流程
- 重置会平仓所有持仓
- 重置后会重新初始化本金
- 重置后重新创建网格订单

### 4. 与剥头皮模式
- 本金保护优先级高于剥头皮模式
- 本金保护激活时，剥头皮模式会被暂停

---

## 🎯 适用场景

### ✅ 适合使用本金保护

1. **波动较大的市场**
   - 价格可能大幅偏离初始位置
   - 需要保护本金不受大幅亏损

2. **长期运行策略**
   - 网格可能运行数天或数周
   - 需要自动止损和重置机制

3. **风险控制**
   - 不希望亏损超过一定阈值
   - 希望自动回本后重新开始

### ❌ 不适合使用本金保护

1. **高频交易**
   - 频繁触发可能影响交易效率

2. **小波动市场**
   - 价格很少触发保护阈值
   - 功能可能用不上

---

## 📚 相关文档

- [网格交易系统文档](README.md)
- [网格重置管理器](core/services/grid/coordinator/grid_reset_manager.py)
- [余额监控器](core/services/grid/coordinator/balance_monitor.py)

---

**总结**: `CapitalProtectionManager` 是一个自动化的本金保护机制，在价格不利变动时激活，持续监控回本状态，并在回本后自动重置网格，帮助保护初始本金并实现自动恢复。

