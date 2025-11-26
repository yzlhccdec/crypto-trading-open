#!/usr/bin/env python3
"""
MESA引擎交易所适配层演示脚本
展示如何使用交易所适配器进行各种操作
"""

import asyncio
import sys
import os
import yaml
from pathlib import Path
from decimal import Decimal
from typing import Dict, Any, Optional

# 添加项目根目录到路径（必须在导入 core 模块之前）
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

import structlog
from core.adapters.exchanges.models import OrderSide, OrderType, ExchangeType
from core.adapters.exchanges.interface import ExchangeConfig
from core.adapters.exchanges.factory import ExchangeFactory
from core.adapters.exchanges.manager import ExchangeManager


# 配置日志
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class ExchangeAdapterDemo:
    """交易所适配层演示类"""

    def __init__(self):
        # ExchangeManager 现在接受可选的 event_bus 参数，可以为 None
        self.exchange_manager = ExchangeManager(event_bus=None)
        self.factory = ExchangeFactory()

    async def initialize(self):
        """初始化演示环境"""
        logger.info("🚀 初始化交易所适配层演示")

        # 配置演示用的交易所（使用测试配置）
        demo_configs = self._get_demo_configs()

        # 注册交易所适配器
        for exchange_id, config in demo_configs.items():
            try:
                adapter = self.factory.create_adapter(exchange_id, config)
                self.exchange_manager.register_exchange(exchange_id, config)
                logger.info(f"✅ 注册交易所适配器: {exchange_id}")
            except Exception as e:
                logger.warning(f"⚠️ 跳过交易所 {exchange_id}: {e}")

        # 启动交易所管理器
        await self.exchange_manager.start()
        logger.info("✅ 交易所管理器启动完成")

    def _get_demo_configs(self) -> Dict[str, ExchangeConfig]:
        """获取演示用的交易所配置（从配置文件加载，只加载 GRVT）"""
        configs = {}
        config_dir = Path(__file__).parent.parent / "config" / "exchanges"
        
        # 只加载 GRVT 交易所
        exchange_id = 'grvt'
        config_file = config_dir / f"{exchange_id}_config.yaml"
        
        if not config_file.exists():
            logger.warning(f"⚠️ GRVT 配置文件不存在: {config_file}")
            logger.info("💡 提示：请确保配置文件存在，或设置环境变量：")
            logger.info("   - GRVT_PRIVATE_KEY")
            logger.info("   - GRVT_API_KEY")
            logger.info("   - GRVT_TRADING_ACCOUNT_ID")
            logger.info("   - GRVT_ENV (可选，默认testnet)")
            return configs
        
        try:
            config = self._load_exchange_config_from_file(exchange_id, config_file)
            if config:
                configs[exchange_id] = config
                logger.info(f"✅ 从配置文件加载: {exchange_id}")
            else:
                logger.warning(f"⚠️ GRVT 配置文件加载失败")
        except Exception as e:
            logger.error(f"❌ 加载 GRVT 配置失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
        
        return configs
    
    def _load_exchange_config_from_file(self, exchange_id: str, config_file: Path) -> Optional[ExchangeConfig]:
        """从YAML配置文件加载交易所配置"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            # 获取交易所配置块
            exchange_data = config_data.get(exchange_id, {})
            
            # 检查是否启用
            if not exchange_data.get('enabled', True):
                logger.debug(f"{exchange_id} 未启用，跳过")
                return None
            
            # 获取API配置
            api_config = exchange_data.get('api_config', {})
            auth_config = api_config.get('auth', {}) if api_config else {}
            
            # 优先使用环境变量，其次使用配置文件
            api_key = os.getenv(f'{exchange_id.upper()}_API_KEY') or auth_config.get('api_key', '') or exchange_data.get('api_key', '')
            api_secret = os.getenv(f'{exchange_id.upper()}_API_SECRET') or auth_config.get('api_secret', '') or auth_config.get('private_key', '') or exchange_data.get('api_secret', '')
            
            # 特殊处理GRVT
            if exchange_id == 'grvt':
                private_key = os.getenv('GRVT_PRIVATE_KEY') or auth_config.get('api_key_private_key', '') or api_secret
                trading_account_id = os.getenv('GRVT_TRADING_ACCOUNT_ID') or auth_config.get('trading_account_id', '')
                env = os.getenv('GRVT_ENV') or api_config.get('env', 'testnet')
                
                return ExchangeConfig(
                    exchange_id='grvt',
                    name=exchange_data.get('name', 'GRVT'),
                    exchange_type=ExchangeType.PERPETUAL,
                    api_key=api_key,
                    api_secret=private_key,
                    testnet=(env != 'prod'),
                    enable_websocket=exchange_data.get('websocket', {}).get('enabled', True),
                    extra_params={
                        'api_key_private_key': private_key,
                        'api_key': api_key,
                        'trading_account_id': trading_account_id,
                        'env': env,
                        'endpoint_version': api_config.get('endpoint_version', 'v1'),
                        'ws_stream_version': api_config.get('ws_stream_version', 'v1'),
                    }
                )
            
            # 处理其他交易所
            exchange_type_str = exchange_data.get('type', 'perpetual').lower()
            exchange_type = ExchangeType.PERPETUAL if 'perpetual' in exchange_type_str else ExchangeType.FUTURES if 'futures' in exchange_type_str else ExchangeType.SPOT
            
            # 获取API配置
            api_base_config = exchange_data.get('api', {}) if 'api' in exchange_data else {}
            
            config = ExchangeConfig(
                exchange_id=exchange_id,
                name=exchange_data.get('name', exchange_id.upper()),
                exchange_type=exchange_type,
                api_key=api_key,
                api_secret=api_secret,
                testnet=exchange_data.get('testnet', False),
                base_url=api_base_config.get('base_url') or api_config.get('base_url'),
                ws_url=api_base_config.get('ws_url') or api_config.get('ws_url'),
                enable_websocket=exchange_data.get('websocket', {}).get('enabled', True),
                rate_limits=exchange_data.get('rate_limits', {}),
                default_leverage=exchange_data.get('default_leverage', 1),
                default_margin_mode=exchange_data.get('default_margin_mode', 'cross'),
            )
            
            return config
            
        except Exception as e:
            logger.error(f"加载 {exchange_id} 配置失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None
    
    def _get_default_demo_configs(self) -> Dict[str, ExchangeConfig]:
        """获取默认演示配置（当配置文件不存在时使用）"""
        return {
            'binance': ExchangeConfig(
                exchange_id='binance',
                name='Binance Demo',
                exchange_type=ExchangeType.FUTURES,
                api_key='demo_api_key',
                api_secret='demo_api_secret',
                testnet=True,
                rate_limits={'requests_per_second': 10},
                connect_timeout=30,
                precision={'base': 8, 'quote': 8}
            ),
            'hyperliquid': ExchangeConfig(
                exchange_id='hyperliquid',
                name='Hyperliquid Demo',
                exchange_type=ExchangeType.PERPETUAL,
                api_key='demo_api_key',
                api_secret='demo_api_secret',
                testnet=True,
                rate_limits={'requests_per_second': 5},
                connect_timeout=30,
                precision={'base': 6, 'quote': 6}
            )
        }

    async def demo_basic_info(self):
        """演示基础信息获取"""
        logger.info("📊 演示基础信息获取")

        for exchange_id in self.exchange_manager.get_registered_exchanges():
            try:
                adapter = self.exchange_manager.get_adapter(exchange_id)
                if not adapter:
                    continue

                logger.info(f"--- {exchange_id.upper()} 交易所信息 ---")

                # 获取交易所信息
                exchange_info = await adapter.get_exchange_info()
                logger.info(f"交易所名称: {exchange_info.name}")
                logger.info(f"支持功能: {exchange_info.supported_features}")
                logger.info(f"状态: {exchange_info.status}")

                # 健康检查
                health = await adapter.health_check()
                logger.info(f"健康状态: {health}")

            except Exception as e:
                logger.error(f"获取 {exchange_id} 基础信息失败: {e}")

    async def demo_market_data(self):
        """演示市场数据获取"""
        logger.info("📈 演示市场数据获取")

        # 测试符号列表
        test_symbols = ['BTC/USDT', 'ETH/USDT']

        for exchange_id in self.exchange_manager.get_registered_exchanges():
            try:
                adapter = self.exchange_manager.get_adapter(exchange_id)
                if not adapter:
                    continue

                logger.info(f"--- {exchange_id.upper()} 市场数据 ---")

                for symbol in test_symbols:
                    try:
                        # 获取行情数据
                        ticker = await adapter.get_ticker(symbol)
                        logger.info(
                            f"{symbol} 行情: 价格={ticker.last}, 买1={ticker.bid}, 卖1={ticker.ask}")

                        # 获取订单簿
                        orderbook = await adapter.get_orderbook(symbol, limit=5)
                        logger.info(
                            f"{symbol} 订单簿: 买单={len(orderbook.bids)}, 卖单={len(orderbook.asks)}")

                        # 获取最近成交
                        trades = await adapter.get_trades(symbol, limit=5)
                        logger.info(f"{symbol} 最近成交: {len(trades)}笔")

                    except Exception as e:
                        logger.warning(f"获取 {symbol} 数据失败: {e}")

            except Exception as e:
                logger.error(f"获取 {exchange_id} 市场数据失败: {e}")

    async def demo_account_info(self):
        """演示账户信息获取"""
        logger.info("💰 演示账户信息获取")

        for exchange_id in self.exchange_manager.get_registered_exchanges():
            try:
                adapter = self.exchange_manager.get_adapter(exchange_id)
                if not adapter:
                    continue

                logger.info(f"--- {exchange_id.upper()} 账户信息 ---")

                # 获取账户余额
                balances = await adapter.get_balances()
                if balances:
                    for balance in balances[:5]:  # 只显示前5个
                        if balance.total and balance.total > 0:
                            logger.info(
                                f"余额: {balance.currency} = {balance.total} (可用: {balance.free})")
                else:
                    logger.info("暂无余额数据")

                # 获取持仓信息
                positions = await adapter.get_positions()
                if positions:
                    for position in positions[:5]:  # 只显示前5个
                        if position.size and position.size != 0:
                            logger.info(
                                f"持仓: {position.symbol} {position.side.value} = {position.size}")
                else:
                    logger.info("暂无持仓数据")

            except Exception as e:
                logger.error(f"获取 {exchange_id} 账户信息失败: {e}")

    async def demo_order_management(self):
        """演示订单管理"""
        logger.info("📋 演示订单管理")

        for exchange_id in self.exchange_manager.get_registered_exchanges():
            try:
                adapter = self.exchange_manager.get_adapter(exchange_id)
                if not adapter:
                    continue

                logger.info(f"--- {exchange_id.upper()} 订单管理 ---")

                # 获取开放订单
                open_orders = await adapter.get_open_orders()
                logger.info(f"开放订单数量: {len(open_orders)}")

                # 获取历史订单
                order_history = await adapter.get_order_history(limit=5)
                logger.info(f"历史订单数量: {len(order_history)}")

                # 演示订单创建（仅模拟，不实际下单）
                logger.info("📝 模拟创建订单流程:")
                logger.info("  - 符号: BTC/USDT")
                logger.info("  - 方向: BUY")
                logger.info("  - 类型: LIMIT")
                logger.info("  - 数量: 0.001")
                logger.info("  - 价格: 50000")
                logger.info("  ⚠️ 演示模式，未实际下单")

            except Exception as e:
                logger.error(f"演示 {exchange_id} 订单管理失败: {e}")

    async def demo_websocket_subscriptions(self):
        """演示WebSocket订阅功能"""
        logger.info("🔔 演示WebSocket订阅功能")

        # 回调函数定义
        def on_ticker_update(ticker_data):
            logger.info(f"📊 行情更新: {ticker_data.symbol} = {ticker_data.last}")

        def on_orderbook_update(orderbook_data):
            logger.info(
                f"📋 订单簿更新: {orderbook_data.symbol} 买单={len(orderbook_data.bids)} 卖单={len(orderbook_data.asks)}")

        def on_trade_update(trade_data):
            logger.info(
                f"💱 成交更新: {trade_data.symbol} {trade_data.side.value} {trade_data.amount}@{trade_data.price}")

        def on_user_data_update(data):
            logger.info(f"👤 用户数据更新: {data.get('type', 'unknown')}")

        # 为第一个可用的交易所设置订阅
        adapter_ids = self.exchange_manager.get_registered_exchanges()
        if adapter_ids:
            exchange_id = adapter_ids[0]
            adapter = self.exchange_manager.get_adapter(exchange_id)

            if adapter:
                logger.info(f"为 {exchange_id} 设置订阅...")

                try:
                    # 订阅行情
                    await adapter.subscribe_ticker('BTC/USDT', on_ticker_update)
                    logger.info("✅ 订阅行情成功")

                    # 订阅订单簿
                    await adapter.subscribe_orderbook('BTC/USDT', on_orderbook_update)
                    logger.info("✅ 订阅订单簿成功")

                    # 订阅成交
                    await adapter.subscribe_trades('BTC/USDT', on_trade_update)
                    logger.info("✅ 订阅成交成功")

                    # 订阅用户数据
                    await adapter.subscribe_user_data(on_user_data_update)
                    logger.info("✅ 订阅用户数据成功")

                    # 运行5秒以观察订阅数据
                    logger.info("⏰ 运行5秒以观察订阅数据...")
                    await asyncio.sleep(5)

                    # 取消订阅
                    await adapter.unsubscribe()
                    logger.info("✅ 取消订阅成功")

                except Exception as e:
                    logger.error(f"WebSocket订阅演示失败: {e}")
        else:
            logger.warning("没有可用的交易所适配器")

    async def demo_exchange_manager_features(self):
        """演示交易所管理器功能"""
        logger.info("🎛️ 演示交易所管理器功能")

        # 获取所有适配器状态
        logger.info("📊 适配器状态:")
        for adapter_id in self.exchange_manager.get_registered_exchanges():
            adapter = self.exchange_manager.get_adapter(adapter_id)
            if adapter:
                status = adapter.get_status()
                logger.info(f"  {adapter_id}: {status}")

        # 健康检查
        logger.info("🔍 执行健康检查...")
        health_report = await self.exchange_manager.health_check_all()
        logger.info(f"健康检查结果: {health_report}")

        # 获取统计信息
        logger.info("📈 获取统计信息...")
        registered_count = len(
            self.exchange_manager.get_registered_exchanges())
        active_count = len(self.exchange_manager.get_active_exchanges())
        stats = {
            'registered_exchanges': registered_count,
            'active_exchanges': active_count,
            'running': self.exchange_manager.is_running()
        }
        logger.info(f"统计信息: {stats}")

    async def run_demo(self):
        """运行完整演示"""
        try:
            logger.info("🎯 开始MESA交易所适配层演示")

            # 初始化
            await self.initialize()

            # 基础信息演示
            await self.demo_basic_info()
            await asyncio.sleep(1)

            # 市场数据演示
            await self.demo_market_data()
            await asyncio.sleep(1)

            # 账户信息演示
            await self.demo_account_info()
            await asyncio.sleep(1)

            # 订单管理演示
            await self.demo_order_management()
            await asyncio.sleep(1)

            # WebSocket订阅演示
            await self.demo_websocket_subscriptions()
            await asyncio.sleep(1)

            # 交易所管理器功能演示
            await self.demo_exchange_manager_features()
            await asyncio.sleep(1)

            logger.info("✅ 演示完成!")

        except Exception as e:
            logger.error(f"演示过程中发生错误: {e}")
            raise
        finally:
            # 清理资源
            await self.cleanup()

    async def cleanup(self):
        """清理资源"""
        logger.info("🧹 清理资源")
        try:
            await self.exchange_manager.stop()
            logger.info("✅ 交易所管理器已停止")
        except Exception as e:
            logger.error(f"清理资源失败: {e}")


async def main():
    """主函数"""
    logger.info("🚀 启动MESA交易所适配层演示")

    demo = ExchangeAdapterDemo()

    try:
        await demo.run_demo()
    except KeyboardInterrupt:
        logger.info("🛑 用户中断演示")
    except Exception as e:
        logger.error(f"演示失败: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
