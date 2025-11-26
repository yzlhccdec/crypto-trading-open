"""
GRVT交易所适配器

基于MESA架构的GRVT适配器，提供统一的交易接口。
使用 grvt-pysdk 的 Raw API。
整合了分离的模块：grvt_base.py、grvt_rest.py、grvt_websocket.py
"""

import asyncio
import traceback
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal
from datetime import datetime
import yaml
import os

from ....logging import get_logger

from ..adapter import ExchangeAdapter
from ..interface import ExchangeConfig, ExchangeStatus
from ..models import (
    ExchangeType, OrderSide, OrderType, OrderStatus,
    PositionSide, MarginMode, OrderData, PositionData,
    BalanceData, TickerData, OHLCVData, OrderBookData,
    TradeData, ExchangeInfo, OrderBookLevel
)

from .grvt_base import GrvtBase
from .grvt_rest import GrvtRest
from .grvt_websocket import GrvtWebSocket
from ..subscription_manager import create_subscription_manager, DataType


class GrvtAdapter(ExchangeAdapter):
    """GRVT交易所适配器 - 统一接口"""

    def __init__(self, config: ExchangeConfig, event_bus=None):
        super().__init__(config, event_bus)

        # 转换配置为字典
        config_dict = self._convert_config_to_dict(config)

        # 初始化各个模块
        self._base = GrvtBase(config_dict)
        self._rest = GrvtRest(config_dict, self.logger)
        self._websocket = GrvtWebSocket(config_dict, self.logger)

        # 设置日志器
        self._base.set_logger(self.logger)

        # 连接状态
        self._connected = False
        self._authenticated = False

        # 缓存支持的交易对
        self._supported_symbols = []
        self._market_info = {}

        # 初始化订阅管理器
        try:
            config_dict = self._load_grvt_config()

            symbol_cache_service = self._get_symbol_cache_service()

            self._subscription_manager = create_subscription_manager(
                exchange_config=config_dict,
                symbol_cache_service=symbol_cache_service,
                logger=self.logger
            )

            if self.logger:
                self.logger.info(
                    f"✅ GRVT订阅管理器初始化成功，模式: {config_dict.get('subscription_mode', {}).get('mode', 'unknown')}")

        except Exception as e:
            if self.logger:
                self.logger.warning(f"创建GRVT订阅管理器失败，使用默认配置: {e}")
            # 使用默认配置
            default_config = {
                'exchange_id': 'grvt',
                'subscription_mode': {
                    'mode': 'predefined',
                    'predefined': {
                        'symbols': ['BTC/USDC:PERP', 'ETH/USDC:PERP', 'SOL/USDC:PERP'],
                        'data_types': {'ticker': True, 'orderbook': True, 'trades': False, 'user_data': False}
                    }
                }
            }

            symbol_cache_service = self._get_symbol_cache_service()
            self._subscription_manager = create_subscription_manager(
                exchange_config=default_config,
                symbol_cache_service=symbol_cache_service,
                logger=self.logger
            )

        self.logger.info("✅ GRVT适配器初始化完成")

    def _convert_config_to_dict(self, config: ExchangeConfig) -> Dict[str, Any]:
        """
        将ExchangeConfig转换为字典

        Args:
            config: ExchangeConfig对象

        Returns:
            配置字典
        """
        config_dict = {
            "env": getattr(config, 'env', 'testnet'),
            "api_key_private_key": getattr(config, 'api_key_private_key', ''),
            "api_key": getattr(config, 'api_key', ''),
            "trading_account_id": getattr(config, 'trading_account_id', ''),
            "endpoint_version": getattr(config, 'endpoint_version', 'v1'),
            "ws_stream_version": getattr(config, 'ws_stream_version', 'v1'),
        }

        # 如果配置为空，从环境变量或配置文件加载
        if not config_dict.get('api_key_private_key'):
            try:
                grvt_config = self._load_grvt_config()
                api_config = grvt_config.get('api_config', {})
                auth_config = api_config.get('auth', {})

                config_dict['api_key_private_key'] = auth_config.get(
                    'api_key_private_key', '') or os.getenv('GRVT_PRIVATE_KEY', '')
                config_dict['api_key'] = auth_config.get(
                    'api_key', '') or os.getenv('GRVT_API_KEY', '')
                config_dict['trading_account_id'] = auth_config.get(
                    'trading_account_id', '') or os.getenv('GRVT_TRADING_ACCOUNT_ID', '')
                config_dict['env'] = api_config.get(
                    'env', 'testnet') or os.getenv('GRVT_ENV', 'testnet')
                config_dict['endpoint_version'] = api_config.get(
                    'endpoint_version', 'v1') or os.getenv('GRVT_END_POINT_VERSION', 'v1')
                config_dict['ws_stream_version'] = api_config.get(
                    'ws_stream_version', 'v1') or os.getenv('GRVT_WS_STREAM_VERSION', 'v1')

                if self.logger:
                    self.logger.info("✅ 从grvt_config.yaml或环境变量加载API配置")
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"⚠️ 无法从配置文件加载GRVT配置: {e}")

        # 添加可选配置
        if hasattr(config, 'base_url'):
            config_dict['base_url'] = config.base_url
        if hasattr(config, 'ws_url'):
            config_dict['ws_url'] = config.ws_url

        return config_dict

    def _load_grvt_config(self) -> Dict[str, Any]:
        """加载GRVT配置文件"""
        config_path = "config/exchanges/grvt_config.yaml"

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                if self.logger:
                    self.logger.info(f"✅ 加载GRVT配置文件: {config_path}")
                return config
        except FileNotFoundError:
            if self.logger:
                self.logger.warning(f"GRVT配置文件未找到: {config_path}")
            return {'exchange_id': 'grvt'}
        except Exception as e:
            if self.logger:
                self.logger.error(f"加载GRVT配置文件失败: {e}")
            return {'exchange_id': 'grvt'}

    def _get_symbol_cache_service(self):
        """获取符号缓存服务实例"""
        try:
            # 尝试从依赖注入容器获取符号缓存服务
            from ....di.container import get_container
            from ....services.symbol_manager.interfaces.symbol_cache import ISymbolCacheService

            container = get_container()
            symbol_cache_service = container.get(ISymbolCacheService)

            if self.logger:
                self.logger.info("✅ 获取符号缓存服务成功")

            return symbol_cache_service
        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ 无法获取符号缓存服务: {e}")
            return None

    # ==================== 生命周期管理 ====================

    async def _do_connect(self) -> bool:
        """执行连接"""
        try:
            # 初始化REST API
            if not await self._rest.initialize():
                return False

            # 连接WebSocket（如果启用）
            if getattr(self.config, 'enable_websocket', True):
                if not await self._websocket.connect():
                    self.logger.warning("⚠️ WebSocket连接失败，但REST API可用")

            self._connected = True
            return True

        except Exception as e:
            self.logger.error(f"❌ GRVT连接失败: {str(e)}")
            return False

    async def _do_disconnect(self) -> None:
        """执行断开连接"""
        try:
            await self._rest.close()
            await self._websocket.disconnect()
            self._connected = False
        except Exception as e:
            self.logger.error(f"❌ GRVT断开连接失败: {str(e)}")

    async def _do_authenticate(self) -> bool:
        """
        执行认证
        
        根据 GRVT API 文档 (https://api-docs.grvt.io/#authentication):
        - 使用 API 密钥进行认证
        - 获取会话 cookie 和 X-Grvt-Account-Id 头部值
        
        通过调用 get_balances() 来验证认证是否成功，如果出错则认为是认证失败。
        """
        try:
            # 检查 REST 客户端是否已初始化
            if not self._rest._raw_client:
                self.logger.warning("⚠️ GRVT REST 客户端未初始化，无法进行认证")
                return False
            
            # 检查是否为公共访问模式（没有 API 密钥）
            config_dict = self._convert_config_to_dict(self.config)
            has_api_key = bool(config_dict.get('api_key') or config_dict.get('api_key_private_key'))
            
            if not has_api_key:
                # 公共访问模式下不需要认证
                self.logger.info("ℹ️ GRVT公共访问模式，跳过认证")
                self._authenticated = True
                return True
            
            # 私有模式下验证认证：通过调用 get_balances() 来验证
            # 如果出错就认为是认证失败
            try:
                await self._rest.get_balances()
                self._authenticated = True
                self.logger.info("✅ GRVT认证成功（通过 get_balances 验证）")
                return True
            except Exception as e:
                # 任何错误都认为是认证失败
                self.logger.error(f"❌ GRVT认证失败: {str(e)}")
                self.logger.debug(traceback.format_exc())
                return False
                
        except Exception as e:
            self.logger.error(f"❌ GRVT认证失败: {str(e)}")
            self.logger.debug(traceback.format_exc())
            return False

    async def _do_health_check(self) -> Dict[str, Any]:
        """执行健康检查"""
        health_data = {
            'exchange_time': None,
            'rest_connected': False,
            'websocket_connected': False,
            'market_count': 0,
            'subscriptions': 0
        }

        try:
            # 检查REST API健康状态
            exchange_info = await self._rest.get_exchange_info()
            health_data['exchange_time'] = exchange_info.timestamp
            health_data['rest_connected'] = True
            health_data['market_count'] = len(exchange_info.markets) if exchange_info.markets else 0

            # 检查WebSocket连接状态
            health_data['websocket_connected'] = self._websocket._ws_connected
            
            # 获取订阅数量
            if hasattr(self._subscription_manager, 'get_subscription_count'):
                health_data['subscriptions'] = self._subscription_manager.get_subscription_count()
            elif hasattr(self._websocket, '_active_subscriptions'):
                health_data['subscriptions'] = len(self._websocket._active_subscriptions)

            # 注意：不设置status字段，让基类来处理
            return health_data

        except Exception as e:
            health_data['error'] = str(e)
            if self.logger:
                self.logger.error(f"GRVT健康检查失败: {str(e)}")
            return health_data

    # ==================== 市场数据接口 ====================

    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        return await self._rest.get_exchange_info()

    async def get_ticker(self, symbol: str) -> TickerData:
        """获取单个行情数据"""
        return await self._rest.get_ticker(symbol)

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个行情数据"""
        return await self._rest.get_tickers(symbols)

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿"""
        return await self._rest.get_orderbook(symbol, limit)

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OHLCVData]:
        """获取K线数据"""
        return await self._rest.get_ohlcv(symbol, timeframe, since, limit)

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """获取最近成交记录"""
        return await self._rest.get_trades(symbol, since, limit)

    # ==================== 账户和交易接口 ====================

    async def get_balances(self) -> List[BalanceData]:
        """获取账户余额"""
        return await self._rest.get_balances()

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息"""
        return await self._rest.get_positions(symbols)

    async def create_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        amount: Decimal,
        price: Optional[Decimal] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> OrderData:
        """创建订单"""
        return await self._rest.create_order(symbol, side, order_type, amount, price, params)

    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """取消订单"""
        return await self._rest.cancel_order(order_id, symbol)

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """取消所有订单"""
        return await self._rest.cancel_all_orders(symbol)

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """获取订单信息"""
        return await self._rest.get_order(order_id, symbol)

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        return await self._rest.get_open_orders(symbol)

    # ==================== WebSocket订阅接口 ====================

    async def subscribe_ticker(
        self,
        symbol: str,
        callback: Callable[[TickerData], None]
    ) -> None:
        """订阅行情数据流"""
        await self._websocket.subscribe_ticker(symbol, callback)

    async def subscribe_orderbook(
        self,
        symbol: str,
        callback: Callable[[OrderBookData], None]
    ) -> None:
        """订阅订单簿数据流"""
        await self._websocket.subscribe_orderbook(symbol, callback)

    async def subscribe_trades(
        self,
        symbol: str,
        callback: Callable[[TradeData], None]
    ) -> None:
        """订阅成交数据流"""
        await self._websocket.subscribe_trades(symbol, callback)

    async def subscribe_user_data(
        self,
        callback: Callable[[Dict[str, Any]], None]
    ) -> None:
        """订阅用户数据流"""
        await self._websocket.subscribe_user_data(callback)

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        await self._websocket.unsubscribe(symbol)
        self._subscription_manager.remove_subscription(symbol)

    # ==================== 订单历史接口 ====================

    async def get_order_history(
        self,
        symbol: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OrderData]:
        """获取历史订单"""
        try:
            # GRVT REST API 可能没有专门的订单历史接口
            # 尝试从开放订单和已成交订单中获取
            if hasattr(self._rest, 'get_order_history'):
                return await self._rest.get_order_history(symbol, since, limit)
            else:
                # 如果没有专门的接口，返回空列表或尝试其他方法
                self.logger.warning("GRVT REST API 不支持 get_order_history，返回空列表")
                return []
        except Exception as e:
            self.logger.error(f"获取订单历史失败: {e}")
            return []

    # ==================== 交易设置接口 ====================

    async def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        """设置杠杆倍数"""
        try:
            if hasattr(self._rest, 'set_leverage'):
                return await self._rest.set_leverage(symbol, leverage)
            else:
                self.logger.warning(f"GRVT REST API 不支持 set_leverage")
                return {
                    'symbol': symbol,
                    'leverage': leverage,
                    'success': False,
                    'message': 'GRVT API does not support set_leverage'
                }
        except Exception as e:
            self.logger.error(f"设置杠杆失败: {e}")
            return {
                'symbol': symbol,
                'leverage': leverage,
                'success': False,
                'error': str(e)
            }

    async def set_margin_mode(self, symbol: str, margin_mode: str) -> Dict[str, Any]:
        """设置保证金模式"""
        try:
            if hasattr(self._rest, 'set_margin_mode'):
                return await self._rest.set_margin_mode(symbol, margin_mode)
            else:
                self.logger.warning(f"GRVT REST API 不支持 set_margin_mode")
                return {
                    'symbol': symbol,
                    'margin_mode': margin_mode,
                    'success': False,
                    'message': 'GRVT API does not support set_margin_mode'
                }
        except Exception as e:
            self.logger.error(f"设置保证金模式失败: {e}")
            return {
                'symbol': symbol,
                'margin_mode': margin_mode,
                'success': False,
                'error': str(e)
            }

    # ==================== 工具方法 ====================

    def normalize_symbol(self, symbol: str) -> str:
        """标准化交易对符号"""
        return self._base.normalize_symbol(symbol)

    def denormalize_symbol(self, symbol: str) -> str:
        """反标准化交易对符号"""
        return self._base.denormalize_symbol(symbol)

