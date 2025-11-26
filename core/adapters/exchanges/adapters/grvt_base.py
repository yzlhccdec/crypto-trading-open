"""
GRVT交易所适配器 - 基础模块

提供GRVT交易所的基础配置、工具方法和数据解析功能
使用 grvt-pysdk
"""

import os
from typing import Dict, Any, Optional, List
from decimal import Decimal, InvalidOperation
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class GrvtBase:
    """GRVT交易所基础类"""

    # GRVT 环境配置
    ENVIRONMENTS = {
        'prod': 'prod',
        'testnet': 'testnet',
        'staging': 'staging',
        'dev': 'dev'
    }

    # 默认环境
    DEFAULT_ENV = 'testnet'

    # 订单状态映射
    ORDER_STATUS_MAP = {
        'pending': 'open',
        'open': 'open',
        'filled': 'filled',
        'partially_filled': 'open',
        'canceled': 'canceled',
        'cancelled': 'canceled',
        'expired': 'expired',
        'rejected': 'rejected',
    }

    # 订单方向映射
    ORDER_SIDE_MAP = {
        'buy': 'buy',
        'sell': 'sell',
        'BUY': 'buy',
        'SELL': 'sell',
    }

    # 订单类型映射
    ORDER_TYPE_MAP = {
        'limit': 'limit',
        'market': 'market',
        'LIMIT': 'limit',
        'MARKET': 'market',
    }

    def __init__(self, config: Dict[str, Any]):
        """
        初始化GRVT基础类

        Args:
            config: 配置字典，包含API密钥、环境等信息
        """
        self.config = config
        self.logger = None

        # 环境配置
        self.env = config.get('env', os.getenv('GRVT_ENV', self.DEFAULT_ENV))
        if self.env not in self.ENVIRONMENTS:
            self.env = self.DEFAULT_ENV
            logger.warning(f"⚠️ 无效的环境配置，使用默认环境: {self.DEFAULT_ENV}")

        # API 配置（从环境变量或配置中获取）
        self.private_key = config.get('api_key_private_key') or os.getenv('GRVT_PRIVATE_KEY', '')
        self.api_key = config.get('api_key') or os.getenv('GRVT_API_KEY', '')
        self.trading_account_id = config.get('trading_account_id') or os.getenv('GRVT_TRADING_ACCOUNT_ID', '')
        
        # API 版本配置
        self.endpoint_version = config.get('endpoint_version') or os.getenv('GRVT_END_POINT_VERSION', 'v1')
        self.ws_stream_version = config.get('ws_stream_version') or os.getenv('GRVT_WS_STREAM_VERSION', 'v1')

        # 市场信息缓存
        self._markets_cache: Dict[str, Dict[str, Any]] = {}
        self._symbol_to_market_id: Dict[str, str] = {}

        logger.info(f"✅ GRVT基础类初始化: 环境={self.env}")

    def set_logger(self, logger_instance):
        """设置日志器"""
        self.logger = logger_instance

    def _get_logger(self):
        """获取日志器"""
        return self.logger or logger

    def normalize_symbol(self, symbol: str) -> str:
        """
        标准化交易对符号

        Args:
            symbol: 交易对符号（如 "BTC/USDC:PERP" 或 "BTC_USDC_PERP"）

        Returns:
            标准化后的符号（GRVT格式）
        """
        # 移除后缀
        symbol = symbol.replace(':PERP', '').replace('_PERP', '').replace('-PERP', '')
        
        # 统一分隔符为 /
        symbol = symbol.replace('_', '/').replace('-', '/')
        
        return symbol.upper()

    def denormalize_symbol(self, grvt_symbol: str) -> str:
        """
        反标准化交易对符号（转换为通用格式）

        Args:
            grvt_symbol: GRVT格式的符号

        Returns:
            通用格式的符号（如 "BTC/USDC:PERP"）
        """
        # GRVT 通常使用 BTC/USDC 格式，转换为通用格式
        if '/' in grvt_symbol:
            return f"{grvt_symbol}:PERP"
        return grvt_symbol

    def parse_order_status(self, status: str) -> str:
        """
        解析订单状态

        Args:
            status: GRVT订单状态

        Returns:
            标准化的订单状态
        """
        status_lower = status.lower()
        return self.ORDER_STATUS_MAP.get(status_lower, status_lower)

    def parse_order_side(self, side: str) -> str:
        """
        解析订单方向

        Args:
            side: GRVT订单方向

        Returns:
            标准化的订单方向（buy/sell）
        """
        return self.ORDER_SIDE_MAP.get(side, side.lower())

    def parse_order_type(self, order_type: str) -> str:
        """
        解析订单类型

        Args:
            order_type: GRVT订单类型

        Returns:
            标准化的订单类型（limit/market）
        """
        return self.ORDER_TYPE_MAP.get(order_type, order_type.lower())

    def safe_decimal(self, value: Any, default: Decimal = Decimal('0')) -> Decimal:
        """
        安全转换为Decimal

        Args:
            value: 要转换的值
            default: 默认值

        Returns:
            Decimal值
        """
        try:
            if value is None:
                return default
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))
        except (ValueError, TypeError, InvalidOperation):
            return default

    def safe_float(self, value: Any, default: float = 0.0) -> float:
        """
        安全转换为float

        Args:
            value: 要转换的值
            default: 默认值

        Returns:
            float值
        """
        try:
            if value is None:
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    def safe_int(self, value: Any, default: int = 0) -> int:
        """
        安全转换为int

        Args:
            value: 要转换的值
            default: 默认值

        Returns:
            int值
        """
        try:
            if value is None:
                return default
            return int(value)
        except (ValueError, TypeError):
            return default

    def get_market_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取市场信息

        Args:
            symbol: 交易对符号

        Returns:
            市场信息字典，如果不存在则返回None
        """
        normalized_symbol = self.normalize_symbol(symbol)
        return self._markets_cache.get(normalized_symbol)

    def update_market_cache(self, markets: List[Dict[str, Any]]):
        """
        更新市场信息缓存

        Args:
            markets: 市场信息列表
        """
        for market in markets:
            symbol = market.get('symbol') or market.get('name')
            if symbol:
                normalized_symbol = self.normalize_symbol(symbol)
                self._markets_cache[normalized_symbol] = market
                
                # 建立反向映射
                if 'id' in market:
                    self._symbol_to_market_id[normalized_symbol] = str(market['id'])

    def get_market_id(self, symbol: str) -> Optional[str]:
        """
        获取市场ID

        Args:
            symbol: 交易对符号

        Returns:
            市场ID，如果不存在则返回None
        """
        normalized_symbol = self.normalize_symbol(symbol)
        return self._symbol_to_market_id.get(normalized_symbol)

