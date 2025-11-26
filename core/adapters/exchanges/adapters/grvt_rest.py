"""
GRVT交易所REST API模块

使用 grvt-pysdk 的 Raw API 实现REST API
"""

import asyncio
import os
import random
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime

# GRVT SDK 导入
from pysdk.grvt_raw_async import GrvtRawAsync
from pysdk.grvt_raw_sync import GrvtRawSync
from pysdk.grvt_raw_base import GrvtApiConfig, GrvtError
from pysdk.grvt_raw_env import GrvtEnv
from pysdk import grvt_raw_types as grvt_types
    

from .grvt_base import GrvtBase
from ..models import (
    TickerData, OrderBookData, TradeData, BalanceData, OrderData,
    PositionData, OHLCVData, ExchangeInfo, ExchangeType,
    OrderSide, OrderType, OrderStatus, PositionSide, MarginMode,
    OrderBookLevel
)


class GrvtRest(GrvtBase):
    """GRVT REST API接口实现 - 使用 Raw API"""

    def __init__(self, config, logger=None):
        super().__init__(config)
        self.logger = logger
        
        # GRVT SDK 实例（延迟初始化）
        self._raw_client = None
        
        # 重试配置
        self.max_retries = 3
        self.retry_delay = 1.0

    async def initialize(self) -> bool:
        """初始化GRVT Raw API客户端"""
        try:
            # 检查 GRVT SDK 是否可用
            if GrvtRawAsync is None:
                if GrvtRawSync is None:
                    if self.logger:
                        self.logger.error("❌ GRVT SDK 未安装或无法导入")
                    return False
            
            # 将环境字符串转换为 GrvtEnv 枚举
            env_map = {
                'prod': GrvtEnv.PROD,
                'testnet': GrvtEnv.TESTNET,
                'staging': GrvtEnv.STAGING,
                'dev': GrvtEnv.DEV,
            }
            grvt_env = env_map.get(self.env.lower(), GrvtEnv.TESTNET)
            
            # 创建 GrvtApiConfig
            api_config = GrvtApiConfig(
                env=grvt_env,
                trading_account_id=self.trading_account_id if self.trading_account_id else None,
                private_key=self.private_key if self.private_key else None,
                api_key=self.api_key if self.api_key else None,
                logger=self.logger
            )
            
            # 创建GRVT Raw API客户端
            self._raw_client = GrvtRawAsync(api_config)
            self._use_sync = False
            
            # 测试连接（获取市场信息）
            try:
                # 尝试获取市场列表来验证连接
                resp = await self._raw_client.get_all_instruments_v1(
                    grvt_types.ApiGetAllInstrumentsRequest(is_active=True)
                )
                
                # 检查是否是错误响应
                if isinstance(resp, GrvtError):
                    if self.logger:
                        self.logger.warning(f"⚠️ 获取市场列表返回错误: {resp.message}")
                elif resp.result is not None:
                    instruments = resp.result
                    if instruments:
                        # 转换为市场信息格式并更新缓存
                        markets = []
                        for inst in instruments:
                            # Instrument 对象包含: instrument, instrument_hash, base, quote, kind 等
                            # instrument 格式: "ETH_USDT_Perp" -> 转换为 "ETH/USDT:PERP"
                            normalized_symbol = self.denormalize_symbol(inst.instrument) if inst.instrument else ''
                            markets.append({
                                'symbol': normalized_symbol,
                                'name': inst.instrument if inst.instrument else '',
                                'marketId': inst.instrument_hash if inst.instrument_hash else None,
                                'id': inst.instrument_hash if inst.instrument_hash else None,
                                'base': inst.base if inst.base else '',
                                'quote': inst.quote if inst.quote else '',
                                'kind': inst.kind.value if inst.kind else None,
                            })
                        self.update_market_cache(markets)
                        if self.logger:
                            self.logger.info(f"✅ GRVT REST初始化成功，加载 {len(markets)} 个市场")
                    else:
                        if self.logger:
                            self.logger.warning("⚠️ GRVT市场列表为空")
                else:
                    if self.logger:
                        self.logger.warning("⚠️ 无法解析市场列表响应")
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"⚠️ 获取市场列表失败，但连接已建立: {str(e)}")
                    import traceback
                    self.logger.debug(traceback.format_exc())
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ GRVT REST初始化失败: {str(e)}")
                import traceback
                self.logger.error(traceback.format_exc())
            return False

    async def close(self):
        """关闭连接"""
        if self._raw_client:
            # SDK 可能没有 close 方法，直接设置为 None
            self._raw_client = None

    async def _execute_with_retry(self, func, *args, **kwargs):
        """带重试的API调用"""
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                # 如果是协程函数，直接await
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    # 如果是同步函数，在线程池中运行
                    result = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: func(*args, **kwargs)
                    )
                return result
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    if self.logger:
                        self.logger.warning(f"API调用失败 (尝试 {attempt + 1}/{self.max_retries}): {str(e)}")
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    if self.logger:
                        self.logger.error(f"API调用最终失败: {str(e)}")
        
        raise last_error

    # ==================== 市场数据接口 ====================

    async def _get_markets(self) -> List[Dict[str, Any]]:
        """获取市场列表的辅助方法"""
        try:
            resp = await self._execute_with_retry(
                self._raw_client.get_all_instruments_v1,
                grvt_types.ApiGetAllInstrumentsRequest(is_active=True)
            )
            
            # 检查是否是错误响应
            if isinstance(resp, GrvtError):
                if self.logger:
                    self.logger.warning(f"⚠️ 获取市场列表返回错误: {resp.message}")
                return []
            
            # 解析响应
            if resp.result is None:
                return []
            
            instruments = resp.result
            markets = []
            for inst in instruments:
                # Instrument 对象包含: instrument, instrument_hash, base, quote, kind 等
                # instrument 格式: "ETH_USDT_Perp" -> 转换为 "ETH/USDT:PERP"
                normalized_symbol = self.denormalize_symbol(inst.instrument) if inst.instrument else ''
                markets.append({
                    'symbol': normalized_symbol,
                    'name': inst.instrument if inst.instrument else '',
                    'marketId': inst.instrument_hash if inst.instrument_hash else None,
                    'id': inst.instrument_hash if inst.instrument_hash else None,
                    'base': inst.base if inst.base else '',
                    'quote': inst.quote if inst.quote else '',
                    'kind': inst.kind.value if inst.kind else None,
                })
            return markets
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取市场列表失败: {e}")
            return []

    async def get_exchange_info(self) -> ExchangeInfo:
        """获取交易所信息"""
        try:
            # 获取市场列表
            markets = await self._get_markets()
            markets_dict = {}
            if markets:
                for market in markets:
                    symbol = market.get('symbol') or market.get('name')
                    if symbol:
                        markets_dict[symbol] = market
            
            return ExchangeInfo(
                name="GRVT",
                id="grvt",
                type=ExchangeType.PERPETUAL,
                supported_features=[
                    "perpetual_trading", "websocket",
                    "orderbook", "ticker", "ohlcv", "user_stream"
                ],
                rate_limits={},
                precision={},
                fees={},
                markets=markets_dict,
                status="operational",
                timestamp=datetime.now()
            )
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取交易所信息失败: {str(e)}")
            raise

    async def get_ticker(self, symbol: str) -> TickerData:
        """获取单个行情数据"""
        try:
            normalized_symbol = self.normalize_symbol(symbol)
            
            # 使用 ticker_v1 API
            resp = await self._execute_with_retry(
                self._raw_client.ticker_v1,
                grvt_types.ApiTickerRequest(instrument=normalized_symbol)
            )
            
            # 检查是否是错误响应
            if isinstance(resp, GrvtError):
                raise ValueError(f"获取ticker失败: {resp.message}")
            
            if resp.result is None:
                raise ValueError(f"未找到交易对: {symbol}")
            
            # 提取 ticker 数据
            ticker_obj = resp.result
            ticker_data = {
                'symbol': normalized_symbol,
                'last': float(ticker_obj.last_price) if ticker_obj.last_price else None,
                'bid': float(ticker_obj.best_bid_price) if ticker_obj.best_bid_price else None,
                'ask': float(ticker_obj.best_ask_price) if ticker_obj.best_ask_price else None,
                'high': float(ticker_obj.high_24h) if ticker_obj.high_24h else None,
                'low': float(ticker_obj.low_24h) if ticker_obj.low_24h else None,
                'volume': float(ticker_obj.volume_24h_b) if ticker_obj.volume_24h_b else None,
                'timestamp': int(int(ticker_obj.event_time) / 1_000_000) if ticker_obj.event_time else None,
            }
            
            return self._parse_ticker(ticker_data, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取行情失败 {symbol}: {str(e)}")
            raise

    async def get_tickers(self, symbols: Optional[List[str]] = None) -> List[TickerData]:
        """获取多个行情数据"""
        try:
            if symbols:
                normalized_symbols = [self.normalize_symbol(s) for s in symbols]
                result = []
                for sym in normalized_symbols:
                    try:
                        ticker = await self.get_ticker(sym)
                        result.append(ticker)
                    except Exception as e:
                        if self.logger:
                            self.logger.warning(f"获取 {sym} 行情失败: {str(e)}")
            else:
                # 获取所有市场行情
                markets = await self._get_markets()
                result = []
                for market in markets:
                    symbol = market.get('symbol') or market.get('name')
                    if symbol:
                        try:
                            original_symbol = self.denormalize_symbol(symbol)
                            ticker = await self.get_ticker(original_symbol)
                            result.append(ticker)
                        except Exception as e:
                            if self.logger:
                                self.logger.warning(f"获取 {symbol} 行情失败: {str(e)}")
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取行情列表失败: {str(e)}")
            raise

    async def get_orderbook(self, symbol: str, limit: Optional[int] = None) -> OrderBookData:
        """获取订单簿"""
        try:
            normalized_symbol = self.normalize_symbol(symbol)
            depth = limit if limit else 10  # 默认深度为10
            
            # 使用 orderbook_levels_v1 API
            resp = await self._execute_with_retry(
                self._raw_client.orderbook_levels_v1,
                grvt_types.ApiOrderbookLevelsRequest(
                    instrument=normalized_symbol,
                    depth=depth
                )
            )
            
            # 检查是否是错误响应
            if isinstance(resp, GrvtError):
                raise ValueError(f"获取订单簿失败: {resp.message}")
            
            if resp.result is None:
                raise ValueError(f"未找到交易对订单簿: {symbol}")
            
            # 提取订单簿数据
            orderbook_obj = resp.result
            orderbook_data = {
                'symbol': normalized_symbol,
                'bids': [
                    [float(level.price), float(level.size)]
                    for level in orderbook_obj.bids
                ],
                'asks': [
                    [float(level.price), float(level.size)]
                    for level in orderbook_obj.asks
                ],
                'timestamp': int(int(orderbook_obj.event_time) / 1_000_000) if orderbook_obj.event_time else None,
            }
            
            return self._parse_orderbook(orderbook_data, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取订单簿失败 {symbol}: {str(e)}")
            raise

    async def get_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[OHLCVData]:
        """获取K线数据"""
        try:
            normalized_symbol = self.normalize_symbol(symbol)
            since_timestamp = int(since.timestamp() * 1_000_000) if since else None  # GRVT使用微秒
            
            # 调用Raw API获取OHLCV
            resp = await self._execute_with_retry(
                self._raw_client.candlestick_v1,
                grvt_types.ApiCandlestickRequest(
                    instrument=normalized_symbol,
                    interval=timeframe,
                    start_time=since_timestamp,
                    limit=limit
                )
            )
            
            # 检查是否是错误响应
            if isinstance(resp, GrvtError):
                raise ValueError(f"获取K线数据失败: {resp.message}")
            
            if resp.result is None:
                return []
            
            result = []
            for candlestick in resp.result:
                # 解析K线数据
                ohlcv_data = [
                    int(candlestick.event_time) if candlestick.event_time else 0,
                    float(candlestick.open) if candlestick.open else 0.0,
                    float(candlestick.high) if candlestick.high else 0.0,
                    float(candlestick.low) if candlestick.low else 0.0,
                    float(candlestick.close) if candlestick.close else 0.0,
                    float(candlestick.volume) if candlestick.volume else 0.0,
                ]
                result.append(self._parse_ohlcv(ohlcv_data, symbol))
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取K线数据失败 {symbol}: {str(e)}")
            raise

    async def get_trades(
        self,
        symbol: str,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[TradeData]:
        """获取最近成交记录"""
        try:
            normalized_symbol = self.normalize_symbol(symbol)
            since_timestamp = int(since.timestamp() * 1_000_000) if since else None  # GRVT使用微秒
            
            # 调用Raw API获取trades
            resp = await self._execute_with_retry(
                self._raw_client.trade_history_v1,
                grvt_types.ApiTradeHistoryRequest(
                    instrument=normalized_symbol,
                    start_time=since_timestamp,
                    limit=limit
                )
            )
            
            # 检查是否是错误响应
            if isinstance(resp, GrvtError):
                raise ValueError(f"获取成交记录失败: {resp.message}")
            
            if resp.result is None:
                return []
            
            result = []
            for trade in resp.result:
                trade_data = {
                    'id': str(trade.trade_id) if trade.trade_id else '',
                    'price': float(trade.price) if trade.price else 0.0,
                    'amount': float(trade.size) if trade.size else 0.0,
                    'side': 'buy' if trade.side and trade.side.value == 'buy' else 'sell',
                    'timestamp': int(trade.event_time) if trade.event_time else 0,
                }
                result.append(self._parse_trade(trade_data, symbol))
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取成交记录失败 {symbol}: {str(e)}")
            raise

    # ==================== 账户和交易接口 ====================

    async def get_balances(self) -> List[BalanceData]:
        """获取账户余额"""
        try:
            # 调用Raw API获取余额
            if not self.trading_account_id:
                raise ValueError("trading_account_id 未配置，无法获取余额")
            resp = await self._execute_with_retry(
                self._raw_client.sub_account_summary_v1,
                grvt_types.ApiSubAccountSummaryRequest(
                    sub_account_id=self.trading_account_id
                )
            )
            
            # 检查是否是错误响应
            if isinstance(resp, GrvtError):
                raise ValueError(f"获取余额失败: {resp.message}")
            
            if resp.result is None:
                return []
            
            # 解析余额数据
            balance_data = {}
            for currency_balance in resp.result:
                currency = currency_balance.currency if currency_balance.currency else ''
                if currency:
                    balance_data[currency] = {
                        'free': float(currency_balance.available) if currency_balance.available else 0.0,
                        'used': float(currency_balance.locked) if currency_balance.locked else 0.0,
                        'total': float(currency_balance.total) if currency_balance.total else 0.0,
                    }
            
            return self._parse_balances(balance_data)
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取余额失败: {str(e)}")
            raise

    async def get_positions(self, symbols: Optional[List[str]] = None) -> List[PositionData]:
        """获取持仓信息"""
        try:
            # 调用Raw API获取持仓
            if not self.trading_account_id:
                raise ValueError("trading_account_id 未配置，无法获取持仓")
            resp = await self._execute_with_retry(
                self._raw_client.positions_v1,
                grvt_types.ApiPositionsRequest(
                    sub_account_id=self.trading_account_id
                )
            )
            
            # 检查是否是错误响应
            if isinstance(resp, GrvtError):
                raise ValueError(f"获取持仓失败: {resp.message}")
            
            if resp.result is None:
                return []
            
            # 解析持仓数据
            positions_data = []
            for position in resp.result:
                pos_data = {
                    'symbol': position.instrument if position.instrument else '',
                    'side': 'long' if position.side and position.side.value == 'long' else 'short',
                    'size': float(position.size) if position.size else 0.0,
                    'entryPrice': float(position.entry_price) if position.entry_price else 0.0,
                    'markPrice': float(position.mark_price) if position.mark_price else 0.0,
                    'unrealizedPnl': float(position.unrealized_pnl) if position.unrealized_pnl else 0.0,
                    'realizedPnl': float(position.realized_pnl) if position.realized_pnl else 0.0,
                    'leverage': int(position.leverage) if position.leverage else 1,
                    'margin': float(position.margin) if position.margin else 0.0,
                    'liquidationPrice': float(position.liquidation_price) if position.liquidation_price else None,
                    'timestamp': int(position.event_time) if position.event_time else 0,
                }
                positions_data.append(pos_data)
            
            # 如果指定了symbols，进行过滤
            if symbols:
                normalized_symbols = [self.normalize_symbol(s) for s in symbols]
                positions_data = [
                    pos for pos in positions_data
                    if self.normalize_symbol(pos.get('symbol', '')) in normalized_symbols
                ]
            
            return self._parse_positions(positions_data)
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取持仓失败: {str(e)}")
            raise

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
        try:
            if not self.trading_account_id:
                raise ValueError("trading_account_id 未配置，无法创建订单")
            
            normalized_symbol = self.normalize_symbol(symbol)
            
            # 构建订单腿
            order_leg = grvt_types.OrderLeg(
                instrument=normalized_symbol,
                size=str(amount),
                is_buying_asset=(side == OrderSide.BUY),
                limit_price=str(int(price * 1_000_000_000)) if price is not None and order_type == OrderType.LIMIT else None
            )
            
            # 构建订单元数据
            client_order_id = str(random.randint(2**63, 2**64 - 1))
            order_metadata = grvt_types.OrderMetadata(
                client_order_id=client_order_id
            )
            
            # 构建订单对象
            order = grvt_types.Order(
                sub_account_id=self.trading_account_id,
                time_in_force=grvt_types.TimeInForce.GOOD_TILL_TIME if order_type == OrderType.LIMIT else grvt_types.TimeInForce.IMMEDIATE_OR_CANCEL,
                legs=[order_leg],
                signature=None,  # SDK 会自动处理签名
                metadata=order_metadata,
                is_market=(order_type == OrderType.MARKET),
                post_only=params.get('post_only', False) if params else False,
                reduce_only=params.get('reduce_only', False) if params else False,
            )
            
            # 构建订单请求
            order_req = grvt_types.ApiCreateOrderRequest(order=order)
            
            # 调用Raw API创建订单
            resp = await self._execute_with_retry(
                self._raw_client.create_order_v1,
                order_req
            )
            
            # 检查是否是错误响应
            if isinstance(resp, GrvtError):
                raise ValueError(f"创建订单失败: {resp.message}")
            
            if resp.result is None:
                raise ValueError("创建订单失败: 响应结果为空")
            
            # 解析订单数据
            order_obj = resp.result
            order_data = {
                'id': str(order_obj.order_id) if order_obj.order_id else '',
                'symbol': normalized_symbol,
                'side': 'buy' if side == OrderSide.BUY else 'sell',
                'type': 'market' if order_type == OrderType.MARKET else 'limit',
                'amount': float(amount),
                'price': float(price) if price is not None else None,
                'status': 'open',
                'timestamp': int(datetime.now().timestamp() * 1000),
            }
            
            return self._parse_order(order_data, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"创建订单失败 {symbol}: {str(e)}")
            raise

    async def cancel_order(self, order_id: str, symbol: str) -> OrderData:
        """取消订单"""
        try:
            normalized_symbol = self.normalize_symbol(symbol)
            
            # 调用Raw API取消订单
            if not self.trading_account_id:
                raise ValueError("trading_account_id 未配置，无法取消订单")
            resp = await self._execute_with_retry(
                self._raw_client.cancel_order_v1,
                grvt_types.ApiCancelOrderRequest(
                    sub_account_id=self.trading_account_id,
                    order_id=order_id
                )
            )
            
            # 检查是否是错误响应
            if isinstance(resp, GrvtError):
                raise ValueError(f"取消订单失败: {resp.message}")
            
            # 取消订单后，获取订单信息确认
            order = await self.get_order(order_id, symbol)
            return order
        except Exception as e:
            if self.logger:
                self.logger.error(f"取消订单失败 {order_id}: {str(e)}")
            raise

    async def cancel_all_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """取消所有订单"""
        try:
            # 调用Raw API取消所有订单
            if not self.trading_account_id:
                raise ValueError("trading_account_id 未配置，无法取消所有订单")
            resp = await self._execute_with_retry(
                self._raw_client.cancel_all_orders_v1,
                grvt_types.ApiCancelAllOrdersRequest(
                    sub_account_id=self.trading_account_id
                )
            )
            
            # 检查是否是错误响应
            if isinstance(resp, GrvtError):
                raise ValueError(f"取消所有订单失败: {resp.message}")
            
            # 取消后获取开放订单列表确认
            open_orders = await self.get_open_orders(symbol)
            return open_orders
        except Exception as e:
            if self.logger:
                self.logger.error(f"取消所有订单失败: {str(e)}")
            raise

    async def get_order(self, order_id: str, symbol: str) -> OrderData:
        """获取订单信息"""
        try:
            normalized_symbol = self.normalize_symbol(symbol)
            
            # 调用Raw API获取订单
            if not self.trading_account_id:
                raise ValueError("trading_account_id 未配置，无法获取订单")
            resp = await self._execute_with_retry(
                self._raw_client.get_order_v1,
                grvt_types.ApiGetOrderRequest(
                    sub_account_id=self.trading_account_id,
                    order_id=order_id
                )
            )
            
            # 检查是否是错误响应
            if isinstance(resp, GrvtError):
                raise ValueError(f"获取订单失败: {resp.message}")
            
            if resp.result is None:
                raise ValueError(f"未找到订单: {order_id}")
            
            # 解析订单数据
            order_obj = resp.result
            order_data = {
                'id': str(order_obj.order_id) if order_obj.order_id else order_id,
                'symbol': normalized_symbol,
                'side': 'buy' if order_obj.side and order_obj.side.value == 'buy' else 'sell',
                'type': 'market' if order_obj.order_type and order_obj.order_type.value == 'market' else 'limit',
                'amount': float(order_obj.size) if order_obj.size else 0.0,
                'price': float(order_obj.price) if order_obj.price else None,
                'filled': float(order_obj.filled_size) if order_obj.filled_size else 0.0,
                'remaining': float(order_obj.size) - float(order_obj.filled_size) if order_obj.size and order_obj.filled_size else 0.0,
                'status': 'filled' if order_obj.status and 'filled' in order_obj.status.value.lower() else 'open',
                'timestamp': int(order_obj.event_time) if order_obj.event_time else int(datetime.now().timestamp() * 1000),
            }
            
            return self._parse_order(order_data, symbol)
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取订单失败 {order_id}: {str(e)}")
            raise

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[OrderData]:
        """获取开放订单"""
        try:
            normalized_symbol = self.normalize_symbol(symbol) if symbol else None
            
            # 调用Raw API获取开放订单
            if not self.trading_account_id:
                raise ValueError("trading_account_id 未配置，无法获取开放订单")
            resp = await self._execute_with_retry(
                self._raw_client.open_orders_v1,
                grvt_types.ApiOpenOrdersRequest(
                    sub_account_id=self.trading_account_id
                )
            )
            
            # 检查是否是错误响应
            if isinstance(resp, GrvtError):
                raise ValueError(f"获取开放订单失败: {resp.message}")
            
            if resp.result is None:
                return []
            
            result = []
            for order_obj in resp.result:
                order_data = {
                    'id': str(order_obj.order_id) if order_obj.order_id else '',
                    'symbol': order_obj.instrument if order_obj.instrument else (symbol or ''),
                    'side': 'buy' if order_obj.side and order_obj.side.value == 'buy' else 'sell',
                    'type': 'market' if order_obj.order_type and order_obj.order_type.value == 'market' else 'limit',
                    'amount': float(order_obj.size) if order_obj.size else 0.0,
                    'price': float(order_obj.price) if order_obj.price else None,
                    'filled': float(order_obj.filled_size) if order_obj.filled_size else 0.0,
                    'remaining': float(order_obj.size) - float(order_obj.filled_size) if order_obj.size and order_obj.filled_size else 0.0,
                    'status': 'open',
                    'timestamp': int(order_obj.event_time) if order_obj.event_time else int(datetime.now().timestamp() * 1000),
                }
                original_symbol = self.denormalize_symbol(order_data['symbol'])
                result.append(self._parse_order(order_data, original_symbol))
            
            return result
        except Exception as e:
            if self.logger:
                self.logger.error(f"获取开放订单失败: {str(e)}")
            raise

    # ==================== 数据解析方法 ====================

    def _parse_ticker(self, ticker_data: Dict, symbol: str) -> TickerData:
        """解析行情数据"""
        return TickerData(
            symbol=symbol,
            exchange="grvt",
            last=self.safe_decimal(ticker_data.get('last') or ticker_data.get('l') or ticker_data.get('close')),
            bid=self.safe_decimal(ticker_data.get('bid') or ticker_data.get('b')),
            ask=self.safe_decimal(ticker_data.get('ask') or ticker_data.get('a')),
            high=self.safe_decimal(ticker_data.get('high') or ticker_data.get('h')),
            low=self.safe_decimal(ticker_data.get('low') or ticker_data.get('lo')),
            volume=self.safe_decimal(ticker_data.get('volume') or ticker_data.get('v')),
            quote_volume=self.safe_decimal(ticker_data.get('quoteVolume') or ticker_data.get('qv')),
            change=self.safe_decimal(ticker_data.get('change') or ticker_data.get('c')),
            change_percent=self.safe_decimal(ticker_data.get('percentage') or ticker_data.get('p')),
            timestamp=datetime.fromtimestamp(ticker_data.get('timestamp', 0) / 1000) if ticker_data.get('timestamp') else datetime.now()
        )

    def _parse_orderbook(self, orderbook_data: Dict, symbol: str) -> OrderBookData:
        """解析订单簿数据"""
        bids_data = orderbook_data.get('bids', []) or orderbook_data.get('b', [])
        asks_data = orderbook_data.get('asks', []) or orderbook_data.get('a', [])
        
        bids = [
            OrderBookLevel(
                price=self.safe_decimal(level[0] if isinstance(level, (list, tuple)) else level.get('price', level.get('p'))),
                quantity=self.safe_decimal(level[1] if isinstance(level, (list, tuple)) else level.get('quantity', level.get('q')))
            )
            for level in bids_data
        ]
        
        asks = [
            OrderBookLevel(
                price=self.safe_decimal(level[0] if isinstance(level, (list, tuple)) else level.get('price', level.get('p'))),
                quantity=self.safe_decimal(level[1] if isinstance(level, (list, tuple)) else level.get('quantity', level.get('q')))
            )
            for level in asks_data
        ]
        
        return OrderBookData(
            symbol=symbol,
            exchange="grvt",
            bids=bids,
            asks=asks,
            timestamp=datetime.fromtimestamp(orderbook_data.get('timestamp', 0) / 1000) if orderbook_data.get('timestamp') else datetime.now()
        )

    def _parse_trade(self, trade_data: Dict, symbol: str) -> TradeData:
        """解析成交记录"""
        return TradeData(
            symbol=symbol,
            exchange="grvt",
            trade_id=str(trade_data.get('id', trade_data.get('i', ''))),
            price=self.safe_decimal(trade_data.get('price', trade_data.get('p'))),
            quantity=self.safe_decimal(trade_data.get('amount', trade_data.get('a'))),
            side=OrderSide.BUY if (trade_data.get('side', trade_data.get('s', '')).lower() == 'buy') else OrderSide.SELL,
            timestamp=datetime.fromtimestamp(trade_data.get('timestamp', trade_data.get('t', 0)) / 1000) if trade_data.get('timestamp') or trade_data.get('t') else datetime.now()
        )

    def _parse_ohlcv(self, ohlcv_data: List, symbol: str) -> OHLCVData:
        """解析K线数据"""
        # OHLCV通常是数组格式: [timestamp, open, high, low, close, volume]
        return OHLCVData(
            symbol=symbol,
            exchange="grvt",
            timestamp=datetime.fromtimestamp(ohlcv_data[0] / 1000),
            open=self.safe_decimal(ohlcv_data[1]),
            high=self.safe_decimal(ohlcv_data[2]),
            low=self.safe_decimal(ohlcv_data[3]),
            close=self.safe_decimal(ohlcv_data[4]),
            volume=self.safe_decimal(ohlcv_data[5]) if len(ohlcv_data) > 5 else Decimal('0')
        )

    def _parse_balances(self, balance_data: Dict) -> List[BalanceData]:
        """解析余额数据"""
        result = []
        
        # 处理余额数据（可能是字典或列表）
        if isinstance(balance_data, dict):
            # 如果是字典，遍历所有币种
            for currency, balance_info in balance_data.items():
                if currency in ['info', 'free', 'used', 'total']:
                    continue
                if isinstance(balance_info, dict):
                    result.append(BalanceData(
                        currency=currency.upper(),
                        available=self.safe_decimal(balance_info.get('free') or balance_info.get('available')),
                        locked=self.safe_decimal(balance_info.get('used') or balance_info.get('locked')),
                        total=self.safe_decimal(balance_info.get('total')),
                        usd_value=self.safe_decimal(balance_info.get('usdValue', 0))
                    ))
                else:
                    result.append(BalanceData(
                        currency=currency.upper(),
                        available=self.safe_decimal(balance_info),
                        locked=Decimal('0'),
                        total=self.safe_decimal(balance_info),
                        usd_value=Decimal('0')
                    ))
        elif isinstance(balance_data, list):
            # 如果是列表，遍历所有余额项
            for item in balance_data:
                currency = item.get('currency', item.get('c', ''))
                if currency:
                    result.append(BalanceData(
                        currency=currency.upper(),
                        available=self.safe_decimal(item.get('available', item.get('a'))),
                        locked=self.safe_decimal(item.get('locked', item.get('l'))),
                        total=self.safe_decimal(item.get('total', item.get('t'))),
                        usd_value=self.safe_decimal(item.get('usdValue', item.get('uv', 0)))
                    ))
        
        return result

    def _parse_positions(self, positions_data: List[Dict]) -> List[PositionData]:
        """解析持仓数据"""
        result = []
        
        for pos in positions_data:
            symbol = self.denormalize_symbol(pos.get('symbol', pos.get('s', '')))
            side_str = pos.get('side', pos.get('sd', '')).lower()
            
            side = PositionSide.LONG if side_str == 'long' else PositionSide.SHORT
            
            result.append(PositionData(
                symbol=symbol,
                side=side,
                size=self.safe_decimal(pos.get('size', pos.get('sz')) or pos.get('contracts', pos.get('c'))),
                entry_price=self.safe_decimal(pos.get('entryPrice', pos.get('ep')) or pos.get('entry_price')),
                mark_price=self.safe_decimal(pos.get('markPrice', pos.get('mp')) or pos.get('mark_price')),
                current_price=self.safe_decimal(pos.get('markPrice', pos.get('mp')) or pos.get('mark_price')),
                unrealized_pnl=self.safe_decimal(pos.get('unrealizedPnl', pos.get('up')) or pos.get('unrealizedPnl')),
                realized_pnl=self.safe_decimal(pos.get('realizedPnl', pos.get('rp')) or pos.get('realizedPnl')),
                percentage=self.safe_decimal(pos.get('percentage', pos.get('p'))),
                leverage=self.safe_int(pos.get('leverage', pos.get('l', 1))),
                margin_mode=MarginMode.CROSS if (pos.get('marginMode', pos.get('mm', '')).lower() == 'cross') else MarginMode.ISOLATED,
                margin=self.safe_decimal(pos.get('margin', pos.get('m'))),
                liquidation_price=self.safe_decimal(pos.get('liquidationPrice', pos.get('lp')) or pos.get('liquidation_price')),
                timestamp=datetime.fromtimestamp(pos.get('timestamp', pos.get('t', 0)) / 1000) if (pos.get('timestamp') or pos.get('t')) else datetime.now(),
                raw_data=pos
            ))
        
        return result

    def _parse_order(self, order_data: Dict, symbol: str) -> OrderData:
        """解析订单数据"""
        status_str = (order_data.get('status', order_data.get('st', '')) or '').lower()
        status = OrderStatus.OPEN
        if 'filled' in status_str:
            status = OrderStatus.FILLED
        elif 'canceled' in status_str or 'cancelled' in status_str:
            status = OrderStatus.CANCELED
        elif 'rejected' in status_str:
            status = OrderStatus.REJECTED
        
        side_str = (order_data.get('side', order_data.get('sd', '')) or '').lower()
        side = OrderSide.BUY if side_str == 'buy' else OrderSide.SELL
        
        type_str = (order_data.get('type', order_data.get('t', '')) or '').lower()
        order_type = OrderType.MARKET if type_str == 'market' else OrderType.LIMIT
        
        return OrderData(
            id=str(order_data.get('id', order_data.get('i', ''))),
            symbol=symbol,
            exchange="grvt",
            side=side,
            type=order_type,
            status=status,
            price=self.safe_decimal(order_data.get('price', order_data.get('p'))),
            amount=self.safe_decimal(order_data.get('amount', order_data.get('a'))),
            filled=self.safe_decimal(order_data.get('filled', order_data.get('f'))),
            remaining=self.safe_decimal(order_data.get('remaining', order_data.get('r'))),
            fee=self.safe_decimal(
                order_data.get('fee', {}).get('cost') if isinstance(order_data.get('fee'), dict) 
                else order_data.get('fee', order_data.get('fe'))
            ),
            timestamp=datetime.fromtimestamp(order_data.get('timestamp', order_data.get('t', 0)) / 1000) if (order_data.get('timestamp') or order_data.get('t')) else datetime.now(),
            raw_data=order_data
        )
