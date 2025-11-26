"""
GRVT WebSocket模块

使用 grvt-pysdk 的 Raw API 实现WebSocket连接和数据订阅
"""

import asyncio
import os
from typing import Dict, List, Optional, Any, Callable
from decimal import Decimal
from datetime import datetime

from .grvt_base import GrvtBase
from ..models import (
    TickerData, OrderBookData, TradeData, OrderBookLevel, OrderSide
)


class GrvtWebSocket(GrvtBase):
    """GRVT WebSocket客户端 - 使用 Raw API"""

    def __init__(self, config, logger=None):
        """初始化WebSocket适配器"""
        super().__init__(config)
        self.logger = logger
        
        # GRVT SDK WebSocket 实例（延迟初始化）
        self._raw_ws = None
        self._ws_connected = False
        
        # 订阅管理
        self._subscriptions: List[tuple] = []  # (sub_type, symbol, callback)
        self._active_subscriptions = set()
        
        # 控制标志
        self._should_stop = False
        self._reconnecting = False
        
        # 缓存
        self._latest_orderbooks: Dict[str, Dict[str, Any]] = {}

    async def _init_raw_ws(self) -> bool:
        """初始化GRVT Raw WebSocket实例"""
        try:
            if self._raw_ws:
                return True
            
            # 设置环境变量（GRVT SDK 使用环境变量配置）
            if not os.getenv('GRVT_PRIVATE_KEY') and self.private_key:
                os.environ['GRVT_PRIVATE_KEY'] = self.private_key
            if not os.getenv('GRVT_API_KEY') and self.api_key:
                os.environ['GRVT_API_KEY'] = self.api_key
            if not os.getenv('GRVT_TRADING_ACCOUNT_ID') and self.trading_account_id:
                os.environ['GRVT_TRADING_ACCOUNT_ID'] = str(self.trading_account_id)
            if not os.getenv('GRVT_ENV'):
                os.environ['GRVT_ENV'] = self.env
            if not os.getenv('GRVT_END_POINT_VERSION'):
                os.environ['GRVT_END_POINT_VERSION'] = self.endpoint_version
            if not os.getenv('GRVT_WS_STREAM_VERSION'):
                os.environ['GRVT_WS_STREAM_VERSION'] = self.ws_stream_version
            
            # GRVT SDK 导入路径
            try:
                from pysdk.grvt_raw_ws import GrvtRawWS
            except ImportError:
                # 尝试备用导入路径
                try:
                    from grvt_pysdk.grvt_raw_ws import GrvtRawWS
                except ImportError:
                    # 如果没有专门的WebSocket类，可能需要使用其他方式
                    if self.logger:
                        self.logger.error("❌ 未找到GRVT Raw WebSocket类")
                    return False
            
            # 创建GRVT Raw WebSocket实例
            # GRVT SDK 使用环境变量配置，所以不需要传参
            self._raw_ws = GrvtRawWS()
            
            if self.logger:
                self.logger.info("✅ GRVT Raw WebSocket实例已初始化")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 初始化GRVT Raw WebSocket失败: {str(e)}")
                import traceback
                self.logger.error(traceback.format_exc())
            return False

    async def connect(self) -> bool:
        """连接WebSocket"""
        try:
            if not await self._init_raw_ws():
                return False
            
            # GRVT Raw WebSocket 连接
            if hasattr(self._raw_ws, 'connect'):
                if asyncio.iscoroutinefunction(self._raw_ws.connect):
                    await self._raw_ws.connect()
                else:
                    await asyncio.get_event_loop().run_in_executor(
                        None, self._raw_ws.connect
                    )
            elif hasattr(self._raw_ws, 'start'):
                if asyncio.iscoroutinefunction(self._raw_ws.start):
                    await self._raw_ws.start()
                else:
                    await asyncio.get_event_loop().run_in_executor(
                        None, self._raw_ws.start
                    )
            
            self._ws_connected = True
            
            # 启动消息处理循环
            if hasattr(self._raw_ws, 'listen') or hasattr(self._raw_ws, 'receive'):
                self._message_handler_task = asyncio.create_task(
                    self._message_handler_loop()
                )
            
            if self.logger:
                self.logger.info("✅ GRVT WebSocket连接成功")
            
            return True
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 连接GRVT WebSocket失败: {str(e)}")
            self._ws_connected = False
            return False

    async def disconnect(self) -> None:
        """断开WebSocket连接"""
        try:
            self._should_stop = True
            
            if self._raw_ws:
                if hasattr(self._raw_ws, 'close'):
                    if asyncio.iscoroutinefunction(self._raw_ws.close):
                        await self._raw_ws.close()
                    else:
                        await asyncio.get_event_loop().run_in_executor(
                            None, self._raw_ws.close
                        )
                elif hasattr(self._raw_ws, 'stop'):
                    if asyncio.iscoroutinefunction(self._raw_ws.stop):
                        await self._raw_ws.stop()
                    else:
                        await asyncio.get_event_loop().run_in_executor(
                            None, self._raw_ws.stop
                        )
                self._raw_ws = None
            
            self._ws_connected = False
            
            if self.logger:
                self.logger.info("✅ GRVT WebSocket连接已断开")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 断开GRVT WebSocket失败: {str(e)}")

    async def _message_handler_loop(self):
        """消息处理循环"""
        try:
            while not self._should_stop and self._ws_connected:
                try:
                    # 接收消息
                    if hasattr(self._raw_ws, 'receive'):
                        if asyncio.iscoroutinefunction(self._raw_ws.receive):
                            message = await self._raw_ws.receive()
                        else:
                            message = await asyncio.get_event_loop().run_in_executor(
                                None, self._raw_ws.receive
                            )
                    elif hasattr(self._raw_ws, 'listen'):
                        if asyncio.iscoroutinefunction(self._raw_ws.listen):
                            async for message in self._raw_ws.listen():
                                await self._handle_message(message)
                            break
                        else:
                            for message in self._raw_ws.listen():
                                await self._handle_message(message)
                            break
                    else:
                        # 如果没有receive/listen方法，等待一段时间后重试
                        await asyncio.sleep(1)
                        continue
                    
                    if message:
                        await self._handle_message(message)
                        
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"处理WebSocket消息失败: {str(e)}")
                    await asyncio.sleep(1)
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"消息处理循环错误: {str(e)}")

    async def _handle_message(self, message: Dict[str, Any]):
        """处理接收到的消息"""
        try:
            msg_type = message.get('type') or message.get('t') or message.get('event')
            symbol = message.get('symbol') or message.get('s')
            
            # 消息类型到订阅类型的映射
            type_map = {
                'ticker': ('ticker', lambda m, s: self._parse_ticker(m, s)),
                'orderbook': ('orderbook', lambda m, s: self._parse_orderbook(m, s)),
                'depth': ('orderbook', lambda m, s: self._parse_orderbook(m, s)),
                'trade': ('trades', lambda m, s: self._parse_trade(m, s)),
                'order': ('user_data', lambda m, s: m),
                'user_data': ('user_data', lambda m, s: m),
            }
            
            for key, (sub_type, parser) in type_map.items():
                if key in str(msg_type).lower():
                    if symbol and sub_type != 'user_data':
                        original_symbol = self.denormalize_symbol(symbol)
                        for st, ss, cb in self._subscriptions:
                            if st == sub_type and (ss == original_symbol or ss == symbol):
                                cb(parser(message, original_symbol))
                    elif sub_type == 'user_data':
                        for st, ss, cb in self._subscriptions:
                            if st == 'user_data':
                                cb(message)
                    break
        except Exception as e:
            if self.logger:
                self.logger.error(f"处理消息失败: {str(e)}")

    async def _do_subscribe(self, method_name: str, channel: str, symbol: Optional[str] = None) -> None:
        """执行订阅的通用方法"""
        normalized_symbol = self.normalize_symbol(symbol) if symbol else None
        
        # 尝试使用特定方法（如 subscribe_ticker, subscribe_orderbook 等）
        if hasattr(self._raw_ws, method_name):
            method = getattr(self._raw_ws, method_name)
            is_async = asyncio.iscoroutinefunction(method)
            args = [normalized_symbol] if normalized_symbol else []
            
            if is_async:
                await method(*args)
            else:
                method(*args)
        # 使用通用 subscribe 方法
        elif hasattr(self._raw_ws, 'subscribe'):
            sub_params = {'channel': channel}
            if normalized_symbol:
                sub_params['symbol'] = normalized_symbol
            
            if asyncio.iscoroutinefunction(self._raw_ws.subscribe):
                await self._raw_ws.subscribe(**sub_params)
            else:
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self._raw_ws.subscribe(**sub_params)
                )
        else:
            raise NotImplementedError(f"GRVT WebSocket 不支持 {method_name} 或 subscribe 方法")

    async def subscribe_ticker(self, symbol: str, callback: Callable[[TickerData], None]) -> None:
        """订阅行情数据流"""
        try:
            await self._do_subscribe('subscribe_ticker', 'ticker', symbol)
            normalized_symbol = self.normalize_symbol(symbol)
            self._subscriptions.append(('ticker', symbol, callback))
            self._active_subscriptions.add(('ticker', normalized_symbol))
            if self.logger:
                self.logger.info(f"✅ 已订阅行情: {symbol}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅行情失败 {symbol}: {str(e)}")
            raise

    async def subscribe_orderbook(self, symbol: str, callback: Callable[[OrderBookData], None]) -> None:
        """订阅订单簿数据流"""
        try:
            await self._do_subscribe('subscribe_orderbook', 'orderbook', symbol)
            normalized_symbol = self.normalize_symbol(symbol)
            self._subscriptions.append(('orderbook', symbol, callback))
            self._active_subscriptions.add(('orderbook', normalized_symbol))
            if self.logger:
                self.logger.info(f"✅ 已订阅订单簿: {symbol}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅订单簿失败 {symbol}: {str(e)}")
            raise

    async def subscribe_trades(self, symbol: str, callback: Callable[[TradeData], None]) -> None:
        """订阅成交数据流"""
        try:
            await self._do_subscribe('subscribe_trades', 'trades', symbol)
            normalized_symbol = self.normalize_symbol(symbol)
            self._subscriptions.append(('trades', symbol, callback))
            self._active_subscriptions.add(('trades', normalized_symbol))
            if self.logger:
                self.logger.info(f"✅ 已订阅成交: {symbol}")
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅成交失败 {symbol}: {str(e)}")
            raise

    async def subscribe_user_data(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """订阅用户数据流（订单更新、持仓变化等）"""
        try:
            await self._do_subscribe('subscribe_user_data', 'user_data')
            self._subscriptions.append(('user_data', None, callback))
            self._active_subscriptions.add(('user_data', None))
            if self.logger:
                self.logger.info("✅ 已订阅用户数据流")
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 订阅用户数据流失败: {str(e)}")
            raise

    async def unsubscribe(self, symbol: Optional[str] = None) -> None:
        """取消订阅"""
        try:
            if symbol:
                normalized_symbol = self.normalize_symbol(symbol)
                # 取消特定交易对的订阅
                subscriptions_to_remove = [
                    sub for sub in self._subscriptions
                    if sub[1] == symbol or sub[1] == normalized_symbol
                ]
                
                for sub_type, sub_symbol, _ in subscriptions_to_remove:
                    self._active_subscriptions.discard((sub_type, normalized_symbol))
                    self._subscriptions.remove((sub_type, sub_symbol, _))
                    
                    # 调用SDK的取消订阅方法
                    if hasattr(self._raw_ws, 'unsubscribe'):
                        if asyncio.iscoroutinefunction(self._raw_ws.unsubscribe):
                            await self._raw_ws.unsubscribe(symbol=normalized_symbol)
                        else:
                            await asyncio.get_event_loop().run_in_executor(
                                None, self._raw_ws.unsubscribe, normalized_symbol
                            )
                
                if self.logger:
                    self.logger.info(f"✅ 已取消订阅: {symbol}")
            else:
                # 取消所有订阅
                if hasattr(self._raw_ws, 'unsubscribe_all'):
                    if asyncio.iscoroutinefunction(self._raw_ws.unsubscribe_all):
                        await self._raw_ws.unsubscribe_all()
                    else:
                        await asyncio.get_event_loop().run_in_executor(
                            None, self._raw_ws.unsubscribe_all
                        )
                
                self._subscriptions.clear()
                self._active_subscriptions.clear()
                
                if self.logger:
                    self.logger.info("✅ 已取消所有订阅")
                    
        except Exception as e:
            if self.logger:
                self.logger.error(f"❌ 取消订阅失败: {str(e)}")

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
            timestamp=datetime.fromtimestamp(ticker_data.get('timestamp', ticker_data.get('t', 0)) / 1000) if (ticker_data.get('timestamp') or ticker_data.get('t')) else datetime.now()
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
            timestamp=datetime.fromtimestamp(orderbook_data.get('timestamp', orderbook_data.get('t', 0)) / 1000) if (orderbook_data.get('timestamp') or orderbook_data.get('t')) else datetime.now()
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
            timestamp=datetime.fromtimestamp(trade_data.get('timestamp', trade_data.get('t', 0)) / 1000) if (trade_data.get('timestamp') or trade_data.get('t')) else datetime.now()
        )
