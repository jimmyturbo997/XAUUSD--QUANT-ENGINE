#!/usr/bin/env python3
"""Async XAUUSD (gold) high-frequency intraday trading engine.

Pipeline:
    Feed -> BarBuilder -> StrategyAgent (ATR / Bollinger breakout)
         -> RiskAgent (ATR-driven position sizing + limits)
         -> ExecutionAgent (routing, retries, brackets) -> Portfolio

Requires Python >= 3.10. Standard library only; no third-party dependencies.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import dataclasses
import json
import logging
import logging.handlers
import math
import os
import random
import signal
import statistics
import sys
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Coroutine, Deque, Dict, List, Optional, Tuple

__version__ = "1.4.0"

# --------------------------------------------------------------------------- #
# Instrument constants (spot gold / XAUUSD CFD conventions)
# --------------------------------------------------------------------------- #
SYMBOL = "XAUUSD"
CONTRACT_SIZE = 100.0            # troy ounces per 1.00 standard lot
TICK_SIZE = 0.01                 # USD quote step
MIN_LOT = 0.01
LOT_STEP = 0.01
DEFAULT_BALANCE = 100_000.0
DEFAULT_RISK_PER_TRADE = 0.005   # 0.5% of equity per trade
DEFAULT_LEVERAGE = 100.0
TYPICAL_SPREAD = 0.28            # USD


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EngineError(Exception):
    """Base class for every failure raised inside the engine."""


class FeedError(EngineError):
    """Market-data source failed or is unavailable."""


class BrokerError(EngineError):
    """Order routing failure."""


class TransientBrokerError(BrokerError):
    """Recoverable: requote, socket timeout, partial connectivity."""


class BrokerRejected(BrokerError):
    """Permanent: margin call, market closed, invalid quantity."""


class KillSwitchTripped(EngineError):
    """Engine-wide shutdown demanded by risk or the circuit breaker."""


# --------------------------------------------------------------------------- #
# Domain types
# --------------------------------------------------------------------------- #
class Side(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"

    @property
    def opposite(self) -> "Side":
        return Side.SHORT if self is Side.LONG else Side.LONG


class Action(str, Enum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"


class Topic(str, Enum):
    TICK = "tick"
    BAR = "bar"
    SIGNAL = "signal"
    ORDER = "order"
    FILL = "fill"
    REJECT = "reject"


@dataclass(frozen=True)
class Tick:
    ts: float            # epoch seconds on the trading clock
    bid: float
    ask: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid


@dataclass(frozen=True)
class Bar:
    symbol: str
    start_ts: float
    end_ts: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    ticks: int


@dataclass(frozen=True)
class Bands:
    upper: float
    mid: float
    lower: float


@dataclass(frozen=True)
class Signal:
    ts: float
    symbol: str
    side: Side
    action: Action
    ref_price: float
    atr: float
    reason: str
    strength: float
    bar_ts: Optional[float] = None
    position_id: Optional[str] = None


@dataclass(frozen=True)
class Order:
    client_order_id: str
    ts: float
    symbol: str
    side: Side
    action: Action
    lots: float
    ref_price: float
    reason: str
    atr: float = 0.0
    stop: Optional[float] = None
    target: Optional[float] = None
    position_id: Optional[str] = None
    risk_usd: float = 0.0
    attempt: int = 0


@dataclass(frozen=True)
class Fill:
    client_order_id: str
    ts: float
    symbol: str
    side: Side
    action: Action
    lots: float
    price: float
    fee: float
    slippage: float
    position_id: Optional[str]
    realized_pnl: float = 0.0


@dataclass(frozen=True)
class Reject:
    client_order_id: str
    ts: float
    symbol: str
    code: str
    message: str


@dataclass
class Position:
    position_id: str
    symbol: str
    side: Side
    lots: float
    original_lots: float
    entry_price: float
    opened_ts: float
    stop: float = 0.0
    target: float = 0.0
    atr_at_entry: float = 0.0
    realized_pnl: float = 0.0
    fees: float = 0.0
    reason: str = ""
    exit_reason: str = ""
    exit_requested: bool = False
    closed: bool = False


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
CORRELATION: ContextVar[str] = ContextVar("correlation_id", default="-")


class _CorrelationFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.corr = CORRELATION.get()
        if not hasattr(record, "component"):
            record.component = record.name.split(".")[-1]
        if not hasattr(record, "fields"):
            record.fields = {}
        return True


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "component": getattr(record, "component", record.name),
            "corr": getattr(record, "corr", "-"),
            "msg": record.getMessage(),
        }
        payload.update(getattr(record, "fields", {}) or {})
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


_TEXT_FORMATTER = logging.Formatter(
    "%(asctime)s.%(msecs)03d %(levelname)-8s [%(component)-12s] cid=%(corr)-14s %(message)s",
    datefmt="%H:%M:%S",
)


def configure_logging(level: str = "INFO", log_file: Optional[str] = None,
                      log_format: str = "text") -> logging.Logger:
    """Configure the `xau` logger with console plus rotating file handlers."""
    root = logging.getLogger("xau")
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.handlers.clear()
    root.propagate = False

    formatter: logging.Formatter = (
        _JsonFormatter() if log_format == "json" else _TEXT_FORMATTER
    )
    correlation_filter = _CorrelationFilter()

    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(formatter)
    console.addFilter(correlation_filter)
    root.addHandler(console)

    if log_file:
        try:
            directory = os.path.dirname(os.path.abspath(log_file))
            os.makedirs(directory, exist_ok=True)
            file_handler = logging.handlers.RotatingFileHandler(
                log_file, maxBytes=32 * 1024 * 1024, backupCount=5, encoding="utf-8"
            )
            file_handler.setFormatter(formatter)
            file_handler.addFilter(correlation_filter)
            root.addHandler(file_handler)
        except OSError:
            root.exception("cannot open log file %s; console only", log_file)

    return root


def log_event(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    """Emit a structured record; values are rendered by the JSON formatter."""
    logger.log(level, message, extra={"fields": fields})


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


# --------------------------------------------------------------------------- #
# Event bus
# --------------------------------------------------------------------------- #
class EventBus:
    """Fan-out bus of bounded asyncio queues.

    Backpressure policy: when a subscriber falls behind the oldest event is
    dropped, because in a live trading loop the newest quote beats a stale one.
    """

    def __init__(self, logger: logging.Logger, maxsize: int = 8192) -> None:
        self._logger = logger.getChild("bus")
        self._maxsize = maxsize
        self._subs: Dict[Topic, List[Tuple[str, asyncio.Queue]]] = {t: [] for t in Topic}
        self._published: Dict[Topic, int] = {t: 0 for t in Topic}
        self._dropped: Dict[Topic, int] = {t: 0 for t in Topic}

    def subscribe(self, topic: Topic, name: str) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._maxsize)
        self._subs[topic].append((name, queue))
        return queue

    async def publish(self, topic: Topic, event: Any) -> None:
        self._published[topic] += 1
        for name, queue in self._subs[topic]:
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                self._dropped[topic] += 1
                total = sum(self._dropped.values())
                if total % 100 == 1:
                    self._logger.warning(
                        "backpressure: subscriber %s on %s is lagging, %d events dropped",
                        name, topic.value, total,
                    )
            queue.put_nowait(event)

    def stats(self) -> Dict[str, int]:
        return {
            "published": sum(self._published.values()),
            "dropped": sum(self._dropped.values()),
            "pending": sum(q.qsize() for pairs in self._subs.values() for _, q in pairs),
        }

    def queue_depth(self) -> Dict[str, int]:
        return {
            topic.value: sum(q.qsize() for _, q in self._subs[topic])
            for topic in Topic
        }


# --------------------------------------------------------------------------- #
# Indicators
# --------------------------------------------------------------------------- #
class WilderATR:
    """Average True Range with Wilder smoothing, O(1) per bar."""

    def __init__(self, period: int = 14) -> None:
        if period < 2:
            raise ValueError("ATR period must be >= 2")
        self.period = period
        self._prev_close: Optional[float] = None
        self._seed: Deque[float] = collections.deque(maxlen=period)
        self._value: Optional[float] = None
        self._prev_value: Optional[float] = None

    def update(self, bar: Bar) -> Optional[float]:
        if self._prev_close is None:
            true_range = bar.high - bar.low
        else:
            true_range = max(
                bar.high - bar.low,
                abs(bar.high - self._prev_close),
                abs(bar.low - self._prev_close),
            )
        self._prev_close = bar.close

        if self._value is None:
            self._seed.append(true_range)
            if len(self._seed) < self.period:
                return None
            self._value = sum(self._seed) / self.period
            self._prev_value = self._value
            return self._value

        self._prev_value = self._value
        self._value = (self._value * (self.period - 1) + true_range) / self.period
        return self._value

    @property
    def value(self) -> Optional[float]:
        return self._value

    @property
    def slope(self) -> float:
        if self._value is None or self._prev_value is None:
            return 0.0
        return self._value - self._prev_value

    @property
    def expanding(self) -> bool:
        return self.slope > 0.0


class BollingerBands:
    """SMA(centre) +/- k * population std dev."""

    def __init__(self, period: int = 20, num_std: float = 2.0) -> None:
        if period < 2:
            raise ValueError("Bollinger period must be >= 2")
        self.period = period
        self.num_std = num_std
        self._closes: Deque[float] = collections.deque(maxlen=period)

    def update(self, close: float) -> Optional[Bands]:
        self._closes.append(close)
        if len(self._closes) < self.period:
            return None
        mid = sum(self._closes) / self.period
        std = statistics.pstdev(self._closes) or TICK_SIZE
        return Bands(upper=mid + self.num_std * std, mid=mid,
                     lower=mid - self.num_std * std)


class EMA:
    """Exponential moving average with SMA warm-up and slope reporting."""

    def __init__(self, period: int = 50) -> None:
        if period < 1:
            raise ValueError("EMA period must be >= 1")
        self.period = period
        self._alpha = 2.0 / (period + 1.0)
        self._warm: Deque[float] = collections.deque(maxlen=period)
        self._value: Optional[float] = None
        self._prev: Optional[float] = None

    def update(self, price: float) -> Optional[float]:
        self._prev = self._value
        if self._value is None:
            self._warm.append(price)
            if len(self._warm) < self.period:
                return None
            self._value = sum(self._warm) / self.period
            return self._value
        self._value = self._alpha * price + (1.0 - self._alpha) * self._value
        return self._value

    @property
    def value(self) -> Optional[float]:
        return self._value

    @property
    def slope_bps(self) -> float:
        """Bar-over-bar drift of the average, in basis points of price."""
        if self._value is None or not self._prev:
            return 0.0
        return (self._value - self._prev) / self._prev * 10_000.0


class RollingMean:
    def __init__(self, period: int = 20) -> None:
        self._items: Deque[float] = collections.deque(maxlen=period)

    def update(self, value: float) -> Optional[float]:
        self._items.append(value)
        return sum(self._items) / len(self._items) if self._items else None

    @property
    def value(self) -> Optional[float]:
        return sum(self._items) / len(self._items) if self._items else None


# --------------------------------------------------------------------------- #
# Portfolio
# --------------------------------------------------------------------------- #
class Portfolio:
    """Cash book, open positions and exposure accounting."""

    def __init__(self, balance: float, leverage: float, logger: logging.Logger) -> None:
        if balance <= 0:
            raise ValueError("starting balance must be positive")
        if leverage <= 0:
            raise ValueError("leverage must be positive")
        self._logger = logger.getChild("portfolio")
        self._lock = asyncio.Lock()
        self.starting_balance = balance
        self.day_start_equity = balance
        self.balance = balance
        self.equity = balance
        self.leverage = leverage
        self.positions: Dict[str, Position] = {}
        self.closed_trades: List[Position] = []
        self.realized_pnl = 0.0
        self.total_fees = 0.0
        self.total_slippage = 0.0
        self.peak_equity = balance
        self.max_drawdown = 0.0
        self.last_bid = 0.0
        self.last_ask = 0.0

    # -- derived state -----------------------------------------------------
    @property
    def open_positions(self) -> List[Position]:
        return [p for p in self.positions.values() if not p.closed]

    def gross_lots(self) -> float:
        return sum(p.lots for p in self.open_positions)

    def exposure_lots(self, side: Side) -> float:
        return sum(p.lots for p in self.open_positions if p.side is side)

    def open_count(self, side: Optional[Side] = None) -> int:
        positions = self.open_positions
        if side is not None:
            positions = [p for p in positions if p.side is side]
        return len(positions)

    @property
    def used_margin(self) -> float:
        notional = sum(p.lots * CONTRACT_SIZE * p.entry_price for p in self.open_positions)
        return notional / self.leverage

    @property
    def free_margin(self) -> float:
        return max(0.0, self.equity - self.used_margin)

    def unrealized_pnl(self) -> float:
        if not self.open_positions or self.last_bid <= 0:
            return 0.0
        total = 0.0
        for p in self.open_positions:
            exit_price = self.last_bid if p.side is Side.LONG else self.last_ask
            sign = 1.0 if p.side is Side.LONG else -1.0
            total += (exit_price - p.entry_price) * CONTRACT_SIZE * p.lots * sign
        return total

    def mark_to_market(self, tick: Tick) -> float:
        self.last_bid, self.last_ask = tick.bid, tick.ask
        self.equity = self.balance + self.unrealized_pnl()
        self.peak_equity = max(self.peak_equity, self.equity)
        if self.peak_equity > 0:
            drawdown = (self.peak_equity - self.equity) / self.peak_equity
            self.max_drawdown = max(self.max_drawdown, drawdown)
        return self.equity

    def day_pnl(self) -> float:
        return self.equity - self.day_start_equity

    # -- mutations ---------------------------------------------------------
    async def apply_fill(self, fill: Fill, reason: str = "") -> Optional[Position]:
        async with self._lock:
            if fill.action is Action.OPEN:
                position = Position(
                    position_id=fill.position_id or new_id("pos"),
                    symbol=fill.symbol,
                    side=fill.side,
                    lots=round(fill.lots, 8),
                    original_lots=round(fill.lots, 8),
                    entry_price=fill.price,
                    opened_ts=fill.ts,
                    fees=fill.fee,
                    reason=reason,
                )
                self.positions[position.position_id] = position
                self.total_slippage += fill.slippage * fill.lots * CONTRACT_SIZE
                self.balance -= fill.fee
                self._logger.info(
                    "OPENED %s %s %.2f lots @ %.2f id=%s fee=%.2f",
                    position.side.value, position.symbol, position.lots,
                    position.entry_price, position.position_id, fill.fee,
                )
                return position

            position = self.positions.get(fill.position_id or "")
            if position is None or position.closed:
                self._logger.error(
                    "close fill %s has no live position (id=%s)",
                    fill.client_order_id, fill.position_id,
                )
                return None

            sign = 1.0 if position.side is Side.LONG else -1.0
            if fill.lots > position.lots + 1e-9:
                self._logger.warning(
                    "clamping close of %.2f lots to remaining %.2f on %s",
                    fill.lots, position.lots, position.position_id,
                )
                settled_lots = position.lots
            else:
                settled_lots = round(fill.lots, 8)
            gross_pnl = (fill.price - position.entry_price) * CONTRACT_SIZE * settled_lots * sign
            fee = fill.fee * (settled_lots / fill.lots if fill.lots else 0.0)
            net_pnl = gross_pnl - fee

            position.realized_pnl += gross_pnl
            position.fees += fee
            position.lots = round(position.lots - settled_lots, 8)
            self.balance += net_pnl
            self.realized_pnl += gross_pnl
            self.total_fees += fee
            self.total_slippage += fill.slippage * settled_lots * CONTRACT_SIZE

            if position.lots <= 1e-9:
                position.lots = 0.0
                position.closed = True
                position.exit_reason = reason or position.exit_reason
                self.closed_trades.append(position)
                self._logger.info(
                    "CLOSED %s net=%.2f gross=%.2f fees=%.2f side=%s reason=%s",
                    position.position_id, net_pnl, gross_pnl, position.fees,
                    position.side.value, position.exit_reason or "-",
                )
            else:
                self._logger.info(
                    "PARTIAL close %.2f/%.2f lots on %s net=%.2f",
                    settled_lots, position.original_lots, position.position_id, net_pnl,
                )

            if self.last_bid > 0:
                self.equity = self.balance + self.unrealized_pnl()
                self.peak_equity = max(self.peak_equity, self.equity)
            return position

    async def attach_bracket(self, position_id: str, stop: float, target: float,
                             atr: float) -> None:
        async with self._lock:
            position = self.positions.get(position_id)
            if position is None or position.closed:
                return
            position.stop = round(stop, 2)
            position.target = round(target, 2)
            position.atr_at_entry = atr
            self._logger.info(
                "BRACKET %s stop=%.2f target=%.2f atr=%.3f",
                position_id, position.stop, position.target, atr,
            )

    async def trail_stop(self, position_id: str, new_stop: float) -> bool:
        async with self._lock:
            position = self.positions.get(position_id)
            if position is None or position.closed:
                return False
            if position.side is Side.LONG and new_stop > position.stop:
                position.stop = round(new_stop, 2)
                return True
            if position.side is Side.SHORT and 0.0 < new_stop < position.stop:
                position.stop = round(new_stop, 2)
                return True
            return False

    async def request_exit(self, position_id: str, reason: str) -> bool:
        """Claim the single exit request for a position.

        Whoever wins this claim is the only component allowed to publish the
        closing signal, so a stop hit on 100 consecutive ticks still produces
        exactly one order.
        """
        async with self._lock:
            position = self.positions.get(position_id)
            if position is None or position.closed or position.exit_requested:
                return False
            position.exit_requested = True
            position.exit_reason = reason
            return True

    async def release_exit(self, position_id: str) -> None:
        """Re-arm a position for exit attempts after a close order failed."""
        async with self._lock:
            position = self.positions.get(position_id)
            if position is not None and not position.closed:
                position.exit_requested = False

    def snapshot(self) -> Dict[str, Any]:
        return {
            "balance": round(self.balance, 2),
            "equity": round(self.equity, 2),
            "unrealized_pnl": round(self.unrealized_pnl(), 2),
            "used_margin": round(self.used_margin, 2),
            "free_margin": round(self.free_margin, 2),
            "open_positions": len(self.open_positions),
            "gross_lots": round(self.gross_lots(), 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "fees": round(self.total_fees, 2),
            "slippage_usd": round(self.total_slippage, 2),
            "max_drawdown_pct": round(self.max_drawdown * 100.0, 3),
        }


# --------------------------------------------------------------------------- #
# Risk
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RiskConfig:
    risk_per_trade: float = DEFAULT_RISK_PER_TRADE
    leverage: float = DEFAULT_LEVERAGE
    atr_stop_mult: float = 2.0
    atr_target_mult: float = 3.0
    min_lot: float = MIN_LOT
    max_lot: float = 20.0
    max_open_positions: int = 3
    max_positions_per_side: int = 2
    max_gross_lots: float = 12.0
    daily_loss_limit_pct: float = 0.04
    max_drawdown_pct: float = 0.10
    min_stop_distance: float = 0.20
    max_stop_distance: float = 25.0


class RiskAgent:
    """Turns a signal into a sized, limit-checked order.

    lot = (equity * risk_per_trade) / (stop_distance_usd * CONTRACT_SIZE)
    with stop_distance = ATR * atr_stop_mult, so wider volatility means
    automatically smaller size.
    """

    def __init__(self, bus: EventBus, portfolio: Portfolio, cfg: RiskConfig,
                 logger: logging.Logger) -> None:
        self.name = "risk"
        self._bus = bus
        self._portfolio = portfolio
        self._cfg = cfg
        self._logger = logger.getChild("risk")
        self.evaluated = 0
        self.approved = 0
        self.rejections: Dict[str, int] = collections.Counter()

    def lot_size(self, equity: float, stop_distance: float) -> float:
        if stop_distance <= 0 or equity <= 0:
            return 0.0
        raw = equity * self._cfg.risk_per_trade / (stop_distance * CONTRACT_SIZE)
        return math.floor(raw / LOT_STEP) * LOT_STEP

    async def on_event(self, sig: Signal) -> None:
        self.evaluated += 1
        order_id = new_id("ord")
        token = CORRELATION.set(order_id)
        try:
            if sig.action is Action.CLOSE:
                await self._approve_close(sig, order_id)
            else:
                await self._approve_open(sig, order_id)
        except KillSwitchTripped:
            raise
        except Exception as exc:  # defensive: never kill the pipeline on one signal
            self._logger.exception("risk evaluation failed for %s", sig.reason)
            self._reject("RISK_ERROR", f"{type(exc).__name__}: {exc}", sig)
        finally:
            CORRELATION.reset(token)

    # -- close path --------------------------------------------------------
    async def _approve_close(self, sig: Signal, order_id: str) -> None:
        position = self._portfolio.positions.get(sig.position_id or "")
        if position is None or position.closed:
            self._reject("NO_POSITION", f"unknown position {sig.position_id}", sig,
                         level=logging.DEBUG)
            return
        if position.lots <= 0:
            self._reject("NOTHING_TO_CLOSE", "position already flattened", sig,
                         level=logging.DEBUG)
            return

        order = Order(
            client_order_id=order_id,
            ts=sig.ts,
            symbol=sig.symbol,
            side=position.side.opposite,
            action=Action.CLOSE,
            lots=position.lots,
            ref_price=sig.ref_price,
            reason=sig.reason,
            atr=sig.atr,
            position_id=position.position_id,
        )
        self.approved += 1
        await self._bus.publish(Topic.ORDER, order)
        log_event(self._logger, logging.INFO, "risk: exit approved",
                  order=order.client_order_id, position=position.position_id,
                  lots=order.lots, reason=order.reason)

    # -- open path ---------------------------------------------------------
    async def _approve_open(self, sig: Signal, order_id: str) -> None:
        cfg = self._cfg
        book = self._portfolio

        if cfg.max_drawdown_pct > 0 and book.max_drawdown >= cfg.max_drawdown_pct:
            raise KillSwitchTripped(
                f"drawdown {book.max_drawdown:.2%} breaches {cfg.max_drawdown_pct:.2%}"
            )

        day_loss_limit = book.day_start_equity * cfg.daily_loss_limit_pct
        if book.day_pnl() <= -day_loss_limit:
            self._reject("DAILY_LOSS_LIMIT",
                         f"day PnL {book.day_pnl():,.2f} at limit -{day_loss_limit:,.2f}", sig)
            return

        if book.open_count() >= cfg.max_open_positions:
            self._reject("MAX_POSITIONS", f"{book.open_count()} positions open", sig)
            return
        if book.open_count(sig.side) >= cfg.max_positions_per_side:
            self._reject("SIDE_CAP", f"{sig.side.value} position cap reached", sig)
            return

        if sig.atr <= 0:
            self._reject("NO_ATR", "ATR unavailable, risk cannot be measured", sig)
            return

        stop_distance = sig.atr * cfg.atr_stop_mult
        if not cfg.min_stop_distance <= stop_distance <= cfg.max_stop_distance:
            self._reject("STOP_DISTANCE",
                         f"stop distance {stop_distance:.2f} outside policy "
                         f"[{cfg.min_stop_distance}, {cfg.max_stop_distance}]", sig)
            return

        if sig.side is Side.LONG:
            stop = sig.ref_price - stop_distance
            target = sig.ref_price + sig.atr * cfg.atr_target_mult
        else:
            stop = sig.ref_price + stop_distance
            target = sig.ref_price - sig.atr * cfg.atr_target_mult

        lots = self.lot_size(book.equity, stop_distance)
        if lots < cfg.min_lot:
            self._reject(
                "LOT_TOO_SMALL",
                f"sized {lots:.2f} < {cfg.min_lot} (equity={book.equity:,.0f}, "
                f"stop={stop_distance:.2f})", sig,
            )
            return
        lots = min(lots, cfg.max_lot)

        if book.gross_lots() + lots > cfg.max_gross_lots:
            capped = math.floor(
                (cfg.max_gross_lots - book.gross_lots()) / LOT_STEP
            ) * LOT_STEP
            if capped < cfg.min_lot:
                self._reject("MAX_GROSS_LOTS", f"gross cap {cfg.max_gross_lots} reached", sig)
                return
            self._logger.warning("size throttled %.2f -> %.2f lots by gross cap", lots, capped)
            lots = capped

        notional = lots * CONTRACT_SIZE * sig.ref_price
        required_margin = notional / cfg.leverage
        if required_margin > book.free_margin:
            self._reject(
                "MARGIN",
                f"margin ${required_margin:,.2f} > free ${book.free_margin:,.2f}", sig,
            )
            return

        risk_usd = stop_distance * CONTRACT_SIZE * lots
        order = Order(
            client_order_id=order_id,
            ts=sig.ts,
            symbol=sig.symbol,
            side=sig.side,
            action=Action.OPEN,
            lots=round(lots, 8),
            ref_price=sig.ref_price,
            reason=sig.reason,
            atr=sig.atr,
            stop=stop,
            target=target,
            risk_usd=risk_usd,
        )
        self.approved += 1
        await self._bus.publish(Topic.ORDER, order)
        log_event(
            self._logger, logging.INFO, "risk: entry approved",
            order=order.client_order_id, side=order.side.value, lots=order.lots,
            risk_usd=round(risk_usd, 2),
            risk_pct=round(risk_usd / book.equity * 100.0, 4),
            stop=round(stop, 2), target=round(target, 2),
            atr=round(sig.atr, 3),
            reward_risk=round(cfg.atr_target_mult / cfg.atr_stop_mult, 2),
            margin=round(required_margin, 2),
        )

    def _reject(self, code: str, message: str, sig: Signal,
                level: int = logging.WARNING) -> None:
        self.rejections[code] += 1
        self._logger.log(
            level,
            "SIGNAL REJECTED [%s] %s %s @ %.2f (%s) :: %s",
            code, sig.action.value, sig.side.value, sig.ref_price, sig.reason, message,
        )

    def stats(self) -> Dict[str, Any]:
        return {
            "evaluated": self.evaluated,
            "approved": self.approved,
            "rejections": dict(self.rejections),
        }


# --------------------------------------------------------------------------- #
# Strategy
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class StrategyConfig:
    bar_seconds: int = 60
    atr_period: int = 14
    atr_channel_mult: float = 2.2
    bb_period: int = 20
    bb_std: float = 2.0
    trend_ema: int = 50
    volume_mult: float = 1.1
    cooldown_bars: int = 3
    min_atr: float = 0.20
    max_atr: float = 20.0
    trail_atr_mult: float = 2.5
    one_position_per_side: bool = True


class StrategyAgent:
    """Long/short breakout detection on the confluence of the ATR channel and
    Bollinger Bands, filtered by trend, volume and volatility expansion."""

    def __init__(self, bus: EventBus, portfolio: Portfolio, cfg: StrategyConfig,
                 logger: logging.Logger) -> None:
        self.name = "strategy"
        self._bus = bus
        self._portfolio = portfolio
        self._cfg = cfg
        self._logger = logger.getChild("strategy")
        self._atr = WilderATR(cfg.atr_period)
        self._bb = BollingerBands(cfg.bb_period, cfg.bb_std)
        self._trend = EMA(cfg.trend_ema)
        self._volume = RollingMean(cfg.bb_period)
        self._bars = 0
        self._last_entry_bar: Dict[Side, float] = {}
        self.signals: Dict[str, int] = collections.Counter()
        self.warmup = max(cfg.atr_period, cfg.bb_period, cfg.trend_ema)

    @property
    def last_atr(self) -> Optional[float]:
        return self._atr.value

    @property
    def bars_seen(self) -> int:
        return self._bars

    async def on_event(self, bar: Bar) -> None:
        try:
            atr = self._atr.update(bar)
            bands = self._bb.update(bar.close)
            trend = self._trend.update(bar.close)
            avg_volume = self._volume.update(bar.volume)
            self._bars += 1

            if atr is None or bands is None or trend is None:
                if self._bars == 1 or self._bars % 25 == 0:
                    self._logger.info("indicator warm-up %d/%d bars", self._bars, self.warmup)
                return

            await self._manage_positions(bar, atr, bands)
            await self._scan_entries(bar, atr, bands, trend, avg_volume)
        except Exception as exc:  # a strategy fault must not stop the data flow
            self._logger.exception("strategy error on bar end=%s", bar.end_ts)
            raise EngineError(f"strategy fault: {exc}") from exc

    # -- entries -----------------------------------------------------------
    async def _scan_entries(self, bar: Bar, atr: float, bands: Bands, trend: float,
                            avg_volume: Optional[float]) -> None:
        cfg = self._cfg
        if self._bars < self.warmup:
            return
        if not cfg.min_atr <= atr <= cfg.max_atr:
            return

        upper_channel = bands.mid + cfg.atr_channel_mult * atr
        lower_channel = bands.mid - cfg.atr_channel_mult * atr

        volume_confirms = True
        if avg_volume and avg_volume > 0:
            volume_confirms = bar.volume >= avg_volume * cfg.volume_mult

        trend_up = bar.close > trend and self._trend.slope_bps >= -0.5
        trend_down = bar.close < trend and self._trend.slope_bps <= 0.5

        long_break = (
            bar.close > bands.upper
            and bar.close > upper_channel
            and bar.high >= bands.upper
            and trend_up
            and volume_confirms
            and self._atr.expanding
        )
        short_break = (
            bar.close < bands.lower
            and bar.close < lower_channel
            and bar.low <= bands.lower
            and trend_down
            and volume_confirms
            and self._atr.expanding
        )

        if long_break and not short_break:
            await self._emit(bar, Side.LONG, atr, bands, "ATR+BB upside breakout")
        elif short_break and not long_break:
            await self._emit(bar, Side.SHORT, atr, bands, "ATR+BB downside breakout")

    async def _emit(self, bar: Bar, side: Side, atr: float, bands: Bands,
                    reason: str) -> None:
        cfg = self._cfg
        if cfg.one_position_per_side and self._portfolio.exposure_lots(side) > 0:
            return
        previous = self._last_entry_bar.get(side, 0.0)
        if bar.end_ts - previous < cfg.cooldown_bars * cfg.bar_seconds:
            return

        distance = bands.mid - bar.close if side is Side.LONG else bar.close - bands.mid
        signal = Signal(
            ts=bar.end_ts,
            symbol=bar.symbol,
            side=side,
            action=Action.OPEN,
            ref_price=bar.close,
            atr=atr,
            reason=reason,
            strength=round(abs(distance) / max(atr, TICK_SIZE), 3),
            bar_ts=bar.end_ts,
        )
        self._last_entry_bar[side] = bar.end_ts
        self.signals[side.value] += 1
        await self._bus.publish(Topic.SIGNAL, signal)
        log_event(
            self._logger, logging.INFO, "BREAKOUT SIGNAL",
            side=side.value, close=round(bar.close, 2), atr=round(atr, 3),
            bb_upper=round(bands.upper, 2), bb_mid=round(bands.mid, 2),
            bb_lower=round(bands.lower, 2), strength=signal.strength,
            volume=round(bar.volume, 2), reason=reason,
        )

    # -- exits / trailing --------------------------------------------------
    async def _manage_positions(self, bar: Bar, atr: float, bands: Bands) -> None:
        cfg = self._cfg
        for position in self._portfolio.open_positions:
            if position.stop <= 0:
                continue
            trail = (
                bar.close - atr * cfg.trail_atr_mult
                if position.side is Side.LONG
                else bar.close + atr * cfg.trail_atr_mult
            )
            if await self._portfolio.trail_stop(position.position_id, trail):
                self._logger.debug("trailed stop on %s -> %.2f",
                                   position.position_id, trail)

            reverted = (
                (position.side is Side.LONG and bar.close < bands.mid)
                or (position.side is Side.SHORT and bar.close > bands.mid)
            )
            if reverted and not position.exit_requested:
                if not await self._portfolio.request_exit(
                        position.position_id, "channel mid reversion"):
                    continue
                self.signals["EXIT"] += 1
                await self._bus.publish(Topic.SIGNAL, self._close_signal(position, bar, atr))
                log_event(self._logger, logging.INFO, "STRUCTURAL EXIT",
                          position=position.position_id, close=round(bar.close, 2),
                          mid=round(bands.mid, 2))

    def _close_signal(self, position: Position, bar: Bar, atr: float) -> Signal:
        return Signal(
            ts=bar.end_ts,
            symbol=position.symbol,
            side=position.side.opposite,
            action=Action.CLOSE,
            ref_price=bar.close,
            atr=atr,
            reason="channel mid reversion",
            strength=0.0,
            bar_ts=bar.end_ts,
            position_id=position.position_id,
        )


# --------------------------------------------------------------------------- #
# Broker adapters
# --------------------------------------------------------------------------- #
class BrokerAdapter:
    """Order routing interface shared by the simulator and live venues."""

    async def submit(self, order: Order, tick: Optional[Tick]) -> Fill:  # pragma: no cover
        raise NotImplementedError


class SimulatedGoldBroker(BrokerAdapter):
    """Latency, spread-crossing, slippage, partial fills and failure modes."""

    def __init__(self, rng: random.Random, logger: logging.Logger,
                 latency_ms: Tuple[int, int] = (8, 60),
                 fee_per_lot: float = 0.70,
                 reject_rate: float = 0.015,
                 partial_rate: float = 0.08) -> None:
        self._rng = rng
        self._logger = logger.getChild("broker")
        self._latency = latency_ms
        self._fee_per_lot = fee_per_lot
        self._reject_rate = max(0.0, min(reject_rate, 1.0))
        self._partial_rate = max(0.0, min(partial_rate, 1.0))
        self.submitted = 0
        self.filled = 0
        self.requotes = 0

    async def submit(self, order: Order, tick: Optional[Tick]) -> Fill:
        self.submitted += 1
        await asyncio.sleep(self._rng.uniform(*self._latency) / 1000.0)

        if tick is None or tick.bid <= 0:
            raise TransientBrokerError("no fresh quote")
        if self._rng.random() < self._reject_rate:
            if self._rng.random() < 0.6:
                self.requotes += 1
                raise TransientBrokerError("requote / gateway timeout")
            raise BrokerRejected("venue rejected order (stale quote / market closed)")

        # Order.side is the transaction direction: closing a short is a buy,
        # closing a long is a sell, so the action does not change the mapping.
        buys = order.side is Side.LONG
        base = tick.ask if buys else tick.bid

        slippage = self._rng.gauss(0.0, max(tick.spread, 0.05) * 0.35)
        if order.lots >= 5.0:  # market-impact on large clips
            slippage += self._rng.uniform(0.005, 0.04) * order.lots * (1.0 if buys else -1.0)
        price = round(max(TICK_SIZE, base + slippage), 2)

        lots = order.lots
        if self._rng.random() < self._partial_rate:
            chunk = math.floor(order.lots * self._rng.uniform(0.4, 0.85) / LOT_STEP) * LOT_STEP
            lots = max(chunk, MIN_LOT)

        signed_slip = (price - base) if buys else (base - price)
        self.filled += 1
        return Fill(
            client_order_id=order.client_order_id,
            ts=tick.ts,
            symbol=order.symbol,
            side=order.side,
            action=order.action,
            lots=round(lots, 8),
            price=price,
            fee=round(lots * self._fee_per_lot, 4),
            slippage=round(signed_slip, 4),
            position_id=None,
        )


class ExecutionAgent:
    """Routes approved orders, retries transient failures, manages brackets."""

    def __init__(self, bus: EventBus, portfolio: Portfolio, broker: BrokerAdapter,
                 logger: logging.Logger,
                 max_retries: int = 4, retry_base_s: float = 0.05,
                 submit_timeout_s: float = 2.0) -> None:
        self.name = "execution"
        self._bus = bus
        self._portfolio = portfolio
        self._broker = broker
        self._logger = logger.getChild("exec")
        self._max_retries = max_retries
        self._retry_base = retry_base_s
        self._submit_timeout = submit_timeout_s
        self._last_tick: Optional[Tick] = None
        self._seen_orders: Deque[str] = collections.deque(maxlen=4096)
        self._seen_ids: set[str] = set()
        self._pending: set[str] = set()
        self.roundtrip_ms: Deque[float] = collections.deque(maxlen=2048)
        self.attempts = 0
        self.executed = 0
        self.failed = 0

    def observe_tick(self, tick: Tick) -> None:
        self._last_tick = tick

    async def on_event(self, order: Order) -> None:
        if order.client_order_id in self._seen_ids:
            self._logger.warning("duplicate order %s dropped (idempotency)",
                                 order.client_order_id)
            return
        if order.client_order_id in self._pending:
            return
        if order.lots < MIN_LOT - 1e-12:
            await self._reject(order, "BAD_LOT", f"lot {order.lots} below minimum")
            return

        self._pending.add(order.client_order_id)
        token = CORRELATION.set(order.client_order_id)
        try:
            await self._route(order)
        except Exception as exc:  # unexpected broker defect
            self._logger.exception("execution fault on %s", order.client_order_id)
            await self._reject(order, "EXEC_FAULT", f"{type(exc).__name__}: {exc}")
        finally:
            self._pending.discard(order.client_order_id)
            CORRELATION.reset(token)

    async def _route(self, order: Order) -> None:
        started = time.monotonic()
        last_error: Optional[BaseException] = None

        for attempt in range(1, self._max_retries + 1):
            self.attempts += 1
            try:
                if self._last_tick is None:
                    raise TransientBrokerError("quote not ready")
                fill = await asyncio.wait_for(
                    self._broker.submit(order, self._last_tick),
                    timeout=self._submit_timeout,
                )
                await self._settle(order, fill, started, attempt)
                return
            except TransientBrokerError as exc:
                last_error = exc
                backoff = min(2.0, self._retry_base * (2 ** (attempt - 1)))
                backoff *= 0.5 + random.random()
                self._logger.warning(
                    "order %s attempt %d/%d transient failure: %s (retrying in %.3fs)",
                    order.client_order_id, attempt, self._max_retries, exc, backoff,
                )
                await asyncio.sleep(backoff)
            except (BrokerRejected, asyncio.TimeoutError, EngineError) as exc:
                last_error = exc
                break

        self.failed += 1
        await self._reject(order, "EXEC_FAILED",
                           f"{type(last_error).__name__}: {last_error}")
        self._logger.error("order %s permanently failed after %d attempts: %s",
                           order.client_order_id, self._max_retries, last_error)

    async def _settle(self, order: Order, fill: Fill, started: float,
                      attempt: int) -> None:
        self._remember(order.client_order_id)
        position_id = order.position_id
        if fill.action is Action.OPEN:
            position_id = new_id("pos")
        settled = dataclasses.replace(fill, position_id=position_id)

        position = await self._portfolio.apply_fill(settled, order.reason)

        if settled.action is Action.OPEN and position is not None:
            stop = self._reanchor_stop(order, settled)
            await self._portfolio.attach_bracket(
                position.position_id, stop, order.target or 0.0, order.atr,
            )

        roundtrip = (time.monotonic() - started) * 1000.0
        self.roundtrip_ms.append(roundtrip)
        self.executed += 1
        await self._bus.publish(Topic.FILL, settled)
        log_event(
            self._logger, logging.INFO, "FILL",
            order=settled.client_order_id, action=settled.action.value,
            side=settled.side.value, lots=settled.lots, price=settled.price,
            fee=settled.fee, slippage=settled.slippage, attempts=attempt,
            roundtrip_ms=round(roundtrip, 2),
            position=position.position_id if position else None,
        )

        if settled.action is Action.CLOSE and position is not None and not position.closed:
            remainder = dataclasses.replace(
                order,
                client_order_id=new_id("ord"),
                lots=position.lots,
                reason=f"{order.reason} (remainder of partial fill)",
                attempt=order.attempt + 1,
                position_id=position.position_id,
            )
            self._logger.warning("partial close on %s: queuing remainder %.2f lots",
                                 position.position_id, remainder.lots)
            await self._bus.publish(Topic.ORDER, remainder)

    @staticmethod
    def _reanchor_stop(order: Order, fill: Fill) -> float:
        """Tighten the bracket when the fill was better than planned, never widen it."""
        if order.stop is None:
            return 0.0
        if order.atr <= 0:
            return round(order.stop, 2)
        distance = abs(order.ref_price - order.stop)
        if order.side is Side.LONG:
            return round(min(order.stop, fill.price - distance), 2)
        return round(max(order.stop, fill.price + distance), 2)

    async def _reject(self, order: Order, code: str, message: str) -> None:
        self._remember(order.client_order_id)
        if order.action is Action.CLOSE and order.position_id:
            # Re-arm the position so the tick monitor can retry the exit.
            await self._portfolio.release_exit(order.position_id)
        await self._bus.publish(
            Topic.REJECT,
            Reject(order.client_order_id, time.time(), order.symbol, code, message),
        )

    def _remember(self, client_order_id: str) -> None:
        if len(self._seen_orders) == self._seen_orders.maxlen:
            self._seen_ids.discard(self._seen_orders[0])
        self._seen_orders.append(client_order_id)
        self._seen_ids.add(client_order_id)

    def stats(self) -> Dict[str, Any]:
        latencies = list(self.roundtrip_ms)
        return {
            "attempts": self.attempts,
            "executed": self.executed,
            "failed": self.failed,
            "p50_rt_ms": round(statistics.median(latencies), 2) if latencies else 0.0,
            "p95_rt_ms": round(percentile(latencies, 0.95), 2) if latencies else 0.0,
        }


def percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(q * len(ordered)) - 1))
    return ordered[index]


# --------------------------------------------------------------------------- #
# Market data feeds
# --------------------------------------------------------------------------- #
class BaseFeed:
    name = "base"

    async def stream(self, publish: Callable[[Tick], Coroutine[Any, Any, None]]) -> None:
        raise FeedError(f"{self.name} feed does not implement stream()")


_HOURS = [0, 3, 6, 8, 10, 12, 13, 14, 15, 16, 17, 18, 20, 22, 24]
_INTENSITY = [0.55, 0.5, 0.62, 0.95, 1.05, 1.2, 1.35, 1.62, 1.58, 1.5,
              1.32, 1.1, 0.9, 0.75, 0.6]


def session_intensity(ts: float) -> float:
    """Liquidity/volatility profile of the gold session (UTC hour)."""
    dt = datetime.fromtimestamp(ts, timezone.utc)
    hour = dt.hour + dt.minute / 60.0 + dt.second / 3600.0
    for i in range(1, len(_HOURS)):
        if hour <= _HOURS[i]:
            span = _HOURS[i] - _HOURS[i - 1]
            fraction = (hour - _HOURS[i - 1]) / span if span else 0.0
            return _INTENSITY[i - 1] + (_INTENSITY[i] - _INTENSITY[i - 1]) * fraction
    return _INTENSITY[-1]


class SimulatedGoldFeed(BaseFeed):
    """GARCH(1,1) conditional volatility + session seasonality + OU micro noise."""

    name = "simulated"

    def __init__(self, rng: random.Random, logger: logging.Logger,
                 start_price: float = 2_680.0, tick_step_s: float = 0.25,
                 speed: float = 25.0, seed_sigma: float = 2.0e-5) -> None:
        self._rng = rng
        self._logger = logger.getChild("feed")
        self._tick_step = tick_step_s
        self._speed = max(1.0, speed)
        self._sigma = seed_sigma
        # omega chosen so the GARCH unconditional sigma equals seed_sigma
        self._omega = seed_sigma ** 2 * 0.02
        self._alpha = 0.09
        self._beta = 0.89
        self._anchor = start_price
        self._ou = 0.0
        self._ts = datetime.now(timezone.utc).timestamp()
        self.ticks_published = 0

    async def stream(self, publish: Callable[[Tick], Coroutine[Any, Any, None]]) -> None:
        self._logger.info(
            "simulated feed online: anchor=%.2f step=%.2fs speed=x%.1f",
            self._anchor, self._tick_step, self._speed,
        )
        batch = max(1, int(round(1.0 / self._tick_step)))
        while True:
            for _ in range(batch):
                await publish(self._next_tick())
            await asyncio.sleep(1.0 / self._speed)

    def _next_tick(self) -> Tick:
        self._ts += self._tick_step
        self.ticks_published += 1
        intensity = session_intensity(self._ts)

        shock = self._rng.gauss(0.0, self._sigma * math.sqrt(intensity))
        self._sigma = math.sqrt(max(
            self._omega,
            self._alpha * shock ** 2 + self._beta * self._sigma ** 2,
        ))

        drift = 2e-8 * math.sin(self._ts / 43_000.0)
        jump = self._rng.gauss(0.0, self._sigma * 22.0) if self._rng.random() < 6e-5 else 0.0
        self._anchor *= math.exp(drift + shock + jump)

        self._ou += -0.35 * self._ou + self._rng.gauss(0.0, self._sigma * 0.45)
        mid = self._anchor * math.exp(self._ou)

        spread = max(
            TICK_SIZE,
            TYPICAL_SPREAD * (0.55 + 1.35 * intensity) * (1.0 + 6_000.0 * self._sigma),
        )
        half = spread / 2.0
        return Tick(ts=self._ts, bid=round(mid - half, 2), ask=round(mid + half, 2))


class WebSocketGoldFeed(BaseFeed):
    """Optional live adapter skeleton for a websocket OHLCV vendor.

    Enabled with `--feed live`; requires the `websockets` package and the
    XAU_WS_URL / XAU_API_KEY environment variables.
    """

    name = "websocket"

    def __init__(self, logger: logging.Logger, url: Optional[str] = None,
                 api_key: Optional[str] = None) -> None:
        self._logger = logger.getChild("feed.ws")
        self._url = url or os.getenv("XAU_WS_URL")
        self._api_key = api_key or os.getenv("XAU_API_KEY")
        self.ticks_published = 0

    async def stream(self, publish: Callable[[Tick], Coroutine[Any, Any, None]]) -> None:
        if not self._url:
            raise FeedError("live mode needs XAU_WS_URL (and optionally XAU_API_KEY)")
        try:
            import websockets
        except ImportError as exc:
            raise FeedError("optional dependency 'websockets' is not installed") from exc

        self._logger.info("connecting to live feed %s", self._url)
        connect = websockets.connect(
            self._url,
            **({"additional_headers": {"Authorization": f"Bearer {self._api_key}"}}
               if self._api_key else {}),
            ping_interval=20,
            close_timeout=5,
        )
        async with connect as ws:
            await ws.send(json.dumps({"op": "subscribe", "args": [SYMBOL]}))
            async for raw in ws:
                try:
                    message = json.loads(raw)
                    tick = Tick(
                        ts=float(message["ts"]),
                        bid=float(message["bid"]),
                        ask=float(message["ask"]),
                    )
                    if tick.bid <= 0 or tick.ask < tick.bid:
                        raise ValueError("implausible quote")
                except (KeyError, TypeError, ValueError) as exc:
                    self._logger.warning("dropping malformed frame (%s): %.120s", exc, raw)
                    continue
                self.ticks_published += 1
                await publish(tick)


# --------------------------------------------------------------------------- #
# Bar aggregation
# --------------------------------------------------------------------------- #
class BarBuilder:
    """Tick stream -> fixed-width OHLCV bars, emitted on bucket boundaries."""

    def __init__(self, bus: EventBus, logger: logging.Logger, bar_seconds: int,
                 symbol: str = SYMBOL) -> None:
        if bar_seconds <= 0:
            raise ValueError("bar_seconds must be positive")
        self.name = "bars"
        self._bus = bus
        self._logger = logger.getChild("bars")
        self._seconds = bar_seconds
        self._symbol = symbol
        self._current: Optional[Bar] = None
        self.emitted = 0

    def observe(self, tick: Tick) -> Optional[Bar]:
        bucket = math.floor(tick.ts / self._seconds) * self._seconds
        finished = None
        if self._current is not None and bucket > self._current.start_ts:
            finished = self._current
            self._current = None

        mid = tick.mid
        if self._current is None:
            self._current = Bar(
                symbol=self._symbol, start_ts=bucket, end_ts=bucket + self._seconds,
                open=mid, high=tick.ask, low=tick.bid, close=mid,
                volume=max(tick.spread, TICK_SIZE), ticks=1,
            )
            return finished

        c = self._current
        self._current = Bar(
            symbol=c.symbol, start_ts=c.start_ts, end_ts=c.end_ts, open=c.open,
            high=max(c.high, tick.ask), low=min(c.low, tick.bid), close=mid,
            volume=c.volume + max(tick.spread, TICK_SIZE), ticks=c.ticks + 1,
        )
        return finished

    async def on_tick(self, tick: Tick) -> None:
        finished = self.observe(tick)
        if finished is not None:
            self.emitted += 1
            self._logger.debug(
                "BAR o=%.2f h=%.2f l=%.2f c=%.2f vol=%.2f ticks=%d",
                finished.open, finished.high, finished.low, finished.close,
                finished.volume, finished.ticks,
            )
            await self._bus.publish(Topic.BAR, finished)

    async def flush(self) -> None:
        if self._current is not None:
            pending = self._current
            self._current = None
            await self._bus.publish(Topic.BAR, pending)


# --------------------------------------------------------------------------- #
# Performance
# --------------------------------------------------------------------------- #
class PerformanceTracker:
    """Sharpe on per-bar mark-to-market returns plus realised-trade statistics."""

    def __init__(self, portfolio: Portfolio, bar_seconds: int) -> None:
        self._portfolio = portfolio
        self._bar_seconds = bar_seconds
        self._prev_equity: Optional[float] = None
        self._returns: Deque[float] = collections.deque(maxlen=100_000)
        self.wins = 0
        self.losses = 0
        self.gross_win = 0.0
        self.gross_loss = 0.0
        self._scanned = 0

    def on_bar(self, bar: Bar) -> None:
        self._scan_trades()
        equity = self._portfolio.equity
        if self._prev_equity:
            self._returns.append((equity - self._prev_equity) / self._prev_equity)
        self._prev_equity = equity

    def _scan_trades(self) -> None:
        closed = self._portfolio.closed_trades
        while self._scanned < len(closed):
            trade = closed[self._scanned]
            self._scanned += 1
            net = trade.realized_pnl - trade.fees
            if net >= 0:
                self.wins += 1
                self.gross_win += net
            else:
                self.losses += 1
                self.gross_loss += abs(net)

    @property
    def sharpe(self) -> float:
        if len(self._returns) < 20:
            return 0.0
        values = list(self._returns)
        mean = statistics.fmean(values)
        deviation = statistics.pstdev(values)
        if deviation <= 1e-12:
            return 0.0
        bars_per_year = (3600.0 / self._bar_seconds) * 24.0 * 365.0
        return mean / deviation * math.sqrt(bars_per_year)

    def summary(self) -> Dict[str, Any]:
        self._scan_trades()
        trades = self.wins + self.losses
        profit_factor = (self.gross_win / self.gross_loss) if self.gross_loss > 0 else None
        return {
            "bars_observed": len(self._returns),
            "sharpe_annualized": round(self.sharpe, 2),
            "closed_trades": trades,
            "win_rate_pct": round(self.wins / trades * 100.0, 1) if trades else 0.0,
            "profit_factor": round(profit_factor, 2) if profit_factor else None,
            "avg_win": round(self.gross_win / self.wins, 2) if self.wins else 0.0,
            "avg_loss": round(self.gross_loss / self.losses, 2) if self.losses else 0.0,
        }


# --------------------------------------------------------------------------- #
# Resilience
# --------------------------------------------------------------------------- #
class CircuitBreaker:
    def __init__(self, logger: logging.Logger, failure_threshold: int = 8,
                 feed_threshold: int = 12) -> None:
        self._logger = logger.getChild("breaker")
        self._limits = {"feed": feed_threshold}
        self._default_limit = failure_threshold
        self._failures: Dict[str, int] = collections.Counter()
        self.tripped = False
        self.reason = ""

    def record_success(self, component: str) -> None:
        self._failures[component] = 0

    def record_failure(self, component: str, detail: str = "") -> bool:
        self._failures[component] += 1
        limit = self._limits.get(component, self._default_limit)
        self._logger.warning("failure %d/%d in %s :: %s",
                             self._failures[component], limit, component, detail)
        if self._failures[component] >= limit:
            self.trip(f"{component} exceeded {limit} consecutive failures: {detail}")
            return True
        return False

    def trip(self, reason: str) -> None:
        if not self.tripped:
            self.tripped = True
            self.reason = reason
            self._logger.critical("KILL SWITCH TRIPPED :: %s", reason)


class FeedAgent:
    """Exposes the feed's reconnect loop through the supervised run() interface."""

    def __init__(self, feed: BaseFeed, bus: EventBus, logger: logging.Logger) -> None:
        self.name = "feed"
        self._feed = feed
        self._bus = bus
        self._logger = logger.getChild("feed.agent")
        self.drops = 0

    async def run(self, shutdown: asyncio.Event) -> None:
        try:
            await self._feed.stream(lambda tick: self._bus.publish(Topic.TICK, tick))
        except asyncio.CancelledError:
            raise
        except EngineError as exc:
            self.drops += 1
            self._logger.warning("feed dropped (reconnect #%d): %s", self.drops, exc)
            raise
        # A live stream that ends cleanly is still a data outage for us.
        raise FeedError("feed stream closed unexpectedly")


class AgentRunner:
    """Supervises one agent loop with backoff restarts and breaker reporting."""

    def __init__(self, agent: Any, shutdown: asyncio.Event, breaker: CircuitBreaker,
                 logger: logging.Logger, max_restarts: int = 20) -> None:
        self._agent = agent
        self._shutdown = shutdown
        self._breaker = breaker
        self._logger = logger.getChild("supervisor")
        self._max_restarts = max_restarts
        self.restarts = 0

    async def run(self) -> None:
        name = self._agent.name
        while not self._shutdown.is_set():
            try:
                await self._agent.run(self._shutdown)
                self._breaker.record_success(name)
                return
            except asyncio.CancelledError:
                raise
            except KillSwitchTripped as exc:
                self._breaker.trip(f"{name}: {exc}")
                self._shutdown.set()
                return
            except EngineError as exc:
                self.restarts += 1
                detail = str(exc)
            except Exception as exc:  # noqa: BLE001 - last line of defence
                self.restarts += 1
                detail = f"uncaptured {type(exc).__name__}"
                self._logger.exception("uncaptured fault in %s", name)

            if self._breaker.record_failure(name, detail) or self.restarts > self._max_restarts:
                self._shutdown.set()
                return
            delay = min(5.0, 0.2 * (2 ** min(self.restarts, 5)))
            self._logger.error("restarting %s in %.2fs (restart #%d)", name, delay, self.restarts)
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                continue


class Agent:
    """Thin glue: consume one topic, dispatch to an async handler."""

    def __init__(self, name: str, queue: asyncio.Queue,
                 handler: Callable[[Any], Coroutine[Any, Any, None]],
                 logger: logging.Logger) -> None:
        self.name = name
        self._queue = queue
        self._handler = handler
        self._logger = logger.getChild(name)
        self.processed = 0
        self.errors = 0

    async def run(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise

            try:
                await self._handler(event)
                self.processed += 1
            except KillSwitchTripped:
                raise
            except EngineError:
                self.errors += 1
                raise
            except Exception as exc:  # noqa: BLE001
                self.errors += 1
                self._logger.exception("handler %s failed on %r", self.name, event)
                raise EngineError(f"{self.name} handler fault: {exc}") from exc

    def stats(self) -> Dict[str, int]:
        return {"processed": self.processed, "errors": self.errors}


# --------------------------------------------------------------------------- #
# Orchestrator
# --------------------------------------------------------------------------- #
class TradingEngine:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.logger = configure_logging(args.log_level, args.log_file, args.log_format)
        self.rng = random.Random(args.seed)
        self.shutdown = asyncio.Event()
        self.breaker = CircuitBreaker(self.logger)
        self.bus = EventBus(self.logger, maxsize=args.queue_size)

        self.portfolio = Portfolio(args.balance, args.leverage, self.logger)
        self.strategy = StrategyAgent(self.bus, self.portfolio,
                                      StrategyConfig(bar_seconds=args.bar_seconds),
                                      self.logger)
        self.risk = RiskAgent(self.bus, self.portfolio,
                              RiskConfig(risk_per_trade=args.risk_per_trade,
                                         leverage=args.leverage),
                              self.logger)
        broker = SimulatedGoldBroker(
            self.rng, self.logger,
            latency_ms=(args.latency_min_ms, args.latency_max_ms),
            reject_rate=args.reject_rate,
        )
        self.execution = ExecutionAgent(self.bus, self.portfolio, broker, self.logger)
        self.bars = BarBuilder(self.bus, self.logger, args.bar_seconds)
        self.performance = PerformanceTracker(self.portfolio, args.bar_seconds)
        self.feed: BaseFeed = (
            WebSocketGoldFeed(self.logger) if args.feed == "live"
            else SimulatedGoldFeed(self.rng, self.logger, start_price=args.start_price,
                                   speed=args.speed)
        )

        self._queues = {
            Topic.TICK: self.bus.subscribe(Topic.TICK, "tick-router"),
            Topic.BAR: self.bus.subscribe(Topic.BAR, "bar-router"),
            Topic.SIGNAL: self.bus.subscribe(Topic.SIGNAL, "risk"),
            Topic.ORDER: self.bus.subscribe(Topic.ORDER, "execution"),
            Topic.FILL: self.bus.subscribe(Topic.FILL, "audit"),
            Topic.REJECT: self.bus.subscribe(Topic.REJECT, "audit"),
        }
        self._tasks: List[asyncio.Task] = []
        self._agents: List[Any] = []
        self._started = time.monotonic()
        self._last_price = 0.0

    # -- stages ------------------------------------------------------------
    async def _on_tick(self, tick: Tick) -> None:
        self._last_price = tick.mid
        self.execution.observe_tick(tick)
        self.portfolio.mark_to_market(tick)
        await self._check_brackets(tick)
        await self.bars.on_tick(tick)

    async def _on_bar(self, bar: Bar) -> None:
        self.performance.on_bar(bar)
        await self.strategy.on_event(bar)

    async def _check_brackets(self, tick: Tick) -> None:
        """Intra-bar stop/target monitoring; exits go through the risk gate."""
        for position in self.portfolio.open_positions:
            if position.exit_requested:
                continue
            exit_price = tick.bid if position.side is Side.LONG else tick.ask
            favorable = position.side is Side.LONG

            reason = None
            if position.stop > 0 and (
                (favorable and exit_price <= position.stop)
                or (not favorable and exit_price >= position.stop)
            ):
                locked_in = (
                    (favorable and position.stop > position.entry_price)
                    or (not favorable and 0 < position.stop < position.entry_price)
                )
                reason = "trailing-stop" if locked_in else "stop-loss"
            elif position.target > 0 and (
                (favorable and exit_price >= position.target)
                or (not favorable and exit_price <= position.target)
            ):
                reason = "take-profit"

            if reason:
                await self._publish_exit(position, exit_price, tick.ts, reason)

    async def _publish_exit(self, position: Position, price: float, ts: float,
                            reason: str) -> None:
        """Claim the position's single exit slot, then route it through risk."""
        if not await self.portfolio.request_exit(position.position_id, reason):
            return
        self.strategy.signals["EXIT"] += 1
        await self.bus.publish(Topic.SIGNAL, Signal(
            ts=ts, symbol=position.symbol, side=position.side.opposite,
            action=Action.CLOSE, ref_price=price,
            atr=position.atr_at_entry or (self.strategy.last_atr or 0.0),
            reason=reason, strength=0.0, bar_ts=ts,
            position_id=position.position_id,
        ))
        log_event(self.logger.getChild("risk"), logging.INFO, "EXIT TRIGGERED",
                  position=position.position_id, reason=reason,
                  price=round(price, 2), stop=position.stop, target=position.target)

    async def _audit_fill(self, fill: Fill) -> None:
        log_event(self.logger.getChild("audit"), logging.DEBUG, "fill audited",
                  order=fill.client_order_id, lots=fill.lots, price=fill.price,
                  fee=fill.fee, position=fill.position_id)

    async def _audit_reject(self, reject: Reject) -> None:
        log_event(self.logger.getChild("audit"), logging.WARNING, "order rejected",
                  code=reject.code, order=reject.client_order_id, message=reject.message)

    # -- housekeeping ------------------------------------------------------
    async def _heartbeat(self, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=self.args.heartbeat_s)
                return
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                raise
            try:
                account = self.portfolio.snapshot()
                performance = self.performance.summary()
                self.logger.info(
                    "HEARTBEAT px=%.2f equity=$%s rt=$%s dd=%s%% open=%d lots=%.2f "
                    "| ticks=%d bars=%d sig=%d ord=%d fill=%d "
                    "| sharpe=%s trades=%d win%%=%s | bus=%s | exec=%s",
                    self._last_price, f"{account['equity']:,.2f}",
                    f"{account['balance']:,.2f}", f"{account['max_drawdown_pct']:.2f}",
                    account["open_positions"], account["gross_lots"],
                    getattr(self.feed, "ticks_published", 0), self.bars.emitted,
                    sum(self.strategy.signals.values()), self.risk.approved,
                    self.execution.executed,
                    performance["sharpe_annualized"], performance["closed_trades"],
                    performance["win_rate_pct"], self.bus.stats(),
                    self.execution.stats(),
                )
            except Exception:  # never let reporting break the loop
                self.logger.exception("heartbeat failed")

    async def _run_for(self, seconds: int) -> None:
        if seconds <= 0:
            await asyncio.Event().wait()
            return
        try:
            await asyncio.wait_for(self.shutdown.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            self.logger.info("run window of %ds elapsed", seconds)
            self.shutdown.set()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for name in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, name, None)
            if sig is None:
                continue
            try:
                loop.add_signal_handler(sig, self._on_posix_signal, name)
            except (NotImplementedError, RuntimeError):
                signal.signal(sig, lambda *_: self._on_posix_signal(name))

    def _on_posix_signal(self, name: str) -> None:
        if self.shutdown.is_set():
            self.logger.warning("second %s received; exiting immediately", name)
            os._exit(1)
        self.logger.warning("received %s -> graceful shutdown", name)
        self.shutdown.set()

    # -- lifecycle ---------------------------------------------------------
    async def run(self) -> Dict[str, Any]:
        args = self.args
        self.logger.info(
            "ENGINE v%s | feed=%s balance=$%s risk/trade=%.3f%% leverage=%.0fx "
            "bar=%ds speed=x%.1f window=%ds queue=%d",
            __version__, self.feed.name, f"{args.balance:,.2f}",
            args.risk_per_trade * 100, args.leverage, args.bar_seconds,
            args.speed, args.seconds, args.queue_size,
        )
        self._install_signal_handlers()

        supervised: List[Any] = [
            FeedAgent(self.feed, self.bus, self.logger),
            Agent("tick-router", self._queues[Topic.TICK], self._on_tick, self.logger),
            Agent("bar-router", self._queues[Topic.BAR], self._on_bar, self.logger),
            Agent("risk", self._queues[Topic.SIGNAL], self.risk.on_event, self.logger),
            Agent("execution", self._queues[Topic.ORDER], self.execution.on_event,
                  self.logger),
            Agent("fill-audit", self._queues[Topic.FILL], self._audit_fill, self.logger),
            Agent("reject-audit", self._queues[Topic.REJECT], self._audit_reject,
                  self.logger),
        ]
        self._agents = supervised
        for agent in supervised:
            self._tasks.append(asyncio.create_task(
                AgentRunner(agent, self.shutdown, self.breaker, self.logger).run(),
                name=f"agent:{agent.name}",
            ))
        self._tasks.append(asyncio.create_task(self._heartbeat(self.shutdown),
                                               name="heartbeat"))
        self._tasks.append(asyncio.create_task(self._run_for(args.seconds), name="timer"))

        await self.shutdown.wait()
        self.logger.info("draining %d tasks after shutdown request", len(self._tasks))
        for task in self._tasks:
            task.cancel()
        results = await asyncio.gather(*self._tasks, return_exceptions=True)
        for task, result in zip(self._tasks, results):
            if isinstance(result, BaseException) and not isinstance(
                result, (asyncio.CancelledError,)
            ):
                self.logger.error("task %s exited with %r", task.get_name(), result)

        depth = self.bus.queue_depth()
        if any(depth.values()):
            self.logger.info("residual undelivered events at shutdown: %s", depth)

        report = self.final_report()
        self.logger.info("SESSION REPORT\n%s",
                         json.dumps(report, indent=2, ensure_ascii=False, default=str))
        return report

    # -- reporting ---------------------------------------------------------
    def final_report(self) -> Dict[str, Any]:
        account = self.portfolio.snapshot()
        return {
            "version": __version__,
            "feed": self.feed.name,
            "wall_clock_s": round(time.monotonic() - self._started, 2),
            "account": account,
            "performance": self.performance.summary(),
            "strategy": {
                "bars": self.bars.emitted,
                "signals": dict(self.strategy.signals),
                "last_atr": round(self.strategy.last_atr or 0.0, 3),
                "warmup_bars": self.strategy.warmup,
            },
            "risk": self.risk.stats(),
            "execution": self.execution.stats(),
            "agents": {
                agent.name: agent.stats()
                for agent in self._agents if hasattr(agent, "stats")
            },
            "bus": {**self.bus.stats(), "depth": self.bus.queue_depth()},
            "open_positions": [
                {
                    "id": p.position_id, "side": p.side.value, "lots": p.lots,
                    "entry": p.entry_price, "stop": p.stop, "target": p.target,
                }
                for p in self.portfolio.open_positions
            ],
            "kill_switch": {"tripped": self.breaker.tripped, "reason": self.breaker.reason},
        }


# --------------------------------------------------------------------------- #
# CLI entry point
# --------------------------------------------------------------------------- #
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="engine.py",
        description="Async XAUUSD ATR/Bollinger breakout trading engine",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--feed", choices=["simulated", "live"], default="simulated")
    parser.add_argument("--seconds", type=int, default=600,
                        help="run window in wall-clock seconds (0 = until interrupted)")
    parser.add_argument("--balance", type=float, default=DEFAULT_BALANCE)
    parser.add_argument("--risk-per-trade", type=float, default=DEFAULT_RISK_PER_TRADE,
                        help="fraction of equity risked per trade (0.005 = 0.5%%)")
    parser.add_argument("--leverage", type=float, default=DEFAULT_LEVERAGE)
    parser.add_argument("--bar-seconds", type=int, default=60, help="OHLCV bar width")
    parser.add_argument("--speed", type=float, default=25.0,
                        help="simulated seconds per wall-clock second")
    parser.add_argument("--start-price", type=float, default=2_680.0)
    parser.add_argument("--reject-rate", type=float, default=0.015,
                        help="simulated venue failure probability per order")
    parser.add_argument("--latency-min-ms", type=int, default=8)
    parser.add_argument("--latency-max-ms", type=int, default=60)
    parser.add_argument("--queue-size", type=int, default=8192)
    parser.add_argument("--heartbeat-s", type=float, default=15.0)
    parser.add_argument("--seed", type=int, default=None,
                        help="RNG seed (best-effort reproducibility)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--log-file", default=os.path.join("logs", "engine.log"))
    parser.add_argument("--log-format", default="text", choices=["text", "json"])
    return parser


async def run_async(args: argparse.Namespace) -> Tuple[int, Dict[str, Any]]:
    engine = TradingEngine(args)
    report = await engine.run()
    net = report["account"]["equity"] - args.balance
    engine.logger.info(
        "session finished: net PnL %s$%.2f (%.3f%%) on $%.2f | trades=%d | sharpe=%s",
        "+" if net >= 0 else "-", abs(net), abs(net) / args.balance * 100.0,
        args.balance, report["performance"]["closed_trades"],
        report["performance"]["sharpe_annualized"],
    )
    return (0 if not report["kill_switch"]["tripped"] else 3), report


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if args.risk_per_trade <= 0 or args.risk_per_trade > 0.05:
        raise SystemExit("risk-per-trade must be in (0, 0.05]")
    if args.seed is not None:
        random.seed(args.seed)
    try:
        exit_code, _ = asyncio.run(run_async(args))
        return exit_code
    except KeyboardInterrupt:
        logging.getLogger("xau").warning("interrupted from keyboard")
        return 130
    except EngineError as exc:
        logging.getLogger("xau").critical("fatal engine error: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
