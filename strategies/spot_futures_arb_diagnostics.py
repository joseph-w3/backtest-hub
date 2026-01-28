# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

from collections import deque
from datetime import timedelta
from decimal import Decimal

from quant_trade_v1.common.events import TimeEvent
from quant_trade_v1.core.datetime import unix_nanos_to_dt
from quant_trade_v1.config import PositiveFloat
from quant_trade_v1.config import StrategyConfig
from quant_trade_v1.core.rust.common import LogColor
from quant_trade_v1.model.book import OrderBook
from quant_trade_v1.model.data import FundingRateUpdate
from quant_trade_v1.model.data import MarkPriceUpdate
from quant_trade_v1.model.data import OrderBookDeltas
from quant_trade_v1.model.data import OrderBookDepth10
from quant_trade_v1.model.data import TradeTick
from quant_trade_v1.model.enums import BookType
from quant_trade_v1.model.enums import OrderSide
from quant_trade_v1.model.enums import book_type_from_str
from quant_trade_v1.model.identifiers import InstrumentId
from quant_trade_v1.model.identifiers import Venue
from quant_trade_v1.model.instruments import Instrument
from quant_trade_v1.trading.strategy import Strategy


# *** THIS IS A TEST STRATEGY WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY. ***


class SpotFuturesArbDiagnosticsConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``SpotFuturesArbDiagnostics`` instances.

    Parameters
    ----------
    spot_instrument_ids : list[InstrumentId]
        Spot instrument IDs to subscribe and trade.
    futures_instrument_ids : list[InstrumentId]
        Futures instrument IDs to subscribe and trade.
    book_type : str, default 'L2_MBP'
        The order book type for subscriptions.
    subscribe_order_book_deltas : bool, default True
        If order book deltas should be subscribed to.
    subscribe_order_book_depth : bool, default False
        If order book depth snapshots should be subscribed to (depth=10).
    subscribe_trade_ticks : bool, default True
        If trade ticks should be subscribed to.
    subscribe_funding_rates : bool, default True
        If funding rate updates should be subscribed to (futures only).
    subscribe_mark_prices : bool, default True
        If mark price updates should be subscribed to (futures only).
    order_quantity : Decimal, default 5000
        Fixed order quantity for test orders.
    log_interval_minutes : PositiveFloat, default 10.0
        Interval in minutes for logging recent data snapshots.
    half_hour_order_minutes : PositiveFloat, default 30.0
        Interval in minutes for the periodic test order.
    hourly_order_minutes : PositiveFloat, default 60.0
        Interval in minutes for the ACTUSDT/DOTUSDT test orders.
    close_positions_minutes : PositiveFloat, default 120.0
        Interval in minutes for closing all positions.
    dry_run : bool, default False
        If dry run mode is active. If True, no orders are submitted.

    """

    spot_instrument_ids: list[InstrumentId]
    futures_instrument_ids: list[InstrumentId]
    book_type: str = "L2_MBP"
    subscribe_order_book_deltas: bool = True
    subscribe_order_book_depth: bool = False
    subscribe_trade_ticks: bool = True
    subscribe_funding_rates: bool = True
    subscribe_mark_prices: bool = True
    order_quantity: Decimal = Decimal("5000")
    log_interval_minutes: PositiveFloat = 10.0
    half_hour_order_minutes: PositiveFloat = 30.0
    hourly_order_minutes: PositiveFloat = 60.0
    close_positions_minutes: PositiveFloat = 120.0
    dry_run: bool = False


class SpotFuturesArbDiagnostics(Strategy):
    TIMER_LOG = "spot_futures_log_snapshot"
    TIMER_HALF_HOUR = "spot_futures_half_hour_order"
    TIMER_HOURLY = "spot_futures_hourly_order"
    TIMER_CLOSE = "spot_futures_close_positions"

    def __init__(self, config: SpotFuturesArbDiagnosticsConfig) -> None:
        super().__init__(config)
        self.book_type: BookType = book_type_from_str(self.config.book_type)

        self._spot_instrument_ids: list[InstrumentId] = []
        self._futures_instrument_ids: list[InstrumentId] = []
        self._all_instrument_ids: list[InstrumentId] = []
        self._instruments: dict[InstrumentId, Instrument] = {}
        self._spot_venue: Venue | None = None
        self._futures_venue: Venue | None = None
        self._venues: list[Venue] = []

        self._recent_orderbooks: dict[InstrumentId, deque[dict]] = {}
        self._recent_trades: dict[InstrumentId, deque[dict]] = {}
        self._recent_funding: dict[InstrumentId, deque[dict]] = {}
        self._recent_mark_prices: dict[InstrumentId, deque[dict]] = {}

        self._account_balance_logged: set[str] = set()

    def on_start(self) -> None:
        self._spot_instrument_ids = list(self.config.spot_instrument_ids)
        self._futures_instrument_ids = list(self.config.futures_instrument_ids)
        self._all_instrument_ids = self._spot_instrument_ids + self._futures_instrument_ids

        if not self._all_instrument_ids:
            self.log.error("No instruments configured for spot/futures diagnostics.")
            self.stop()
            return

        missing = []
        for instrument_id in self._all_instrument_ids:
            instrument = self.cache.instrument(instrument_id)
            if instrument is None:
                missing.append(instrument_id)
            else:
                self._instruments[instrument_id] = instrument

        if missing:
            self.log.error(f"Missing instruments in cache: {missing}")
            self.stop()
            return

        spot_venues: list[Venue] = []
        spot_seen: set[str] = set()
        for instrument_id in self._spot_instrument_ids:
            venue = instrument_id.venue
            venue_name = venue.value
            if venue_name not in spot_seen:
                spot_venues.append(venue)
                spot_seen.add(venue_name)

        futures_venues: list[Venue] = []
        futures_seen: set[str] = set()
        for instrument_id in self._futures_instrument_ids:
            venue = instrument_id.venue
            venue_name = venue.value
            if venue_name not in futures_seen:
                futures_venues.append(venue)
                futures_seen.add(venue_name)

        if len(spot_venues) > 1:
            self.log.warning(
                f"Multiple spot venues detected: {[venue.value for venue in spot_venues]}",
            )
        if len(futures_venues) > 1:
            self.log.warning(
                f"Multiple futures venues detected: {[venue.value for venue in futures_venues]}",
            )
        self._spot_venue = spot_venues[0] if spot_venues else None
        self._futures_venue = futures_venues[0] if futures_venues else None

        venues: list[Venue] = []
        seen_values: set[str] = set()
        for venue in (self._spot_venue, self._futures_venue):
            if venue is None:
                continue
            venue_value = venue.value
            if venue_value in seen_values:
                continue
            venues.append(venue)
            seen_values.add(venue_value)
        self._venues = venues
        self._initialize_recent_storage()
        self._subscribe_streams()
        self._setup_timers()
        self._log_account_balance_once()

    def on_reset(self) -> None:
        self._initialize_recent_storage()
        self._account_balance_logged.clear()

    def on_stop(self) -> None:
        self.clock.cancel_timers()

    def on_timer(self, event: TimeEvent) -> None:
        if event.name == self.TIMER_LOG:
            self._log_recent_snapshots(event)
        elif event.name == self.TIMER_HALF_HOUR:
            self._submit_half_hour_orders(event)
        elif event.name == self.TIMER_HOURLY:
            self._submit_hourly_orders(event)
        elif event.name == self.TIMER_CLOSE:
            self._close_all_positions(event)

    def on_order_book_deltas(self, deltas: OrderBookDeltas) -> None:
        self._record_order_book_snapshot(deltas.instrument_id, deltas.ts_event)

    def on_order_book_depth(self, depth: OrderBookDepth10) -> None:
        self._record_order_book_snapshot(depth.instrument_id, depth.ts_event)

    def on_order_book(self, order_book: OrderBook) -> None:
        ts_event = getattr(order_book, "ts_event", self.clock.timestamp_ns())
        self._record_order_book_snapshot(order_book.instrument_id, ts_event, order_book=order_book)

    def on_trade_tick(self, trade: TradeTick) -> None:
        payload = TradeTick.to_dict(trade)
        self._record_recent(self._recent_trades, trade.instrument_id, trade.ts_event, payload)

    def on_funding_rate(self, funding_rate: FundingRateUpdate) -> None:
        payload = FundingRateUpdate.to_dict(funding_rate)
        self._record_recent(
            self._recent_funding,
            funding_rate.instrument_id,
            funding_rate.ts_event,
            payload,
        )

    def on_mark_price(self, mark_price: MarkPriceUpdate) -> None:
        payload = MarkPriceUpdate.to_dict(mark_price)
        self._record_recent(
            self._recent_mark_prices,
            mark_price.instrument_id,
            mark_price.ts_event,
            payload,
        )

    def _initialize_recent_storage(self) -> None:
        self._recent_orderbooks = {
            instrument_id: deque(maxlen=10) for instrument_id in self._all_instrument_ids
        }
        self._recent_trades = {
            instrument_id: deque(maxlen=10) for instrument_id in self._all_instrument_ids
        }
        self._recent_funding = {
            instrument_id: deque(maxlen=10) for instrument_id in self._futures_instrument_ids
        }
        self._recent_mark_prices = {
            instrument_id: deque(maxlen=10) for instrument_id in self._futures_instrument_ids
        }

    def _subscribe_streams(self) -> None:
        for instrument_id in self._all_instrument_ids:
            if self.config.subscribe_order_book_deltas:
                self.subscribe_order_book_deltas(instrument_id, self.book_type)
            if self.config.subscribe_order_book_depth:
                self.subscribe_order_book_depth(
                    instrument_id=instrument_id,
                    book_type=self.book_type,
                    depth=10,
                )
            if self.config.subscribe_trade_ticks:
                self.subscribe_trade_ticks(instrument_id)

        if self.config.subscribe_funding_rates:
            for instrument_id in self._futures_instrument_ids:
                self.subscribe_funding_rates(instrument_id)

        if self.config.subscribe_mark_prices:
            for instrument_id in self._futures_instrument_ids:
                self.subscribe_mark_prices(instrument_id)

    def _setup_timers(self) -> None:
        self.clock.set_timer(
            name=self.TIMER_LOG,
            interval=timedelta(minutes=self.config.log_interval_minutes),
            callback=self.on_timer,
        )
        self.clock.set_timer(
            name=self.TIMER_HALF_HOUR,
            interval=timedelta(minutes=self.config.half_hour_order_minutes),
            callback=self.on_timer,
        )
        self.clock.set_timer(
            name=self.TIMER_HOURLY,
            interval=timedelta(minutes=self.config.hourly_order_minutes),
            callback=self.on_timer,
        )
        self.clock.set_timer(
            name=self.TIMER_CLOSE,
            interval=timedelta(minutes=self.config.close_positions_minutes),
            callback=self.on_timer,
        )

    def _record_order_book_snapshot(
        self,
        instrument_id: InstrumentId,
        ts_event: int,
        order_book: OrderBook | None = None,
    ) -> None:
        book = order_book or self.cache.order_book(instrument_id)
        if book is None:
            return
        payload = {"top5": book.pprint(5)}
        self._record_recent(self._recent_orderbooks, instrument_id, ts_event, payload)

    @staticmethod
    def _record_recent(
        store: dict[InstrumentId, deque[dict]],
        instrument_id: InstrumentId,
        ts_event: int,
        payload: dict,
    ) -> None:
        if instrument_id not in store:
            store[instrument_id] = deque(maxlen=10)
        store[instrument_id].append({"ts_event": ts_event, "data": payload})

    def _log_recent_snapshots(self, event: TimeEvent) -> None:
        event_time = unix_nanos_to_dt(event.ts_event)
        self.log.info(
            f"Snapshot timer fired at {event_time}",
            color=LogColor.YELLOW,
        )

        for instrument_id in self._all_instrument_ids:
            self.log.info(f"Recent data for {instrument_id}", color=LogColor.CYAN)
            self._log_recent_stream(
                label="orderbook",
                entries=self._recent_orderbooks.get(instrument_id, deque()),
            )
            self._log_recent_stream(
                label="trade",
                entries=self._recent_trades.get(instrument_id, deque()),
            )
            if instrument_id in self._futures_instrument_ids:
                self._log_recent_stream(
                    label="funding_rate",
                    entries=self._recent_funding.get(instrument_id, deque()),
                )
                self._log_recent_stream(
                    label="mark_price",
                    entries=self._recent_mark_prices.get(instrument_id, deque()),
                )

        self._log_account_state_snapshot()
        self._log_free_balance_snapshot()
        self._log_liquidation_buffer_snapshot()

    def _log_recent_stream(self, label: str, entries: deque[dict]) -> None:
        if not entries:
            self.log.info(f"{label}: no data collected yet.")
            return

        timestamps = [entry["ts_event"] for entry in entries]
        intervals = [
            (timestamps[idx] - timestamps[idx - 1]) / 1_000_000_000
            for idx in range(1, len(timestamps))
        ]
        details = [
            {
                "ts": str(unix_nanos_to_dt(entry["ts_event"])),
                "data": entry["data"],
            }
            for entry in entries
        ]
        self.log.info(
            f"{label}: count={len(entries)} intervals_sec={intervals}\n{details}",
            LogColor.CYAN,
        )

    def _submit_half_hour_orders(self, event: TimeEvent) -> None:
        event_time = unix_nanos_to_dt(event.ts_event)
        self.log.info(
            f"Half-hour order timer fired at {event_time}",
            color=LogColor.YELLOW,
        )

        pair = self._first_available_pair()
        if pair is None:
            self.log.warning("No spot/futures pair available for half-hour orders.")
            return

        spot_id, futures_id = pair
        self._submit_spot_futures_orders(
            spot_id,
            futures_id,
            reason="half-hour",
        )
        self._log_account_state_snapshot()
        self._log_free_balance_snapshot()

    def _submit_hourly_orders(self, event: TimeEvent) -> None:
        event_time = unix_nanos_to_dt(event.ts_event)
        self.log.info(
            f"Hourly order timer fired at {event_time}",
            color=LogColor.YELLOW,
        )

        for symbol in ("ACTUSDT", "DOTUSDT"):
            pair = self._pair_for_symbol(symbol)
            if pair is None:
                self.log.warning(f"Pair {symbol} not configured for hourly order.")
                continue
            spot_id, futures_id = pair
            self._submit_spot_futures_orders(
                spot_id,
                futures_id,
                reason=f"hourly-{symbol}",
            )

        self._log_account_state_snapshot()
        self._log_free_balance_snapshot()

    def _close_all_positions(self, event: TimeEvent) -> None:
        event_time = unix_nanos_to_dt(event.ts_event)
        self.log.info(
            f"Close positions timer fired at {event_time}",
            color=LogColor.YELLOW,
        )

        for instrument_id in self._all_instrument_ids:
            self.close_all_positions(instrument_id)

        self._log_account_state_snapshot()
        self._log_free_balance_snapshot()

    def _submit_spot_futures_orders(
        self,
        spot_id: InstrumentId,
        futures_id: InstrumentId,
        reason: str,
    ) -> None:
        self._submit_order(spot_id, OrderSide.BUY, reason=f"{reason}-spot")
        self._submit_order(futures_id, OrderSide.SELL, reason=f"{reason}-futures")

    def _submit_order(self, instrument_id: InstrumentId, side: OrderSide, reason: str) -> None:
        instrument = self._instruments.get(instrument_id)
        if instrument is None:
            self.log.error(f"Instrument not available for order: {instrument_id}")
            return

        order = self.order_factory.market(
            instrument_id=instrument_id,
            order_side=side,
            quantity=instrument.make_qty(self.config.order_quantity),
        )
        self.log.info(f"Submitting order ({reason}): {order}", color=LogColor.BLUE)
        if self.config.dry_run:
            self.log.warning("Dry run mode active; order not submitted.")
            return
        self.submit_order(order)

    def _first_available_pair(self) -> tuple[InstrumentId, InstrumentId] | None:
        for spot_id in self._spot_instrument_ids:
            symbol = self._instrument_symbol(spot_id)
            pair = self._pair_for_symbol(symbol)
            if pair is not None:
                return pair
        return None

    def _pair_for_symbol(
        self,
        symbol: str,
    ) -> tuple[InstrumentId, InstrumentId] | None:
        spot_id = self._find_instrument(self._spot_instrument_ids, symbol)
        futures_id = self._find_instrument(self._futures_instrument_ids, symbol)
        if spot_id is None or futures_id is None:
            return None
        return spot_id, futures_id

    @staticmethod
    def _find_instrument(
        instruments: list[InstrumentId],
        symbol: str,
    ) -> InstrumentId | None:
        for instrument_id in instruments:
            if SpotFuturesArbDiagnostics._instrument_symbol(instrument_id) == symbol:
                return instrument_id
        return None

    @staticmethod
    def _instrument_symbol(instrument_id: InstrumentId) -> str:
        raw_symbol = str(instrument_id).split(".")[0]
        if raw_symbol.endswith("-PERP"):
            return raw_symbol[:-5]
        return raw_symbol

    def _log_account_balance_once(self) -> None:
        if not self._venues:
            return

        for venue in self._venues:
            venue_name = venue.value
            if venue_name in self._account_balance_logged:
                continue
            if self.cache.account_for_venue(venue) is None:
                continue
            account = self.portfolio.account(venue)
            if account is None:
                continue
            balances = account.balances_total()
            self.log.info(f"Account balance totals ({venue}): {balances}")
            self._account_balance_logged.add(venue_name)

    def _log_account_state_snapshot(self) -> None:
        if not self._venues:
            return

        for venue in self._venues:
            positions = self.cache.positions_open(venue=venue)
            if not positions:
                self.log.info(f"Account state snapshot ({venue}): no open positions.")
                continue

            positions_by_instrument: dict[InstrumentId, list] = {}
            for position in positions:
                positions_by_instrument.setdefault(position.instrument_id, []).append(position)

            snapshot = []
            for instrument_id, instrument_positions in positions_by_instrument.items():
                net_position = self.portfolio.net_position(instrument_id)
                unrealized_pnl = self.portfolio.unrealized_pnl(instrument_id)
                snapshot.append(
                    {
                        "instrument_id": f"{instrument_id}",
                        "net_position": str(net_position),
                        "unrealized_pnl": str(unrealized_pnl) if unrealized_pnl is not None else None,
                        "positions": [position.to_dict() for position in instrument_positions],
                    },
                )

            self.log.info(
                f"Account state snapshot ({venue}):\n{snapshot}",
                LogColor.CYAN,
            )

    def _log_free_balance_snapshot(self) -> None:
        if not self._venues:
            return

        for venue in self._venues:
            account = self.portfolio.account(venue)
            if account is None:
                self.log.warning(f"Free balance snapshot ({venue}): account not available.")
                continue
            free_balances = account.balances_free()
            snapshot = {currency.code: str(amount) for currency, amount in free_balances.items()}
            self.log.info(
                f"Free balance snapshot ({venue}) (after margin/locks): {snapshot}",
                LogColor.CYAN,
            )

    def _log_liquidation_buffer_snapshot(self) -> None:
        if not self._venues:
            return

        for venue in self._venues:
            account = self.portfolio.account(venue)
            if account is None:
                self.log.warning(f"Liquidation buffer snapshot ({venue}): account not available.")
                continue
            if not account.is_margin_account:
                self.log.info(
                    f"Liquidation buffer snapshot ({venue}): not a margin account.",
                )
                continue
            positions = self.cache.positions_open(venue=venue)
            if not positions:
                self.log.info(f"Liquidation buffer snapshot ({venue}): no open positions.")
                continue

            free_balances = account.balances_free()
            snapshot = []
            instrument_ids = {position.instrument_id for position in positions}
            for instrument_id in instrument_ids:
                margin_balance = account.margin(instrument_id)
                if margin_balance is None:
                    snapshot.append(
                        {
                            "instrument_id": f"{instrument_id}",
                            "maintenance_margin": None,
                            "free_balance": None,
                            "buffer": None,
                            "buffer_ratio": None,
                        },
                    )
                    continue

                free_balance = free_balances.get(margin_balance.currency)
                if free_balance is None:
                    snapshot.append(
                        {
                            "instrument_id": f"{instrument_id}",
                            "maintenance_margin": str(margin_balance.maintenance),
                            "free_balance": None,
                            "buffer": None,
                            "buffer_ratio": None,
                        },
                    )
                    continue

                maintenance = margin_balance.maintenance.as_decimal()
                buffer_value = free_balance.as_decimal() - maintenance
                buffer_ratio = None
                if maintenance > 0:
                    buffer_ratio = buffer_value / maintenance

                snapshot.append(
                    {
                        "instrument_id": f"{instrument_id}",
                        "maintenance_margin": str(margin_balance.maintenance),
                        "free_balance": str(free_balance),
                        "buffer": f"{buffer_value} {margin_balance.currency.code}",
                        "buffer_ratio": str(buffer_ratio) if buffer_ratio is not None else None,
                    },
                )

            self.log.info(
                f"Liquidation buffer snapshot ({venue}) (approximate):\n{snapshot}",
                LogColor.CYAN,
            )
