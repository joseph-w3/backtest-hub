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

from quant_trade_v1.config import StrategyConfig
from quant_trade_v1.model.enums import BookType
from quant_trade_v1.model.enums import book_type_from_str
from quant_trade_v1.model.identifiers import InstrumentId
from quant_trade_v1.trading.strategy import Strategy

from .utils.common import DemoUtils

# *** THIS IS A MINIMAL TEST STRATEGY FOR STRATEGY-BUNDLE VALIDATION ONLY. ***


class SimpleBundleDemoConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``SimpleBundleDemo`` instances.

    Parameters
    ----------
    spot_instrument_ids : list[InstrumentId]
        Spot instrument IDs to subscribe and trade.
    futures_instrument_ids : list[InstrumentId]
        Futures instrument IDs to subscribe and trade.
    book_type : str, default 'L2_MBP'
        The order book type for subscriptions.
    log_on_start : bool, default True
        If a startup log should be emitted.

    """

    spot_instrument_ids: list[InstrumentId]
    futures_instrument_ids: list[InstrumentId]
    book_type: str = "L2_MBP"
    log_on_start: bool = True


class SimpleBundleDemo(Strategy):
    def __init__(self, config: SimpleBundleDemoConfig) -> None:
        super().__init__(config)
        self.book_type: BookType = book_type_from_str(self.config.book_type)

    def on_start(self) -> None:
        if self.config.log_on_start:
            ts_ns = self.clock.timestamp_ns()
            formatted = DemoUtils.format_ts(ts_ns)
            self.log.info(f"SimpleBundleDemo started at {formatted}")
