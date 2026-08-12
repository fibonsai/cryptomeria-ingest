use crate::bitstamp::types::{
    BitstampWsMessage, MessageType, OrderBookData, OrderEntry, TradeData,
};
use crate::items::{LobItem, LobLevel};
use crate::traits::{LevelsWithinPct, OrderBook as OrderBookTrait};
use ordered_float::OrderedFloat;
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};

/// Bitstamp LOB is temporarily disabled while a known bug in the order-book
/// model is being fixed (see the README warning). The LOB *stream* still
/// subscribes and receives messages but returns an empty object
/// (a `LobItem` with empty `bids`/`asks`) instead of the buggy data.
///
/// All parsing/processing logic in this module is retained and still covered
/// by unit tests. To re-enable, flip this to `false`.
pub const BITSTAMP_LOB_DISABLED: bool = true;

/// Direction of a price level.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Side {
    Bid,
    Ask,
}

/// Order info for tracking individual Bitstamp orders.
#[derive(Debug, Clone)]
pub struct OrderInfo {
    pub price: OrderedFloat<f64>,
    pub size: f64,
    pub side: Side,
}

/// In-memory order book for Bitstamp.
///
/// This book stores **every** order and aggregated level received from the
/// exchange WebSocket — complete snapshots plus all incremental updates — with
/// no pre-filtering. The configured filters (`max_level`, `max_level_pct`) are
/// applied only when [`to_lob_item`](OrderBook::to_lob_item) produces a
/// `LobItem` for the stream.
#[derive(Debug, Clone)]
pub struct OrderBook {
    /// Individual order tracking.
    pub orders: HashMap<u64, OrderInfo>,
    /// Aggregated price levels for bids (descending iteration).
    pub bids: BTreeMap<Reverse<OrderedFloat<f64>>, f64>,
    /// Aggregated price levels for asks (ascending iteration).
    pub asks: BTreeMap<OrderedFloat<f64>, f64>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub fn num_bids(&self) -> usize {
        self.bids.len()
    }

    pub fn num_asks(&self) -> usize {
        self.asks.len()
    }

    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first_key_value().map(|(k, _)| k.0.0)
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first_key_value().map(|(k, _)| k.0)
    }

    pub fn spread(&self) -> Option<f64> {
        match (self.best_ask(), self.best_bid()) {
            (Some(a), Some(b)) => Some(a - b),
            _ => None,
        }
    }

    pub fn levels_within_pct(&self, top_pct: f64) -> LevelsWithinPct {
        let bid_threshold = self.best_bid().map(|b| b * (1.0 - top_pct / 100.0));
        let ask_threshold = self.best_ask().map(|a| a * (1.0 + top_pct / 100.0));

        let bids: Vec<(f64, f64)> = self
            .bids
            .iter()
            .filter(|(k, _)| match bid_threshold {
                Some(t) => k.0.0 >= t,
                None => true,
            })
            .map(|(k, v)| (k.0.0, *v))
            .collect();

        let asks: Vec<(f64, f64)> = self
            .asks
            .iter()
            .filter(|(k, _)| match ask_threshold {
                Some(t) => k.0 <= t,
                None => true,
            })
            .map(|(k, v)| (k.0, *v))
            .collect();

        (bids, asks)
    }

    fn apply_order(&mut self, entry: &OrderEntry) {
        let price = match entry.price.parse::<f64>() {
            Ok(p) => OrderedFloat(p),
            Err(_) => return,
        };
        let amount = match entry.amount.parse::<f64>() {
            Ok(a) => a,
            Err(_) => return,
        };
        let side = if entry.is_bid() { Side::Bid } else { Side::Ask };

        if amount == 0.0 {
            // Remove order
            if let Some(old) = self.orders.remove(&entry.id) {
                self.rebuild_price_level(old.side, old.price);
            }
        } else {
            // Add or update order
            self.orders.insert(
                entry.id,
                OrderInfo {
                    price,
                    size: amount,
                    side,
                },
            );
            self.rebuild_price_level(side, price);
        }
    }

    fn rebuild_price_level(&mut self, side: Side, price: OrderedFloat<f64>) {
        let total: f64 = self
            .orders
            .values()
            .filter(|o| o.side == side && o.price == price)
            .map(|o| o.size)
            .sum();

        if total == 0.0 {
            match side {
                Side::Bid => {
                    self.bids.remove(&Reverse(price));
                }
                Side::Ask => {
                    self.asks.remove(&price);
                }
            }
        } else {
            match side {
                Side::Bid => {
                    self.bids.insert(Reverse(price), total);
                }
                Side::Ask => {
                    self.asks.insert(price, total);
                }
            }
        }
    }

    /// Process a Bitstamp WebSocket message, applying ALL levels to the
    /// in-memory book without any pre-filtering.
    pub fn process_msg(&mut self, msg: &BitstampWsMessage) {
        let data = match msg.data.as_ref() {
            Some(d) => d,
            None => return,
        };

        match msg.message_type() {
            MessageType::L2Snapshot | MessageType::L2Update => {
                // order_book / diff_order_book style
                if let Ok(ob) = serde_json::from_value::<OrderBookData>(data.clone()) {
                    self.apply_orderbook(&ob);
                }
            }
            MessageType::Trade => {
                if let Ok(trade) = serde_json::from_value::<TradeData>(data.clone())
                    && let (Some(price), Some(amount)) = (trade.price_f64(), trade.amount_f64())
                {
                    let side = if trade.side() == "buy" {
                        Side::Bid
                    } else {
                        Side::Ask
                    };
                    self.apply_order(&OrderEntry {
                        id: trade.id,
                        id_str: "".to_string(),
                        price: format!("{:.8}", price),
                        amount: format!("{:.8}", amount),
                        order_type: if side == Side::Bid { 0 } else { 1 },
                        timestamp: trade.timestamp.clone(),
                    });
                }
            }
            _ => {}
        }
    }

    pub fn apply_orderbook(&mut self, ob: &OrderBookData) {
        for level in &ob.bids {
            if level.len() >= 2
                && let (Ok(price), Ok(amount)) = (level[0].parse::<f64>(), level[1].parse::<f64>())
            {
                self.apply_order(&OrderEntry {
                    id: 0, // dummy
                    id_str: "".to_string(),
                    price: format!("{:.8}", price),
                    amount: format!("{:.8}", amount),
                    order_type: 0, // bid
                    timestamp: "0".to_string(),
                });
            }
        }
        for level in &ob.asks {
            if level.len() >= 2
                && let (Ok(price), Ok(amount)) = (level[0].parse::<f64>(), level[1].parse::<f64>())
            {
                self.apply_order(&OrderEntry {
                    id: 0, // dummy
                    id_str: "".to_string(),
                    price: format!("{:.8}", price),
                    amount: format!("{:.8}", amount),
                    order_type: 1, // ask
                    timestamp: "0".to_string(),
                });
            }
        }
    }

    pub fn total_bid_size(&self) -> f64 {
        self.bids.values().sum()
    }

    pub fn total_ask_size(&self) -> f64 {
        self.asks.values().sum()
    }

    pub fn display(&self, instrument: &str, _top_pct: f64) -> String {
        let num_bids = self.num_bids();
        let num_asks = self.num_asks();
        let spread_str = match self.spread() {
            Some(s) => format!("{:.2}", s),
            None => "?".to_string(),
        };

        let bids_str = self.format_side(self.bids.iter().map(|(k, v)| (k.0.0, *v)));
        let asks_str = self.format_side(self.asks.iter().map(|(k, v)| (k.0, *v)));

        format!(
            "{}  bids={}  asks={}  spread={}  bids: [ {} ] | asks: [ {} ]",
            instrument, num_bids, num_asks, spread_str, bids_str, asks_str
        )
    }

    fn format_side(&self, levels: impl Iterator<Item = (f64, f64)>) -> String {
        let formatted: Vec<String> = levels
            .map(|(price, amount)| format!("{:.2} ({})", price, amount))
            .collect();

        formatted.join(", ")
    }

    /// Create a LobItem with post-filtering applied.
    ///
    /// Create a LobItem containing **all** in-memory levels — no filtering.
    ///
    /// Guaranteed to return every level received from the WebSocket and stored
    /// via `process_msg` / `apply_orderbook` / `apply_order`. Filtering by
    /// `max_level` / `max_level_pct` is applied only in [`to_lob_item`].
    pub fn full_lob_item(&self, ts: u64, exchange: &str) -> Option<LobItem> {
        self.to_lob_item(ts, exchange, None, 0.0)
    }

    /// Applies max_level and max_level_pct filters, sorts bids ascending (worst to best,
    /// so best_bid is last element) and asks ascending (best to worst, so best_ask is first element).
    ///
    /// **Guarantee:** calling this method does **not** mutate the in-memory order book.
    /// The book retains all levels received from the WebSocket; only the returned
    /// `LobItem` is filtered.
    pub fn to_lob_item(
        &self,
        ts: u64,
        exchange: &str,
        max_level: Option<usize>,
        max_level_pct: f64,
    ) -> Option<LobItem> {
        if self.bids.is_empty() || self.asks.is_empty() {
            return None;
        }

        let bid_best = self.best_bid();
        let ask_best = self.best_ask();

        // Normalize max_level_pct: 0.0 or >= 100.0 means no filtering (treat as 100.0).
        let max_level_pct = if max_level_pct == 0.0 || max_level_pct >= 100.0 {
            100.0
        } else {
            max_level_pct
        };

        // Filter and sort bids: ascending (worst to best), so best_bid is last
        let mut bids: Vec<LobLevel> = self
            .bids
            .iter()
            .filter_map(|(k, v)| {
                let price = k.0.0;
                let amount = *v;
                if amount == 0.0 {
                    return None;
                }
                // Apply max_level_pct filter
                if max_level_pct < 100.0
                    && let Some(best) = bid_best
                    && price < best * (1.0 - max_level_pct / 100.0)
                {
                    return None;
                }
                Some(LobLevel {
                    price,
                    size: amount,
                })
            })
            .collect();

        // Sort bids ascending (worst to best) so best_bid is last
        bids.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Apply max_level filter to bids (keep the best N, which are now at the end)
        if let Some(max) = max_level
            && bids.len() > max
        {
            bids = bids[bids.len().saturating_sub(max)..].to_vec();
        }

        // Filter and sort asks: ascending (best to worst), so best_ask is first
        let mut asks: Vec<LobLevel> = self
            .asks
            .iter()
            .filter_map(|(k, v)| {
                let price = k.0;
                let amount = *v;
                if amount == 0.0 {
                    return None;
                }
                // Apply max_level_pct filter
                if max_level_pct < 100.0
                    && let Some(best) = ask_best
                    && price > best * (1.0 + max_level_pct / 100.0)
                {
                    return None;
                }
                Some(LobLevel {
                    price,
                    size: amount,
                })
            })
            .collect();

        // Sort asks ascending (best to worst) so best_ask is first
        asks.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Apply max_level filter to asks (keep the best N, which are at the beginning)
        if let Some(max) = max_level
            && asks.len() > max
        {
            asks.truncate(max);
        }

        // Check again after filtering
        if bids.is_empty() || asks.is_empty() {
            return None;
        }

        Some(LobItem {
            ts,
            exchange: exchange.to_string(),
            bids,
            asks,
        })
    }
}

impl OrderBookTrait for OrderBook {
    fn new() -> Self {
        OrderBook::new()
    }
    fn num_bids(&self) -> usize {
        OrderBook::num_bids(self)
    }
    fn num_asks(&self) -> usize {
        OrderBook::num_asks(self)
    }
    fn best_bid(&self) -> Option<f64> {
        OrderBook::best_bid(self)
    }
    fn best_ask(&self) -> Option<f64> {
        OrderBook::best_ask(self)
    }
    fn spread(&self) -> Option<f64> {
        OrderBook::spread(self)
    }
    fn levels_within_pct(&self, top_pct: f64) -> LevelsWithinPct {
        OrderBook::levels_within_pct(self, top_pct)
    }
    fn total_bid_size(&self) -> f64 {
        OrderBook::total_bid_size(self)
    }
    fn total_ask_size(&self) -> f64 {
        OrderBook::total_ask_size(self)
    }
    fn display(&self, instrument: &str, top_pct: f64) -> String {
        OrderBook::display(self, instrument, top_pct)
    }
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_book_empty() {
        let book = OrderBook::new();
        assert_eq!(book.num_bids(), 0);
        assert_eq!(book.num_asks(), 0);
    }

    #[test]
    fn test_apply_order_add_bid() {
        let mut book = OrderBook::new();
        let entry = OrderEntry {
            id: 1,
            id_str: "1".to_string(),
            price: "100.0".to_string(),
            amount: "1.5".to_string(),
            order_type: 0,
            timestamp: "0".to_string(),
        };
        book.apply_order(&entry);
        assert_eq!(book.num_bids(), 1);
        assert_eq!(book.num_asks(), 0);
    }

    #[test]
    fn test_apply_order_add_ask() {
        let mut book = OrderBook::new();
        let entry = OrderEntry {
            id: 2,
            id_str: "2".to_string(),
            price: "101.0".to_string(),
            amount: "2.0".to_string(),
            order_type: 1,
            timestamp: "0".to_string(),
        };
        book.apply_order(&entry);
        assert_eq!(book.num_bids(), 0);
        assert_eq!(book.num_asks(), 1);
    }

    #[test]
    fn test_apply_order_remove() {
        let mut book = OrderBook::new();
        let entry_add = OrderEntry {
            id: 1,
            id_str: "1".to_string(),
            price: "100.0".to_string(),
            amount: "1.5".to_string(),
            order_type: 0,
            timestamp: "0".to_string(),
        };
        let entry_rem = OrderEntry {
            id: 1,
            id_str: "1".to_string(),
            price: "100.0".to_string(),
            amount: "0.0".to_string(),
            order_type: 0,
            timestamp: "0".to_string(),
        };
        book.apply_order(&entry_add);
        assert_eq!(book.num_bids(), 1);
        book.apply_order(&entry_rem);
        assert_eq!(book.num_bids(), 0);
    }

    #[test]
    fn test_apply_order_update_size() {
        let mut book = OrderBook::new();
        let entry_add = OrderEntry {
            id: 1,
            id_str: "1".to_string(),
            price: "100.0".to_string(),
            amount: "1.5".to_string(),
            order_type: 0,
            timestamp: "0".to_string(),
        };
        let entry_upd = OrderEntry {
            id: 1,
            id_str: "1".to_string(),
            price: "100.0".to_string(),
            amount: "2.5".to_string(),
            order_type: 0,
            timestamp: "0".to_string(),
        };
        book.apply_order(&entry_add);
        assert_eq!(book.num_bids(), 1);
        book.apply_order(&entry_upd);
        assert_eq!(book.num_bids(), 1);
        // total size should be updated
    }

    #[test]
    fn test_orderbook_snapshot_apply() {
        let mut book = OrderBook::new();
        let ob = OrderBookData {
            bids: vec![
                vec!["100.0".into(), "1.5".into()],
                vec!["99.0".into(), "2.0".into()],
            ],
            asks: vec![
                vec!["101.0".into(), "0.5".into()],
                vec!["102.0".into(), "1.0".into()],
            ],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        };
        book.apply_orderbook(&ob);
        assert_eq!(book.num_bids(), 2);
        assert_eq!(book.num_asks(), 2);
    }

    #[test]
    fn test_spread_calculation() {
        let mut book = OrderBook::new();
        let ob = OrderBookData {
            bids: vec![vec!["100.0".into(), "1.0".into()]],
            asks: vec![vec!["101.0".into(), "1.0".into()]],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        };
        book.apply_orderbook(&ob);
        let s = book.spread();
        assert!(s.is_some());
        assert!((s.unwrap() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_display_contains_counts() {
        let mut book = OrderBook::new();
        let ob = OrderBookData {
            bids: vec![
                vec!["100.0".into(), "1.0".into()],
                vec!["99.0".into(), "2.0".into()],
            ],
            asks: vec![vec!["101.0".into(), "3.0".into()]],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        };
        book.apply_orderbook(&ob);
        let out = book.display("BTC/USD", 0.0);
        assert!(out.contains("bids=2"));
        assert!(out.contains("asks=1"));
    }

    #[test]
    fn test_levels_within_pct_filters_bids() {
        let mut book = OrderBook::new();
        let ob = OrderBookData {
            bids: vec![
                vec!["100.0".into(), "1.0".into()],
                vec!["99.5".into(), "2.0".into()],
                vec!["99.0".into(), "3.0".into()],
                vec!["98.0".into(), "4.0".into()],
            ],
            asks: vec![],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        };
        book.apply_orderbook(&ob);
        let (bids, asks) = book.levels_within_pct(0.5);
        assert_eq!(asks.len(), 0);
        assert_eq!(bids.len(), 2);
        assert!((bids[0].0 - 100.0).abs() < f64::EPSILON);
        assert!((bids[1].0 - 99.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_to_lob_item_returns_none_when_bids_empty() {
        let mut book = OrderBook::new();
        book.apply_orderbook(&OrderBookData {
            bids: vec![],
            asks: vec![vec!["101.0".into(), "1.0".into()]],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        });
        let result = book.to_lob_item(0, "test", None, 0.0);
        assert!(result.is_none(), "Should return None when bids are empty");
    }

    #[test]
    fn test_to_lob_item_returns_none_when_asks_empty() {
        let mut book = OrderBook::new();
        book.apply_orderbook(&OrderBookData {
            bids: vec![vec!["100.0".into(), "1.0".into()]],
            asks: vec![],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        });
        let result = book.to_lob_item(0, "test", None, 0.0);
        assert!(result.is_none(), "Should return None when asks are empty");
    }

    #[test]
    fn test_to_lob_item_filters_by_max_level() {
        let mut book = OrderBook::new();
        book.apply_orderbook(&OrderBookData {
            bids: vec![
                vec!["100.0".into(), "1.0".into()],
                vec!["99.0".into(), "2.0".into()],
                vec!["98.0".into(), "3.0".into()],
                vec!["97.0".into(), "4.0".into()],
            ],
            asks: vec![
                vec!["101.0".into(), "1.0".into()],
                vec!["102.0".into(), "2.0".into()],
                vec!["103.0".into(), "3.0".into()],
            ],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        });
        let lob = book.to_lob_item(0, "test", Some(2), 0.0).unwrap();
        assert_eq!(lob.bids.len(), 2, "Should have 2 bids with max_level=2");
        assert_eq!(lob.asks.len(), 2, "Should have 2 asks with max_level=2");
    }

    #[test]
    fn test_to_lob_item_sorts_bids_with_best_bid_last() {
        let mut book = OrderBook::new();
        book.apply_orderbook(&OrderBookData {
            bids: vec![vec!["100.0".into(), "1.0".into()]],
            asks: vec![vec!["101.0".into(), "1.0".into()]],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        });
        // Add more bids to test sorting
        book.apply_orderbook(&OrderBookData {
            bids: vec![vec!["100.0".into(), "1.0".into()]],
            asks: vec![vec!["101.0".into(), "1.0".into()]],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        });

        // Manually add bids via apply_order
        book.apply_order(&OrderEntry {
            id: 1,
            id_str: "1".to_string(),
            price: "99.0".to_string(),
            amount: "2.0".to_string(),
            order_type: 0,
            timestamp: "0".to_string(),
        });
        book.apply_order(&OrderEntry {
            id: 2,
            id_str: "2".to_string(),
            price: "98.0".to_string(),
            amount: "3.0".to_string(),
            order_type: 0,
            timestamp: "0".to_string(),
        });

        let lob = book.to_lob_item(0, "test", Some(10), 0.0).unwrap();
        // Bids should be sorted ascending, best_bid (100.0) should be last
        assert_eq!(lob.bids.len(), 3);
        assert!((lob.bids[0].price - 98.0).abs() < f64::EPSILON);
        assert!((lob.bids[1].price - 99.0).abs() < f64::EPSILON);
        assert!(
            (lob.bids[2].price - 100.0).abs() < f64::EPSILON,
            "Best bid (100.0) should be last"
        );
    }

    #[test]
    fn test_to_lob_item_sorts_asks_with_best_ask_first() {
        let mut book = OrderBook::new();
        book.apply_order(&OrderEntry {
            id: 1,
            id_str: "1".to_string(),
            price: "101.0".to_string(),
            amount: "1.0".to_string(),
            order_type: 1,
            timestamp: "0".to_string(),
        });
        book.apply_order(&OrderEntry {
            id: 2,
            id_str: "2".to_string(),
            price: "102.0".to_string(),
            amount: "2.0".to_string(),
            order_type: 1,
            timestamp: "0".to_string(),
        });
        book.apply_order(&OrderEntry {
            id: 3,
            id_str: "3".to_string(),
            price: "103.0".to_string(),
            amount: "3.0".to_string(),
            order_type: 1,
            timestamp: "0".to_string(),
        });
        book.apply_order(&OrderEntry {
            id: 0,
            id_str: "0".to_string(),
            price: "100.0".to_string(),
            amount: "0.5".to_string(),
            order_type: 0,
            timestamp: "0".to_string(),
        });

        let lob = book.to_lob_item(0, "test", Some(10), 0.0).unwrap();
        // Asks should be sorted ascending, best_ask (101.0) should be first
        assert_eq!(lob.asks.len(), 3);
        assert!(
            (lob.asks[0].price - 101.0).abs() < f64::EPSILON,
            "Best ask (101.0) should be first"
        );
        assert!((lob.asks[1].price - 102.0).abs() < f64::EPSILON);
        assert!((lob.asks[2].price - 103.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_to_lob_item_filters_by_max_level_pct() {
        let mut book = OrderBook::new();
        book.apply_orderbook(&OrderBookData {
            bids: vec![vec!["100.0".into(), "1.0".into()]],
            asks: vec![vec!["101.0".into(), "1.0".into()]],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        });
        // Add bids at -2% and -5% from best
        book.apply_order(&OrderEntry {
            id: 1,
            id_str: "1".to_string(),
            price: "98.0".to_string(),
            amount: "2.0".to_string(),
            order_type: 0,
            timestamp: "0".to_string(),
        });
        book.apply_order(&OrderEntry {
            id: 2,
            id_str: "2".to_string(),
            price: "95.0".to_string(),
            amount: "3.0".to_string(),
            order_type: 0,
            timestamp: "0".to_string(),
        });

        let lob = book.to_lob_item(0, "test", None, 1.0).unwrap();
        // With 1% tolerance, bid at 98.0 (100*0.99=99) should be excluded
        // Only 95.0 and 100.0 should pass? Actually 95 < 99, so should be excluded
        // Wait, let me re-check: 100 * (1 - 1/100) = 99.0
        // So 98.0 < 99.0 and 95.0 < 99.0, both should be excluded
        // Only 100.0 should remain
        assert_eq!(lob.bids.len(), 1, "Should only have the best bid within 1%");
        assert!((lob.bids[0].price - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_to_lob_item_pct_zero_keeps_all_levels() {
        let mut book = OrderBook::new();
        book.apply_orderbook(&OrderBookData {
            bids: vec![
                vec!["100.0".into(), "1.0".into()],
                vec!["99.0".into(), "2.0".into()],
                vec!["98.0".into(), "3.0".into()],
            ],
            asks: vec![
                vec!["101.0".into(), "1.0".into()],
                vec!["102.0".into(), "2.0".into()],
                vec!["103.0".into(), "3.0".into()],
            ],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        });
        // max_level_pct = 0.0 should be normalized to 100.0 → no filtering
        let lob = book.to_lob_item(0, "test", None, 0.0).unwrap();
        assert_eq!(lob.bids.len(), 3, "pct=0.0 should keep all bids");
        assert_eq!(lob.asks.len(), 3, "pct=0.0 should keep all asks");
    }

    #[test]
    fn test_to_lob_item_pct_100_keeps_all_levels() {
        let mut book = OrderBook::new();
        book.apply_orderbook(&OrderBookData {
            bids: vec![vec!["100.0".into(), "1.0".into()]],
            asks: vec![vec!["101.0".into(), "1.0".into()]],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        });
        let lob = book.to_lob_item(0, "test", None, 100.0).unwrap();
        assert_eq!(lob.bids.len(), 1);
        assert_eq!(lob.asks.len(), 1);
    }

    #[test]
    fn test_to_lob_item_pct_above_100_keeps_all_levels() {
        let mut book = OrderBook::new();
        book.apply_orderbook(&OrderBookData {
            bids: vec![
                vec!["100.0".into(), "1.0".into()],
                vec!["50.0".into(), "2.0".into()],
            ],
            asks: vec![vec!["101.0".into(), "1.0".into()]],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        });
        // max_level_pct = 150.0 should be normalized to 100.0 → no filtering
        let lob = book.to_lob_item(0, "test", None, 150.0).unwrap();
        assert_eq!(lob.bids.len(), 2, "pct=150.0 should keep all bids");
        assert_eq!(lob.asks.len(), 1);
    }

    // ------------------------------------------------------------------
    // Guarantee: in-memory OrderBook retains ALL levels from every WS
    // message. Filtering happens ONLY in to_lob_item / full_lob_item.
    // ------------------------------------------------------------------

    #[test]
    fn test_full_lob_item_returns_all_levels() {
        let mut book = OrderBook::new();
        let ob = OrderBookData {
            bids: vec![
                vec!["100.0".into(), "1.0".into()],
                vec!["99.0".into(), "2.0".into()],
                vec!["98.0".into(), "3.0".into()],
                vec!["97.0".into(), "4.0".into()],
            ],
            asks: vec![
                vec!["101.0".into(), "1.0".into()],
                vec!["102.0".into(), "2.0".into()],
                vec!["103.0".into(), "3.0".into()],
                vec!["104.0".into(), "4.0".into()],
            ],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        };
        book.apply_orderbook(&ob);
        let lob = book.full_lob_item(0, "bitstamp").unwrap();
        assert_eq!(
            lob.bids.len(),
            4,
            "full_lob_item must return ALL 4 bid levels (no filtering)"
        );
        assert_eq!(
            lob.asks.len(),
            4,
            "full_lob_item must return ALL 4 ask levels (no filtering)"
        );
    }

    #[test]
    fn test_memory_retains_all_levels_after_filtered_lob_item() {
        let mut book = OrderBook::new();
        let ob = OrderBookData {
            bids: vec![
                vec!["100.0".into(), "1.0".into()],
                vec!["99.0".into(), "2.0".into()],
                vec!["98.0".into(), "3.0".into()],
                vec!["97.0".into(), "4.0".into()],
            ],
            asks: vec![
                vec!["101.0".into(), "1.0".into()],
                vec!["102.0".into(), "2.0".into()],
                vec!["103.0".into(), "3.0".into()],
                vec!["104.0".into(), "4.0".into()],
            ],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        };
        book.apply_orderbook(&ob);
        // to_lob_item with max_level=1 must produce a 1-level lob...
        let lob = book.to_lob_item(0, "bitstamp", Some(1), 0.0).unwrap();
        assert_eq!(lob.bids.len(), 1, "filtered lob should have 1 bid");
        assert_eq!(lob.asks.len(), 1, "filtered lob should have 1 ask");
        // ...but the in-memory book must STILL contain all 4 levels.
        assert_eq!(book.num_bids(), 4, "memory book must retain all 4 bids");
        assert_eq!(book.num_asks(), 4, "memory book must retain all 4 asks");
    }

    #[test]
    fn test_full_lob_item_equals_unfiltered_to_lob_item() {
        let mut book = OrderBook::new();
        let ob = OrderBookData {
            bids: vec![
                vec!["100.0".into(), "1.0".into()],
                vec!["99.0".into(), "2.0".into()],
                vec!["98.0".into(), "3.0".into()],
            ],
            asks: vec![
                vec!["101.0".into(), "1.0".into()],
                vec!["102.0".into(), "2.0".into()],
                vec!["103.0".into(), "3.0".into()],
            ],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        };
        book.apply_orderbook(&ob);
        let full = book.full_lob_item(0, "bitstamp").unwrap();
        let unfiltered = book.to_lob_item(0, "bitstamp", None, 0.0).unwrap();
        assert_eq!(full.bids.len(), unfiltered.bids.len());
        assert_eq!(full.asks.len(), unfiltered.asks.len());
        for (a, b) in full.bids.iter().zip(unfiltered.bids.iter()) {
            assert_eq!(a.price, b.price);
            assert_eq!(a.size, b.size);
        }
        for (a, b) in full.asks.iter().zip(unfiltered.asks.iter()) {
            assert_eq!(a.price, b.price);
            assert_eq!(a.size, b.size);
        }
    }

    #[test]
    fn test_to_lob_item_with_filter_returns_fewer_levels_than_full() {
        let mut book = OrderBook::new();
        let ob = OrderBookData {
            bids: vec![
                vec!["100.0".into(), "1.0".into()],
                vec!["99.0".into(), "2.0".into()],
                vec!["98.0".into(), "3.0".into()],
                vec!["97.0".into(), "4.0".into()],
                vec!["96.0".into(), "5.0".into()],
            ],
            asks: vec![
                vec!["101.0".into(), "1.0".into()],
                vec!["102.0".into(), "2.0".into()],
                vec!["103.0".into(), "3.0".into()],
                vec!["104.0".into(), "4.0".into()],
                vec!["105.0".into(), "5.0".into()],
            ],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        };
        book.apply_orderbook(&ob);
        let full = book.full_lob_item(0, "bitstamp").unwrap();
        let filtered = book.to_lob_item(0, "bitstamp", Some(2), 0.0).unwrap();
        assert_eq!(full.bids.len(), 5, "memory has 5 bids");
        assert_eq!(filtered.bids.len(), 2, "filtered lob has 2 bids");
        assert_eq!(full.asks.len(), 5, "memory has 5 asks");
        assert_eq!(filtered.asks.len(), 2, "filtered lob has 2 asks");
    }
}
