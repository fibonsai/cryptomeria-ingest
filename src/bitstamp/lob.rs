use crate::bitstamp::types::{
    BitstampWsMessage, MessageType, OrderBookData, OrderEntry, TradeData,
};
use crate::traits::{LevelsWithinPct, LobFilter, OrderBook as OrderBookTrait};
use ordered_float::OrderedFloat;
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};

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
        Self::with_snapshot_depth(400)
    }

    pub fn with_snapshot_depth(_snapshot_depth: usize) -> Self {
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

    pub fn process_msg(&mut self, msg: &BitstampWsMessage, filter: Option<&LobFilter>) {
        let data = match msg.data.as_ref() {
            Some(d) => d,
            None => return,
        };

        match msg.message_type() {
            MessageType::L2Snapshot | MessageType::L2Update => {
                // order_book / diff_order_book style
                if let Ok(ob) = serde_json::from_value::<OrderBookData>(data.clone()) {
                    let ob = if let Some(f) = filter {
                        self.filter_orderbook(ob, f)
                    } else {
                        ob
                    };
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

    fn filter_orderbook(&self, mut ob: OrderBookData, filter: &LobFilter) -> OrderBookData {
        let bid_best = self.best_bid();
        let ask_best = self.best_ask();

        // Filter bids - track included levels within batch
        let mut bids_included = 0usize;
        let initial_bids = self.num_bids();
        ob.bids.retain(|level| {
            if level.len() >= 2 {
                if let (Ok(price), Ok(amount)) = (level[0].parse::<f64>(), level[1].parse::<f64>()) {
                    let side_is_bid = true;
                    let price_exists = self.bids.contains_key(&Reverse(OrderedFloat(price)));
                    let current_levels = initial_bids + bids_included;
                    let include = filter.should_include(
                        bid_best,
                        ask_best,
                        price,
                        amount,
                        side_is_bid,
                        current_levels,
                        price_exists,
                    );
                    if include {
                        bids_included += 1;
                    }
                    include
                } else {
                    true
                }
            } else {
                true
            }
        });

        // Filter asks - track included levels within batch
        let mut asks_included = 0usize;
        let initial_asks = self.num_asks();
        ob.asks.retain(|level| {
            if level.len() >= 2 {
                if let (Ok(price), Ok(amount)) = (level[0].parse::<f64>(), level[1].parse::<f64>()) {
                    let side_is_bid = false;
                    let price_exists = self.asks.contains_key(&OrderedFloat(price));
                    let current_levels = initial_asks + asks_included;
                    let include = filter.should_include(
                        bid_best,
                        ask_best,
                        price,
                        amount,
                        side_is_bid,
                        current_levels,
                        price_exists,
                    );
                    if include {
                        asks_included += 1;
                    }
                    include
                } else {
                    true
                }
            } else {
                true
            }
        });

        ob
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
}

impl OrderBookTrait for OrderBook {
    fn new() -> Self {
        OrderBook::new()
    }
    fn with_snapshot_depth(depth: usize) -> Self {
        OrderBook::with_snapshot_depth(depth)
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
        let mut book = OrderBook::new();
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
                ["100.0".into(), "1.5".into()],
                ["99.0".into(), "2.0".into()],
            ],
            asks: vec![
                ["101.0".into(), "0.5".into()],
                ["102.0".into(), "1.0".into()],
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
            bids: vec![["100.0".into(), "1.0".into()]],
            asks: vec![["101.0".into(), "1.0".into()]],
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
                ["100.0".into(), "1.0".into()],
                ["99.0".into(), "2.0".into()],
            ],
            asks: vec![["101.0".into(), "3.0".into()]],
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
                ["100.0".into(), "1.0".into()],
                ["99.5".into(), "2.0".into()],
                ["99.0".into(), "3.0".into()],
                ["98.0".into(), "4.0".into()],
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
    fn test_filter_orderbook_batch_respects_max_level() {
        // Test that filter_orderbook correctly limits the number of new levels
        // added in a single batch, even when the book is empty.
        let filter = LobFilter::MaxLevel(3);
        let book = OrderBook::new();

        // Simulate a snapshot with 5 bid levels (book is empty)
        let ob = OrderBookData {
            bids: vec![
                ["100.0".into(), "1.0".into()],
                ["99.0".into(), "2.0".into()],
                ["98.0".into(), "3.0".into()],
                ["97.0".into(), "4.0".into()],
                ["96.0".into(), "5.0".into()],
            ],
            asks: vec![],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        };

        let filtered = book.filter_orderbook(ob, &filter);
        // Should only include first 3 levels (max_level=3)
        assert_eq!(filtered.bids.len(), 3, "batch filter should respect max_level");
        // Verify it's the best 3 prices (highest for bids)
        assert!((filtered.bids[0][0].parse::<f64>().unwrap() - 100.0).abs() < f64::EPSILON);
        assert!((filtered.bids[1][0].parse::<f64>().unwrap() - 99.0).abs() < f64::EPSILON);
        assert!((filtered.bids[2][0].parse::<f64>().unwrap() - 98.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_filter_orderbook_batch_respects_max_level_asks() {
        // Same test for asks side
        let filter = LobFilter::MaxLevel(2);
        let book = OrderBook::new();

        let ob = OrderBookData {
            bids: vec![],
            asks: vec![
                ["101.0".into(), "1.0".into()],
                ["102.0".into(), "2.0".into()],
                ["103.0".into(), "3.0".into()],
                ["104.0".into(), "4.0".into()],
            ],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        };

        let filtered = book.filter_orderbook(ob, &filter);
        // Should only include first 2 levels (max_level=2)
        assert_eq!(filtered.asks.len(), 2, "batch filter should respect max_level for asks");
        // Verify it's the best 2 prices (lowest for asks)
        assert!((filtered.asks[0][0].parse::<f64>().unwrap() - 101.0).abs() < f64::EPSILON);
        assert!((filtered.asks[1][0].parse::<f64>().unwrap() - 102.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_filter_orderbook_existing_price_always_included() {
        // Existing price levels should always be included regardless of max_level
        let filter = LobFilter::MaxLevel(1);
        let mut book = OrderBook::new();
        let ob = OrderBookData {
            bids: vec![["100.0".into(), "1.0".into()]],
            asks: vec![],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        };
        book.apply_orderbook(&ob);
        assert_eq!(book.num_bids(), 1);

        // Update existing level + add new level
        let ob2 = OrderBookData {
            bids: vec![
                ["100.0".into(), "5.0".into()], // existing - should be included
                ["99.0".into(), "2.0".into()],  // new - should be filtered out (max=1, already have 1)
            ],
            asks: vec![],
            timestamp: "0".to_string(),
            microtimestamp: "0".to_string(),
        };

        let filtered = book.filter_orderbook(ob2, &filter);
        // Should include the existing price update, but not the new level
        assert_eq!(filtered.bids.len(), 1);
        assert!((filtered.bids[0][0].parse::<f64>().unwrap() - 100.0).abs() < f64::EPSILON);
    }
}
