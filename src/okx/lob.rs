use crate::items::{LobItem, LobLevel};
use crate::okx::types::OkxWsMessage;
use crate::okx::types::{PriceLevel, extract_levels};
use crate::traits::{LevelsWithinPct, OrderBook as OrderBookTrait};
use ordered_float::OrderedFloat;
use std::cmp::Reverse;
use std::collections::BTreeMap;

/// Direction of a price level.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Side {
    Bid,
    Ask,
}

/// In-memory order book maintaining full LOB2 state.
///
/// This book stores **every** level received from the exchange WebSocket —
/// complete snapshots plus all incremental updates — with no pre-filtering.
/// The configured filters (`max_level`, `max_level_pct`) are applied only when
/// [`to_lob_item`](OrderBook::to_lob_item) produces a `LobItem` for the stream.
///
/// Bids are stored with `Reverse<OrderedFloat<price>>` as key so iteration yields
/// descending price (best bid first). Asks use `OrderedFloat<price>` for ascending
/// order (best ask first). `OrderedFloat` provides the `Ord` implementation that
/// `f64` lacks while treating NaN as less than any finite value.
#[derive(Debug, Clone)]
pub struct OrderBook {
    /// bids: Reverse<OrderedFloat<price>> → amount  (descending iteration)
    pub bids: BTreeMap<Reverse<OrderedFloat<f64>>, f64>,
    /// asks: OrderedFloat<price> → amount  (ascending iteration)
    pub asks: BTreeMap<OrderedFloat<f64>, f64>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    /// Number of bid price levels.
    pub fn num_bids(&self) -> usize {
        self.bids.len()
    }

    /// Number of ask price levels.
    pub fn num_asks(&self) -> usize {
        self.asks.len()
    }

    /// Best bid price, or `None` if no bids.
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.first_key_value().map(|(k, _)| k.0.0)
    }

    /// Best ask price, or `None` if no asks.
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.first_key_value().map(|(k, _)| k.0)
    }

    /// Spread (best_ask - best_bid), or `None` if either side is empty.
    pub fn spread(&self) -> Option<f64> {
        match (self.best_ask(), self.best_bid()) {
            (Some(a), Some(b)) => Some(a - b),
            _ => None,
        }
    }

    /// Get the top N bid levels as (price, size) tuples, sorted descending by price.
    pub fn top_bids(&self, n: usize) -> Vec<(f64, f64)> {
        self.bids.iter().take(n).map(|(k, v)| (k.0.0, *v)).collect()
    }

    /// Get the top N ask levels as (price, size) tuples, sorted ascending by price.
    pub fn top_asks(&self, n: usize) -> Vec<(f64, f64)> {
        self.asks.iter().take(n).map(|(k, v)| (k.0, *v)).collect()
    }

    /// Get (bids, asks) within `top_pct` of the best price on each side.
    ///
    /// Returns `(Vec<(price, size)>, Vec<(price, size)>)` with bids descending
    /// and asks ascending. Only levels within `top_pct%` of the best price
    /// are included, matching the terminal display filter.
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

    /// Clear all levels on the given side and insert fresh ones from `data`.
    /// Only levels with amount > 0.0 are inserted.
    pub fn apply_snapshot(&mut self, data: &[PriceLevel], side: Side) {
        match side {
            Side::Bid => {
                self.bids.clear();
                for level in data {
                    if let Some((price, amount)) = parse_price_level(level)
                        && amount > 0.0
                    {
                        self.bids.insert(Reverse(OrderedFloat(price)), amount);
                    }
                }
            }
            Side::Ask => {
                self.asks.clear();
                for level in data {
                    if let Some((price, amount)) = parse_price_level(level)
                        && amount > 0.0
                    {
                        self.asks.insert(OrderedFloat(price), amount);
                    }
                }
            }
        }
    }

    /// Apply incremental changes for the given side.
    ///
    /// - `size == 0.0` → remove the price level
    /// - `size > 0.0` → upsert the level
    pub fn apply_update(&mut self, data: &[PriceLevel], side: Side) {
        for level in data {
            if let Some((price, amount)) = parse_price_level(level) {
                match side {
                    Side::Bid => {
                        if amount == 0.0 {
                            self.bids.remove(&Reverse(OrderedFloat(price)));
                        } else {
                            self.bids.insert(Reverse(OrderedFloat(price)), amount);
                        }
                    }
                    Side::Ask => {
                        if amount == 0.0 {
                            self.asks.remove(&OrderedFloat(price));
                        } else {
                            self.asks.insert(OrderedFloat(price), amount);
                        }
                    }
                }
            }
        }
    }

    /// Process an OKX WebSocket message: extract bids/asks from `data[0]`
    /// and apply snapshot or update logic without pre-filtering.
    pub fn process_msg(&mut self, msg: &OkxWsMessage) {
        let data = match msg.data.first() {
            Some(d) => d,
            None => return,
        };

        let action = msg.action.as_deref().unwrap_or("snapshot");

        for (key, side) in [("bids", Side::Bid), ("asks", Side::Ask)] {
            let levels = extract_levels(data, key);
            if levels.is_empty() {
                continue;
            }
            if !levels.is_empty() {
                match action {
                    "snapshot" => self.apply_snapshot(&levels, side),
                    "update" => self.apply_update(&levels, side),
                    _ => {}
                }
            }
        }
    }

    /// Total size across all bid levels.
    pub fn total_bid_size(&self) -> f64 {
        self.bids.values().sum()
    }

    /// Total size across all ask levels.
    pub fn total_ask_size(&self) -> f64 {
        self.asks.values().sum()
    }

    /// Format the order book for terminal display.
    ///
    /// The LOB is already pre-filtered, so all levels are shown. No post-filtering
    /// is applied.
    ///
    /// Output: `BTC-USDT  bids=143  asks=137  spread=0.10  bids: [ px (sz), ... ] | asks: [ px (sz), ... ]`
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

    /// Format one side of the book.
    /// No post-filtering is applied since the LOB is already pre-filtered.
    fn format_side(&self, levels: impl Iterator<Item = (f64, f64)>) -> String {
        let formatted: Vec<String> = levels
            .map(|(price, amount)| format!("{:.2} ({})", price, amount))
            .collect();

        formatted.join(", ")
    }

    /// Create a LobItem containing **all** in-memory levels — no filtering.
    ///
    /// This is guaranteed to return every level that has been received from the
    /// WebSocket and stored via `process_msg` / `apply_snapshot` / `apply_update`.
    /// Filtering by `max_level` / `max_level_pct` is applied only in [`to_lob_item`],
    /// which is the path used when forwarding to the stream.
    pub fn full_lob_item(&self, ts: u64, exchange: &str) -> Option<LobItem> {
        self.to_lob_item(ts, exchange, None, 0.0)
    }

    /// Create a LobItem with post-filtering applied.
    ///
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

use crate::okx::types::parse_price_level;

#[cfg(test)]
mod tests {
    use super::*;

    fn price_level(price: &str, size: &str) -> PriceLevel {
        vec![price.to_string(), size.to_string()]
    }

    #[test]
    fn test_new_book_empty() {
        let book = OrderBook::new();
        assert_eq!(book.num_bids(), 0);
        assert_eq!(book.num_asks(), 0);
        assert_eq!(book.best_bid(), None);
        assert_eq!(book.best_ask(), None);
        assert_eq!(book.spread(), None);
    }

    #[test]
    fn test_apply_snapshot_replaces_levels() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[price_level("100.0", "1.0"), price_level("99.0", "2.0")],
            Side::Bid,
        );
        assert_eq!(book.num_bids(), 2);
        assert!((book.best_bid().unwrap() - 100.0).abs() < f64::EPSILON);

        // Second snapshot replaces
        book.apply_snapshot(&[price_level("98.0", "3.0")], Side::Bid);
        assert_eq!(book.num_bids(), 1);
        assert!((book.best_bid().unwrap() - 98.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_apply_update_upserts() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[price_level("100.0", "1.0")], Side::Bid);
        book.apply_update(&[price_level("100.0", "5.0")], Side::Bid);
        assert_eq!(*book.bids.get(&Reverse(OrderedFloat(100.0))).unwrap(), 5.0);
    }

    #[test]
    fn test_apply_update_removes() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[price_level("100.0", "1.0"), price_level("99.0", "2.0")],
            Side::Bid,
        );
        book.apply_update(&[price_level("100.0", "0.0")], Side::Bid);
        assert_eq!(book.num_bids(), 1);
        assert!(!book.bids.contains_key(&Reverse(OrderedFloat(100.0))));
    }

    #[test]
    fn test_apply_update_unknown_level() {
        let mut book = OrderBook::new();
        book.apply_update(&[price_level("999.0", "0.0")], Side::Bid);
        assert_eq!(book.num_bids(), 0);
    }

    #[test]
    fn test_snapshot_then_updates() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[
                price_level("100.0", "10.0"),
                price_level("99.0", "20.0"),
                price_level("98.0", "30.0"),
            ],
            Side::Bid,
        );
        book.apply_snapshot(
            &[price_level("101.0", "15.0"), price_level("102.0", "25.0")],
            Side::Ask,
        );

        assert!((book.best_bid().unwrap() - 100.0).abs() < f64::EPSILON);
        assert!((book.best_ask().unwrap() - 101.0).abs() < f64::EPSILON);

        // Update: remove best bid, reduce ask
        book.apply_update(&[price_level("100.0", "0.0")], Side::Bid);
        book.apply_update(&[price_level("101.0", "10.0")], Side::Ask);

        assert!((book.best_bid().unwrap() - 99.0).abs() < f64::EPSILON);
        assert_eq!(*book.asks.get(&OrderedFloat(101.0)).unwrap(), 10.0);
    }

    #[test]
    fn test_spread_calculation() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[price_level("100.0", "1.0")], Side::Bid);
        book.apply_snapshot(&[price_level("101.0", "1.0")], Side::Ask);
        let s = book.spread();
        assert!(s.is_some());
        assert!((s.unwrap() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_spread_empty() {
        let book = OrderBook::new();
        assert_eq!(book.spread(), None);
    }

    #[test]
    fn test_display_contains_counts() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[price_level("100.0", "1.0"), price_level("99.0", "2.0")],
            Side::Bid,
        );
        book.apply_snapshot(&[price_level("101.0", "3.0")], Side::Ask);
        let out = book.display("BTC-USDT", 100.0);
        assert!(out.contains("bids=2"));
        assert!(out.contains("asks=1"));
        assert!(out.contains("bids: ["));
        assert!(out.contains("] | asks: ["));
    }

    #[test]
    fn test_display_empty_book() {
        let book = OrderBook::new();
        let out = book.display("BTC-USDT", 0.1);
        assert!(out.contains("bids=0"));
        assert!(out.contains("asks=0"));
        assert!(out.contains("bids: ["));
        assert!(out.contains("] | asks: ["));
    }

    #[test]
    fn test_display_shows_all_levels() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[
                price_level("100.0", "1.0"),
                price_level("99.5", "2.0"),
                price_level("99.0", "3.0"),
                price_level("98.0", "4.0"),
            ],
            Side::Bid,
        );
        let out = book.display("X", 0.5);
        assert!(out.contains("100.00"), "out = {}", out);
        assert!(out.contains("99.50"), "out = {}", out);
        assert!(out.contains("99.00"), "out = {}", out);
        assert!(out.contains("98.00"), "out = {}", out);
    }

    #[test]
    fn test_display_format_brackets() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[price_level("100.0", "1.0")], Side::Bid);
        book.apply_snapshot(&[price_level("101.0", "2.0")], Side::Ask);
        let out = book.display("T", 100.0);
        assert!(
            out.starts_with("T  bids=1  asks=1  spread=1.00  bids: [ "),
            "out = {}",
            out
        );
        assert!(out.contains("] | asks: [ "), "out = {}", out);
    }

    #[test]
    fn test_process_msg_snapshot() {
        let json = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "asks": [["101.0","1.5","0","1"],["102.0","2.0","0","1"]],
                "bids": [["100.0","3.0","0","2"],["99.0","0.5","0","1"]],
                "ts": "1000",
                "checksum": 0
            }]
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        let mut book = OrderBook::new();
        book.process_msg(&msg);
        assert_eq!(book.num_bids(), 2);
        assert_eq!(book.num_asks(), 2);
        assert!((book.best_bid().unwrap() - 100.0).abs() < f64::EPSILON);
        assert!((book.best_ask().unwrap() - 101.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_process_msg_update() {
        let json_snap = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{"asks":[["101.0","1.0","0","0"]],"bids":[["100.0","1.0","0","0"]],"ts":"0","checksum":0}]
        }"#;
        let json_upd = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "update",
            "data": [{"asks":[["101.0","0","0","0"]],"bids":[["100.0","5.0","0","0"]],"ts":"1","checksum":0}]
        }"#;
        let mut book = OrderBook::new();
        book.process_msg(&OkxWsMessage::from_json(json_snap).unwrap());
        assert_eq!(book.num_bids(), 1);
        assert_eq!(book.num_asks(), 1);

        book.process_msg(&OkxWsMessage::from_json(json_upd).unwrap());
        assert_eq!(book.num_asks(), 0); // removed
        assert_eq!(*book.bids.get(&Reverse(OrderedFloat(100.0))).unwrap(), 5.0); // upserted
    }

    #[test]
    fn test_levels_within_pct_filters_bids() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[
                price_level("100.0", "1.0"),
                price_level("99.5", "2.0"),
                price_level("99.0", "3.0"),
                price_level("98.0", "4.0"),
            ],
            Side::Bid,
        );
        // top_pct=0.5 → only bids >= 100.0 * (1 - 0.5/100) = 99.5
        let (bids, asks) = book.levels_within_pct(0.5);
        assert_eq!(asks.len(), 0);
        assert_eq!(bids.len(), 2);
        assert!((bids[0].0 - 100.0).abs() < f64::EPSILON);
        assert!((bids[1].0 - 99.5).abs() < f64::EPSILON);
        assert!((bids[0].1 - 1.0).abs() < f64::EPSILON);
        assert!((bids[1].1 - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_levels_within_pct_filters_asks() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[
                price_level("101.0", "1.0"),
                price_level("101.5", "2.0"),
                price_level("102.0", "3.0"),
            ],
            Side::Ask,
        );
        // top_pct=0.5 → only asks <= 101.0 * (1 + 0.5/100) = 101.505
        let (bids, asks) = book.levels_within_pct(0.5);
        assert_eq!(bids.len(), 0);
        assert_eq!(asks.len(), 2);
        assert!((asks[0].0 - 101.0).abs() < f64::EPSILON);
        assert!((asks[1].0 - 101.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_levels_within_pct_empty_handling() {
        let book = OrderBook::new();
        let (bids, asks) = book.levels_within_pct(0.1);
        assert!(bids.is_empty());
        assert!(asks.is_empty());
    }

    #[test]
    fn test_levels_within_pct_shows_all_at_100() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[price_level("100.0", "1.0"), price_level("50.0", "2.0")],
            Side::Bid,
        );
        let (bids, _) = book.levels_within_pct(100.0);
        assert_eq!(bids.len(), 2);
    }

    #[test]
    fn test_full_lob_flow_snapshot_update_depth() {
        let mut book = OrderBook::new();

        // 1. Apply a snapshot with multiple levels on both sides
        book.apply_snapshot(
            &[
                price_level("100.0", "1.0"),
                price_level("99.5", "2.0"),
                price_level("99.0", "3.0"),
                price_level("98.0", "4.0"),
            ],
            Side::Bid,
        );
        book.apply_snapshot(
            &[
                price_level("101.0", "1.5"),
                price_level("101.5", "2.5"),
                price_level("102.0", "3.5"),
            ],
            Side::Ask,
        );

        assert_eq!(book.num_bids(), 4);
        assert_eq!(book.num_asks(), 3);

        // 2. Apply an update that removes a bid level (zero volume) and adds a new ask level
        book.apply_update(
            &[price_level("99.5", "0.0")], // remove bid at 99.5
            Side::Bid,
        );
        book.apply_update(
            &[price_level("103.0", "5.0")], // new ask at 103.0
            Side::Ask,
        );
        assert_eq!(book.num_bids(), 3);
        assert!((book.best_bid().unwrap() - 100.0).abs() < f64::EPSILON);
        assert_eq!(book.num_asks(), 4);

        // 3. Verify levels_within_pct with narrow filter (0.1%)
        let (bids, asks) = book.levels_within_pct(0.1);
        // top_pct=0.1: bid_threshold=100*0.999=99.9, ask_threshold=101*1.001=101.101
        // Bids >= 99.9: only 100.0 (1 level)
        // Asks <= 101.101: only 101.0 (1 level)
        assert_eq!(bids.len(), 1, "narrow filter: only best bid");
        assert_eq!(asks.len(), 1, "narrow filter: only best ask");

        // 4. Verify levels_within_pct with wider filter (1.0%)
        let (bids, asks) = book.levels_within_pct(1.0);
        // top_pct=1.0: bid_threshold=100*0.99=99.0, ask_threshold=101*1.01=102.01
        // Bids >= 99.0: 100.0, 99.0 (2 levels — 99.5 was removed)
        // Asks <= 102.01: 101.0, 101.5, 102.0 (3 levels)
        assert_eq!(bids.len(), 2, "1% filter shows 2 bids");
        assert_eq!(asks.len(), 3, "1% filter shows 3 asks");

        // 5. Verify that removed level (99.5) does not appear even with 100% filter
        let (bids, _) = book.levels_within_pct(100.0);
        assert_eq!(bids.len(), 3, "after removal, only 3 bids remain");
        assert!(
            !bids.iter().any(|(p, _)| (*p - 99.5).abs() < f64::EPSILON),
            "removed bid at 99.5 should not appear"
        );
    }

    #[test]
    fn test_zero_amount_passes_parse_level() {
        let level = price_level("100.0", "0.0");
        let result = parse_price_level(&level);
        assert!(result.is_some(), "zero amount should parse");
        let (price, amount) = result.unwrap();
        assert!((price - 100.0).abs() < f64::EPSILON);
        assert!((amount - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_to_lob_item_returns_none_when_empty() {
        let book = OrderBook::new();
        let result = book.to_lob_item(0, "test", None, 0.0);
        assert!(
            result.is_none(),
            "Should return None when bids or asks are empty"
        );
    }

    #[test]
    fn test_to_lob_item_filters_by_max_level() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[
                price_level("100.0", "1.0"),
                price_level("99.0", "2.0"),
                price_level("98.0", "3.0"),
            ],
            Side::Bid,
        );
        book.apply_snapshot(
            &[
                price_level("101.0", "1.0"),
                price_level("102.0", "2.0"),
                price_level("103.0", "3.0"),
            ],
            Side::Ask,
        );
        let lob = book.to_lob_item(0, "test", Some(2), 0.0).unwrap();
        assert_eq!(lob.bids.len(), 2);
        assert_eq!(lob.asks.len(), 2);
    }

    #[test]
    fn test_to_lob_item_sorts_bids_with_best_bid_last() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[price_level("100.0", "1.0")], Side::Bid);
        book.apply_snapshot(&[price_level("101.0", "1.0")], Side::Ask);
        book.apply_update(&[price_level("99.0", "2.0")], Side::Bid);
        book.apply_update(&[price_level("98.0", "3.0")], Side::Bid);
        let lob = book.to_lob_item(0, "test", Some(10), 0.0).unwrap();
        assert_eq!(lob.bids.len(), 3);
        assert!(
            (lob.bids[2].price - 100.0).abs() < f64::EPSILON,
            "Best bid (100.0) should be last element"
        );
    }

    #[test]
    fn test_to_lob_item_sorts_asks_with_best_ask_first() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[price_level("101.0", "1.0")], Side::Ask);
        book.apply_snapshot(&[price_level("100.0", "1.0")], Side::Bid);
        book.apply_update(&[price_level("102.0", "2.0")], Side::Ask);
        book.apply_update(&[price_level("103.0", "3.0")], Side::Ask);
        let lob = book.to_lob_item(0, "test", Some(10), 0.0).unwrap();
        assert_eq!(lob.asks.len(), 3);
        assert!(
            (lob.asks[0].price - 101.0).abs() < f64::EPSILON,
            "Best ask (101.0) should be first element"
        );
    }

    #[test]
    fn test_to_lob_item_pct_zero_keeps_all_levels() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[
                price_level("100.0", "1.0"),
                price_level("99.0", "2.0"),
                price_level("98.0", "3.0"),
            ],
            Side::Bid,
        );
        book.apply_snapshot(
            &[
                price_level("101.0", "1.0"),
                price_level("102.0", "2.0"),
                price_level("103.0", "3.0"),
            ],
            Side::Ask,
        );
        // max_level_pct = 0.0 should be normalized to 100.0 → no filtering
        let lob = book.to_lob_item(0, "test", None, 0.0).unwrap();
        assert_eq!(lob.bids.len(), 3, "pct=0.0 should keep all bids");
        assert_eq!(lob.asks.len(), 3, "pct=0.0 should keep all asks");
    }

    #[test]
    fn test_to_lob_item_pct_100_keeps_all_levels() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[price_level("100.0", "1.0")], Side::Bid);
        book.apply_snapshot(&[price_level("101.0", "1.0")], Side::Ask);
        let lob = book.to_lob_item(0, "test", None, 100.0).unwrap();
        assert_eq!(lob.bids.len(), 1);
        assert_eq!(lob.asks.len(), 1);
    }

    #[test]
    fn test_to_lob_item_pct_above_100_keeps_all_levels() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[price_level("100.0", "1.0"), price_level("50.0", "2.0")],
            Side::Bid,
        );
        book.apply_snapshot(&[price_level("101.0", "1.0")], Side::Ask);
        // max_level_pct = 150.0 should be normalized to 100.0 → no filtering
        let lob = book.to_lob_item(0, "test", None, 150.0).unwrap();
        assert_eq!(lob.bids.len(), 2, "pct=150.0 should keep all bids");
        assert_eq!(lob.asks.len(), 1);
    }

    // ------------------------------------------------------------------
    // Guarantee: in-memory OrderBook retains ALL levels from every WS
    // snapshot/update. Filtering (max_level / max_level_pct) happens
    // ONLY in to_lob_item / full_lob_item — never during processing.
    // ------------------------------------------------------------------

    #[test]
    fn test_full_lob_item_returns_all_levels() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[
                price_level("100.0", "1.0"),
                price_level("99.0", "2.0"),
                price_level("98.0", "3.0"),
                price_level("97.0", "4.0"),
            ],
            Side::Bid,
        );
        book.apply_snapshot(
            &[
                price_level("101.0", "1.0"),
                price_level("102.0", "2.0"),
                price_level("103.0", "3.0"),
                price_level("104.0", "4.0"),
            ],
            Side::Ask,
        );
        let lob = book.full_lob_item(0, "okx").unwrap();
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
        book.apply_snapshot(
            &[
                price_level("100.0", "1.0"),
                price_level("99.0", "2.0"),
                price_level("98.0", "3.0"),
                price_level("97.0", "4.0"),
            ],
            Side::Bid,
        );
        book.apply_snapshot(
            &[
                price_level("101.0", "1.0"),
                price_level("102.0", "2.0"),
                price_level("103.0", "3.0"),
                price_level("104.0", "4.0"),
            ],
            Side::Ask,
        );
        // to_lob_item with max_level=1 must produce a 1-level lob...
        let lob = book.to_lob_item(0, "okx", Some(1), 0.0).unwrap();
        assert_eq!(lob.bids.len(), 1, "filtered lob should have 1 bid");
        assert_eq!(lob.asks.len(), 1, "filtered lob should have 1 ask");
        // ...but the in-memory book must STILL contain all 4 levels.
        assert_eq!(
            book.num_bids(),
            4,
            "memory book must retain all 4 bids after filtered emit"
        );
        assert_eq!(
            book.num_asks(),
            4,
            "memory book must retain all 4 asks after filtered emit"
        );
    }

    #[test]
    fn test_full_lob_item_equals_unfiltered_to_lob_item() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[
                price_level("100.0", "1.0"),
                price_level("99.0", "2.0"),
                price_level("98.0", "3.0"),
            ],
            Side::Bid,
        );
        book.apply_snapshot(
            &[
                price_level("101.0", "1.0"),
                price_level("102.0", "2.0"),
                price_level("103.0", "3.0"),
            ],
            Side::Ask,
        );
        let full = book.full_lob_item(0, "okx").unwrap();
        let unfiltered = book.to_lob_item(0, "okx", None, 0.0).unwrap();
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
        book.apply_snapshot(
            &[
                price_level("100.0", "1.0"),
                price_level("99.0", "2.0"),
                price_level("98.0", "3.0"),
                price_level("97.0", "4.0"),
                price_level("96.0", "5.0"),
            ],
            Side::Bid,
        );
        book.apply_snapshot(
            &[
                price_level("101.0", "1.0"),
                price_level("102.0", "2.0"),
                price_level("103.0", "3.0"),
                price_level("104.0", "4.0"),
                price_level("105.0", "5.0"),
            ],
            Side::Ask,
        );
        let full = book.full_lob_item(0, "okx").unwrap();
        let filtered = book.to_lob_item(0, "okx", Some(2), 0.0).unwrap();
        assert_eq!(full.bids.len(), 5, "memory has 5 bids");
        assert_eq!(filtered.bids.len(), 2, "filtered lob has 2 bids");
        assert_eq!(full.asks.len(), 5, "memory has 5 asks");
        assert_eq!(filtered.asks.len(), 2, "filtered lob has 2 asks");
    }
}
