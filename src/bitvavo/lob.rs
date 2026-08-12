use crate::bitvavo::types::{BookSnapshot, BookUpdate, PriceLevel};
use crate::items::{LobItem, LobLevel};
use crate::traits::{LevelsWithinPct, OrderBook as OrderBookTrait};
use ordered_float::OrderedFloat;
use std::cmp::Reverse;
use std::collections::BTreeMap;

/// In-memory order book for Bitvavo, with sequence-number-aware
/// `apply_snapshot` / `apply_update` / pending-deltas buffering.
///
/// This book stores **every** level received from the exchange WebSocket —
/// complete snapshots plus all incremental updates — with no pre-filtering.
/// The configured filters (`max_level`, `max_level_pct`) are applied only when
/// [`to_lob_item`](OrderBook::to_lob_item) produces a `LobItem` for the stream.
#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: BTreeMap<Reverse<OrderedFloat<f64>>, f64>,
    pub asks: BTreeMap<OrderedFloat<f64>, f64>,
    /// `mdSeqNo` from the last applied snapshot / update. `None` until the
    /// first snapshot arrives.
    pub last_mdseq: Option<u64>,
    /// Book deltas received before the snapshot; replayed after `drain_pending`.
    pub pending: Vec<BookUpdate>,
}

/// Parsed price/size pair.
pub type Level = (f64, f64);

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_mdseq: None,
            pending: Vec::new(),
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

    pub fn top_bids(&self, n: usize) -> Vec<Level> {
        self.bids.iter().take(n).map(|(k, v)| (k.0.0, *v)).collect()
    }

    pub fn top_asks(&self, n: usize) -> Vec<Level> {
        self.asks.iter().take(n).map(|(k, v)| (k.0, *v)).collect()
    }

    pub fn levels_within_pct(&self, top_pct: f64) -> crate::traits::LevelsWithinPct {
        let bid_threshold = self.best_bid().map(|b| b * (1.0 - top_pct / 100.0));
        let ask_threshold = self.best_ask().map(|a| a * (1.0 + top_pct / 100.0));

        let bids: Vec<Level> = self
            .bids
            .iter()
            .filter(|(k, _)| match bid_threshold {
                Some(t) => k.0.0 >= t,
                None => true,
            })
            .map(|(k, v)| (k.0.0, *v))
            .collect();

        let asks: Vec<Level> = self
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

    fn total_bid_size(&self) -> f64 {
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

    /// Convert raw Bitvavo price levels `[price, size]` strings into `(f64, f64)`.
    fn parse_levels(levels: &[PriceLevel]) -> Vec<Level> {
        levels
            .iter()
            .filter_map(|lv| {
                let price = lv[0].parse::<f64>().ok()?;
                let size = lv[1].parse::<f64>().ok()?;
                Some((price, size))
            })
            .collect()
    }

    /// Apply a full snapshot from a `getBook` response.
    /// Clears existing bids/asks, inserts all levels with size > 0, and sets
    /// `last_mdseq` to the snapshot's `mdSeqNo`.
    pub fn apply_snapshot(&mut self, snap: &BookSnapshot) {
        self.bids.clear();
        self.asks.clear();
        for (price, size) in Self::parse_levels(&snap.bids) {
            if size > 0.0 {
                self.bids.insert(Reverse(OrderedFloat(price)), size);
            }
        }
        for (price, size) in Self::parse_levels(&snap.asks) {
            if size > 0.0 {
                self.asks.insert(OrderedFloat(price), size);
            }
        }
        self.last_mdseq = Some(snap.mdseqno);
    }

    /// Apply an incremental book update, respecting sequence-number continuity.
    ///
    /// - If no snapshot has been loaded yet (`last_mdseq` is `None`), the update
    ///   is pushed to `pending` and replayed later via `drain_pending`.
    /// - If the update's `startMdSeqNo <= last_mdseq`, it is skipped (already applied).
    /// - Otherwise, bids/asks deltas are applied (size 0 = remove level) and
    ///   `last_mdseq` is set to `endMdSeqNo`.
    pub fn apply_update(&mut self, update: &BookUpdate) {
        if self.last_mdseq.is_none() {
            self.pending.push(update.clone());
            return;
        }

        let last = self.last_mdseq.unwrap_or(0);
        if update.start_md_seq_no <= last {
            return;
        }

        for (price, size) in Self::parse_levels(&update.bids) {
            if size == 0.0 {
                self.bids.remove(&Reverse(OrderedFloat(price)));
            } else {
                self.bids.insert(Reverse(OrderedFloat(price)), size);
            }
        }
        for (price, size) in Self::parse_levels(&update.asks) {
            if size == 0.0 {
                self.asks.remove(&OrderedFloat(price));
            } else {
                self.asks.insert(OrderedFloat(price), size);
            }
        }
        self.last_mdseq = Some(update.end_md_seq_no);
    }

    /// After a snapshot is loaded, replay all buffered pending updates.
    /// Each update re-enters the sequence-number logic in `apply_update`.
    pub fn drain_pending(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        for update in pending {
            self.apply_update(&update);
        }
    }

    /// Create a LobItem containing **all** in-memory levels — no filtering.
    ///
    /// Guaranteed to return every level received from the WebSocket and stored
    /// via `apply_snapshot` / `apply_update` / `drain_pending`. Filtering by
    /// `max_level` / `max_level_pct` is applied only in [`to_lob_item`].
    pub fn full_lob_item(&self, ts: u64, exchange: &str) -> Option<LobItem> {
        self.to_lob_item(ts, exchange, None, 0.0)
    }

    /// Create a LobItem with post-filtering applied.
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

        bids.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

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

        asks.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        if let Some(max) = max_level
            && asks.len() > max
        {
            asks.truncate(max);
        }

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

    /// Helper: create a PriceLevel from string price/size.
    fn lv(price: &str, size: &str) -> PriceLevel {
        vec![price.to_string(), size.to_string()]
    }

    fn snapshot(
        market: &str,
        mdseqno: u64,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
    ) -> BookSnapshot {
        BookSnapshot {
            market: market.to_string(),
            nonce: mdseqno,
            bids,
            asks,
            timestamp: Some(1752139200000000000),
            mdseqno,
        }
    }

    fn update(
        market: &str,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        start: u64,
        end: u64,
    ) -> BookUpdate {
        BookUpdate {
            market: market.to_string(),
            bids,
            asks,
            start_md_seq_no: start,
            end_md_seq_no: end,
            timestamp: Some(1752139200000000001),
        }
    }

    #[test]
    fn test_new_book_empty() {
        let book = OrderBook::new();
        assert_eq!(book.num_bids(), 0);
        assert_eq!(book.num_asks(), 0);
        assert!(book.last_mdseq.is_none());
    }

    #[test]
    fn test_apply_snapshot_sets_levels_and_mdseq() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            438525,
            vec![lv("4999.9", "0.015"), lv("4999.0", "1.0")],
            vec![lv("5001.1", "0.015")],
        ));
        assert_eq!(book.num_bids(), 2);
        assert_eq!(book.num_asks(), 1);
        assert_eq!(book.last_mdseq, Some(438525));
    }

    #[test]
    fn test_apply_snapshot_zero_size_not_inserted() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            1,
            vec![lv("100.0", "0.0"), lv("101.0", "1.0")],
            vec![],
        ));
        assert_eq!(book.num_bids(), 1);
    }

    #[test]
    fn test_apply_snapshot_replaces_existing() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            1,
            vec![lv("100.0", "1.0")],
            vec![lv("101.0", "1.0")],
        ));
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            2,
            vec![lv("200.0", "2.0")],
            vec![lv("201.0", "2.0")],
        ));
        assert_eq!(book.num_bids(), 1);
        assert_eq!(book.num_asks(), 1);
        assert_eq!(book.best_bid(), Some(200.0));
        assert_eq!(book.last_mdseq, Some(2));
    }

    #[test]
    fn test_apply_update_before_snapshot_buffers_pending() {
        let mut book = OrderBook::new();
        let upd = update("BTC-EUR", vec![lv("100.0", "1.0")], vec![], 100, 100);
        book.apply_update(&upd);
        assert!(book.pending.len() == 1);
        assert!(book.bids.is_empty());
    }

    #[test]
    fn test_apply_update_after_snapshot_applies_and_advances() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            100,
            vec![lv("100.0", "1.0")],
            vec![lv("101.0", "1.0")],
        ));
        let upd = update("BTC-EUR", vec![lv("100.0", "2.0")], vec![], 101, 101);
        book.apply_update(&upd);
        assert_eq!(book.last_mdseq, Some(101));
        assert_eq!(*book.bids.get(&Reverse(OrderedFloat(100.0))).unwrap(), 2.0);
    }

    #[test]
    fn test_apply_update_skips_already_applied() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&snapshot("BTC-EUR", 100, vec![lv("100.0", "1.0")], vec![]));
        // startMdSeqNo <= last_mdseq -> skip
        let upd = update("BTC-EUR", vec![lv("100.0", "5.0")], vec![], 100, 100);
        book.apply_update(&upd);
        assert_eq!(*book.bids.get(&Reverse(OrderedFloat(100.0))).unwrap(), 1.0);
        assert_eq!(book.last_mdseq, Some(100));
    }

    #[test]
    fn test_apply_update_removes_level_when_size_zero() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            100,
            vec![lv("100.0", "1.0"), lv("99.0", "2.0")],
            vec![],
        ));
        let upd = update("BTC-EUR", vec![lv("100.0", "0.0")], vec![], 101, 101);
        book.apply_update(&upd);
        assert_eq!(book.num_bids(), 1);
        assert!(!book.bids.contains_key(&Reverse(OrderedFloat(100.0))));
    }

    #[test]
    fn test_drain_pending_after_snapshot() {
        let mut book = OrderBook::new();
        // Buffer an update before snapshot
        book.apply_update(&update(
            "BTC-EUR",
            vec![lv("100.0", "1.0")],
            vec![],
            100,
            100,
        ));
        // Load snapshot at mdSeqNo 100
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            100,
            vec![lv("100.0", "1.0")],
            vec![lv("101.0", "1.0")],
        ));
        // Pending update had startMdSeqNo=100 <= last_mdseq=100 -> skipped on drain
        book.drain_pending();
        assert_eq!(book.last_mdseq, Some(100));
        assert!(book.pending.is_empty());
    }

    #[test]
    fn test_drain_pending_applies_updates_after_snapshot() {
        let mut book = OrderBook::new();
        // Buffer updates before snapshot
        book.apply_update(&update(
            "BTC-EUR",
            vec![lv("100.0", "1.0")],
            vec![],
            101,
            101,
        ));
        book.apply_update(&update(
            "BTC-EUR",
            vec![lv("100.0", "2.0")],
            vec![],
            102,
            102,
        ));
        // Load snapshot at mdSeqNo 100
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            100,
            vec![lv("100.0", "1.0")],
            vec![lv("101.0", "1.0")],
        ));
        book.drain_pending();
        // Both buffered updates have startMdSeqNo > 100, so both should be applied
        assert_eq!(book.last_mdseq, Some(102));
        assert_eq!(*book.bids.get(&Reverse(OrderedFloat(100.0))).unwrap(), 2.0);
    }

    #[test]
    fn test_to_lob_item_returns_none_when_empty() {
        let book = OrderBook::new();
        let result = book.to_lob_item(0, "bitvavo", None, 0.0);
        assert!(result.is_none());
    }

    #[test]
    fn test_to_lob_item_basic() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            1,
            vec![lv("100.0", "1.0"), lv("99.0", "2.0")],
            vec![lv("101.0", "1.0"), lv("102.0", "2.0")],
        ));
        let lob = book
            .to_lob_item(1752139200000, "bitvavo", None, 0.0)
            .unwrap();
        assert_eq!(lob.bids.len(), 2);
        assert_eq!(lob.asks.len(), 2);
        assert_eq!(lob.exchange, "bitvavo");
        assert_eq!(lob.ts, 1752139200000);
        assert_eq!(lob.asks[0].price, 101.0);
        assert_eq!(lob.bids[lob.bids.len() - 1].price, 100.0);
    }

    #[test]
    fn test_to_lob_item_filters_by_max_level() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            1,
            vec![lv("100.0", "1.0"), lv("99.0", "2.0"), lv("98.0", "3.0")],
            vec![lv("101.0", "1.0"), lv("102.0", "2.0"), lv("103.0", "3.0")],
        ));
        let lob = book.to_lob_item(0, "bitvavo", Some(2), 0.0).unwrap();
        assert_eq!(lob.bids.len(), 2);
        assert_eq!(lob.asks.len(), 2);
    }

    #[test]
    fn test_to_lob_item_pct_zero_keeps_all_levels() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            1,
            vec![lv("100.0", "1.0"), lv("99.0", "2.0"), lv("98.0", "3.0")],
            vec![lv("101.0", "1.0"), lv("102.0", "2.0"), lv("103.0", "3.0")],
        ));
        let lob = book.to_lob_item(0, "bitvavo", None, 0.0).unwrap();
        assert_eq!(lob.bids.len(), 3);
        assert_eq!(lob.asks.len(), 3);
    }

    #[test]
    fn test_spread_calculation() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            1,
            vec![lv("100.0", "1.0")],
            vec![lv("101.0", "1.0")],
        ));
        let s = book.spread();
        assert!(s.is_some());
        assert!((s.unwrap() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_display_contains_counts() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            1,
            vec![lv("100.0", "1.0"), lv("99.0", "2.0")],
            vec![lv("101.0", "1.0")],
        ));
        let out = book.display("BTC-EUR", 0.0);
        assert!(out.contains("bids=2"));
        assert!(out.contains("asks=1"));
    }

    // ------------------------------------------------------------------
    // Guarantee: in-memory OrderBook retains ALL levels from every WS
    // snapshot/update. Filtering happens ONLY in to_lob_item /
    // full_lob_item — never during processing.
    // ------------------------------------------------------------------

    #[test]
    fn test_full_lob_item_returns_all_levels() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            1,
            vec![
                lv("100.0", "1.0"),
                lv("99.0", "2.0"),
                lv("98.0", "3.0"),
                lv("97.0", "4.0"),
            ],
            vec![
                lv("101.0", "1.0"),
                lv("102.0", "2.0"),
                lv("103.0", "3.0"),
                lv("104.0", "4.0"),
            ],
        ));
        let lob = book.full_lob_item(0, "bitvavo").unwrap();
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
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            1,
            vec![
                lv("100.0", "1.0"),
                lv("99.0", "2.0"),
                lv("98.0", "3.0"),
                lv("97.0", "4.0"),
            ],
            vec![
                lv("101.0", "1.0"),
                lv("102.0", "2.0"),
                lv("103.0", "3.0"),
                lv("104.0", "4.0"),
            ],
        ));
        // to_lob_item with max_level=1 must produce a 1-level lob...
        let lob = book.to_lob_item(0, "bitvavo", Some(1), 0.0).unwrap();
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
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            1,
            vec![lv("100.0", "1.0"), lv("99.0", "2.0"), lv("98.0", "3.0")],
            vec![lv("101.0", "1.0"), lv("102.0", "2.0"), lv("103.0", "3.0")],
        ));
        let full = book.full_lob_item(0, "bitvavo").unwrap();
        let unfiltered = book.to_lob_item(0, "bitvavo", None, 0.0).unwrap();
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
        book.apply_snapshot(&snapshot(
            "BTC-EUR",
            1,
            vec![
                lv("100.0", "1.0"),
                lv("99.0", "2.0"),
                lv("98.0", "3.0"),
                lv("97.0", "4.0"),
                lv("96.0", "5.0"),
            ],
            vec![
                lv("101.0", "1.0"),
                lv("102.0", "2.0"),
                lv("103.0", "3.0"),
                lv("104.0", "4.0"),
                lv("105.0", "5.0"),
            ],
        ));
        let full = book.full_lob_item(0, "bitvavo").unwrap();
        let filtered = book.to_lob_item(0, "bitvavo", Some(2), 0.0).unwrap();
        assert_eq!(full.bids.len(), 5, "memory has 5 bids");
        assert_eq!(filtered.bids.len(), 2, "filtered lob has 2 bids");
        assert_eq!(full.asks.len(), 5, "memory has 5 asks");
        assert_eq!(filtered.asks.len(), 2, "filtered lob has 2 asks");
    }
}
