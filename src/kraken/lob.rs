use crate::items::{LobItem, LobLevel};
use crate::kraken::types::KrakenWsMessage;
use crate::traits::{LevelsWithinPct, OrderBook as OrderBookTrait};
use log::warn;
use ordered_float::OrderedFloat;
use serde_json::Value;
use std::cmp::Reverse;
use std::collections::BTreeMap;

/// Direction of a price level.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Side {
    Bid,
    Ask,
}

/// In-memory order book for Kraken.
///
/// This book stores **every** level received from the exchange WebSocket —
/// complete snapshots plus all incremental updates — with no pre-filtering.
/// The configured filters (`max_level`, `max_level_pct`) are applied only when
/// [`to_lob_item`](OrderBook::to_lob_item) produces a `LobItem` for the stream.
#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: BTreeMap<Reverse<OrderedFloat<f64>>, f64>,
    pub asks: BTreeMap<OrderedFloat<f64>, f64>,
    /// Last `sequence` number seen on the `book` channel. `None` until the
    /// first message with a sequence arrives. Used to detect gaps and
    /// out-of-order messages.
    last_sequence: Option<u64>,
    /// Set to `true` when a sequence gap, out-of-order message, or a
    /// duplicate sequence is detected. The owning adapter should drop all
    /// levels (`reset`) and await a fresh snapshot before emitting again.
    needs_resync: bool,
    /// Observability flag set when the last `verify_checksum` detected a
    /// mismatch. Does not by itself drop the book.
    checksum_failed: bool,
}

/// A raw price level from Kraken: (price, size).
pub type PriceLevel = (f64, f64);

/// Extract price levels from a JSON data object by key ("bids" or "asks").
fn extract_levels(data: &Value, key: &str) -> Vec<PriceLevel> {
    data.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let price = v.get("price")?.as_f64()?;
                    let qty = v.get("qty")?.as_f64()?;
                    Some((price, qty))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Serialize a price/size for the Kraken checksum string, mirroring ccxt's
/// `format_number`: the integer and decimal parts are joined (decimal point
/// removed) and leading zeros are stripped — e.g. `50001.5` -> `"500015"`,
/// `0.5` -> `"5"`.
fn format_number(value: f64) -> String {
    let s = format!("{value}");
    let mut parts = s.splitn(2, '.');
    let integer = parts.next().unwrap_or_default();
    let decimals = parts.next().unwrap_or_default();
    let joined = format!("{integer}{decimals}");
    joined.trim_start_matches('0').to_string()
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_sequence: None,
            needs_resync: false,
            checksum_failed: false,
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

    pub fn top_bids(&self, n: usize) -> Vec<(f64, f64)> {
        self.bids.iter().take(n).map(|(k, v)| (k.0.0, *v)).collect()
    }

    pub fn top_asks(&self, n: usize) -> Vec<(f64, f64)> {
        self.asks.iter().take(n).map(|(k, v)| (k.0, *v)).collect()
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

    pub fn apply_snapshot(&mut self, data: &[PriceLevel], side: Side) {
        // A new (full) snapshot re-establishes the baseline: any previous
        // sequence tracking, resync flag, and checksum-failure flag are
        // discarded.
        if side == Side::Ask {
            self.last_sequence = None;
            self.needs_resync = false;
            self.checksum_failed = false;
        }
        match side {
            Side::Bid => {
                self.bids.clear();
                for &(price, amount) in data {
                    if amount > 0.0 {
                        self.bids.insert(Reverse(OrderedFloat(price)), amount);
                    }
                }
            }
            Side::Ask => {
                self.asks.clear();
                for &(price, amount) in data {
                    if amount > 0.0 {
                        self.asks.insert(OrderedFloat(price), amount);
                    }
                }
            }
        }
        // A full snapshot may legitimately be partial (only one side present),
        // leaving the other half stale from a previous sync. Guard against a
        // crossed book as a safety net; if crossed, the book will be cleared
        // and re-seeded from the next full snapshot.
        self.repair_crossing();
    }

    pub fn apply_update(&mut self, data: &[PriceLevel], side: Side) {
        for &(price, amount) in data {
            match side {
                Side::Bid => {
                    if amount == 0.0 {
                        self.bids.remove(&Reverse(OrderedFloat(price)));
                    } else {
                        // Reject any bid that would cross above the best ask.
                        // Once crossed the book stays crossed for all subsequent
                        // updates unless explicitly repaired.
                        if let Some(best_ask) = self.best_ask()
                            && price >= best_ask
                        {
                            warn!(
                                "[kraken] rejecting bid update at {:.2} >= best ask {:.2} (cross guard)",
                                price, best_ask
                            );
                            continue;
                        }
                        self.bids.insert(Reverse(OrderedFloat(price)), amount);
                    }
                }
                Side::Ask => {
                    if amount == 0.0 {
                        self.asks.remove(&OrderedFloat(price));
                    } else {
                        // Reject any ask that would cross below the best bid.
                        if let Some(best_bid) = self.best_bid()
                            && price <= best_bid
                        {
                            warn!(
                                "[kraken] rejecting ask update at {:.2} <= best bid {:.2} (cross guard)",
                                price, best_bid
                            );
                            continue;
                        }
                        self.asks.insert(OrderedFloat(price), amount);
                    }
                }
            }
        }
        self.repair_crossing();
    }

    /// Repair a crossed book (best bid >= best ask).
    ///
    /// Both sides are cleared and the book awaits the next full snapshot.
    /// With the per-update crossing guard above a cross should not arise in
    /// `apply_update`; this exists as a safety net for partial snapshots and
    /// stale reconnect state.
    fn repair_crossing(&mut self) {
        if let (Some(b), Some(a)) = (self.best_bid(), self.best_ask())
            && b >= a
        {
            warn!(
                "[kraken] detected crossed book (bid {:.2} >= ask {:.2}); clearing stale book",
                b, a
            );
            self.bids.clear();
            self.asks.clear();
        }
    }
    /// Process a Kraken WebSocket message, applying ALL levels to the
    /// in-memory book without any pre-filtering.
    ///
    /// After applying the levels, sequence-number continuity and (when the
    /// exchange provides one) the CRC32 checksum are validated. A detected
    /// gap, out-of-order message, or checksum mismatch flags the book via
    /// [`needs_resync`]; the owning adapter is expected to call [`reset`] and
    /// await a fresh snapshot before emitting again.
    pub fn process_msg(&mut self, msg: &KrakenWsMessage) {
        let data = match msg.data.first() {
            Some(d) => d,
            None => return,
        };

        let action = msg.msg_type.as_deref().unwrap_or("snapshot");

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

        // Integrity checks after the levels are applied (the checksum the
        // exchange computed covers the post-update book state).
        self.track_sequence(msg.sequence);
        if let Some(checksum) = data.get("checksum").and_then(|c| c.as_i64())
            && checksum != 0
        {
            self.verify_checksum(checksum);
        }
    }

    /// Record the latest `book`-channel sequence number and flag a resync on
    /// any discontinuity (gap, duplicate, or out-of-order message).
    pub fn track_sequence(&mut self, seq: Option<u64>) {
        let Some(seq) = seq else { return };
        match self.last_sequence {
            None => {}
            Some(last) => {
                if seq <= last {
                    warn!(
                        "[kraken] out-of-order/duplicate sequence: got {} after {} (reconnect/dup)",
                        seq, last
                    );
                    self.needs_resync = true;
                } else if seq > last + 1 {
                    warn!(
                        "[kraken] sequence gap: expected {} got {} (lost {} messages)",
                        last + 1,
                        seq,
                        seq - last - 1
                    );
                    self.needs_resync = true;
                }
            }
        }
        self.last_sequence = Some(seq);
    }

    /// Compare the local CRC32 of the top-10 levels against the
    /// exchange-supplied `checksum`. Returns `true` on match (or when the
    /// exchange sent no checksum). On mismatch a warning is logged and
    /// [`checksum_failed`](Self::checksum_failed) is flagged.
    ///
    /// A checksum mismatch is **not**, by itself, treated as ground truth for
    /// dropping the book: the exact Kraken CRC32 string format cannot be
    /// unambiguously verified without a live test vector, and Kraken WS v2
    /// sends only one snapshot per (re)subscribe with no mid-stream
    /// resnapshot — clearing on every mismatch would silently starve the stream
    /// if the local algorithm ever drifts. Real corruption (lost messages) is
    /// unambiguously signalled by a sequence gap/out-of-order message, which
    /// [`needs_resync`](Self::needs_resync) does treat as authoritative.
    pub fn verify_checksum(&mut self, checksum: i64) -> bool {
        if checksum <= 0 {
            return true;
        }
        let computed = self.compute_checksum(10);
        if (computed as i64) != checksum {
            warn!(
                "[kraken] checksum mismatch: local {} != exchange {} (sequence continuity is the authoritative resync signal)",
                computed, checksum
            );
            self.checksum_failed = true;
            return false;
        }
        true
    }

    /// Best-effort CRC32 of the top-`depth` levels, mirroring Kraken's
    /// documented book-checksum algorithm (and ccxt's `handle_order_book`):
    /// the top `depth` asks (price ascending) and top `depth` bids (price
    /// descending) are each serialized as `format_number(price) ++
    /// format_number(size)`, concatenated asks-then-bids without separators,
    /// and CRC32'd. `format_number` joins the integer and decimal parts of the
    /// number and strips leading zeros, per Kraken/ccxt.
    pub fn compute_checksum(&self, depth: usize) -> u32 {
        let mut s = String::new();
        // Asks first (best/lowest first), then bids (best/highest first).
        for (k, v) in self.asks.iter().take(depth) {
            s.push_str(&format_number(k.0));
            s.push_str(&format_number(*v));
        }
        for (k, v) in self.bids.iter().take(depth) {
            s.push_str(&format_number(k.0.0));
            s.push_str(&format_number(*v));
        }
        crc32fast::hash(s.as_bytes())
    }

    /// Returns `true` when the book must be resynced (sequence gap,
    /// out-of-order message, or duplicate sequence). Sequence discontinuity
    /// is the authoritative corruption signal: it indicates messages were lost
    /// and the local book can no longer be reconciled without a fresh snapshot.
    pub fn needs_resync(&self) -> bool {
        self.needs_resync
    }

    /// Returns `true` when the last `verify_checksum` call detected a mismatch.
    /// Cleared by [`reset`](Self::reset) and by a fresh
    /// [`apply_snapshot`](Self::apply_snapshot). Observability signal only.
    pub fn checksum_failed(&self) -> bool {
        self.checksum_failed
    }

    /// Discard all book levels and sync state. Called by the adapter after
    /// `on_reconnect` and when `needs_resync` is flagged, so the book is
    /// re-seeded by the next full snapshot.
    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_sequence = None;
        self.needs_resync = false;
        self.checksum_failed = false;
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

    /// Create a LobItem containing **all** in-memory levels — no filtering.
    ///
    /// Guaranteed to return every level received from the WebSocket and stored
    /// via `process_msg` / `apply_snapshot` / `apply_update`. Filtering by
    /// `max_level` / `max_level_pct` is applied only in [`to_lob_item`].
    pub fn full_lob_item(&self, ts: u64, exchange: &str) -> Option<LobItem> {
        self.to_lob_item(ts, exchange, None, 0.0)
    }

    /// Create a LobItem with post-filtering applied.
    ///
    /// This is the **only** place where `max_level` / `max_level_pct` filters
    /// are applied. The in-memory book is never mutated.
    ///
    /// Sorts bids ascending (worst to best, so best_bid is last element)
    /// and asks ascending (best to worst, so best_ask is first element).
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
    fn test_apply_snapshot_replaces_levels() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0), (49900.0, 2.0)], Side::Bid);
        assert_eq!(book.num_bids(), 2);
        assert!((book.best_bid().unwrap() - 50000.0).abs() < f64::EPSILON);

        book.apply_snapshot(&[(49800.0, 3.0)], Side::Bid);
        assert_eq!(book.num_bids(), 1);
    }

    #[test]
    fn test_apply_update_upserts() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_update(&[(50000.0, 5.0)], Side::Bid);
        assert_eq!(
            *book.bids.get(&Reverse(OrderedFloat(50000.0))).unwrap(),
            5.0
        );
    }

    #[test]
    fn test_apply_update_removes() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0), (49900.0, 2.0)], Side::Bid);
        book.apply_update(&[(50000.0, 0.0)], Side::Bid);
        assert_eq!(book.num_bids(), 1);
    }

    #[test]
    fn test_spread_calculation() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        let s = book.spread();
        assert!(s.is_some());
        assert!((s.unwrap() - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_display_contains_counts() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0), (49900.0, 2.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 3.0)], Side::Ask);
        let out = book.display("XBT/USD", 100.0);
        assert!(out.contains("bids=2"));
        assert!(out.contains("asks=1"));
    }

    #[test]
    fn test_process_msg_snapshot() {
        let json = r#"{
            "channel": "book",
            "type": "snapshot",
            "data": [{
                "symbol": "XBT/USD",
                "bids": [
                    {"price": 50000.0, "qty": 1.0},
                    {"price": 49900.0, "qty": 2.0}
                ],
                "asks": [
                    {"price": 50100.0, "qty": 1.5}
                ],
                "checksum": 0,
                "timestamp": "2024-01-15T10:30:00.000000Z"
            }]
        }"#;
        let msg = KrakenWsMessage::from_json(json).unwrap();
        let mut book = OrderBook::new();
        book.process_msg(&msg);
        assert_eq!(book.num_bids(), 2);
        assert_eq!(book.num_asks(), 1);
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
        book.apply_snapshot(&[(50000.0, 1.0), (49900.0, 2.0), (49800.0, 3.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0), (50200.0, 2.0), (50300.0, 3.0)], Side::Ask);
        let lob = book.to_lob_item(0, "test", Some(2), 0.0).unwrap();
        assert_eq!(lob.bids.len(), 2);
        assert_eq!(lob.asks.len(), 2);
    }

    #[test]
    fn test_to_lob_item_sorts_bids_with_best_bid_last() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        book.apply_update(&[(49900.0, 2.0)], Side::Bid);
        book.apply_update(&[(49800.0, 3.0)], Side::Bid);
        let lob = book.to_lob_item(0, "test", Some(10), 0.0).unwrap();
        assert_eq!(lob.bids.len(), 3);
        assert!(
            (lob.bids[2].price - 50000.0).abs() < f64::EPSILON,
            "Best bid (50000.0) should be last element"
        );
    }

    #[test]
    fn test_to_lob_item_sorts_asks_with_best_ask_first() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_update(&[(50200.0, 2.0)], Side::Ask);
        book.apply_update(&[(50300.0, 3.0)], Side::Ask);
        let lob = book.to_lob_item(0, "test", Some(10), 0.0).unwrap();
        assert_eq!(lob.asks.len(), 3);
        assert!(
            (lob.asks[0].price - 50100.0).abs() < f64::EPSILON,
            "Best ask (50100.0) should be first element"
        );
    }

    #[test]
    fn test_to_lob_item_pct_zero_keeps_all_levels() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0), (49900.0, 2.0), (49800.0, 3.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0), (50200.0, 2.0), (50300.0, 3.0)], Side::Ask);
        // max_level_pct = 0.0 should be normalized to 100.0 → no filtering
        let lob = book.to_lob_item(0, "test", None, 0.0).unwrap();
        assert_eq!(lob.bids.len(), 3, "pct=0.0 should keep all bids");
        assert_eq!(lob.asks.len(), 3, "pct=0.0 should keep all asks");
    }

    #[test]
    fn test_to_lob_item_pct_100_keeps_all_levels() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        let lob = book.to_lob_item(0, "test", None, 100.0).unwrap();
        assert_eq!(lob.bids.len(), 1);
        assert_eq!(lob.asks.len(), 1);
    }

    #[test]
    fn test_to_lob_item_pct_above_100_keeps_all_levels() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0), (40000.0, 2.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        // max_level_pct = 150.0 should be normalized to 100.0 → no filtering
        let lob = book.to_lob_item(0, "test", None, 150.0).unwrap();
        assert_eq!(lob.bids.len(), 2, "pct=150.0 should keep all bids");
        assert_eq!(lob.asks.len(), 1);
    }

    // ------------------------------------------------------------------
    // Guarantee: in-memory OrderBook retains ALL levels from every WS
    // snapshot/update. Filtering happens ONLY in to_lob_item /
    // full_lob_item — never during processing.
    // ------------------------------------------------------------------

    #[test]
    fn test_full_lob_item_returns_all_levels() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[
                (50000.0, 1.0),
                (49900.0, 2.0),
                (49800.0, 3.0),
                (49700.0, 4.0),
            ],
            Side::Bid,
        );
        book.apply_snapshot(
            &[
                (50100.0, 1.0),
                (50200.0, 2.0),
                (50300.0, 3.0),
                (50400.0, 4.0),
            ],
            Side::Ask,
        );
        let lob = book.full_lob_item(0, "kraken").unwrap();
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
                (50000.0, 1.0),
                (49900.0, 2.0),
                (49800.0, 3.0),
                (49700.0, 4.0),
            ],
            Side::Bid,
        );
        book.apply_snapshot(
            &[
                (50100.0, 1.0),
                (50200.0, 2.0),
                (50300.0, 3.0),
                (50400.0, 4.0),
            ],
            Side::Ask,
        );
        // to_lob_item with max_level=1 must produce a 1-level lob...
        let lob = book.to_lob_item(0, "kraken", Some(1), 0.0).unwrap();
        assert_eq!(lob.bids.len(), 1, "filtered lob should have 1 bid");
        assert_eq!(lob.asks.len(), 1, "filtered lob should have 1 ask");
        // ...but the in-memory book must STILL contain all 4 levels.
        assert_eq!(book.num_bids(), 4, "memory book must retain all 4 bids");
        assert_eq!(book.num_asks(), 4, "memory book must retain all 4 asks");
    }

    #[test]
    fn test_full_lob_item_equals_unfiltered_to_lob_item() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0), (49900.0, 2.0), (49800.0, 3.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0), (50200.0, 2.0), (50300.0, 3.0)], Side::Ask);
        let full = book.full_lob_item(0, "kraken").unwrap();
        let unfiltered = book.to_lob_item(0, "kraken", None, 0.0).unwrap();
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

    // ------------------------------------------------------------------
    // Crossing guard: the in-memory book must never allow the best bid
    // to rise above the best ask (or the best ask to fall below the
    // best bid). Such a state is a corrupt book caused by a stale or
    // out-of-order update; once crossed the book stays crossed for all
    // subsequent updates unless explicitly repaired.
    // ------------------------------------------------------------------

    #[test]
    fn test_update_cannot_cross_book_as_bid() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(49900.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        assert!(book.best_bid() < Some(50100.0));

        // An update that would push a bid above the best ask must be guarded.
        book.apply_update(&[(50200.0, 5.0)], Side::Bid);
        assert!(
            book.best_bid().map(|b| b <= 50100.0).unwrap_or(true),
            "best bid must not exceed best ask after update, but best_bid={:?} best_ask={:?}",
            book.best_bid(),
            book.best_ask()
        );
    }

    #[test]
    fn test_update_cannot_cross_book_as_ask() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);

        // An update that would push an ask below the best bid must be guarded.
        book.apply_update(&[(49950.0, 5.0)], Side::Ask);
        assert!(
            book.best_ask().map(|a| a >= 50000.0).unwrap_or(true),
            "best ask must not fall below best bid after update, but best_bid={:?} best_ask={:?}",
            book.best_bid(),
            book.best_ask()
        );
    }

    #[test]
    fn test_to_lob_item_with_filter_returns_fewer_levels_than_full() {
        let mut book = OrderBook::new();
        book.apply_snapshot(
            &[
                (50000.0, 1.0),
                (49900.0, 2.0),
                (49800.0, 3.0),
                (49700.0, 4.0),
                (49600.0, 5.0),
            ],
            Side::Bid,
        );
        book.apply_snapshot(
            &[
                (50100.0, 1.0),
                (50200.0, 2.0),
                (50300.0, 3.0),
                (50400.0, 4.0),
                (50500.0, 5.0),
            ],
            Side::Ask,
        );
        let full = book.full_lob_item(0, "kraken").unwrap();
        let filtered = book.to_lob_item(0, "kraken", Some(2), 0.0).unwrap();
        assert_eq!(full.bids.len(), 5, "memory has 5 bids");
        assert_eq!(filtered.bids.len(), 2, "filtered lob has 2 bids");
        assert_eq!(full.asks.len(), 5, "memory has 5 asks");
        assert_eq!(filtered.asks.len(), 2, "filtered lob has 2 asks");
    }

    // ------------------------------------------------------------------
    // CRC32 checksum formatting (mirrors Kraken/ccxt `format_number` so the
    // locally-computed checksum has the best chance of matching the exchange).
    // ------------------------------------------------------------------

    #[test]
    fn test_format_number_strips_decimal_and_leading_zeros() {
        assert_eq!(format_number(50001.5), "500015");
        assert_eq!(format_number(0.5), "5");
        assert_eq!(format_number(50000.0), "50000");
        assert_eq!(format_number(1.0), "1");
        assert_eq!(format_number(0.5666), "5666");
    }

    #[test]
    fn test_compute_checksum_is_deterministic_and_differs_by_state() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0), (49900.0, 2.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.5), (50200.0, 0.5)], Side::Ask);

        let c1 = book.compute_checksum(10);
        let c2 = book.compute_checksum(10);
        assert_eq!(c1, c2, "compute_checksum must be deterministic");

        // A different book state must yield a different checksum.
        let mut other = book.clone();
        other.apply_snapshot(&[(50000.0, 99.0)], Side::Bid);
        let c3 = other.compute_checksum(10);
        assert_ne!(c1, c3, "different book state must change the checksum");
    }

    // ------------------------------------------------------------------
    // Integrity guardrails: sequence-number continuity and CRC32 checksum
    // validation. A detected gap/out-of-order/checksum-mismatch marks the
    // book as needing a resync (clear + await a fresh snapshot) so a
    // silently-corrupted, non-crossed book is never emitted.
    // ------------------------------------------------------------------

    #[test]
    fn test_sequence_gap_triggers_resync() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        book.track_sequence(Some(1));
        assert!(!book.needs_resync());
        // Jump from seq 1 to seq 5: a gap (messages 2..4 were lost).
        book.track_sequence(Some(5));
        assert!(book.needs_resync(), "a sequence gap must flag resync");
    }

    #[test]
    fn test_sequence_out_of_order_triggers_resync() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        book.track_sequence(Some(10));
        assert!(!book.needs_resync());
        // A sequence that goes backwards is a corrupt/reordered stream.
        book.track_sequence(Some(4));
        assert!(
            book.needs_resync(),
            "out-of-order sequence must flag resync"
        );
    }

    #[test]
    fn test_sequence_duplicate_is_a_gap() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        book.track_sequence(Some(3));
        // Duplicate sequence number (3 again) is not a +1 progression.
        book.track_sequence(Some(3));
        assert!(book.needs_resync(), "duplicate sequence must flag resync");
    }

    #[test]
    fn test_sequence_monotonic_keeps_book_healthy() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        book.track_sequence(Some(1));
        book.track_sequence(Some(2));
        book.track_sequence(Some(3));
        assert!(
            !book.needs_resync(),
            "contiguous sequence must not flag resync"
        );
    }

    #[test]
    fn test_sequence_none_is_noop() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        book.track_sequence(None);
        book.track_sequence(None);
        assert!(!book.needs_resync());
    }

    #[test]
    fn test_checksum_mismatch_flags_without_resetting_book() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        // Deliberately wrong checksum: must warn + set the observability flag,
        // but must NOT drop the book (warn-only best-effort; sequence gaps are
        // the authoritative reset signal).
        let computed = book.compute_checksum(10);
        let wrong = (computed as i64).wrapping_add(1);
        assert!(!book.verify_checksum(wrong), "mismatch must return false");
        assert!(book.checksum_failed(), "mismatch must flag checksum_failed");
        assert!(!book.needs_resync(), "mismatch must NOT flag needs_resync");
        // Book levels are retained (not cleared by a checksum mismatch).
        assert_eq!(book.num_bids(), 1);
        assert_eq!(book.num_asks(), 1);
    }

    #[test]
    fn test_checksum_match_does_not_trigger_resync() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        let computed = book.compute_checksum(10);
        assert!(
            book.verify_checksum(computed as i64),
            "match must return true"
        );
        assert!(!book.needs_resync());
    }

    #[test]
    fn test_checksum_zero_skips_validation() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        // checksum == 0 means "no checksum sent" (e.g. test fixtures): skip.
        assert!(book.verify_checksum(0));
        assert!(!book.needs_resync());
    }

    #[test]
    fn test_reset_clears_book_and_sync_state() {
        let mut book = OrderBook::new();
        book.apply_snapshot(&[(50000.0, 1.0)], Side::Bid);
        book.apply_snapshot(&[(50100.0, 1.0)], Side::Ask);
        book.track_sequence(Some(7));
        // Corrupt the book via a deliberate checksum mismatch, then reset.
        let computed = book.compute_checksum(10);
        book.verify_checksum((computed as i64).wrapping_add(1));
        assert!(book.checksum_failed());
        assert_eq!(book.num_bids(), 1);
        book.reset();
        assert_eq!(book.num_bids(), 0);
        assert_eq!(book.num_asks(), 0);
        assert!(book.best_bid().is_none());
        assert!(!book.needs_resync());
        assert!(!book.checksum_failed(), "reset must clear checksum_failed");
    }
}
