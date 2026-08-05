use serde::Serialize;

/// Type alias for a single vector of (price, size) levels.
pub type LevelVec = Vec<(f64, f64)>;
/// Type alias for the (bids, asks) return type of `levels_within_pct`.
pub type LevelsWithinPct = (LevelVec, LevelVec);

/// LOB pre-filter configuration.
///
/// - `MaxLevelPct(f64)`: only keep levels within `pct%` of the best price.
/// - `MaxLevel(usize)`: only keep the top N best levels per side.
#[derive(Debug, Clone, Copy, Serialize)]
pub enum LobFilter {
    MaxLevelPct(f64),
    MaxLevel(usize),
}

impl LobFilter {
    /// Determine whether a level should be included in the LOB.
    ///
    /// Levels with `amount == 0` (removals) are always included to maintain
    /// LOB consistency. For `MaxLevel`, price updates at existing levels are
    /// always included.
    #[allow(clippy::too_many_arguments)]
    pub fn should_include(
        &self,
        best_bid: Option<f64>,
        best_ask: Option<f64>,
        price: f64,
        amount: f64,
        side_is_bid: bool,
        current_levels_on_side: usize,
        price_exists: bool,
    ) -> bool {
        if amount == 0.0 {
            return true;
        }
        match *self {
            LobFilter::MaxLevelPct(pct) => {
                let best = if side_is_bid { best_bid } else { best_ask };
                match best {
                    None => true,
                    Some(best_price) => {
                        if side_is_bid {
                            price >= best_price * (1.0 - pct / 100.0)
                        } else {
                            price <= best_price * (1.0 + pct / 100.0)
                        }
                    }
                }
            }
            LobFilter::MaxLevel(max) => {
                if price_exists {
                    return true;
                }
                current_levels_on_side < max
            }
        }
    }
}

/// Shared OrderBook trait — methods common across all exchange order books.
pub trait OrderBook {
    fn new() -> Self;
    fn with_snapshot_depth(depth: usize) -> Self;
    fn num_bids(&self) -> usize;
    fn num_asks(&self) -> usize;
    fn best_bid(&self) -> Option<f64>;
    fn best_ask(&self) -> Option<f64>;
    fn spread(&self) -> Option<f64>;
    fn levels_within_pct(&self, top_pct: f64) -> LevelsWithinPct;
    fn total_bid_size(&self) -> f64;
    fn total_ask_size(&self) -> f64;
    fn display(&self, instrument: &str, top_pct: f64) -> String;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lob_filter_pct_bid_inclusion() {
        let filter = LobFilter::MaxLevelPct(1.0);
        assert!(filter.should_include(Some(100.0), Some(101.0), 99.5, 1.0, true, 0, false));
        assert!(!filter.should_include(Some(100.0), Some(101.0), 98.0, 1.0, true, 0, false));
    }

    #[test]
    fn test_lob_filter_pct_ask_inclusion() {
        let filter = LobFilter::MaxLevelPct(1.0);
        assert!(filter.should_include(Some(100.0), Some(101.0), 101.5, 1.0, false, 0, false));
        assert!(!filter.should_include(Some(100.0), Some(101.0), 103.0, 1.0, false, 0, false));
    }

    #[test]
    fn test_lob_filter_zero_amount_always_included() {
        let filter = LobFilter::MaxLevelPct(0.5);
        assert!(filter.should_include(Some(100.0), Some(101.0), 50.0, 0.0, true, 5, false));
        assert!(filter.should_include(Some(100.0), Some(101.0), 200.0, 0.0, false, 5, false));
    }

    #[test]
    fn test_lob_filter_max_level_count() {
        let filter = LobFilter::MaxLevel(3);
        assert!(filter.should_include(Some(100.0), Some(101.0), 100.0, 1.0, true, 0, false));
        assert!(filter.should_include(Some(100.0), Some(101.0), 99.0, 1.0, true, 1, false));
        assert!(filter.should_include(Some(100.0), Some(101.0), 98.0, 1.0, true, 2, false));
        assert!(!filter.should_include(Some(100.0), Some(101.0), 97.0, 1.0, true, 3, false));
        assert!(filter.should_include(Some(100.0), Some(101.0), 100.0, 1.0, true, 3, true));
    }

    #[test]
    fn test_lob_filter_empty_book_allows_all() {
        let filter = LobFilter::MaxLevelPct(0.5);
        assert!(filter.should_include(None, None, 100.0, 1.0, true, 0, false));
    }
}
