/// Type alias for a single vector of (price, size) levels.
pub type LevelVec = Vec<(f64, f64)>;
/// Type alias for the (bids, asks) return type of `levels_within_pct`.
pub type LevelsWithinPct = (LevelVec, LevelVec);

/// Shared OrderBook trait — methods common across all exchange order books.
pub trait OrderBook {
    fn new() -> Self;
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
