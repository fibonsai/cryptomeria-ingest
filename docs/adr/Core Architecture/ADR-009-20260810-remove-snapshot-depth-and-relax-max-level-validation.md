# ADR-009: Remove snapshot_depth and relax max_level validation

- **Category**: Core Architecture
- **Status**: Accepted
- **Implemented**: N/A
- **Created**: 2026-08-10 11:15

## Context

The `DataSourceConfig` struct carried a `snapshot_depth` field (default 400) that was intended to control the depth of REST order-book snapshots. However, analysis of the codebase revealed that **no exchange actually used it**:

- **Bitstamp** (`src/bitstamp/ws.rs`): `fetch_snapshot()` built the REST URL `{rest_url}/order_book/{instrument}` with no depth query parameter — the field was stored on the adapter but never read in the URL.
- **OKX** (`src/okx/lob.rs`): `with_snapshot_depth` ignored its `depth` argument and delegated to `OrderBook::new()`.
- **Kraken** (`src/kraken/lob.rs`): same as OKX — `with_snapshot_depth` ignored the depth argument.

Additionally, the `MaxLevelAndPctConflict` validation rule prevented users from setting both `max_level` and `max_level_pct` simultaneously, even though the `to_lob_item` filtering logic already supported applying both filters (percentage first, then level-count cap). The `max_level_pct` field also lacked normalization: a value of `0.0` meant "no filtering" (via `if max_level_pct > 0.0`), but `100.0` and values above 100 caused degenerate threshold calculations (e.g., `best * (1.0 - 100/100) = 0.0`), silently filtering out all levels.

## Decision

1. **Remove `snapshot_depth`** from `DataSourceConfig` entirely, along with the `default_snapshot_depth()` helper, the `InvalidSnapshotDepth` error variant, and all plumbing through the exchange adapter constructors and `build_channel_streams` mod.rs files.

2. **For Bitstamp**, use `max_level` as the REST snapshot depth parameter: the `fetch_snapshot()` method now appends `?group=N` to the REST URL where `N` is `max_level` if set, otherwise defaults to `400` (Bitstamp's maximum).

3. **Remove the `MaxLevelAndPctConflict`** validation error and the `with_snapshot_depth` method from the `OrderBook` trait, so both filters can be applied concurrently. The existing `to_lob_item` filter order (percentage first, then `max_level` cap) is now reachable.

4. **Normalize `max_level_pct`** inside each exchange's `to_lob_item`: values of `0.0`, `100.0`, or `>= 100.0` are treated as `100.0` (no percentage filtering). The filter condition changes from `max_level_pct > 0.0` to `max_level_pct < 100.0`.

5. **Remove the `with_snapshot_depth` method** from the `OrderBook` trait and all three exchange implementations, since it was a no-op in OKX/Kraken and a trivial identity in Bitstamp.

## Consequences

### Positive

- Removes dead configuration (`snapshot_depth`) that silently did nothing on OKX and Kraken and was never wired into Bitstamp's REST URL.
- Allows users to combine `max_level` and `max_level_pct` for more precise control (e.g., "top 5 levels within 1% of the best price").
- `max_level_pct = 0.0` and `100.0` now both correctly mean "no filtering" — no more degenerate threshold behavior.
- Fewer public API surface area to document and maintain.
- Bitstamp REST snapshots now respect the `max_level` setting, allowing users to request fewer levels for faster initial sync.

### Negative

- **Breaking API change**: `DataSourceConfig` loses the `snapshot_depth` field. Any downstream code using the struct literal syntax directly will fail to compile. TOML/JSON configs that set `snapshot_depth` will get a serde deserialization error.
- Users who relied on the (undocumented, unused) `with_snapshot_depth` trait method on `OrderBook` must remove their calls.
- The default `max_level_pct` of `0.0` still means "no filtering" but now this is achieved via normalization to `100.0` rather than a `> 0.0` guard.

### Neutral

- `max_level` and `max_level_pct` remain **optional** even when `Lob` is in `data_kind` — they default to full depth / no filter. Only `max_level` without `Lob` in `data_kind` is still a validation error.
- The `Default` impl for `DataSourceConfig` no longer sets `snapshot_depth: 400`; the Bitstamp default depth of 400 is now baked into `fetch_snapshot()` as a fallback.

## References

- Issue #47: Bitstamp: use max_level as snapshot depth, remove snapshot_depth; make max_level and max_level_pct mandatory when data_kind includes Lob
- `src/config.rs` — validated configuration structure
- `src/traits.rs` — `OrderBook` trait
- `src/bitstamp/ws.rs` — `fetch_snapshot()` REST URL construction
- `src/okx/lob.rs`, `src/kraken/lob.rs`, `src/bitstamp/lob.rs` — `to_lob_item` filter logic
