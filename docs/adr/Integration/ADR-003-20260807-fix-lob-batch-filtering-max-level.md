# ADR-003: Fix LOB batch filtering to respect max_level configuration

**Category**: Integration  
**Status**: Accepted  
**Created**: 2026-08-07 19:30

## Context

The `max_level` filter (configured via `LobFilter::MaxLevel`) was not correctly enforced when processing batched price level updates. When a snapshot cleared the order book and a subsequent update added many levels in a single batch, all levels passed the filter because `current_levels_on_side` was computed once before filtering, reflecting the empty book state.

This affected all three exchange adapters:
- `src/okx/lob.rs` - `filter_levels` function
- `src/kraken/lob.rs` - `filter_levels` function  
- `src/bitstamp/lob.rs` - `filter_orderbook` function

The bug allowed more price levels than configured, potentially causing memory issues and incorrect market data representation.

## Options Considered

### Option 1: Track included levels within batch (chosen)

Add a counter that increments each time a level is included in the filtered result. Compute `current_levels_on_side = initial_levels + included_in_batch` for each level evaluation.

Pros:
- Minimal code change
- Preserves existing filter logic
- Works for both bids and asks
- Existing price updates still always pass (price_exists check)

Cons:
- Requires moving some variables outside the filter closure

### Option 2: Pre-filter and sort batch before applying filter

Sort all levels by price (best first), truncate to max_level, then apply filter.

Pros:
- Simpler logic conceptually
- Guarantees correct ordering

Cons:
- More complex for incremental updates where existing prices must always pass
- Changes the filtering semantics (snapshot vs update handling)

### Option 3: Apply filter after batch is applied to book

Apply all levels to the book first, then re-filter the entire book.

Pros:
- Simpler filter logic

Cons:
- Significant performance impact (re-processing entire book)
- Breaks snapshot-first stream pattern
- Could remove levels that were just added

## Decision

Implement Option 1: Track included levels within the batch using a mutable counter (`included_in_batch` for OKX/Kraken, `bids_included`/`asks_included` for Bitstamp).

Key implementation details:
- Hoist `best_bid`, `best_ask`, `side_is_bid` outside the filter closure
- Capture `initial_levels_on_side` before filtering
- For each level, compute `current_levels_on_side = initial_levels_on_side + included_in_batch`
- Increment `included_in_batch` only when a level is included
- Preserve existing behavior: existing price levels always pass (`price_exists` check)

## Consequences

### Positive
- Correctly enforces `max_level` configuration for batched updates
- Minimal code changes (~20 lines per exchange)
- Preserves all existing behavior for existing price updates
- Adds comprehensive test coverage (3 tests per exchange)

### Negative
- Slightly more complex filter closure logic
- Mutable counter in functional-style filter (acceptable for this use case)

### Neutral
- No API changes
- No configuration changes required
- Existing deployments will automatically get correct behavior