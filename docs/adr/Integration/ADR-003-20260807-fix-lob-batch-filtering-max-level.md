# ADR-003: Fix LOB Batch Filtering Max Level Constraint

## Status
Proposed (2026-08-07)

## Context

When processing LOB updates with `max_level` configuration, the order book could exceed the configured limit. The bug manifested when:

1. A trade consumed all levels on one side of the book
2. An update arrived with many new levels
3. The filter incorrectly allowed all levels to pass

### Example Log
```
2026-08-07T18:28:00.943614Z LOB: bids=[], asks=[64829.1:0.28]
2026-08-07T18:28:01.043637Z LOB: bids=[60 levels...], asks=[64829.1:0.04]
```

The bids should have been limited to `max_level` but showed 60 levels.

## Problem

In the `filter_levels` function, `current_levels_on_side` was computed once at the start of filtering:

```rust
let current_levels_on_side = match side {
    Side::Bid => self.num_bids(),
    Side::Ask => self.num_asks(),
};
```

This value never updated for levels accepted within the batch. With `max_level=3` and `current_levels=0`:

| Level | price_exists | current | condition | Result |
|-------|--------------|---------|-----------|--------|
| L1 | false | 0 | 0<3=true | INCLUDED |
| L2 | false | 0 | 0<3=true | INCLUDED (BUG: should be 1<3) |
| L3 | false | 0 | 0<3=true | INCLUDED (BUG: should be 2<3) |
| L4 | false | 0 | 0<3=true | INCLUDED (BUG: should be 3<3=false) |

## Decision

Track `included_in_batch` counter and compute `current_levels_on_side` dynamically during iteration:

```rust
let initial_levels_on_side = match side {
    Side::Bid => self.num_bids(),
    Side::Ask => self.num_asks(),
};
let mut included_in_batch = 0usize;

levels.iter().filter(|level| {
    let current_levels_on_side = initial_levels_on_side + included_in_batch;
    let include = filter.should_include(/* ... */);
    if include {
        included_in_batch += 1;
    }
    include
})
```

## Consequences

### Positive
- `max_level` constraint is correctly enforced
- All exchanges (OKX, Kraken, Bitstamp) behave consistently
- Existing prices in updates are always included (correct behavior)

### Negative
- Slightly more complex filter logic
- Counter increment happens inside filter closure

## Affected APIs

- `src/okx/lob.rs:filter_levels`
- `src/kraken/lob.rs:filter_levels`
- `src/bitstamp/lob.rs:filter_orderbook`

## Tests Added

- `test_filter_levels_batch_respects_max_level` - empty book, batch respects limit
- `test_filter_levels_batch_respects_max_level_asks` - same for asks side
- `test_filter_levels_existing_price_always_included` - existing prices pass filter
- `test_filter_levels_with_existing_levels_respects_max_level` - counts existing levels
- `test_filter_levels_existing_price_in_updates_included_always` - updates with existing prices

## Alternatives Considered

1. **Filter at output time in `normalize_lob`** - Would require changing the output path; current approach fixes the root cause in the filtering logic.

2. **Use `retain` with external counter** - Similar complexity; current approach is clearer with local counter.

3. **Sort and truncate after filtering** - Would lose the ability to prioritize existing price updates.

## Related Issues

- Issue #26 - LOB batch filtering bug
