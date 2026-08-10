# ADR-013: Include Channel Name and Reconnection State in Wsloop Log Context

- **Category**: Core Architecture
- **Status**: Accepted
- **Implemented**: (pending PR)
- **Created**: 2026-08-10 21:00
- **Deciders**: Implementation team

## Context

The wsloop (`src/wsloop.rs`) shared reconnect/backoff loop previously logged connection events with `exchange`, `instrument`, and `url`, but omitted the **channel name** and **reconnection state**. When running multiple instrument/channel connections simultaneously, operators could not identify which channel a connect/disconnect event referred to. Additionally, reconnection retries (backoff sleep, `on_reconnect` snapshot fetch) and fatal failures (`MaxReconnectsExceeded`) were silent — no log indicated the attempt number, backoff delay, or reason for the final failure.

## Options Considered

### Option A: Pre-compute channel_names before connect_async, log retries and failures

Move `channel_names` computation to the top of the `'outer` reconnect loop (before `connect_async`), making it available in all log statements. Add `warn!` logging for reconnection attempts (attempt number, delay, max_attempts) before the backoff sleep. Add `error!` logging before each `return Err(IngestError::MaxReconnectsExceeded(...))` return site. Change `on_reconnect` handling from `if let Ok` to `match` to capture and log `Err` cases.

**Pros:**
- All wsloop log statements include channel context for disambiguation
- Reconnection lifecycle is fully observable (connect → subscribe → disconnect → retry → fail)
- Failure reasons are logged with full context before the error propagates
- Minimal code change — reuses existing `channel_names` and `backoff_delay` logic
- Consistent with the existing `key=value` structured log style

**Cons:**
- Slightly more verbose log output during reconnection storms (acceptable — these are warnings/errors)

### Option B: Add a logging helper/macro

Create a helper function or macro that wraps all log calls with channel context.

**Pros:**
- DRY: channel context injected automatically

**Cons:**
- Over-engineered for a static loop with fixed context variables
- Would require refactoring every existing log call
- Introduces an abstraction layer with no behavioral benefit

### Option C: Only log channel name on connect, skip retry/failure logs

Partially address the issue by only adding channel to connect/disconnect logs.

**Pros:**
- Smaller change

**Cons:**
- Leaves reconnection retries and failures silent — the core observability gap remains
- Inconsistent: some events have channel context, others don't

## Decision

Chosen **Option A**: Pre-compute `channel_names` before `connect_async` and log reconnection retry state and failure reasons.

The `channel_names` string is computed once per loop iteration (one `subscribe_msgs()` call, same as before), making it available in both the connect-success and connect-failure paths. The retry log uses a `warn!` level (matching the existing silence-timeout warning), and fatal-failure logs use `error!` before the `MaxReconnectsExceeded` return. The `on_reconnect` hook is now a `match` that logs `warn!` on error.

## Consequences

### Positive
- All wsloop connection events include `channel={channel_names}` for disambiguation
- Reconnection attempts are logged with attempt number, backoff delay, and max_attempts
- Fatal failures (max attempts exceeded, snapshot fetch failure) are logged with full context before the error propagates
- Operators can now trace the full connect → disconnect → retry → fail lifecycle in logs

### Negative
- Reconnection storms produce more log output (acceptable — these are at `warn!` and `error!` levels)
- `subscribe_msgs()` is now called once per loop iteration rather than once per successful connection (negligible overhead — it's a pure function building JSON strings)
