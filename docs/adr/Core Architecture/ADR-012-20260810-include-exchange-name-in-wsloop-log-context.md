# ADR-012: Include Exchange Name in Wsloop Log Context

- **Category**: Core Architecture
- **Status**: Accepted
- **Implemented**: (pending PR)
- **Created**: 2026-08-10 15:30
- **Deciders**: Implementation team

## Context

The wsloop (`src/wsloop.rs`) provides a single reconnect/backoff loop shared across all exchange adapters (OKX, Kraken, Bitstamp). Log statements emitted from this loop previously included only `instrument={instrument}` and `url={url}`, but no exchange name. When running against multiple exchanges simultaneously, it was impossible to identify which exchange a log line referred to without inspecting the URL.

## Options Considered

### Option A: Add `exchange()` to `ExchangeAdapter` trait

Add a required `fn exchange(&self) -> &str` method to the trait. Each adapter already stores the exchange name internally (`"okx"`, `"kraken"`, `"bitstamp"`), so this is a trivial passthrough. The wsloop clones the value once before spawning the worker task and includes it in all log statements.

**Pros:**
- Clean, idiomatic trait extension
- All log statements gain exchange context
- No changes to adapter data models (field already exists)
- Consistent with the existing `instrument()` pattern

**Cons:**
- All implementors must add the new method (only 3 adapters, all trivial)

### Option B: Parse exchange from URL

Derive the exchange name from the URL string in the wsloop.

**Pros:**
- No trait change needed

**Cons:**
- Brittle: URL strings can change; parsing adds fragility
- Inconsistent with existing `instrument()` pattern
- Adds runtime parsing overhead

### Option C: Pass exchange as separate parameter

Pass the exchange name as a separate string parameter to `run_exchange_stream`.

**Pros:**
- No trait change needed

**Cons:**
- Duplicates information already available on the adapter
- Easy to pass inconsistent values (exchange name vs. actual adapter)
- Breaks the current adapter-encapsulation pattern

## Decision

Chosen **Option A**: Add `fn exchange(&self) -> &str` to the `ExchangeAdapter` trait.

The exchange name is already stored on every adapter struct; the trait just didn't expose it. Adding a trait method is the most idiomatic and maintainable approach, consistent with the existing `instrument()` accessor. The wsloop clones the value once before spawning the worker task and includes `exchange={exchange}` in all 13 log statements.

## Consequences

### Positive
- All wsloop log statements now include the originating exchange name
- Operators can filter and correlate logs per exchange when running multi-exchange ingestion
- No behavioral or performance impact (string clone once per connection lifecycle)

### Negative
- New required trait method — any future `ExchangeAdapter` implementors must provide it. This is intentional and low-burden.
