# ADR-002: Drop the `log` facade and log directly through `rasant`

## Category

Core Architecture

## Status

Accepted

## Created

2026-08-07 13:30

## Context

The crate logged through the `log` crate's `LevelFilter` plus a thin
`src/logging.rs` wrapper that layered `info/warn/error/debug(source, msg)`
over a global `OnceLock<Mutex<rasant::Logger>>`. The `log` crate is a logging
facade; it does no output itself and was used only to parse `RUST_LOG` before
mapping onto `rasant::Level`. That is a dependency spent on a single
parse-and-convert step. Meanwhile the aggregation `logging` module duplicated
`rasant`'s own API.

Rust's standard library has no built-in logging API, so replacing `rasant`
with `println!`/`eprintln!` was considered and rejected. Instead we keep
`rasant` as the logging implementation, remove the facade, and call it
directly through a small internal accessor.

## Options Considered

- **Keep the `log` facade + `logging.rs` wrapper** (`logging::info(source, msg)`).
  Rejected: the facade is unused for actual dispatch (rasant owns output) and the
  wrapper only re-shapes `rasant`'s API.
- **Replace all output with `println!`/`eprintln!`**. Rejected: no log levels,
  no timestamping, and Rust std has no logging abstraction, so we would lose
  filtering and structured output.
- **Log directly through `rasant` without the facade** (chosen). `rasant::Level`
  implements `TryFrom<&str>`, parsing `RUST_LOG` directly; a comment-level
  `use crate::logger::logger as log;` aliases the accessor at each call site.

## Decision

1. Remove the direct `log = "0.4"` dependency from `Cargo.toml`.
2. Delete `src/logging.rs` and its public re-exports from `src/lib.rs`.
3. Add a small `src/logger.rs` exposing a `pub(crate)` accessor backed by a
   `static OnceLock<Mutex<rasant::Logger>>`. Lazy init reads `RUST_LOG` via
   `Level::try_from` (default `Level::Info`, plus a `warn` → `Level::Warning`
   alias to preserve prior behavior) and installs a stdout sink.
4. Call sites log through the aliased accessor, embedding the source label in
   the message, e.g. `log().lock().unwrap().log(Level::Info, &format!("[okx] event: ..."))`.

The logger stays internal to the crate; the previous public
`logging::{info, warn, error, debug, init}` crate-level functions are removed.

## Consequences

- **Positive**: one fewer dependency; single logging implementation; no manual
  `LevelFilter` mapping; `RUST_LOG` filtering retained.
- **Negative**: the former public logging helpers at crate level are gone
  (library consumers that called them must log themselves); tests no longer
  suppress strays log lines since the `log` facade is not initialized for
  consumers, but crate-internal usage is unchanged. `log` remains only as a
  transitive dependency of networking libs (`native-tls`, `reqwest`,
  `tungstenite`, `serial_test`), which is out of scope.