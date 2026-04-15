# Examples Guide

[Русская версия](ru/EXAMPLES.md)

All examples live in `zig/examples/` and can be built with:

```bash
cd zig
zig build examples
```

## Example Index

### `smoke.zig`

Minimal import/build smoke test. Useful when you only want to verify that the package is wired correctly as a dependency.

### `high_level_select.zig`

Covers:

- `Client.connectTcp`
- `client.newQuery`
- `client.Do`
- `BlockBuffer`
- `asFixed()` and reading `String` result columns

### `result_binding.zig`

Covers:

- `ResultBinding`
- `ResultBindingColumn`
- `OwnedByteSlices`
- primitive typed sinks
- `Query.metrics`

### `high_level_insert_stream.zig`

Covers:

- `Query.input`
- `Query.on_input`
- streaming `INSERT`
- column builders `initOwnedFixedColumn` and `initOwnedStringColumn`

### `high_level_handlers.zig`

Covers:

- `on_result`
- `on_totals`
- `on_extremes`
- `on_progress`
- `on_profile`
- `on_logs_batch`
- `on_profile_events_batch`
- query-scoped settings

### `observer.zig`

Covers:

- `ClientOptions.observer`
- `ConnectObserveEvent`
- `QueryObserveEvent`
- `on_log`

### `cancel_query.zig`

Covers:

- `QueryContext`
- cooperative cancellation through `is_canceled`
- automatic `Cancel` + connection close inside `Do(...)`
- `client.isClosed()`

### `columns.zig`

Covers typed builders and views:

- fixed-width
- `Nullable`
- `Array`
- `Map`
- `Tuple`
- `LowCardinality`

This is a local example and does not require a real server.

### `low_level_packets.zig`

Covers the raw packet loop:

- `sendQuery`
- `readServerPacket`
- handling `data/totals/extremes/progress/profile/end_of_stream`

### `pool.zig`

Covers:

- `Pool.init`
- `pool.Do`
- `pool.stat`

Important: the runtime pool also supports waiter wakeups, `min_conns` replenishment, and opportunistic idle health checks, even though the example only shows the basic happy path.

### `tables_status.zig`

Covers:

- `requestTablesStatus`
- `TablesStatusResponse`

### `ssh_auth.zig`

Covers:

- `ClientOptions.ssh_signer`
- `SshSignFn`

Important: this is only a wire-level shape of the callback. For real login, the callback must return a real SSH signature.

### `live_verify.zig`

Full end-to-end run against a live ClickHouse server:

- `ping`
- `CREATE`
- `INSERT`
- `SELECT`
- `DROP`
- `disabled/none/lz4/lz4hc/zstd`

Run:

```bash
cd zig
zig build run-live-verify -- 127.0.0.1 9000 backend deanon cryptovision
```

## Environment Variables

All network examples read:

- `CH_HOST`
- `CH_PORT`
- `CH_USER`
- `CH_PASSWORD`
- `CH_DATABASE`

If command-line arguments are passed, they take precedence over `CH_*`.
