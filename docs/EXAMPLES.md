# Examples Guide

Все примеры лежат в `zig/examples/` и собираются командой:

```bash
cd zig
zig build examples
```

## Example Index

### `smoke.zig`

Минимальный import/build smoke test. Полезен, если нужно просто проверить, что пакет подтянулся как dependency.

### `high_level_select.zig`

Покрывает:

- `Client.connectTcp`
- `client.newQuery`
- `client.Do`
- `BlockBuffer`
- `asFixed()` и чтение `String` result columns

### `result_binding.zig`

Покрывает:

- `ResultBinding`
- `ResultBindingColumn`
- `OwnedByteSlices`
- primitive typed sinks
- `Query.metrics`

### `high_level_insert_stream.zig`

Покрывает:

- `Query.input`
- `Query.on_input`
- streaming `INSERT`
- column builders `initOwnedFixedColumn` и `initOwnedStringColumn`

### `high_level_handlers.zig`

Покрывает:

- `on_result`
- `on_totals`
- `on_extremes`
- `on_progress`
- `on_profile`
- `on_logs_batch`
- `on_profile_events_batch`
- query-scoped settings

### `observer.zig`

Покрывает:

- `ClientOptions.observer`
- `ConnectObserveEvent`
- `QueryObserveEvent`
- `on_log`

### `cancel_query.zig`

Покрывает:

- `QueryContext`
- cooperative cancellation через `is_canceled`
- автоматический `Cancel` + закрытие соединения внутри `Do(...)`
- `client.isClosed()`

### `columns.zig`

Покрывает typed builders и views:

- fixed-width
- `Nullable`
- `Array`
- `Map`
- `Tuple`
- `LowCardinality`

Это локальный пример без реального сервера.

### `low_level_packets.zig`

Покрывает raw packet-loop:

- `sendQuery`
- `readServerPacket`
- обработку `data/totals/extremes/progress/profile/end_of_stream`

### `pool.zig`

Покрывает:

- `Pool.init`
- `pool.Do`
- `pool.stat`

Важно: runtime pool также поддерживает waiter path, replenishment `min_conns` и opportunistic health-check idle connections, хотя сам пример показывает только базовый happy path.

### `tables_status.zig`

Покрывает:

- `requestTablesStatus`
- `TablesStatusResponse`

### `ssh_auth.zig`

Покрывает:

- `ClientOptions.ssh_signer`
- `SshSignFn`

Важно: это wire-level пример формы callback. Для реального логина в callback нужно возвращать настоящую SSH signature.

### `live_verify.zig`

Полный e2e-прогон против живого ClickHouse:

- `ping`
- `CREATE`
- `INSERT`
- `SELECT`
- `DROP`
- `disabled/none/lz4/lz4hc/zstd`

Запуск:

```bash
cd zig
zig build run-live-verify -- 127.0.0.1 9000 backend deanon cryptovision
```

## Environment Variables

Все сетевые примеры читают:

- `CH_HOST`
- `CH_PORT`
- `CH_USER`
- `CH_PASSWORD`
- `CH_DATABASE`

Если аргументы переданы в командной строке, они имеют приоритет над `CH_*`.
