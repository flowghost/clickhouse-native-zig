# API Reference

Важно:

- текущий основной проверенный deployment path это plain TCP
- актуальная матрица supported / unsupported column types лежит в [TYPES.md](TYPES.md)

## 1. Connection Lifecycle

Основной тип клиента: `ch.Client`.

Создание:

```zig
var client = try ch.Client.connectTcp(allocator, host, port, .{
    .database = "default",
    .user = "default",
    .password = "",
    .client_name = "my-app",
    .compression = .zstd,
});
defer client.deinit();
```

`ClientOptions` поддерживает:

- `database`, `user`, `password`
- `client_name`
- `compression`, `compression_level`
- `quota_key`
- `protocol_version`
- `ssh_signer`
- `dialer`
- `dial_timeout_ms`, `read_timeout_ms`, `write_timeout_ms`, `handshake_timeout_ms`
- `tls`
- `observer`

Если consumer binary ещё не линкует libc, в своём `build.zig` нужно добавить `exe.linkLibC();`, потому что пакет вендорит `lz4/zstd` C sources.

Lifecycle методы:

- `client.deinit()` закрывает соединение и освобождает ресурсы
- `client.close()` закрывает соединение без деинициализации struct
- `client.isClosed()` возвращает текущее состояние сокета
- `client.lastException()` возвращает последний `ExceptionChain`, если сервер прислал `ServerException`
- `client.ping()` выполняет ping/pong roundtrip

## 2. High-Level Query API

High-level orchestration строится вокруг:

- `QueryContext`
- `Query`
- `Client.Do(...)`
- `BlockBuffer`

### `QueryContext`

`QueryContext` передаётся во все callbacks и поддерживает cooperative cancellation:

```zig
const ctx = ch.QueryContext{
    .user_data = &state,
    .is_canceled = isCanceled,
};
```

Где `is_canceled` имеет сигнатуру:

```zig
fn isCanceled(user_data: ?*anyopaque) bool
```

### `Query`

`Query` создаётся через:

```zig
var query = client.newQuery("SELECT 1");
```

Поддерживаемые high-level поля:

- `body`
- `query_id`
- `quota_key`
- `initial_user`
- `settings`
- `parameters`
- `input`
- `on_input`
- `result`
- `result_binding`
- `on_result`
- `totals`
- `totals_binding`
- `on_totals`
- `extremes`
- `extremes_binding`
- `on_extremes`
- `on_progress`
- `on_profile`
- `on_logs`
- `on_logs_batch`
- `on_log`
- `on_profile_events`
- `on_profile_events_batch`
- `on_profile_event`
- `on_table_columns`
- `external_data`
- `external_table`
- `metrics`
- `observer`

### `BlockBuffer`

`BlockBuffer` нужен, когда нужно сохранить result blocks после завершения packet lifetime:

```zig
var results = ch.BlockBuffer.init(allocator);
defer results.deinit();

query.result = &results;
try client.Do(.{}, &query);
```

## 3. Result Routing

`Client.Do(...)` разделяет входящие server packets так:

- `ServerCodeData` -> `query.result` / `query.on_result`
- `ServerCodeTotals` -> `query.totals` / `query.on_totals`
- `ServerCodeExtremes` -> `query.extremes` / `query.on_extremes`
- `ServerCodeProgress` -> `query.on_progress`
- `ServerCodeProfile` -> `query.on_profile`
- `ServerCodeLog` -> `query.on_logs`, `query.on_logs_batch`, `query.on_log`
- `ServerProfileEvents` -> `query.on_profile_events`, `query.on_profile_events_batch`, `query.on_profile_event`
- `ServerCodeTableColumns` -> `query.on_table_columns`

Если обычный result stream содержит больше одного non-empty block и не задан ни `result`, ни `on_result`, клиент возвращает `error.MissingResultHandler`.

## 4. Typed Result Binding

Для typed result collection без ручного разбора `DecodedBlock`:

```zig
var ids = std.ArrayList(u64).init(allocator);
defer ids.deinit();
var labels = ch.OwnedByteSlices.init(allocator);
defer labels.deinit();

var columns = [_]ch.ResultBindingColumn{
    .{ .name = "id", .sink = .{ .uint64s = &ids } },
    .{ .name = "label", .sink = .{ .strings = &labels } },
};
var binding = ch.ResultBinding.init(allocator, columns[0..]);

query.result_binding = &binding;
```

Поддерживаемые sink types:

- `strings`
- `bytes`
- `int8s`
- `int64s`
- `uint64s`
- `bools`
- `values`

`values` использует `OwnedValues` и рекурсивно materialize’ит строки результата в `OwnedValue`:

- `Nullable(...)` -> `OwnedValue.null` или inner value
- `Array(...)` -> `OwnedValue.array`
- `Map(...)` -> `OwnedValue.map`
- `Tuple(...)` -> `OwnedValue.tuple`
- `LowCardinality(...)` -> фактическое dictionary value
- fixed-width non-bool типы -> `OwnedValue.fixed`

## 5. INSERT / Streaming Input

Для single-block insert:

```zig
var insert_query = client.newQuery("INSERT INTO t VALUES");
insert_query.input = columns[0..];
try client.Do(.{}, &insert_query);
```

Для streaming insert:

```zig
fn onInput(ctx: ch.QueryContext, query: *ch.Query) !void {
    // заполнить query.input следующей пачкой
    // вернуть error.EndOfInput, когда данные закончились
}
```

Особенности:

- `input` должен содержать одинаковое количество строк во всех колонках
- если `on_input` задан и начальный `input` пустой, callback будет вызван до первой отправки
- если сервер сначала присылает zero-row schema block, `Do(...)` может доинферить пустые `name/type_name`
- это работает и для composite builders, если outer `type_name` был оставлен пустым
- при отмене `Do(...)` отправляет `Cancel` и закрывает соединение

## 6. Logs And Profile Events

High-level typed batch handlers:

- `OnLogsFn`: `fn (ctx, logs: []const ServerLog) !void`
- `OnLogFn`: `fn (ctx, log: ServerLog) !void`
- `OnProfileEventsFn`: `fn (ctx, events: []const ProfileEvent) !void`
- `OnProfileEventFn`: `fn (ctx, event: ProfileEvent) !void`

Typed structs:

```zig
pub const ServerLog = struct {
    query_id: []const u8,
    source: []const u8,
    text: []const u8,
    host: []const u8,
    time_seconds: u32,
    time_microseconds: u32,
    thread_id: u64,
    priority: i8,
};

pub const ProfileEvent = struct {
    event_type: ProfileEventType,
    name: []const u8,
    value: i64,
    host: []const u8,
    time_seconds: u32,
    thread_id: u64,
};
```

Raw packet-level handlers тоже доступны:

- `on_logs: ?OnDataPacketFn`
- `on_profile_events: ?OnDataPacketFn`

## 7. Transport, TLS And Timeouts

Transport options:

- `dialer`: кастомное открытие `std.net.Stream`
- `dial_timeout_ms`: timeout для default TCP dial path
- `read_timeout_ms`, `write_timeout_ms`: socket-level timeouts
- `handshake_timeout_ms`: timeout на initial handshake/TLS setup

TLS:

```zig
.tls = .{
    .enabled = true,
    .server_name = "clickhouse.example.com",
    .ca_mode = .system,
}
```

`ca_mode` поддерживает:

- `.system`
- `.self_signed`

## 8. Supported Column Families

Кратко:

- fully usable families: `String`, `JSON`, fixed-width numeric types, `Bool`, `Enum8/16`, `Date/Date32`, `DateTime/DateTime64`, `Time32/Time64`, `Decimal*`, `UUID`, `IPv4/IPv6`, `Nullable`, `Array`, `Map`, `Tuple`, `LowCardinality`
- raw-but-usable через `asFixed()` / `OwnedValue.fixed`: часть специальных fixed-width типов, например `Decimal`, `UUID`, `IP`, `DateTime*`
- unsupported families перечислены в [TYPES.md](TYPES.md)
- `.no_verification`

## 8. Observability

Client и query поддерживают `Observer`.

Поддерживаемые события:

- `ConnectObserveEvent.start`
- `ConnectObserveEvent.finish`
- `QueryObserveEvent.start`
- `QueryObserveEvent.progress`
- `QueryObserveEvent.profile`
- `QueryObserveEvent.exception`
- `QueryObserveEvent.finish`

`Query.metrics` позволяет сохранить итоговый `QueryMetrics`.

## 9. Low-Level Protocol API

Если нужен ручной packet loop, доступны:

- `client.sendQuery(query)`
- `client.sendDataPacket(packet)`
- `client.sendEndOfData()`
- `client.readServerPacket()`
- `client.cancel()`

`ServerPacket` содержит:

- `hello`
- `data`
- `totals`
- `extremes`
- `log`
- `profile_events`
- `exception`
- `progress`
- `pong`
- `end_of_stream`
- `profile`
- `table_columns`
- `tables_status`
- `part_uuids`
- `read_task_request`
- `ssh_challenge`

## 10. Pool

Пул соединений:

- `Pool.init(...)`
- `Pool.dial(...)`
- `pool.acquire(...)`
- `pool.Do(...)`
- `pool.ping()`
- `pool.stat()`

`PoolOptions` поддерживает:

- `host`, `port`
- `client_options`
- `max_conns`, `min_conns`
- `max_conn_lifetime_ms`
- `max_conn_idle_time_ms`
- `health_check_period_ms`

Runtime semantics:

- waiters в `acquire(...)` спят на condition variable, а не через busy-loop
- pool автоматически replenishes `min_conns`, если released connection оказался закрыт/expired
- `health_check_period_ms` включает opportunistic ping health-check idle connections

## 11. Tables Status And SSH

Tables status:

- `client.sendTablesStatusRequest(...)`
- `client.requestTablesStatus(...)`

Типы:

- `TablesStatusRequest`
- `TablesStatusResponse`
- `TableStatusEntry`
- `TableStatus`
- `QualifiedTableName`

SSH challenge flow:

- `client.sendSshChallengeRequest()`
- `client.sendSshChallengeResponse(signature_b64)`
- `client.authenticateSsh()`

Для автоматического SSH-auth в `connectTcp(...)` нужно передать `ClientOptions.ssh_signer`.

Сигнатура signer callback:

```zig
pub const SshSignFn = *const fn (
    message: []const u8,
    challenge: []const u8,
    out: *std.ArrayList(u8),
) anyerror!void;
```

`out` должен получить raw signature bytes; base64-encoding клиент делает сам.

## 12. Compression

Поддерживаемые wire codecs:

- `.disabled`
- `.none`
- `.lz4`
- `.lz4hc`
- `.zstd`

Compression задаётся через `ClientOptions.compression`.

High-level `Do(...)` и low-level `sendQuery/readServerPacket` автоматически согласуют:

- compression flag в `Query`
- `network_compression_method`
- compressible block framing/checksum

## 9. Column Builders

Готовые constructors:

- `initOwnedStringColumn`
- `initOwnedVarBytesColumn`
- `initOwnedFixedColumn`
- `initNullableColumn`
- `initArrayColumn`
- `initMapColumn`
- `initTupleColumn`
- `initLowCardinalityColumn`

Они позволяют формировать `Query.input` и `external_data` без ручной сериализации.

## 10. Column Views

Для декодированных block columns доступны:

- `column.asFixed()`
- `column.asNullable(allocator)`
- `column.asArray(allocator)`
- `column.asMap(allocator)`
- `column.asTuple(allocator)`
- `column.asLowCardinality(allocator)`

Полезные view-типы:

- `FixedColumnView`
- `NullableColumnView`
- `ArrayColumnView`
- `MapColumnView`
- `TupleColumnView`
- `LowCardinalityColumnView`

## 11. Error Model

Основные runtime-ошибки:

- `error.ServerException`
- `error.MissingResultHandler`
- `error.EmptyInput`
- `error.Canceled`
- `error.ClientClosed`
- `error.UnsupportedParameters`
- `error.UnexpectedPacket`

При `error.ServerException` подробности можно читать через `client.lastException()`.
