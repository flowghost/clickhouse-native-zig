# clickhouse_native (Zig)

Самостоятельный Zig-пакет для ClickHouse native TCP protocol. Пакет живёт отдельно от Go-модуля в `./zig` и включает:

- high-level client API: `Client.Do(...)`, `Query`, `QueryContext`, `BlockBuffer`
- typed result binding: `ResultBinding`, `ResultBindingColumn`, `OwnedByteSlices`, `OwnedValues`
- low-level native protocol API: `sendQuery`, `sendDataPacket`, `sendEndOfData`, `readServerPacket`
- self-contained wire compression: `None`, `LZ4`, `LZ4HC`, `ZSTD`
- typed column builders/views для `String`, fixed-width, `Nullable`, `Array`, `Map`, `Tuple`, `LowCardinality`
- transport/runtime options: `dialer`, `dial_timeout_ms`, `read_timeout_ms`, `write_timeout_ms`, `handshake_timeout_ms`, `tls`
- observability hooks: `Observer`, `QueryMetrics`
- connection pool: `Pool`, `PooledClient`
- surface для `tables_status` и SSH challenge/response

## Текущий статус

Пакет уже можно использовать как plain TCP ClickHouse client в другом Zig-проекте.

Что подтверждено сейчас:

- `Client.connectTcp(...)`, `ping()`, `Do(...)`, `Pool`
- `INSERT` / `SELECT`
- wire compression: `disabled`, `none`, `lz4`, `lz4hc`, `zstd`
- high-level callbacks, result binding, schema-driven input inference
- live e2e против реального ClickHouse

Что важно учитывать:

- TLS есть в API, но основной проверенный контур сейчас plain TCP
- полная поддержка всех типов ClickHouse ещё не достигнута
- актуальная матрица типов вынесена в [docs/TYPES.md](docs/TYPES.md)

## Быстрый старт

```bash
cd zig
zig build test
zig build examples
```

Live e2e-проверка против реального ClickHouse:

```bash
cd zig
zig build run-live-verify -- 127.0.0.1 9000 backend deanon cryptovision
```

## Подключение как зависимости

В `build.zig.zon` внешнего проекта:

```zig
.{
    .dependencies = .{
        .clickhouse_native = .{
            .path = "../path/to/ch-go/zig",
        },
    },
}
```

В `build.zig`:

```zig
const clickhouse_native = b.dependency("clickhouse_native", .{
    .target = target,
    .optimize = optimize,
}).module("clickhouse_native");

exe.root_module.addImport("clickhouse_native", clickhouse_native);
exe.linkLibC();
```

В коде:

```zig
const ch = @import("clickhouse_native");
```

## Основные entry points

- Подключение: `ch.Client.connectTcp(...)`
- High-level запросы: `client.newQuery(...)`, `client.Do(...)`
- Low-level protocol: `client.sendQuery(...)`, `client.readServerPacket()`
- Typed result buffering: `ch.BlockBuffer`
- Typed result binding: `ch.ResultBinding`
- Recursive result values: `ch.OwnedValues`, `ch.OwnedValue`
- Typed column access: `column.asFixed()`, `column.asNullable()`, `column.asArray()`, `column.asMap()`, `column.asTuple()`, `column.asLowCardinality()`
- Column builders: `initOwnedStringColumn`, `initOwnedFixedColumn`, `initNullableColumn`, `initArrayColumn`, `initMapColumn`, `initTupleColumn`, `initLowCardinalityColumn`
- Schema-driven input inference: `Do(...)` может заполнить пустые `name/type_name` перед `INSERT`
- Pooling: `ch.Pool.init(...)`, `pool.acquire(...)`, `pool.Do(...)`

## Документация

- [API reference](docs/API.md)
- [Examples guide](docs/EXAMPLES.md)
- [Type support matrix](docs/TYPES.md)

## Примеры

- [high_level_select.zig](examples/high_level_select.zig)
- [result_binding.zig](examples/result_binding.zig)
- [high_level_insert_stream.zig](examples/high_level_insert_stream.zig)
- [high_level_handlers.zig](examples/high_level_handlers.zig)
- [observer.zig](examples/observer.zig)
- [cancel_query.zig](examples/cancel_query.zig)
- [columns.zig](examples/columns.zig)
- [low_level_packets.zig](examples/low_level_packets.zig)
- [pool.zig](examples/pool.zig)
- [tables_status.zig](examples/tables_status.zig)
- [ssh_auth.zig](examples/ssh_auth.zig)
- [live_verify.zig](examples/live_verify.zig)
