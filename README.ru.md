# clickhouse_native (Zig)

[English version](README.md)

Самостоятельный Zig-пакет для ClickHouse native TCP protocol.

## Что Входит

- high-level client API: `Client.Do(...)`, `Query`, `QueryContext`, `BlockBuffer`
- typed result binding: `ResultBinding`, `ResultBindingColumn`, `OwnedByteSlices`, `OwnedValues`
- low-level protocol API: `sendQuery`, `sendDataPacket`, `sendEndOfData`, `readServerPacket`
- self-contained wire compression: `disabled`, `none`, `lz4`, `lz4hc`, `zstd`
- typed column builders и views для `String`, fixed-width типов, `Nullable`, `Array`, `Map`, `Tuple`, `LowCardinality`
- transport/runtime options: `dialer`, `dial_timeout_ms`, `read_timeout_ms`, `write_timeout_ms`, `handshake_timeout_ms`, `tls`
- observability hooks: `Observer`, `QueryMetrics`
- connection pool: `Pool`, `PooledClient`
- support для `tables_status` и SSH challenge/response packets

## Текущий Статус

Пакет уже можно использовать как plain TCP ClickHouse client из другого Zig-проекта.

На текущий момент подтверждено:

- `Client.connectTcp(...)`, `ping()`, `Do(...)`, `Pool`
- `INSERT` и `SELECT`
- режимы compression: `disabled`, `none`, `lz4`, `lz4hc`, `zstd`
- high-level callbacks, result binding, schema-driven input inference
- live end-to-end проверка против реального ClickHouse

Что важно учитывать:

- TLS есть в API, но основной проверенный deployment path сейчас plain TCP
- полная поддержка всех типов ClickHouse ещё не завершена
- актуальная матрица типов лежит в [docs/ru/TYPES.md](docs/ru/TYPES.md)

## Быстрый Старт

```bash
cd zig
zig build test
zig build examples
```

Live end-to-end проверка против реального ClickHouse:

```bash
cd zig
zig build run-live-verify -- 127.0.0.1 9000 backend deanon cryptovision
```

## Подключение Как Зависимости

В `build.zig.zon` внешнего проекта:

```zig
.{
    .dependencies = .{
        .clickhouse_native = .{
            .path = "../path/to/clickhouse-native-zig",
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

`linkLibC()` нужен потому, что пакет вендорит `lz4` и `zstd` C sources.

## Основные Entry Points

- connect: `ch.Client.connectTcp(...)`
- high-level queries: `client.newQuery(...)`, `client.Do(...)`
- low-level protocol: `client.sendQuery(...)`, `client.readServerPacket()`
- buffered result storage: `ch.BlockBuffer`
- typed result binding: `ch.ResultBinding`
- recursive result values: `ch.OwnedValues`, `ch.OwnedValue`
- typed column access: `column.asFixed()`, `column.asNullable()`, `column.asArray()`, `column.asMap()`, `column.asTuple()`, `column.asLowCardinality()`
- column builders: `initOwnedStringColumn`, `initOwnedFixedColumn`, `initNullableColumn`, `initArrayColumn`, `initMapColumn`, `initTupleColumn`, `initLowCardinalityColumn`
- schema-driven input inference: `Do(...)` может заполнить пустые `name` и `type_name` перед `INSERT`
- pooling: `ch.Pool.init(...)`, `pool.acquire(...)`, `pool.Do(...)`

## Документация

- [API reference](docs/API.md)
- [Examples guide](docs/EXAMPLES.md)
- [Type support matrix](docs/TYPES.md)
- [Русский API reference](docs/ru/API.md)
- [Русский examples guide](docs/ru/EXAMPLES.md)
- [Русская матрица типов](docs/ru/TYPES.md)

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

## Лицензия

Apache-2.0.
