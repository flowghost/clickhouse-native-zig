# Type Support Matrix

Этот документ описывает текущее покрытие column types в Zig-пакете.

## 1. Готово Для Использования

Эти типы уже проходят decode/encode roundtrip и могут использоваться в обычных `SELECT` / `INSERT` сценариях.

### String-like

- `String`
- `JSON`
- `FixedString(N)`

### Integer / Float / Bool

- `Bool`
- `Int8`, `Int16`, `Int32`, `Int64`, `Int128`, `Int256`
- `UInt8`, `UInt16`, `UInt32`, `UInt64`, `UInt128`, `UInt256`
- `Float32`, `Float64`
- `BFloat16`

### Date / Time / Decimal / UUID / IP

- `Date`
- `Date32`
- `DateTime(...)`
- `DateTime64(...)`
- `Time32(...)`
- `Time64(...)`
- `Decimal(P, S)`
- `Decimal32`, `Decimal64`, `Decimal128`, `Decimal256`
- `UUID`
- `IPv4`
- `IPv6`

### Enums / Intervals / Other Fixed-Width

- `Enum8(...)`
- `Enum16(...)`
- `Interval*`
- `Nothing`
- `Point`

### Composite

- `Nullable(T)`
- `Array(T)`
- `Map(K, V)`
- `Tuple(...)`
- `LowCardinality(T)`

Эти composite-типы поддерживаются и в nested combinations, если внутренние типы тоже поддерживаются, например:

- `Array(LowCardinality(String))`
- `Map(LowCardinality(String), LowCardinality(String))`
- `Nullable(Array(String))`
- `Tuple(String, Nullable(Int64), Map(String, UInt64))`

## 2. API Semantics

Есть две формы работы с типами:

- typed/friendly API
- wire-compatible raw API

### Typed / Friendly

Самые удобные high-level builders/views сейчас есть для:

- `String`
- fixed-width family через `initOwnedFixedColumn(...)` и `asFixed()`
- `Nullable`
- `Array`
- `Map`
- `Tuple`
- `LowCardinality`

### Wire-Compatible Raw

Часть типов уже wire-compatible, но без отдельного специализированного Zig wrapper type. Они приходят как fixed-width или raw value representation:

- `Decimal*`
- `UUID`
- `IPv4`, `IPv6`
- `Date`, `Date32`, `DateTime`, `DateTime64`
- `Time32`, `Time64`
- `Enum8`, `Enum16`
- `Point`

Для них можно использовать:

- `column.asFixed()`
- `ResultBinding` с sink `values`
- manual interpretation of returned bytes

## 3. High-Level Binding

`ResultBinding` поддерживает:

- primitive sinks: `strings`, `bytes`, `int8s`, `int64s`, `uint64s`, `bools`
- recursive sink: `values`

`values` materialize’ит строки результата в `OwnedValue`, поэтому подходит для:

- `Nullable`
- `Array`
- `Map`
- `Tuple`
- `LowCardinality`
- fixed-width unsupported-by-special-wrapper types

## 4. Input Inference

`Client.Do(...)` умеет доинферить input schema по zero-row metadata block, если client-side column был создан с пустым `name` и/или `type_name`.

Это уже работает для:

- `String`
- fixed-width columns
- `encoded` composite builders
- `Array`, `Map`, `Tuple`, `Nullable`, `LowCardinality`, если outer `type_name` был оставлен пустым

## 5. Ещё Не Реализовано

Эти семейства пока не стоит считать поддержанными:

- `SimpleAggregateFunction(...)`
- `AggregateFunction(...)`
- `Variant(...)`
- `Dynamic`
- параметризованный `JSON(...)`
- `Object(...)`
- `QBit`
- геометрические alias-типы вроде `Ring`, `Polygon`, `MultiPolygon`

Если в схеме используются именно они, текущий Zig-клиент пока не является полным drop-in parity с Go `proto`.

## 6. Практическая Рекомендация

Пакет уже подходит для интеграции, если твои таблицы в основном состоят из:

- чисел
- строк
- дат/времени
- `UUID` / `IP`
- `Decimal`
- `Enum`
- `Nullable`
- `Array`
- `Map`
- `Tuple`
- `LowCardinality`

Если в схеме есть `AggregateFunction` / `Variant` / `Dynamic` / `Object`, лучше сначала прогнать конкретную схему на тестовом стенде или добить поддержку этих типов отдельно.
