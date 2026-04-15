# Type Support Matrix

[Русская версия](ru/TYPES.md)

This document describes the current column type coverage in the Zig package.

## 1. Ready For Use

These types already pass decode/encode roundtrip tests and can be used in normal `SELECT` / `INSERT` flows.

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

Nested combinations are supported as long as the inner types are also supported, for example:

- `Array(LowCardinality(String))`
- `Map(LowCardinality(String), LowCardinality(String))`
- `Nullable(Array(String))`
- `Tuple(String, Nullable(Int64), Map(String, UInt64))`

## 2. API Semantics

There are two practical ways to work with types:

- typed/friendly API
- wire-compatible raw API

### Typed / Friendly

The most convenient high-level builders/views currently exist for:

- `String`
- fixed-width types through `initOwnedFixedColumn(...)` and `asFixed()`
- `Nullable`
- `Array`
- `Map`
- `Tuple`
- `LowCardinality`

### Wire-Compatible Raw

Some types are already wire-compatible but do not yet have a dedicated specialized Zig wrapper. They appear as fixed-width or raw value representations:

- `Decimal*`
- `UUID`
- `IPv4`, `IPv6`
- `Date`, `Date32`, `DateTime`, `DateTime64`
- `Time32`, `Time64`
- `Enum8`, `Enum16`
- `Point`

For these you can use:

- `column.asFixed()`
- `ResultBinding` with the `values` sink
- manual interpretation of returned bytes

## 3. High-Level Binding

`ResultBinding` supports:

- primitive sinks: `strings`, `bytes`, `int8s`, `int64s`, `uint64s`, `bools`
- recursive sink: `values`

`values` materializes result rows into `OwnedValue`, so it works for:

- `Nullable`
- `Array`
- `Map`
- `Tuple`
- `LowCardinality`
- fixed-width types that do not have a specialized wrapper yet

## 4. Input Inference

`Client.Do(...)` can infer input schema from a zero-row metadata block if the client-side column was created with an empty `name` and/or `type_name`.

This already works for:

- `String`
- fixed-width columns
- `encoded` composite builders
- `Array`, `Map`, `Tuple`, `Nullable`, `LowCardinality` if the outer `type_name` was left empty

## 5. Not Implemented Yet

These families should not be considered supported yet:

- `SimpleAggregateFunction(...)`
- `AggregateFunction(...)`
- `Variant(...)`
- `Dynamic`
- parameterized `JSON(...)`
- `Object(...)`
- `QBit`
- geometry aliases such as `Ring`, `Polygon`, `MultiPolygon`

If your schema uses these types, the current Zig client is not yet full drop-in parity with Go `proto`.

## 6. Practical Recommendation

The package is already suitable for integration if your tables mostly use:

- numbers
- strings
- date/time types
- `UUID` / `IP`
- `Decimal`
- `Enum`
- `Nullable`
- `Array`
- `Map`
- `Tuple`
- `LowCardinality`

If your schema includes `AggregateFunction`, `Variant`, `Dynamic`, or `Object`, it is better to validate that schema on a staging environment first or finish support for those types separately.
