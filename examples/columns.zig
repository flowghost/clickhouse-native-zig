const std = @import("std");
const ch = @import("clickhouse_native");

pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();
    const stdout = std.io.getStdOut().writer();

    const ids = [_]u64{ 1, 2, 3 };
    var id_column = try ch.initOwnedFixedColumn(allocator, "id", "UInt64", ids[0..]);
    defer id_column.deinit(allocator);
    const id_values = try (try id_column.asFixed()).slice(u64);
    try stdout.print("fixed column rows={d} first={d}\n", .{ id_values.len, id_values[0] });

    const nullable_values_raw = [_]i64{ 10, 20, 30 };
    var nullable_values = try ch.initOwnedFixedColumn(allocator, "score_inner", "Int64", nullable_values_raw[0..]);
    defer nullable_values.deinit(allocator);
    var nullable_column = try ch.initNullableColumn(allocator, "score", "Nullable(Int64)", &.{ false, true, false }, nullable_values);
    defer nullable_column.deinit(allocator);
    var nullable_view = try nullable_column.asNullable(allocator);
    defer nullable_view.deinit(allocator);
    try stdout.print("nullable rows={d} middle_is_null={any}\n", .{ nullable_view.rows(), nullable_view.isNull(1) });

    const tag_values = [_][]const u8{ "red", "blue", "green" };
    var tag_items = try ch.initOwnedStringColumn(allocator, "tags_items", tag_values[0..]);
    defer tag_items.deinit(allocator);
    var tag_column = try ch.initArrayColumn(allocator, "tags", "Array(String)", &.{ 2, 3 }, tag_items);
    defer tag_column.deinit(allocator);
    var array_view = try tag_column.asArray(allocator);
    defer array_view.deinit(allocator);
    const row0 = array_view.rowRange(0);
    try stdout.print("array rows={d} row0={d}..{d}\n", .{ array_view.rows(), row0.start, row0.end });

    const map_keys_raw = [_][]const u8{ "a", "b", "x" };
    const map_values_raw = [_]u64{ 10, 11, 42 };
    var map_keys = try ch.initOwnedStringColumn(allocator, "attrs_keys", map_keys_raw[0..]);
    defer map_keys.deinit(allocator);
    var map_values = try ch.initOwnedFixedColumn(allocator, "attrs_values", "UInt64", map_values_raw[0..]);
    defer map_values.deinit(allocator);
    var map_column = try ch.initMapColumn(allocator, "attrs", "Map(String, UInt64)", &.{ 2, 3 }, map_keys, map_values);
    defer map_column.deinit(allocator);
    var map_view = try map_column.asMap(allocator);
    defer map_view.deinit(allocator);
    const map_row1 = map_view.rowRange(1);
    try stdout.print("map rows={d} row1={d}..{d}\n", .{ map_view.rows(), map_row1.start, map_row1.end });

    const tuple_left_raw = [_][]const u8{ "alpha", "beta" };
    const tuple_right_raw = [_]i64{ 7, 9 };
    var tuple_left = try ch.initOwnedStringColumn(allocator, "left", tuple_left_raw[0..]);
    defer tuple_left.deinit(allocator);
    var tuple_right = try ch.initOwnedFixedColumn(allocator, "right", "Int64", tuple_right_raw[0..]);
    defer tuple_right.deinit(allocator);
    const tuple_fields = [_]ch.TupleField{
        .{ .name = "left", .column = tuple_left },
        .{ .name = "right", .column = tuple_right },
    };
    var tuple_column = try ch.initTupleColumn(allocator, "pair", "Tuple(String, Int64)", tuple_fields[0..]);
    defer tuple_column.deinit(allocator);
    var tuple_view = try tuple_column.asTuple(allocator);
    defer tuple_view.deinit(allocator);
    try stdout.print("tuple rows={d} fields={d}\n", .{ tuple_view.rows, tuple_view.fields.len });

    const dict_raw = [_][]const u8{ "alpha", "beta" };
    const keys_raw = [_]u8{ 0, 1, 0 };
    var dictionary = try ch.initOwnedStringColumn(allocator, "name_dict", dict_raw[0..]);
    defer dictionary.deinit(allocator);
    var keys = try ch.initOwnedFixedColumn(allocator, "name_keys", "UInt8", keys_raw[0..]);
    defer keys.deinit(allocator);
    var lc_column = try ch.initLowCardinalityColumn(allocator, "name", "LowCardinality(String)", dictionary, keys);
    defer lc_column.deinit(allocator);
    var lc_view = try lc_column.asLowCardinality(allocator);
    defer lc_view.deinit(allocator);
    try stdout.print("low_cardinality rows={d} dictionary_rows={d}\n", .{ lc_view.rows(), lc_view.dictionary.rowCount() });
}
