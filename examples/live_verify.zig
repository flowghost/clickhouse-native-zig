const std = @import("std");
const ch = @import("clickhouse_native");

const CompressionCase = struct {
    label: []const u8,
    mode: ch.BlockCompression,
};

const compression_cases = [_]CompressionCase{
    .{ .label = "disabled", .mode = .disabled },
    .{ .label = "none", .mode = .none },
    .{ .label = "lz4", .mode = .lz4 },
    .{ .label = "lz4hc", .mode = .lz4hc },
    .{ .label = "zstd", .mode = .zstd },
};

pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next();
    const host = args.next() orelse "127.0.0.1";
    const port = if (args.next()) |raw| try std.fmt.parseInt(u16, raw, 10) else 9000;
    const user = if (args.next()) |value| value else try optionalEnvOrDefault(allocator, "CH_USER", "default");
    const password = if (args.next()) |value| value else try optionalEnvOrDefault(allocator, "CH_PASSWORD", "");
    const database = if (args.next()) |value| value else try optionalEnvOrDefault(allocator, "CH_DATABASE", "default");

    const stdout = std.io.getStdOut().writer();
    try stdout.print("Verifying ClickHouse native protocol against {s}:{d} as {s}/{s}\n", .{ host, port, user, database });

    for (compression_cases) |case| {
        try stdout.print("  - {s}\n", .{case.label});
        try verifyCompressionCase(allocator, host, port, user, password, database, case);
    }

    try stdout.writeAll("All live verification checks passed\n");
}

fn optionalEnvOrDefault(allocator: std.mem.Allocator, key: []const u8, default: []const u8) ![]const u8 {
    return std.process.getEnvVarOwned(allocator, key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => default,
        else => err,
    };
}

fn verifyCompressionCase(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    user: []const u8,
    password: []const u8,
    database: []const u8,
    case: CompressionCase,
) !void {
    var client = try ch.Client.connectTcp(allocator, host, port, .{
        .database = database,
        .user = user,
        .password = password,
        .client_name = "zig-live-verify",
        .compression = case.mode,
    });
    defer client.deinit();

    std.debug.print("    ping\n", .{});
    try client.ping();

    const table = try std.fmt.allocPrint(allocator, "zig_native_verify_{s}", .{case.label});
    defer allocator.free(table);

    const drop_sql = try std.fmt.allocPrint(allocator, "DROP TABLE IF EXISTS {s}", .{table});
    defer allocator.free(drop_sql);
    std.debug.print("    drop {s}\n", .{table});
    try execQuery(&client, drop_sql);

    const create_sql = try std.fmt.allocPrint(allocator,
        \\CREATE TABLE {s}
        \\(
        \\    id UInt64,
        \\    name LowCardinality(String),
        \\    tags Array(String),
        \\    score Nullable(Int64),
        \\    attrs Map(String, UInt64),
        \\    pair Tuple(String, Int64)
        \\) ENGINE = Memory
    , .{table});
    defer allocator.free(create_sql);
    std.debug.print("    create {s}\n", .{table});
    try execQuery(&client, create_sql);

    const insert_sql = try std.fmt.allocPrint(allocator, "INSERT INTO {s} VALUES", .{table});
    defer allocator.free(insert_sql);
    std.debug.print("    insert {s}\n", .{table});
    var columns = try makeInsertColumns(allocator);
    defer deinitColumns(allocator, &columns);
    var insert_query = client.newQuery(insert_sql);
    insert_query.input = columns[0..];
    try runQuery(&client, &insert_query);

    const select_sql = try std.fmt.allocPrint(allocator, "SELECT id, name, tags, score, attrs, pair FROM {s} ORDER BY id", .{table});
    defer allocator.free(select_sql);
    std.debug.print("    select {s}\n", .{table});
    var result_buffer = ch.BlockBuffer.init(allocator);
    defer result_buffer.deinit();
    var select_query = client.newQuery(select_sql);
    select_query.result = &result_buffer;
    try runQuery(&client, &select_query);
    try expectSelectRows(allocator, &result_buffer);

    std.debug.print("    final drop {s}\n", .{table});
    try execQuery(&client, drop_sql);
}

const DrainMode = enum {
    allow_data,
    expect_empty_data,
    allow_metadata_data,
};

fn execQuery(client: *ch.Client, sql: []const u8) !void {
    var query = client.newQuery(sql);
    try runQuery(client, &query);
}

fn runQuery(client: *ch.Client, query: *ch.Query) !void {
    client.Do(.{}, query) catch |err| {
        if (client.lastException()) |exception| {
            const stderr = std.io.getStdErr().writer();
            for (exception.items) |item| {
                stderr.print("      server exception {d} {s}: {s}\n", .{ item.code, item.name, item.message }) catch {};
            }
        }
        return err;
    };
}

fn drainUntilEnd(client: *ch.Client, mode: DrainMode) !void {
    while (true) {
        var packet = try client.readServerPacket();
        defer packet.deinit();
        std.debug.print("      packet {s}\n", .{packetName(packet.value)});

        switch (packet.value) {
            .end_of_stream => return,
            .progress, .profile, .log, .profile_events, .table_columns => {},
            .data, .totals, .extremes => |data| switch (mode) {
                .allow_data => if (!data.block.isEnd()) {},
                .expect_empty_data => if (!data.block.isEnd()) return error.UnexpectedDataBlock,
                .allow_metadata_data => if (data.block.rows != 0) return error.UnexpectedDataBlock,
            },
            .exception => return error.ServerException,
            else => return error.UnexpectedPacket,
        }
    }
}

fn expectSelectRows(allocator: std.mem.Allocator, result_buffer: *const ch.BlockBuffer) !void {
    var saw_block = false;
    for (result_buffer.blocks.items) |block| {
        if (block.isEnd() or block.rows == 0) continue;
        if (saw_block) return error.UnexpectedExtraDataBlock;
        saw_block = true;
        try validateSelectBlock(allocator, block);
    }
    if (!saw_block) return error.MissingDataBlock;
}

fn packetName(packet: ch.ServerPacket) []const u8 {
    return switch (packet) {
        .hello => "hello",
        .data => "data",
        .totals => "totals",
        .extremes => "extremes",
        .log => "log",
        .profile_events => "profile_events",
        .exception => "exception",
        .progress => "progress",
        .pong => "pong",
        .end_of_stream => "end_of_stream",
        .profile => "profile",
        .table_columns => "table_columns",
        .tables_status => "tables_status",
        .part_uuids => "part_uuids",
        .read_task_request => "read_task_request",
        .ssh_challenge => "ssh_challenge",
    };
}

fn validateSelectBlock(allocator: std.mem.Allocator, block: ch.DecodedBlock) !void {
    if (block.rows != 2) return error.UnexpectedRowCount;
    if (block.columns.len != 6) return error.UnexpectedColumnCount;

    const ids = try block.columns[0].asFixed();
    const id_values = try ids.slice(u64);
    if (id_values.len != 2 or id_values[0] != 1 or id_values[1] != 2) {
        return error.UnexpectedIds;
    }

    var name_view = try block.columns[1].asLowCardinality(allocator);
    defer name_view.deinit(allocator);
    if (name_view.rows() != 2) return error.UnexpectedLowCardinalityRows;
    try expectLowCardinalityValues(name_view, &.{ "alpha", "beta" });

    var tags_view = try block.columns[2].asArray(allocator);
    defer tags_view.deinit(allocator);
    if (tags_view.rows() != 2) return error.UnexpectedArrayRows;
    switch (tags_view.values) {
        .string => |column| {
            const row0 = tags_view.rowRange(0);
            const row1 = tags_view.rowRange(1);
            if (row0.start != 0 or row0.end != 2) return error.UnexpectedArrayOffsets;
            if (row1.start != 2 or row1.end != 3) return error.UnexpectedArrayOffsets;
            if (!std.mem.eql(u8, column.values[0], "red")) return error.UnexpectedArrayValues;
            if (!std.mem.eql(u8, column.values[1], "blue")) return error.UnexpectedArrayValues;
            if (!std.mem.eql(u8, column.values[2], "green")) return error.UnexpectedArrayValues;
        },
        else => return error.UnexpectedArrayValues,
    }

    var score_view = try block.columns[3].asNullable(allocator);
    defer score_view.deinit(allocator);
    if (!score_view.isNull(0) or score_view.isNull(1)) return error.UnexpectedNullableState;
    const score_values = try score_view.values.asFixed();
    const score_slice = try score_values.slice(i64);
    if (score_slice.len != 2 or score_slice[1] != 7) return error.UnexpectedNullableValues;

    var attrs_view = try block.columns[4].asMap(allocator);
    defer attrs_view.deinit(allocator);
    if (attrs_view.rows() != 2) return error.UnexpectedMapRows;
    const attrs_row0 = attrs_view.rowRange(0);
    const attrs_row1 = attrs_view.rowRange(1);
    if (attrs_row0.start != 0 or attrs_row0.end != 2) return error.UnexpectedMapOffsets;
    if (attrs_row1.start != 2 or attrs_row1.end != 3) return error.UnexpectedMapOffsets;
    switch (attrs_view.keys) {
        .string => |column| {
            if (!std.mem.eql(u8, column.values[0], "a")) return error.UnexpectedMapKeys;
            if (!std.mem.eql(u8, column.values[1], "b")) return error.UnexpectedMapKeys;
            if (!std.mem.eql(u8, column.values[2], "x")) return error.UnexpectedMapKeys;
        },
        else => return error.UnexpectedMapKeys,
    }
    const attr_values = try attrs_view.values.asFixed();
    const attr_slice = try attr_values.slice(u64);
    if (attr_slice.len != 3 or attr_slice[0] != 10 or attr_slice[1] != 11 or attr_slice[2] != 42) {
        return error.UnexpectedMapValues;
    }

    var pair_view = try block.columns[5].asTuple(allocator);
    defer pair_view.deinit(allocator);
    if (pair_view.rows != 2 or pair_view.fields.len != 2) return error.UnexpectedTupleShape;
    switch (pair_view.fields[0].column) {
        .string => |column| {
            if (!std.mem.eql(u8, column.values[0], "left")) return error.UnexpectedTupleValues;
            if (!std.mem.eql(u8, column.values[1], "right")) return error.UnexpectedTupleValues;
        },
        else => return error.UnexpectedTupleValues,
    }
    const tuple_nums = try pair_view.fields[1].column.asFixed();
    const tuple_num_slice = try tuple_nums.slice(i64);
    if (tuple_num_slice.len != 2 or tuple_num_slice[0] != -5 or tuple_num_slice[1] != 99) {
        return error.UnexpectedTupleValues;
    }
}

fn expectLowCardinalityValues(view: ch.LowCardinalityColumnView, expected: []const []const u8) !void {
    const dictionary_values = switch (view.dictionary) {
        .string => |column| column.values,
        else => return error.UnexpectedLowCardinalityDictionary,
    };

    const key_fixed = try view.keys.asFixed();
    const matched = switch (key_fixed.width) {
        1 => blk: {
            const keys = try key_fixed.slice(u8);
            break :blk lowCardinalityMatchesExpected(dictionary_values, keys, expected) or
                lowCardinalityMatchesExpectedWithOneBased(dictionary_values, keys, expected);
        },
        2 => blk: {
            const keys = try key_fixed.slice(u16);
            break :blk lowCardinalityMatchesExpected(dictionary_values, keys, expected) or
                lowCardinalityMatchesExpectedWithOneBased(dictionary_values, keys, expected);
        },
        4 => blk: {
            const keys = try key_fixed.slice(u32);
            break :blk lowCardinalityMatchesExpected(dictionary_values, keys, expected) or
                lowCardinalityMatchesExpectedWithOneBased(dictionary_values, keys, expected);
        },
        8 => blk: {
            const keys = try key_fixed.slice(u64);
            break :blk lowCardinalityMatchesExpected(dictionary_values, keys, expected) or
                lowCardinalityMatchesExpectedWithOneBased(dictionary_values, keys, expected);
        },
        else => return error.UnexpectedLowCardinalityDictionary,
    };
    if (!matched) return error.UnexpectedLowCardinalityDictionary;
}

fn lowCardinalityMatchesExpected(dictionary: []const []const u8, raw_keys: anytype, expected: []const []const u8) bool {
    if (raw_keys.len != expected.len) return false;
    for (expected, 0..) |want, idx| {
        const key = @as(usize, raw_keys[idx]);
        if (key >= dictionary.len) return false;
        if (!std.mem.eql(u8, dictionary[key], want)) return false;
    }
    return true;
}

fn lowCardinalityMatchesExpectedWithOneBased(dictionary: []const []const u8, raw_keys: anytype, expected: []const []const u8) bool {
    if (raw_keys.len != expected.len) return false;
    for (expected, 0..) |want, idx| {
        const raw_key = @as(usize, raw_keys[idx]);
        if (raw_key == 0) return false;
        const key = raw_key - 1;
        if (key >= dictionary.len) return false;
        if (!std.mem.eql(u8, dictionary[key], want)) return false;
    }
    return true;
}

fn makeInsertColumns(allocator: std.mem.Allocator) ![6]ch.Column {
    const ids = [_]u64{ 1, 2 };

    const dictionary_values = [_][]const u8{ "alpha", "beta" };
    const tag_values = [_][]const u8{ "red", "blue", "green" };
    const map_keys = [_][]const u8{ "a", "b", "x" };
    const tuple_strings = [_][]const u8{ "left", "right" };

    var columns: [6]ch.Column = undefined;

    columns[0] = try ch.initOwnedFixedColumn(allocator, "id", "UInt64", ids[0..]);

    var name_dictionary = try ch.initOwnedStringColumn(allocator, "name_dictionary", dictionary_values[0..]);
    defer name_dictionary.deinit(allocator);
    var name_keys = try ch.initOwnedFixedColumn(allocator, "name_keys", "UInt8", &[_]u8{ 0, 1 });
    defer name_keys.deinit(allocator);
    columns[1] = try ch.initLowCardinalityColumn(allocator, "name", "LowCardinality(String)", name_dictionary, name_keys);

    var tags_values = try ch.initOwnedStringColumn(allocator, "tags_values", tag_values[0..]);
    defer tags_values.deinit(allocator);
    columns[2] = try ch.initArrayColumn(allocator, "tags", "Array(String)", &[_]u64{ 2, 3 }, tags_values);

    var score_values = try ch.initOwnedFixedColumn(allocator, "score_values", "Int64", &[_]i64{ 0, 7 });
    defer score_values.deinit(allocator);
    columns[3] = try ch.initNullableColumn(allocator, "score", "Nullable(Int64)", &[_]bool{ true, false }, score_values);

    var attrs_keys = try ch.initOwnedStringColumn(allocator, "attrs_keys", map_keys[0..]);
    defer attrs_keys.deinit(allocator);
    var attrs_values = try ch.initOwnedFixedColumn(allocator, "attrs_values", "UInt64", &[_]u64{ 10, 11, 42 });
    defer attrs_values.deinit(allocator);
    columns[4] = try ch.initMapColumn(allocator, "attrs", "Map(String, UInt64)", &[_]u64{ 2, 3 }, attrs_keys, attrs_values);

    var tuple_strings_col = try ch.initOwnedStringColumn(allocator, "pair_strings", tuple_strings[0..]);
    defer tuple_strings_col.deinit(allocator);
    var tuple_ints = try ch.initOwnedFixedColumn(allocator, "pair_ints", "Int64", &[_]i64{ -5, 99 });
    defer tuple_ints.deinit(allocator);
    columns[5] = try ch.initTupleColumn(allocator, "pair", "Tuple(String, Int64)", &.{
        .{ .name = "", .column = tuple_strings_col },
        .{ .name = "", .column = tuple_ints },
    });

    return columns;
}

fn deinitColumns(allocator: std.mem.Allocator, columns: []ch.Column) void {
    for (columns) |*column| {
        column.deinit(allocator);
    }
}
