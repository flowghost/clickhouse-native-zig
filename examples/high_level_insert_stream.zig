const std = @import("std");
const ch = @import("clickhouse_native");
const common = @import("common.zig");

const StreamState = struct {
    stage: usize = 0,
    batches: [2][2]ch.Column,

    fn deinit(self: *StreamState, allocator: std.mem.Allocator) void {
        for (&self.batches) |*batch| {
            for (batch) |*column| {
                column.deinit(allocator);
            }
        }
    }
};

fn onInput(ctx: ch.QueryContext, query: *ch.Query) !void {
    const state: *StreamState = @ptrCast(@alignCast(ctx.user_data.?));
    switch (state.stage) {
        0 => {
            query.input = state.batches[0][0..];
            state.stage = 1;
        },
        1 => {
            query.input = state.batches[1][0..];
            state.stage = 2;
        },
        else => {
            query.input = &.{};
            return error.EndOfInput;
        },
    }
}

pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    var config = try common.loadConnectionConfig(allocator);
    defer config.deinit();

    var client = try common.connectClient(allocator, config, .lz4);
    defer client.deinit();

    const table = try common.randomTableName(allocator, "zig_stream_example");
    defer allocator.free(table);

    const drop_sql = try std.fmt.allocPrint(allocator, "DROP TABLE IF EXISTS {s}", .{table});
    defer allocator.free(drop_sql);
    const create_sql = try std.fmt.allocPrint(allocator,
        \\CREATE TABLE {s}
        \\(
        \\    id UInt64,
        \\    name String
        \\) ENGINE = Memory
    , .{table});
    defer allocator.free(create_sql);
    const insert_sql = try std.fmt.allocPrint(allocator, "INSERT INTO {s} VALUES", .{table});
    defer allocator.free(insert_sql);
    const select_sql = try std.fmt.allocPrint(allocator, "SELECT id, name FROM {s} ORDER BY id", .{table});
    defer allocator.free(select_sql);

    try common.execQuery(&client, drop_sql);
    try common.execQuery(&client, create_sql);

    const batch1_ids = [_]u64{ 1, 2 };
    const batch1_names = [_][]const u8{ "left", "right" };
    const batch2_ids = [_]u64{ 3 };
    const batch2_names = [_][]const u8{ "tail" };

    var stream_state = StreamState{
        .batches = .{
            .{
                try ch.initOwnedFixedColumn(allocator, "id", "UInt64", batch1_ids[0..]),
                try ch.initOwnedStringColumn(allocator, "name", batch1_names[0..]),
            },
            .{
                try ch.initOwnedFixedColumn(allocator, "id", "UInt64", batch2_ids[0..]),
                try ch.initOwnedStringColumn(allocator, "name", batch2_names[0..]),
            },
        },
    };
    defer stream_state.deinit(allocator);

    var insert_query = client.newQuery(insert_sql);
    insert_query.on_input = onInput;
    try client.Do(.{ .user_data = &stream_state }, &insert_query);

    var results = ch.BlockBuffer.init(allocator);
    defer results.deinit();
    var select_query = client.newQuery(select_sql);
    select_query.result = &results;
    try client.Do(.{}, &select_query);

    const stdout = std.io.getStdOut().writer();
    for (results.blocks.items) |block| {
        if (block.isEnd() or block.rows == 0) continue;
        const ids = try (try block.columns[0].asFixed()).slice(u64);
        const names = switch (block.columns[1]) {
            .string => |column| column.values,
            else => return error.UnexpectedColumnType,
        };
        for (ids, 0..) |id, idx| {
            try stdout.print("inserted {d} -> {s}\n", .{ id, names[idx] });
        }
    }

    try common.execQuery(&client, drop_sql);
}
