const std = @import("std");
const ch = @import("clickhouse_native");
const common = @import("common.zig");

pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    var config = try common.loadConnectionConfig(allocator);
    defer config.deinit();

    var client = try common.connectClient(allocator, config, .zstd);
    defer client.deinit();

    var results = ch.BlockBuffer.init(allocator);
    defer results.deinit();

    var query = client.newQuery("SELECT number AS id, toString(number) AS label FROM numbers(5)");
    query.result = &results;
    try client.Do(.{}, &query);

    const stdout = std.io.getStdOut().writer();
    for (results.blocks.items) |block| {
        if (block.isEnd() or block.rows == 0) continue;

        const ids_view = try block.columns[0].asFixed();
        const ids = try ids_view.slice(u64);
        const labels = switch (block.columns[1]) {
            .string => |column| column.values,
            else => return error.UnexpectedColumnType,
        };

        for (ids, 0..) |id, idx| {
            try stdout.print("{d}: {s}\n", .{ id, labels[idx] });
        }
    }
}
