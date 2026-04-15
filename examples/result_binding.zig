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

    var ids = std.ArrayList(u64).init(allocator);
    defer ids.deinit();
    var labels = ch.OwnedByteSlices.init(allocator);
    defer labels.deinit();
    var metrics = ch.QueryMetrics{};

    var binding_columns = [_]ch.ResultBindingColumn{
        .{ .name = "id", .sink = .{ .uint64s = &ids } },
        .{ .name = "label", .sink = .{ .strings = &labels } },
    };
    var binding = ch.ResultBinding.init(allocator, binding_columns[0..]);

    var query = client.newQuery("SELECT number AS id, toString(number) AS label FROM numbers(5)");
    query.result_binding = &binding;
    query.metrics = &metrics;
    try client.Do(.{}, &query);

    const stdout = std.io.getStdOut().writer();
    try stdout.print("rows={d} blocks={d}\n", .{ metrics.rows_received, metrics.blocks_received });
    for (ids.items, 0..) |id, idx| {
        try stdout.print("{d}: {s}\n", .{ id, labels.items.items[idx] });
    }
}
