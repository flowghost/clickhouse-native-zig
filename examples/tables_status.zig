const std = @import("std");
const ch = @import("clickhouse_native");
const common = @import("common.zig");

pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    var config = try common.loadConnectionConfig(allocator);
    defer config.deinit();

    var client = try common.connectClient(allocator, config, .disabled);
    defer client.deinit();

    var packet = try client.requestTablesStatus(.{
        .tables = &.{
            .{ .database = "system", .table = "numbers" },
        },
    });
    defer packet.deinit();

    const stdout = std.io.getStdOut().writer();
    switch (packet.value) {
        .tables_status => |response| {
            for (response.entries) |entry| {
                try stdout.print(
                    "{s}.{s} replicated={any} readonly={any} delay={d}\n",
                    .{ entry.table.database, entry.table.table, entry.status.is_replicated, entry.status.is_readonly, entry.status.absolute_delay },
                );
            }
        },
        .exception => return error.ServerException,
        else => return error.UnexpectedPacket,
    }
}
