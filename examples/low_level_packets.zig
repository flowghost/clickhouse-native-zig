const std = @import("std");
const ch = @import("clickhouse_native");
const common = @import("common.zig");

pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    var config = try common.loadConnectionConfig(allocator);
    defer config.deinit();

    var client = try common.connectClient(allocator, config, .none);
    defer client.deinit();

    try client.sendQuery(client.newQuery("SELECT number FROM numbers(3)"));
    const stdout = std.io.getStdOut().writer();

    while (true) {
        var packet = try client.readServerPacket();
        defer packet.deinit();

        switch (packet.value) {
            .data => |data| {
                if (!data.block.isEnd()) try stdout.print("data block rows={d}\n", .{data.block.rows});
            },
            .totals => |data| {
                if (!data.block.isEnd()) try stdout.print("totals rows={d}\n", .{data.block.rows});
            },
            .extremes => |data| {
                if (!data.block.isEnd()) try stdout.print("extremes rows={d}\n", .{data.block.rows});
            },
            .progress => |progress| try stdout.print("progress rows={d}\n", .{progress.rows}),
            .profile => |profile| try stdout.print("profile blocks={d}\n", .{profile.blocks}),
            .exception => return error.ServerException,
            .end_of_stream => break,
            else => try stdout.print("packet={s}\n", .{@tagName(packet.value)}),
        }
    }
}
