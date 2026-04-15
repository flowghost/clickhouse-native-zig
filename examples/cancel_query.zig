const std = @import("std");
const ch = @import("clickhouse_native");
const common = @import("common.zig");

const CancelState = struct {
    canceled: bool = false,
};

fn isCanceled(user_data: ?*anyopaque) bool {
    const state: *CancelState = @ptrCast(@alignCast(user_data.?));
    return state.canceled;
}

fn onProgress(ctx: ch.QueryContext, progress: ch.Progress) !void {
    _ = progress;
    const state: *CancelState = @ptrCast(@alignCast(ctx.user_data.?));
    state.canceled = true;
    std.debug.print("progress arrived, requesting cancel\n", .{});
}

pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    var config = try common.loadConnectionConfig(allocator);
    defer config.deinit();

    var client = try common.connectClient(allocator, config, .disabled);
    defer client.deinit();

    var state = CancelState{};
    var query = client.newQuery("SELECT number FROM system.numbers LIMIT 1000000000");
    query.on_progress = onProgress;

    client.Do(.{
        .user_data = &state,
        .is_canceled = isCanceled,
    }, &query) catch |err| switch (err) {
        error.Canceled => {
            const stdout = std.io.getStdOut().writer();
            try stdout.print("query canceled, client closed={any}\n", .{client.isClosed()});
            return;
        },
        else => return err,
    };

    std.debug.print("query finished before cancellation was triggered\n", .{});
}
