const std = @import("std");
const ch = @import("clickhouse_native");
const common = @import("common.zig");

const HandlerState = struct {
    result_blocks: usize = 0,
    totals_blocks: usize = 0,
    extremes_blocks: usize = 0,
    log_rows: usize = 0,
    profile_event_rows: usize = 0,
};

fn stateFromContext(ctx: ch.QueryContext) *HandlerState {
    return @ptrCast(@alignCast(ctx.user_data.?));
}

fn onResult(ctx: ch.QueryContext, block: *const ch.DecodedBlock) !void {
    var state = stateFromContext(ctx);
    state.result_blocks += 1;
    std.debug.print("result block: rows={d} columns={d}\n", .{ block.rows, block.columns.len });
}

fn onTotals(ctx: ch.QueryContext, block: *const ch.DecodedBlock) !void {
    var state = stateFromContext(ctx);
    state.totals_blocks += 1;
    std.debug.print("totals block: rows={d}\n", .{block.rows});
}

fn onExtremes(ctx: ch.QueryContext, block: *const ch.DecodedBlock) !void {
    var state = stateFromContext(ctx);
    state.extremes_blocks += 1;
    std.debug.print("extremes block: rows={d}\n", .{block.rows});
}

fn onProgress(_: ch.QueryContext, progress: ch.Progress) !void {
    std.debug.print("progress: rows={d} bytes={d}\n", .{ progress.rows, progress.bytes });
}

fn onProfile(_: ch.QueryContext, profile: ch.Profile) !void {
    std.debug.print("profile: rows={d} blocks={d} bytes={d}\n", .{ profile.rows, profile.blocks, profile.bytes });
}

fn onLogsBatch(ctx: ch.QueryContext, logs: []const ch.ServerLog) !void {
    var state = stateFromContext(ctx);
    state.log_rows += logs.len;
    std.debug.print("logs batch: {d} rows\n", .{logs.len});
}

fn onProfileEventsBatch(ctx: ch.QueryContext, events: []const ch.ProfileEvent) !void {
    var state = stateFromContext(ctx);
    state.profile_event_rows += events.len;
    std.debug.print("profile events batch: {d} rows\n", .{events.len});
}

pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    var config = try common.loadConnectionConfig(allocator);
    defer config.deinit();

    var client = try common.connectClient(allocator, config, .zstd);
    defer client.deinit();

    var result_buffer = ch.BlockBuffer.init(allocator);
    defer result_buffer.deinit();
    var totals_buffer = ch.BlockBuffer.init(allocator);
    defer totals_buffer.deinit();
    var extremes_buffer = ch.BlockBuffer.init(allocator);
    defer extremes_buffer.deinit();

    var state = HandlerState{};
    var query = client.newQuery(
        "SELECT number % 2 AS bucket, sum(number) AS total FROM numbers(10) GROUP BY bucket WITH TOTALS ORDER BY bucket",
    );
    query.result = &result_buffer;
    query.totals = &totals_buffer;
    query.extremes = &extremes_buffer;
    query.on_result = onResult;
    query.on_totals = onTotals;
    query.on_extremes = onExtremes;
    query.on_progress = onProgress;
    query.on_profile = onProfile;
    query.on_logs_batch = onLogsBatch;
    query.on_profile_events_batch = onProfileEventsBatch;
    query.settings = try allocator.alloc(ch.Setting, 3);
    defer query.deinit(allocator);
    query.settings[0] = .{ .key = "extremes", .value = "1", .important = true };
    query.settings[1] = .{ .key = "send_logs_level", .value = "trace", .important = true };
    query.settings[2] = .{ .key = "log_queries", .value = "1", .important = true };

    try client.Do(.{ .user_data = &state }, &query);

    const stdout = std.io.getStdOut().writer();
    try stdout.print(
        "result_blocks={d} totals_blocks={d} extremes_blocks={d} log_rows={d} profile_event_rows={d}\n",
        .{ state.result_blocks, state.totals_blocks, state.extremes_blocks, state.log_rows, state.profile_event_rows },
    );
}
