const std = @import("std");
const ch = @import("clickhouse_native");
const common = @import("common.zig");

const ObserverState = struct {};

fn onLog(level: ch.LogLevel, scope: []const u8, message: []const u8, user_data: ?*anyopaque) void {
    _ = user_data;
    std.debug.print("[{s}] {s}: {s}\n", .{ @tagName(level), scope, message });
}

fn onConnect(event: ch.ConnectObserveEvent, user_data: ?*anyopaque) void {
    _ = user_data;
    switch (event) {
        .start => |value| std.debug.print("connect start {s}:{d} tls={}\n", .{ value.host, value.port, value.tls_enabled }),
        .finish => |value| std.debug.print("connect finish protocol={d} err={any}\n", .{ value.protocol_version, value.err }),
    }
}

fn onQuery(event: ch.QueryObserveEvent, user_data: ?*anyopaque) void {
    _ = user_data;
    switch (event) {
        .start => |value| std.debug.print("query start {s}\n", .{value.id}),
        .progress => |value| std.debug.print("progress rows={d} bytes={d}\n", .{ value.rows, value.bytes }),
        .profile => |value| std.debug.print("profile rows={d} blocks={d}\n", .{ value.rows, value.blocks }),
        .exception => |value| std.debug.print("server exception count={d}\n", .{value.items.len}),
        .finish => |value| std.debug.print("query finish err={any} rows={d}\n", .{ value.err, value.metrics.rows_received }),
    }
}

pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    var config = try common.loadConnectionConfig(allocator);
    defer config.deinit();

    var observer_state = ObserverState{};
    var client = try ch.Client.connectTcp(allocator, config.host, config.port, .{
        .database = config.database,
        .user = config.user,
        .password = config.password,
        .client_name = "zig-example",
        .compression = .zstd,
        .observer = .{
            .user_data = &observer_state,
            .on_log = onLog,
            .on_connect = onConnect,
            .on_query = onQuery,
        },
    });
    defer client.deinit();

    var query = client.newQuery("SELECT number, toString(number) FROM numbers(3)");
    try client.Do(.{}, &query);
}
