const std = @import("std");
const ch = @import("clickhouse_native");
const common = @import("common.zig");

pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();

    var config = try common.loadConnectionConfig(allocator);
    defer config.deinit();

    var pool = try ch.Pool.init(allocator, .{
        .host = config.host,
        .port = config.port,
        .client_options = .{
            .database = config.database,
            .user = config.user,
            .password = config.password,
            .client_name = "zig-example-pool",
            .compression = .zstd,
        },
        .max_conns = 2,
        .min_conns = 1,
    });
    defer pool.deinit();

    var query = ch.Query{
        .body = "SELECT number FROM numbers(2)",
        .compression = .disabled,
        .info = .{
            .protocol_version = ch.default_protocol_version,
            .major = 0,
            .minor = 1,
        },
    };
    try pool.Do(.{}, &query);

    const stats = pool.stat();
    const stdout = std.io.getStdOut().writer();
    try stdout.print("pool total={d} idle={d} acquired={d}\n", .{
        stats.total_conns,
        stats.idle_conns,
        stats.acquired_conns,
    });
}
