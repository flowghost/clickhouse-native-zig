const std = @import("std");
const ch = @import("clickhouse_native");

pub const ConnectionConfig = struct {
    allocator: std.mem.Allocator,
    host: []u8,
    port: u16,
    user: []u8,
    password: []u8,
    database: []u8,

    pub fn deinit(self: *ConnectionConfig) void {
        self.allocator.free(self.host);
        self.allocator.free(self.user);
        self.allocator.free(self.password);
        self.allocator.free(self.database);
        self.* = undefined;
    }
};

pub fn loadConnectionConfig(allocator: std.mem.Allocator) !ConnectionConfig {
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    return .{
        .allocator = allocator,
        .host = try stringArgOrEnv(allocator, args, 1, "CH_HOST", "127.0.0.1"),
        .port = try portArgOrEnv(allocator, args, 2, "CH_PORT", 9000),
        .user = try stringArgOrEnv(allocator, args, 3, "CH_USER", "default"),
        .password = try stringArgOrEnv(allocator, args, 4, "CH_PASSWORD", ""),
        .database = try stringArgOrEnv(allocator, args, 5, "CH_DATABASE", "default"),
    };
}

pub fn connectClient(allocator: std.mem.Allocator, config: ConnectionConfig, compression: ch.BlockCompression) !ch.Client {
    return ch.Client.connectTcp(allocator, config.host, config.port, .{
        .database = config.database,
        .user = config.user,
        .password = config.password,
        .client_name = "zig-example",
        .compression = compression,
    });
}

pub fn execQuery(client: *ch.Client, sql: []const u8) !void {
    var query = client.newQuery(sql);
    try client.Do(.{}, &query);
}

pub fn randomTableName(allocator: std.mem.Allocator, prefix: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}_{x}", .{ prefix, std.crypto.random.int(u64) });
}

fn stringArgOrEnv(allocator: std.mem.Allocator, args: []const []const u8, index: usize, env_key: []const u8, default: []const u8) ![]u8 {
    if (args.len > index) return allocator.dupe(u8, args[index]);
    return std.process.getEnvVarOwned(allocator, env_key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => allocator.dupe(u8, default),
        else => err,
    };
}

fn portArgOrEnv(allocator: std.mem.Allocator, args: []const []const u8, index: usize, env_key: []const u8, default: u16) !u16 {
    if (args.len > index) return std.fmt.parseInt(u16, args[index], 10);

    const raw = std.process.getEnvVarOwned(allocator, env_key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => return default,
        else => return err,
    };
    defer allocator.free(raw);
    return std.fmt.parseInt(u16, raw, 10);
}
