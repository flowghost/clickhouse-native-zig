const std = @import("std");
const ch = @import("clickhouse_native");
const common = @import("common.zig");

fn envHexSigner(message: []const u8, challenge: []const u8, out: *std.ArrayList(u8)) !void {
    _ = message;
    _ = challenge;

    const allocator = std.heap.page_allocator;
    const hex_value = std.process.getEnvVarOwned(allocator, "CH_SSH_SIGNATURE_HEX") catch |err| switch (err) {
        error.EnvironmentVariableNotFound => return error.MissingEnvironmentVariable,
        else => return err,
    };
    defer allocator.free(hex_value);

    if (hex_value.len % 2 != 0) return error.InvalidHexSignature;
    const raw = try allocator.alloc(u8, hex_value.len / 2);
    defer allocator.free(raw);
    _ = try std.fmt.hexToBytes(raw, hex_value);
    try out.appendSlice(raw);
}

pub fn main() !void {
    var gpa_state = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_state.deinit();
    const allocator = gpa_state.allocator();
    const stdout = std.io.getStdOut().writer();

    _ = std.process.getEnvVarOwned(allocator, "CH_SSH_SIGNATURE_HEX") catch {
        try stdout.writeAll(
            "Set CH_SSH_SIGNATURE_HEX to a raw SSH signature encoded as hex.\n" ++
            "This example only demonstrates the SshSignFn callback shape.\n",
        );
        return;
    };

    var config = try common.loadConnectionConfig(allocator);
    defer config.deinit();

    var client = try ch.Client.connectTcp(allocator, config.host, config.port, .{
        .database = config.database,
        .user = config.user,
        .password = "",
        .client_name = "zig-ssh-example",
        .ssh_signer = envHexSigner,
    });
    defer client.deinit();

    try client.ping();
    try stdout.writeAll("SSH-authenticated ping completed\n");
}
