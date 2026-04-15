const std = @import("std");
const ch = @import("clickhouse_native");

pub fn main() !void {
    var encoder = ch.Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    const hello = ch.ClientHello{
        .name = "zig-smoke",
        .major = 1,
        .minor = 0,
        .protocol_version = ch.default_protocol_version,
        .database = "default",
        .user = "default",
        .password = "",
    };
    try hello.encodePacket(&encoder);
    _ = encoder.bytes();
}
