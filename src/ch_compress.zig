const std = @import("std");

pub const BlockCompression = enum {
    none,
    lz4,
    zstd,
    lz4hc,
};

pub const U128 = struct {
    low: u64,
    high: u64,
};

pub const checksum_size: usize = 16;
pub const compress_header_size: usize = 1 + 4 + 4;
pub const header_size: usize = checksum_size + compress_header_size;
pub const method_offset: usize = 16;
pub const raw_size_offset: usize = 17;
pub const data_size_offset: usize = 21;
pub const max_data_size: usize = 1024 * 1024 * 128;
pub const max_block_size: usize = max_data_size;

pub const MethodEncoding = enum(u8) {
    none = 0x02,
    lz4 = 0x82,
    zstd = 0x90,
};

pub const FrameHeader = struct {
    checksum: U128,
    method: MethodEncoding,
    compressed_size: usize,
    data_size: usize,
};

const k0: u64 = 0xc3a5c85c97cb3127;
const k1: u64 = 0xb492b66fbe98f273;
const k2: u64 = 0x9ae16a3b2f90404f;
const k3: u64 = 0xc949d7c7509e6557;

extern fn LZ4_compressBound(src_size: c_int) c_int;
extern fn LZ4_compress_default(src: [*c]const u8, dst: [*c]u8, src_size: c_int, dst_capacity: c_int) c_int;
extern fn LZ4_compress_HC(src: [*c]const u8, dst: [*c]u8, src_size: c_int, dst_capacity: c_int, compression_level: c_int) c_int;
extern fn LZ4_decompress_safe(src: [*c]const u8, dst: [*c]u8, compressed_size: c_int, dst_capacity: c_int) c_int;

extern fn ZSTD_compressBound(src_size: usize) usize;
extern fn ZSTD_compress(dst: [*c]u8, dst_capacity: usize, src: [*c]const u8, src_size: usize, compression_level: c_int) usize;
extern fn ZSTD_decompress(dst: [*c]u8, dst_capacity: usize, src: [*c]const u8, compressed_size: usize) usize;
extern fn ZSTD_isError(code: usize) c_uint;
extern fn ZSTD_getErrorName(code: usize) [*:0]const u8;

pub fn checksum128(data: []const u8) U128 {
    return ch128(data);
}

pub fn decodeFrameHeader(header_bytes: []const u8) !FrameHeader {
    if (header_bytes.len != header_size) return error.InvalidCompressedHeader;

    const raw_size = std.mem.readInt(u32, header_bytes[raw_size_offset .. raw_size_offset + 4], .little);
    const data_size = std.mem.readInt(u32, header_bytes[data_size_offset .. data_size_offset + 4], .little);
    if (raw_size < compress_header_size) return error.InvalidCompressedHeader;
    if (data_size > max_data_size) return error.CompressedDataTooLarge;

    const method = std.meta.intToEnum(MethodEncoding, header_bytes[method_offset]) catch return error.UnsupportedCompressionMethod;
    const compressed_size = raw_size - compress_header_size;
    if (compressed_size > max_block_size) return error.CompressedDataTooLarge;

    return .{
        .checksum = .{
            .low = std.mem.readInt(u64, header_bytes[0..8], .little),
            .high = std.mem.readInt(u64, header_bytes[8..16], .little),
        },
        .method = method,
        .compressed_size = compressed_size,
        .data_size = data_size,
    };
}

pub fn frameLengthFromHeader(header_bytes: []const u8) !usize {
    const header = try decodeFrameHeader(header_bytes);
    return std.math.add(usize, header_size, header.compressed_size);
}

pub fn compressFrame(allocator: std.mem.Allocator, data: []const u8, method: BlockCompression, level: u32) ![]u8 {
    if (data.len > max_data_size) return error.CompressedDataTooLarge;

    const payload = switch (method) {
        .none => try allocator.dupe(u8, data),
        .lz4 => try compressLz4(allocator, data),
        .lz4hc => try compressLz4HC(allocator, data, level),
        .zstd => try compressZstd(allocator, data, level),
    };
    defer allocator.free(payload);

    const total = try std.math.add(usize, header_size, payload.len);
    const out = try allocator.alloc(u8, total);
    errdefer allocator.free(out);

    const encoding: MethodEncoding = switch (method) {
        .none => .none,
        .lz4, .lz4hc => .lz4,
        .zstd => .zstd,
    };

    out[method_offset] = @intFromEnum(encoding);
    std.mem.writeInt(u32, out[raw_size_offset .. raw_size_offset + 4], std.math.cast(u32, payload.len + compress_header_size) orelse return error.IntegerOverflow, .little);
    std.mem.writeInt(u32, out[data_size_offset .. data_size_offset + 4], std.math.cast(u32, data.len) orelse return error.IntegerOverflow, .little);
    @memcpy(out[header_size..], payload);

    const checksum = ch128(out[method_offset..]);
    std.mem.writeInt(u64, out[0..8], checksum.low, .little);
    std.mem.writeInt(u64, out[8..16], checksum.high, .little);
    return out;
}

pub fn decompressFrame(allocator: std.mem.Allocator, frame: []const u8) ![]u8 {
    if (frame.len < header_size) return error.InvalidCompressedHeader;
    const header = try decodeFrameHeader(frame[0..header_size]);
    if (frame.len != header_size + header.compressed_size) return error.InvalidCompressedHeader;

    const expected = ch128(frame[method_offset..]);
    if (expected.low != header.checksum.low or expected.high != header.checksum.high) {
        return error.CorruptedCompressedData;
    }

    const payload = frame[header_size..];
    switch (header.method) {
        .none => {
            if (payload.len != header.data_size) return error.InvalidCompressedHeader;
            return allocator.dupe(u8, payload);
        },
        .lz4 => return decompressLz4(allocator, payload, header.data_size),
        .zstd => return decompressZstd(allocator, payload, header.data_size),
    }
}

fn compressLz4(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    const bound = LZ4_compressBound(try castCInt(data.len));
    if (bound <= 0) return error.Lz4CompressionFailed;

    const dst = try allocator.alloc(u8, @intCast(bound));
    errdefer allocator.free(dst);

    const written = LZ4_compress_default(data.ptr, dst.ptr, try castCInt(data.len), bound);
    if (written <= 0) return error.Lz4CompressionFailed;
    return allocator.realloc(dst, @intCast(written));
}

fn compressLz4HC(allocator: std.mem.Allocator, data: []const u8, level: u32) ![]u8 {
    const bound = LZ4_compressBound(try castCInt(data.len));
    if (bound <= 0) return error.Lz4CompressionFailed;

    const dst = try allocator.alloc(u8, @intCast(bound));
    errdefer allocator.free(dst);

    const hc_level: c_int = @intCast(@min(@max(if (level == 0) @as(u32, 9) else level, @as(u32, 1)), @as(u32, 12)));
    const written = LZ4_compress_HC(data.ptr, dst.ptr, try castCInt(data.len), bound, hc_level);
    if (written <= 0) return error.Lz4CompressionFailed;
    return allocator.realloc(dst, @intCast(written));
}

fn decompressLz4(allocator: std.mem.Allocator, payload: []const u8, data_size: usize) ![]u8 {
    const out = try allocator.alloc(u8, data_size);
    errdefer allocator.free(out);

    const read = LZ4_decompress_safe(payload.ptr, out.ptr, try castCInt(payload.len), try castCInt(data_size));
    if (read < 0) return error.Lz4DecompressionFailed;
    if (@as(usize, @intCast(read)) != data_size) return error.Lz4DecompressionFailed;
    return out;
}

fn compressZstd(allocator: std.mem.Allocator, data: []const u8, level: u32) ![]u8 {
    const bound = ZSTD_compressBound(data.len);
    const dst = try allocator.alloc(u8, bound);
    errdefer allocator.free(dst);

    const compression_level: c_int = if (level == 0) 0 else @intCast(level);
    const written = ZSTD_compress(dst.ptr, dst.len, data.ptr, data.len, compression_level);
    try ensureZstdSuccess(written, error.ZstdCompressionFailed);
    return allocator.realloc(dst, written);
}

fn decompressZstd(allocator: std.mem.Allocator, payload: []const u8, data_size: usize) ![]u8 {
    const out = try allocator.alloc(u8, data_size);
    errdefer allocator.free(out);

    const written = ZSTD_decompress(out.ptr, out.len, payload.ptr, payload.len);
    try ensureZstdSuccess(written, error.ZstdDecompressionFailed);
    if (written != data_size) return error.ZstdDecompressionFailed;
    return out;
}

fn ensureZstdSuccess(code: usize, failure: anyerror) !void {
    if (ZSTD_isError(code) != 0) {
        std.log.err("zstd error: {s}", .{std.mem.span(ZSTD_getErrorName(code))});
        return failure;
    }
}

fn castCInt(value: usize) !c_int {
    return std.math.cast(c_int, value) orelse error.IntegerOverflow;
}

fn fetch32(bytes: []const u8) u32 {
    return std.mem.readInt(u32, bytes[0..4], .little);
}

fn rot64(value: u64, shift: u6) u64 {
    if (shift == 0) return value;
    return std.math.rotr(u64, value, shift);
}

fn shiftMix(value: u64) u64 {
    return value ^ (value >> 47);
}

fn hash128to64(x: U128) u64 {
    const mul: u64 = 0x9ddfea08eb382d69;
    var a = (x.low ^ x.high) *% mul;
    a ^= a >> 47;
    var b = (x.high ^ a) *% mul;
    b ^= b >> 47;
    b *%= mul;
    return b;
}

fn weakHash32Seeds(w: u64, x: u64, y: u64, z: u64, a: u64, b: u64) U128 {
    var aa = a +% w;
    var bb = rot64(b +% aa +% z, 21);
    const c = aa;
    aa +%= x;
    aa +%= y;
    bb +%= rot64(aa, 44);
    return .{ .low = aa +% z, .high = bb +% c };
}

fn weakHash32SeedsBytes(s: []const u8, a: u64, b: u64) U128 {
    return weakHash32Seeds(
        std.mem.readInt(u64, s[0..8], .little),
        std.mem.readInt(u64, s[8..16], .little),
        std.mem.readInt(u64, s[16..24], .little),
        std.mem.readInt(u64, s[24..32], .little),
        a,
        b,
    );
}

fn ch16(u: u64, v: u64) u64 {
    return hash128to64(.{ .low = u, .high = v });
}

fn ch0to16(s: []const u8) u64 {
    const length = s.len;
    if (length > 8) {
        const a = std.mem.readInt(u64, s[0..8], .little);
        const b = std.mem.readInt(u64, s[length - 8 ..][0..8], .little);
        return ch16(a, rot64(b +% length, @intCast(length))) ^ b;
    }
    if (length >= 4) {
        const a = @as(u64, fetch32(s));
        return ch16(length +% (a << 3), @as(u64, fetch32(s[length - 4 ..])));
    }
    if (length > 0) {
        const a = s[0];
        const b = s[length >> 1];
        const c = s[length - 1];
        const y = @as(u32, a) + (@as(u32, b) << 8);
        const z = @as(u32, @intCast(length)) + (@as(u32, c) << 2);
        return shiftMix(@as(u64, y) *% k2 ^ @as(u64, z) *% k3) *% k2;
    }
    return k2;
}

fn chMurmur(input: []const u8, seed: U128) U128 {
    const length = input.len;
    var s = input;
    var a = seed.low;
    var b = seed.high;
    var c: u64 = 0;
    var d: u64 = 0;
    var remaining: isize = @intCast(length -| 16);

    if (length <= 16) {
        a = shiftMix(a *% k1) *% k1;
        c = b *% k1 +% ch0to16(s);
        if (length >= 8) {
            d = shiftMix(a +% std.mem.readInt(u64, s[0..8], .little));
        } else {
            d = shiftMix(a +% c);
        }
    } else {
        c = ch16(std.mem.readInt(u64, s[length - 8 ..][0..8], .little) +% k1, a);
        d = ch16(b +% length, c +% std.mem.readInt(u64, s[length - 16 ..][0..8], .little));
        a +%= d;

        a ^= shiftMix(std.mem.readInt(u64, s[0..8], .little) *% k1) *% k1;
        a *%= k1;
        b ^= a;
        c ^= shiftMix(std.mem.readInt(u64, s[8..16], .little) *% k1) *% k1;
        c *%= k1;
        d ^= c;
        s = s[16..];
        remaining -= 16;

        while (remaining > 0 and s.len >= 16) : (remaining -= 16) {
            a ^= shiftMix(std.mem.readInt(u64, s[0..8], .little) *% k1) *% k1;
            a *%= k1;
            b ^= a;
            c ^= shiftMix(std.mem.readInt(u64, s[8..16], .little) *% k1) *% k1;
            c *%= k1;
            d ^= c;
            s = s[16..];
        }
    }

    a = ch16(a, c);
    b = ch16(d, b);
    return .{ .low = a ^ b, .high = ch16(b, a) };
}

fn ch128(input: []const u8) U128 {
    if (input.len >= 16) {
        return ch128Seed(input[16..], .{
            .low = std.mem.readInt(u64, input[0..8], .little) ^ k3,
            .high = std.mem.readInt(u64, input[8..16], .little),
        });
    }
    if (input.len >= 8) {
        const length = @as(u64, @intCast(input.len));
        return ch128Seed(&.{}, .{
            .low = std.mem.readInt(u64, input[0..8], .little) ^ (length *% k0),
            .high = std.mem.readInt(u64, input[input.len - 8 ..][0..8], .little) ^ k1,
        });
    }
    return ch128Seed(input, .{ .low = k0, .high = k1 });
}

fn ch128Seed(input: []const u8, seed: U128) U128 {
    if (input.len < 128) return chMurmur(input, seed);

    var s = input;
    const tail = input;
    var v: U128 = undefined;
    var w: U128 = undefined;
    var x = seed.low;
    var y = seed.high;
    var z = @as(u64, @intCast(s.len)) *% k1;

    v.low = rot64(y ^ k1, 49) *% k1 +% std.mem.readInt(u64, s[0..8], .little);
    v.high = rot64(v.low, 42) *% k1 +% std.mem.readInt(u64, s[8..16], .little);
    w.low = rot64(y +% z, 35) *% k1 +% x;
    w.high = rot64(x +% std.mem.readInt(u64, s[88..96], .little), 53) *% k1;

    while (s.len >= 128) {
        x = rot64(x +% y +% v.low +% std.mem.readInt(u64, s[16..24], .little), 37) *% k1;
        y = rot64(y +% v.high +% std.mem.readInt(u64, s[48..56], .little), 42) *% k1;
        x ^= w.high;
        y ^= v.low;
        z = rot64(z ^ w.low, 33);
        v = weakHash32SeedsBytes(s[0..32], v.high *% k1, x +% w.low);
        w = weakHash32SeedsBytes(s[32..64], z +% w.high, y);
        std.mem.swap(u64, &z, &x);

        const offset = 64;
        x = rot64(x +% y +% v.low +% std.mem.readInt(u64, s[offset + 16 .. offset + 24], .little), 37) *% k1;
        y = rot64(y +% v.high +% std.mem.readInt(u64, s[offset + 48 .. offset + 56], .little), 42) *% k1;
        x ^= w.high;
        y ^= v.low;
        z = rot64(z ^ w.low, 33);
        v = weakHash32SeedsBytes(s[offset .. offset + 32], v.high *% k1, x +% w.low);
        w = weakHash32SeedsBytes(s[offset + 32 .. offset + 64], z +% w.high, y);
        std.mem.swap(u64, &z, &x);

        s = s[128..];
    }

    y +%= rot64(w.low, 37) *% k0 +% z;
    x +%= rot64(v.low +% z, 49) *% k0;

    var consumed: usize = 0;
    while (consumed < s.len) : (consumed += 32) {
        const i = consumed + 32;
        y = rot64(y -% x, 42) *% k0 +% v.high;
        w.low +%= std.mem.readInt(u64, tail[tail.len - i + 16 ..][0..8], .little);
        x = rot64(x, 49) *% k0 +% w.low;
        w.low +%= v.low;
        v = weakHash32SeedsBytes(tail[tail.len - i ..][0..32], v.low, v.high);
    }

    x = ch16(x, v.low);
    y = ch16(y, w.low);

    return .{
        .low = ch16(x +% v.high, w.high) +% y,
        .high = ch16(x +% w.high, y +% v.high),
    };
}
