const builtin = @import("builtin");
const std = @import("std");
const ch_compress = @import("ch_compress.zig");

pub const default_protocol_version: u32 = 54460;
pub const default_client_name = "clickhouse/native-zig";

pub const Feature = enum(u32) {
    block_info = 51903,
    timezone = 54058,
    quota_key_in_client_info = 54060,
    display_name = 54372,
    version_patch = 54401,
    temp_tables = 50264,
    server_logs = 54406,
    column_defaults_metadata = 54410,
    client_write_info = 54420,
    settings_serialized_as_strings = 54429,
    inter_server_secret = 54441,
    open_telemetry = 54442,
    x_forwarded_for_in_client_info = 54443,
    referer_in_client_info = 54447,
    distributed_depth = 54448,
    query_start_time = 54449,
    profile_events = 54451,
    parallel_replicas = 54453,
    custom_serialization = 54454,
    quota_key = 54458,
    parameters = 54459,
    server_query_time_in_progress = 54460,
    tables_status = 54226,
    table_read_only_check = 54467,
    json_strings = 54475,

    pub fn enabled(self: Feature, revision: u32) bool {
        return revision >= @intFromEnum(self);
    }
};

pub const ClientCode = enum(u8) {
    hello = 0,
    query = 1,
    data = 2,
    cancel = 3,
    ping = 4,
    tables_status_request = 5,
    ssh_challenge_request = 11,
    ssh_challenge_response = 12,

    pub fn encode(self: ClientCode, encoder: *Encoder) !void {
        try encoder.putByte(@intFromEnum(self));
    }

    pub fn decode(decoder: *Decoder) !ClientCode {
        const raw = try decoder.readByte();
        return std.meta.intToEnum(ClientCode, raw) catch error.InvalidClientCode;
    }
};

pub const ServerCode = enum(u8) {
    hello = 0,
    data = 1,
    exception = 2,
    progress = 3,
    pong = 4,
    end_of_stream = 5,
    profile = 6,
    totals = 7,
    extremes = 8,
    tables_status = 9,
    log = 10,
    table_columns = 11,
    part_uuids = 12,
    read_task_request = 13,
    profile_events = 14,
    ssh_challenge = 18,

    pub fn encode(self: ServerCode, encoder: *Encoder) !void {
        try encoder.putByte(@intFromEnum(self));
    }

    pub fn decode(decoder: *Decoder) !ServerCode {
        const raw = try decoder.readByte();
        return std.meta.intToEnum(ServerCode, raw) catch error.InvalidServerCode;
    }

    pub fn compressible(self: ServerCode) bool {
        return switch (self) {
            .data, .totals, .extremes => true,
            else => false,
        };
    }
};

pub const Compression = enum(u8) {
    disabled = 0,
    enabled = 1,

    pub fn encode(self: Compression, encoder: *Encoder) !void {
        try encoder.putVarUInt(@intFromEnum(self));
    }

    pub fn decode(decoder: *Decoder) !Compression {
        const raw = try decoder.readVarUInt();
        if (raw > std.math.maxInt(u8)) return error.InvalidCompression;
        return std.meta.intToEnum(Compression, @as(u8, @intCast(raw))) catch error.InvalidCompression;
    }
};

pub const BlockCompression = enum(u8) {
    disabled = 0,
    lz4 = 1,
    zstd = 2,
    none = 3,
    lz4hc = 4,
};

pub const Stage = enum(u8) {
    fetch_columns = 0,
    with_mergeable_state = 1,
    complete = 2,

    pub fn encode(self: Stage, encoder: *Encoder) !void {
        try encoder.putVarUInt(@intFromEnum(self));
    }

    pub fn decode(decoder: *Decoder) !Stage {
        const raw = try decoder.readVarUInt();
        if (raw > std.math.maxInt(u8)) return error.InvalidStage;
        return std.meta.intToEnum(Stage, @as(u8, @intCast(raw))) catch error.InvalidStage;
    }
};

pub const Interface = enum(u8) {
    tcp = 1,
    http = 2,
};

pub const ClientQueryKind = enum(u8) {
    none = 0,
    initial = 1,
    secondary = 2,
};

pub const UUID = [16]u8;

pub const QualifiedTableName = struct {
    database: []const u8,
    table: []const u8,
};

pub const DialerFn = *const fn (allocator: std.mem.Allocator, host: []const u8, port: u16) anyerror!std.net.Stream;
pub const SshSignFn = *const fn (message: []const u8, challenge: []const u8, out: *std.ArrayList(u8)) anyerror!void;

pub const TlsCaMode = enum {
    system,
    self_signed,
    no_verification,
};

pub const TlsOptions = struct {
    enabled: bool = false,
    server_name: []const u8 = "",
    ca_mode: TlsCaMode = .system,
    allow_truncation_attacks: bool = false,
};

pub const Encoder = struct {
    buf: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) Encoder {
        return .{ .buf = std.ArrayList(u8).init(allocator) };
    }

    pub fn deinit(self: *Encoder) void {
        self.buf.deinit();
    }

    pub fn bytes(self: *const Encoder) []const u8 {
        return self.buf.items;
    }

    pub fn clearRetainingCapacity(self: *Encoder) void {
        self.buf.clearRetainingCapacity();
    }

    pub fn putRaw(self: *Encoder, raw: []const u8) !void {
        try self.buf.appendSlice(raw);
    }

    pub fn putByte(self: *Encoder, value: u8) !void {
        try self.buf.append(value);
    }

    pub fn putBool(self: *Encoder, value: bool) !void {
        try self.putByte(if (value) 1 else 0);
    }

    pub fn putVarUInt(self: *Encoder, value: u64) !void {
        var current = value;
        while (true) {
            var byte: u8 = @intCast(current & 0x7f);
            current >>= 7;
            if (current != 0) byte |= 0x80;
            try self.putByte(byte);
            if (current == 0) break;
        }
    }

    pub fn putString(self: *Encoder, value: []const u8) !void {
        try self.putVarUInt(value.len);
        try self.putRaw(value);
    }

    pub fn putInt32LE(self: *Encoder, value: i32) !void {
        var raw: [4]u8 = undefined;
        std.mem.writeInt(i32, &raw, value, .little);
        try self.putRaw(&raw);
    }

    pub fn putInt64LE(self: *Encoder, value: i64) !void {
        var raw: [8]u8 = undefined;
        std.mem.writeInt(i64, &raw, value, .little);
        try self.putRaw(&raw);
    }

    pub fn putUInt64LE(self: *Encoder, value: u64) !void {
        var raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &raw, value, .little);
        try self.putRaw(&raw);
    }
};

pub const Decoder = struct {
    data: []const u8,
    pos: usize = 0,

    pub fn init(data: []const u8) Decoder {
        return .{ .data = data };
    }

    pub fn remaining(self: *const Decoder) []const u8 {
        return self.data[self.pos..];
    }

    pub fn eof(self: *const Decoder) bool {
        return self.pos >= self.data.len;
    }

    pub fn readByte(self: *Decoder) !u8 {
        if (self.pos >= self.data.len) return error.UnexpectedEof;
        const value = self.data[self.pos];
        self.pos += 1;
        return value;
    }

    pub fn readBool(self: *Decoder) !bool {
        const value = try self.readByte();
        return switch (value) {
            0 => false,
            1 => true,
            else => error.InvalidBool,
        };
    }

    pub fn readVarUInt(self: *Decoder) !u64 {
        var value: u64 = 0;
        var shift: u6 = 0;
        while (true) {
            const byte = try self.readByte();
            value |= (@as(u64, byte & 0x7f) << shift);
            if ((byte & 0x80) == 0) return value;
            if (shift >= 63) return error.InvalidVarUInt;
            shift += 7;
        }
    }

    pub fn readSlice(self: *Decoder, len: usize) ![]const u8 {
        if (self.data.len - self.pos < len) return error.UnexpectedEof;
        const out = self.data[self.pos .. self.pos + len];
        self.pos += len;
        return out;
    }

    pub fn readString(self: *Decoder) ![]const u8 {
        const len = try self.readVarUInt();
        if (len > std.math.maxInt(usize)) return error.LengthOverflow;
        return self.readSlice(@as(usize, @intCast(len)));
    }

    pub fn readInt32LE(self: *Decoder) !i32 {
        const raw = try self.readSlice(4);
        return std.mem.readInt(i32, raw[0..4], .little);
    }

    pub fn readInt64LE(self: *Decoder) !i64 {
        const raw = try self.readSlice(8);
        return std.mem.readInt(i64, raw[0..8], .little);
    }

    pub fn readUInt64LE(self: *Decoder) !u64 {
        const raw = try self.readSlice(8);
        return std.mem.readInt(u64, raw[0..8], .little);
    }
};

pub const StreamReader = struct {
    stream: ?std.net.Stream = null,
    user_data: ?*anyopaque = null,
    read_fn: ?*const fn (?*anyopaque, []u8) anyerror!usize = null,
    scratch: [16]u8 = undefined,

    pub fn init(stream: std.net.Stream) StreamReader {
        return .{ .stream = stream };
    }

    pub fn initWithReader(user_data: ?*anyopaque, read_fn: *const fn (?*anyopaque, []u8) anyerror!usize) StreamReader {
        return .{
            .user_data = user_data,
            .read_fn = read_fn,
        };
    }

    fn readSome(self: *StreamReader, buf: []u8) !usize {
        if (self.read_fn) |f| return f(self.user_data, buf);
        return self.stream.?.read(buf);
    }

    fn readExact(self: *StreamReader, buf: []u8) !void {
        var read_len: usize = 0;
        while (read_len < buf.len) {
            const n = try self.readSome(buf[read_len..]);
            if (n == 0) return error.UnexpectedEof;
            read_len += n;
        }
    }

    pub fn readByte(self: *StreamReader) !u8 {
        try self.readExact(self.scratch[0..1]);
        return self.scratch[0];
    }

    pub fn readBool(self: *StreamReader) !bool {
        const value = try self.readByte();
        return switch (value) {
            0 => false,
            1 => true,
            else => error.InvalidBool,
        };
    }

    pub fn readVarUInt(self: *StreamReader) !u64 {
        var value: u64 = 0;
        var shift: u6 = 0;
        while (true) {
            const byte = try self.readByte();
            value |= (@as(u64, byte & 0x7f) << shift);
            if ((byte & 0x80) == 0) return value;
            if (shift >= 63) return error.InvalidVarUInt;
            shift += 7;
        }
    }

    pub fn readInt32LE(self: *StreamReader) !i32 {
        try self.readExact(self.scratch[0..4]);
        return std.mem.readInt(i32, self.scratch[0..4], .little);
    }

    pub fn readInt64LE(self: *StreamReader) !i64 {
        try self.readExact(self.scratch[0..8]);
        return std.mem.readInt(i64, self.scratch[0..8], .little);
    }

    pub fn readUInt64LE(self: *StreamReader) !u64 {
        try self.readExact(self.scratch[0..8]);
        return std.mem.readInt(u64, self.scratch[0..8], .little);
    }

    pub fn readStringAlloc(self: *StreamReader, allocator: std.mem.Allocator) ![]u8 {
        const len = try self.readVarUInt();
        const usize_len = try castVarUInt(usize, len);
        const out = try allocator.alloc(u8, usize_len);
        errdefer allocator.free(out);
        try self.readExact(out);
        return out;
    }
};

pub const ClientHello = struct {
    name: []const u8,
    major: u32,
    minor: u32,
    protocol_version: u32,
    database: []const u8,
    user: []const u8,
    password: []const u8,

    pub fn encodePacket(self: ClientHello, encoder: *Encoder) !void {
        try ClientCode.hello.encode(encoder);
        try encoder.putString(self.name);
        try encoder.putVarUInt(self.major);
        try encoder.putVarUInt(self.minor);
        try encoder.putVarUInt(self.protocol_version);
        try encoder.putString(self.database);
        try encoder.putString(self.user);
        try encoder.putString(self.password);
    }

    pub fn decodePayload(decoder: *Decoder) !ClientHello {
        return .{
            .name = try decoder.readString(),
            .major = try readVarUIntAs(u32, decoder),
            .minor = try readVarUIntAs(u32, decoder),
            .protocol_version = try readVarUIntAs(u32, decoder),
            .database = try decoder.readString(),
            .user = try decoder.readString(),
            .password = try decoder.readString(),
        };
    }

    pub fn decodePacket(decoder: *Decoder) !ClientHello {
        const code = try ClientCode.decode(decoder);
        if (code != .hello) return error.UnexpectedPacket;
        return decodePayload(decoder);
    }

    pub fn decodePayloadFromStream(reader: *StreamReader, allocator: std.mem.Allocator) !ClientHello {
        return .{
            .name = try reader.readStringAlloc(allocator),
            .major = try castVarUInt(u32, try reader.readVarUInt()),
            .minor = try castVarUInt(u32, try reader.readVarUInt()),
            .protocol_version = try castVarUInt(u32, try reader.readVarUInt()),
            .database = try reader.readStringAlloc(allocator),
            .user = try reader.readStringAlloc(allocator),
            .password = try reader.readStringAlloc(allocator),
        };
    }

    pub fn decodePacketFromStream(reader: *StreamReader, allocator: std.mem.Allocator) !ClientHello {
        const code = try readClientCodeFromStream(reader);
        if (code != .hello) return error.UnexpectedPacket;
        return decodePayloadFromStream(reader, allocator);
    }
};

pub const ServerHello = struct {
    name: []const u8,
    major: u32,
    minor: u32,
    revision: u32,
    timezone: []const u8 = "",
    display_name: []const u8 = "",
    patch: u32 = 0,

    pub fn encodePacket(self: ServerHello, encoder: *Encoder, client_revision: u32) !void {
        try ServerCode.hello.encode(encoder);
        try encoder.putString(self.name);
        try encoder.putVarUInt(self.major);
        try encoder.putVarUInt(self.minor);
        try encoder.putVarUInt(self.revision);
        if (Feature.timezone.enabled(client_revision)) {
            try encoder.putString(self.timezone);
        }
        if (Feature.display_name.enabled(client_revision)) {
            try encoder.putString(self.display_name);
        }
        if (Feature.version_patch.enabled(client_revision)) {
            try encoder.putVarUInt(self.patch);
        }
    }

    pub fn decodePayload(decoder: *Decoder, client_revision: u32) !ServerHello {
        var hello = ServerHello{
            .name = try decoder.readString(),
            .major = try readVarUIntAs(u32, decoder),
            .minor = try readVarUIntAs(u32, decoder),
            .revision = try readVarUIntAs(u32, decoder),
        };
        if (Feature.timezone.enabled(client_revision)) {
            hello.timezone = try decoder.readString();
        }
        if (Feature.display_name.enabled(client_revision)) {
            hello.display_name = try decoder.readString();
        }
        if (Feature.version_patch.enabled(client_revision)) {
            hello.patch = try readVarUIntAs(u32, decoder);
        }
        return hello;
    }

    pub fn decodePacket(decoder: *Decoder, client_revision: u32) !ServerHello {
        const code = try ServerCode.decode(decoder);
        if (code != .hello) return error.UnexpectedPacket;
        return decodePayload(decoder, client_revision);
    }

    pub fn decodePayloadFromStream(reader: *StreamReader, allocator: std.mem.Allocator, client_revision: u32) !ServerHello {
        var hello = ServerHello{
            .name = try reader.readStringAlloc(allocator),
            .major = try castVarUInt(u32, try reader.readVarUInt()),
            .minor = try castVarUInt(u32, try reader.readVarUInt()),
            .revision = try castVarUInt(u32, try reader.readVarUInt()),
        };
        if (Feature.timezone.enabled(client_revision)) {
            hello.timezone = try reader.readStringAlloc(allocator);
        }
        if (Feature.display_name.enabled(client_revision)) {
            hello.display_name = try reader.readStringAlloc(allocator);
        }
        if (Feature.version_patch.enabled(client_revision)) {
            hello.patch = try castVarUInt(u32, try reader.readVarUInt());
        }
        return hello;
    }

    pub fn decodePacketFromStream(reader: *StreamReader, allocator: std.mem.Allocator, client_revision: u32) !ServerHello {
        const code = try readServerCodeFromStream(reader);
        if (code != .hello) return error.UnexpectedPacket;
        return decodePayloadFromStream(reader, allocator, client_revision);
    }
};

pub const Exception = struct {
    code: i32,
    name: []const u8,
    message: []const u8,
    stack: []const u8,
    nested: bool,

    pub fn encodeAware(self: Exception, encoder: *Encoder) !void {
        try encoder.putInt32LE(self.code);
        try encoder.putString(self.name);
        try encoder.putString(self.message);
        try encoder.putString(self.stack);
        try encoder.putBool(self.nested);
    }

    pub fn encodePacket(self: Exception, encoder: *Encoder) !void {
        try ServerCode.exception.encode(encoder);
        try self.encodeAware(encoder);
    }

    pub fn decodePayload(decoder: *Decoder) !Exception {
        return .{
            .code = try decoder.readInt32LE(),
            .name = try decoder.readString(),
            .message = try decoder.readString(),
            .stack = try decoder.readString(),
            .nested = try decoder.readBool(),
        };
    }

    pub fn decodePayloadFromStream(reader: *StreamReader, allocator: std.mem.Allocator) !Exception {
        return .{
            .code = try reader.readInt32LE(),
            .name = try reader.readStringAlloc(allocator),
            .message = try reader.readStringAlloc(allocator),
            .stack = try reader.readStringAlloc(allocator),
            .nested = try reader.readBool(),
        };
    }
};

pub const ExceptionChain = struct {
    items: []Exception,

    pub fn top(self: ExceptionChain) Exception {
        return self.items[0];
    }

    pub fn decodeFromStream(reader: *StreamReader, allocator: std.mem.Allocator) !ExceptionChain {
        var exceptions = std.ArrayList(Exception).init(allocator);
        defer exceptions.deinit();

        while (true) {
            const item = try Exception.decodePayloadFromStream(reader, allocator);
            try exceptions.append(item);
            if (!item.nested) break;
        }

        return .{ .items = try exceptions.toOwnedSlice() };
    }
};

pub const Progress = struct {
    rows: u64 = 0,
    bytes: u64 = 0,
    total_rows: u64 = 0,
    wrote_rows: u64 = 0,
    wrote_bytes: u64 = 0,
    elapsed_ns: u64 = 0,

    pub fn encodeAware(self: Progress, encoder: *Encoder, revision: u32) !void {
        try encoder.putVarUInt(self.rows);
        try encoder.putVarUInt(self.bytes);
        try encoder.putVarUInt(self.total_rows);
        if (Feature.client_write_info.enabled(revision)) {
            try encoder.putVarUInt(self.wrote_rows);
            try encoder.putVarUInt(self.wrote_bytes);
        }
        if (Feature.server_query_time_in_progress.enabled(revision)) {
            try encoder.putVarUInt(self.elapsed_ns);
        }
    }

    pub fn encodePacket(self: Progress, encoder: *Encoder, revision: u32) !void {
        try ServerCode.progress.encode(encoder);
        try self.encodeAware(encoder, revision);
    }

    pub fn decodePayload(decoder: *Decoder, revision: u32) !Progress {
        var progress = Progress{
            .rows = try decoder.readVarUInt(),
            .bytes = try decoder.readVarUInt(),
            .total_rows = try decoder.readVarUInt(),
        };
        if (Feature.client_write_info.enabled(revision)) {
            progress.wrote_rows = try decoder.readVarUInt();
            progress.wrote_bytes = try decoder.readVarUInt();
        }
        if (Feature.server_query_time_in_progress.enabled(revision)) {
            progress.elapsed_ns = try decoder.readVarUInt();
        }
        return progress;
    }

    pub fn decodePayloadFromStream(reader: *StreamReader, revision: u32) !Progress {
        var progress = Progress{
            .rows = try reader.readVarUInt(),
            .bytes = try reader.readVarUInt(),
            .total_rows = try reader.readVarUInt(),
        };
        if (Feature.client_write_info.enabled(revision)) {
            progress.wrote_rows = try reader.readVarUInt();
            progress.wrote_bytes = try reader.readVarUInt();
        }
        if (Feature.server_query_time_in_progress.enabled(revision)) {
            progress.elapsed_ns = try reader.readVarUInt();
        }
        return progress;
    }
};

pub const Profile = struct {
    rows: u64 = 0,
    blocks: u64 = 0,
    bytes: u64 = 0,
    applied_limit: bool = false,
    rows_before_limit: u64 = 0,
    calculated_rows_before_limit: bool = false,

    pub fn encodePacket(self: Profile, encoder: *Encoder) !void {
        try ServerCode.profile.encode(encoder);
        try encoder.putVarUInt(self.rows);
        try encoder.putVarUInt(self.blocks);
        try encoder.putVarUInt(self.bytes);
        try encoder.putBool(self.applied_limit);
        try encoder.putVarUInt(self.rows_before_limit);
        try encoder.putBool(self.calculated_rows_before_limit);
    }

    pub fn decodePayload(decoder: *Decoder) !Profile {
        return .{
            .rows = try decoder.readVarUInt(),
            .blocks = try decoder.readVarUInt(),
            .bytes = try decoder.readVarUInt(),
            .applied_limit = try decoder.readBool(),
            .rows_before_limit = try decoder.readVarUInt(),
            .calculated_rows_before_limit = try decoder.readBool(),
        };
    }

    pub fn decodePayloadFromStream(reader: *StreamReader) !Profile {
        return .{
            .rows = try reader.readVarUInt(),
            .blocks = try reader.readVarUInt(),
            .bytes = try reader.readVarUInt(),
            .applied_limit = try reader.readBool(),
            .rows_before_limit = try reader.readVarUInt(),
            .calculated_rows_before_limit = try reader.readBool(),
        };
    }
};

pub const TableColumns = struct {
    first: []const u8,
    second: []const u8,

    pub fn encodePacket(self: TableColumns, encoder: *Encoder) !void {
        try ServerCode.table_columns.encode(encoder);
        try encoder.putString(self.first);
        try encoder.putString(self.second);
    }

    pub fn decodePayload(decoder: *Decoder) !TableColumns {
        return .{
            .first = try decoder.readString(),
            .second = try decoder.readString(),
        };
    }

    pub fn decodePayloadFromStream(reader: *StreamReader, allocator: std.mem.Allocator) !TableColumns {
        return .{
            .first = try reader.readStringAlloc(allocator),
            .second = try reader.readStringAlloc(allocator),
        };
    }
};

pub const TableStatus = struct {
    is_replicated: bool = false,
    absolute_delay: u32 = 0,
    is_readonly: bool = false,

    pub fn encodeAware(self: TableStatus, encoder: *Encoder, revision: u32) !void {
        try encoder.putBool(self.is_replicated);
        if (self.is_replicated) {
            try encoder.putVarUInt(self.absolute_delay);
            if (Feature.table_read_only_check.enabled(revision)) {
                try encoder.putVarUInt(if (self.is_readonly) 1 else 0);
            }
        }
    }

    pub fn decodeAware(decoder: *Decoder, revision: u32) !TableStatus {
        var status = TableStatus{
            .is_replicated = try decoder.readBool(),
        };
        if (status.is_replicated) {
            status.absolute_delay = try readVarUIntAs(u32, decoder);
            if (Feature.table_read_only_check.enabled(revision)) {
                status.is_readonly = (try decoder.readVarUInt()) != 0;
            }
        }
        return status;
    }

    pub fn decodeAwareFromStream(reader: *StreamReader, revision: u32) !TableStatus {
        var status = TableStatus{
            .is_replicated = try reader.readBool(),
        };
        if (status.is_replicated) {
            status.absolute_delay = try castVarUInt(u32, try reader.readVarUInt());
            if (Feature.table_read_only_check.enabled(revision)) {
                status.is_readonly = (try reader.readVarUInt()) != 0;
            }
        }
        return status;
    }
};

pub const TablesStatusRequest = struct {
    tables: []const QualifiedTableName,

    pub fn encodePayload(self: TablesStatusRequest, encoder: *Encoder, revision: u32) !void {
        if (!Feature.tables_status.enabled(revision)) return error.UnsupportedRevision;
        try encoder.putVarUInt(self.tables.len);
        for (self.tables) |table| {
            try encoder.putString(table.database);
            try encoder.putString(table.table);
        }
    }

    pub fn encodePacket(self: TablesStatusRequest, encoder: *Encoder, revision: u32) !void {
        try ClientCode.tables_status_request.encode(encoder);
        try self.encodePayload(encoder, revision);
    }

    pub fn decodePayload(decoder: *Decoder, allocator: std.mem.Allocator, revision: u32) !TablesStatusRequest {
        if (!Feature.tables_status.enabled(revision)) return error.UnsupportedRevision;
        const count = try readVarUIntAs(usize, decoder);
        const tables = try allocator.alloc(QualifiedTableName, count);
        errdefer allocator.free(tables);
        for (tables) |*table| {
            table.* = .{
                .database = try decoder.readString(),
                .table = try decoder.readString(),
            };
        }
        return .{ .tables = tables };
    }

    pub fn decodePacket(decoder: *Decoder, allocator: std.mem.Allocator, revision: u32) !TablesStatusRequest {
        const code = try ClientCode.decode(decoder);
        if (code != .tables_status_request) return error.UnexpectedPacket;
        return decodePayload(decoder, allocator, revision);
    }

    pub fn decodePayloadFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32) !TablesStatusRequest {
        if (!Feature.tables_status.enabled(revision)) return error.UnsupportedRevision;
        const count = try castVarUInt(usize, try reader.readVarUInt());
        const tables = try allocator.alloc(QualifiedTableName, count);
        errdefer allocator.free(tables);
        for (tables) |*table| {
            table.* = .{
                .database = try reader.readStringAlloc(allocator),
                .table = try reader.readStringAlloc(allocator),
            };
        }
        return .{ .tables = tables };
    }

    pub fn decodePacketFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32) !TablesStatusRequest {
        const code = try readClientCodeFromStream(reader);
        if (code != .tables_status_request) return error.UnexpectedPacket;
        return decodePayloadFromStream(reader, allocator, revision);
    }
};

pub const TableStatusEntry = struct {
    table: QualifiedTableName,
    status: TableStatus,
};

pub const TablesStatusResponse = struct {
    entries: []const TableStatusEntry,

    pub fn encodePayload(self: TablesStatusResponse, encoder: *Encoder, revision: u32) !void {
        if (!Feature.tables_status.enabled(revision)) return error.UnsupportedRevision;
        try encoder.putVarUInt(self.entries.len);
        for (self.entries) |entry| {
            try encoder.putString(entry.table.database);
            try encoder.putString(entry.table.table);
            try entry.status.encodeAware(encoder, revision);
        }
    }

    pub fn encodePacket(self: TablesStatusResponse, encoder: *Encoder, revision: u32) !void {
        try ServerCode.tables_status.encode(encoder);
        try self.encodePayload(encoder, revision);
    }

    pub fn decodePayload(decoder: *Decoder, allocator: std.mem.Allocator, revision: u32) !TablesStatusResponse {
        if (!Feature.tables_status.enabled(revision)) return error.UnsupportedRevision;
        const count = try readVarUIntAs(usize, decoder);
        const entries = try allocator.alloc(TableStatusEntry, count);
        errdefer allocator.free(entries);
        for (entries) |*entry| {
            entry.* = .{
                .table = .{
                    .database = try decoder.readString(),
                    .table = try decoder.readString(),
                },
                .status = try TableStatus.decodeAware(decoder, revision),
            };
        }
        return .{ .entries = entries };
    }

    pub fn decodePayloadFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32) !TablesStatusResponse {
        if (!Feature.tables_status.enabled(revision)) return error.UnsupportedRevision;
        const count = try castVarUInt(usize, try reader.readVarUInt());
        const entries = try allocator.alloc(TableStatusEntry, count);
        errdefer allocator.free(entries);
        for (entries) |*entry| {
            entry.* = .{
                .table = .{
                    .database = try reader.readStringAlloc(allocator),
                    .table = try reader.readStringAlloc(allocator),
                },
                .status = try TableStatus.decodeAwareFromStream(reader, revision),
            };
        }
        return .{ .entries = entries };
    }
};

pub const PartUUIDs = struct {
    uuids: []const UUID,

    pub fn encodePayload(self: PartUUIDs, encoder: *Encoder) !void {
        try encoder.putVarUInt(self.uuids.len);
        for (self.uuids) |uuid| {
            try encoder.putRaw(&uuid);
        }
    }

    pub fn encodePacket(self: PartUUIDs, encoder: *Encoder) !void {
        try ServerCode.part_uuids.encode(encoder);
        try self.encodePayload(encoder);
    }

    pub fn decodePayload(decoder: *Decoder, allocator: std.mem.Allocator) !PartUUIDs {
        const count = try readVarUIntAs(usize, decoder);
        const uuids = try allocator.alloc(UUID, count);
        errdefer allocator.free(uuids);
        for (uuids) |*uuid| {
            uuid.* = try readUUID(decoder);
        }
        return .{ .uuids = uuids };
    }

    pub fn decodePayloadFromStream(reader: *StreamReader, allocator: std.mem.Allocator) !PartUUIDs {
        const count = try castVarUInt(usize, try reader.readVarUInt());
        const uuids = try allocator.alloc(UUID, count);
        errdefer allocator.free(uuids);
        for (uuids) |*uuid| {
            uuid.* = try readUUIDFromStream(reader);
        }
        return .{ .uuids = uuids };
    }
};

pub const ReadTaskRequest = struct {
    pub fn encodePacket(_: ReadTaskRequest, encoder: *Encoder) !void {
        try ServerCode.read_task_request.encode(encoder);
    }

    pub fn decodePayload(_: *Decoder) ReadTaskRequest {
        return .{};
    }

    pub fn decodePayloadFromStream(_: *StreamReader) ReadTaskRequest {
        return .{};
    }
};

pub const SSHChallenge = struct {
    challenge: []const u8,

    pub fn encodePacket(self: SSHChallenge, encoder: *Encoder) !void {
        try ServerCode.ssh_challenge.encode(encoder);
        try encoder.putString(self.challenge);
    }

    pub fn decodePayload(decoder: *Decoder) !SSHChallenge {
        return .{ .challenge = try decoder.readString() };
    }

    pub fn decodePayloadFromStream(reader: *StreamReader, allocator: std.mem.Allocator) !SSHChallenge {
        return .{ .challenge = try reader.readStringAlloc(allocator) };
    }
};

pub const SSHChallengeRequest = struct {
    pub fn encodePacket(_: SSHChallengeRequest, encoder: *Encoder) !void {
        try ClientCode.ssh_challenge_request.encode(encoder);
    }

    pub fn decodePacket(decoder: *Decoder) !SSHChallengeRequest {
        const code = try ClientCode.decode(decoder);
        if (code != .ssh_challenge_request) return error.UnexpectedPacket;
        return .{};
    }

    pub fn decodePacketFromStream(reader: *StreamReader) !SSHChallengeRequest {
        const code = try readClientCodeFromStream(reader);
        if (code != .ssh_challenge_request) return error.UnexpectedPacket;
        return .{};
    }
};

pub const SSHChallengeResponse = struct {
    signature: []const u8,

    pub fn encodePacket(self: SSHChallengeResponse, encoder: *Encoder) !void {
        try ClientCode.ssh_challenge_response.encode(encoder);
        try encoder.putString(self.signature);
    }

    pub fn decodePayload(decoder: *Decoder) !SSHChallengeResponse {
        return .{ .signature = try decoder.readString() };
    }

    pub fn decodePacket(decoder: *Decoder) !SSHChallengeResponse {
        const code = try ClientCode.decode(decoder);
        if (code != .ssh_challenge_response) return error.UnexpectedPacket;
        return decodePayload(decoder);
    }

    pub fn decodePayloadFromStream(reader: *StreamReader, allocator: std.mem.Allocator) !SSHChallengeResponse {
        return .{ .signature = try reader.readStringAlloc(allocator) };
    }

    pub fn decodePacketFromStream(reader: *StreamReader, allocator: std.mem.Allocator) !SSHChallengeResponse {
        const code = try readClientCodeFromStream(reader);
        if (code != .ssh_challenge_response) return error.UnexpectedPacket;
        return decodePayloadFromStream(reader, allocator);
    }
};

pub const ClientData = struct {
    table_name: []const u8 = "",

    pub fn encodeAware(self: ClientData, encoder: *Encoder, revision: u32) !void {
        if (Feature.temp_tables.enabled(revision)) {
            try encoder.putString(self.table_name);
        }
    }

    pub fn decodeAware(decoder: *Decoder, revision: u32) !ClientData {
        if (Feature.temp_tables.enabled(revision)) {
            return .{ .table_name = try decoder.readString() };
        }
        return .{};
    }

    pub fn decodeAwareFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32) !ClientData {
        if (Feature.temp_tables.enabled(revision)) {
            return .{ .table_name = try reader.readStringAlloc(allocator) };
        }
        return .{};
    }
};

pub const TraceContext = struct {
    trace_id: [16]u8,
    span_id: [8]u8,
    trace_state: []const u8 = "",
    trace_flags: u8 = 0,
};

pub const ClientInfo = struct {
    protocol_version: u32,
    major: u32,
    minor: u32,
    patch: u32 = 0,
    interface: Interface = .tcp,
    query_kind: ClientQueryKind = .initial,
    initial_user: []const u8 = "",
    initial_query_id: []const u8 = "",
    initial_address: []const u8 = "",
    initial_time: i64 = 0,
    os_user: []const u8 = "",
    client_hostname: []const u8 = "",
    client_name: []const u8 = default_client_name,
    trace: ?TraceContext = null,
    quota_key: []const u8 = "",
    distributed_depth: u32 = 0,
    collaborate_with_initiator: bool = false,
    count_participating_replicas: u32 = 0,
    number_of_current_replica: u32 = 0,

    pub fn encodeAware(self: ClientInfo, encoder: *Encoder, revision: u32) !void {
        try encoder.putByte(@intFromEnum(self.query_kind));
        try encoder.putString(self.initial_user);
        try encoder.putString(self.initial_query_id);
        try encoder.putString(self.initial_address);
        if (Feature.query_start_time.enabled(revision)) {
            try encoder.putInt64LE(self.initial_time);
        }

        try encoder.putByte(@intFromEnum(self.interface));
        try encoder.putString(self.os_user);
        try encoder.putString(self.client_hostname);
        try encoder.putString(self.client_name);
        try encoder.putVarUInt(self.major);
        try encoder.putVarUInt(self.minor);
        try encoder.putVarUInt(self.protocol_version);

        if (Feature.quota_key_in_client_info.enabled(revision)) {
            try encoder.putString(self.quota_key);
        }
        if (Feature.distributed_depth.enabled(revision)) {
            try encoder.putVarUInt(self.distributed_depth);
        }
        if (Feature.version_patch.enabled(revision) and self.interface == .tcp) {
            try encoder.putVarUInt(self.patch);
        }
        if (Feature.open_telemetry.enabled(revision)) {
            if (self.trace) |trace| {
                try encoder.putBool(true);
                try encoder.putRaw(&swap64Chunks(16, trace.trace_id));
                try encoder.putRaw(&swap64Chunks(8, trace.span_id));
                try encoder.putString(trace.trace_state);
                try encoder.putByte(trace.trace_flags);
            } else {
                try encoder.putBool(false);
            }
        }
        if (Feature.parallel_replicas.enabled(revision)) {
            try encoder.putVarUInt(if (self.collaborate_with_initiator) 1 else 0);
            try encoder.putVarUInt(self.count_participating_replicas);
            try encoder.putVarUInt(self.number_of_current_replica);
        }
    }

    pub fn decodeAware(decoder: *Decoder, revision: u32) !ClientInfo {
        const query_kind_raw = try decoder.readByte();
        const query_kind = std.meta.intToEnum(ClientQueryKind, query_kind_raw) catch return error.InvalidClientQueryKind;

        var info = ClientInfo{
            .query_kind = query_kind,
            .initial_user = try decoder.readString(),
            .initial_query_id = try decoder.readString(),
            .initial_address = try decoder.readString(),
            .protocol_version = 0,
            .major = 0,
            .minor = 0,
        };
        if (Feature.query_start_time.enabled(revision)) {
            info.initial_time = try decoder.readInt64LE();
        }

        const interface_raw = try decoder.readByte();
        info.interface = std.meta.intToEnum(Interface, interface_raw) catch return error.InvalidInterface;
        if (info.interface != .tcp) return error.UnsupportedInterface;

        info.os_user = try decoder.readString();
        info.client_hostname = try decoder.readString();
        info.client_name = try decoder.readString();
        info.major = try readVarUIntAs(u32, decoder);
        info.minor = try readVarUIntAs(u32, decoder);
        info.protocol_version = try readVarUIntAs(u32, decoder);

        if (Feature.quota_key_in_client_info.enabled(revision)) {
            info.quota_key = try decoder.readString();
        }
        if (Feature.distributed_depth.enabled(revision)) {
            info.distributed_depth = try readVarUIntAs(u32, decoder);
        }
        if (Feature.version_patch.enabled(revision) and info.interface == .tcp) {
            info.patch = try readVarUIntAs(u32, decoder);
        }
        if (Feature.open_telemetry.enabled(revision)) {
            if (try decoder.readBool()) {
                const raw_trace_id = try decoder.readSlice(16);
                const raw_span_id = try decoder.readSlice(8);

                var trace_id: [16]u8 = undefined;
                var span_id: [8]u8 = undefined;
                std.mem.copyForwards(u8, &trace_id, raw_trace_id);
                std.mem.copyForwards(u8, &span_id, raw_span_id);

                info.trace = .{
                    .trace_id = swap64Chunks(16, trace_id),
                    .span_id = swap64Chunks(8, span_id),
                    .trace_state = try decoder.readString(),
                    .trace_flags = try decoder.readByte(),
                };
            }
        }
        if (Feature.parallel_replicas.enabled(revision)) {
            info.collaborate_with_initiator = (try readVarUIntAs(u32, decoder)) == 1;
            info.count_participating_replicas = try readVarUIntAs(u32, decoder);
            info.number_of_current_replica = try readVarUIntAs(u32, decoder);
        }
        return info;
    }

    pub fn decodeAwareFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32) !ClientInfo {
        const query_kind_raw = try reader.readByte();
        const query_kind = std.meta.intToEnum(ClientQueryKind, query_kind_raw) catch return error.InvalidClientQueryKind;

        var info = ClientInfo{
            .query_kind = query_kind,
            .initial_user = try reader.readStringAlloc(allocator),
            .initial_query_id = try reader.readStringAlloc(allocator),
            .initial_address = try reader.readStringAlloc(allocator),
            .protocol_version = 0,
            .major = 0,
            .minor = 0,
        };
        if (Feature.query_start_time.enabled(revision)) {
            info.initial_time = try reader.readInt64LE();
        }

        const interface_raw = try reader.readByte();
        info.interface = std.meta.intToEnum(Interface, interface_raw) catch return error.InvalidInterface;
        if (info.interface != .tcp) return error.UnsupportedInterface;

        info.os_user = try reader.readStringAlloc(allocator);
        info.client_hostname = try reader.readStringAlloc(allocator);
        info.client_name = try reader.readStringAlloc(allocator);
        info.major = try castVarUInt(u32, try reader.readVarUInt());
        info.minor = try castVarUInt(u32, try reader.readVarUInt());
        info.protocol_version = try castVarUInt(u32, try reader.readVarUInt());

        if (Feature.quota_key_in_client_info.enabled(revision)) {
            info.quota_key = try reader.readStringAlloc(allocator);
        }
        if (Feature.distributed_depth.enabled(revision)) {
            info.distributed_depth = try castVarUInt(u32, try reader.readVarUInt());
        }
        if (Feature.version_patch.enabled(revision) and info.interface == .tcp) {
            info.patch = try castVarUInt(u32, try reader.readVarUInt());
        }
        if (Feature.open_telemetry.enabled(revision)) {
            if (try reader.readBool()) {
                var trace_id: [16]u8 = undefined;
                var span_id: [8]u8 = undefined;
                try reader.readExact(&trace_id);
                try reader.readExact(&span_id);
                info.trace = .{
                    .trace_id = swap64Chunks(16, trace_id),
                    .span_id = swap64Chunks(8, span_id),
                    .trace_state = try reader.readStringAlloc(allocator),
                    .trace_flags = try reader.readByte(),
                };
            }
        }
        if (Feature.parallel_replicas.enabled(revision)) {
            info.collaborate_with_initiator = (try castVarUInt(u32, try reader.readVarUInt())) == 1;
            info.count_participating_replicas = try castVarUInt(u32, try reader.readVarUInt());
            info.number_of_current_replica = try castVarUInt(u32, try reader.readVarUInt());
        }
        return info;
    }
};

pub const Setting = struct {
    key: []const u8,
    value: []const u8,
    important: bool = false,
    custom: bool = false,
    obsolete: bool = false,

    pub fn encode(self: Setting, encoder: *Encoder) !void {
        try encoder.putString(self.key);
        var flags: u64 = 0;
        if (self.important) flags |= 0x01;
        if (self.custom) flags |= 0x02;
        if (self.obsolete) flags |= 0x04;
        try encoder.putVarUInt(flags);
        try encoder.putString(self.value);
    }

    pub fn decode(decoder: *Decoder) !Setting {
        const key = try decoder.readString();
        if (key.len == 0) {
            return .{ .key = "", .value = "" };
        }
        const flags = try decoder.readVarUInt();
        return .{
            .key = key,
            .value = try decoder.readString(),
            .important = (flags & 0x01) != 0,
            .custom = (flags & 0x02) != 0,
            .obsolete = (flags & 0x04) != 0,
        };
    }

    pub fn decodeFromStream(reader: *StreamReader, allocator: std.mem.Allocator) !Setting {
        const key = try reader.readStringAlloc(allocator);
        if (key.len == 0) {
            return .{ .key = "", .value = "" };
        }
        const flags = try reader.readVarUInt();
        return .{
            .key = key,
            .value = try reader.readStringAlloc(allocator),
            .important = (flags & 0x01) != 0,
            .custom = (flags & 0x02) != 0,
            .obsolete = (flags & 0x04) != 0,
        };
    }
};

pub const Parameter = struct {
    key: []const u8,
    value: []const u8,

    pub fn encode(self: Parameter, encoder: *Encoder) !void {
        try (Setting{
            .key = self.key,
            .value = self.value,
            .custom = true,
        }).encode(encoder);
    }

    pub fn decode(decoder: *Decoder) !Parameter {
        const setting = try Setting.decode(decoder);
        return .{
            .key = setting.key,
            .value = setting.value,
        };
    }

    pub fn decodeFromStream(reader: *StreamReader, allocator: std.mem.Allocator) !Parameter {
        const setting = try Setting.decodeFromStream(reader, allocator);
        return .{
            .key = setting.key,
            .value = setting.value,
        };
    }
};

pub const QueryContext = struct {
    user_data: ?*anyopaque = null,
    is_canceled: ?*const fn (?*anyopaque) bool = null,

    pub fn canceled(self: QueryContext) bool {
        return if (self.is_canceled) |f| f(self.user_data) else false;
    }

    pub fn check(self: QueryContext) !void {
        if (self.canceled()) return error.Canceled;
    }
};

pub const LogLevel = enum {
    debug,
    info,
    warn,
    err,
};

pub const QueryMetrics = struct {
    blocks_sent: u64 = 0,
    blocks_received: u64 = 0,
    totals_blocks_received: u64 = 0,
    extremes_blocks_received: u64 = 0,
    rows_received: u64 = 0,
    columns_received: u64 = 0,
    progress_rows: u64 = 0,
    progress_bytes: u64 = 0,
    progress_total_rows: u64 = 0,
    wrote_rows: u64 = 0,
    wrote_bytes: u64 = 0,
};

pub const ConnectStartInfo = struct {
    host: []const u8,
    port: u16,
    tls_enabled: bool,
};

pub const ConnectFinishInfo = struct {
    host: []const u8,
    port: u16,
    tls_enabled: bool,
    protocol_version: u32,
    err: ?anyerror = null,
};

pub const QueryStartInfo = struct {
    id: []const u8,
    body: []const u8,
    compression: Compression,
    database: []const u8,
    user: []const u8,
};

pub const QueryFinishInfo = struct {
    id: []const u8,
    metrics: QueryMetrics,
    err: ?anyerror = null,
};

pub const ConnectObserveEvent = union(enum) {
    start: ConnectStartInfo,
    finish: ConnectFinishInfo,
};

pub const QueryObserveEvent = union(enum) {
    start: QueryStartInfo,
    progress: Progress,
    profile: Profile,
    exception: ExceptionChain,
    finish: QueryFinishInfo,
};

pub const ObserveLogFn = *const fn (level: LogLevel, scope: []const u8, message: []const u8, user_data: ?*anyopaque) void;
pub const ObserveConnectFn = *const fn (event: ConnectObserveEvent, user_data: ?*anyopaque) void;
pub const ObserveQueryFn = *const fn (event: QueryObserveEvent, user_data: ?*anyopaque) void;

pub const Observer = struct {
    user_data: ?*anyopaque = null,
    on_log: ?ObserveLogFn = null,
    on_connect: ?ObserveConnectFn = null,
    on_query: ?ObserveQueryFn = null,

    pub fn enabled(self: Observer) bool {
        return self.on_log != null or self.on_connect != null or self.on_query != null;
    }
};

pub const OnInputFn = *const fn (ctx: QueryContext, query: *Query) anyerror!void;
pub const OnResultFn = *const fn (ctx: QueryContext, block: *const DecodedBlock) anyerror!void;
pub const OnProgressFn = *const fn (ctx: QueryContext, progress: Progress) anyerror!void;
pub const OnProfileFn = *const fn (ctx: QueryContext, profile: Profile) anyerror!void;
pub const OnDataPacketFn = *const fn (ctx: QueryContext, packet: *const DecodedDataPacket) anyerror!void;
pub const OnTableColumnsFn = *const fn (ctx: QueryContext, table_columns: TableColumns) anyerror!void;
pub const OnLogsFn = *const fn (ctx: QueryContext, logs: []const ServerLog) anyerror!void;
pub const OnLogFn = *const fn (ctx: QueryContext, log: ServerLog) anyerror!void;
pub const OnProfileEventsFn = *const fn (ctx: QueryContext, events: []const ProfileEvent) anyerror!void;
pub const OnProfileEventFn = *const fn (ctx: QueryContext, event: ProfileEvent) anyerror!void;

pub const ProfileEventType = enum(i8) {
    increment = 1,
    gauge = 2,
};

pub const ProfileEvent = struct {
    event_type: ProfileEventType,
    name: []const u8,
    value: i64,
    host: []const u8,
    time_seconds: u32,
    thread_id: u64,
};

pub const ServerLog = struct {
    query_id: []const u8,
    source: []const u8,
    text: []const u8,
    host: []const u8,
    time_seconds: u32,
    time_microseconds: u32,
    thread_id: u64,
    priority: i8,
};

pub const BlockBuffer = struct {
    allocator: std.mem.Allocator,
    blocks: std.ArrayList(DecodedBlock),

    pub fn init(allocator: std.mem.Allocator) BlockBuffer {
        return .{
            .allocator = allocator,
            .blocks = std.ArrayList(DecodedBlock).init(allocator),
        };
    }

    pub fn deinit(self: *BlockBuffer) void {
        for (self.blocks.items) |*block| {
            block.deinit(self.allocator);
        }
        self.blocks.deinit();
    }

    pub fn appendClone(self: *BlockBuffer, block: DecodedBlock) !void {
        try self.blocks.append(try block.cloneOwned(self.allocator));
    }

    pub fn clear(self: *BlockBuffer) void {
        for (self.blocks.items) |*block| {
            block.deinit(self.allocator);
        }
        self.blocks.clearRetainingCapacity();
    }
};

pub const OwnedByteSlices = struct {
    allocator: std.mem.Allocator,
    items: std.ArrayList([]u8),

    pub fn init(allocator: std.mem.Allocator) OwnedByteSlices {
        return .{
            .allocator = allocator,
            .items = std.ArrayList([]u8).init(allocator),
        };
    }

    pub fn appendDup(self: *OwnedByteSlices, value: []const u8) !void {
        try self.items.append(try self.allocator.dupe(u8, value));
    }

    pub fn clear(self: *OwnedByteSlices) void {
        for (self.items.items) |value| {
            self.allocator.free(value);
        }
        self.items.clearRetainingCapacity();
    }

    pub fn deinit(self: *OwnedByteSlices) void {
        self.clear();
        self.items.deinit();
    }
};

pub const OwnedFixedValue = struct {
    type_name: []u8,
    bytes: []u8,
};

pub const OwnedMapEntry = struct {
    key: OwnedValue,
    value: OwnedValue,
};

pub const OwnedTupleFieldValue = struct {
    name: []u8,
    value: OwnedValue,
};

pub const OwnedValue = union(enum) {
    null,
    string: []u8,
    bytes: []u8,
    int8: i8,
    int64: i64,
    uint64: u64,
    bool: bool,
    fixed: OwnedFixedValue,
    array: []OwnedValue,
    map: []OwnedMapEntry,
    tuple: []OwnedTupleFieldValue,

    pub fn deinit(self: *OwnedValue, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .null, .int8, .int64, .uint64, .bool => {},
            .string => |value| allocator.free(value),
            .bytes => |value| allocator.free(value),
            .fixed => |value| {
                allocator.free(value.type_name);
                allocator.free(value.bytes);
            },
            .array => |value| {
                for (value) |*item| {
                    item.deinit(allocator);
                }
                allocator.free(value);
            },
            .map => |value| {
                for (value) |*item| {
                    item.key.deinit(allocator);
                    item.value.deinit(allocator);
                }
                allocator.free(value);
            },
            .tuple => |value| {
                for (value) |*item| {
                    allocator.free(item.name);
                    item.value.deinit(allocator);
                }
                allocator.free(value);
            },
        }
    }
};

pub const OwnedValues = struct {
    allocator: std.mem.Allocator,
    items: std.ArrayList(OwnedValue),

    pub fn init(allocator: std.mem.Allocator) OwnedValues {
        return .{
            .allocator = allocator,
            .items = std.ArrayList(OwnedValue).init(allocator),
        };
    }

    pub fn append(self: *OwnedValues, value: OwnedValue) !void {
        try self.items.append(value);
    }

    pub fn clear(self: *OwnedValues) void {
        for (self.items.items) |*value| {
            value.deinit(self.allocator);
        }
        self.items.clearRetainingCapacity();
    }

    pub fn deinit(self: *OwnedValues) void {
        self.clear();
        self.items.deinit();
    }
};

pub const ResultSink = union(enum) {
    strings: *OwnedByteSlices,
    bytes: *OwnedByteSlices,
    int8s: *std.ArrayList(i8),
    int64s: *std.ArrayList(i64),
    uint64s: *std.ArrayList(u64),
    bools: *std.ArrayList(bool),
    values: *OwnedValues,
};

pub const ResultBindingColumn = struct {
    name: []const u8 = "",
    index: ?usize = null,
    sink: ResultSink,
    resolved_index: ?usize = null,
};

pub const ResultBinding = struct {
    allocator: std.mem.Allocator,
    columns: []ResultBindingColumn,

    pub fn init(allocator: std.mem.Allocator, columns: []ResultBindingColumn) ResultBinding {
        return .{
            .allocator = allocator,
            .columns = columns,
        };
    }

    pub fn reset(self: *ResultBinding) void {
        for (self.columns) |*column| {
            column.resolved_index = null;
        }
    }

    pub fn bindBlock(self: *ResultBinding, block: *const DecodedBlock) !void {
        for (self.columns) |*binding| {
            const resolved_index = binding.resolved_index orelse blk: {
                const idx = try resolveResultBindingColumn(block, binding.*);
                binding.resolved_index = idx;
                break :blk idx;
            };
            try appendResultSink(self.allocator, &binding.sink, block.columns[resolved_index]);
        }
    }
};

pub const Query = struct {
    id: []const u8 = "",
    query_id: []const u8 = "",
    body: []const u8,
    quota_key: []const u8 = "",
    initial_user: []const u8 = "",
    secret: []const u8 = "",
    stage: Stage = .complete,
    compression: Compression = .disabled,
    info: ClientInfo,
    settings: []Setting = &.{},
    parameters: []Parameter = &.{},
    input: []const Column = &.{},
    on_input: ?OnInputFn = null,
    result: ?*BlockBuffer = null,
    result_binding: ?*ResultBinding = null,
    on_result: ?OnResultFn = null,
    totals: ?*BlockBuffer = null,
    totals_binding: ?*ResultBinding = null,
    on_totals: ?OnResultFn = null,
    extremes: ?*BlockBuffer = null,
    extremes_binding: ?*ResultBinding = null,
    on_extremes: ?OnResultFn = null,
    on_progress: ?OnProgressFn = null,
    on_profile: ?OnProfileFn = null,
    on_profile_events: ?OnDataPacketFn = null,
    on_profile_events_batch: ?OnProfileEventsFn = null,
    on_profile_event: ?OnProfileEventFn = null,
    on_logs: ?OnDataPacketFn = null,
    on_logs_batch: ?OnLogsFn = null,
    on_log: ?OnLogFn = null,
    external_data: []const Column = &.{},
    external_table: []const u8 = "",
    on_table_columns: ?OnTableColumnsFn = null,
    metrics: ?*QueryMetrics = null,
    observer: ?Observer = null,

    pub fn deinit(self: *Query, allocator: std.mem.Allocator) void {
        if (self.settings.len > 0) allocator.free(self.settings);
        if (self.parameters.len > 0) allocator.free(self.parameters);
        self.settings = &.{};
        self.parameters = &.{};
    }

    pub fn encodePacket(self: Query, encoder: *Encoder, revision: u32) !void {
        try ClientCode.query.encode(encoder);
        try encoder.putString(self.id);
        if (Feature.client_write_info.enabled(revision)) {
            try self.info.encodeAware(encoder, revision);
        }
        if (!Feature.settings_serialized_as_strings.enabled(revision)) {
            return error.UnsupportedRevision;
        }
        for (self.settings) |setting| {
            try setting.encode(encoder);
        }
        try encoder.putString("");
        if (Feature.inter_server_secret.enabled(revision)) {
            try encoder.putString(self.secret);
        }
        try self.stage.encode(encoder);
        try self.compression.encode(encoder);
        try encoder.putString(self.body);
        if (Feature.parameters.enabled(revision)) {
            for (self.parameters) |parameter| {
                try parameter.encode(encoder);
            }
            try encoder.putString("");
        }
    }

    pub fn decodePayload(decoder: *Decoder, allocator: std.mem.Allocator, revision: u32) !Query {
        var query = Query{
            .id = try decoder.readString(),
            .body = "",
            .info = undefined,
        };
        errdefer query.deinit(allocator);

        if (Feature.client_write_info.enabled(revision)) {
            query.info = try ClientInfo.decodeAware(decoder, revision);
        } else {
            query.info = .{
                .protocol_version = revision,
                .major = 0,
                .minor = 0,
            };
        }

        if (!Feature.settings_serialized_as_strings.enabled(revision)) {
            return error.UnsupportedRevision;
        }
        var settings = std.ArrayList(Setting).init(allocator);
        defer settings.deinit();
        while (true) {
            const setting = try Setting.decode(decoder);
            if (setting.key.len == 0) break;
            try settings.append(setting);
        }
        query.settings = try settings.toOwnedSlice();

        if (Feature.inter_server_secret.enabled(revision)) {
            query.secret = try decoder.readString();
        }
        query.stage = try Stage.decode(decoder);
        query.compression = try Compression.decode(decoder);
        query.body = try decoder.readString();

        if (Feature.parameters.enabled(revision)) {
            var parameters = std.ArrayList(Parameter).init(allocator);
            defer parameters.deinit();
            while (true) {
                const parameter = try Parameter.decode(decoder);
                if (parameter.key.len == 0) break;
                try parameters.append(parameter);
            }
            query.parameters = try parameters.toOwnedSlice();
        }

        return query;
    }

    pub fn decodePacket(decoder: *Decoder, allocator: std.mem.Allocator, revision: u32) !Query {
        const code = try ClientCode.decode(decoder);
        if (code != .query) return error.UnexpectedPacket;
        return decodePayload(decoder, allocator, revision);
    }

    pub fn decodePayloadFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32) !Query {
        var query = Query{
            .id = try reader.readStringAlloc(allocator),
            .body = "",
            .info = undefined,
        };
        errdefer query.deinit(allocator);

        if (Feature.client_write_info.enabled(revision)) {
            query.info = try ClientInfo.decodeAwareFromStream(reader, allocator, revision);
        } else {
            query.info = .{
                .protocol_version = revision,
                .major = 0,
                .minor = 0,
            };
        }

        if (!Feature.settings_serialized_as_strings.enabled(revision)) {
            return error.UnsupportedRevision;
        }

        var settings = std.ArrayList(Setting).init(allocator);
        defer settings.deinit();
        while (true) {
            const setting = try Setting.decodeFromStream(reader, allocator);
            if (setting.key.len == 0) break;
            try settings.append(setting);
        }
        query.settings = try settings.toOwnedSlice();

        if (Feature.inter_server_secret.enabled(revision)) {
            query.secret = try reader.readStringAlloc(allocator);
        }
        query.stage = blk: {
            const raw = try reader.readVarUInt();
            break :blk std.meta.intToEnum(Stage, @as(u8, @intCast(raw))) catch return error.InvalidStage;
        };
        query.compression = blk: {
            const raw = try reader.readVarUInt();
            break :blk std.meta.intToEnum(Compression, @as(u8, @intCast(raw))) catch return error.InvalidCompression;
        };
        query.body = try reader.readStringAlloc(allocator);

        if (Feature.parameters.enabled(revision)) {
            var parameters = std.ArrayList(Parameter).init(allocator);
            defer parameters.deinit();
            while (true) {
                const parameter = try Parameter.decodeFromStream(reader, allocator);
                if (parameter.key.len == 0) break;
                try parameters.append(parameter);
            }
            query.parameters = try parameters.toOwnedSlice();
        }

        return query;
    }

    pub fn decodePacketFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32) !Query {
        const code = try readClientCodeFromStream(reader);
        if (code != .query) return error.UnexpectedPacket;
        return decodePayloadFromStream(reader, allocator, revision);
    }
};

pub const BlockInfo = struct {
    overflows: bool = false,
    bucket_num: i32 = 0,

    pub fn encode(self: BlockInfo, encoder: *Encoder) !void {
        try encoder.putVarUInt(1);
        try encoder.putBool(self.overflows);
        try encoder.putVarUInt(2);
        try encoder.putInt32LE(self.bucket_num);
        try encoder.putVarUInt(0);
    }

    pub fn decode(decoder: *Decoder) !BlockInfo {
        var info = BlockInfo{};
        while (true) {
            const field_id = try decoder.readVarUInt();
            switch (field_id) {
                0 => return info,
                1 => info.overflows = try decoder.readBool(),
                2 => info.bucket_num = try decoder.readInt32LE(),
                else => return error.UnknownBlockInfoField,
            }
        }
    }

    pub fn decodeFromStream(reader: *StreamReader) !BlockInfo {
        var info = BlockInfo{};
        while (true) {
            const field_id = try reader.readVarUInt();
            switch (field_id) {
                0 => return info,
                1 => info.overflows = try reader.readBool(),
                2 => info.bucket_num = try reader.readInt32LE(),
                else => return error.UnknownBlockInfoField,
            }
        }
    }
};

pub const StringColumn = struct {
    name: []const u8,
    values: []const []const u8,
    owns_name: bool = false,
    owns_values: bool = false,
    backing_data: []const u8 = "",
    owns_backing_data: bool = false,
};

pub const VarBytesColumn = struct {
    name: []const u8,
    type_name: []const u8,
    values: []const []const u8,
    owns_name: bool = false,
    owns_type_name: bool = false,
    owns_values: bool = false,
    backing_data: []const u8 = "",
    owns_backing_data: bool = false,
};

pub const FixedBytesColumn = struct {
    name: []const u8,
    type_name: []const u8,
    width: usize,
    data: []const u8,
    rows: usize = 0,
    owns_name: bool = false,
    owns_type_name: bool = false,
    owns_data: bool = false,

    pub fn rowCount(self: FixedBytesColumn) usize {
        if (self.width == 0) return self.rows;
        if (self.data.len == 0) return self.rows;
        return self.data.len / self.width;
    }

    pub fn row(self: FixedBytesColumn, index: usize) []const u8 {
        if (self.width == 0) return "";
        const start = index * self.width;
        return self.data[start .. start + self.width];
    }
};

pub const EncodedColumn = struct {
    name: []const u8,
    type_name: []const u8,
    rows: usize,
    state: []const u8 = "",
    payload: []const u8,
    owns_name: bool = false,
    owns_type_name: bool = false,
    owns_state: bool = false,
    owns_payload: bool = false,
};

pub const Int8Column = struct {
    name: []const u8,
    values: []const i8,
    owns_name: bool = false,
    owns_values: bool = false,
};

pub const Int64Column = struct {
    name: []const u8,
    values: []const i64,
    owns_name: bool = false,
    owns_values: bool = false,
};

pub const UInt64Column = struct {
    name: []const u8,
    values: []const u64,
    owns_name: bool = false,
    owns_values: bool = false,
};

pub const Column = union(enum) {
    string: StringColumn,
    var_bytes: VarBytesColumn,
    fixed_bytes: FixedBytesColumn,
    encoded: EncodedColumn,
    int8: Int8Column,
    int64: Int64Column,
    uint64: UInt64Column,

    pub fn name(self: Column) []const u8 {
        return switch (self) {
            .string => |column| column.name,
            .var_bytes => |column| column.name,
            .fixed_bytes => |column| column.name,
            .encoded => |column| column.name,
            .int8 => |column| column.name,
            .int64 => |column| column.name,
            .uint64 => |column| column.name,
        };
    }

    pub fn typeName(self: Column) []const u8 {
        return switch (self) {
            .string => "String",
            .var_bytes => |column| column.type_name,
            .fixed_bytes => |column| column.type_name,
            .encoded => |column| column.type_name,
            .int8 => "Int8",
            .int64 => "Int64",
            .uint64 => "UInt64",
        };
    }

    pub fn rowCount(self: Column) usize {
        return switch (self) {
            .string => |column| column.values.len,
            .var_bytes => |column| column.values.len,
            .fixed_bytes => |column| column.rowCount(),
            .encoded => |column| column.rows,
            .int8 => |column| column.values.len,
            .int64 => |column| column.values.len,
            .uint64 => |column| column.values.len,
        };
    }

    pub fn encodeState(self: Column, encoder: *Encoder) !void {
        switch (self) {
            .encoded => |column| try encoder.putRaw(column.state),
            else => {},
        }
    }

    pub fn encodeValues(self: Column, encoder: *Encoder) !void {
        switch (self) {
            .string => |column| {
                for (column.values) |value| {
                    try encoder.putString(value);
                }
            },
            .var_bytes => |column| {
                for (column.values) |value| {
                    try encoder.putString(value);
                }
            },
            .fixed_bytes => |column| {
                if (column.width == 0) return;
                if ((column.data.len % column.width) != 0) return error.InvalidFixedWidthColumn;
                try encoder.putRaw(column.data);
            },
            .encoded => |column| {
                try encoder.putRaw(column.payload);
            },
            .int8 => |column| {
                for (column.values) |value| {
                    try encoder.putByte(@bitCast(value));
                }
            },
            .int64 => |column| {
                for (column.values) |value| {
                    try encoder.putInt64LE(value);
                }
            },
            .uint64 => |column| {
                for (column.values) |value| {
                    try encoder.putUInt64LE(value);
                }
            },
        }
    }

    pub fn asFixed(self: Column) !FixedColumnView {
        return switch (self) {
            .fixed_bytes => |column| .{
                .name = column.name,
                .type_name = column.type_name,
                .width = column.width,
                .rows = column.rowCount(),
                .data = column.data,
            },
            .int8 => |column| .{
                .name = column.name,
                .type_name = "Int8",
                .width = @sizeOf(i8),
                .rows = column.values.len,
                .data = std.mem.sliceAsBytes(column.values),
            },
            .int64 => |column| .{
                .name = column.name,
                .type_name = "Int64",
                .width = @sizeOf(i64),
                .rows = column.values.len,
                .data = std.mem.sliceAsBytes(column.values),
            },
            .uint64 => |column| .{
                .name = column.name,
                .type_name = "UInt64",
                .width = @sizeOf(u64),
                .rows = column.values.len,
                .data = std.mem.sliceAsBytes(column.values),
            },
            else => error.NotFixedWidthColumn,
        };
    }

    pub fn asNullable(self: Column, allocator: std.mem.Allocator) !NullableColumnView {
        const encoded = switch (self) {
            .encoded => |column| column,
            else => return error.NotCompositeColumn,
        };
        const inner_type = unwrapTypeArgument(encoded.type_name, "Nullable") orelse return error.NotNullableColumn;

        var payload_decoder = Decoder.init(encoded.payload);
        const null_map_raw = try payload_decoder.readSlice(encoded.rows);
        const null_map = try allocator.dupe(u8, null_map_raw);
        errdefer allocator.free(null_map);
        const child_payload = try captureColumnPayload(allocator, &payload_decoder, inner_type, encoded.rows);
        errdefer allocator.free(child_payload);
        if (!payload_decoder.eof()) return error.TrailingColumnData;

        const child_state = try allocator.dupe(u8, encoded.state);
        errdefer allocator.free(child_state);
        const values = try decodeOwnedColumnFromStatePayload(allocator, "", inner_type, encoded.rows, child_state, child_payload);
        errdefer {
            var tmp = values;
            tmp.deinit(allocator);
        }

        return .{
            .null_map = null_map,
            .values = values,
        };
    }

    pub fn asArray(self: Column, allocator: std.mem.Allocator) !ArrayColumnView {
        const encoded = switch (self) {
            .encoded => |column| column,
            else => return error.NotCompositeColumn,
        };
        const inner_type = unwrapTypeArgument(encoded.type_name, "Array") orelse return error.NotArrayColumn;

        var payload_decoder = Decoder.init(encoded.payload);
        const offsets = try allocator.alloc(u64, encoded.rows);
        errdefer allocator.free(offsets);
        var total: usize = 0;
        for (offsets) |*offset| {
            offset.* = try payload_decoder.readUInt64LE();
            total = std.math.cast(usize, offset.*) orelse return error.IntegerOverflow;
        }
        const child_payload = try captureColumnPayload(allocator, &payload_decoder, inner_type, total);
        errdefer allocator.free(child_payload);
        if (!payload_decoder.eof()) return error.TrailingColumnData;

        const child_state = try allocator.dupe(u8, encoded.state);
        errdefer allocator.free(child_state);
        const values = try decodeOwnedColumnFromStatePayload(allocator, "", inner_type, total, child_state, child_payload);
        errdefer {
            var tmp = values;
            tmp.deinit(allocator);
        }

        return .{
            .offsets = offsets,
            .values = values,
        };
    }

    pub fn asMap(self: Column, allocator: std.mem.Allocator) !MapColumnView {
        const encoded = switch (self) {
            .encoded => |column| column,
            else => return error.NotCompositeColumn,
        };
        const inner_type = unwrapTypeArgument(encoded.type_name, "Map") orelse return error.NotMapColumn;
        const pair = try splitTopLevelPair(inner_type);

        var state_decoder = Decoder.init(encoded.state);
        const key_state = try captureColumnState(allocator, &state_decoder, pair.first);
        const value_state = try captureColumnState(allocator, &state_decoder, pair.second);
        var owns_key_state = true;
        var owns_value_state = true;
        errdefer if (owns_key_state) allocator.free(key_state);
        errdefer if (owns_value_state) allocator.free(value_state);
        if (!state_decoder.eof()) return error.TrailingColumnState;

        var payload_decoder = Decoder.init(encoded.payload);
        const offsets = try allocator.alloc(u64, encoded.rows);
        errdefer allocator.free(offsets);
        var total: usize = 0;
        for (offsets) |*offset| {
            offset.* = try payload_decoder.readUInt64LE();
            total = std.math.cast(usize, offset.*) orelse return error.IntegerOverflow;
        }
        const key_payload = try captureColumnPayload(allocator, &payload_decoder, pair.first, total);
        const value_payload = try captureColumnPayload(allocator, &payload_decoder, pair.second, total);
        var owns_key_payload = true;
        var owns_value_payload = true;
        errdefer if (owns_key_payload) allocator.free(key_payload);
        errdefer if (owns_value_payload) allocator.free(value_payload);
        if (!payload_decoder.eof()) return error.TrailingColumnData;

        const keys = try decodeOwnedColumnFromStatePayload(allocator, "", pair.first, total, key_state, key_payload);
        owns_key_state = false;
        owns_key_payload = false;
        errdefer {
            var tmp = keys;
            tmp.deinit(allocator);
        }
        const values = try decodeOwnedColumnFromStatePayload(allocator, "", pair.second, total, value_state, value_payload);
        owns_value_state = false;
        owns_value_payload = false;
        errdefer {
            var tmp = values;
            tmp.deinit(allocator);
        }

        return .{
            .offsets = offsets,
            .keys = keys,
            .values = values,
        };
    }

    pub fn asTuple(self: Column, allocator: std.mem.Allocator) !TupleColumnView {
        const encoded = switch (self) {
            .encoded => |column| column,
            else => return error.NotCompositeColumn,
        };
        const inner_type = unwrapTypeArgument(encoded.type_name, "Tuple") orelse return error.NotTupleColumn;

        var fields = std.ArrayList(TupleField).init(allocator);
        defer fields.deinit();
        errdefer {
            for (fields.items) |*field| {
                field.column.deinit(allocator);
            }
        }

        var state_decoder = Decoder.init(encoded.state);
        var payload_decoder = Decoder.init(encoded.payload);
        var iter = TopLevelSplitIterator.init(inner_type);
        while (iter.next()) |part| {
            const child_type = tupleElementTypeName(part);
            const child_name = tupleElementName(part);
            const child_state = try captureColumnState(allocator, &state_decoder, child_type);
            const child_payload = try captureColumnPayload(allocator, &payload_decoder, child_type, encoded.rows);
            var owns_child_state = true;
            var owns_child_payload = true;
            errdefer if (owns_child_state) allocator.free(child_state);
            errdefer if (owns_child_payload) allocator.free(child_payload);
            const child = try decodeOwnedColumnFromStatePayload(allocator, "", child_type, encoded.rows, child_state, child_payload);
            owns_child_state = false;
            owns_child_payload = false;
            fields.append(.{
                .name = child_name,
                .column = child,
            }) catch |err| {
                var tmp = child;
                tmp.deinit(allocator);
                return err;
            };
        }
        if (!state_decoder.eof()) return error.TrailingColumnState;
        if (!payload_decoder.eof()) return error.TrailingColumnData;

        return .{
            .rows = encoded.rows,
            .fields = try fields.toOwnedSlice(),
        };
    }

    pub fn asLowCardinality(self: Column, allocator: std.mem.Allocator) !LowCardinalityColumnView {
        const encoded = switch (self) {
            .encoded => |column| column,
            else => return error.NotCompositeColumn,
        };
        const inner_type = unwrapTypeArgument(encoded.type_name, "LowCardinality") orelse return error.NotLowCardinalityColumn;

        var state_decoder = Decoder.init(encoded.state);
        const version = try state_decoder.readInt64LE();
        const dictionary_state = try captureColumnState(allocator, &state_decoder, inner_type);
        var owns_dictionary_state = true;
        errdefer if (owns_dictionary_state) allocator.free(dictionary_state);
        if (!state_decoder.eof()) return error.TrailingColumnState;

        var payload_decoder = Decoder.init(encoded.payload);
        const meta = try payload_decoder.readInt64LE();
        const key_width = try lowCardinalityKeyWidthForMeta(meta);
        const dictionary_rows_i64 = try payload_decoder.readInt64LE();
        const dictionary_rows = std.math.cast(usize, dictionary_rows_i64) orelse return error.IntegerOverflow;
        const dictionary_payload = try captureColumnPayload(allocator, &payload_decoder, inner_type, dictionary_rows);
        var owns_dictionary_payload = true;
        errdefer if (owns_dictionary_payload) allocator.free(dictionary_payload);
        const keys_rows_i64 = try payload_decoder.readInt64LE();
        const keys_rows = std.math.cast(usize, keys_rows_i64) orelse return error.IntegerOverflow;
        const key_bytes = try payload_decoder.readSlice(try std.math.mul(usize, keys_rows, key_width));
        const keys_payload = try allocator.dupe(u8, key_bytes);
        var owns_keys_payload = true;
        errdefer if (owns_keys_payload) allocator.free(keys_payload);
        if (!payload_decoder.eof()) return error.TrailingColumnData;

        const dictionary = try decodeOwnedColumnFromStatePayload(allocator, "", inner_type, dictionary_rows, dictionary_state, dictionary_payload);
        owns_dictionary_state = false;
        owns_dictionary_payload = false;
        errdefer {
            var tmp = dictionary;
            tmp.deinit(allocator);
        }

        const key_type = switch (key_width) {
            1 => "UInt8",
            2 => "UInt16",
            4 => "UInt32",
            8 => "UInt64",
            else => return error.InvalidLowCardinalityKeyType,
        };
        const empty_state = try allocator.alloc(u8, 0);
        var owns_keys_state = true;
        errdefer if (owns_keys_state) allocator.free(empty_state);
        const keys = try decodeOwnedColumnFromStatePayload(allocator, "", key_type, keys_rows, empty_state, keys_payload);
        owns_keys_state = false;
        owns_keys_payload = false;
        errdefer {
            var tmp = keys;
            tmp.deinit(allocator);
        }

        return .{
            .serialization_version = version,
            .meta = meta,
            .dictionary = dictionary,
            .keys = keys,
        };
    }

    fn decode(allocator: std.mem.Allocator, decoder: *Decoder, column_name: []const u8, type_name: []const u8, rows: usize) !Column {
        if (rows == 0) return emptyColumnForType(allocator, column_name, type_name);
        if (std.mem.eql(u8, type_name, "String")) {
            const values = try allocator.alloc([]const u8, rows);
            errdefer allocator.free(values);
            for (values, 0..) |*value, idx| {
                _ = idx;
                value.* = try decoder.readString();
            }
            return .{ .string = .{
                .name = column_name,
                .values = values,
                .owns_values = true,
            } };
        }
        if (std.mem.eql(u8, type_name, "JSON")) {
            const values = try allocator.alloc([]const u8, rows);
            errdefer allocator.free(values);
            for (values) |*value| {
                value.* = try decoder.readString();
            }
            return .{ .var_bytes = .{
                .name = column_name,
                .type_name = type_name,
                .values = values,
                .owns_values = true,
            } };
        }
        if (std.mem.eql(u8, type_name, "Int8")) {
            const values = try allocator.alloc(i8, rows);
            errdefer allocator.free(values);
            for (values) |*value| {
                value.* = @bitCast(try decoder.readByte());
            }
            return .{ .int8 = .{
                .name = column_name,
                .values = values,
                .owns_values = true,
            } };
        }
        if (std.mem.eql(u8, type_name, "Int64")) {
            const values = try allocator.alloc(i64, rows);
            errdefer allocator.free(values);
            for (values) |*value| {
                value.* = try decoder.readInt64LE();
            }
            return .{ .int64 = .{
                .name = column_name,
                .values = values,
                .owns_values = true,
            } };
        }
        if (std.mem.eql(u8, type_name, "UInt64")) {
            const values = try allocator.alloc(u64, rows);
            errdefer allocator.free(values);
            for (values) |*value| {
                value.* = try decoder.readUInt64LE();
            }
            return .{ .uint64 = .{
                .name = column_name,
                .values = values,
                .owns_values = true,
            } };
        }
        if (fixedWidthForType(type_name)) |width| {
            const total = try std.math.mul(usize, rows, width);
            const data = try allocator.alloc(u8, total);
            errdefer allocator.free(data);
            const payload = try decoder.readSlice(total);
            @memcpy(data, payload);
            return .{ .fixed_bytes = .{
                .name = column_name,
                .type_name = type_name,
                .width = width,
                .data = data,
                .rows = rows,
                .owns_data = true,
            } };
        }

        const state = try captureColumnState(allocator, decoder, type_name);
        errdefer allocator.free(state);
        const payload = try captureColumnPayload(allocator, decoder, type_name, rows);
        errdefer allocator.free(payload);
        return .{ .encoded = .{
            .name = column_name,
            .type_name = type_name,
            .rows = rows,
            .state = state,
            .payload = payload,
            .owns_state = true,
            .owns_payload = true,
        } };
    }

    fn decodeFromStream(allocator: std.mem.Allocator, reader: *StreamReader, column_name: []const u8, type_name: []const u8, rows: usize) !Column {
        if (rows == 0) return emptyColumnForType(allocator, column_name, type_name);
        const state = try captureColumnStateFromStream(allocator, reader, type_name);
        errdefer allocator.free(state);
        const payload = try captureColumnPayloadFromStream(allocator, reader, type_name, rows);
        errdefer allocator.free(payload);
        return decodeOwnedColumnFromStatePayload(allocator, column_name, type_name, rows, state, payload);
    }

    pub fn deinit(self: *Column, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => |column| {
                if (column.owns_name) allocator.free(column.name);
                if (column.owns_values) allocator.free(column.values);
                if (column.owns_backing_data) allocator.free(column.backing_data);
            },
            .var_bytes => |column| {
                if (column.owns_name) allocator.free(column.name);
                if (column.owns_type_name) allocator.free(column.type_name);
                if (column.owns_values) allocator.free(column.values);
                if (column.owns_backing_data) allocator.free(column.backing_data);
            },
            .fixed_bytes => |column| {
                if (column.owns_name) allocator.free(column.name);
                if (column.owns_type_name) allocator.free(column.type_name);
                if (column.owns_data) allocator.free(column.data);
            },
            .encoded => |column| {
                if (column.owns_name) allocator.free(column.name);
                if (column.owns_type_name) allocator.free(column.type_name);
                if (column.owns_state) allocator.free(column.state);
                if (column.owns_payload) allocator.free(column.payload);
            },
            .int8 => |column| {
                if (column.owns_name) allocator.free(column.name);
                if (column.owns_values) allocator.free(column.values);
            },
            .int64 => |column| {
                if (column.owns_name) allocator.free(column.name);
                if (column.owns_values) allocator.free(column.values);
            },
            .uint64 => |column| {
                if (column.owns_name) allocator.free(column.name);
                if (column.owns_values) allocator.free(column.values);
            },
        }
    }

    pub fn cloneOwned(self: Column, allocator: std.mem.Allocator) !Column {
        return switch (self) {
            .string => |column| blk: {
                var cloned = try initOwnedStringColumn(allocator, column.name, column.values);
                switch (cloned) {
                    .string => |*value| {
                        value.name = try allocator.dupe(u8, column.name);
                        value.owns_name = true;
                    },
                    else => unreachable,
                }
                break :blk cloned;
            },
            .var_bytes => |column| blk: {
                var cloned = try initOwnedVarBytesColumn(allocator, column.name, column.type_name, column.values);
                switch (cloned) {
                    .var_bytes => |*value| {
                        value.name = try allocator.dupe(u8, column.name);
                        value.type_name = try allocator.dupe(u8, column.type_name);
                        value.owns_name = true;
                        value.owns_type_name = true;
                    },
                    else => unreachable,
                }
                break :blk cloned;
            },
            .fixed_bytes => |column| .{ .fixed_bytes = .{
                .name = try allocator.dupe(u8, column.name),
                .type_name = try allocator.dupe(u8, column.type_name),
                .width = column.width,
                .data = try allocator.dupe(u8, column.data),
                .rows = column.rowCount(),
                .owns_name = true,
                .owns_type_name = true,
                .owns_data = true,
            } },
            .encoded => |column| .{ .encoded = .{
                .name = try allocator.dupe(u8, column.name),
                .type_name = try allocator.dupe(u8, column.type_name),
                .rows = column.rows,
                .state = try allocator.dupe(u8, column.state),
                .payload = try allocator.dupe(u8, column.payload),
                .owns_name = true,
                .owns_type_name = true,
                .owns_state = true,
                .owns_payload = true,
            } },
            .int8 => |column| .{ .int8 = .{
                .name = try allocator.dupe(u8, column.name),
                .values = try allocator.dupe(i8, column.values),
                .owns_name = true,
                .owns_values = true,
            } },
            .int64 => |column| .{ .int64 = .{
                .name = try allocator.dupe(u8, column.name),
                .values = try allocator.dupe(i64, column.values),
                .owns_name = true,
                .owns_values = true,
            } },
            .uint64 => |column| .{ .uint64 = .{
                .name = try allocator.dupe(u8, column.name),
                .values = try allocator.dupe(u64, column.values),
                .owns_name = true,
                .owns_values = true,
            } },
        };
    }
};

pub const FixedColumnView = struct {
    name: []const u8,
    type_name: []const u8,
    width: usize,
    rows: usize,
    data: []const u8,

    pub fn row(self: FixedColumnView, index: usize) []const u8 {
        const start = index * self.width;
        return self.data[start .. start + self.width];
    }

    pub fn slice(self: FixedColumnView, comptime T: type) ![]align(1) const T {
        if (self.width != @sizeOf(T)) return error.InvalidFixedWidthColumn;
        if ((self.data.len % @sizeOf(T)) != 0) return error.InvalidFixedWidthColumn;
        const ptr: [*]align(1) const T = @ptrCast(self.data.ptr);
        return ptr[0 .. self.data.len / @sizeOf(T)];
    }

    pub fn boolAt(self: FixedColumnView, index: usize) !bool {
        if (!std.mem.eql(u8, typeBaseName(self.type_name), "Bool")) return error.InvalidBoolColumn;
        const value = self.row(index)[0];
        return switch (value) {
            0 => false,
            1 => true,
            else => error.InvalidBool,
        };
    }

    pub fn bools(self: FixedColumnView, allocator: std.mem.Allocator) ![]bool {
        const out = try allocator.alloc(bool, self.rows);
        errdefer allocator.free(out);
        for (out, 0..) |*value, idx| {
            value.* = try self.boolAt(idx);
        }
        return out;
    }
};

pub const NullableColumnView = struct {
    null_map: []const u8,
    values: Column,

    pub fn rows(self: NullableColumnView) usize {
        return self.null_map.len;
    }

    pub fn isNull(self: NullableColumnView, index: usize) bool {
        return self.null_map[index] != 0;
    }

    pub fn deinit(self: *NullableColumnView, allocator: std.mem.Allocator) void {
        allocator.free(self.null_map);
        self.values.deinit(allocator);
        self.null_map = &.{};
        self.values = undefined;
    }
};

pub const ArrayColumnView = struct {
    offsets: []const u64,
    values: Column,

    pub fn rows(self: ArrayColumnView) usize {
        return self.offsets.len;
    }

    pub fn rowRange(self: ArrayColumnView, index: usize) struct { start: usize, end: usize } {
        const start = if (index == 0) 0 else @as(usize, @intCast(self.offsets[index - 1]));
        const end = @as(usize, @intCast(self.offsets[index]));
        return .{ .start = start, .end = end };
    }

    pub fn deinit(self: *ArrayColumnView, allocator: std.mem.Allocator) void {
        allocator.free(self.offsets);
        self.values.deinit(allocator);
        self.offsets = &.{};
        self.values = undefined;
    }
};

pub const MapColumnView = struct {
    offsets: []const u64,
    keys: Column,
    values: Column,

    pub fn rows(self: MapColumnView) usize {
        return self.offsets.len;
    }

    pub fn rowRange(self: MapColumnView, index: usize) struct { start: usize, end: usize } {
        const start = if (index == 0) 0 else @as(usize, @intCast(self.offsets[index - 1]));
        const end = @as(usize, @intCast(self.offsets[index]));
        return .{ .start = start, .end = end };
    }

    pub fn deinit(self: *MapColumnView, allocator: std.mem.Allocator) void {
        allocator.free(self.offsets);
        self.keys.deinit(allocator);
        self.values.deinit(allocator);
        self.offsets = &.{};
        self.keys = undefined;
        self.values = undefined;
    }
};

pub const TupleField = struct {
    name: []const u8 = "",
    column: Column,
};

pub const TupleColumnView = struct {
    rows: usize,
    fields: []TupleField,

    pub fn deinit(self: *TupleColumnView, allocator: std.mem.Allocator) void {
        for (self.fields) |*field| {
            field.column.deinit(allocator);
        }
        allocator.free(self.fields);
        self.fields = &.{};
        self.rows = 0;
    }
};

pub const LowCardinalityColumnView = struct {
    serialization_version: i64,
    meta: i64,
    dictionary: Column,
    keys: Column,

    pub fn rows(self: LowCardinalityColumnView) usize {
        return self.keys.rowCount();
    }

    pub fn deinit(self: *LowCardinalityColumnView, allocator: std.mem.Allocator) void {
        self.dictionary.deinit(allocator);
        self.keys.deinit(allocator);
        self.dictionary = undefined;
        self.keys = undefined;
    }
};

pub fn initOwnedStringColumn(allocator: std.mem.Allocator, name: []const u8, values: []const []const u8) !Column {
    return buildOwnedStringLikeColumn(allocator, name, "String", values, false);
}

pub fn initOwnedVarBytesColumn(allocator: std.mem.Allocator, name: []const u8, type_name: []const u8, values: []const []const u8) !Column {
    return buildOwnedStringLikeColumn(allocator, name, type_name, values, true);
}

pub fn initOwnedFixedColumn(allocator: std.mem.Allocator, name: []const u8, type_name: []const u8, values: anytype) !Column {
    const normalized = blk: {
        const info = @typeInfo(@TypeOf(values));
        if (info != .pointer) @compileError("initOwnedFixedColumn expects a slice or pointer to array");
        switch (info.pointer.size) {
            .slice => break :blk values,
            .one => {
                const child_info = @typeInfo(info.pointer.child);
                if (child_info != .array) @compileError("initOwnedFixedColumn expects a slice or pointer to array");
                const Elem = child_info.array.child;
                const out: []const Elem = values;
                break :blk out;
            },
            else => @compileError("initOwnedFixedColumn expects a slice or pointer to array"),
        }
    };

    const slice_info = @typeInfo(@TypeOf(normalized));
    const T = slice_info.pointer.child;
    const width = fixedWidthForType(type_name) orelse @sizeOf(T);

    if (T == i8 and std.mem.eql(u8, type_name, "Int8")) {
        const out = try allocator.dupe(i8, normalized);
        return .{ .int8 = .{
            .name = name,
            .values = out,
            .owns_values = true,
        } };
    }
    if (T == i64 and std.mem.eql(u8, type_name, "Int64")) {
        const out = try allocator.dupe(i64, normalized);
        return .{ .int64 = .{
            .name = name,
            .values = out,
            .owns_values = true,
        } };
    }
    if (T == u64 and std.mem.eql(u8, type_name, "UInt64")) {
        const out = try allocator.dupe(u64, normalized);
        return .{ .uint64 = .{
            .name = name,
            .values = out,
            .owns_values = true,
        } };
    }

    const total = try std.math.mul(usize, normalized.len, width);
    const out = try allocator.alloc(u8, total);
    errdefer allocator.free(out);

    if (@typeInfo(T) == .bool) {
        if (width != 1) return error.InvalidFixedWidthColumn;
        for (normalized, 0..) |value, idx| {
            out[idx] = if (value) 1 else 0;
        }
    } else if (@typeInfo(T) == .array and @typeInfo(T).array.child == u8) {
        if (width != @sizeOf(T)) return error.InvalidFixedWidthColumn;
        var offset: usize = 0;
        for (normalized) |value| {
            @memcpy(out[offset .. offset + width], value[0..]);
            offset += width;
        }
    } else {
        if (width != @sizeOf(T)) return error.InvalidFixedWidthColumn;
        @memcpy(out, std.mem.sliceAsBytes(normalized));
    }

    return .{ .fixed_bytes = .{
        .name = name,
        .type_name = type_name,
        .width = width,
        .data = out,
        .rows = normalized.len,
        .owns_data = true,
    } };
}

pub fn initNullableColumn(allocator: std.mem.Allocator, name: []const u8, type_name: []const u8, null_map: []const bool, values: Column) !Column {
    if (type_name.len != 0) {
        const inner_type = unwrapTypeArgument(type_name, "Nullable") orelse return error.NotNullableColumn;
        _ = inner_type;
    }
    if (values.rowCount() != null_map.len) return error.RowCountMismatch;

    var state_encoder = Encoder.init(allocator);
    errdefer state_encoder.deinit();
    try values.encodeState(&state_encoder);

    var payload_encoder = Encoder.init(allocator);
    errdefer payload_encoder.deinit();
    for (null_map) |is_null| {
        try payload_encoder.putByte(if (is_null) 1 else 0);
    }
    try values.encodeValues(&payload_encoder);

    return .{ .encoded = .{
        .name = name,
        .type_name = type_name,
        .rows = null_map.len,
        .state = try state_encoder.buf.toOwnedSlice(),
        .payload = try payload_encoder.buf.toOwnedSlice(),
        .owns_state = true,
        .owns_payload = true,
    } };
}

pub fn initArrayColumn(allocator: std.mem.Allocator, name: []const u8, type_name: []const u8, offsets: []const u64, values: Column) !Column {
    if (type_name.len != 0) {
        const inner_type = unwrapTypeArgument(type_name, "Array") orelse return error.NotArrayColumn;
        _ = inner_type;
    }
    const total = if (offsets.len == 0) 0 else std.math.cast(usize, offsets[offsets.len - 1]) orelse return error.IntegerOverflow;
    if (values.rowCount() != total) return error.RowCountMismatch;

    var state_encoder = Encoder.init(allocator);
    errdefer state_encoder.deinit();
    try values.encodeState(&state_encoder);

    var payload_encoder = Encoder.init(allocator);
    errdefer payload_encoder.deinit();
    for (offsets) |offset| {
        try payload_encoder.putUInt64LE(offset);
    }
    try values.encodeValues(&payload_encoder);

    return .{ .encoded = .{
        .name = name,
        .type_name = type_name,
        .rows = offsets.len,
        .state = try state_encoder.buf.toOwnedSlice(),
        .payload = try payload_encoder.buf.toOwnedSlice(),
        .owns_state = true,
        .owns_payload = true,
    } };
}

pub fn initMapColumn(allocator: std.mem.Allocator, name: []const u8, type_name: []const u8, offsets: []const u64, keys: Column, values: Column) !Column {
    if (type_name.len != 0) {
        const inner_type = unwrapTypeArgument(type_name, "Map") orelse return error.NotMapColumn;
        const pair = try splitTopLevelPair(inner_type);
        _ = pair;
    }

    const total = if (offsets.len == 0) 0 else std.math.cast(usize, offsets[offsets.len - 1]) orelse return error.IntegerOverflow;
    if (keys.rowCount() != total or values.rowCount() != total) return error.RowCountMismatch;

    var state_encoder = Encoder.init(allocator);
    errdefer state_encoder.deinit();
    try keys.encodeState(&state_encoder);
    try values.encodeState(&state_encoder);

    var payload_encoder = Encoder.init(allocator);
    errdefer payload_encoder.deinit();
    for (offsets) |offset| {
        try payload_encoder.putUInt64LE(offset);
    }
    try keys.encodeValues(&payload_encoder);
    try values.encodeValues(&payload_encoder);

    return .{ .encoded = .{
        .name = name,
        .type_name = type_name,
        .rows = offsets.len,
        .state = try state_encoder.buf.toOwnedSlice(),
        .payload = try payload_encoder.buf.toOwnedSlice(),
        .owns_state = true,
        .owns_payload = true,
    } };
}

pub fn initTupleColumn(allocator: std.mem.Allocator, name: []const u8, type_name: []const u8, fields: []const TupleField) !Column {
    if (type_name.len != 0) {
        const inner_type = unwrapTypeArgument(type_name, "Tuple") orelse return error.NotTupleColumn;
        var part_count: usize = 0;
        var iter = TopLevelSplitIterator.init(inner_type);
        while (iter.next()) |_| {
            part_count += 1;
        }
        if (part_count != fields.len) return error.RowCountMismatch;
    }

    const rows = if (fields.len == 0) 0 else fields[0].column.rowCount();
    for (fields) |field| {
        if (field.column.rowCount() != rows) return error.RowCountMismatch;
    }

    var state_encoder = Encoder.init(allocator);
    errdefer state_encoder.deinit();
    var payload_encoder = Encoder.init(allocator);
    errdefer payload_encoder.deinit();
    for (fields) |field| {
        try field.column.encodeState(&state_encoder);
        try field.column.encodeValues(&payload_encoder);
    }

    return .{ .encoded = .{
        .name = name,
        .type_name = type_name,
        .rows = rows,
        .state = try state_encoder.buf.toOwnedSlice(),
        .payload = try payload_encoder.buf.toOwnedSlice(),
        .owns_state = true,
        .owns_payload = true,
    } };
}

pub fn initLowCardinalityColumn(allocator: std.mem.Allocator, name: []const u8, type_name: []const u8, dictionary: Column, keys: Column) !Column {
    if (type_name.len != 0) {
        const inner_type = unwrapTypeArgument(type_name, "LowCardinality") orelse return error.NotLowCardinalityColumn;
        _ = inner_type;
    }
    const key_tag = try lowCardinalityKeyTagForColumn(keys);

    var state_encoder = Encoder.init(allocator);
    errdefer state_encoder.deinit();
    try state_encoder.putInt64LE(1);
    try dictionary.encodeState(&state_encoder);

    var payload_encoder = Encoder.init(allocator);
    errdefer payload_encoder.deinit();
    try payload_encoder.putInt64LE(low_cardinality_update_all | key_tag);
    try payload_encoder.putInt64LE(@intCast(dictionary.rowCount()));
    try dictionary.encodeValues(&payload_encoder);
    try payload_encoder.putInt64LE(@intCast(keys.rowCount()));
    try keys.encodeValues(&payload_encoder);

    return .{ .encoded = .{
        .name = name,
        .type_name = type_name,
        .rows = keys.rowCount(),
        .state = try state_encoder.buf.toOwnedSlice(),
        .payload = try payload_encoder.buf.toOwnedSlice(),
        .owns_state = true,
        .owns_payload = true,
    } };
}

pub const DataBlock = struct {
    info: BlockInfo = .{},
    columns: []const Column,
    rows: usize = 0,

    pub fn encodeRaw(self: DataBlock, encoder: *Encoder, revision: u32) !void {
        const rows = try self.effectiveRows();
        try encoder.putVarUInt(self.columns.len);
        try encoder.putVarUInt(rows);

        for (self.columns) |column| {
            if (column.rowCount() != rows) return error.RowCountMismatch;
            try encoder.putString(column.name());
            try encoder.putString(column.typeName());
            if (Feature.custom_serialization.enabled(revision)) {
                try encoder.putBool(false);
            }
            if (rows == 0) continue;
            try column.encodeState(encoder);
            try column.encodeValues(encoder);
        }
    }

    pub fn encode(self: DataBlock, encoder: *Encoder, revision: u32) !void {
        if (Feature.block_info.enabled(revision)) {
            try self.info.encode(encoder);
        }
        try self.encodeRaw(encoder, revision);
    }

    fn effectiveRows(self: DataBlock) !usize {
        if (self.columns.len == 0) {
            if (self.rows != 0) return error.RowsWithoutColumns;
            return 0;
        }
        const expected = if (self.rows == 0) self.columns[0].rowCount() else self.rows;
        for (self.columns) |column| {
            if (column.rowCount() != expected) return error.RowCountMismatch;
        }
        return expected;
    }
};

pub const DecodedBlock = struct {
    info: BlockInfo = .{},
    columns: []Column = &.{},
    rows: usize = 0,

    pub fn deinit(self: *DecodedBlock, allocator: std.mem.Allocator) void {
        for (self.columns) |*column| {
            column.deinit(allocator);
        }
        if (self.columns.len > 0) allocator.free(self.columns);
        self.columns = &.{};
        self.rows = 0;
    }

    pub fn isEnd(self: DecodedBlock) bool {
        return self.columns.len == 0 and self.rows == 0;
    }

    pub fn decode(decoder: *Decoder, allocator: std.mem.Allocator, revision: u32) !DecodedBlock {
        var info = BlockInfo{};
        if (Feature.block_info.enabled(revision)) {
            info = try BlockInfo.decode(decoder);
        }
        var block = try decodeRaw(decoder, allocator, revision);
        block.info = info;
        return block;
    }

    pub fn decodeRaw(decoder: *Decoder, allocator: std.mem.Allocator, revision: u32) !DecodedBlock {
        var block = DecodedBlock{};
        errdefer block.deinit(allocator);

        const column_count = try readVarUIntAs(usize, decoder);
        const rows = try readVarUIntAs(usize, decoder);
        if (column_count == 0 and rows == 0) {
            block.rows = 0;
            return block;
        }

        block.columns = try allocator.alloc(Column, column_count);
        block.rows = rows;
        var idx: usize = 0;
        errdefer {
            for (block.columns[0..idx]) |*column| column.deinit(allocator);
            allocator.free(block.columns);
            block.columns = &.{};
        }

        while (idx < column_count) : (idx += 1) {
            const name = try decoder.readString();
            const type_name = try decoder.readString();
            if (Feature.custom_serialization.enabled(revision)) {
                if (try decoder.readBool()) return error.UnsupportedCustomSerialization;
            }
            block.columns[idx] = try Column.decode(allocator, decoder, name, type_name, rows);
        }
        return block;
    }

    pub fn decodeFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32) !DecodedBlock {
        var info = BlockInfo{};
        if (Feature.block_info.enabled(revision)) {
            info = try BlockInfo.decodeFromStream(reader);
        }
        var block = try decodeRawFromStream(reader, allocator, revision);
        block.info = info;
        return block;
    }

    pub fn decodeRawFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32) !DecodedBlock {
        var block = DecodedBlock{};
        errdefer block.deinit(allocator);

        const column_count = try castVarUInt(usize, try reader.readVarUInt());
        const rows = try castVarUInt(usize, try reader.readVarUInt());
        if (column_count == 0 and rows == 0) {
            block.rows = 0;
            return block;
        }

        block.columns = try allocator.alloc(Column, column_count);
        block.rows = rows;
        var idx: usize = 0;
        errdefer {
            for (block.columns[0..idx]) |*column| column.deinit(allocator);
            allocator.free(block.columns);
            block.columns = &.{};
        }

        while (idx < column_count) : (idx += 1) {
            const name = try reader.readStringAlloc(allocator);
            const type_name = try reader.readStringAlloc(allocator);
            if (Feature.custom_serialization.enabled(revision)) {
                if (try reader.readBool()) return error.UnsupportedCustomSerialization;
            }
            block.columns[idx] = try Column.decodeFromStream(allocator, reader, name, type_name, rows);
        }
        return block;
    }

    pub fn cloneOwned(self: DecodedBlock, allocator: std.mem.Allocator) !DecodedBlock {
        var out = DecodedBlock{
            .info = self.info,
            .rows = self.rows,
        };
        if (self.columns.len == 0) {
            out.columns = &.{};
            return out;
        }

        out.columns = try allocator.alloc(Column, self.columns.len);
        errdefer allocator.free(out.columns);
        var idx: usize = 0;
        errdefer {
            for (out.columns[0..idx]) |*column| {
                column.deinit(allocator);
            }
        }

        while (idx < self.columns.len) : (idx += 1) {
            out.columns[idx] = try self.columns[idx].cloneOwned(allocator);
        }
        return out;
    }
};

pub const DataPacket = struct {
    temp_table: []const u8 = "",
    block: DataBlock,

    pub fn encodePacket(self: DataPacket, encoder: *Encoder, revision: u32) !void {
        try ClientCode.data.encode(encoder);
        if (Feature.temp_tables.enabled(revision)) {
            try encoder.putString(self.temp_table);
        }
        try self.block.encode(encoder, revision);
    }
};

pub const DecodedDataPacket = struct {
    temp_table: []const u8 = "",
    block: DecodedBlock = .{},

    pub fn deinit(self: *DecodedDataPacket, allocator: std.mem.Allocator) void {
        self.block.deinit(allocator);
    }

    pub fn decodeClientPacket(decoder: *Decoder, allocator: std.mem.Allocator, revision: u32) !DecodedDataPacket {
        const code = try ClientCode.decode(decoder);
        if (code != .data) return error.UnexpectedPacket;
        return decodePayload(decoder, allocator, revision);
    }

    pub fn decodePayload(decoder: *Decoder, allocator: std.mem.Allocator, revision: u32) !DecodedDataPacket {
        var packet = DecodedDataPacket{};
        if (Feature.temp_tables.enabled(revision)) {
            packet.temp_table = try decoder.readString();
        }
        packet.block = try DecodedBlock.decode(decoder, allocator, revision);
        return packet;
    }

    pub fn decodePayloadFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32) !DecodedDataPacket {
        var packet = DecodedDataPacket{};
        if (Feature.temp_tables.enabled(revision)) {
            packet.temp_table = try reader.readStringAlloc(allocator);
        }
        packet.block = try DecodedBlock.decodeFromStream(reader, allocator, revision);
        return packet;
    }

    pub fn decodeClientPacketFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32) !DecodedDataPacket {
        const code = try readClientCodeFromStream(reader);
        if (code != .data) return error.UnexpectedPacket;
        return decodePayloadFromStream(reader, allocator, revision);
    }
};

pub const ServerPacket = union(enum) {
    hello: ServerHello,
    data: DecodedDataPacket,
    totals: DecodedDataPacket,
    extremes: DecodedDataPacket,
    log: DecodedDataPacket,
    profile_events: DecodedDataPacket,
    exception: ExceptionChain,
    progress: Progress,
    pong: void,
    end_of_stream: void,
    profile: Profile,
    table_columns: TableColumns,
    tables_status: TablesStatusResponse,
    part_uuids: PartUUIDs,
    read_task_request: ReadTaskRequest,
    ssh_challenge: SSHChallenge,
};

pub const OwnedServerPacket = struct {
    arena: ?std.heap.ArenaAllocator = null,
    value: ServerPacket,

    pub fn deinit(self: *OwnedServerPacket) void {
        if (self.arena) |*arena| {
            arena.deinit();
            self.arena = null;
        }
    }
};

pub const ClientOptions = struct {
    protocol_version: u32 = default_protocol_version,
    database: []const u8 = "default",
    user: []const u8 = "default",
    password: []const u8 = "",
    quota_key: []const u8 = "",
    client_name: []const u8 = default_client_name,
    client_version_major: u32 = 0,
    client_version_minor: u32 = 1,
    client_version_patch: u32 = 0,
    initial_address: []const u8 = "0.0.0.0:0",
    os_user: []const u8 = "",
    client_hostname: []const u8 = "",
    compression: BlockCompression = .disabled,
    compression_level: u32 = 0,
    ssh_signer: ?SshSignFn = null,
    dialer: ?DialerFn = null,
    dial_timeout_ms: u64 = 0,
    read_timeout_ms: u64 = 0,
    write_timeout_ms: u64 = 0,
    handshake_timeout_ms: u64 = 0,
    tls: TlsOptions = .{},
    observer: Observer = .{},
};

const PreparedQuery = struct {
    wire: Query,
    owned_settings: ?[]Setting = null,
    owned_query_id: ?[]u8 = null,

    fn deinit(self: *PreparedQuery, allocator: std.mem.Allocator) void {
        if (self.owned_settings) |settings| allocator.free(settings);
        if (self.owned_query_id) |query_id| allocator.free(query_id);
        self.* = undefined;
    }
};

const cancel_poll_interval_ns: u64 = 5 * std.time.ns_per_ms;

const InputSchemaWaiter = struct {
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    ready: bool = false,
    failed: bool = false,
    schema: ?DecodedBlock = null,
    err: ?anyerror = null,

    fn init(allocator: std.mem.Allocator) InputSchemaWaiter {
        return .{ .allocator = allocator };
    }

    fn deinit(self: *InputSchemaWaiter) void {
        self.mutex.lock();
        if (self.schema) |*schema| {
            schema.deinit(self.allocator);
            self.schema = null;
        }
        self.mutex.unlock();
    }

    fn signal(self: *InputSchemaWaiter, block: *const DecodedBlock) !void {
        const clone = try block.cloneOwned(self.allocator);
        errdefer {
            var tmp = clone;
            tmp.deinit(self.allocator);
        }

        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.ready or self.failed) {
            var tmp = clone;
            tmp.deinit(self.allocator);
            return;
        }
        self.schema = clone;
        self.ready = true;
        self.cond.broadcast();
    }

    fn fail(self: *InputSchemaWaiter, err: anyerror) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.ready or self.failed) return;
        self.err = err;
        self.failed = true;
        self.cond.broadcast();
    }

    fn isResolved(self: *InputSchemaWaiter) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.ready or self.failed;
    }

    fn wait(self: *InputSchemaWaiter) !DecodedBlock {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (!self.ready and !self.failed) {
            self.cond.wait(&self.mutex);
        }
        if (self.failed) return self.err orelse error.InputInferenceFailed;
        const schema = self.schema.?;
        self.schema = null;
        return schema;
    }
};

const DoRuntime = struct {
    client: *Client,
    ctx: QueryContext,
    query: *Query,
    input_waiter: ?*InputSchemaWaiter = null,
    metrics: *QueryMetrics,
    observer: ?Observer = null,
    mutex: std.Thread.Mutex = .{},
    query_started: bool = false,
    sender_done: bool = false,
    receiver_done: bool = false,
    done: bool = false,
    got_exception: bool = false,
    cancel_sent: bool = false,
    sender_err: ?anyerror = null,
    receiver_err: ?anyerror = null,

    fn markQueryStarted(self: *DoRuntime) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.query_started = true;
    }

    fn queryStarted(self: *DoRuntime) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.query_started;
    }

    fn finishSender(self: *DoRuntime, err: ?anyerror) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.sender_done = true;
        self.sender_err = err;
        if (err != null or self.receiver_done) {
            self.done = true;
        }
    }

    fn finishReceiver(self: *DoRuntime, err: ?anyerror) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.receiver_done = true;
        self.receiver_err = err;
        if (err) |value| {
            if (value == error.ServerException) self.got_exception = true;
            self.done = true;
        } else if (self.sender_done) {
            self.done = true;
        }
    }

    fn shouldCancel(self: *DoRuntime) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.cancel_sent or self.done or self.got_exception or !self.query_started) return false;
        if (!self.ctx.canceled()) return false;
        self.cancel_sent = true;
        return true;
    }

    fn isDone(self: *DoRuntime) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.done;
    }
};

const DoReceiveThreadState = struct {
    runtime: *DoRuntime,
};

const DoCancelThreadState = struct {
    runtime: *DoRuntime,
};

pub const Client = struct {
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
    stream_closed: bool,
    tls_client: ?*std.crypto.tls.Client,
    tls_ca_bundle: ?std.crypto.Certificate.Bundle,
    protocol_version: u32,
    hello: ClientHello,
    server: ServerHello,
    query_defaults: ClientInfo,
    quota_key: []const u8,
    default_query_compression: Compression,
    active_query_compression: Compression,
    block_compression: BlockCompression,
    block_compression_level: u32,
    ssh_signer: ?SshSignFn,
    ssh_auth_user: []const u8,
    owned_hello_user: []u8,
    read_timeout_ms: u64,
    write_timeout_ms: u64,
    handshake_timeout_ms: u64,
    tls_enabled: bool,
    tls_server_name: []const u8,
    observer: Observer,
    server_storage: std.heap.ArenaAllocator,
    last_exception_storage: std.heap.ArenaAllocator,
    last_exception: ?ExceptionChain,

    pub fn connectTcp(allocator: std.mem.Allocator, host: []const u8, port: u16, options: ClientOptions) !Client {
        emitConnectEvent(options.observer, .{
            .start = .{
                .host = host,
                .port = port,
                .tls_enabled = options.tls.enabled,
            },
        });

        const stream = if (options.dialer) |dialer|
            dialer(allocator, host, port)
        else if (options.dial_timeout_ms > 0)
            dialTcpWithTimeout(allocator, host, port, options.dial_timeout_ms)
        else
            std.net.tcpConnectToHost(allocator, host, port);
        const connected_stream = stream catch |err| {
            emitConnectEvent(options.observer, .{
                .finish = .{
                    .host = host,
                    .port = port,
                    .tls_enabled = options.tls.enabled,
                    .protocol_version = options.protocol_version,
                    .err = err,
                },
            });
            return err;
        };

        const client = initStreamWithEndpoint(allocator, connected_stream, host, port, options) catch |err| {
            emitConnectEvent(options.observer, .{
                .finish = .{
                    .host = host,
                    .port = port,
                    .tls_enabled = options.tls.enabled,
                    .protocol_version = options.protocol_version,
                    .err = err,
                },
            });
            return err;
        };
        emitConnectEvent(options.observer, .{
            .finish = .{
                .host = host,
                .port = port,
                .tls_enabled = options.tls.enabled,
                .protocol_version = client.protocol_version,
            },
        });
        return client;
    }

    pub fn initStream(allocator: std.mem.Allocator, stream: std.net.Stream, options: ClientOptions) !Client {
        return initStreamWithEndpoint(allocator, stream, "", 0, options);
    }

    fn initStreamWithEndpoint(allocator: std.mem.Allocator, stream: std.net.Stream, host: []const u8, port: u16, options: ClientOptions) !Client {
        _ = port;
        var owned_hello_user: []u8 = &.{};
        errdefer if (owned_hello_user.len > 0) allocator.free(owned_hello_user);

        const hello_user = if (options.ssh_signer) |_| blk: {
            owned_hello_user = try std.fmt.allocPrint(allocator, " SSH KEY AUTHENTICATION {s}", .{options.user});
            break :blk owned_hello_user;
        } else options.user;

        var client = Client{
            .allocator = allocator,
            .stream = stream,
            .stream_closed = false,
            .tls_client = null,
            .tls_ca_bundle = null,
            .protocol_version = options.protocol_version,
            .hello = .{
                .name = options.client_name,
                .major = options.client_version_major,
                .minor = options.client_version_minor,
                .protocol_version = options.protocol_version,
                .database = options.database,
                .user = hello_user,
                .password = options.password,
            },
            .server = undefined,
            .query_defaults = .{
                .protocol_version = options.protocol_version,
                .major = options.client_version_major,
                .minor = options.client_version_minor,
                .patch = options.client_version_patch,
                .interface = .tcp,
                .query_kind = .initial,
                .initial_user = "",
                .initial_query_id = "",
                .initial_address = options.initial_address,
                .os_user = options.os_user,
                .client_hostname = options.client_hostname,
                .client_name = options.client_name,
                .quota_key = options.quota_key,
                .distributed_depth = 0,
            },
            .quota_key = options.quota_key,
            .default_query_compression = if (options.compression == .disabled) .disabled else .enabled,
            .active_query_compression = .disabled,
            .block_compression = options.compression,
            .block_compression_level = options.compression_level,
            .ssh_signer = options.ssh_signer,
            .ssh_auth_user = options.user,
            .owned_hello_user = owned_hello_user,
            .read_timeout_ms = options.read_timeout_ms,
            .write_timeout_ms = options.write_timeout_ms,
            .handshake_timeout_ms = options.handshake_timeout_ms,
            .tls_enabled = options.tls.enabled,
            .tls_server_name = if (options.tls.server_name.len > 0) options.tls.server_name else host,
            .observer = options.observer,
            .server_storage = std.heap.ArenaAllocator.init(allocator),
            .last_exception_storage = std.heap.ArenaAllocator.init(allocator),
            .last_exception = null,
        };
        errdefer client.stream.close();
        errdefer if (client.owned_hello_user.len > 0) allocator.free(client.owned_hello_user);
        errdefer client.deinitTlsTransport();
        errdefer client.server_storage.deinit();
        errdefer client.last_exception_storage.deinit();

        if (options.handshake_timeout_ms > 0) {
            try applySocketTimeouts(client.stream, options.handshake_timeout_ms, options.handshake_timeout_ms);
        }
        if (options.tls.enabled) {
            try client.initTlsTransport(options.tls);
        }

        var encoder = Encoder.init(allocator);
        defer encoder.deinit();

        try client.hello.encodePacket(&encoder);
        try client.transportWriteAll(encoder.bytes());

        var reader = client.transportReader();
        const first_packet = try readServerCodeFromStream(&reader);
        switch (first_packet) {
            .hello => {
                client.server = try ServerHello.decodePayloadFromStream(&reader, client.server_storage.allocator(), client.protocol_version);
            },
            .exception => {
                var arena = std.heap.ArenaAllocator.init(allocator);
                defer arena.deinit();
                _ = try ExceptionChain.decodeFromStream(&reader, arena.allocator());
                return error.ServerException;
            },
            else => return error.UnexpectedPacket,
        }

        if (client.protocol_version > client.server.revision) {
            client.protocol_version = client.server.revision;
        }
        client.query_defaults.protocol_version = client.protocol_version;

        if (client.ssh_signer != null) {
            try client.authenticateSsh();
        }

        if (Feature.quota_key.enabled(client.protocol_version)) {
            encoder.clearRetainingCapacity();
            try encoder.putString(options.quota_key);
            try client.transportWriteAll(encoder.bytes());
        }

        try applySocketTimeouts(client.stream, options.read_timeout_ms, options.write_timeout_ms);

        return client;
    }

    pub fn deinit(self: *Client) void {
        self.closeStream();
        self.deinitTlsTransport();
        self.server_storage.deinit();
        self.last_exception_storage.deinit();
        if (self.owned_hello_user.len > 0) self.allocator.free(self.owned_hello_user);
        self.* = undefined;
    }

    pub fn isClosed(self: *const Client) bool {
        return self.stream_closed;
    }

    pub fn close(self: *Client) void {
        self.closeStream();
    }

    pub fn lastException(self: *const Client) ?ExceptionChain {
        return self.last_exception;
    }

    fn ensureOpen(self: *const Client) !void {
        if (self.stream_closed) return error.ClientClosed;
    }

    fn closeStream(self: *Client) void {
        if (self.stream_closed) return;
        std.posix.shutdown(self.stream.handle, .both) catch {};
        self.stream.close();
        self.stream_closed = true;
    }

    fn clearLastException(self: *Client) void {
        self.last_exception_storage.deinit();
        self.last_exception_storage = std.heap.ArenaAllocator.init(self.allocator);
        self.last_exception = null;
    }

    fn storeLastException(self: *Client, chain: ExceptionChain) !void {
        self.clearLastException();

        const allocator = self.last_exception_storage.allocator();
        const items = try allocator.alloc(Exception, chain.items.len);
        for (chain.items, 0..) |item, idx| {
            items[idx] = .{
                .code = item.code,
                .name = try allocator.dupe(u8, item.name),
                .message = try allocator.dupe(u8, item.message),
                .stack = try allocator.dupe(u8, item.stack),
                .nested = item.nested,
            };
        }
        self.last_exception = .{ .items = items };
    }

    fn cancelAndCloseIgnoringErrors(self: *Client) void {
        if (self.stream_closed) return;
        self.cancel() catch {};
        self.closeStream();
    }

    pub fn sendQuery(self: *Client, query: Query) !void {
        try self.ensureOpen();
        self.clearLastException();
        var prepared = try self.prepareQuery(query);
        defer prepared.deinit(self.allocator);
        try self.sendPreparedQueryPacket(prepared.wire);
        try self.sendEndOfData();
    }

    pub fn Do(self: *Client, ctx: QueryContext, query: *Query) !void {
        try self.ensureOpen();
        self.clearLastException();
        if (query.parameters.len > 0 and !Feature.parameters.enabled(self.protocol_version)) {
            return error.UnsupportedParameters;
        }

        var prepared = try self.prepareQuery(query.*);
        defer prepared.deinit(self.allocator);

        if (query.result_binding) |binding| binding.reset();
        if (query.totals_binding) |binding| binding.reset();
        if (query.extremes_binding) |binding| binding.reset();

        var metrics = QueryMetrics{};
        const observer = effectiveQueryObserver(self.observer, query.observer);
        emitQueryEvent(observer, .{
            .start = .{
                .id = prepared.wire.id,
                .body = prepared.wire.body,
                .compression = prepared.wire.compression,
                .database = self.hello.database,
                .user = self.hello.user,
            },
        });
        emitLog(observer, .debug, "client", "starting query");

        var final_err: ?anyerror = null;
        defer {
            if (query.metrics) |target| target.* = metrics;
            emitQueryEvent(observer, .{
                .finish = .{
                    .id = prepared.wire.id,
                    .metrics = metrics,
                    .err = final_err,
                },
            });
        }

        var input_waiter = if (queryNeedsInputInference(query.*)) InputSchemaWaiter.init(self.allocator) else null;
        defer if (input_waiter) |*waiter| waiter.deinit();

        var runtime = DoRuntime{
            .client = self,
            .ctx = ctx,
            .query = query,
            .input_waiter = if (input_waiter) |*waiter| waiter else null,
            .metrics = &metrics,
            .observer = observer,
        };

        var receive_state = DoReceiveThreadState{ .runtime = &runtime };
        const receive_thread = try std.Thread.spawn(.{}, runDoReceiveThread, .{&receive_state});

        var cancel_state = DoCancelThreadState{ .runtime = &runtime };
        const cancel_thread = std.Thread.spawn(.{}, runDoCancelThread, .{&cancel_state}) catch |err| {
            self.closeStream();
            receive_thread.join();
            final_err = err;
            return err;
        };

        var sender_err: ?anyerror = null;
        doSender(&runtime, prepared.wire) catch |err| {
            sender_err = err;
            if (input_waiter) |*waiter| waiter.fail(err);
        };
        if (sender_err != null) {
            if (runtime.queryStarted()) {
                runtime.client.cancelAndCloseIgnoringErrors();
            } else {
                runtime.client.closeStream();
            }
        }
        runtime.finishSender(sender_err);

        receive_thread.join();
        cancel_thread.join();

        if (ctx.canceled() and !runtime.got_exception) {
            final_err = error.Canceled;
            return error.Canceled;
        }
        if (runtime.sender_err) |err| {
            final_err = err;
            return err;
        }
        if (runtime.receiver_err) |err| {
            final_err = err;
            return err;
        }
    }

    pub fn newQuery(self: *const Client, body: []const u8) Query {
        return .{
            .body = body,
            .compression = self.default_query_compression,
            .info = self.queryInfo(""),
        };
    }

    pub fn queryInfo(self: *const Client, query_id: []const u8) ClientInfo {
        var info = self.query_defaults;
        info.initial_query_id = query_id;
        return info;
    }

    fn prepareQuery(self: *const Client, query: Query) !PreparedQuery {
        var prepared = PreparedQuery{
            .wire = query,
        };
        errdefer prepared.deinit(self.allocator);

        if (query.query_id.len != 0) {
            prepared.wire.id = query.query_id;
        }
        if (prepared.wire.id.len == 0) {
            prepared.owned_query_id = try generateQueryId(self.allocator);
            prepared.wire.id = prepared.owned_query_id.?;
        }

        if (query.quota_key.len != 0) {
            prepared.wire.info.quota_key = query.quota_key;
        }
        if (query.initial_user.len != 0) {
            prepared.wire.info.initial_user = query.initial_user;
        }
        prepared.wire.info.initial_query_id = prepared.wire.id;

        prepared.owned_settings = try self.effectiveQuerySettings(prepared.wire);
        if (prepared.owned_settings) |settings| {
            prepared.wire.settings = settings;
        }

        return prepared;
    }

    fn sendPreparedQueryPacket(self: *Client, query: Query) !void {
        var encoder = Encoder.init(self.allocator);
        defer encoder.deinit();

        self.active_query_compression = query.compression;
        try query.encodePacket(&encoder, self.protocol_version);
        try self.transportWriteAll(encoder.bytes());
    }

    fn sendExternalData(self: *Client, query: Query) !void {
        if (query.external_data.len > 0) {
            try self.sendDataPacket(.{
                .temp_table = if (query.external_table.len == 0) "_data" else query.external_table,
                .block = .{
                    .info = .{ .bucket_num = -1 },
                    .columns = query.external_data,
                    .rows = 0,
                },
            });
        }
        try self.sendEndOfData();
    }

    fn doSender(runtime: *DoRuntime, prepared_wire: Query) !void {
        try runtime.ctx.check();
        try runtime.client.sendPreparedQueryPacket(prepared_wire);
        runtime.markQueryStarted();
        try runtime.client.sendExternalData(runtime.query.*);
        if (runtime.query.external_data.len > 0) {
            runtime.metrics.blocks_sent += 1;
        }
        if (runtime.input_waiter) |waiter| {
            var schema = try waiter.wait();
            defer schema.deinit(runtime.client.allocator);
            try inferInputColumns(runtime.client.allocator, runtime.query, &schema);
        }
        try runtime.client.sendInput(runtime.query, runtime.ctx, runtime.metrics);
    }

    fn sendInput(self: *Client, query: *Query, ctx: QueryContext, metrics: *QueryMetrics) !void {
        if (query.input.len == 0 and query.on_input == null) return;

        if (query.on_input != null and try inputRowCount(query.input) == 0) {
            query.on_input.?(ctx, query) catch |err| {
                if (err != error.EndOfInput) return err;
            };
        }

        while (true) {
            try ctx.check();

            const rows = try inputRowCount(query.input);
            if (rows > 0) {
                try self.sendDataPacket(.{
                    .block = .{
                        .info = .{ .bucket_num = -1 },
                        .columns = query.input,
                        .rows = rows,
                    },
                });
                metrics.blocks_sent += 1;
            } else if (query.on_input == null) {
                return error.EmptyInput;
            }

            const on_input = query.on_input orelse break;
            var stop = false;
            on_input(ctx, query) catch |err| {
                if (err != error.EndOfInput) return err;
                if (try inputRowCount(query.input) > 0) {
                    query.on_input = null;
                    return;
                }
                stop = true;
            };
            if (stop) break;
        }

        try self.sendEndOfData();
    }

    fn handleResultPacket(
        self: *Client,
        ctx: QueryContext,
        data: *const DecodedDataPacket,
        buffer: ?*BlockBuffer,
        binding: ?*ResultBinding,
        callback: ?OnResultFn,
        saw_non_empty_result: ?*bool,
    ) !void {
        _ = self;
        if (data.block.isEnd()) return;
        if (callback) |f| {
            try f(ctx, &data.block);
        }
        if (buffer) |target| {
            try target.appendClone(data.block);
        }
        if (binding) |target| {
            try target.bindBlock(&data.block);
        }
        if (callback == null and buffer == null and binding == null and saw_non_empty_result != null and data.block.rows > 0) {
            if (saw_non_empty_result.?.*) return error.MissingResultHandler;
            saw_non_empty_result.?.* = true;
        }
    }

    fn receiveQueryPackets(self: *Client, query: Query, ctx: QueryContext, input_waiter: ?*InputSchemaWaiter, metrics: *QueryMetrics, observer: ?Observer) !void {
        var saw_non_empty_result = false;
        while (true) {
            var packet = try self.readServerPacket();
            defer packet.deinit();

            switch (packet.value) {
                .end_of_stream => return,
                .exception => |exception| {
                    emitQueryEvent(observer, .{ .exception = exception });
                    return error.ServerException;
                },
                .progress => |progress| {
                    metrics.progress_rows += progress.rows;
                    metrics.progress_bytes += progress.bytes;
                    metrics.progress_total_rows += progress.total_rows;
                    metrics.wrote_rows += progress.wrote_rows;
                    metrics.wrote_bytes += progress.wrote_bytes;
                    emitQueryEvent(observer, .{ .progress = progress });
                    if (query.on_progress) |f| try f(ctx, progress);
                },
                .profile => |profile| {
                    emitQueryEvent(observer, .{ .profile = profile });
                    if (query.on_profile) |f| try f(ctx, profile);
                },
                .table_columns => |table_columns| {
                    if (query.on_table_columns) |f| try f(ctx, table_columns);
                },
                .log => |*data| {
                    if (!data.block.isEnd()) {
                        if (query.on_logs) |f| try f(ctx, data);
                        try self.dispatchLogs(ctx, query, data);
                    }
                },
                .profile_events => |*data| {
                    if (!data.block.isEnd()) {
                        if (query.on_profile_events) |f| try f(ctx, data);
                        try self.dispatchProfileEvents(ctx, query, data);
                    }
                },
                .data => |*data| {
                    if (shouldCaptureInputSchema(input_waiter, data)) {
                        try input_waiter.?.signal(&data.block);
                        continue;
                    }
                    if (!data.block.isEnd()) {
                        metrics.blocks_received += 1;
                        metrics.rows_received += @intCast(data.block.rows);
                        metrics.columns_received += @intCast(data.block.columns.len);
                    }
                    try self.handleResultPacket(ctx, data, query.result, query.result_binding, query.on_result, &saw_non_empty_result);
                },
                .totals => |*data| {
                    if (!data.block.isEnd()) metrics.totals_blocks_received += 1;
                    try self.handleResultPacket(ctx, data, query.totals, query.totals_binding, query.on_totals orelse query.on_result, null);
                },
                .extremes => |*data| {
                    if (!data.block.isEnd()) metrics.extremes_blocks_received += 1;
                    try self.handleResultPacket(ctx, data, query.extremes, query.extremes_binding, query.on_extremes, null);
                },
                else => return error.UnexpectedPacket,
            }
        }
    }

    pub fn sendDataPacket(self: *Client, packet: DataPacket) !void {
        try self.ensureOpen();
        var encoder = Encoder.init(self.allocator);
        defer encoder.deinit();
        try ClientCode.data.encode(&encoder);
        if (Feature.temp_tables.enabled(self.protocol_version)) {
            try encoder.putString(packet.temp_table);
        }

        if (self.active_query_compression == .enabled and self.block_compression != .disabled) {
            var block_encoder = Encoder.init(self.allocator);
            defer block_encoder.deinit();
            try packet.block.encode(&block_encoder, self.protocol_version);

            const compressed = try ch_compress.compressFrame(
                self.allocator,
                block_encoder.bytes(),
                switch (self.block_compression) {
                    .disabled => unreachable,
                    .lz4 => .lz4,
                    .zstd => .zstd,
                    .none => .none,
                    .lz4hc => .lz4hc,
                },
                self.block_compression_level,
            );
            defer self.allocator.free(compressed);
            try encoder.putRaw(compressed);
        } else {
            try packet.block.encode(&encoder, self.protocol_version);
        }
        try self.transportWriteAll(encoder.bytes());
    }

    pub fn sendTablesStatusRequest(self: *Client, request: TablesStatusRequest) !void {
        try self.ensureOpen();
        var encoder = Encoder.init(self.allocator);
        defer encoder.deinit();
        try request.encodePacket(&encoder, self.protocol_version);
        try self.transportWriteAll(encoder.bytes());
    }

    pub fn requestTablesStatus(self: *Client, request: TablesStatusRequest) !OwnedServerPacket {
        self.clearLastException();
        try self.sendTablesStatusRequest(request);
        var packet = try self.readServerPacket();
        switch (packet.value) {
            .tables_status, .exception => return packet,
            else => {
                packet.deinit();
                return error.UnexpectedPacket;
            },
        }
    }

    pub fn sendSshChallengeRequest(self: *Client) !void {
        try self.ensureOpen();
        var encoder = Encoder.init(self.allocator);
        defer encoder.deinit();
        try (SSHChallengeRequest{}).encodePacket(&encoder);
        try self.transportWriteAll(encoder.bytes());
    }

    pub fn sendSshChallengeResponse(self: *Client, signature: []const u8) !void {
        try self.ensureOpen();
        var encoder = Encoder.init(self.allocator);
        defer encoder.deinit();
        try (SSHChallengeResponse{ .signature = signature }).encodePacket(&encoder);
        try self.transportWriteAll(encoder.bytes());
    }

    pub fn authenticateSsh(self: *Client) !void {
        self.clearLastException();
        const signer = self.ssh_signer orelse return error.MissingSshSigner;

        try self.sendSshChallengeRequest();
        var packet = try self.readServerPacket();
        defer packet.deinit();

        const challenge = switch (packet.value) {
            .ssh_challenge => |value| value,
            .exception => return error.ServerException,
            else => return error.UnexpectedPacket,
        };

        const sign_message = try std.fmt.allocPrint(self.allocator, "{d}{s}{s}{s}", .{
            self.protocol_version,
            self.hello.database,
            self.ssh_auth_user,
            challenge.challenge,
        });
        defer self.allocator.free(sign_message);

        var raw_signature = std.ArrayList(u8).init(self.allocator);
        defer raw_signature.deinit();
        try signer(sign_message, challenge.challenge, &raw_signature);

        const encoded_len = std.base64.standard.Encoder.calcSize(raw_signature.items.len);
        const encoded_signature = try self.allocator.alloc(u8, encoded_len);
        defer self.allocator.free(encoded_signature);
        _ = std.base64.standard.Encoder.encode(encoded_signature, raw_signature.items);

        try self.sendSshChallengeResponse(encoded_signature);
    }

    pub fn sendEndOfData(self: *Client) !void {
        const empty_columns = [_]Column{};
        try self.sendDataPacket(.{
            .temp_table = "",
            .block = .{
                .columns = &empty_columns,
                .rows = 0,
            },
        });
    }

    pub fn ping(self: *Client) !void {
        try self.ensureOpen();
        self.clearLastException();
        var encoder = Encoder.init(self.allocator);
        defer encoder.deinit();
        try encodePingPacket(&encoder);
        try self.transportWriteAll(encoder.bytes());

        var packet = try self.readServerPacket();
        defer packet.deinit();

        switch (packet.value) {
            .pong => {},
            .exception => return error.ServerException,
            else => return error.UnexpectedPacket,
        }
    }

    pub fn cancel(self: *Client) !void {
        try self.ensureOpen();
        var encoder = Encoder.init(self.allocator);
        defer encoder.deinit();
        try encodeCancelPacket(&encoder);
        try self.transportWriteAll(encoder.bytes());
    }

    pub fn readServerPacket(self: *Client) !OwnedServerPacket {
        try self.ensureOpen();
        var reader = self.transportReader();
        const code = try readServerCodeFromStream(&reader);

        switch (code) {
            .pong => return .{ .value = .{ .pong = {} } },
            .end_of_stream => return .{ .value = .{ .end_of_stream = {} } },
            .progress => return .{ .value = .{ .progress = try Progress.decodePayloadFromStream(&reader, self.protocol_version) } },
            .profile => return .{ .value = .{ .profile = try Profile.decodePayloadFromStream(&reader) } },
            else => {
                var arena = std.heap.ArenaAllocator.init(self.allocator);
                errdefer arena.deinit();
                const arena_allocator = arena.allocator();

                const value: ServerPacket = switch (code) {
                    .hello => .{ .hello = try ServerHello.decodePayloadFromStream(&reader, arena_allocator, self.protocol_version) },
                    .data => .{ .data = try self.decodeDataPacketFromStream(&reader, arena_allocator, true) },
                    .totals => .{ .totals = try self.decodeDataPacketFromStream(&reader, arena_allocator, true) },
                    .extremes => .{ .extremes = try self.decodeDataPacketFromStream(&reader, arena_allocator, true) },
                    .log => .{ .log = try self.decodeDataPacketFromStream(&reader, arena_allocator, true) },
                    .profile_events => .{ .profile_events = try self.decodeDataPacketFromStream(&reader, arena_allocator, true) },
                    .exception => .{ .exception = try ExceptionChain.decodeFromStream(&reader, arena_allocator) },
                    .table_columns => .{ .table_columns = try TableColumns.decodePayloadFromStream(&reader, arena_allocator) },
                    .tables_status => .{ .tables_status = try TablesStatusResponse.decodePayloadFromStream(&reader, arena_allocator, self.protocol_version) },
                    .part_uuids => .{ .part_uuids = try PartUUIDs.decodePayloadFromStream(&reader, arena_allocator) },
                    .read_task_request => .{ .read_task_request = ReadTaskRequest.decodePayloadFromStream(&reader) },
                    .ssh_challenge => .{ .ssh_challenge = try SSHChallenge.decodePayloadFromStream(&reader, arena_allocator) },
                    else => return error.UnsupportedServerPacket,
                };

                switch (value) {
                    .exception => |exception| try self.storeLastException(exception),
                    else => {},
                }

                return .{
                    .arena = arena,
                    .value = value,
                };
            },
        }
    }

    fn decodeDataPacketFromStream(self: *Client, reader: *StreamReader, allocator: std.mem.Allocator, compressible: bool) !DecodedDataPacket {
        var packet = DecodedDataPacket{};
        if (Feature.temp_tables.enabled(self.protocol_version)) {
            packet.temp_table = try reader.readStringAlloc(allocator);
        }

        if (compressible and self.active_query_compression == .enabled) {
            packet.block = try decodeAdaptiveDataBlockFromStream(reader, allocator, self.protocol_version);
            return packet;
        }

        packet.block = try DecodedBlock.decodeFromStream(reader, allocator, self.protocol_version);
        return packet;
    }

    fn effectiveQuerySettings(self: *const Client, query: Query) !?[]Setting {
        const method = self.compressionMethodSettingValue(query) orelse return null;
        if (findSettingValue(query.settings, "network_compression_method") != null) return null;

        const settings = try self.allocator.alloc(Setting, query.settings.len + 1);
        errdefer self.allocator.free(settings);
        if (query.settings.len > 0) {
            @memcpy(settings[0..query.settings.len], query.settings);
        }
        settings[query.settings.len] = .{
            .key = "network_compression_method",
            .value = method,
            .important = true,
        };
        return settings;
    }

    fn compressionMethodSettingValue(self: *const Client, query: Query) ?[]const u8 {
        if (query.compression != .enabled) return null;
        return switch (self.block_compression) {
            .disabled => null,
            .lz4 => "LZ4",
            .zstd => "ZSTD",
            .none => "NONE",
            .lz4hc => "LZ4HC",
        };
    }

    fn dispatchLogs(self: *Client, ctx: QueryContext, query: Query, data: *const DecodedDataPacket) !void {
        if (query.on_logs_batch == null and query.on_log == null) return;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const logs = try decodeLogsBatch(arena.allocator(), &data.block);
        if (query.on_logs_batch) |f| try f(ctx, logs);
        if (query.on_log) |f| {
            for (logs) |item| {
                try f(ctx, item);
            }
        }
    }

    fn dispatchProfileEvents(self: *Client, ctx: QueryContext, query: Query, data: *const DecodedDataPacket) !void {
        if (query.on_profile_events_batch == null and query.on_profile_event == null) return;

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();

        const events = try decodeProfileEventsBatch(arena.allocator(), &data.block);
        if (query.on_profile_events_batch) |f| try f(ctx, events);
        if (query.on_profile_event) |f| {
            for (events) |item| {
                try f(ctx, item);
            }
        }
    }

    fn initTlsTransport(self: *Client, options: TlsOptions) !void {
        const tls_client = try self.allocator.create(std.crypto.tls.Client);
        errdefer self.allocator.destroy(tls_client);

        switch (options.ca_mode) {
            .system => {
                var bundle = std.crypto.Certificate.Bundle{};
                try bundle.rescan(self.allocator);
                errdefer bundle.deinit(self.allocator);

                tls_client.* = try std.crypto.tls.Client.init(self.stream, .{
                    .host = if (self.tls_server_name.len == 0)
                        .no_verification
                    else
                        .{ .explicit = self.tls_server_name },
                    .ca = .{ .bundle = bundle },
                });
                tls_client.allow_truncation_attacks = options.allow_truncation_attacks;
                self.tls_ca_bundle = bundle;
            },
            .self_signed => {
                tls_client.* = try std.crypto.tls.Client.init(self.stream, .{
                    .host = if (self.tls_server_name.len == 0)
                        .no_verification
                    else
                        .{ .explicit = self.tls_server_name },
                    .ca = .self_signed,
                });
                tls_client.allow_truncation_attacks = options.allow_truncation_attacks;
            },
            .no_verification => {
                tls_client.* = try std.crypto.tls.Client.init(self.stream, .{
                    .host = .no_verification,
                    .ca = .no_verification,
                });
                tls_client.allow_truncation_attacks = options.allow_truncation_attacks;
            },
        }

        self.tls_client = tls_client;
    }

    fn deinitTlsTransport(self: *Client) void {
        if (self.tls_client) |tls_client| {
            if (tls_client.ssl_key_log) |key_log| {
                key_log.file.close();
            }
            self.allocator.destroy(tls_client);
            self.tls_client = null;
        }
        if (self.tls_ca_bundle) |*bundle| {
            bundle.deinit(self.allocator);
            self.tls_ca_bundle = null;
        }
    }

    fn transportReader(self: *Client) StreamReader {
        return StreamReader.initWithReader(self, clientTransportReadAdapter);
    }

    fn transportRead(self: *Client, buffer: []u8) !usize {
        if (self.tls_client) |tls_client| {
            return tls_client.read(self.stream, buffer);
        }
        return self.stream.read(buffer);
    }

    fn transportWriteAll(self: *Client, bytes: []const u8) !void {
        if (self.tls_client) |tls_client| {
            try tls_client.writeAll(self.stream, bytes);
            return;
        }
        try self.stream.writeAll(bytes);
    }
};

fn clientTransportReadAdapter(user_data: ?*anyopaque, buffer: []u8) anyerror!usize {
    const client: *Client = @ptrCast(@alignCast(user_data.?));
    return client.transportRead(buffer);
}

fn emitConnectEvent(observer: Observer, event: ConnectObserveEvent) void {
    if (observer.on_connect) |f| {
        f(event, observer.user_data);
    }
}

fn emitQueryEvent(observer: ?Observer, event: QueryObserveEvent) void {
    const value = observer orelse return;
    if (value.on_query) |f| {
        f(event, value.user_data);
    }
}

fn emitLog(observer: ?Observer, level: LogLevel, scope: []const u8, message: []const u8) void {
    const value = observer orelse return;
    if (value.on_log) |f| {
        f(level, scope, message, value.user_data);
    }
}

fn effectiveQueryObserver(client_observer: Observer, query_observer: ?Observer) ?Observer {
    if (query_observer) |value| return if (value.enabled()) value else null;
    return if (client_observer.enabled()) client_observer else null;
}

fn applySocketTimeouts(stream: std.net.Stream, read_timeout_ms: u64, write_timeout_ms: u64) !void {
    const read_tv = msToTimeval(read_timeout_ms);
    const write_tv = msToTimeval(write_timeout_ms);
    try std.posix.setsockopt(stream.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, &std.mem.toBytes(read_tv));
    try std.posix.setsockopt(stream.handle, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, &std.mem.toBytes(write_tv));
}

fn msToTimeval(value_ms: u64) std.posix.timeval {
    return .{
        .sec = @intCast(value_ms / std.time.ms_per_s),
        .usec = @intCast((value_ms % std.time.ms_per_s) * std.time.us_per_ms),
    };
}

fn remainingDialTimeoutMs(deadline_ns: i128) ?i32 {
    const now = std.time.nanoTimestamp();
    if (now >= deadline_ns) return null;
    const remaining_ns = @as(u64, @intCast(deadline_ns - now));
    const remaining_ms = std.math.divCeil(u64, remaining_ns, std.time.ns_per_ms) catch unreachable;
    const clamped = @min(remaining_ms, @as(u64, std.math.maxInt(i32)));
    return @intCast(clamped);
}

fn dialTcpWithTimeout(allocator: std.mem.Allocator, host: []const u8, port: u16, timeout_ms: u64) !std.net.Stream {
    if (builtin.os.tag == .windows) {
        return std.net.tcpConnectToHost(allocator, host, port);
    }

    const list = try std.net.getAddressList(allocator, host, port);
    defer list.deinit();
    if (list.addrs.len == 0) return error.UnknownHostName;

    const deadline_ns = std.time.nanoTimestamp() + @as(i128, @intCast(timeout_ms)) * std.time.ns_per_ms;
    var last_err: ?anyerror = null;

    for (list.addrs) |addr| {
        const stream = dialTcpAddressWithDeadline(addr, deadline_ns) catch |err| switch (err) {
            error.ConnectionRefused,
            error.NetworkUnreachable,
            error.ConnectionTimedOut,
            error.AddressFamilyNotSupported,
            error.DialTimeout,
            => {
                last_err = err;
                continue;
            },
            else => return err,
        };
        return stream;
    }

    return last_err orelse error.DialTimeout;
}

fn dialTcpAddressWithDeadline(address: std.net.Address, deadline_ns: i128) !std.net.Stream {
    const sockfd = try std.posix.socket(
        address.any.family,
        std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC | std.posix.SOCK.NONBLOCK,
        std.posix.IPPROTO.TCP,
    );
    errdefer {
        var stream = std.net.Stream{ .handle = sockfd };
        stream.close();
    }

    std.posix.connect(sockfd, &address.any, address.getOsSockLen()) catch |err| switch (err) {
        error.WouldBlock, error.ConnectionPending => {},
        else => return err,
    };

    var poll_fds = [_]std.posix.pollfd{.{
        .fd = sockfd,
        .events = std.posix.POLL.OUT,
        .revents = 0,
    }};

    while (true) {
        const timeout = remainingDialTimeoutMs(deadline_ns) orelse return error.DialTimeout;
        const ready = try std.posix.poll(&poll_fds, timeout);
        if (ready == 0) return error.DialTimeout;
        try std.posix.getsockoptError(sockfd);
        try clearSocketNonblocking(sockfd);
        return .{ .handle = sockfd };
    }
}

fn clearSocketNonblocking(sockfd: std.posix.socket_t) !void {
    var fl_flags = try std.posix.fcntl(sockfd, std.posix.F.GETFL, 0);
    fl_flags &= ~@as(usize, 1 << @bitOffsetOf(std.posix.O, "NONBLOCK"));
    _ = try std.posix.fcntl(sockfd, std.posix.F.SETFL, fl_flags);
}

fn runDoReceiveThread(state: *DoReceiveThreadState) void {
    const runtime = state.runtime;
    var err: ?anyerror = null;
    runtime.client.receiveQueryPackets(runtime.query.*, runtime.ctx, runtime.input_waiter, runtime.metrics, runtime.observer) catch |value| {
        err = value;
    };
    if (runtime.input_waiter) |waiter| {
        if (err) |value| {
            waiter.fail(value);
        } else if (!waiter.isResolved()) {
            waiter.fail(error.InputSchemaUnavailable);
        }
    }
    runtime.finishReceiver(err);
}

fn runDoCancelThread(state: *DoCancelThreadState) void {
    const runtime = state.runtime;
    while (true) {
        if (runtime.isDone()) return;
        if (runtime.shouldCancel()) {
            runtime.client.cancelAndCloseIgnoringErrors();
            return;
        }
        std.time.sleep(cancel_poll_interval_ns);
    }
}

pub fn encodePingPacket(encoder: *Encoder) !void {
    try ClientCode.ping.encode(encoder);
}

pub fn encodeCancelPacket(encoder: *Encoder) !void {
    try ClientCode.cancel.encode(encoder);
}

fn inputRowCount(input: []const Column) !usize {
    return try (DataBlock{
        .columns = input,
        .rows = 0,
    }).effectiveRows();
}

fn queryNeedsInputInference(query: Query) bool {
    if (query.input.len == 0) return false;
    for (query.input) |column| {
        if (columnNeedsInference(column)) return true;
    }
    return false;
}

fn shouldCaptureInputSchema(input_waiter: ?*InputSchemaWaiter, data: *const DecodedDataPacket) bool {
    const waiter = input_waiter orelse return false;
    if (data.block.isEnd()) return false;
    if (data.block.rows != 0 or data.block.columns.len == 0) return false;
    if (waiter.isResolved()) return false;
    return true;
}

fn columnNeedsInference(column: Column) bool {
    return switch (column) {
        .string => |value| value.name.len == 0,
        .var_bytes => |value| value.name.len == 0 or value.type_name.len == 0,
        .fixed_bytes => |value| value.name.len == 0 or value.type_name.len == 0 or value.width == 0,
        .encoded => |value| value.name.len == 0 or value.type_name.len == 0,
        .int8 => |value| value.name.len == 0,
        .int64 => |value| value.name.len == 0,
        .uint64 => |value| value.name.len == 0,
    };
}

fn inferInputColumns(allocator: std.mem.Allocator, query: *Query, schema: *const DecodedBlock) !void {
    const input_columns = @constCast(query.input);
    for (input_columns, 0..) |*input_column, idx| {
        const schema_column = if (input_column.name().len != 0)
            findSchemaColumnByName(schema, input_column.name()) orelse return error.MissingInputSchemaColumn
        else if (idx < schema.columns.len)
            schema.columns[idx]
        else
            return error.MissingInputSchemaColumn;

        try inferInputColumn(allocator, input_column, schema_column);
    }
}

fn findSchemaColumnByName(schema: *const DecodedBlock, name: []const u8) ?Column {
    for (schema.columns) |column| {
        if (std.mem.eql(u8, column.name(), name)) return column;
    }
    return null;
}

fn inferInputColumn(allocator: std.mem.Allocator, input_column: *Column, schema_column: Column) !void {
    switch (input_column.*) {
        .string => |*value| {
            if (value.name.len == 0) try setStringColumnName(allocator, value, schema_column.name());
            if (!std.mem.eql(u8, schema_column.typeName(), "String")) return error.InputInferenceTypeMismatch;
        },
        .var_bytes => |*value| {
            if (value.name.len == 0) try setVarBytesColumnName(allocator, value, schema_column.name());
            if (value.type_name.len == 0) {
                try setVarBytesColumnTypeName(allocator, value, schema_column.typeName());
            } else if (!std.mem.eql(u8, value.type_name, schema_column.typeName())) {
                return error.InputInferenceTypeMismatch;
            }
        },
        .fixed_bytes => |*value| {
            if (value.name.len == 0) try setFixedBytesColumnName(allocator, value, schema_column.name());
            const server_type_name = schema_column.typeName();
            const server_width = fixedWidthForType(server_type_name) orelse return error.InputInferenceTypeMismatch;
            if (value.width == 0) {
                value.width = server_width;
            } else if (value.width != server_width) {
                return error.InputInferenceTypeMismatch;
            }
            if (value.type_name.len == 0) {
                try setFixedBytesColumnTypeName(allocator, value, server_type_name);
            } else if (!std.mem.eql(u8, value.type_name, server_type_name)) {
                return error.InputInferenceTypeMismatch;
            }
        },
        .encoded => |*value| {
            if (value.name.len == 0) try setEncodedColumnName(allocator, value, schema_column.name());
            if (value.type_name.len == 0) {
                try setEncodedColumnTypeName(allocator, value, schema_column.typeName());
            } else if (!std.mem.eql(u8, value.type_name, schema_column.typeName())) {
                return error.InputInferenceTypeMismatch;
            }
        },
        .int8 => |*value| {
            if (value.name.len == 0) try setInt8ColumnName(allocator, value, schema_column.name());
            if (!std.mem.eql(u8, schema_column.typeName(), "Int8")) return error.InputInferenceTypeMismatch;
        },
        .int64 => |*value| {
            if (value.name.len == 0) try setInt64ColumnName(allocator, value, schema_column.name());
            if (!std.mem.eql(u8, schema_column.typeName(), "Int64")) return error.InputInferenceTypeMismatch;
        },
        .uint64 => |*value| {
            if (value.name.len == 0) try setUInt64ColumnName(allocator, value, schema_column.name());
            if (!std.mem.eql(u8, schema_column.typeName(), "UInt64")) return error.InputInferenceTypeMismatch;
        },
    }
}

fn setStringColumnName(allocator: std.mem.Allocator, column: *StringColumn, name: []const u8) !void {
    if (column.owns_name and column.name.len > 0) allocator.free(column.name);
    column.name = try allocator.dupe(u8, name);
    column.owns_name = true;
}

fn setVarBytesColumnName(allocator: std.mem.Allocator, column: *VarBytesColumn, name: []const u8) !void {
    if (column.owns_name and column.name.len > 0) allocator.free(column.name);
    column.name = try allocator.dupe(u8, name);
    column.owns_name = true;
}

fn setVarBytesColumnTypeName(allocator: std.mem.Allocator, column: *VarBytesColumn, type_name: []const u8) !void {
    if (column.owns_type_name and column.type_name.len > 0) allocator.free(column.type_name);
    column.type_name = try allocator.dupe(u8, type_name);
    column.owns_type_name = true;
}

fn setFixedBytesColumnName(allocator: std.mem.Allocator, column: *FixedBytesColumn, name: []const u8) !void {
    if (column.owns_name and column.name.len > 0) allocator.free(column.name);
    column.name = try allocator.dupe(u8, name);
    column.owns_name = true;
}

fn setFixedBytesColumnTypeName(allocator: std.mem.Allocator, column: *FixedBytesColumn, type_name: []const u8) !void {
    if (column.owns_type_name and column.type_name.len > 0) allocator.free(column.type_name);
    column.type_name = try allocator.dupe(u8, type_name);
    column.owns_type_name = true;
}

fn setEncodedColumnName(allocator: std.mem.Allocator, column: *EncodedColumn, name: []const u8) !void {
    if (column.owns_name and column.name.len > 0) allocator.free(column.name);
    column.name = try allocator.dupe(u8, name);
    column.owns_name = true;
}

fn setEncodedColumnTypeName(allocator: std.mem.Allocator, column: *EncodedColumn, type_name: []const u8) !void {
    if (column.owns_type_name and column.type_name.len > 0) allocator.free(column.type_name);
    column.type_name = try allocator.dupe(u8, type_name);
    column.owns_type_name = true;
}

fn setInt8ColumnName(allocator: std.mem.Allocator, column: *Int8Column, name: []const u8) !void {
    if (column.owns_name and column.name.len > 0) allocator.free(column.name);
    column.name = try allocator.dupe(u8, name);
    column.owns_name = true;
}

fn setInt64ColumnName(allocator: std.mem.Allocator, column: *Int64Column, name: []const u8) !void {
    if (column.owns_name and column.name.len > 0) allocator.free(column.name);
    column.name = try allocator.dupe(u8, name);
    column.owns_name = true;
}

fn setUInt64ColumnName(allocator: std.mem.Allocator, column: *UInt64Column, name: []const u8) !void {
    if (column.owns_name and column.name.len > 0) allocator.free(column.name);
    column.name = try allocator.dupe(u8, name);
    column.owns_name = true;
}

fn generateQueryId(allocator: std.mem.Allocator) ![]u8 {
    var bytes: [16]u8 = undefined;
    std.crypto.random.bytes(&bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    var out: [36]u8 = undefined;
    const hex = "0123456789abcdef";
    const groups = [_]usize{ 4, 2, 2, 2, 6 };
    var byte_index: usize = 0;
    var out_index: usize = 0;
    for (groups, 0..) |group_len, group_idx| {
        for (0..group_len) |_| {
            const byte = bytes[byte_index];
            out[out_index] = hex[byte >> 4];
            out[out_index + 1] = hex[byte & 0x0f];
            out_index += 2;
            byte_index += 1;
        }
        if (group_idx + 1 != groups.len) {
            out[out_index] = '-';
            out_index += 1;
        }
    }

    return allocator.dupe(u8, &out);
}

fn blockColumnByName(block: *const DecodedBlock, name: []const u8) ?Column {
    for (block.columns) |column| {
        if (std.mem.eql(u8, column.name(), name)) return column;
    }
    return null;
}

fn resolveResultBindingColumn(block: *const DecodedBlock, binding: ResultBindingColumn) !usize {
    if (binding.index) |idx| {
        if (idx >= block.columns.len) return error.ResultBindingColumnMissing;
        if (binding.name.len > 0 and !std.mem.eql(u8, block.columns[idx].name(), binding.name)) {
            return error.ResultBindingColumnMismatch;
        }
        return idx;
    }
    if (binding.name.len == 0) return error.ResultBindingColumnMissing;
    for (block.columns, 0..) |column, idx| {
        if (std.mem.eql(u8, column.name(), binding.name)) return idx;
    }
    return error.ResultBindingColumnMissing;
}

fn ownedValueFromFixedBytes(allocator: std.mem.Allocator, type_name: []const u8, bytes: []const u8) !OwnedValue {
    const base = typeBaseName(type_name);
    if (std.mem.eql(u8, base, "Bool")) {
        return .{ .bool = switch (bytes[0]) {
            0 => false,
            1 => true,
            else => return error.InvalidBool,
        } };
    }

    return .{ .fixed = .{
        .type_name = try allocator.dupe(u8, type_name),
        .bytes = try allocator.dupe(u8, bytes),
    } };
}

fn unsignedColumnValueAt(column: Column, index: usize) !u64 {
    return switch (column) {
        .uint64 => |value| value.values[index],
        else => blk: {
            const fixed = try column.asFixed();
            const base = typeBaseName(fixed.type_name);
            const row = fixed.row(index);
            if (std.mem.eql(u8, base, "UInt8")) break :blk row[0];
            if (std.mem.eql(u8, base, "UInt16")) break :blk std.mem.readInt(u16, row[0..2], .little);
            if (std.mem.eql(u8, base, "UInt32")) break :blk std.mem.readInt(u32, row[0..4], .little);
            if (std.mem.eql(u8, base, "UInt64")) break :blk std.mem.readInt(u64, row[0..8], .little);
            return error.ResultBindingTypeMismatch;
        },
    };
}

fn ownedValueFromColumnRow(allocator: std.mem.Allocator, column: Column, row: usize) !OwnedValue {
    return switch (column) {
        .string => |value| .{ .string = try allocator.dupe(u8, value.values[row]) },
        .var_bytes => |value| .{ .bytes = try allocator.dupe(u8, value.values[row]) },
        .int8 => |value| .{ .int8 = value.values[row] },
        .int64 => |value| .{ .int64 = value.values[row] },
        .uint64 => |value| .{ .uint64 = value.values[row] },
        .fixed_bytes => |value| try ownedValueFromFixedBytes(allocator, value.type_name, value.row(row)),
        .encoded => |_| blk: {
            if (unwrapTypeArgument(column.typeName(), "Nullable") != null) {
                var view = try column.asNullable(allocator);
                defer view.deinit(allocator);
                if (view.isNull(row)) break :blk .null;
                break :blk try ownedValueFromColumnRow(allocator, view.values, row);
            }
            if (unwrapTypeArgument(column.typeName(), "Array") != null) {
                var view = try column.asArray(allocator);
                defer view.deinit(allocator);
                const range = view.rowRange(row);
                const values = try allocator.alloc(OwnedValue, range.end - range.start);
                var filled: usize = 0;
                errdefer {
                    for (values[0..filled]) |*item| {
                        item.deinit(allocator);
                    }
                    allocator.free(values);
                }
                for (range.start..range.end) |idx| {
                    values[filled] = try ownedValueFromColumnRow(allocator, view.values, idx);
                    filled += 1;
                }
                break :blk .{ .array = values };
            }
            if (unwrapTypeArgument(column.typeName(), "Map") != null) {
                var view = try column.asMap(allocator);
                defer view.deinit(allocator);
                const range = view.rowRange(row);
                const entries = try allocator.alloc(OwnedMapEntry, range.end - range.start);
                var filled: usize = 0;
                errdefer {
                    for (entries[0..filled]) |*item| {
                        item.key.deinit(allocator);
                        item.value.deinit(allocator);
                    }
                    allocator.free(entries);
                }
                for (range.start..range.end) |idx| {
                    entries[filled].key = try ownedValueFromColumnRow(allocator, view.keys, idx);
                    errdefer entries[filled].key.deinit(allocator);
                    entries[filled].value = try ownedValueFromColumnRow(allocator, view.values, idx);
                    filled += 1;
                }
                break :blk .{ .map = entries };
            }
            if (unwrapTypeArgument(column.typeName(), "Tuple") != null) {
                var view = try column.asTuple(allocator);
                defer view.deinit(allocator);
                const fields = try allocator.alloc(OwnedTupleFieldValue, view.fields.len);
                var filled: usize = 0;
                errdefer {
                    for (fields[0..filled]) |*item| {
                        allocator.free(item.name);
                        item.value.deinit(allocator);
                    }
                    allocator.free(fields);
                }
                for (view.fields) |field| {
                    fields[filled].name = try allocator.dupe(u8, field.name);
                    errdefer allocator.free(fields[filled].name);
                    fields[filled].value = try ownedValueFromColumnRow(allocator, field.column, row);
                    filled += 1;
                }
                break :blk .{ .tuple = fields };
            }
            if (unwrapTypeArgument(column.typeName(), "LowCardinality") != null) {
                var view = try column.asLowCardinality(allocator);
                defer view.deinit(allocator);
                const dictionary_index = std.math.cast(usize, try unsignedColumnValueAt(view.keys, row)) orelse return error.IntegerOverflow;
                if (dictionary_index >= view.dictionary.rowCount()) return error.ResultBindingTypeMismatch;
                break :blk try ownedValueFromColumnRow(allocator, view.dictionary, dictionary_index);
            }
            return error.ResultBindingTypeMismatch;
        },
    };
}

fn appendResultSink(allocator: std.mem.Allocator, sink: *ResultSink, column: Column) !void {
    switch (sink.*) {
        .strings => |target| switch (column) {
            .string => |value| {
                for (value.values) |item| try target.appendDup(item);
            },
            .var_bytes => |value| {
                for (value.values) |item| try target.appendDup(item);
            },
            .fixed_bytes => |value| {
                for (0..value.rowCount()) |idx| try target.appendDup(value.row(idx));
            },
            else => return error.ResultBindingTypeMismatch,
        },
        .bytes => |target| switch (column) {
            .string => |value| {
                for (value.values) |item| try target.appendDup(item);
            },
            .var_bytes => |value| {
                for (value.values) |item| try target.appendDup(item);
            },
            .fixed_bytes => |value| {
                for (0..value.rowCount()) |idx| try target.appendDup(value.row(idx));
            },
            else => return error.ResultBindingTypeMismatch,
        },
        .int8s => |target| switch (column) {
            .int8 => |value| try target.appendSlice(value.values),
            else => return error.ResultBindingTypeMismatch,
        },
        .int64s => |target| switch (column) {
            .int64 => |value| try target.appendSlice(value.values),
            else => return error.ResultBindingTypeMismatch,
        },
        .uint64s => |target| switch (column) {
            .uint64 => |value| try target.appendSlice(value.values),
            else => return error.ResultBindingTypeMismatch,
        },
        .bools => |target| {
            const fixed = try column.asFixed();
            if (!std.mem.eql(u8, fixed.type_name, "Bool")) return error.ResultBindingTypeMismatch;
            for (0..fixed.rows) |idx| {
                try target.append(try fixed.boolAt(idx));
            }
        },
        .values => |target| {
            for (0..column.rowCount()) |idx| {
                var value = try ownedValueFromColumnRow(allocator, column, idx);
                errdefer value.deinit(allocator);
                try target.append(value);
            }
        },
    }
}

fn stringColumnValues(column: Column) ![]const []const u8 {
    return switch (column) {
        .string => |value| value.values,
        .var_bytes => |value| value.values,
        else => error.UnexpectedColumnType,
    };
}

fn fixedColumnSlice(column: Column, comptime T: type) ![]align(1) const T {
    const fixed = try column.asFixed();
    return fixed.slice(T);
}

fn signedMetricValues(allocator: std.mem.Allocator, column: Column) ![]i64 {
    switch (column) {
        .int64 => |value| return allocator.dupe(i64, value.values),
        .uint64 => |value| {
            const out = try allocator.alloc(i64, value.values.len);
            errdefer allocator.free(out);
            for (value.values, 0..) |item, idx| {
                out[idx] = std.math.cast(i64, item) orelse return error.IntegerOverflow;
            }
            return out;
        },
        else => {
            const fixed = try column.asFixed();
            const base = typeBaseName(fixed.type_name);
            if (std.mem.eql(u8, base, "Int64")) {
                const values = try fixed.slice(i64);
                const out = try allocator.alloc(i64, values.len);
                @memcpy(out, values);
                return out;
            }
            if (std.mem.eql(u8, base, "UInt64")) {
                const values = try fixed.slice(u64);
                const out = try allocator.alloc(i64, values.len);
                errdefer allocator.free(out);
                for (values, 0..) |item, idx| {
                    out[idx] = std.math.cast(i64, item) orelse return error.IntegerOverflow;
                }
                return out;
            }
            return error.UnexpectedColumnType;
        },
    }
}

fn decodeLogsBatch(allocator: std.mem.Allocator, block: *const DecodedBlock) ![]ServerLog {
    const time_seconds = try fixedColumnSlice(blockColumnByName(block, "event_time") orelse return error.MissingColumn, u32);
    const time_microseconds = try fixedColumnSlice(blockColumnByName(block, "event_time_microseconds") orelse return error.MissingColumn, u32);
    const hosts = try stringColumnValues(blockColumnByName(block, "host_name") orelse return error.MissingColumn);
    const query_ids = try stringColumnValues(blockColumnByName(block, "query_id") orelse return error.MissingColumn);
    const thread_ids = try fixedColumnSlice(blockColumnByName(block, "thread_id") orelse return error.MissingColumn, u64);
    const priorities = try fixedColumnSlice(blockColumnByName(block, "priority") orelse return error.MissingColumn, i8);
    const sources = try stringColumnValues(blockColumnByName(block, "source") orelse return error.MissingColumn);
    const texts = try stringColumnValues(blockColumnByName(block, "text") orelse return error.MissingColumn);

    const rows = block.rows;
    if (time_seconds.len != rows or time_microseconds.len != rows or hosts.len != rows or query_ids.len != rows or thread_ids.len != rows or priorities.len != rows or sources.len != rows or texts.len != rows) {
        return error.UnexpectedLogBlock;
    }

    const out = try allocator.alloc(ServerLog, rows);
    for (out, 0..) |*item, idx| {
        item.* = .{
            .query_id = query_ids[idx],
            .source = sources[idx],
            .text = texts[idx],
            .host = hosts[idx],
            .time_seconds = time_seconds[idx],
            .time_microseconds = time_microseconds[idx],
            .thread_id = thread_ids[idx],
            .priority = priorities[idx],
        };
    }
    return out;
}

fn decodeProfileEventsBatch(allocator: std.mem.Allocator, block: *const DecodedBlock) ![]ProfileEvent {
    const hosts = try stringColumnValues(blockColumnByName(block, "host_name") orelse return error.MissingColumn);
    const time_seconds = try fixedColumnSlice(blockColumnByName(block, "current_time") orelse return error.MissingColumn, u32);
    const thread_ids = try fixedColumnSlice(blockColumnByName(block, "thread_id") orelse return error.MissingColumn, u64);
    const types = try fixedColumnSlice(blockColumnByName(block, "type") orelse return error.MissingColumn, i8);
    const names = try stringColumnValues(blockColumnByName(block, "name") orelse return error.MissingColumn);
    const values = try signedMetricValues(allocator, blockColumnByName(block, "value") orelse return error.MissingColumn);
    errdefer allocator.free(values);

    const rows = block.rows;
    if (hosts.len != rows or time_seconds.len != rows or thread_ids.len != rows or types.len != rows or names.len != rows or values.len != rows) {
        return error.UnexpectedProfileEventsBlock;
    }

    const out = try allocator.alloc(ProfileEvent, rows);
    for (out, 0..) |*item, idx| {
        item.* = .{
            .event_type = std.meta.intToEnum(ProfileEventType, types[idx]) catch return error.InvalidProfileEventType,
            .name = names[idx],
            .value = values[idx],
            .host = hosts[idx],
            .time_seconds = time_seconds[idx],
            .thread_id = thread_ids[idx],
        };
    }
    return out;
}

fn findSettingValue(settings: []const Setting, key: []const u8) ?[]const u8 {
    for (settings) |setting| {
        if (std.mem.eql(u8, setting.key, key)) return setting.value;
    }
    return null;
}

fn readUUID(decoder: *Decoder) !UUID {
    const raw = try decoder.readSlice(16);
    var uuid: UUID = undefined;
    @memcpy(&uuid, raw);
    return uuid;
}

fn readUUIDFromStream(reader: *StreamReader) !UUID {
    var uuid: UUID = undefined;
    try reader.readExact(&uuid);
    return uuid;
}

fn readVarUIntAs(comptime T: type, decoder: *Decoder) !T {
    return castVarUInt(T, try decoder.readVarUInt());
}

fn castVarUInt(comptime T: type, value: u64) !T {
    return std.math.cast(T, value) orelse error.IntegerOverflow;
}

const TypePair = struct {
    first: []const u8,
    second: []const u8,
};

const TopLevelSplitIterator = struct {
    input: []const u8,
    index: usize = 0,

    pub fn init(input: []const u8) TopLevelSplitIterator {
        return .{ .input = input };
    }

    pub fn next(self: *TopLevelSplitIterator) ?[]const u8 {
        while (self.index < self.input.len and std.ascii.isWhitespace(self.input[self.index])) {
            self.index += 1;
        }
        if (self.index >= self.input.len) return null;

        const start = self.index;
        var depth: usize = 0;
        var in_string = false;
        var escaped = false;

        while (self.index < self.input.len) {
            const ch = self.input[self.index];
            if (in_string) {
                self.index += 1;
                if (escaped) {
                    escaped = false;
                    continue;
                }
                switch (ch) {
                    '\\' => escaped = true,
                    '\'' => in_string = false,
                    else => {},
                }
                continue;
            }

            switch (ch) {
                '\'' => in_string = true,
                '(' => depth += 1,
                ')' => {
                    if (depth > 0) depth -= 1;
                },
                ',' => {
                    if (depth == 0) {
                        const part = trimTypeName(self.input[start..self.index]);
                        self.index += 1;
                        return part;
                    }
                },
                else => {},
            }
            self.index += 1;
        }

        return trimTypeName(self.input[start..self.input.len]);
    }
};

fn trimTypeName(input: []const u8) []const u8 {
    return std.mem.trim(u8, input, " \t\r\n");
}

fn typeBaseName(type_name: []const u8) []const u8 {
    const trimmed = trimTypeName(type_name);
    var idx: usize = 0;
    var in_string = false;
    var escaped = false;
    while (idx < trimmed.len) : (idx += 1) {
        const ch = trimmed[idx];
        if (in_string) {
            if (escaped) {
                escaped = false;
                continue;
            }
            switch (ch) {
                '\\' => escaped = true,
                '\'' => in_string = false,
                else => {},
            }
            continue;
        }
        switch (ch) {
            '\'' => in_string = true,
            '(' => return trimTypeName(trimmed[0..idx]),
            else => {},
        }
    }
    return trimmed;
}

fn unwrapTypeArgument(type_name: []const u8, prefix: []const u8) ?[]const u8 {
    const trimmed = trimTypeName(type_name);
    if (!std.mem.eql(u8, typeBaseName(trimmed), prefix)) return null;
    if (trimmed.len <= prefix.len + 2) return null;
    if (trimmed[prefix.len] != '(' or trimmed[trimmed.len - 1] != ')') return null;
    return trimTypeName(trimmed[prefix.len + 1 .. trimmed.len - 1]);
}

fn topLevelFirstSpace(input: []const u8) ?usize {
    var idx: usize = 0;
    var depth: usize = 0;
    var in_string = false;
    var escaped = false;

    while (idx < input.len) : (idx += 1) {
        const ch = input[idx];
        if (in_string) {
            if (escaped) {
                escaped = false;
                continue;
            }
            switch (ch) {
                '\\' => escaped = true,
                '\'' => in_string = false,
                else => {},
            }
            continue;
        }
        switch (ch) {
            '\'' => in_string = true,
            '(' => depth += 1,
            ')' => {
                if (depth > 0) depth -= 1;
            },
            ' ', '\t', '\r', '\n' => {
                if (depth == 0) return idx;
            },
            else => {},
        }
    }

    return null;
}

fn tupleElementTypeName(part: []const u8) []const u8 {
    const trimmed = trimTypeName(part);
    if (topLevelFirstSpace(trimmed)) |idx| {
        const tail = trimTypeName(trimmed[idx + 1 ..]);
        if (tail.len != 0) return tail;
    }
    return trimmed;
}

fn tupleElementName(part: []const u8) []const u8 {
    const trimmed = trimTypeName(part);
    if (topLevelFirstSpace(trimmed)) |idx| {
        const head = trimTypeName(trimmed[0..idx]);
        const tail = trimTypeName(trimmed[idx + 1 ..]);
        if (head.len != 0 and tail.len != 0) return head;
    }
    return "";
}

fn splitTopLevelPair(input: []const u8) !TypePair {
    var iter = TopLevelSplitIterator.init(input);
    const first = iter.next() orelse return error.InvalidTypeName;
    const second = iter.next() orelse return error.InvalidTypeName;
    if (iter.next() != null) return error.InvalidTypeName;
    return .{
        .first = trimTypeName(first),
        .second = trimTypeName(second),
    };
}

fn copyRawFromStream(reader: *StreamReader, encoder: *Encoder, len: usize) !void {
    var remaining = len;
    var buf: [4096]u8 = undefined;
    while (remaining > 0) {
        const chunk: usize = @min(remaining, buf.len);
        try reader.readExact(buf[0..chunk]);
        try encoder.putRaw(buf[0..chunk]);
        remaining -= chunk;
    }
}

fn copyStringFromStream(reader: *StreamReader, encoder: *Encoder) !void {
    const len = try reader.readVarUInt();
    try encoder.putVarUInt(len);
    try copyRawFromStream(reader, encoder, try castVarUInt(usize, len));
}

fn buildOwnedStringLikeColumn(allocator: std.mem.Allocator, name: []const u8, type_name: []const u8, values: []const []const u8, is_var_bytes: bool) !Column {
    const copied_values = try allocator.alloc([]const u8, values.len);
    errdefer allocator.free(copied_values);

    var total: usize = 0;
    for (values) |value| {
        total = try std.math.add(usize, total, value.len);
    }

    const backing = try allocator.alloc(u8, total);
    errdefer allocator.free(backing);

    var offset: usize = 0;
    for (values, 0..) |value, idx| {
        @memcpy(backing[offset .. offset + value.len], value);
        copied_values[idx] = backing[offset .. offset + value.len];
        offset += value.len;
    }

    if (is_var_bytes) {
        return .{ .var_bytes = .{
            .name = name,
            .type_name = type_name,
            .values = copied_values,
            .owns_values = true,
            .backing_data = backing,
            .owns_backing_data = true,
        } };
    }

    return .{ .string = .{
        .name = name,
        .values = copied_values,
        .owns_values = true,
        .backing_data = backing,
        .owns_backing_data = true,
    } };
}

fn decodeOwnedStringPayloadColumn(allocator: std.mem.Allocator, column_name: []const u8, type_name: []const u8, rows: usize, payload: []u8, is_var_bytes: bool) !Column {
    var decoder = Decoder.init(payload);
    const values = try allocator.alloc([]const u8, rows);
    errdefer allocator.free(values);
    for (values) |*value| {
        value.* = try decoder.readString();
    }
    if (!decoder.eof()) return error.TrailingColumnData;

    if (is_var_bytes) {
        return .{ .var_bytes = .{
            .name = column_name,
            .type_name = type_name,
            .values = values,
            .owns_values = true,
            .backing_data = payload,
            .owns_backing_data = true,
        } };
    }

    return .{ .string = .{
        .name = column_name,
        .values = values,
        .owns_values = true,
        .backing_data = payload,
        .owns_backing_data = true,
    } };
}

fn decodeOwnedColumnFromStatePayload(allocator: std.mem.Allocator, column_name: []const u8, type_name: []const u8, rows: usize, state: []u8, payload: []u8) !Column {
    errdefer allocator.free(state);
    errdefer allocator.free(payload);

    if (rows == 0) {
        allocator.free(state);
        allocator.free(payload);
        return emptyColumnForType(allocator, column_name, type_name);
    }

    if (std.mem.eql(u8, type_name, "String")) {
        if (state.len != 0) return error.UnexpectedColumnState;
        allocator.free(state);
        return decodeOwnedStringPayloadColumn(allocator, column_name, type_name, rows, payload, false);
    }
    if (std.mem.eql(u8, type_name, "JSON")) {
        if (state.len != 0) return error.UnexpectedColumnState;
        allocator.free(state);
        return decodeOwnedStringPayloadColumn(allocator, column_name, type_name, rows, payload, true);
    }

    if (state.len == 0 and
        (std.mem.eql(u8, type_name, "Int8") or
            std.mem.eql(u8, type_name, "Int64") or
            std.mem.eql(u8, type_name, "UInt64") or
            fixedWidthForType(type_name) != null))
    {
        var decoder = Decoder.init(payload);
        var column = try Column.decode(allocator, &decoder, column_name, type_name, rows);
        errdefer column.deinit(allocator);
        if (!decoder.eof()) return error.TrailingColumnData;
        allocator.free(state);
        allocator.free(payload);
        return column;
    }

    return .{ .encoded = .{
        .name = column_name,
        .type_name = type_name,
        .rows = rows,
        .state = state,
        .payload = payload,
        .owns_state = true,
        .owns_payload = true,
    } };
}

fn captureColumnState(allocator: std.mem.Allocator, decoder: *Decoder, type_name: []const u8) ![]u8 {
    const start = decoder.pos;
    try skipColumnState(decoder, type_name);
    return allocator.dupe(u8, decoder.data[start..decoder.pos]);
}

fn captureColumnPayload(allocator: std.mem.Allocator, decoder: *Decoder, type_name: []const u8, rows: usize) ![]u8 {
    const start = decoder.pos;
    try skipColumnPayload(decoder, type_name, rows);
    return allocator.dupe(u8, decoder.data[start..decoder.pos]);
}

fn captureColumnStateFromStream(allocator: std.mem.Allocator, reader: *StreamReader, type_name: []const u8) ![]u8 {
    var encoder = Encoder.init(allocator);
    errdefer encoder.deinit();
    try copyColumnStateFromStream(reader, &encoder, type_name);
    return encoder.buf.toOwnedSlice();
}

fn captureColumnPayloadFromStream(allocator: std.mem.Allocator, reader: *StreamReader, type_name: []const u8, rows: usize) ![]u8 {
    var encoder = Encoder.init(allocator);
    errdefer encoder.deinit();
    try copyColumnPayloadFromStream(reader, &encoder, type_name, rows);
    return encoder.buf.toOwnedSlice();
}

fn skipColumnState(decoder: *Decoder, type_name: []const u8) !void {
    const trimmed = trimTypeName(type_name);
    if (unwrapTypeArgument(trimmed, "Array")) |inner| {
        return skipColumnState(decoder, inner);
    }
    if (unwrapTypeArgument(trimmed, "Nullable")) |inner| {
        return skipColumnState(decoder, inner);
    }
    if (unwrapTypeArgument(trimmed, "LowCardinality")) |inner| {
        _ = try decoder.readInt64LE();
        return skipColumnState(decoder, inner);
    }
    if (unwrapTypeArgument(trimmed, "Map")) |inner| {
        const pair = try splitTopLevelPair(inner);
        try skipColumnState(decoder, pair.first);
        try skipColumnState(decoder, pair.second);
        return;
    }
    if (unwrapTypeArgument(trimmed, "Tuple")) |inner| {
        var iter = TopLevelSplitIterator.init(inner);
        while (iter.next()) |part| {
            try skipColumnState(decoder, tupleElementTypeName(part));
        }
        return;
    }
}

fn skipColumnPayload(decoder: *Decoder, type_name: []const u8, rows: usize) !void {
    const trimmed = trimTypeName(type_name);

    if (unwrapTypeArgument(trimmed, "Array")) |inner| {
        var total: usize = 0;
        for (0..rows) |_| {
            total = std.math.cast(usize, try decoder.readUInt64LE()) orelse return error.IntegerOverflow;
        }
        return skipColumnPayload(decoder, inner, total);
    }

    if (unwrapTypeArgument(trimmed, "Nullable")) |inner| {
        _ = try decoder.readSlice(rows);
        return skipColumnPayload(decoder, inner, rows);
    }

    if (unwrapTypeArgument(trimmed, "LowCardinality")) |inner| {
        const meta = try decoder.readInt64LE();
        const key_width = try lowCardinalityKeyWidthForMeta(meta);
        const index_rows_i64 = try decoder.readInt64LE();
        const index_rows = std.math.cast(usize, index_rows_i64) orelse return error.IntegerOverflow;
        try skipColumnPayload(decoder, inner, index_rows);

        const key_rows_i64 = try decoder.readInt64LE();
        const key_rows = std.math.cast(usize, key_rows_i64) orelse return error.IntegerOverflow;
        _ = try decoder.readSlice(try std.math.mul(usize, key_rows, key_width));
        return;
    }

    if (unwrapTypeArgument(trimmed, "Map")) |inner| {
        const pair = try splitTopLevelPair(inner);
        var total: usize = 0;
        for (0..rows) |_| {
            total = std.math.cast(usize, try decoder.readUInt64LE()) orelse return error.IntegerOverflow;
        }
        try skipColumnPayload(decoder, pair.first, total);
        try skipColumnPayload(decoder, pair.second, total);
        return;
    }

    if (unwrapTypeArgument(trimmed, "Tuple")) |inner| {
        var iter = TopLevelSplitIterator.init(inner);
        while (iter.next()) |part| {
            try skipColumnPayload(decoder, tupleElementTypeName(part), rows);
        }
        return;
    }

    if (std.mem.eql(u8, trimmed, "String") or std.mem.eql(u8, trimmed, "JSON")) {
        for (0..rows) |_| {
            _ = try decoder.readString();
        }
        return;
    }

    if (fixedWidthForType(trimmed)) |width| {
        _ = try decoder.readSlice(try std.math.mul(usize, rows, width));
        return;
    }

    return error.UnsupportedColumnType;
}

fn copyColumnStateFromStream(reader: *StreamReader, encoder: *Encoder, type_name: []const u8) !void {
    const trimmed = trimTypeName(type_name);
    if (unwrapTypeArgument(trimmed, "Array")) |inner| {
        return copyColumnStateFromStream(reader, encoder, inner);
    }
    if (unwrapTypeArgument(trimmed, "Nullable")) |inner| {
        return copyColumnStateFromStream(reader, encoder, inner);
    }
    if (unwrapTypeArgument(trimmed, "LowCardinality")) |inner| {
        try encoder.putInt64LE(try reader.readInt64LE());
        return copyColumnStateFromStream(reader, encoder, inner);
    }
    if (unwrapTypeArgument(trimmed, "Map")) |inner| {
        const pair = try splitTopLevelPair(inner);
        try copyColumnStateFromStream(reader, encoder, pair.first);
        try copyColumnStateFromStream(reader, encoder, pair.second);
        return;
    }
    if (unwrapTypeArgument(trimmed, "Tuple")) |inner| {
        var iter = TopLevelSplitIterator.init(inner);
        while (iter.next()) |part| {
            try copyColumnStateFromStream(reader, encoder, tupleElementTypeName(part));
        }
        return;
    }
}

fn copyColumnPayloadFromStream(reader: *StreamReader, encoder: *Encoder, type_name: []const u8, rows: usize) !void {
    const trimmed = trimTypeName(type_name);

    if (unwrapTypeArgument(trimmed, "Array")) |inner| {
        var total: usize = 0;
        for (0..rows) |_| {
            const offset = try reader.readUInt64LE();
            try encoder.putUInt64LE(offset);
            total = std.math.cast(usize, offset) orelse return error.IntegerOverflow;
        }
        return copyColumnPayloadFromStream(reader, encoder, inner, total);
    }

    if (unwrapTypeArgument(trimmed, "Nullable")) |inner| {
        try copyRawFromStream(reader, encoder, rows);
        return copyColumnPayloadFromStream(reader, encoder, inner, rows);
    }

    if (unwrapTypeArgument(trimmed, "LowCardinality")) |inner| {
        const meta = try reader.readInt64LE();
        try encoder.putInt64LE(meta);
        const key_width = try lowCardinalityKeyWidthForMeta(meta);

        const index_rows_i64 = try reader.readInt64LE();
        try encoder.putInt64LE(index_rows_i64);
        const index_rows = std.math.cast(usize, index_rows_i64) orelse return error.IntegerOverflow;
        try copyColumnPayloadFromStream(reader, encoder, inner, index_rows);

        const key_rows_i64 = try reader.readInt64LE();
        try encoder.putInt64LE(key_rows_i64);
        const key_rows = std.math.cast(usize, key_rows_i64) orelse return error.IntegerOverflow;
        try copyRawFromStream(reader, encoder, try std.math.mul(usize, key_rows, key_width));
        return;
    }

    if (unwrapTypeArgument(trimmed, "Map")) |inner| {
        const pair = try splitTopLevelPair(inner);
        var total: usize = 0;
        for (0..rows) |_| {
            const offset = try reader.readUInt64LE();
            try encoder.putUInt64LE(offset);
            total = std.math.cast(usize, offset) orelse return error.IntegerOverflow;
        }
        try copyColumnPayloadFromStream(reader, encoder, pair.first, total);
        try copyColumnPayloadFromStream(reader, encoder, pair.second, total);
        return;
    }

    if (unwrapTypeArgument(trimmed, "Tuple")) |inner| {
        var iter = TopLevelSplitIterator.init(inner);
        while (iter.next()) |part| {
            try copyColumnPayloadFromStream(reader, encoder, tupleElementTypeName(part), rows);
        }
        return;
    }

    if (std.mem.eql(u8, trimmed, "String") or std.mem.eql(u8, trimmed, "JSON")) {
        for (0..rows) |_| {
            try copyStringFromStream(reader, encoder);
        }
        return;
    }

    if (fixedWidthForType(trimmed)) |width| {
        try copyRawFromStream(reader, encoder, try std.math.mul(usize, rows, width));
        return;
    }

    return error.UnsupportedColumnType;
}

fn emptyColumnForType(allocator: std.mem.Allocator, column_name: []const u8, type_name: []const u8) !Column {
    if (std.mem.eql(u8, type_name, "String")) {
        return .{ .string = .{
            .name = column_name,
            .values = try allocator.alloc([]const u8, 0),
            .owns_values = true,
        } };
    }
    if (std.mem.eql(u8, type_name, "JSON")) {
        return .{ .var_bytes = .{
            .name = column_name,
            .type_name = type_name,
            .values = try allocator.alloc([]const u8, 0),
            .owns_values = true,
        } };
    }
    if (std.mem.eql(u8, type_name, "Int8")) {
        return .{ .int8 = .{
            .name = column_name,
            .values = try allocator.alloc(i8, 0),
            .owns_values = true,
        } };
    }
    if (std.mem.eql(u8, type_name, "Int64")) {
        return .{ .int64 = .{
            .name = column_name,
            .values = try allocator.alloc(i64, 0),
            .owns_values = true,
        } };
    }
    if (std.mem.eql(u8, type_name, "UInt64")) {
        return .{ .uint64 = .{
            .name = column_name,
            .values = try allocator.alloc(u64, 0),
            .owns_values = true,
        } };
    }
    if (fixedWidthForType(type_name)) |width| {
        return .{ .fixed_bytes = .{
            .name = column_name,
            .type_name = type_name,
            .width = width,
            .data = try allocator.alloc(u8, 0),
            .rows = 0,
            .owns_data = true,
        } };
    }
    return .{ .encoded = .{
        .name = column_name,
        .type_name = type_name,
        .rows = 0,
        .state = "",
        .payload = "",
    } };
}

const low_cardinality_key_mask: i64 = 0xff;
const low_cardinality_has_additional_keys_bit: i64 = 1 << 9;
const low_cardinality_need_update_dictionary_bit: i64 = 1 << 10;
const low_cardinality_update_all: i64 = low_cardinality_has_additional_keys_bit | low_cardinality_need_update_dictionary_bit;

fn lowCardinalityKeyWidthForMeta(meta: i64) !usize {
    if ((meta & low_cardinality_has_additional_keys_bit) == 0) return error.InvalidLowCardinalityKeyType;
    const raw = @as(u8, @intCast(@as(u64, @bitCast(meta & low_cardinality_key_mask))));
    return switch (raw) {
        0 => 1,
        1 => 2,
        2 => 4,
        3 => 8,
        else => error.InvalidLowCardinalityKeyType,
    };
}

fn lowCardinalityKeyTagForColumn(column: Column) !i64 {
    const type_name = typeBaseName(column.typeName());
    if (std.mem.eql(u8, type_name, "UInt8")) return 0;
    if (std.mem.eql(u8, type_name, "UInt16")) return 1;
    if (std.mem.eql(u8, type_name, "UInt32")) return 2;
    if (std.mem.eql(u8, type_name, "UInt64")) return 3;
    return error.InvalidLowCardinalityKeyType;
}

fn fixedWidthForType(type_name: []const u8) ?usize {
    const trimmed = trimTypeName(type_name);
    const base = typeBaseName(trimmed);

    if (std.mem.eql(u8, base, "UInt8")) return 1;
    if (std.mem.eql(u8, base, "Int8")) return 1;
    if (std.mem.eql(u8, base, "Bool")) return 1;
    if (std.mem.eql(u8, base, "Enum8")) return 1;

    if (std.mem.eql(u8, base, "UInt16")) return 2;
    if (std.mem.eql(u8, base, "Int16")) return 2;
    if (std.mem.eql(u8, base, "Date")) return 2;
    if (std.mem.eql(u8, base, "Enum16")) return 2;
    if (std.mem.eql(u8, base, "BFloat16")) return 2;

    if (std.mem.eql(u8, base, "UInt32")) return 4;
    if (std.mem.eql(u8, base, "Int32")) return 4;
    if (std.mem.eql(u8, base, "Float32")) return 4;
    if (std.mem.eql(u8, base, "DateTime")) return 4;
    if (std.mem.eql(u8, base, "Date32")) return 4;
    if (std.mem.eql(u8, base, "IPv4")) return 4;
    if (std.mem.eql(u8, base, "Time32")) return 4;
    if (std.mem.eql(u8, base, "Decimal32")) return 4;

    if (std.mem.eql(u8, base, "UInt64")) return 8;
    if (std.mem.eql(u8, base, "Int64")) return 8;
    if (std.mem.eql(u8, base, "Float64")) return 8;
    if (std.mem.eql(u8, base, "Time64")) return 8;
    if (std.mem.eql(u8, base, "DateTime64")) return 8;
    if (std.mem.eql(u8, base, "Decimal64")) return 8;
    if (std.mem.startsWith(u8, base, "Interval")) return 8;

    if (std.mem.eql(u8, base, "Int128")) return 16;
    if (std.mem.eql(u8, base, "UInt128")) return 16;
    if (std.mem.eql(u8, base, "Decimal128")) return 16;
    if (std.mem.eql(u8, base, "UUID")) return 16;
    if (std.mem.eql(u8, base, "IPv6")) return 16;
    if (std.mem.eql(u8, base, "Point")) return 16;

    if (std.mem.eql(u8, base, "Int256")) return 32;
    if (std.mem.eql(u8, base, "UInt256")) return 32;
    if (std.mem.eql(u8, base, "Decimal256")) return 32;

    if (std.mem.eql(u8, base, "Nothing")) return 1;

    if (std.mem.eql(u8, base, "FixedString")) {
        const inner = unwrapTypeArgument(trimmed, "FixedString") orelse return null;
        return std.fmt.parseInt(usize, inner, 10) catch null;
    }

    if (std.mem.eql(u8, base, "Decimal")) {
        const inner = unwrapTypeArgument(trimmed, "Decimal") orelse return null;
        var iter = TopLevelSplitIterator.init(inner);
        const precision_text = iter.next() orelse return null;
        const precision = std.fmt.parseInt(u32, precision_text, 10) catch return null;
        return switch (precision) {
            0...9 => 4,
            10...18 => 8,
            19...38 => 16,
            39...76 => 32,
            else => null,
        };
    }

    return null;
}

fn readClientCodeFromStream(reader: *StreamReader) !ClientCode {
    const raw = try reader.readVarUInt();
    return std.meta.intToEnum(ClientCode, try castVarUInt(u8, raw)) catch error.InvalidClientCode;
}

fn readServerCodeFromStream(reader: *StreamReader) !ServerCode {
    const raw = try reader.readVarUInt();
    return std.meta.intToEnum(ServerCode, try castVarUInt(u8, raw)) catch error.InvalidServerCode;
}

fn swap64Chunks(comptime N: usize, input: [N]u8) [N]u8 {
    comptime {
        if (N % 8 != 0) @compileError("swap64Chunks expects a multiple of 8 bytes");
    }
    var out = input;
    inline for (0..N / 8) |idx| {
        std.mem.reverse(u8, out[idx * 8 .. idx * 8 + 8]);
    }
    return out;
}

pub const PoolOptions = struct {
    host: []const u8,
    port: u16,
    client_options: ClientOptions = .{},
    max_conn_lifetime_ms: u64 = 60 * 60 * 1000,
    max_conn_idle_time_ms: u64 = 30 * 60 * 1000,
    max_conns: usize = 0,
    min_conns: usize = 0,
    health_check_period_ms: u64 = 60 * 1000,
};

pub const PoolStats = struct {
    total_conns: usize,
    idle_conns: usize,
    acquired_conns: usize,
};

const PoolEntry = struct {
    client: Client,
    created_ns: i128,
    last_used_ns: i128,
    in_use: bool = false,
};

pub const PooledClient = struct {
    pool: *Pool,
    entry: ?*PoolEntry,

    pub fn client(self: *const PooledClient) *Client {
        return &self.entry.?.client;
    }

    pub fn Do(self: *PooledClient, ctx: QueryContext, query: *Query) !void {
        return self.client().Do(ctx, query);
    }

    pub fn ping(self: *PooledClient) !void {
        return self.client().ping();
    }

    pub fn release(self: *PooledClient) void {
        const entry = self.entry orelse return;
        self.entry = null;
        self.pool.releaseEntry(entry);
    }

    pub fn deinit(self: *PooledClient) void {
        self.release();
    }
};

pub const Pool = struct {
    allocator: std.mem.Allocator,
    host: []u8,
    port: u16,
    options: PoolOptions,
    max_conns: usize,
    mutex: std.Thread.Mutex = .{},
    cond: std.Thread.Condition = .{},
    entries: std.ArrayList(*PoolEntry),
    pending_creates: usize = 0,
    last_health_check_ns: i128 = 0,
    closed: bool = false,

    pub fn init(allocator: std.mem.Allocator, options: PoolOptions) !Pool {
        return initWithMode(allocator, options, false);
    }

    pub fn dial(allocator: std.mem.Allocator, options: PoolOptions) !Pool {
        return initWithMode(allocator, options, true);
    }

    fn initWithMode(allocator: std.mem.Allocator, options: PoolOptions, verify_on_init: bool) !Pool {
        const host = try allocator.dupe(u8, options.host);
        errdefer allocator.free(host);

        const max_conns = if (options.max_conns == 0)
            std.Thread.getCpuCount() catch 4
        else
            options.max_conns;
        if (options.min_conns > max_conns) return error.InvalidPoolConfiguration;

        var pool = Pool{
            .allocator = allocator,
            .host = host,
            .port = options.port,
            .options = options,
            .max_conns = max_conns,
            .entries = std.ArrayList(*PoolEntry).init(allocator),
            .last_health_check_ns = std.time.nanoTimestamp(),
        };
        errdefer pool.entries.deinit();

        for (0..options.min_conns) |_| {
            const entry = try pool.createEntry();
            entry.in_use = false;
            try pool.entries.append(entry);
        }

        if (verify_on_init) {
            var conn = try pool.acquire(.{});
            defer conn.release();
        }

        return pool;
    }

    pub fn deinit(self: *Pool) void {
        self.close();
        self.entries.deinit();
        self.allocator.free(self.host);
        self.* = undefined;
    }

    pub fn close(self: *Pool) void {
        self.mutex.lock();
        if (self.closed) {
            self.mutex.unlock();
            return;
        }
        self.closed = true;
        const entries = self.entries.items;
        self.entries.items.len = 0;
        self.cond.broadcast();
        self.mutex.unlock();

        for (entries) |entry| {
            self.destroyEntry(entry);
        }
    }

    pub fn stat(self: *Pool) PoolStats {
        self.mutex.lock();
        defer self.mutex.unlock();

        var idle: usize = 0;
        for (self.entries.items) |entry| {
            if (!entry.in_use) idle += 1;
        }
        return .{
            .total_conns = self.entries.items.len,
            .idle_conns = idle,
            .acquired_conns = self.entries.items.len - idle,
        };
    }

    pub fn acquire(self: *Pool, ctx: QueryContext) !PooledClient {
        while (true) {
            try ctx.check();
            self.maybeRunMaintenance();

            var maybe_entry: ?*PoolEntry = null;
            var should_create = false;
            self.mutex.lock();
            if (self.closed) {
                self.mutex.unlock();
                return error.PoolClosed;
            }

            const now = std.time.nanoTimestamp();
            self.reapExpiredLocked(now);

            for (self.entries.items) |entry| {
                if (entry.in_use) continue;
                entry.in_use = true;
                maybe_entry = entry;
                break;
            }

            if (maybe_entry) |entry| {
                self.mutex.unlock();
                return .{
                    .pool = self,
                    .entry = entry,
                };
            }

            if (self.entries.items.len + self.pending_creates < self.max_conns) {
                self.pending_creates += 1;
                should_create = true;
            } else {
                self.cond.timedWait(&self.mutex, pool_wait_interval_ns) catch {};
            }
            self.mutex.unlock();

            if (should_create) {
                const entry = self.createEntry() catch |err| {
                    self.mutex.lock();
                    self.pending_creates -= 1;
                    self.cond.broadcast();
                    self.mutex.unlock();
                    return err;
                };

                self.mutex.lock();
                self.pending_creates -= 1;
                if (self.closed) {
                    self.cond.broadcast();
                    self.mutex.unlock();
                    self.destroyEntry(entry);
                    return error.PoolClosed;
                }
                entry.in_use = true;
                self.entries.append(entry) catch |err| {
                    self.cond.broadcast();
                    self.mutex.unlock();
                    self.destroyEntry(entry);
                    return err;
                };
                self.cond.broadcast();
                self.mutex.unlock();
                return .{
                    .pool = self,
                    .entry = entry,
                };
            }
        }
    }

    pub fn Do(self: *Pool, ctx: QueryContext, query: *Query) !void {
        var conn = try self.acquire(ctx);
        defer conn.release();
        return conn.Do(ctx, query);
    }

    pub fn ping(self: *Pool) !void {
        var conn = try self.acquire(.{});
        defer conn.release();
        return conn.ping();
    }

    fn createEntry(self: *Pool) !*PoolEntry {
        const entry = try self.allocator.create(PoolEntry);
        errdefer self.allocator.destroy(entry);
        entry.* = .{
            .client = try Client.connectTcp(self.allocator, self.host, self.port, self.options.client_options),
            .created_ns = std.time.nanoTimestamp(),
            .last_used_ns = std.time.nanoTimestamp(),
        };
        return entry;
    }

    fn destroyEntry(self: *Pool, entry: *PoolEntry) void {
        entry.client.deinit();
        self.allocator.destroy(entry);
    }

    fn releaseEntry(self: *Pool, entry: *PoolEntry) void {
        var destroy_now = false;
        self.mutex.lock();
        entry.in_use = false;
        entry.last_used_ns = std.time.nanoTimestamp();
        if (self.closed or self.entryExpired(entry, entry.last_used_ns) or entry.client.isClosed()) {
            if (indexOfPoolEntry(self.entries.items, entry)) |idx| {
                _ = self.entries.swapRemove(idx);
                destroy_now = true;
            } else {
                destroy_now = true;
            }
        } else {
            self.cond.signal();
        }
        self.mutex.unlock();

        if (destroy_now) {
            self.destroyEntry(entry);
            self.cond.broadcast();
        }
        self.maybeRunMaintenance();
    }

    fn reapExpiredLocked(self: *Pool, now: i128) void {
        var idx: usize = 0;
        while (idx < self.entries.items.len) {
            const entry = self.entries.items[idx];
            if (entry.in_use) {
                idx += 1;
                continue;
            }
            if (!self.entryExpired(entry, now) and !entry.client.isClosed()) {
                idx += 1;
                continue;
            }

            _ = self.entries.swapRemove(idx);
            self.mutex.unlock();
            self.destroyEntry(entry);
            self.mutex.lock();
        }
    }

    fn ensureMinConns(self: *Pool) void {
        while (true) {
            self.mutex.lock();
            const closed = self.closed;
            const total = self.entries.items.len + self.pending_creates;
            if (closed or total >= self.options.min_conns or total >= self.max_conns) {
                self.mutex.unlock();
                return;
            }
            self.pending_creates += 1;
            self.mutex.unlock();

            const entry = self.createEntry() catch {
                self.mutex.lock();
                self.pending_creates -= 1;
                self.cond.broadcast();
                self.mutex.unlock();
                return;
            };
            self.mutex.lock();
            self.pending_creates -= 1;
            if (self.closed) {
                self.cond.broadcast();
                self.mutex.unlock();
                self.destroyEntry(entry);
                return;
            }
            self.entries.append(entry) catch {
                self.cond.broadcast();
                self.mutex.unlock();
                self.destroyEntry(entry);
                return;
            };
            self.cond.broadcast();
            self.mutex.unlock();
        }
    }

    fn maybeRunMaintenance(self: *Pool) void {
        const due = blk: {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.closed) return;
            const now = std.time.nanoTimestamp();
            if (!self.healthCheckDueLocked(now)) break :blk false;
            self.last_health_check_ns = now;
            break :blk true;
        };

        if (due) {
            self.healthCheckIdleEntries();
        }
        self.ensureMinConns();
    }

    fn healthCheckDueLocked(self: *const Pool, now: i128) bool {
        if (self.options.health_check_period_ms == 0) return false;
        if (self.last_health_check_ns == 0) return true;
        const period_ns = @as(i128, @intCast(self.options.health_check_period_ms)) * std.time.ns_per_ms;
        return now - self.last_health_check_ns >= period_ns;
    }

    fn healthCheckIdleEntries(self: *Pool) void {
        while (true) {
            var candidate: ?*PoolEntry = null;
            self.mutex.lock();
            if (self.closed) {
                self.mutex.unlock();
                return;
            }
            const now = std.time.nanoTimestamp();
            self.reapExpiredLocked(now);
            for (self.entries.items) |entry| {
                if (entry.in_use) continue;
                entry.in_use = true;
                candidate = entry;
                break;
            }
            self.mutex.unlock();

            const entry = candidate orelse return;
            var ping_ok = true;
            entry.client.ping() catch {
                ping_ok = false;
            };

            var destroy_now = false;
            self.mutex.lock();
            if (indexOfPoolEntry(self.entries.items, entry) == null) {
                self.mutex.unlock();
                continue;
            }
            if (self.closed or !ping_ok or entry.client.isClosed() or self.entryExpired(entry, std.time.nanoTimestamp())) {
                if (indexOfPoolEntry(self.entries.items, entry)) |idx| {
                    _ = self.entries.swapRemove(idx);
                }
                destroy_now = true;
            } else {
                entry.in_use = false;
                entry.last_used_ns = std.time.nanoTimestamp();
                self.cond.signal();
            }
            self.mutex.unlock();

            if (destroy_now) {
                self.destroyEntry(entry);
                self.cond.broadcast();
            }
        }
    }

    fn entryExpired(self: *const Pool, entry: *const PoolEntry, now: i128) bool {
        if (self.options.max_conn_lifetime_ms > 0) {
            const lifetime_ns = @as(i128, @intCast(self.options.max_conn_lifetime_ms)) * std.time.ns_per_ms;
            if (now - entry.created_ns > lifetime_ns) return true;
        }
        if (self.options.max_conn_idle_time_ms > 0) {
            const idle_ns = @as(i128, @intCast(self.options.max_conn_idle_time_ms)) * std.time.ns_per_ms;
            if (now - entry.last_used_ns > idle_ns) return true;
        }
        return false;
    }
};

fn indexOfPoolEntry(entries: []const *PoolEntry, target: *PoolEntry) ?usize {
    for (entries, 0..) |entry, idx| {
        if (entry == target) return idx;
    }
    return null;
}

const pool_wait_interval_ns: u64 = 10 * std.time.ns_per_ms;

fn readFixture(allocator: std.mem.Allocator, relative_path: []const u8) ![]u8 {
    return try std.fs.cwd().readFileAlloc(allocator, relative_path, std.math.maxInt(usize));
}

fn readCompressedFrameFromStream(reader: *StreamReader, allocator: std.mem.Allocator) ![]u8 {
    var header: [ch_compress.header_size]u8 = undefined;
    try reader.readExact(&header);
    const frame_len = try ch_compress.frameLengthFromHeader(&header);
    const frame = try allocator.alloc(u8, frame_len);
    errdefer allocator.free(frame);
    @memcpy(frame[0..header.len], &header);
    if (frame.len > header.len) {
        try reader.readExact(frame[header.len..]);
    }
    return frame;
}

fn decodeClientDataPacketFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32, compressed: bool) !DecodedDataPacket {
    const code = try readClientCodeFromStream(reader);
    if (code != .data) return error.UnexpectedPacket;

    var packet = DecodedDataPacket{};
    if (Feature.temp_tables.enabled(revision)) {
        packet.temp_table = try reader.readStringAlloc(allocator);
    }

    if (compressed) {
        const frame = try readCompressedFrameFromStream(reader, allocator);
        const raw_block = try ch_compress.decompressFrame(allocator, frame);
        var decoder = Decoder.init(raw_block);
        packet.block = try DecodedBlock.decode(&decoder, allocator, revision);
        if (!decoder.eof()) return error.TrailingCompressedBlockData;
        return packet;
    }

    packet.block = try DecodedBlock.decodeFromStream(reader, allocator, revision);
    return packet;
}

fn expectEmptyClientDataPacketFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32, compressed: bool) !void {
    var packet = try decodeClientDataPacketFromStream(reader, allocator, revision, compressed);
    defer packet.deinit(allocator);
    if (!packet.block.isEnd()) return error.TestUnexpectedBlock;
}

const CaptureStreamState = struct {
    reader: *StreamReader,
    captured: std.ArrayList(u8),

    fn init(allocator: std.mem.Allocator, reader: *StreamReader) CaptureStreamState {
        return .{
            .reader = reader,
            .captured = std.ArrayList(u8).init(allocator),
        };
    }

    fn deinit(self: *CaptureStreamState) void {
        self.captured.deinit();
    }
};

fn captureStreamReadAdapter(user_data: ?*anyopaque, buf: []u8) anyerror!usize {
    const state: *CaptureStreamState = @ptrCast(@alignCast(user_data.?));
    const n = try state.reader.readSome(buf);
    try state.captured.appendSlice(buf[0..n]);
    return n;
}

const ReplayStreamState = struct {
    prefix: []const u8,
    prefix_pos: usize = 0,
    reader: *StreamReader,
};

fn replayStreamReadAdapter(user_data: ?*anyopaque, buf: []u8) anyerror!usize {
    const state: *ReplayStreamState = @ptrCast(@alignCast(user_data.?));
    if (state.prefix_pos < state.prefix.len) {
        const remaining = state.prefix.len - state.prefix_pos;
        const n = @min(remaining, buf.len);
        @memcpy(buf[0..n], state.prefix[state.prefix_pos .. state.prefix_pos + n]);
        state.prefix_pos += n;
        return n;
    }
    return state.reader.readSome(buf);
}

fn decodeCompressedDataBlockFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32) !DecodedBlock {
    const compressed_frame = try readCompressedFrameFromStream(reader, allocator);
    defer allocator.free(compressed_frame);
    const raw_block = try ch_compress.decompressFrame(allocator, compressed_frame);
    defer allocator.free(raw_block);
    var decoder = Decoder.init(raw_block);
    var block = try DecodedBlock.decode(&decoder, allocator, revision);
    defer block.deinit(allocator);
    if (!decoder.eof()) return error.TrailingCompressedBlockData;
    return block.cloneOwned(allocator);
}

fn decodeAdaptiveDataBlockFromStream(reader: *StreamReader, allocator: std.mem.Allocator, revision: u32) !DecodedBlock {
    var capture = CaptureStreamState.init(allocator, reader);
    defer capture.deinit();

    var capture_reader = StreamReader.initWithReader(&capture, captureStreamReadAdapter);
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const plain_block = DecodedBlock.decodeFromStream(&capture_reader, arena.allocator(), revision) catch |plain_err| {
        var replay = ReplayStreamState{
            .prefix = capture.captured.items,
            .reader = reader,
        };
        var replay_reader = StreamReader.initWithReader(&replay, replayStreamReadAdapter);
        const block = decodeCompressedDataBlockFromStream(&replay_reader, allocator, revision) catch |compressed_err| switch (compressed_err) {
            error.InvalidCompressedHeader,
            error.CompressedDataTooLarge,
            error.CorruptedCompressedData,
            error.UnsupportedCompressionMethod,
            error.Lz4DecompressionFailed,
            error.ZstdDecompressionFailed,
            error.TrailingCompressedBlockData,
            => return plain_err,
            else => return compressed_err,
        };
        return block;
    };

    if (capturedPacketLooksCompressed(capture.captured.items)) {
        var replay = ReplayStreamState{
            .prefix = capture.captured.items,
            .reader = reader,
        };
        var replay_reader = StreamReader.initWithReader(&replay, replayStreamReadAdapter);
        return try decodeCompressedDataBlockFromStream(&replay_reader, allocator, revision);
    }

    return plain_block.cloneOwned(allocator);
}

fn capturedPacketLooksCompressed(captured: []const u8) bool {
    if (captured.len < ch_compress.header_size) return false;
    _ = ch_compress.decodeFrameHeader(captured[0..ch_compress.header_size]) catch return false;
    return true;
}

fn expectConnectionClosed(reader: *StreamReader) !void {
    var buf: [1]u8 = undefined;
    const n = reader.readSome(&buf) catch |err| switch (err) {
        error.ConnectionResetByPeer,
        error.BrokenPipe,
        error.NotOpenForReading,
        => return,
        else => return err,
    };
    if (n != 0) return error.TestUnexpectedPacket;
}

fn compressionMethodSetting(block_compression: BlockCompression) ?[]const u8 {
    return switch (block_compression) {
        .disabled => null,
        .none => "NONE",
        .lz4 => "LZ4",
        .lz4hc => "LZ4HC",
        .zstd => "ZSTD",
    };
}

fn writeMockServerDataPacket(stream: std.net.Stream, allocator: std.mem.Allocator, revision: u32, code: ServerCode, block: DataBlock, compression: BlockCompression) !void {
    var encoder = Encoder.init(allocator);
    defer encoder.deinit();
    try code.encode(&encoder);
    if (Feature.temp_tables.enabled(revision)) {
        try encoder.putString("");
    }

    if (compression == .disabled) {
        try block.encode(&encoder, revision);
    } else {
        var block_encoder = Encoder.init(allocator);
        defer block_encoder.deinit();
        try block.encode(&block_encoder, revision);
        const compressed = try ch_compress.compressFrame(
            allocator,
            block_encoder.bytes(),
            switch (compression) {
                .disabled => unreachable,
                .none => .none,
                .lz4 => .lz4,
                .lz4hc => .lz4hc,
                .zstd => .zstd,
            },
            0,
        );
        defer allocator.free(compressed);
        try encoder.putRaw(compressed);
    }

    try stream.writeAll(encoder.bytes());
}

const MockServerState = struct {
    err: ?anyerror = null,
    saw_hello: bool = false,
    saw_ping: bool = false,
    saw_query: bool = false,
};

const CompositeMockServerState = struct {
    err: ?anyerror = null,
    state: []const u8,
    payload: []const u8,
    saw_hello: bool = false,
    saw_query: bool = false,
};

const CompressedSelectMockServerState = struct {
    err: ?anyerror = null,
    method: ch_compress.BlockCompression,
    saw_hello: bool = false,
    saw_query: bool = false,
};

const CompressedInsertMockServerState = struct {
    err: ?anyerror = null,
    method: ch_compress.BlockCompression = .zstd,
    saw_hello: bool = false,
    saw_query: bool = false,
    saw_data: bool = false,
    saw_end_of_data: bool = false,
};

const SurfaceMockServerState = struct {
    err: ?anyerror = null,
    saw_hello: bool = false,
    saw_tables_status_request: bool = false,
    saw_ssh_challenge_request: bool = false,
    saw_ssh_challenge_response: bool = false,
};

const SshHandshakeMockServerState = struct {
    err: ?anyerror = null,
    saw_hello: bool = false,
    saw_ssh_challenge_request: bool = false,
    saw_ssh_challenge_response: bool = false,
    saw_quota_key: bool = false,
};

const DoSelectMockServerState = struct {
    err: ?anyerror = null,
    compression: BlockCompression = .disabled,
    server_compression: ?BlockCompression = null,
    saw_hello: bool = false,
    saw_query: bool = false,
};

const DoInsertMockServerState = struct {
    err: ?anyerror = null,
    saw_hello: bool = false,
    saw_query: bool = false,
    saw_first_data: bool = false,
    saw_end_of_data: bool = false,
};

const DoInferInsertMockServerState = struct {
    err: ?anyerror = null,
    saw_hello: bool = false,
    saw_query: bool = false,
    saw_schema: bool = false,
    saw_data: bool = false,
    saw_end_of_data: bool = false,
};

const DoCancelMockServerState = struct {
    err: ?anyerror = null,
    saw_hello: bool = false,
    saw_query: bool = false,
    saw_cancel: bool = false,
};

const DoExceptionMockServerState = struct {
    err: ?anyerror = null,
    saw_hello: bool = false,
    saw_query: bool = false,
};

const DoSenderFailureMockServerState = struct {
    err: ?anyerror = null,
    saw_hello: bool = false,
    saw_query: bool = false,
    saw_disconnect: bool = false,
};

const DoInferMissingSchemaMockServerState = struct {
    err: ?anyerror = null,
    saw_hello: bool = false,
    saw_query: bool = false,
};

const PoolMockServerState = struct {
    err: ?anyerror = null,
    hello_count: usize = 0,
    query_count: usize = 0,
};

const PoolLifecycleMockServerState = struct {
    err: ?anyerror = null,
    expected_hello_count: usize,
    hello_count: usize = 0,
};

fn runMockServer(server: *std.net.Server, state: *MockServerState) void {
    runMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runCompositeMockServer(server: *std.net.Server, state: *CompositeMockServerState) void {
    runCompositeMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runCompressedSelectMockServer(server: *std.net.Server, state: *CompressedSelectMockServerState) void {
    runCompressedSelectMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runCompressedInsertMockServer(server: *std.net.Server, state: *CompressedInsertMockServerState) void {
    runCompressedInsertMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runSurfaceMockServer(server: *std.net.Server, state: *SurfaceMockServerState) void {
    runSurfaceMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runSshHandshakeMockServer(server: *std.net.Server, state: *SshHandshakeMockServerState) void {
    runSshHandshakeMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runDoSelectMockServer(server: *std.net.Server, state: *DoSelectMockServerState) void {
    runDoSelectMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runDoInsertMockServer(server: *std.net.Server, state: *DoInsertMockServerState) void {
    runDoInsertMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runDoInferInsertMockServer(server: *std.net.Server, state: *DoInferInsertMockServerState) void {
    runDoInferInsertMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runDoCancelMockServer(server: *std.net.Server, state: *DoCancelMockServerState) void {
    runDoCancelMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runDoExceptionMockServer(server: *std.net.Server, state: *DoExceptionMockServerState) void {
    runDoExceptionMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runDoSenderFailureMockServer(server: *std.net.Server, state: *DoSenderFailureMockServerState) void {
    runDoSenderFailureMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runDoInferMissingSchemaMockServer(server: *std.net.Server, state: *DoInferMissingSchemaMockServerState) void {
    runDoInferMissingSchemaMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runPoolMockServer(server: *std.net.Server, state: *PoolMockServerState) void {
    runPoolMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runPoolLifecycleMockServer(server: *std.net.Server, state: *PoolLifecycleMockServerState) void {
    runPoolLifecycleMockServerImpl(server, state) catch |err| {
        state.err = err;
    };
}

fn runMockServerImpl(server: *std.net.Server, state: *MockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    const hello = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    if (!std.mem.eql(u8, hello.user, "default")) return error.TestUnexpectedHello;
    state.saw_hello = true;

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 1,
        .revision = default_protocol_version,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    if (Feature.quota_key.enabled(default_protocol_version)) {
        const quota_key = try reader.readStringAlloc(arena.allocator());
        if (quota_key.len != 0) return error.TestUnexpectedQuotaKey;
    }

    const ping_code = try readClientCodeFromStream(&reader);
    if (ping_code != .ping) return error.TestUnexpectedPacket;
    state.saw_ping = true;
    encoder.clearRetainingCapacity();
    try ServerCode.pong.encode(&encoder);
    try conn.stream.writeAll(encoder.bytes());

    var query = try Query.decodePacketFromStream(&reader, arena.allocator(), default_protocol_version);
    defer query.deinit(arena.allocator());
    if (!std.mem.eql(u8, query.body, "SELECT 1")) return error.TestUnexpectedQuery;
    if (findSettingValue(query.settings, "network_compression_method") != null) return error.TestUnexpectedCompressionSetting;
    state.saw_query = true;
    try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, false);

    encoder.clearRetainingCapacity();
    try ServerCode.end_of_stream.encode(&encoder);
    try conn.stream.writeAll(encoder.bytes());
}

fn runCompositeMockServerImpl(server: *std.net.Server, state: *CompositeMockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    _ = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    state.saw_hello = true;

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 1,
        .revision = default_protocol_version,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    if (Feature.quota_key.enabled(default_protocol_version)) {
        _ = try reader.readStringAlloc(arena.allocator());
    }

    var query = try Query.decodePacketFromStream(&reader, arena.allocator(), default_protocol_version);
    defer query.deinit(arena.allocator());
    state.saw_query = true;
    try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, false);

    encoder.clearRetainingCapacity();
    try ServerCode.data.encode(&encoder);
    if (Feature.temp_tables.enabled(default_protocol_version)) {
        try encoder.putString("");
    }
    try (DataBlock{
        .columns = &.{
            .{ .encoded = .{
                .name = "v",
                .type_name = "Array(LowCardinality(String))",
                .rows = 5,
                .state = state.state,
                .payload = state.payload,
            } },
        },
        .rows = 5,
    }).encode(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    encoder.clearRetainingCapacity();
    try ServerCode.end_of_stream.encode(&encoder);
    try conn.stream.writeAll(encoder.bytes());
}

fn runCompressedSelectMockServerImpl(server: *std.net.Server, state: *CompressedSelectMockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    _ = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    state.saw_hello = true;

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 1,
        .revision = default_protocol_version,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    if (Feature.quota_key.enabled(default_protocol_version)) {
        _ = try reader.readStringAlloc(arena.allocator());
    }

    var query = try Query.decodePacketFromStream(&reader, arena.allocator(), default_protocol_version);
    defer query.deinit(arena.allocator());
    if (query.compression != .enabled) return error.TestUnexpectedCompression;
    const expected_method = switch (state.method) {
        .none => "NONE",
        .lz4 => "LZ4",
        .lz4hc => "LZ4HC",
        .zstd => "ZSTD",
    };
    const actual_method = findSettingValue(query.settings, "network_compression_method") orelse return error.TestMissingCompressionSetting;
    if (!std.mem.eql(u8, actual_method, expected_method)) return error.TestUnexpectedCompressionSetting;
    state.saw_query = true;
    try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, true);

    const names = [_][]const u8{ "alpha", "beta" };
    const counts = [_]u64{ 11, 42 };
    const columns = [_]Column{
        .{ .string = .{ .name = "name", .values = &names } },
        .{ .uint64 = .{ .name = "count", .values = &counts } },
    };

    var block_encoder = Encoder.init(std.heap.page_allocator);
    defer block_encoder.deinit();
    try (DataBlock{
        .info = .{ .bucket_num = -1 },
        .columns = &columns,
        .rows = 2,
    }).encode(&block_encoder, default_protocol_version);

    const compressed = try ch_compress.compressFrame(std.heap.page_allocator, block_encoder.bytes(), state.method, 0);
    defer std.heap.page_allocator.free(compressed);

    encoder.clearRetainingCapacity();
    try ServerCode.data.encode(&encoder);
    if (Feature.temp_tables.enabled(default_protocol_version)) {
        try encoder.putString("");
    }
    try encoder.putRaw(compressed);
    try conn.stream.writeAll(encoder.bytes());

    encoder.clearRetainingCapacity();
    try ServerCode.end_of_stream.encode(&encoder);
    try conn.stream.writeAll(encoder.bytes());
}

fn runCompressedInsertMockServerImpl(server: *std.net.Server, state: *CompressedInsertMockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    _ = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    state.saw_hello = true;

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 1,
        .revision = default_protocol_version,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    if (Feature.quota_key.enabled(default_protocol_version)) {
        _ = try reader.readStringAlloc(arena.allocator());
    }

    var query = try Query.decodePacketFromStream(&reader, arena.allocator(), default_protocol_version);
    defer query.deinit(arena.allocator());
    if (query.compression != .enabled) return error.TestUnexpectedCompression;
    const expected_method = switch (state.method) {
        .none => "NONE",
        .lz4 => "LZ4",
        .lz4hc => "LZ4HC",
        .zstd => "ZSTD",
    };
    const actual_method = findSettingValue(query.settings, "network_compression_method") orelse return error.TestMissingCompressionSetting;
    if (!std.mem.eql(u8, actual_method, expected_method)) return error.TestUnexpectedCompressionSetting;
    state.saw_query = true;
    try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, true);

    var packet = try decodeClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, true);
    defer packet.deinit(arena.allocator());
    var block = packet.block;
    packet.block = .{};
    defer block.deinit(arena.allocator());
    state.saw_data = true;

    if (block.rows != 2 or block.columns.len != 1) return error.TestUnexpectedBlock;
    switch (block.columns[0]) {
        .string => |column| {
            if (!std.mem.eql(u8, column.values[0], "left")) return error.TestUnexpectedBlock;
            if (!std.mem.eql(u8, column.values[1], "right")) return error.TestUnexpectedBlock;
        },
        else => return error.TestUnexpectedColumnType,
    }

    var end_packet = try decodeClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, true);
    defer end_packet.deinit(arena.allocator());
    var end_block = end_packet.block;
    end_packet.block = .{};
    defer end_block.deinit(arena.allocator());
    if (!end_block.isEnd()) return error.TestUnexpectedBlock;
    state.saw_end_of_data = true;
}

fn runSurfaceMockServerImpl(server: *std.net.Server, state: *SurfaceMockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    const hello = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    state.saw_hello = true;
    const revision: u32 = @min(hello.protocol_version, @as(u32, 54467));

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 8,
        .revision = 54467,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, revision);
    try conn.stream.writeAll(encoder.bytes());

    if (Feature.quota_key.enabled(revision)) {
        _ = try reader.readStringAlloc(arena.allocator());
    }

    const request = try TablesStatusRequest.decodePacketFromStream(&reader, arena.allocator(), revision);
    if (request.tables.len != 2) return error.TestUnexpectedPacket;
    state.saw_tables_status_request = true;

    encoder.clearRetainingCapacity();
    try (TablesStatusResponse{
        .entries = &.{
            .{
                .table = .{ .database = "db1", .table = "t1" },
                .status = .{ .is_replicated = true, .absolute_delay = 7, .is_readonly = true },
            },
            .{
                .table = .{ .database = "db2", .table = "t2" },
                .status = .{ .is_replicated = false },
            },
        },
    }).encodePacket(&encoder, revision);
    try conn.stream.writeAll(encoder.bytes());

    _ = try SSHChallengeRequest.decodePacketFromStream(&reader);
    state.saw_ssh_challenge_request = true;

    encoder.clearRetainingCapacity();
    try (SSHChallenge{ .challenge = "nonce-42" }).encodePacket(&encoder);
    try conn.stream.writeAll(encoder.bytes());

    const response = try SSHChallengeResponse.decodePacketFromStream(&reader, arena.allocator());
    if (!std.mem.eql(u8, response.signature, "ZmFrZS1zaWc=")) return error.TestUnexpectedPacket;
    state.saw_ssh_challenge_response = true;

    const uuids = [_]UUID{
        .{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 },
        .{ 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 },
    };
    encoder.clearRetainingCapacity();
    try (PartUUIDs{ .uuids = &uuids }).encodePacket(&encoder);
    try conn.stream.writeAll(encoder.bytes());

    encoder.clearRetainingCapacity();
    try (ReadTaskRequest{}).encodePacket(&encoder);
    try conn.stream.writeAll(encoder.bytes());
}

fn runSshHandshakeMockServerImpl(server: *std.net.Server, state: *SshHandshakeMockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    const hello = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    if (!std.mem.eql(u8, hello.user, " SSH KEY AUTHENTICATION default")) return error.TestUnexpectedHello;
    state.saw_hello = true;

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 8,
        .revision = default_protocol_version,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    _ = try SSHChallengeRequest.decodePacketFromStream(&reader);
    state.saw_ssh_challenge_request = true;

    encoder.clearRetainingCapacity();
    try (SSHChallenge{ .challenge = "handshake-challenge" }).encodePacket(&encoder);
    try conn.stream.writeAll(encoder.bytes());

    const response = try SSHChallengeResponse.decodePacketFromStream(&reader, arena.allocator());
    if (!std.mem.eql(u8, response.signature, "c2ln")) return error.TestUnexpectedPacket;
    state.saw_ssh_challenge_response = true;

    if (Feature.quota_key.enabled(default_protocol_version)) {
        const quota_key = try reader.readStringAlloc(arena.allocator());
        if (quota_key.len != 0) return error.TestUnexpectedQuotaKey;
        state.saw_quota_key = true;
    }
}

fn runDoSelectMockServerImpl(server: *std.net.Server, state: *DoSelectMockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    _ = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    state.saw_hello = true;

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 1,
        .revision = default_protocol_version,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    if (Feature.quota_key.enabled(default_protocol_version)) {
        _ = try reader.readStringAlloc(arena.allocator());
    }

    var query = try Query.decodePacketFromStream(&reader, arena.allocator(), default_protocol_version);
    defer query.deinit(arena.allocator());
    if (!std.mem.eql(u8, query.body, "SELECT name, count FROM t")) return error.TestUnexpectedQuery;
    if (compressionMethodSetting(state.compression)) |expected_method| {
        if (query.compression != .enabled) return error.TestUnexpectedCompression;
        const actual_method = findSettingValue(query.settings, "network_compression_method") orelse return error.TestMissingCompressionSetting;
        if (!std.mem.eql(u8, actual_method, expected_method)) return error.TestUnexpectedCompressionSetting;
    }
    state.saw_query = true;
    try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, state.compression != .disabled);
    const server_compression = state.server_compression orelse state.compression;

    encoder.clearRetainingCapacity();
    try (Progress{
        .rows = 2,
        .bytes = 16,
        .total_rows = 2,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    encoder.clearRetainingCapacity();
    try (Profile{
        .rows = 2,
        .bytes = 16,
        .blocks = 1,
    }).encodePacket(&encoder);
    try conn.stream.writeAll(encoder.bytes());

    const names = [_][]const u8{ "alpha", "beta" };
    const counts = [_]u64{ 11, 42 };
    const columns = [_]Column{
        .{ .string = .{ .name = "name", .values = &names } },
        .{ .uint64 = .{ .name = "count", .values = &counts } },
    };

    try writeMockServerDataPacket(conn.stream, std.heap.page_allocator, default_protocol_version, .data, .{
        .info = .{ .bucket_num = -1 },
        .columns = &columns,
        .rows = 2,
    }, server_compression);

    const total_counts = [_]u64{53};
    const total_columns = [_]Column{
        .{ .uint64 = .{ .name = "total_count", .values = &total_counts } },
    };
    try writeMockServerDataPacket(conn.stream, std.heap.page_allocator, default_protocol_version, .totals, .{
        .info = .{ .bucket_num = -1 },
        .columns = &total_columns,
        .rows = 1,
    }, server_compression);

    const extreme_min = [_]u64{11};
    const extreme_max = [_]u64{42};
    const extreme_columns = [_]Column{
        .{ .uint64 = .{ .name = "min_count", .values = &extreme_min } },
        .{ .uint64 = .{ .name = "max_count", .values = &extreme_max } },
    };
    try writeMockServerDataPacket(conn.stream, std.heap.page_allocator, default_protocol_version, .extremes, .{
        .info = .{ .bucket_num = -1 },
        .columns = &extreme_columns,
        .rows = 1,
    }, server_compression);

    const log_times = [_]u32{123};
    const log_micros = [_]u32{456789};
    const log_hosts = [_][]const u8{"host-a"};
    const log_query_ids = [_][]const u8{"query-a"};
    const log_thread_ids = [_]u64{77};
    const log_priorities = [_]i8{2};
    const log_sources = [_][]const u8{"MockSource"};
    const log_texts = [_][]const u8{"mock log"};
    const log_columns = [_]Column{
        try initOwnedFixedColumn(std.heap.page_allocator, "event_time", "DateTime('UTC')", log_times[0..]),
        try initOwnedFixedColumn(std.heap.page_allocator, "event_time_microseconds", "UInt32", log_micros[0..]),
        try initOwnedStringColumn(std.heap.page_allocator, "host_name", log_hosts[0..]),
        try initOwnedStringColumn(std.heap.page_allocator, "query_id", log_query_ids[0..]),
        try initOwnedFixedColumn(std.heap.page_allocator, "thread_id", "UInt64", log_thread_ids[0..]),
        try initOwnedFixedColumn(std.heap.page_allocator, "priority", "Int8", log_priorities[0..]),
        try initOwnedStringColumn(std.heap.page_allocator, "source", log_sources[0..]),
        try initOwnedStringColumn(std.heap.page_allocator, "text", log_texts[0..]),
    };
    defer {
        var owned = log_columns;
        for (&owned) |*column| {
            column.deinit(std.heap.page_allocator);
        }
    }
    try writeMockServerDataPacket(conn.stream, std.heap.page_allocator, default_protocol_version, .log, .{
        .info = .{ .bucket_num = -1 },
        .columns = &log_columns,
        .rows = 1,
    }, server_compression);

    const event_times = [_]u32{222};
    const event_hosts = [_][]const u8{"host-b"};
    const event_thread_ids = [_]u64{88};
    const event_types = [_]i8{1};
    const event_names = [_][]const u8{"RowsRead"};
    const event_values = [_]u64{999};
    const profile_event_columns = [_]Column{
        try initOwnedStringColumn(std.heap.page_allocator, "host_name", event_hosts[0..]),
        try initOwnedFixedColumn(std.heap.page_allocator, "current_time", "DateTime('UTC')", event_times[0..]),
        try initOwnedFixedColumn(std.heap.page_allocator, "thread_id", "UInt64", event_thread_ids[0..]),
        try initOwnedFixedColumn(std.heap.page_allocator, "type", "Int8", event_types[0..]),
        try initOwnedStringColumn(std.heap.page_allocator, "name", event_names[0..]),
        try initOwnedFixedColumn(std.heap.page_allocator, "value", "UInt64", event_values[0..]),
    };
    defer {
        var owned = profile_event_columns;
        for (&owned) |*column| {
            column.deinit(std.heap.page_allocator);
        }
    }
    try writeMockServerDataPacket(conn.stream, std.heap.page_allocator, default_protocol_version, .profile_events, .{
        .info = .{ .bucket_num = -1 },
        .columns = &profile_event_columns,
        .rows = 1,
    }, server_compression);

    encoder.clearRetainingCapacity();
    try ServerCode.end_of_stream.encode(&encoder);
    try conn.stream.writeAll(encoder.bytes());
}

fn runDoInsertMockServerImpl(server: *std.net.Server, state: *DoInsertMockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    _ = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    state.saw_hello = true;

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 1,
        .revision = default_protocol_version,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    if (Feature.quota_key.enabled(default_protocol_version)) {
        _ = try reader.readStringAlloc(arena.allocator());
    }

    var query = try Query.decodePacketFromStream(&reader, arena.allocator(), default_protocol_version);
    defer query.deinit(arena.allocator());
    if (!std.mem.eql(u8, query.body, "INSERT INTO t VALUES")) return error.TestUnexpectedQuery;
    state.saw_query = true;
    try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, false);

    var data_packet = try decodeClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, false);
    defer data_packet.deinit(arena.allocator());
    if (data_packet.block.rows != 2 or data_packet.block.columns.len != 1) return error.TestUnexpectedBlock;
    switch (data_packet.block.columns[0]) {
        .string => |column| {
            if (!std.mem.eql(u8, column.values[0], "left")) return error.TestUnexpectedBlock;
            if (!std.mem.eql(u8, column.values[1], "right")) return error.TestUnexpectedBlock;
        },
        else => return error.TestUnexpectedColumnType,
    }
    state.saw_first_data = true;

    try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, false);
    state.saw_end_of_data = true;

    encoder.clearRetainingCapacity();
    try ServerCode.end_of_stream.encode(&encoder);
    try conn.stream.writeAll(encoder.bytes());
}

fn runDoInferInsertMockServerImpl(server: *std.net.Server, state: *DoInferInsertMockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    _ = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    state.saw_hello = true;

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 1,
        .revision = default_protocol_version,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    if (Feature.quota_key.enabled(default_protocol_version)) {
        _ = try reader.readStringAlloc(arena.allocator());
    }

    var query = try Query.decodePacketFromStream(&reader, arena.allocator(), default_protocol_version);
    defer query.deinit(arena.allocator());
    if (!std.mem.eql(u8, query.body, "INSERT INTO t VALUES")) return error.TestUnexpectedQuery;
    state.saw_query = true;
    try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, false);

    const schema_columns = [_]Column{
        .{ .encoded = .{
            .name = "tags",
            .type_name = "Array(String)",
            .rows = 0,
            .state = "",
            .payload = "",
        } },
    };
    encoder.clearRetainingCapacity();
    try ServerCode.data.encode(&encoder);
    if (Feature.temp_tables.enabled(default_protocol_version)) {
        try encoder.putString("");
    }
    try (DataBlock{
        .info = .{ .bucket_num = -1 },
        .columns = &schema_columns,
        .rows = 0,
    }).encode(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());
    state.saw_schema = true;

    var data_packet = try decodeClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, false);
    defer data_packet.deinit(arena.allocator());
    if (data_packet.block.rows != 2 or data_packet.block.columns.len != 1) return error.TestUnexpectedBlock;
    if (!std.mem.eql(u8, data_packet.block.columns[0].name(), "tags")) return error.TestUnexpectedColumnType;
    if (!std.mem.eql(u8, data_packet.block.columns[0].typeName(), "Array(String)")) return error.TestUnexpectedColumnType;
    var array_view = try data_packet.block.columns[0].asArray(arena.allocator());
    defer array_view.deinit(arena.allocator());
    if (array_view.rows() != 2) return error.TestUnexpectedBlock;
    switch (array_view.values) {
        .string => |column| {
            if (!std.mem.eql(u8, column.values[0], "a")) return error.TestUnexpectedBlock;
            if (!std.mem.eql(u8, column.values[1], "b")) return error.TestUnexpectedBlock;
            if (!std.mem.eql(u8, column.values[2], "c")) return error.TestUnexpectedBlock;
        },
        else => return error.TestUnexpectedColumnType,
    }
    state.saw_data = true;

    try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, false);
    state.saw_end_of_data = true;

    encoder.clearRetainingCapacity();
    try ServerCode.end_of_stream.encode(&encoder);
    try conn.stream.writeAll(encoder.bytes());
}

fn runDoCancelMockServerImpl(server: *std.net.Server, state: *DoCancelMockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    _ = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    state.saw_hello = true;

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 1,
        .revision = default_protocol_version,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    if (Feature.quota_key.enabled(default_protocol_version)) {
        _ = try reader.readStringAlloc(arena.allocator());
    }

    var query = try Query.decodePacketFromStream(&reader, arena.allocator(), default_protocol_version);
    defer query.deinit(arena.allocator());
    if (!std.mem.eql(u8, query.body, "SELECT cancel_me")) return error.TestUnexpectedQuery;
    state.saw_query = true;
    try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, false);

    encoder.clearRetainingCapacity();
    try (Progress{
        .rows = 1,
        .bytes = 8,
        .total_rows = 10,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    const cancel_code = try readClientCodeFromStream(&reader);
    if (cancel_code != .cancel) return error.TestUnexpectedPacket;
    state.saw_cancel = true;
}

fn runDoExceptionMockServerImpl(server: *std.net.Server, state: *DoExceptionMockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    _ = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    state.saw_hello = true;

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 1,
        .revision = default_protocol_version,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    if (Feature.quota_key.enabled(default_protocol_version)) {
        _ = try reader.readStringAlloc(arena.allocator());
    }

    var query = try Query.decodePacketFromStream(&reader, arena.allocator(), default_protocol_version);
    defer query.deinit(arena.allocator());
    if (!std.mem.eql(u8, query.body, "SELECT boom()")) return error.TestUnexpectedQuery;
    state.saw_query = true;
    try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, false);

    encoder.clearRetainingCapacity();
    try (Exception{
        .code = 60,
        .name = "DB::Exception",
        .message = "DB::Exception: mock failure",
        .stack = "stack",
        .nested = false,
    }).encodePacket(&encoder);
    try conn.stream.writeAll(encoder.bytes());
}

fn runDoSenderFailureMockServerImpl(server: *std.net.Server, state: *DoSenderFailureMockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    _ = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    state.saw_hello = true;

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 1,
        .revision = default_protocol_version,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    if (Feature.quota_key.enabled(default_protocol_version)) {
        _ = try reader.readStringAlloc(arena.allocator());
    }

    var query = try Query.decodePacketFromStream(&reader, arena.allocator(), default_protocol_version);
    defer query.deinit(arena.allocator());
    if (!std.mem.eql(u8, query.body, "INSERT INTO t VALUES")) return error.TestUnexpectedQuery;
    state.saw_query = true;
    try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, false);
    const next_code = readClientCodeFromStream(&reader) catch |err| switch (err) {
        error.UnexpectedEof,
        error.ConnectionResetByPeer,
        => {
            state.saw_disconnect = true;
            return;
        },
        else => return err,
    };
    if (next_code != .cancel) return error.TestUnexpectedPacket;
    try expectConnectionClosed(&reader);
    state.saw_disconnect = true;
}

fn runDoInferMissingSchemaMockServerImpl(server: *std.net.Server, state: *DoInferMissingSchemaMockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    _ = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    state.saw_hello = true;

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 1,
        .revision = default_protocol_version,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    if (Feature.quota_key.enabled(default_protocol_version)) {
        _ = try reader.readStringAlloc(arena.allocator());
    }

    var query = try Query.decodePacketFromStream(&reader, arena.allocator(), default_protocol_version);
    defer query.deinit(arena.allocator());
    if (!std.mem.eql(u8, query.body, "INSERT INTO t VALUES")) return error.TestUnexpectedQuery;
    state.saw_query = true;
    try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, false);

    encoder.clearRetainingCapacity();
    try ServerCode.end_of_stream.encode(&encoder);
    try conn.stream.writeAll(encoder.bytes());
}

fn runPoolMockServerImpl(server: *std.net.Server, state: *PoolMockServerState) !void {
    const conn = try server.accept();
    defer conn.stream.close();

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();

    var reader = StreamReader.init(conn.stream);
    _ = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
    state.hello_count += 1;

    var encoder = Encoder.init(std.heap.page_allocator);
    defer encoder.deinit();

    try (ServerHello{
        .name = "ClickHouse server",
        .major = 24,
        .minor = 1,
        .revision = default_protocol_version,
        .timezone = "UTC",
        .display_name = "mock",
        .patch = 1,
    }).encodePacket(&encoder, default_protocol_version);
    try conn.stream.writeAll(encoder.bytes());

    if (Feature.quota_key.enabled(default_protocol_version)) {
        _ = try reader.readStringAlloc(arena.allocator());
    }

    for (0..2) |_| {
        var query = try Query.decodePacketFromStream(&reader, arena.allocator(), default_protocol_version);
        defer query.deinit(arena.allocator());
        if (!std.mem.eql(u8, query.body, "SELECT pooled")) return error.TestUnexpectedQuery;
        state.query_count += 1;
        try expectEmptyClientDataPacketFromStream(&reader, arena.allocator(), default_protocol_version, false);

        encoder.clearRetainingCapacity();
        try ServerCode.end_of_stream.encode(&encoder);
        try conn.stream.writeAll(encoder.bytes());
    }
}

fn runPoolLifecycleMockServerImpl(server: *std.net.Server, state: *PoolLifecycleMockServerState) !void {
    var handled: usize = 0;
    while (handled < state.expected_hello_count) : (handled += 1) {
        const conn = try server.accept();
        defer conn.stream.close();

        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();

        var reader = StreamReader.init(conn.stream);
        _ = try ClientHello.decodePacketFromStream(&reader, arena.allocator());
        state.hello_count += 1;

        var encoder = Encoder.init(std.heap.page_allocator);
        defer encoder.deinit();

        try (ServerHello{
            .name = "ClickHouse server",
            .major = 24,
            .minor = 1,
            .revision = default_protocol_version,
            .timezone = "UTC",
            .display_name = "mock",
            .patch = 1,
        }).encodePacket(&encoder, default_protocol_version);
        try conn.stream.writeAll(encoder.bytes());

        if (Feature.quota_key.enabled(default_protocol_version)) {
            _ = try reader.readStringAlloc(arena.allocator());
        }

        var buf: [256]u8 = undefined;
        while (true) {
            const n = conn.stream.read(&buf) catch |err| switch (err) {
                error.ConnectionResetByPeer => break,
                else => return err,
            };
            if (n == 0) break;
        }
    }
}

fn concatBytes(allocator: std.mem.Allocator, parts: []const []const u8) ![]u8 {
    var total: usize = 0;
    for (parts) |part| {
        total = try std.math.add(usize, total, part.len);
    }

    const out = try allocator.alloc(u8, total);
    var offset: usize = 0;
    for (parts) |part| {
        @memcpy(out[offset .. offset + part.len], part);
        offset += part.len;
    }
    return out;
}

fn lowCardinalityStatePrefix(allocator: std.mem.Allocator, versions: usize) ![]u8 {
    var encoder = Encoder.init(allocator);
    errdefer encoder.deinit();
    for (0..versions) |_| {
        try encoder.putInt64LE(1);
    }
    return encoder.buf.toOwnedSlice();
}

fn expectColumnFixtureRoundtrip(type_name: []const u8, rows: usize, fixture_path: []const u8, state_prefix: []const u8) !void {
    const payload = try readFixture(std.testing.allocator, fixture_path);
    defer std.testing.allocator.free(payload);

    const input = try concatBytes(std.testing.allocator, &.{ state_prefix, payload });
    defer std.testing.allocator.free(input);

    var decoder = Decoder.init(input);
    var column = try Column.decode(std.testing.allocator, &decoder, "v", type_name, rows);
    defer column.deinit(std.testing.allocator);

    try std.testing.expectEqual(rows, column.rowCount());
    try std.testing.expect(decoder.eof());

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();
    try column.encodeState(&encoder);
    try column.encodeValues(&encoder);
    try std.testing.expectEqualSlices(u8, input, encoder.bytes());
}

fn decodeFixtureColumn(allocator: std.mem.Allocator, type_name: []const u8, rows: usize, fixture_path: []const u8, state_prefix: []const u8) !Column {
    const payload = try readFixture(allocator, fixture_path);
    defer allocator.free(payload);

    const input = try concatBytes(allocator, &.{ state_prefix, payload });
    defer allocator.free(input);

    var decoder = Decoder.init(input);
    var column = try Column.decode(allocator, &decoder, "v", type_name, rows);
    errdefer column.deinit(allocator);
    if (!decoder.eof()) return error.TrailingColumnData;
    return column;
}

fn encodeColumnStateAndValues(allocator: std.mem.Allocator, column: Column) ![]u8 {
    var encoder = Encoder.init(allocator);
    errdefer encoder.deinit();
    try column.encodeState(&encoder);
    try column.encodeValues(&encoder);
    return encoder.buf.toOwnedSlice();
}

fn makeQueryInfo() ClientInfo {
    return .{
        .protocol_version = default_protocol_version,
        .major = 0,
        .minor = 1,
    };
}

fn mockSshSigner(message: []const u8, challenge: []const u8, out: *std.ArrayList(u8)) !void {
    if (!std.mem.eql(u8, challenge, "handshake-challenge")) return error.TestUnexpectedPacket;
    const expected_message = try std.fmt.allocPrint(std.testing.allocator, "{d}{s}{s}{s}", .{
        default_protocol_version,
        "default",
        "default",
        "handshake-challenge",
    });
    defer std.testing.allocator.free(expected_message);
    try std.testing.expectEqualStrings(expected_message, message);
    try out.appendSlice("sig");
}

fn makeClientForSettings(allocator: std.mem.Allocator, compression: BlockCompression) Client {
    return .{
        .allocator = allocator,
        .stream = undefined,
        .stream_closed = false,
        .tls_client = null,
        .tls_ca_bundle = null,
        .protocol_version = default_protocol_version,
        .hello = undefined,
        .server = undefined,
        .query_defaults = .{
            .protocol_version = default_protocol_version,
            .major = 0,
            .minor = 1,
        },
        .quota_key = "",
        .default_query_compression = if (compression == .disabled) .disabled else .enabled,
        .active_query_compression = .disabled,
        .block_compression = compression,
        .block_compression_level = 0,
        .ssh_signer = null,
        .ssh_auth_user = "",
        .owned_hello_user = &.{},
        .read_timeout_ms = 0,
        .write_timeout_ms = 0,
        .handshake_timeout_ms = 0,
        .tls_enabled = false,
        .tls_server_name = "",
        .observer = .{},
        .server_storage = std.heap.ArenaAllocator.init(allocator),
        .last_exception_storage = std.heap.ArenaAllocator.init(allocator),
        .last_exception = null,
    };
}

const DoResultState = struct {
    result_calls: usize = 0,
    totals_calls: usize = 0,
    extremes_calls: usize = 0,
    progress_calls: usize = 0,
    profile_calls: usize = 0,
    log_batch_calls: usize = 0,
    log_calls: usize = 0,
    profile_events_batch_calls: usize = 0,
    profile_event_calls: usize = 0,
};

fn doResultStateFromContext(ctx: QueryContext) *DoResultState {
    return @ptrCast(@alignCast(ctx.user_data.?));
}

fn onDoResult(ctx: QueryContext, block: *const DecodedBlock) !void {
    var state = doResultStateFromContext(ctx);
    state.result_calls += 1;
    switch (block.columns[0]) {
        .string => |column| {
            try std.testing.expectEqualStrings("alpha", column.values[0]);
            try std.testing.expectEqualStrings("beta", column.values[1]);
        },
        else => return error.TestUnexpectedColumnType,
    }
    switch (block.columns[1]) {
        .uint64 => |column| {
            try std.testing.expectEqual(@as(u64, 11), column.values[0]);
            try std.testing.expectEqual(@as(u64, 42), column.values[1]);
        },
        else => return error.TestUnexpectedColumnType,
    }
}

fn onDoTotals(ctx: QueryContext, block: *const DecodedBlock) !void {
    var state = doResultStateFromContext(ctx);
    state.totals_calls += 1;
    try std.testing.expectEqual(@as(usize, 1), block.rows);
    switch (block.columns[0]) {
        .uint64 => |column| try std.testing.expectEqual(@as(u64, 53), column.values[0]),
        else => return error.TestUnexpectedColumnType,
    }
}

fn onDoExtremes(ctx: QueryContext, block: *const DecodedBlock) !void {
    var state = doResultStateFromContext(ctx);
    state.extremes_calls += 1;
    try std.testing.expectEqual(@as(usize, 1), block.rows);
    switch (block.columns[0]) {
        .uint64 => |column| try std.testing.expectEqual(@as(u64, 11), column.values[0]),
        else => return error.TestUnexpectedColumnType,
    }
    switch (block.columns[1]) {
        .uint64 => |column| try std.testing.expectEqual(@as(u64, 42), column.values[0]),
        else => return error.TestUnexpectedColumnType,
    }
}

fn onDoProgress(ctx: QueryContext, progress: Progress) !void {
    var state = doResultStateFromContext(ctx);
    state.progress_calls += 1;
    try std.testing.expectEqual(@as(u64, 2), progress.rows);
}

fn onDoProfile(ctx: QueryContext, profile: Profile) !void {
    var state = doResultStateFromContext(ctx);
    state.profile_calls += 1;
    try std.testing.expectEqual(@as(u64, 2), profile.rows);
}

fn onDoLogsBatch(ctx: QueryContext, logs: []const ServerLog) !void {
    var state = doResultStateFromContext(ctx);
    state.log_batch_calls += 1;
    try std.testing.expectEqual(@as(usize, 1), logs.len);
    try std.testing.expectEqualStrings("host-a", logs[0].host);
    try std.testing.expectEqualStrings("query-a", logs[0].query_id);
    try std.testing.expectEqualStrings("MockSource", logs[0].source);
    try std.testing.expectEqualStrings("mock log", logs[0].text);
    try std.testing.expectEqual(@as(u64, 77), logs[0].thread_id);
    try std.testing.expectEqual(@as(i8, 2), logs[0].priority);
}

fn onDoLog(ctx: QueryContext, log: ServerLog) !void {
    var state = doResultStateFromContext(ctx);
    state.log_calls += 1;
    try std.testing.expectEqualStrings("mock log", log.text);
}

fn onDoProfileEventsBatch(ctx: QueryContext, events: []const ProfileEvent) !void {
    var state = doResultStateFromContext(ctx);
    state.profile_events_batch_calls += 1;
    try std.testing.expectEqual(@as(usize, 1), events.len);
    try std.testing.expectEqual(.increment, events[0].event_type);
    try std.testing.expectEqualStrings("RowsRead", events[0].name);
    try std.testing.expectEqual(@as(i64, 999), events[0].value);
}

fn onDoProfileEvent(ctx: QueryContext, event: ProfileEvent) !void {
    var state = doResultStateFromContext(ctx);
    state.profile_event_calls += 1;
    try std.testing.expectEqualStrings("host-b", event.host);
}

const CancelState = struct {
    canceled: bool = false,
    progress_calls: usize = 0,
};

fn cancelStateFromContext(ctx: QueryContext) *CancelState {
    return @ptrCast(@alignCast(ctx.user_data.?));
}

fn cancelRequested(user_data: ?*anyopaque) bool {
    const state: *CancelState = @ptrCast(@alignCast(user_data.?));
    return state.canceled;
}

fn onCancelProgress(ctx: QueryContext, progress: Progress) !void {
    _ = progress;
    var state = cancelStateFromContext(ctx);
    state.progress_calls += 1;
    state.canceled = true;
}

const StreamingInputState = struct {
    stage: u8 = 0,
    columns: [1]Column,
};

fn streamingInputStateFromContext(ctx: QueryContext) *StreamingInputState {
    return @ptrCast(@alignCast(ctx.user_data.?));
}

fn onStreamingInput(ctx: QueryContext, query: *Query) !void {
    const state = streamingInputStateFromContext(ctx);
    switch (state.stage) {
        0 => {
            query.input = state.columns[0..];
            state.stage = 1;
        },
        1 => {
            query.input = &.{};
            state.stage = 2;
            return error.EndOfInput;
        },
        else => return error.EndOfInput,
    }
}

fn onFailingInput(ctx: QueryContext, query: *Query) !void {
    _ = ctx;
    _ = query;
    return error.TestInputFailure;
}

const ObserverState = struct {
    connect_start_calls: usize = 0,
    connect_finish_calls: usize = 0,
    query_start_calls: usize = 0,
    query_finish_calls: usize = 0,
    progress_calls: usize = 0,
    profile_calls: usize = 0,
    exception_calls: usize = 0,
    last_finish_metrics: QueryMetrics = .{},
};

fn observerStateConnect(event: ConnectObserveEvent, user_data: ?*anyopaque) void {
    const state: *ObserverState = @ptrCast(@alignCast(user_data.?));
    switch (event) {
        .start => state.connect_start_calls += 1,
        .finish => state.connect_finish_calls += 1,
    }
}

fn observerStateQuery(event: QueryObserveEvent, user_data: ?*anyopaque) void {
    const state: *ObserverState = @ptrCast(@alignCast(user_data.?));
    switch (event) {
        .start => state.query_start_calls += 1,
        .progress => state.progress_calls += 1,
        .profile => state.profile_calls += 1,
        .exception => state.exception_calls += 1,
        .finish => |finish| {
            state.query_finish_calls += 1;
            state.last_finish_metrics = finish.metrics;
        },
    }
}

var test_dialer_called = false;

fn testDialer(allocator: std.mem.Allocator, host: []const u8, port: u16) !std.net.Stream {
    test_dialer_called = true;
    return std.net.tcpConnectToHost(allocator, host, port);
}

const PoolAcquireWaiterState = struct {
    pool: *Pool,
    acquired: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    err: ?anyerror = null,
};

fn runPoolAcquireWaiter(state: *PoolAcquireWaiterState) void {
    var conn = state.pool.acquire(.{}) catch |err| {
        state.err = err;
        return;
    };
    defer conn.release();
    state.acquired.store(true, .release);
}

fn expectCompressedFixture(method: ch_compress.MethodEncoding, fixture_path: []const u8, raw: []const u8) !void {
    const frame = try readFixture(std.testing.allocator, fixture_path);
    defer std.testing.allocator.free(frame);

    const header = try ch_compress.decodeFrameHeader(frame[0..ch_compress.header_size]);
    try std.testing.expectEqual(method, header.method);
    try std.testing.expectEqual(raw.len, header.data_size);
    try std.testing.expectEqual(frame.len, try ch_compress.frameLengthFromHeader(frame[0..ch_compress.header_size]));

    const decompressed = try ch_compress.decompressFrame(std.testing.allocator, frame);
    defer std.testing.allocator.free(decompressed);
    try std.testing.expectEqualSlices(u8, raw, decompressed);
}

test "client hello matches golden bytes" {
    const golden = try readFixture(std.testing.allocator, "proto/_golden/client_hello.raw");
    defer std.testing.allocator.free(golden);

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();

    const hello = ClientHello{
        .name = "ch",
        .major = 1,
        .minor = 1,
        .protocol_version = 41000,
        .database = "github",
        .user = "neo",
        .password = "",
    };
    try hello.encodePacket(&encoder);
    try std.testing.expectEqualSlices(u8, golden, encoder.bytes());

    var decoder = Decoder.init(golden);
    const decoded = try ClientHello.decodePacket(&decoder);
    try std.testing.expectEqualStrings("ch", decoded.name);
    try std.testing.expectEqual(@as(u32, 1), decoded.major);
    try std.testing.expectEqual(@as(u32, 1), decoded.minor);
    try std.testing.expectEqual(@as(u32, 41000), decoded.protocol_version);
    try std.testing.expectEqualStrings("github", decoded.database);
    try std.testing.expectEqualStrings("neo", decoded.user);
    try std.testing.expectEqualStrings("", decoded.password);
    try std.testing.expect(decoder.eof());
}

test "exception matches golden bytes" {
    const golden = try readFixture(std.testing.allocator, "proto/_golden/type/Exception.raw");
    defer std.testing.allocator.free(golden);

    var decoder = Decoder.init(golden);
    const exception = try Exception.decodePayload(&decoder);
    try std.testing.expectEqual(@as(i32, 60), exception.code);
    try std.testing.expectEqualStrings("DB::Exception", exception.name);
    try std.testing.expect(exception.nested == false);

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();
    try exception.encodeAware(&encoder);
    try std.testing.expectEqualSlices(u8, golden, encoder.bytes());
}

test "progress matches golden bytes" {
    const golden = try readFixture(std.testing.allocator, "proto/_golden/progress.raw");
    defer std.testing.allocator.free(golden);

    const progress = Progress{
        .rows = 100,
        .bytes = 608120,
        .total_rows = 1000,
        .wrote_rows = 441,
        .wrote_bytes = 91023,
    };

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();
    try progress.encodeAware(&encoder, default_protocol_version);
    try std.testing.expectEqualSlices(u8, golden, encoder.bytes());

    var decoder = Decoder.init(golden);
    const decoded = try Progress.decodePayload(&decoder, default_protocol_version);
    try std.testing.expectEqual(progress.rows, decoded.rows);
    try std.testing.expectEqual(progress.bytes, decoded.bytes);
    try std.testing.expectEqual(progress.total_rows, decoded.total_rows);
    try std.testing.expectEqual(progress.wrote_rows, decoded.wrote_rows);
    try std.testing.expectEqual(progress.wrote_bytes, decoded.wrote_bytes);
}

test "profile matches golden bytes" {
    const golden = try readFixture(std.testing.allocator, "proto/_golden/type/Profile.raw");
    defer std.testing.allocator.free(golden);

    const profile = Profile{
        .rows = 1234,
        .blocks = 235123,
        .bytes = 424,
        .applied_limit = true,
        .rows_before_limit = 2341,
        .calculated_rows_before_limit = false,
    };

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();
    try profile.encodePacket(&encoder);
    try std.testing.expectEqualSlices(u8, golden, encoder.bytes());

    var decoder = Decoder.init(golden);
    const code = try ServerCode.decode(&decoder);
    try std.testing.expectEqual(ServerCode.profile, code);
    const decoded = try Profile.decodePayload(&decoder);
    try std.testing.expectEqual(profile.rows, decoded.rows);
    try std.testing.expectEqual(profile.blocks, decoded.blocks);
    try std.testing.expectEqual(profile.bytes, decoded.bytes);
}

test "table columns matches golden bytes" {
    const golden = try readFixture(std.testing.allocator, "proto/_golden/type/TableColumns.raw");
    defer std.testing.allocator.free(golden);

    const table_columns = TableColumns{
        .first = "",
        .second = "columns format version: 1\n1 columns:\n`id` UInt8\n",
    };

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();
    try table_columns.encodePacket(&encoder);
    try std.testing.expectEqualSlices(u8, golden, encoder.bytes());

    var decoder = Decoder.init(golden);
    const code = try ServerCode.decode(&decoder);
    try std.testing.expectEqual(ServerCode.table_columns, code);
    const decoded = try TableColumns.decodePayload(&decoder);
    try std.testing.expectEqualStrings(table_columns.first, decoded.first);
    try std.testing.expectEqualStrings(table_columns.second, decoded.second);
}

test "tables status request and response roundtrip" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();

    const request = TablesStatusRequest{
        .tables = &.{
            .{ .database = "db1", .table = "t1" },
            .{ .database = "db2", .table = "t2" },
        },
    };
    try request.encodePacket(&encoder, 54467);

    var decoder = Decoder.init(encoder.bytes());
    const decoded_request = try TablesStatusRequest.decodePacket(&decoder, arena.allocator(), 54467);
    try std.testing.expectEqual(@as(usize, 2), decoded_request.tables.len);
    try std.testing.expectEqualStrings("db1", decoded_request.tables[0].database);
    try std.testing.expectEqualStrings("t2", decoded_request.tables[1].table);
    try std.testing.expect(decoder.eof());

    encoder.clearRetainingCapacity();
    const response = TablesStatusResponse{
        .entries = &.{
            .{
                .table = .{ .database = "db1", .table = "t1" },
                .status = .{ .is_replicated = true, .absolute_delay = 9, .is_readonly = true },
            },
            .{
                .table = .{ .database = "db2", .table = "t2" },
                .status = .{ .is_replicated = false },
            },
        },
    };
    try response.encodePacket(&encoder, 54467);

    decoder = Decoder.init(encoder.bytes());
    const code = try ServerCode.decode(&decoder);
    try std.testing.expectEqual(ServerCode.tables_status, code);
    const decoded_response = try TablesStatusResponse.decodePayload(&decoder, arena.allocator(), 54467);
    try std.testing.expectEqual(@as(usize, 2), decoded_response.entries.len);
    try std.testing.expect(decoded_response.entries[0].status.is_replicated);
    try std.testing.expect(decoded_response.entries[0].status.is_readonly);
    try std.testing.expectEqual(@as(u32, 9), decoded_response.entries[0].status.absolute_delay);
    try std.testing.expect(!decoded_response.entries[1].status.is_replicated);
    try std.testing.expect(decoder.eof());
}

test "ssh challenge and response roundtrip" {
    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();

    try (SSHChallengeRequest{}).encodePacket(&encoder);
    var decoder = Decoder.init(encoder.bytes());
    _ = try SSHChallengeRequest.decodePacket(&decoder);
    try std.testing.expect(decoder.eof());

    encoder.clearRetainingCapacity();
    try (SSHChallenge{ .challenge = "nonce" }).encodePacket(&encoder);
    decoder = Decoder.init(encoder.bytes());
    const code = try ServerCode.decode(&decoder);
    try std.testing.expectEqual(ServerCode.ssh_challenge, code);
    const challenge = try SSHChallenge.decodePayload(&decoder);
    try std.testing.expectEqualStrings("nonce", challenge.challenge);
    try std.testing.expect(decoder.eof());

    encoder.clearRetainingCapacity();
    try (SSHChallengeResponse{ .signature = "c2ln" }).encodePacket(&encoder);
    decoder = Decoder.init(encoder.bytes());
    const decoded_response = try SSHChallengeResponse.decodePacket(&decoder);
    try std.testing.expectEqualStrings("c2ln", decoded_response.signature);
    try std.testing.expect(decoder.eof());
}

test "part uuids roundtrip" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const uuids = [_]UUID{
        .{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },
        .{ 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 },
    };

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();
    try (PartUUIDs{ .uuids = &uuids }).encodePacket(&encoder);

    var decoder = Decoder.init(encoder.bytes());
    const code = try ServerCode.decode(&decoder);
    try std.testing.expectEqual(ServerCode.part_uuids, code);
    const packet = try PartUUIDs.decodePayload(&decoder, arena.allocator());
    try std.testing.expectEqual(@as(usize, 2), packet.uuids.len);
    try std.testing.expectEqualSlices(u8, &uuids[0], &packet.uuids[0]);
    try std.testing.expect(decoder.eof());
}

test "server hello matches golden bytes" {
    const golden = try readFixture(std.testing.allocator, "proto/_golden/type/ServerHello.raw");
    defer std.testing.allocator.free(golden);

    var decoder = Decoder.init(golden);
    const decoded = try ServerHello.decodePacket(&decoder, default_protocol_version);
    try std.testing.expectEqualStrings("ClickHouse server", decoded.name);
    try std.testing.expectEqual(@as(u32, 21), decoded.major);
    try std.testing.expectEqual(@as(u32, 11), decoded.minor);
    try std.testing.expectEqual(@as(u32, 54450), decoded.revision);
    try std.testing.expectEqualStrings("Europe/Moscow", decoded.timezone);
    try std.testing.expectEqualStrings("alpha", decoded.display_name);
    try std.testing.expectEqual(@as(u32, 3), decoded.patch);
    try std.testing.expect(decoder.eof());

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();
    try decoded.encodePacket(&encoder, default_protocol_version);
    try std.testing.expectEqualSlices(u8, golden, encoder.bytes());
}

test "client info matches golden bytes" {
    const golden = try readFixture(std.testing.allocator, "proto/_golden/client_info.raw");
    defer std.testing.allocator.free(golden);

    const info = ClientInfo{
        .protocol_version = 54450,
        .major = 21,
        .minor = 11,
        .patch = 4,
        .interface = .tcp,
        .query_kind = .initial,
        .initial_user = "",
        .initial_query_id = "23ad2c07-2f68-4005-9bac-da8f467bdd3b",
        .initial_address = "0.0.0.0:0",
        .os_user = "ernado",
        .client_hostname = "nexus",
        .client_name = "ClickHouse ",
        .quota_key = "",
        .distributed_depth = 0,
    };

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();
    try info.encodeAware(&encoder, 54450);
    try std.testing.expectEqualSlices(u8, golden, encoder.bytes());

    var decoder = Decoder.init(golden);
    const decoded = try ClientInfo.decodeAware(&decoder, 54450);
    try std.testing.expectEqualStrings(info.initial_query_id, decoded.initial_query_id);
    try std.testing.expectEqualStrings(info.os_user, decoded.os_user);
    try std.testing.expectEqualStrings(info.client_hostname, decoded.client_hostname);
    try std.testing.expectEqualStrings(info.client_name, decoded.client_name);
    try std.testing.expectEqual(info.protocol_version, decoded.protocol_version);
    try std.testing.expectEqual(info.patch, decoded.patch);
    try std.testing.expect(decoder.eof());
}

test "client info open telemetry matches golden bytes" {
    const golden = try readFixture(std.testing.allocator, "proto/_golden/client_info_otel.raw");
    defer std.testing.allocator.free(golden);

    const info = ClientInfo{
        .protocol_version = 54429,
        .major = 21,
        .minor = 11,
        .patch = 4,
        .interface = .tcp,
        .query_kind = .initial,
        .initial_user = "",
        .initial_query_id = "40c268ad-de50-434d-a391-800da9aa70c3",
        .initial_address = "0.0.0.0:0",
        .os_user = "user",
        .client_hostname = "hostname",
        .client_name = "Name",
        .trace = .{
            .trace_id = .{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
            .span_id = .{ 1, 2, 3, 4, 5, 6, 7, 8 },
        },
    };

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();
    try info.encodeAware(&encoder, default_protocol_version);
    try std.testing.expectEqualSlices(u8, golden, encoder.bytes());

    var decoder = Decoder.init(golden);
    const decoded = try ClientInfo.decodeAware(&decoder, default_protocol_version);
    try std.testing.expect(decoded.trace != null);
    const trace = decoded.trace.?;
    try std.testing.expectEqualSlices(u8, &info.trace.?.trace_id, &trace.trace_id);
    try std.testing.expectEqualSlices(u8, &info.trace.?.span_id, &trace.span_id);
    try std.testing.expect(decoder.eof());
}

test "query matches golden bytes" {
    const golden = try readFixture(std.testing.allocator, "proto/_golden/query_create_db.raw");
    defer std.testing.allocator.free(golden);

    const query = Query{
        .id = "23ad2c07-2f68-4005-9bac-da8f467bdd3b",
        .body = "CREATE DATABASE test;",
        .secret = "",
        .stage = .complete,
        .compression = .disabled,
        .info = .{
            .protocol_version = 54450,
            .major = 21,
            .minor = 11,
            .patch = 4,
            .interface = .tcp,
            .query_kind = .initial,
            .initial_user = "",
            .initial_query_id = "23ad2c07-2f68-4005-9bac-da8f467bdd3b",
            .initial_address = "0.0.0.0:0",
            .os_user = "ernado",
            .client_hostname = "nexus",
            .client_name = "ClickHouse ",
            .quota_key = "",
            .distributed_depth = 0,
        },
    };

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();
    try query.encodePacket(&encoder, 54450);
    try std.testing.expectEqualSlices(u8, golden, encoder.bytes());

    var decoder = Decoder.init(golden);
    var decoded = try Query.decodePacket(&decoder, std.testing.allocator, 54450);
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(query.id, decoded.id);
    try std.testing.expectEqualStrings(query.body, decoded.body);
    try std.testing.expectEqualStrings(query.info.os_user, decoded.info.os_user);
    try std.testing.expectEqual(query.stage, decoded.stage);
    try std.testing.expectEqual(query.compression, decoded.compression);
    try std.testing.expect(decoder.eof());
}

test "raw block matches golden bytes" {
    const golden = try readFixture(std.testing.allocator, "proto/_golden/block_title_data.raw");
    defer std.testing.allocator.free(golden);

    const titles = [_][]const u8{ "Foo", "Bar" };
    const data = [_]i64{ 1, 2 };
    const columns = [_]Column{
        .{ .string = .{ .name = "title", .values = &titles } },
        .{ .int64 = .{ .name = "data", .values = &data } },
    };

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();
    try (DataBlock{ .columns = &columns, .rows = 2 }).encodeRaw(&encoder, default_protocol_version);
    try std.testing.expectEqualSlices(u8, golden, encoder.bytes());

    var decoder = Decoder.init(golden);
    var block = try DecodedBlock.decodeRaw(&decoder, std.testing.allocator, default_protocol_version);
    defer block.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), block.rows);
    try std.testing.expectEqual(@as(usize, 2), block.columns.len);

    switch (block.columns[0]) {
        .string => |column| {
            try std.testing.expectEqualStrings("title", column.name);
            try std.testing.expectEqualStrings("Foo", column.values[0]);
            try std.testing.expectEqualStrings("Bar", column.values[1]);
        },
        else => return error.TestUnexpectedColumnType,
    }
    switch (block.columns[1]) {
        .int64 => |column| {
            try std.testing.expectEqualStrings("data", column.name);
            try std.testing.expectEqual(@as(i64, 1), column.values[0]);
            try std.testing.expectEqual(@as(i64, 2), column.values[1]);
        },
        else => return error.TestUnexpectedColumnType,
    }
    try std.testing.expect(decoder.eof());
}

test "full block matches golden bytes" {
    const golden = try readFixture(std.testing.allocator, "proto/_golden/block_int8_uint64.raw");
    defer std.testing.allocator.free(golden);

    const count = [_]i8{ 1, 2, 3, 4, 5 };
    const users = [_]u64{ 5467267, 175676, 956105, 18347896, 554714 };
    const columns = [_]Column{
        .{ .int8 = .{ .name = "count", .values = &count } },
        .{ .uint64 = .{ .name = "users", .values = &users } },
    };

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();
    try (DataBlock{
        .info = .{ .bucket_num = -1 },
        .columns = &columns,
        .rows = 5,
    }).encode(&encoder, default_protocol_version);
    try std.testing.expectEqualSlices(u8, golden, encoder.bytes());

    var decoder = Decoder.init(golden);
    var block = try DecodedBlock.decode(&decoder, std.testing.allocator, default_protocol_version);
    defer block.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 5), block.rows);
    try std.testing.expectEqual(@as(i32, -1), block.info.bucket_num);
    try std.testing.expectEqual(@as(usize, 2), block.columns.len);
    try std.testing.expect(decoder.eof());
}

test "client data packet roundtrip" {
    const values = [_][]const u8{ "alpha", "beta" };
    const columns = [_]Column{
        .{ .string = .{ .name = "name", .values = &values } },
    };

    var encoder = Encoder.init(std.testing.allocator);
    defer encoder.deinit();

    const packet = DataPacket{
        .temp_table = "_data",
        .block = .{
            .info = .{ .bucket_num = -1 },
            .columns = &columns,
            .rows = 2,
        },
    };
    try packet.encodePacket(&encoder, default_protocol_version);

    var decoder = Decoder.init(encoder.bytes());
    var decoded = try DecodedDataPacket.decodeClientPacket(&decoder, std.testing.allocator, default_protocol_version);
    defer decoded.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("_data", decoded.temp_table);
    try std.testing.expectEqual(@as(usize, 2), decoded.block.rows);
    try std.testing.expect(decoder.eof());
}

test "generic column fixtures roundtrip" {
    try expectColumnFixtureRoundtrip("Bool", 50, "proto/_golden/col_bool.raw", "");
    try expectColumnFixtureRoundtrip("Enum8('v' = 1)", 50, "proto/_golden/col_enum8.raw", "");
    try expectColumnFixtureRoundtrip("DateTime('UTC')", 50, "proto/_golden/col_datetime.raw", "");
    try expectColumnFixtureRoundtrip("DateTime64(3, 'UTC')", 50, "proto/_golden/col_datetime64.raw", "");
    try expectColumnFixtureRoundtrip("Decimal(10, 2)", 50, "proto/_golden/col_decimal64.raw", "");
    try expectColumnFixtureRoundtrip("FixedString(32)", 6, "proto/_golden/col_fixed_str.raw", "");
    try expectColumnFixtureRoundtrip("UUID", 50, "proto/_golden/col_uuid.raw", "");
    try expectColumnFixtureRoundtrip("Nullable(Nothing)", 50, "proto/_golden/col_nothing_nullable.raw", "");
    try expectColumnFixtureRoundtrip("Nullable(String)", 4, "proto/_golden/col_nullable_of_str.raw", "");
    try expectColumnFixtureRoundtrip("Array(String)", 2, "proto/_golden/col_arr_of_str.raw", "");
    try expectColumnFixtureRoundtrip("Tuple(String, Int64)", 50, "proto/_golden/col_tuple_str_int64.raw", "");
    try expectColumnFixtureRoundtrip("Tuple(strings String, ints Int64)", 50, "proto/_golden/col_tuple_named_str_int64.raw", "");
    try expectColumnFixtureRoundtrip("Map(String, String)", 2, "proto/_golden/col_map_of_str_str.raw", "");
}

test "stateful generic column fixtures roundtrip" {
    const low_cardinality_state = try lowCardinalityStatePrefix(std.testing.allocator, 1);
    defer std.testing.allocator.free(low_cardinality_state);

    const double_low_cardinality_state = try lowCardinalityStatePrefix(std.testing.allocator, 2);
    defer std.testing.allocator.free(double_low_cardinality_state);

    try expectColumnFixtureRoundtrip("LowCardinality(String)", 5, "proto/_golden/col_low_cardinality_of_str.raw", low_cardinality_state);
    try expectColumnFixtureRoundtrip("Array(LowCardinality(String))", 5, "proto/_golden/col_arr_low_cardinality_u8_str.raw", low_cardinality_state);
    try expectColumnFixtureRoundtrip("Map(LowCardinality(String), LowCardinality(String))", 2, "proto/_golden/col_map_of_low_cardinality_str_str.raw", double_low_cardinality_state);
}

test "fixed column helpers expose typed values" {
    const numbers = [_]u32{ 7, 11, 42 };
    var numeric = try initOwnedFixedColumn(std.testing.allocator, "n", "UInt32", numbers[0..]);
    defer numeric.deinit(std.testing.allocator);

    const fixed_numeric = try numeric.asFixed();
    const numeric_slice = try fixed_numeric.slice(u32);
    try std.testing.expectEqual(@as(usize, numbers.len), numeric_slice.len);
    for (numbers, 0..) |expected, idx| {
        try std.testing.expectEqual(expected, numeric_slice[idx]);
    }

    const bool_bytes = [_]u8{ 1, 0, 1, 1 };
    var boolean = Column{ .fixed_bytes = .{
        .name = "flag",
        .type_name = "Bool",
        .width = 1,
        .data = &bool_bytes,
        .rows = bool_bytes.len,
    } };
    const fixed_bool = try boolean.asFixed();
    const decoded = try fixed_bool.bools(std.testing.allocator);
    defer std.testing.allocator.free(decoded);
    try std.testing.expectEqualSlices(bool, &.{ true, false, true, true }, decoded);
}

test "nullable view and builder roundtrip" {
    var column = try decodeFixtureColumn(std.testing.allocator, "Nullable(String)", 4, "proto/_golden/col_nullable_of_str.raw", "");
    defer column.deinit(std.testing.allocator);

    var view = try column.asNullable(std.testing.allocator);
    defer view.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 4), view.rows());
    try std.testing.expect(!view.isNull(0));
    try std.testing.expect(view.isNull(1));

    switch (view.values) {
        .string => |values| {
            try std.testing.expectEqualStrings("foo", values.values[0]);
            try std.testing.expectEqualStrings("", values.values[1]);
            try std.testing.expectEqualStrings("bar", values.values[2]);
            try std.testing.expectEqualStrings("baz", values.values[3]);
        },
        else => return error.TestUnexpectedColumnType,
    }

    const null_map = [_]bool{
        view.isNull(0),
        view.isNull(1),
        view.isNull(2),
        view.isNull(3),
    };
    var rebuilt = try initNullableColumn(std.testing.allocator, "v", "Nullable(String)", &null_map, view.values);
    defer rebuilt.deinit(std.testing.allocator);
    const encoded = try encodeColumnStateAndValues(std.testing.allocator, rebuilt);
    defer std.testing.allocator.free(encoded);

    const golden = try readFixture(std.testing.allocator, "proto/_golden/col_nullable_of_str.raw");
    defer std.testing.allocator.free(golden);
    try std.testing.expectEqualSlices(u8, golden, encoded);
}

test "array map and tuple views rebuild fixture bytes" {
    {
        var column = try decodeFixtureColumn(std.testing.allocator, "Array(String)", 2, "proto/_golden/col_arr_of_str.raw", "");
        defer column.deinit(std.testing.allocator);

        var view = try column.asArray(std.testing.allocator);
        defer view.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(usize, 2), view.rows());
        const second = view.rowRange(1);
        try std.testing.expect(second.end >= second.start);

        var rebuilt = try initArrayColumn(std.testing.allocator, "v", "Array(String)", view.offsets, view.values);
        defer rebuilt.deinit(std.testing.allocator);
        const encoded = try encodeColumnStateAndValues(std.testing.allocator, rebuilt);
        defer std.testing.allocator.free(encoded);

        const golden = try readFixture(std.testing.allocator, "proto/_golden/col_arr_of_str.raw");
        defer std.testing.allocator.free(golden);
        try std.testing.expectEqualSlices(u8, golden, encoded);
    }

    {
        var column = try decodeFixtureColumn(std.testing.allocator, "Map(String, String)", 2, "proto/_golden/col_map_of_str_str.raw", "");
        defer column.deinit(std.testing.allocator);

        var view = try column.asMap(std.testing.allocator);
        defer view.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(usize, 2), view.rows());

        var rebuilt = try initMapColumn(std.testing.allocator, "v", "Map(String, String)", view.offsets, view.keys, view.values);
        defer rebuilt.deinit(std.testing.allocator);
        const encoded = try encodeColumnStateAndValues(std.testing.allocator, rebuilt);
        defer std.testing.allocator.free(encoded);

        const golden = try readFixture(std.testing.allocator, "proto/_golden/col_map_of_str_str.raw");
        defer std.testing.allocator.free(golden);
        try std.testing.expectEqualSlices(u8, golden, encoded);
    }

    {
        var column = try decodeFixtureColumn(std.testing.allocator, "Tuple(strings String, ints Int64)", 50, "proto/_golden/col_tuple_named_str_int64.raw", "");
        defer column.deinit(std.testing.allocator);

        var view = try column.asTuple(std.testing.allocator);
        defer view.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(usize, 50), view.rows);
        try std.testing.expectEqual(@as(usize, 2), view.fields.len);
        try std.testing.expectEqualStrings("strings", view.fields[0].name);
        try std.testing.expectEqualStrings("ints", view.fields[1].name);

        var rebuilt = try initTupleColumn(std.testing.allocator, "v", "Tuple(strings String, ints Int64)", view.fields);
        defer rebuilt.deinit(std.testing.allocator);
        const encoded = try encodeColumnStateAndValues(std.testing.allocator, rebuilt);
        defer std.testing.allocator.free(encoded);

        const golden = try readFixture(std.testing.allocator, "proto/_golden/col_tuple_named_str_int64.raw");
        defer std.testing.allocator.free(golden);
        try std.testing.expectEqualSlices(u8, golden, encoded);
    }
}

test "low cardinality view and builder roundtrip" {
    const state_prefix = try lowCardinalityStatePrefix(std.testing.allocator, 1);
    defer std.testing.allocator.free(state_prefix);

    var column = try decodeFixtureColumn(std.testing.allocator, "LowCardinality(String)", 5, "proto/_golden/col_low_cardinality_of_str.raw", state_prefix);
    defer column.deinit(std.testing.allocator);

    var view = try column.asLowCardinality(std.testing.allocator);
    defer view.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(i64, 1), view.serialization_version);
    try std.testing.expectEqual(@as(usize, 5), view.rows());

    switch (view.dictionary) {
        .string => |dictionary| {
            try std.testing.expect(dictionary.values.len > 0);
        },
        else => return error.TestUnexpectedColumnType,
    }

    var rebuilt = try initLowCardinalityColumn(std.testing.allocator, "v", "LowCardinality(String)", view.dictionary, view.keys);
    defer rebuilt.deinit(std.testing.allocator);
    const encoded = try encodeColumnStateAndValues(std.testing.allocator, rebuilt);
    defer std.testing.allocator.free(encoded);

    const payload = try readFixture(std.testing.allocator, "proto/_golden/col_low_cardinality_of_str.raw");
    defer std.testing.allocator.free(payload);
    const golden = try concatBytes(std.testing.allocator, &.{ state_prefix, payload });
    defer std.testing.allocator.free(golden);
    try std.testing.expectEqualSlices(u8, golden, encoded);
}

test "effective query settings inject compression method" {
    inline for ([_]struct {
        compression: BlockCompression,
        expected: ?[]const u8,
    }{
        .{ .compression = .disabled, .expected = null },
        .{ .compression = .lz4, .expected = "LZ4" },
        .{ .compression = .zstd, .expected = "ZSTD" },
        .{ .compression = .none, .expected = "NONE" },
        .{ .compression = .lz4hc, .expected = "LZ4HC" },
    }) |case| {
        var client = makeClientForSettings(std.testing.allocator, case.compression);
        defer client.server_storage.deinit();

        const query = Query{
            .body = "SELECT 1",
            .compression = if (case.compression == .disabled) .disabled else .enabled,
            .info = makeQueryInfo(),
        };
        const settings = try client.effectiveQuerySettings(query);
        defer if (settings) |owned| std.testing.allocator.free(owned);

        if (case.expected) |expected| {
            const owned = settings orelse return error.TestMissingCompressionSetting;
            try std.testing.expectEqual(@as(usize, 1), owned.len);
            try std.testing.expectEqualStrings("network_compression_method", owned[0].key);
            try std.testing.expectEqualStrings(expected, owned[0].value);
            try std.testing.expect(owned[0].important);
        } else {
            try std.testing.expect(settings == null);
        }
    }
}

test "effective query settings keep explicit compression method" {
    var client = makeClientForSettings(std.testing.allocator, .zstd);
    defer client.server_storage.deinit();

    var explicit_settings = [_]Setting{
        .{
            .key = "network_compression_method",
            .value = "LZ4",
            .important = true,
        },
    };
    const query = Query{
        .body = "SELECT 1",
        .compression = .enabled,
        .info = makeQueryInfo(),
        .settings = explicit_settings[0..],
    };

    const settings = try client.effectiveQuerySettings(query);
    try std.testing.expect(settings == null);
}

test "zero-row stateful block keeps headers only" {
    var golden = Encoder.init(std.testing.allocator);
    defer golden.deinit();

    try golden.putVarUInt(1);
    try golden.putVarUInt(0);
    try golden.putString("v");
    try golden.putString("LowCardinality(String)");
    if (Feature.custom_serialization.enabled(default_protocol_version)) {
        try golden.putBool(false);
    }

    var decoder = Decoder.init(golden.bytes());
    var block = try DecodedBlock.decodeRaw(&decoder, std.testing.allocator, default_protocol_version);
    defer block.deinit(std.testing.allocator);

    try std.testing.expect(decoder.eof());
    try std.testing.expectEqual(@as(usize, 0), block.rows);
    try std.testing.expectEqual(@as(usize, 1), block.columns.len);
    try std.testing.expectEqual(@as(usize, 0), block.columns[0].rowCount());
    try std.testing.expectEqualStrings("LowCardinality(String)", block.columns[0].typeName());

    var reencoded = Encoder.init(std.testing.allocator);
    defer reencoded.deinit();
    try (DataBlock{
        .columns = block.columns,
        .rows = block.rows,
    }).encodeRaw(&reencoded, default_protocol_version);
    try std.testing.expectEqualSlices(u8, golden.bytes(), reencoded.bytes());
}

test "tcp client handles remaining native protocol packets" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = SurfaceMockServerState{};
    const thread = try std.Thread.spawn(.{}, runSurfaceMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
        .protocol_version = 54467,
    });
    defer client.deinit();

    var status_packet = try client.requestTablesStatus(.{
        .tables = &.{
            .{ .database = "db1", .table = "t1" },
            .{ .database = "db2", .table = "t2" },
        },
    });
    defer status_packet.deinit();
    switch (status_packet.value) {
        .tables_status => |response| {
            try std.testing.expectEqual(@as(usize, 2), response.entries.len);
            try std.testing.expectEqualStrings("db1", response.entries[0].table.database);
            try std.testing.expect(response.entries[0].status.is_replicated);
            try std.testing.expect(response.entries[0].status.is_readonly);
        },
        else => return error.TestUnexpectedPacket,
    }

    try client.sendSshChallengeRequest();
    var challenge_packet = try client.readServerPacket();
    defer challenge_packet.deinit();
    switch (challenge_packet.value) {
        .ssh_challenge => |challenge| {
            try std.testing.expectEqualStrings("nonce-42", challenge.challenge);
        },
        else => return error.TestUnexpectedPacket,
    }
    try client.sendSshChallengeResponse("ZmFrZS1zaWc=");

    var uuids_packet = try client.readServerPacket();
    defer uuids_packet.deinit();
    switch (uuids_packet.value) {
        .part_uuids => |packet| {
            try std.testing.expectEqual(@as(usize, 2), packet.uuids.len);
            try std.testing.expectEqual(@as(u8, 0), packet.uuids[0][0]);
            try std.testing.expectEqual(@as(u8, 15), packet.uuids[0][15]);
        },
        else => return error.TestUnexpectedPacket,
    }

    var read_task_packet = try client.readServerPacket();
    defer read_task_packet.deinit();
    switch (read_task_packet.value) {
        .read_task_request => {},
        else => return error.TestUnexpectedPacket,
    }

    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_tables_status_request);
    try std.testing.expect(state.saw_ssh_challenge_request);
    try std.testing.expect(state.saw_ssh_challenge_response);
}

test "tcp client performs ssh auth during handshake" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = SshHandshakeMockServerState{};
    const thread = try std.Thread.spawn(.{}, runSshHandshakeMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
        .ssh_signer = mockSshSigner,
    });
    defer client.deinit();

    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_ssh_challenge_request);
    try std.testing.expect(state.saw_ssh_challenge_response);
    try std.testing.expect(state.saw_quota_key);
}

test "compressed frame fixtures decode" {
    const raw = try readFixture(std.testing.allocator, "compress/_golden/data_raw.raw");
    defer std.testing.allocator.free(raw);

    try expectCompressedFixture(.none, "compress/_golden/data_compressed_none.raw", raw);
    try expectCompressedFixture(.lz4, "compress/_golden/data_compressed_lz4.raw", raw);
    try expectCompressedFixture(.lz4, "compress/_golden/data_compressed_lz4hc.raw", raw);
    try expectCompressedFixture(.zstd, "compress/_golden/data_compressed_zstd.raw", raw);
}

test "compressed frame roundtrip" {
    const raw = try readFixture(std.testing.allocator, "compress/_golden/data_raw.raw");
    defer std.testing.allocator.free(raw);

    inline for ([_]ch_compress.BlockCompression{ .none, .lz4, .lz4hc, .zstd }) |method| {
        const frame = try ch_compress.compressFrame(std.testing.allocator, raw, method, 0);
        defer std.testing.allocator.free(frame);

        const decoded = try ch_compress.decompressFrame(std.testing.allocator, frame);
        defer std.testing.allocator.free(decoded);
        try std.testing.expectEqualSlices(u8, raw, decoded);
    }
}

test "tcp client reads compressed block" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = CompressedSelectMockServerState{
        .method = .lz4,
    };
    const thread = try std.Thread.spawn(.{}, runCompressedSelectMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
        .compression = .lz4,
    });
    defer client.deinit();

    try client.sendQuery(client.newQuery("SELECT name, count FROM t"));

    var packet = try client.readServerPacket();
    defer packet.deinit();
    switch (packet.value) {
        .data => |data| {
            try std.testing.expectEqual(@as(usize, 2), data.block.rows);
            try std.testing.expectEqual(@as(usize, 2), data.block.columns.len);
            switch (data.block.columns[0]) {
                .string => |column| {
                    try std.testing.expectEqualStrings("alpha", column.values[0]);
                    try std.testing.expectEqualStrings("beta", column.values[1]);
                },
                else => return error.TestUnexpectedColumnType,
            }
            switch (data.block.columns[1]) {
                .uint64 => |column| {
                    try std.testing.expectEqual(@as(u64, 11), column.values[0]);
                    try std.testing.expectEqual(@as(u64, 42), column.values[1]);
                },
                else => return error.TestUnexpectedColumnType,
            }
        },
        else => return error.TestUnexpectedPacket,
    }

    var end_packet = try client.readServerPacket();
    defer end_packet.deinit();
    switch (end_packet.value) {
        .end_of_stream => {},
        else => return error.TestUnexpectedPacket,
    }

    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_query);
}

test "tcp client reads stateful composite block" {
    const state_prefix = try lowCardinalityStatePrefix(std.testing.allocator, 1);
    defer std.testing.allocator.free(state_prefix);

    const payload = try readFixture(std.testing.allocator, "proto/_golden/col_arr_low_cardinality_u8_str.raw");
    defer std.testing.allocator.free(payload);

    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = CompositeMockServerState{
        .state = state_prefix,
        .payload = payload,
    };
    const thread = try std.Thread.spawn(.{}, runCompositeMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
    });
    defer client.deinit();

    try client.sendQuery(client.newQuery("SELECT arrayJoin([1])"));

    var packet = try client.readServerPacket();
    defer packet.deinit();
    switch (packet.value) {
        .data => |data| {
            try std.testing.expectEqual(@as(usize, 5), data.block.rows);
            try std.testing.expectEqual(@as(usize, 1), data.block.columns.len);
            switch (data.block.columns[0]) {
                .encoded => |column| {
                    try std.testing.expectEqualStrings("Array(LowCardinality(String))", column.type_name);
                    try std.testing.expectEqualSlices(u8, state_prefix, column.state);
                    try std.testing.expectEqualSlices(u8, payload, column.payload);
                },
                else => return error.TestUnexpectedColumnType,
            }
        },
        else => return error.TestUnexpectedPacket,
    }

    var end_packet = try client.readServerPacket();
    defer end_packet.deinit();
    switch (end_packet.value) {
        .end_of_stream => {},
        else => return error.TestUnexpectedPacket,
    }

    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_query);
}

test "tcp client writes compressed blocks" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = CompressedInsertMockServerState{};
    const thread = try std.Thread.spawn(.{}, runCompressedInsertMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
        .compression = .zstd,
    });
    defer client.deinit();

    try client.sendQuery(client.newQuery("INSERT INTO t VALUES"));

    const values = [_][]const u8{ "left", "right" };
    const columns = [_]Column{
        .{ .string = .{ .name = "v", .values = &values } },
    };
    try client.sendDataPacket(.{
        .temp_table = "",
        .block = .{
            .info = .{ .bucket_num = -1 },
            .columns = &columns,
            .rows = 2,
        },
    });
    try client.sendEndOfData();

    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_query);
    try std.testing.expect(state.saw_data);
    try std.testing.expect(state.saw_end_of_data);
}

test "client Do routes results totals logs profile and profile events" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = DoSelectMockServerState{};
    const thread = try std.Thread.spawn(.{}, runDoSelectMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
    });
    defer client.deinit();

    var callbacks = DoResultState{};
    var result_buffer = BlockBuffer.init(std.testing.allocator);
    defer result_buffer.deinit();
    var totals_buffer = BlockBuffer.init(std.testing.allocator);
    defer totals_buffer.deinit();
    var extremes_buffer = BlockBuffer.init(std.testing.allocator);
    defer extremes_buffer.deinit();

    var query = client.newQuery("SELECT name, count FROM t");
    query.result = &result_buffer;
    query.totals = &totals_buffer;
    query.extremes = &extremes_buffer;
    query.on_result = onDoResult;
    query.on_totals = onDoTotals;
    query.on_extremes = onDoExtremes;
    query.on_progress = onDoProgress;
    query.on_profile = onDoProfile;
    query.on_logs_batch = onDoLogsBatch;
    query.on_log = onDoLog;
    query.on_profile_events_batch = onDoProfileEventsBatch;
    query.on_profile_event = onDoProfileEvent;

    try client.Do(.{ .user_data = &callbacks }, &query);
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_query);
    try std.testing.expectEqual(@as(usize, 1), callbacks.result_calls);
    try std.testing.expectEqual(@as(usize, 1), callbacks.totals_calls);
    try std.testing.expectEqual(@as(usize, 1), callbacks.extremes_calls);
    try std.testing.expectEqual(@as(usize, 1), callbacks.progress_calls);
    try std.testing.expectEqual(@as(usize, 1), callbacks.profile_calls);
    try std.testing.expectEqual(@as(usize, 1), callbacks.log_batch_calls);
    try std.testing.expectEqual(@as(usize, 1), callbacks.log_calls);
    try std.testing.expectEqual(@as(usize, 1), callbacks.profile_events_batch_calls);
    try std.testing.expectEqual(@as(usize, 1), callbacks.profile_event_calls);
    try std.testing.expectEqual(@as(usize, 1), result_buffer.blocks.items.len);
    try std.testing.expectEqual(@as(usize, 1), totals_buffer.blocks.items.len);
    try std.testing.expectEqual(@as(usize, 1), extremes_buffer.blocks.items.len);

    const block = result_buffer.blocks.items[0];
    try std.testing.expectEqual(@as(usize, 2), block.rows);
    switch (block.columns[0]) {
        .string => |column| try std.testing.expectEqualStrings("alpha", column.values[0]),
        else => return error.TestUnexpectedColumnType,
    }
    switch (totals_buffer.blocks.items[0].columns[0]) {
        .uint64 => |column| try std.testing.expectEqual(@as(u64, 53), column.values[0]),
        else => return error.TestUnexpectedColumnType,
    }
}

test "client Do routes compressed results logs and profile events" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = DoSelectMockServerState{
        .compression = .lz4,
    };
    const thread = try std.Thread.spawn(.{}, runDoSelectMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
        .compression = .lz4,
    });
    defer client.deinit();

    var callbacks = DoResultState{};
    var query = client.newQuery("SELECT name, count FROM t");
    query.on_logs_batch = onDoLogsBatch;
    query.on_log = onDoLog;
    query.on_profile_events_batch = onDoProfileEventsBatch;
    query.on_profile_event = onDoProfileEvent;

    try client.Do(.{ .user_data = &callbacks }, &query);
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_query);
    try std.testing.expectEqual(@as(usize, 1), callbacks.log_batch_calls);
    try std.testing.expectEqual(@as(usize, 1), callbacks.log_calls);
    try std.testing.expectEqual(@as(usize, 1), callbacks.profile_events_batch_calls);
    try std.testing.expectEqual(@as(usize, 1), callbacks.profile_event_calls);
}

test "client Do handles none compression when server sends plain data packets" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = DoSelectMockServerState{
        .compression = .none,
        .server_compression = .disabled,
    };
    const thread = try std.Thread.spawn(.{}, runDoSelectMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
        .compression = .none,
    });
    defer client.deinit();

    var result_buffer = BlockBuffer.init(std.testing.allocator);
    defer result_buffer.deinit();
    var query = client.newQuery("SELECT name, count FROM t");
    query.result = &result_buffer;

    try client.Do(.{}, &query);
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_query);
    try std.testing.expectEqual(@as(usize, 1), result_buffer.blocks.items.len);
    try std.testing.expectEqual(@as(usize, 2), result_buffer.blocks.items[0].rows);
}

test "client Do streams input via OnInput" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = DoInsertMockServerState{};
    const thread = try std.Thread.spawn(.{}, runDoInsertMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
    });
    defer client.deinit();

    const values = [_][]const u8{ "left", "right" };
    var input_state = StreamingInputState{
        .columns = .{
            .{ .string = .{ .name = "v", .values = &values } },
        },
    };

    var query = client.newQuery("INSERT INTO t VALUES");
    query.input = &.{};
    query.on_input = onStreamingInput;

    try client.Do(.{ .user_data = &input_state }, &query);
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_query);
    try std.testing.expect(state.saw_first_data);
    try std.testing.expect(state.saw_end_of_data);
    try std.testing.expectEqual(@as(u8, 2), input_state.stage);
}

test "client Do sends cancel and closes client on context cancellation" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = DoCancelMockServerState{};
    const thread = try std.Thread.spawn(.{}, runDoCancelMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
    });
    defer client.deinit();

    var cancel_state = CancelState{};
    var query = client.newQuery("SELECT cancel_me");
    query.on_progress = onCancelProgress;

    try std.testing.expectError(error.Canceled, client.Do(.{
        .user_data = &cancel_state,
        .is_canceled = cancelRequested,
    }, &query));
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_query);
    try std.testing.expect(state.saw_cancel);
    try std.testing.expectEqual(@as(usize, 1), cancel_state.progress_calls);
    try std.testing.expect(client.isClosed());
}

test "client Do closes transport when sender fails locally" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = DoSenderFailureMockServerState{};
    const thread = try std.Thread.spawn(.{}, runDoSenderFailureMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
    });
    defer client.deinit();

    var query = client.newQuery("INSERT INTO t VALUES");
    query.input = &.{};
    query.on_input = onFailingInput;

    try std.testing.expectError(error.TestInputFailure, client.Do(.{}, &query));
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_query);
    try std.testing.expect(state.saw_disconnect);
    try std.testing.expect(client.isClosed());
}

test "client Do fails input inference when server finishes without schema" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = DoInferMissingSchemaMockServerState{};
    const thread = try std.Thread.spawn(.{}, runDoInferMissingSchemaMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
    });
    defer client.deinit();

    const values = [_][]const u8{ "a", "b" };
    const columns = [_]Column{
        .{ .var_bytes = .{
            .name = "",
            .type_name = "",
            .values = &values,
        } },
    };

    var query = client.newQuery("INSERT INTO t VALUES");
    query.input = &columns;

    try std.testing.expectError(error.InputSchemaUnavailable, client.Do(.{}, &query));
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_query);
    try std.testing.expect(client.isClosed());
}

test "client stores last exception after server error" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = DoExceptionMockServerState{};
    const thread = try std.Thread.spawn(.{}, runDoExceptionMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
    });
    defer client.deinit();

    var query = client.newQuery("SELECT boom()");
    try std.testing.expectError(error.ServerException, client.Do(.{}, &query));
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_query);

    const last_exception = client.lastException() orelse return error.TestUnexpectedPacket;
    try std.testing.expectEqual(@as(usize, 1), last_exception.items.len);
    try std.testing.expectEqual(@as(i32, 60), last_exception.items[0].code);
    try std.testing.expectEqualStrings("DB::Exception", last_exception.items[0].name);
    try std.testing.expectEqualStrings("DB::Exception: mock failure", last_exception.items[0].message);
}

test "client Do binds typed result columns and reports metrics" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = DoSelectMockServerState{};
    const thread = try std.Thread.spawn(.{}, runDoSelectMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
    });
    defer client.deinit();

    var names = OwnedByteSlices.init(std.testing.allocator);
    defer names.deinit();
    var counts = std.ArrayList(u64).init(std.testing.allocator);
    defer counts.deinit();
    var total_counts = std.ArrayList(u64).init(std.testing.allocator);
    defer total_counts.deinit();
    var metrics = QueryMetrics{};

    var binding_columns = [_]ResultBindingColumn{
        .{ .name = "name", .sink = .{ .strings = &names } },
        .{ .name = "count", .sink = .{ .uint64s = &counts } },
    };
    var totals_columns = [_]ResultBindingColumn{
        .{ .name = "total_count", .sink = .{ .uint64s = &total_counts } },
    };
    var binding = ResultBinding.init(std.testing.allocator, binding_columns[0..]);
    var totals_binding = ResultBinding.init(std.testing.allocator, totals_columns[0..]);

    var query = client.newQuery("SELECT name, count FROM t");
    query.result_binding = &binding;
    query.totals_binding = &totals_binding;
    query.metrics = &metrics;

    try client.Do(.{}, &query);
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 2), names.items.items.len);
    try std.testing.expectEqualStrings("alpha", names.items.items[0]);
    try std.testing.expectEqualStrings("beta", names.items.items[1]);
    try std.testing.expectEqual(@as(usize, 2), counts.items.len);
    try std.testing.expectEqual(@as(u64, 11), counts.items[0]);
    try std.testing.expectEqual(@as(u64, 42), counts.items[1]);
    try std.testing.expectEqual(@as(usize, 1), total_counts.items.len);
    try std.testing.expectEqual(@as(u64, 53), total_counts.items[0]);
    try std.testing.expectEqual(@as(u64, 1), metrics.blocks_received);
    try std.testing.expectEqual(@as(u64, 2), metrics.rows_received);
    try std.testing.expectEqual(@as(u64, 2), metrics.columns_received);
}

test "client Do binds composite result values recursively" {
    const state_prefix = try lowCardinalityStatePrefix(std.testing.allocator, 1);
    defer std.testing.allocator.free(state_prefix);

    const payload = try readFixture(std.testing.allocator, "proto/_golden/col_arr_low_cardinality_u8_str.raw");
    defer std.testing.allocator.free(payload);

    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = CompositeMockServerState{
        .state = state_prefix,
        .payload = payload,
    };
    const thread = try std.Thread.spawn(.{}, runCompositeMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
    });
    defer client.deinit();

    var values = OwnedValues.init(std.testing.allocator);
    defer values.deinit();
    var binding_columns = [_]ResultBindingColumn{
        .{ .name = "v", .sink = .{ .values = &values } },
    };
    var binding = ResultBinding.init(std.testing.allocator, binding_columns[0..]);

    var query = client.newQuery("SELECT arrayJoin([1])");
    query.result_binding = &binding;
    try client.Do(.{}, &query);
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 5), values.items.items.len);
    switch (values.items.items[0]) {
        .array => |items| {
            try std.testing.expect(items.len > 0);
            switch (items[0]) {
                .string => |value| try std.testing.expect(value.len > 0),
                else => return error.TestUnexpectedColumnType,
            }
        },
        else => return error.TestUnexpectedColumnType,
    }
}

test "client Do infers composite input schema before sending blocks" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = DoInferInsertMockServerState{};
    const thread = try std.Thread.spawn(.{}, runDoInferInsertMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
    });
    defer client.deinit();

    const values = [_][]const u8{ "a", "b", "c" };
    var strings = try initOwnedStringColumn(std.testing.allocator, "", values[0..]);
    defer strings.deinit(std.testing.allocator);

    const offsets = [_]u64{ 2, 3 };
    var input = [_]Column{try initArrayColumn(std.testing.allocator, "", "", offsets[0..], strings)};
    defer input[0].deinit(std.testing.allocator);
    var query = client.newQuery("INSERT INTO t VALUES");
    query.input = input[0..];
    try client.Do(.{}, &query);
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_query);
    try std.testing.expect(state.saw_schema);
    try std.testing.expect(state.saw_data);
    try std.testing.expect(state.saw_end_of_data);
    try std.testing.expectEqualStrings("tags", input[0].name());
    try std.testing.expectEqualStrings("Array(String)", input[0].typeName());
}

test "fixed-width input inference fills missing width and type" {
    const raw = [_]u8{ 1, 0, 0, 0, 2, 0, 0, 0 };
    var input = Column{ .fixed_bytes = .{
        .name = "",
        .type_name = "",
        .width = 0,
        .data = raw[0..],
        .rows = 2,
    } };
    defer input.deinit(std.testing.allocator);

    const schema = Column{ .fixed_bytes = .{
        .name = "n",
        .type_name = "UInt32",
        .width = 4,
        .data = "",
        .rows = 0,
    } };

    try inferInputColumn(std.testing.allocator, &input, schema);
    try std.testing.expectEqualStrings("n", input.name());
    try std.testing.expectEqualStrings("UInt32", input.typeName());
    switch (input) {
        .fixed_bytes => |value| try std.testing.expectEqual(@as(usize, 4), value.width),
        else => return error.TestUnexpectedColumnType,
    }
}

test "observer receives connect and query lifecycle events" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = DoSelectMockServerState{};
    const thread = try std.Thread.spawn(.{}, runDoSelectMockServer, .{ &server, &state });

    var observer_state = ObserverState{};
    const observer = Observer{
        .user_data = &observer_state,
        .on_connect = observerStateConnect,
        .on_query = observerStateQuery,
    };

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
        .observer = observer,
    });
    defer client.deinit();

    var query = client.newQuery("SELECT name, count FROM t");
    try client.Do(.{}, &query);
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), observer_state.connect_start_calls);
    try std.testing.expectEqual(@as(usize, 1), observer_state.connect_finish_calls);
    try std.testing.expectEqual(@as(usize, 1), observer_state.query_start_calls);
    try std.testing.expectEqual(@as(usize, 1), observer_state.query_finish_calls);
    try std.testing.expectEqual(@as(usize, 1), observer_state.progress_calls);
    try std.testing.expectEqual(@as(usize, 1), observer_state.profile_calls);
    try std.testing.expectEqual(@as(usize, 0), observer_state.exception_calls);
    try std.testing.expectEqual(@as(u64, 1), observer_state.last_finish_metrics.blocks_received);
    try std.testing.expectEqual(@as(u64, 2), observer_state.last_finish_metrics.rows_received);
}

test "connectTcp uses custom dialer" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = DoSelectMockServerState{};
    const thread = try std.Thread.spawn(.{}, runDoSelectMockServer, .{ &server, &state });

    test_dialer_called = false;

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
        .dialer = testDialer,
    });
    defer client.deinit();

    var query = client.newQuery("SELECT name, count FROM t");
    try client.Do(.{}, &query);
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(test_dialer_called);
}

test "pool reuses a single client connection" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = PoolMockServerState{};
    const thread = try std.Thread.spawn(.{}, runPoolMockServer, .{ &server, &state });

    var pool = try Pool.init(std.testing.allocator, .{
        .host = "127.0.0.1",
        .port = server.listen_address.getPort(),
        .client_options = .{ .client_name = "zig-test" },
        .max_conns = 1,
        .min_conns = 1,
    });
    defer pool.deinit();

    var query = Query{
        .body = "SELECT pooled",
        .compression = .disabled,
        .info = makeQueryInfo(),
    };
    try pool.Do(.{}, &query);
    try pool.Do(.{}, &query);

    const stats = pool.stat();
    try std.testing.expectEqual(@as(usize, 1), stats.total_conns);
    try std.testing.expectEqual(@as(usize, 1), stats.idle_conns);
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), state.hello_count);
    try std.testing.expectEqual(@as(usize, 2), state.query_count);
}

test "pool acquire waits for released connection" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = PoolLifecycleMockServerState{
        .expected_hello_count = 1,
    };
    const thread = try std.Thread.spawn(.{}, runPoolLifecycleMockServer, .{ &server, &state });

    var pool = try Pool.init(std.testing.allocator, .{
        .host = "127.0.0.1",
        .port = server.listen_address.getPort(),
        .client_options = .{ .client_name = "zig-test" },
        .max_conns = 1,
        .min_conns = 1,
        .health_check_period_ms = 0,
    });

    var conn = try pool.acquire(.{});
    var waiter_state = PoolAcquireWaiterState{ .pool = &pool };
    const waiter = try std.Thread.spawn(.{}, runPoolAcquireWaiter, .{&waiter_state});

    std.time.sleep(20 * std.time.ns_per_ms);
    try std.testing.expect(!waiter_state.acquired.load(.acquire));

    conn.release();
    waiter.join();

    try std.testing.expect(waiter_state.err == null);
    try std.testing.expect(waiter_state.acquired.load(.acquire));
    const stats = pool.stat();
    try std.testing.expectEqual(@as(usize, 1), stats.total_conns);
    try std.testing.expectEqual(@as(usize, 1), stats.idle_conns);

    pool.deinit();
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 1), state.hello_count);
}

test "pool replenishes min connections after closed client is released" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = PoolLifecycleMockServerState{
        .expected_hello_count = 2,
    };
    const thread = try std.Thread.spawn(.{}, runPoolLifecycleMockServer, .{ &server, &state });

    var pool = try Pool.init(std.testing.allocator, .{
        .host = "127.0.0.1",
        .port = server.listen_address.getPort(),
        .client_options = .{ .client_name = "zig-test" },
        .max_conns = 1,
        .min_conns = 1,
        .health_check_period_ms = 0,
    });

    var conn = try pool.acquire(.{});
    conn.client().closeStream();
    conn.release();

    const stats = pool.stat();
    try std.testing.expectEqual(@as(usize, 1), stats.total_conns);
    try std.testing.expectEqual(@as(usize, 1), stats.idle_conns);

    pool.deinit();
    thread.join();

    if (state.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 2), state.hello_count);
}

test "tcp client handshake ping and query" {
    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try address.listen(.{ .reuse_address = true });
    defer server.deinit();

    var state = MockServerState{};
    const thread = try std.Thread.spawn(.{}, runMockServer, .{ &server, &state });

    var client = try Client.connectTcp(std.testing.allocator, "127.0.0.1", server.listen_address.getPort(), .{
        .client_name = "zig-test",
    });
    defer client.deinit();

    try client.ping();
    try client.sendQuery(client.newQuery("SELECT 1"));

    var packet = try client.readServerPacket();
    defer packet.deinit();
    switch (packet.value) {
        .end_of_stream => {},
        else => return error.TestUnexpectedPacket,
    }

    thread.join();

    if (state.err) |err| return err;
    try std.testing.expect(state.saw_hello);
    try std.testing.expect(state.saw_ping);
    try std.testing.expect(state.saw_query);
}
