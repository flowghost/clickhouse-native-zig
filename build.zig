const std = @import("std");

const lz4_sources = &.{
    "vendor/lz4/lz4.c",
    "vendor/lz4/lz4hc.c",
};

const zstd_sources = &.{
    "vendor/zstd/common/debug.c",
    "vendor/zstd/common/entropy_common.c",
    "vendor/zstd/common/error_private.c",
    "vendor/zstd/common/fse_decompress.c",
    "vendor/zstd/common/pool.c",
    "vendor/zstd/common/threading.c",
    "vendor/zstd/common/xxhash.c",
    "vendor/zstd/common/zstd_common.c",
    "vendor/zstd/compress/fse_compress.c",
    "vendor/zstd/compress/hist.c",
    "vendor/zstd/compress/huf_compress.c",
    "vendor/zstd/compress/zstd_compress.c",
    "vendor/zstd/compress/zstd_compress_literals.c",
    "vendor/zstd/compress/zstd_compress_sequences.c",
    "vendor/zstd/compress/zstd_compress_superblock.c",
    "vendor/zstd/compress/zstd_double_fast.c",
    "vendor/zstd/compress/zstd_fast.c",
    "vendor/zstd/compress/zstd_lazy.c",
    "vendor/zstd/compress/zstd_ldm.c",
    "vendor/zstd/compress/zstd_opt.c",
    "vendor/zstd/compress/zstd_preSplit.c",
    "vendor/zstd/decompress/huf_decompress.c",
    "vendor/zstd/decompress/zstd_ddict.c",
    "vendor/zstd/decompress/zstd_decompress.c",
    "vendor/zstd/decompress/zstd_decompress_block.c",
};

const zstd_c_flags = &.{
    "-DDEBUGLEVEL=0",
    "-DXXH_NAMESPACE=ZSTD_",
    "-DZSTD_LEGACY_SUPPORT=0",
    "-DZSTD_DISABLE_ASM",
};

const Example = struct {
    name: []const u8,
    source: []const u8,
};

const examples = [_]Example{
    .{ .name = "protocol-smoke", .source = "examples/smoke.zig" },
    .{ .name = "high-level-select", .source = "examples/high_level_select.zig" },
    .{ .name = "result-binding", .source = "examples/result_binding.zig" },
    .{ .name = "high-level-insert-stream", .source = "examples/high_level_insert_stream.zig" },
    .{ .name = "high-level-handlers", .source = "examples/high_level_handlers.zig" },
    .{ .name = "observer", .source = "examples/observer.zig" },
    .{ .name = "cancel-query", .source = "examples/cancel_query.zig" },
    .{ .name = "columns", .source = "examples/columns.zig" },
    .{ .name = "low-level-packets", .source = "examples/low_level_packets.zig" },
    .{ .name = "tables-status", .source = "examples/tables_status.zig" },
    .{ .name = "pool", .source = "examples/pool.zig" },
    .{ .name = "ssh-auth", .source = "examples/ssh_auth.zig" },
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const module = b.addModule("clickhouse_native", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    addCompressionSources(module);

    const tests = b.addTest(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    addCompressionSources(tests.root_module);
    const run_tests = b.addRunArtifact(tests);

    const test_step = b.step("test", "Run package tests");
    test_step.dependOn(&run_tests.step);

    const examples_step = b.step("examples", "Build all Zig examples");
    const example_step = b.step("example", "Alias for `zig build examples`");
    for (examples) |entry| {
        const exe = b.addExecutable(.{
            .name = entry.name,
            .root_source_file = b.path(entry.source),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        exe.root_module.addImport("clickhouse_native", module);
        examples_step.dependOn(&exe.step);
    }

    const live_verify = b.addExecutable(.{
        .name = "live-verify",
        .root_source_file = b.path("examples/live_verify.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    live_verify.root_module.addImport("clickhouse_native", module);
    examples_step.dependOn(&live_verify.step);
    example_step.dependOn(examples_step);

    const run_live_verify = b.addRunArtifact(live_verify);
    if (b.args) |args| {
        run_live_verify.addArgs(args);
    }

    const live_verify_step = b.step("run-live-verify", "Run live ClickHouse integration verification");
    live_verify_step.dependOn(&run_live_verify.step);
}

fn addCompressionSources(module: *std.Build.Module) void {
    const b = module.owner;

    module.addIncludePath(b.path("vendor/lz4"));
    module.addIncludePath(b.path("vendor/zstd"));

    module.addCSourceFiles(.{
        .files = lz4_sources,
    });
    module.addCSourceFiles(.{
        .files = zstd_sources,
        .flags = zstd_c_flags,
    });
}
