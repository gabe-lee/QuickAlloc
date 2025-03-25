const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const math = std.math;
const Allocator = std.mem.Allocator;
const PageAllocator = std.heap.PageAllocator;
const List = std.ArrayList;

//CHECKPOINT switch to per-bucket slabs again
pub const QuickAllocOptions = struct {
    /// The size of the memory region requested from the backing allocator when any bucket does not have
    /// any more blocks left. This size will then be divided into individual blocks and appended to that bucket
    /// free list.
    ///
    /// This value must follow the following rules:
    /// - slab_size >= largest bucket size
    /// - slab_size >= std.heap.page_size_min
    slab_size: AllocSize = ._16_KB,
    /// A list of all allocation-size buckets desired for this allocator. Any requested allocation will be
    /// sent to the smallest bucket that can hold it.
    ///
    /// This list MUST follow the following rules:
    /// - all sizes in order from smallest to largest
    /// - all sizes different
    /// - smallest size is >= @sizeOf(usize)
    /// - largest size is <= slab_size
    bucket_sizes: []const AllocSize = &.{ ._1_KB, ._4_KB, ._16_KB },
    /// If the backing page allocator fails to allocate memory for a new slab, how should this allocator respond
    slab_allocation_fail_behavior: ErrorBehavior = if (builtin.mode == .Debug) .PANIC else .UNREACHABLE,
};

pub const AllocSize = enum(usize) {
    _1_B = 0,
    _2_B = 1,
    _4_B = 2,
    _8_B = 3,
    _16_B = 4,
    _32_B = 5,
    _64_B = 6,
    _128_B = 7,
    _256_B = 8,
    _512_B = 9,
    _1_KB = 10,
    _2_KB = 11,
    _4_KB = 12,
    _8_KB = 13,
    _16_KB = 14,
    _32_KB = 15,
    _64_KB = 16,
    _128_KB = 17,
    _256_KB = 18,
    _512_KB = 19,
    _1_MB = 20,
    _2_MB = 21,
    _4_MB = 22,
    _8_MB = 23,
    _16_MB = 24,
    _32_MB = 25,
    _64_MB = 26,
    _128_MB = 27,
    _256_MB = 28,
    _512_MB = 29,
    _1_GB = 30,
    _2_GB = 31,
    _4_GB = 32,

    pub inline fn bytes_log2(self: AllocSize) usize {
        return @intFromEnum(self);
    }

    pub inline fn bytes(self: AllocSize) usize {
        return @as(usize, 1) << @intFromEnum(self);
    }
};

pub const ErrorBehavior = enum {
    RETURN,
    LOG_AND_RETURN,
    PANIC,
    UNREACHABLE,
};

fn check_buckets(comptime buckets: []AllocSize, comptime slab_size: AllocSize) void {
    var idx: usize = 0;
    var last_size: usize = @intFromEnum(buckets[idx]);
    while (idx < buckets.len) : (idx += 1) {
        if (last_size > @intFromEnum(slab_size)) @panic("Cannot have a block size larger than slab_size");
        if ((1 << last_size) < @sizeOf(usize)) @panic("Cannot have a block size smaller than @sizeOf(usize)");
        if (idx < buckets.len - 1) {
            const next_size = @intFromEnum(buckets[idx + 1]);
            if (last_size >= next_size) @panic("bucket sizes MUST be in sorted order from smallest to largest, and there cannot be 2 buckets of the same size");
            last_size = next_size;
        }
    }
}

pub fn define_allocator(comptime options: QuickAllocOptions) type {
    const bucket_count = options.bucket_sizes.len;
    if (bucket_count == 0) @panic("must provide at least one allocation bucket");
    if ((@as(usize, 1) << @intFromEnum(options.slab_size)) < std.heap.page_size_min) @panic("slab_size must be >= std.heap.page_size_min");
    const buckets = options.bucket_sizes;
    check_buckets(buckets, options.slab_size);
    const block_log2_sizes: [bucket_count]usize = calc: {
        const array: [bucket_count]usize = @splat(0);
        var i: usize = 0;
        while (i < bucket_count) : (i += 1) {
            array[i].* = buckets[i].bytes_log2();
        }
        break :calc array;
    };
    const block_sizes: [bucket_count]usize = calc: {
        const array: [bucket_count]usize = @splat(0);
        var i: usize = 0;
        while (i < bucket_count) : (i += 1) {
            array[i].* = buckets[i].bytes();
        }
        break :calc array;
    };
    const slab_log2_size: usize = options.slab_size.bytes_log2();
    const slab_size: usize = options.slab_size.bytes();
    const blocks_per_slab: [bucket_count]usize = calc: {
        const array: [bucket_count]usize = @splat(0);
        var i: usize = 0;
        while (i < bucket_count) : (i += 1) {
            array[i].* = block_sizes[i] / slab_size;
        }
        break :calc array;
    };
    const largest_block_size = block_log2_sizes[bucket_count - 1];
    const largest_size_log2_count = largest_block_size + 1;
    const size_to_bucket_mapping: [largest_size_log2_count]usize = calc: {
        const array: [largest_size_log2_count]usize = @splat(0);
        var size: usize = 0;
        var bucket: usize = 0;
        while (size < largest_size_log2_count) {
            if (size > block_log2_sizes[bucket]) bucket += 1;
            array[size].* = bucket;
            size += 1;
        }
        break :calc array;
    };
    return struct {
        first_free_block_by_bucket: [BUCKET_COUNT]usize = @splat(0),
        first_unused_addr_by_bucket: [BUCKET_COUNT]usize = @splat(0),
        // first_free_allocation: usize,

        const QuickAlloc = @This();
        const BUCKET_COUNT = bucket_count;
        const ALLOC_ERROR_BEHAVIOR = options.slab_allocation_fail_behavior;
        const BLOCK_SIZE = block_sizes;
        const BLOCK_SIZE_LOG2 = block_log2_sizes;
        const SLAB_SIZE = slab_size;
        const SLAB_SIZE_LOG2 = slab_log2_size;
        const SLAB_SIZE_MODULO: usize = SLAB_SIZE - 1;
        const LARGEST_BLOCK_SIZE = BLOCK_SIZE[BUCKET_COUNT - 1];
        const LARGEST_BLOCK_SIZE_LOG2 = BLOCK_SIZE_LOG2[BUCKET_COUNT - 1];
        const BLOCKS_PER_SLAB = blocks_per_slab;
        const SIZE_LOG2_TO_BUCKET = size_to_bucket_mapping;
        const LARGEST_SIZE_LOG2 = largest_size_log2_count - 1;

        pub const VTABLE: Allocator.VTable = .{
            .alloc = alloc,
            .resize = resize,
            .remap = remap,
            .free = free,
        };

        pub inline fn allocator(self: *const QuickAlloc) Allocator {
            return Allocator{ .ptr = @ptrCast(@alignCast(self)), .vtable = &VTABLE };
        }

        fn alloc(self_opaque: *anyopaque, len: usize, alignment: mem.Alignment, ret_addr: usize) ?[*]u8 {
            _ = ret_addr;
            const self: *QuickAlloc = @ptrCast(@alignCast(self_opaque));
            const size_log2 = get_size_log2(len, alignment);
            const bucket = SIZE_LOG2_TO_BUCKET[size_log2];
            if (self.first_free_block_by_bucket[bucket] != 0) {
                const first_free_address = self.claim_address_from_free_list(bucket);
                return @ptrFromInt(first_free_address);
            }
            if (self.first_unused_addr_by_bucket[bucket] != 0) {
                const first_free_address = self.claim_address_from_unused_list(bucket);
                return @ptrFromInt(first_free_address);
            }
            return self.alloc_new_slab(bucket);
        }

        fn alloc_new_slab(self: *QuickAlloc, bucket: usize) ?[*]u8 {
            const block_size = BLOCK_SIZE[bucket];
            const new_slab = PageAllocator.map(SLAB_SIZE, mem.Alignment.fromByteUnits(block_size)) orelse return handle_error(?[*]u8, null, ALLOC_ERROR_BEHAVIOR, ERROR_ALLOCATION_FAIL, @tagName(@as(AllocSize, @enumFromInt(BLOCK_SIZE_LOG2[bucket]))));
            self.first_unused_addr_by_bucket[bucket] = (@intFromPtr(new_slab) + block_size) & SLAB_SIZE_MODULO;
            return new_slab;
        }

        fn resize(self_opaque: *anyopaque, memory: []u8, alignment: mem.Alignment, new_len: usize, ret_addr: usize) bool {
            _ = ret_addr;
            _ = self_opaque;
            const old_size_log2 = get_size_log2(memory.len, alignment);
            const new_size_log2 = get_size_log2(new_len, alignment);
            const old_bucket = SIZE_LOG2_TO_BUCKET[old_size_log2];
            const new_bucket = SIZE_LOG2_TO_BUCKET[new_size_log2];
            return old_bucket == new_bucket;
        }

        fn remap(self_opaque: *anyopaque, memory: []u8, alignment: mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
            _ = ret_addr;
            _ = self_opaque;
            const old_size_log2 = get_size_log2(memory.len, alignment);
            const new_size_log2 = get_size_log2(new_len, alignment);
            const old_bucket = SIZE_LOG2_TO_BUCKET[old_size_log2];
            const new_bucket = SIZE_LOG2_TO_BUCKET[new_size_log2];
            if (old_bucket == new_bucket) return memory.ptr;
            return null;
        }

        fn free(self_opaque: *anyopaque, memory: []u8, alignment: mem.Alignment, ret_addr: usize) void {
            _ = ret_addr;
            const self: *QuickAlloc = @ptrCast(@alignCast(self_opaque));
            const size_log2 = get_size_log2(memory.len, alignment);
            const bucket = SIZE_LOG2_TO_BUCKET[size_log2];
            self.return_address_to_free_list(bucket, @intFromPtr(memory.ptr));
        }

        inline fn return_address_to_free_list(self: *QuickAlloc, bucket: usize, address: usize) void {
            const prev_first_free_address = self.first_free_block_by_bucket[bucket];
            const curr_first_free_ptr: *usize = @ptrFromInt(address);
            curr_first_free_ptr.* = prev_first_free_address;
            self.first_free_block_by_bucket[bucket] = address;
        }

        inline fn claim_address_from_free_list(self: *QuickAlloc, bucket: usize) usize {
            const first_free_address = self.first_free_block_by_bucket[bucket];
            const next_free_address: *usize = @ptrFromInt(first_free_address);
            self.first_free_block_by_bucket[bucket].* = next_free_address.*;
            return first_free_address;
        }

        inline fn claim_address_from_unused_list(self: *QuickAlloc, bucket: usize) usize {
            const first_unused_addr = self.first_unused_addr_by_bucket[bucket];
            const next_unused_addr_pre_mod = first_unused_addr + BLOCK_SIZE[bucket];
            const next_unused_addr = next_unused_addr_pre_mod & SLAB_SIZE_MODULO;
            self.first_unused_addr_by_bucket[bucket] = next_unused_addr;
            return first_unused_addr;
        }

        // pub fn organize_free_blocks(minimum_allocations_to_keep: usize, maximum_allocations_to_keep: usize) void {}

        inline fn get_size_log2(len: usize, alignment: mem.Alignment) usize {
            const size_log2 = @max(@bitSizeOf(usize) - @clz(len - 1), @intFromEnum(alignment));
            assert(size_log2 <= LARGEST_SIZE_LOG2);
            return size_log2;
        }

        pub fn log_free_blocks(self: *const QuickAlloc) void {
            @branchHint(.cold);
            var i: usize = 0;
            std.log.info("\n[QuickAlloc] Free Block Report\nSIZE GROUP | FREE BLOCKS | FREE SLABS\n----------+-------------+-----------", .{});
            while (i < BUCKET_COUNT) : (i += 1) {
                var block_count: usize = 0;
                var first_free_addr: usize = self.first_free_block_by_bucket[i];
                while (first_free_addr != 0) {
                    block_count += 1;
                    const first_free_ptr: *usize = @ptrFromInt(first_free_addr);
                    first_free_addr = first_free_ptr.*;
                }
                const slab_count = block_count / BLOCKS_PER_SLAB[i];
                std.log.info("{s: >10} | {d: >11} | {d: >10}\n", .{ @tagName(@as(AllocSize, @enumFromInt(BLOCK_SIZE_LOG2[i]))), block_count, slab_count });
            }
        }
    };
}

const ERROR_ALLOCATION_FAIL = "Backing allocator failed to allocate memory for bucket: {s}";

inline fn handle_error(comptime return_type: type, comptime return_val: return_type, comptime behavior: ErrorBehavior, msg: []const u8, args: anytype) return_type {
    switch (behavior) {
        .RETURN => return return_val,
        .LOG_AND_RETURN => {
            std.log.err(msg, args);
            return return_val;
        },
        .PANIC => std.debug.panic(msg, args),
        .UNREACHABLE => unreachable,
    }
}
