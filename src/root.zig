const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const math = std.math;
const Allocator = std.mem.Allocator;
const PageAllocator = std.heap.PageAllocator;
const List = std.ArrayList;
const BranchHint = std.builtin.BranchHint;

pub const QuickAllocOptions = struct {
    /// A list of all allocation size buckets desired for this allocator. Any requested allocation will be
    /// sent to the smallest bucket that can hold it, and take exactly one single block from that bucket.
    ///
    /// This list MUST follow the following rules:
    /// - all buckets in sorted order from smallest block_size to largest block_size
    /// - all block sizes different
    /// - smallest block size is >= @sizeOf(usize)
    /// - all slab sizes >= their respective block sizes
    /// - all slab sizes >= std.heap.page_size_min
    buckets: []const BucketDef,
    /// If a requested allocation exceeds the maximum block size, how should this allocator respond
    ///
    /// the `.UNREACHABLE` option will slightly reduce the overhead of checking whether a requested allocation does or does not fall
    /// into the 'large' category and prevent branching
    large_allocation_behavior: LargeAllocBehavior = LargeAllocBehavior.USE_PAGE_ALLOCATOR,
    /// How likely it is for this allocator to get an allocation request greater than the largest block size
    hint_large_allocation: Hint = Hint.UNKNOWN,
    /// How likely is it that a free block will exist for any given bucket
    /// that was once used but returned (freed) back to the allocator for re-use
    hint_buckets_have_free_blocks_that_were_used_in_the_past: Hint = Hint.UNKNOWN,
    /// How likely is it that a free block will exist for any given bucket that has never been used before
    hint_buckets_have_free_blocks_that_have_never_been_used: Hint = Hint.UNKNOWN,
    /// How often you plan on using the provided usage statistic logging function
    hint_log_usage_statistics: Hint = Hint.ALMOST_NEVER,
};

/// How often you, the user, predict a specific case will occur given your application use case
///
/// These hints are translated directly into `std.builting.BranchHint` for their respective branches:
/// - .UNKNOWN == .none
/// - .VERY_LIKELY = .likely
/// - .VERY_UNLIKELY = .unlikely
/// - .ALMOST_NEVER = .cold
/// - .CANNOT_PREDICT = .unpredictable
pub const Hint = enum {
    /// You do not know how likely this case is
    UNKNOWN,
    /// You consider this case much more likely to occur
    VERY_LIKELY,
    /// You consider this case much less likely to occur
    VERY_UNLIKELY,
    /// You consider this case to be extremely rare,
    ALMOST_NEVER,
    /// The frequency of this case occuring cannot be predicted
    CANNOT_PREDICT,

    inline fn to_hint(comptime self: Hint) BranchHint {
        return switch (self) {
            Hint.UNKNOWN => BranchHint.none,
            Hint.VERY_LIKELY => BranchHint.likely,
            Hint.VERY_UNLIKELY => BranchHint.unlikely,
            Hint.ALMOST_NEVER => BranchHint.cold,
            Hint.CANNOT_PREDICT => BranchHint.unpredictable,
        };
    }
};

pub const BucketDef = struct {
    /// The size of the memory region requested from the backing allocator when this bucket does not have
    /// any more blocks left. This size will then be divided into individual blocks and appended to this bucket
    /// free list.
    ///
    /// This value must follow the following rules:
    /// - slab_size >= bucket size
    /// - slab_size >= std.heap.page_size_min
    slab_size: AllocSize,
    /// The size of individiual memory blocks for this bucket. The slab_size is divided into individual blocks
    /// for this bucket to use, but it WILL NOT use multiple blocks for a single allocation.
    ///
    /// Instead an allocation larger than this will be sent to the next largest bucket that can hold the requested bytes.
    ///
    /// This value must follow the following rules:
    /// - block size is >= @sizeOf(usize)
    /// - block_size is <= slab_size
    block_size: AllocSize,
};

pub const AllocSize = enum(math.Log2Int(usize)) {
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
    _8_GB = 33,
    _16_GB = 34,
    _32_GB = 35,
    _64_GB = 36,
    _128_GB = 37,
    _256_GB = 38,
    _512_GB = 39,
    _1_Terabyte = 40,
    _2_Terabytes = 41,
    _4_Terabytes = 42,
    _8_Terabytes = 43,
    _16_Terabytes = 44,
    _32_Terabytes = 45,
    _64_Terabytes = 46,
    _128_Terabytes = 47,
    _256_Terabytes = 48,
    _512_Terabytes = 49,
    _1_Petabyte = 50,
    _2_Petabytes = 51,
    _4_Petabytes = 52,
    _8_Petabytes = 53,
    _16_Petabytes = 54,
    _32_Petabytes = 55,
    _64_Petabytes = 56,
    _128_Petabytes = 57,
    _256_Petabytes = 58,
    _512_Petabytes = 59,
    _1_Exabyte = 60,
    _2_Exabytes = 61,
    _4_Exabytes = 62,
    _8_Exabytes = 63,
    _16_Exabytes = 64,

    pub inline fn bytes_log2(self: AllocSize) usize {
        return @intFromEnum(self);
    }

    pub inline fn bytes(self: AllocSize) usize {
        return @as(usize, 1) << @intFromEnum(self);
    }

    pub inline fn to_alignment(self: AllocSize) mem.Alignment {
        return @enumFromInt(@intFromEnum(self));
    }
};

// pub const ErrorBehavior = enum {
//     RETURN_FAIL_VALUE,
//     LOG_AND_RETURN_FAIL_VALUE,
//     PANIC,
//     UNREACHABLE,
// };

pub const LargeAllocBehavior = enum {
    USE_PAGE_ALLOCATOR,
    USE_PAGE_ALLOCATOR_AND_LOG,
    PANIC,
    UNREACHABLE,
};

fn build_table_type(comptime buckets: []const BucketDef) type {
    // Sort slabs by size, check their validity
    var idx: usize = 0;
    const temp_slab_buf: [buckets.len]usize = undefined;
    var temp_slab_len: usize = 0;
    outer: while (idx < buckets.len) : (idx += 1) {
        const slab_size = buckets[idx].slab_size.bytes_log2();
        const slab_bytes = buckets[idx].slab_size.bytes();
        if (slab_bytes < std.heap.page_size_min) @panic("all slab sizes must be >= std.heap.page_size_min");
        var sidx: usize = 0;
        while (sidx < temp_slab_len) : (sidx += 1) {
            if (slab_size < temp_slab_buf[sidx]) {
                mem.copyBackwards(usize, temp_slab_buf[sidx + 1 .. temp_slab_len + 1], temp_slab_buf[sidx..temp_slab_len]);
                temp_slab_buf[sidx] = slab_size;
                temp_slab_len += 1;
                continue :outer;
            }
            if (slab_size == temp_slab_buf[sidx]) continue :outer;
        }
        temp_slab_buf[temp_slab_len] = slab_size;
        temp_slab_len += 1;
    }
    const slab_arr: [temp_slab_len]usize = undefined;
    @memcpy(slab_arr[0..temp_slab_len], temp_slab_buf[0..temp_slab_len]);
    // build table type
    return struct {
        pub const largest_block_size_log2: usize = buckets[buckets.len - 1].block_size.bytes_log2();
        pub const smallest_block_size_log2: usize = buckets[0].block_size.bytes_log2();
        pub const size_count: usize = buckets[buckets.len - 1].block_size.bytes_log2() + 1;
        pub const slab_count: usize = slab_arr.len;
        pub const slab_sizes: [slab_count]usize = slab_arr;
        block_log2_sizes: [buckets.len]math.Log2Int(usize) = undefined,
        block_byte_sizes: [buckets.len]usize = undefined,
        slab_log2_sizes: [buckets.len]math.Log2Int(usize) = undefined,
        slab_byte_sizes: [buckets.len]usize = undefined,
        slab_modulo: [buckets.len]usize = undefined,
        blocks_per_slab: [buckets.len]usize = undefined,
        leftover_blocks_per_slab: [buckets.len]usize = undefined,
        bucket_to_slab_mapping: [buckets.len]usize = undefined,
        alloc_size_to_bucket_mapping: [size_count]usize = undefined,
    };
}

fn build_tables(comptime TableType: type, comptime buckets: []const BucketDef) TableType {
    // check buckets and build bucket idx tables
    var idx: usize = 0;
    var tables = TableType{};
    var this_block_size: usize = buckets[idx].block_size.bytes_log2();
    while (idx < buckets.len) : (idx += 1) {
        const this_block_bytes = buckets[idx].block_size.bytes();
        const this_slab_size = buckets[idx].slab_size.bytes_log2();
        const this_slab_bytes = buckets[idx].slab_size.bytes();
        if (this_block_size > this_slab_size) @panic("Cannot have a block size larger than slab_size");
        if (this_block_bytes < @sizeOf(usize)) @panic("Cannot have a block size smaller than @sizeOf(usize)");
        if (idx < buckets.len - 1) {
            const next_size = @intFromEnum(buckets[idx + 1]);
            if (this_block_size >= next_size) @panic("bucket sizes MUST be in sorted order from smallest to largest, and there cannot be 2 buckets of the same size");
            this_block_size = next_size;
        }
        const slab_idx = calc: {
            var s: usize = 0;
            while (s < TableType.slab_count) : (s += 1) {
                if (this_slab_size == TableType.slab_sizes[s]) break :calc s;
            }
            unreachable;
        };
        tables.block_log2_sizes[idx] = this_block_size;
        tables.block_byte_sizes[idx] = this_block_bytes;
        tables.slab_log2_sizes[idx] = this_slab_size;
        tables.slab_byte_sizes[idx] = this_slab_bytes;
        tables.slab_modulo[idx] = this_block_bytes - 1;
        tables.blocks_per_slab[idx] = this_slab_bytes / this_block_bytes;
        tables.leftover_blocks_per_slab[idx] = tables.blocks_per_slab[idx] - 1;
        tables.bucket_to_slab_mapping[idx] = slab_idx;
    }
    // build alloc_size to bucket_idx mapping
    idx = 0;
    var bucket_idx: usize = 0;
    while (idx < tables.size_count) : (idx += 1) {
        const size = @as(usize, 1) << idx;
        if (size > tables.block_log2_sizes[bucket_idx]) bucket_idx += 1;
        tables.alloc_size_to_bucket_mapping[idx] = bucket_idx;
    }
}

pub fn define_allocator(comptime options: QuickAllocOptions) type {
    if (options.bucket_sizes.len == 0) @panic("must provide at least one allocation bucket");
    const TableType = build_table_type(options.buckets);
    const tables = build_tables(TableType, options.buckets);
    return struct {
        first_recycled_block_by_bucket: [BUCKET_COUNT]usize = @splat(0),
        recycled_block_count_by_bucket: [BUCKET_COUNT]usize = @splat(0),
        first_brand_new_block_by_bucket: [BUCKET_COUNT]usize = @splat(0),
        brand_new_block_count_by_bucket: [BUCKET_COUNT]usize = @splat(0),
        // first_free_allocation: usize,

        const QuickAlloc = @This();
        const BUCKET_COUNT = TableType.size_count;
        const ALLOC_ERROR_BEHAVIOR = options.slab_allocation_fail_behavior;
        const BLOCK_BYTES = tables.block_byte_sizes;
        const BLOCK_BYTES_LOG2 = tables.block_log2_sizes;
        const SLAB_BYTES = tables.slab_byte_sizes;
        const SLAB_BYTES_LOG2 = tables.slab_log2_sizes;
        const SLAB_MODULO = tables.slab_modulo;
        const BLOCKS_PER_SLAB = tables.blocks_per_slab;
        const LEFTOVER_BLOCKS_PER_SLAB = tables.leftover_blocks_per_slab;
        const ALLOC_SIZE_LOG2_TO_BUCKET_IDX = tables.alloc_size_to_bucket_mapping;
        const LARGEST_BLOCK_SIZE_LOG2 = TableType.largest_block_size_log2;
        const SMALLEST_BLOCK_SIZE_LOG2 = TableType.smallest_block_size_log2;
        const LARGE_ALLOC_BEHAVIOR = options.large_allocation_behavior;
        const LARGE_ALLOC_HINT = options.hint_large_allocation.to_hint();
        const RECYCLE_HINT = options.hint_buckets_have_free_blocks_that_were_used_in_the_past;
        const BRAND_NEW_HINT = options.hint_buckets_have_free_blocks_that_have_never_been_used;
        const STAT_LOG_HINT = options.hint_log_usage_statistics;
        const MSG_LARGE_ALLOCATION = "Large allocation: largest bucket size is " ++ @tagName(@as(AllocSize, @enumFromInt(LARGEST_BLOCK_SIZE_LOG2))) ++ ", but the requested allocation would require a bucket size of {s}";
        const MSG_LARGE_RESIZE = "Large resize/remap: largest bucket size is " ++ @tagName(@as(AllocSize, @enumFromInt(LARGEST_BLOCK_SIZE_LOG2))) ++ ", but either the old memory allocation ({s}) or the new memory allocation ({s}) would exceed this limit";
        const MSG_LARGE_FREE = "Large free: largest bucket size is " ++ @tagName(@as(AllocSize, @enumFromInt(LARGEST_BLOCK_SIZE_LOG2))) ++ ", but the memory provided to free exceeds this limit: {s}";

        pub const VTABLE: Allocator.VTable = .{
            .alloc = alloc,
            .resize = resize,
            .remap = remap,
            .free = free,
        };

        pub inline fn allocator(self: *QuickAlloc) Allocator {
            return Allocator{ .ptr = @ptrCast(@alignCast(self)), .vtable = &VTABLE };
        }

        fn alloc(self_opaque: *anyopaque, len: usize, alignment: mem.Alignment, ret_addr: usize) ?[*]u8 {
            _ = ret_addr;
            const self: *QuickAlloc = @ptrCast(@alignCast(self_opaque));
            const alloc_size_log2 = get_size_log2(len, alignment);
            switch (LARGE_ALLOC_BEHAVIOR) {
                .USE_PAGE_ALLOC => if (alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(LARGE_ALLOC_HINT);
                    return PageAllocator.map(len, alignment);
                },
                .USE_PAGE_ALLOC_AND_LOG => if (alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(LARGE_ALLOC_HINT);
                    std.log.info(MSG_LARGE_ALLOCATION, .{@tagName(@as(AllocSize, @enumFromInt(alloc_size_log2)))});
                    return PageAllocator.map(len, alignment);
                },
                .PANIC => if (alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(.cold);
                    std.debug.panic(MSG_LARGE_ALLOCATION, .{@tagName(@as(AllocSize, @enumFromInt(alloc_size_log2)))});
                },
                .UNREACHABLE => if (alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) unreachable,
            }
            const bucket_idx = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[alloc_size_log2];
            if (self.recycled_block_count_by_bucket[bucket_idx] != 0) {
                @branchHint(RECYCLE_HINT);
                const first_free_address = self.first_recycled_block_by_bucket[bucket_idx];
                const next_free_address: *usize = @ptrFromInt(first_free_address);
                self.first_recycled_block_by_bucket[bucket_idx] = next_free_address.*;
                self.recycled_block_count_by_bucket[bucket_idx] -= 1;
                return @ptrFromInt(first_free_address);
            }
            if (self.brand_new_block_count_by_bucket[bucket_idx] != 0) {
                @branchHint(BRAND_NEW_HINT);
                const first_free_address = self.first_brand_new_block_by_bucket[bucket_idx];
                const next_unused_addr = first_free_address + BLOCK_BYTES[bucket_idx];
                self.first_brand_new_block_by_bucket[bucket_idx] = next_unused_addr;
                self.brand_new_block_count_by_bucket[bucket_idx] -= 1;
                return @ptrFromInt(first_free_address);
            }
            const block_size = BLOCK_BYTES[bucket_idx];
            const new_slab = PageAllocator.map(SLAB_BYTES[bucket_idx], bytes_log2_to_alignment(BLOCK_BYTES_LOG2[bucket_idx])) orelse return null;
            self.first_brand_new_block_by_bucket[bucket_idx] = @intFromPtr(new_slab) + block_size;
            self.brand_new_block_count_by_bucket[bucket_idx] = LEFTOVER_BLOCKS_PER_SLAB[bucket_idx];
            return new_slab;
        }

        fn resize(self_opaque: *anyopaque, memory: []u8, alignment: mem.Alignment, new_len: usize, ret_addr: usize) bool {
            _ = ret_addr;
            _ = self_opaque;
            const old_alloc_size_log2 = get_size_log2(memory.len, alignment);
            const new_alloc_size_log2 = get_size_log2(new_len, alignment);
            switch (LARGE_ALLOC_BEHAVIOR) {
                .USE_PAGE_ALLOC, .USE_PAGE_ALLOC_AND_LOG => if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 or new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(LARGE_ALLOC_HINT);
                    if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 and new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) return PageAllocator.realloc(memory, new_len, false) != null;
                    return false;
                },
                .PANIC => if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 or new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(.cold);
                    std.debug.panic(MSG_LARGE_RESIZE, .{ @tagName(@as(AllocSize, @enumFromInt(old_alloc_size_log2))), @tagName(@as(AllocSize, @enumFromInt(new_alloc_size_log2))) });
                },
                .UNREACHABLE => if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 or new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) unreachable,
            }
            const old_bucket = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[old_alloc_size_log2];
            const new_bucket = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[new_alloc_size_log2];
            return old_bucket == new_bucket;
        }

        fn remap(self_opaque: *anyopaque, memory: []u8, alignment: mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
            _ = ret_addr;
            _ = self_opaque;
            const old_alloc_size_log2 = get_size_log2(memory.len, alignment);
            const new_alloc_size_log2 = get_size_log2(new_len, alignment);
            switch (LARGE_ALLOC_BEHAVIOR) {
                .USE_PAGE_ALLOC, .USE_PAGE_ALLOC_AND_LOG => if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 or new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(LARGE_ALLOC_HINT);
                    if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 and new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) return PageAllocator.realloc(memory, new_len, true);
                    return null;
                },
                .PANIC => if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 or new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(.cold);
                    std.debug.panic(MSG_LARGE_RESIZE, .{ @tagName(@as(AllocSize, @enumFromInt(old_alloc_size_log2))), @tagName(@as(AllocSize, @enumFromInt(new_alloc_size_log2))) });
                },
                .UNREACHABLE => if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 or new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) unreachable,
            }
            const old_bucket = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[old_alloc_size_log2];
            const new_bucket = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[new_alloc_size_log2];
            if (old_bucket == new_bucket) return memory.ptr;
            return null;
        }

        fn free(self_opaque: *anyopaque, memory: []u8, alignment: mem.Alignment, ret_addr: usize) void {
            _ = ret_addr;
            const self: *QuickAlloc = @ptrCast(@alignCast(self_opaque));
            const alloc_size_log2 = get_size_log2(memory.len, alignment);
            switch (LARGE_ALLOC_BEHAVIOR) {
                .USE_PAGE_ALLOC, .USE_PAGE_ALLOC_AND_LOG => if (alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(LARGE_ALLOC_HINT);
                    return PageAllocator.unmap(@alignCast(memory));
                },
                .PANIC => if (alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(.cold);
                    std.debug.panic(MSG_LARGE_FREE, .{@tagName(@as(AllocSize, @enumFromInt(alloc_size_log2)))});
                },
                .UNREACHABLE => if (alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) unreachable,
            }
            const bucket_idx = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[alloc_size_log2];
            const prev_first_free_address = self.first_recycled_block_by_bucket[bucket_idx];
            const curr_first_free_addr: usize = @intFromPtr(memory.ptr);
            const curr_first_free_ptr: *usize = @ptrFromInt(curr_first_free_addr);
            curr_first_free_ptr.* = prev_first_free_address;
            self.first_recycled_block_by_bucket[bucket_idx] = curr_first_free_addr;
            self.recycled_block_count_by_bucket[bucket_idx] += 1;
        }

        // pub fn organize_free_blocks(minimum_allocations_to_keep: usize, maximum_allocations_to_keep: usize) void {}

        pub fn log_usage_statistics(self: *const QuickAlloc) void {
            @branchHint(STAT_LOG_HINT);
            var i: usize = 0;
            std.log.info("\n[QuickAlloc] Free Block Report\nSIZE GROUP | FREE BLOCKS | FREE SLABS\n----------+-------------+-----------", .{});
            while (i < BUCKET_COUNT) : (i += 1) {
                var block_count: usize = 0;
                var first_free_addr: usize = self.first_recycled_block_by_bucket[i];
                while (first_free_addr != 0) {
                    block_count += 1;
                    const first_free_ptr: *usize = @ptrFromInt(first_free_addr);
                    first_free_addr = first_free_ptr.*;
                }
                const slab_count = block_count / BLOCKS_PER_SLAB[i];
                std.log.info("{s: >10} | {d: >11} | {d: >10}\n", .{ @tagName(@as(AllocSize, @enumFromInt(BLOCK_BYTES_LOG2[i]))), block_count, slab_count });
            }
        }
    };
}

inline fn next_addr_within_same_slab_or_zero(next_addr: usize, modulo: usize, comptime smallest_block_size_log2: math.Log2Int(usize), comptime largest_block_size_log2: math.Log2Int(usize)) usize {
    const BITS_TO_FILL_LEFT = comptime @bitSizeOf(usize) - smallest_block_size_log2;
    const BITS_TO_FILL_RIGHT = comptime largest_block_size_log2;
    var next_addr_mask = next_addr & modulo;
    if (BITS_TO_FILL_LEFT > 1) next_addr_mask |= next_addr_mask << 1;
    if (BITS_TO_FILL_LEFT > 2) next_addr_mask |= next_addr_mask << 2;
    if (BITS_TO_FILL_LEFT > 4) next_addr_mask |= next_addr_mask << 4;
    if (BITS_TO_FILL_LEFT > 8) next_addr_mask |= next_addr_mask << 8;
    if (BITS_TO_FILL_LEFT > 16) next_addr_mask |= next_addr_mask << 16;
    if (BITS_TO_FILL_LEFT > 32) next_addr_mask |= next_addr_mask << 32;
    if (BITS_TO_FILL_LEFT > 64) next_addr_mask |= next_addr_mask << 64;
    if (BITS_TO_FILL_RIGHT > 1) next_addr_mask |= next_addr_mask >> 1;
    if (BITS_TO_FILL_RIGHT > 2) next_addr_mask |= next_addr_mask >> 2;
    if (BITS_TO_FILL_RIGHT > 4) next_addr_mask |= next_addr_mask >> 4;
    if (BITS_TO_FILL_RIGHT > 8) next_addr_mask |= next_addr_mask >> 8;
    if (BITS_TO_FILL_RIGHT > 16) next_addr_mask |= next_addr_mask >> 16;
    if (BITS_TO_FILL_RIGHT > 32) next_addr_mask |= next_addr_mask >> 32;
    if (BITS_TO_FILL_RIGHT > 64) next_addr_mask |= next_addr_mask >> 64;
    return next_addr & next_addr_mask;
}

inline fn get_size_log2(len: usize, alignment: mem.Alignment) usize {
    const size_log2 = @max(@bitSizeOf(usize) - @clz(len - 1), @intFromEnum(alignment));
    return size_log2;
}

inline fn bytes_log2_to_alignment(val: math.Log2Int(usize)) mem.Alignment {
    return @enumFromInt(val);
}
