const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const math = std.math;
const Allocator = std.mem.Allocator;
const PageAllocator = std.heap.PageAllocator;
const List = std.ArrayList;
const BranchHint = std.builtin.BranchHint;

const Log2Usize = math.Log2Int(usize);

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
    /// the `.UNREACHABLE` option will slightly reduce the processing overhead of checking whether a
    /// requested allocation does or does not fall into the 'large' category and prevent branching
    large_allocation_behavior: LargeAllocBehavior = LargeAllocBehavior.USE_PAGE_ALLOCATOR,
    /// Enable additional tracking for allocation usage.
    ///
    /// This option allows logging functions to report additional statistics,
    /// (or for the user to inspect the stats themselves). This adds a moderate amount of processing overhead to all
    /// allocation functions and increases the memory footprint of the allocator by around 3x.
    ///
    /// It provides no additional behavioral functionality or performance.
    track_allocation_statistics: bool = false,
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

pub const AllocSize = enum(Log2Usize) {
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

    pub inline fn bytes_log2(self: AllocSize) usize {
        return @intFromEnum(self);
    }

    pub inline fn bytes(self: AllocSize) usize {
        return @as(usize, 1) << @intFromEnum(self);
    }

    pub inline fn to_alignment(self: AllocSize) mem.Alignment {
        return @enumFromInt(@intFromEnum(self));
    }

    pub inline fn get_name(self: AllocSize) []const u8 {
        return get_name_from_bytes_log2(@intFromEnum(self));
    }

    pub fn get_name_from_bytes_log2(val: Log2Usize) []const u8 {
        return switch (val) {
            0 => "1 byte",
            1 => "2 bytes",
            2 => "4 bytes",
            3 => "8 bytes",
            4 => "16 bytes",
            5 => "32 bytes",
            6 => "64 bytes",
            7 => "128 bytes",
            8 => "256 bytes",
            9 => "512 bytes",
            10 => "1 kilobyte",
            11 => "2 kilobytes",
            12 => "4 kilobytes",
            13 => "8 kilobytes",
            14 => "16 kilobytes",
            15 => "32 kilobytes",
            16 => "64 kilobytes",
            17 => "128 kilobytes",
            18 => "256 kilobytes",
            19 => "512 kilobytes",
            20 => "1 megabyte",
            21 => "2 megabytes",
            22 => "4 megabytes",
            23 => "8 megabytes",
            24 => "16 megabytes",
            25 => "32 megabytes",
            26 => "64 megabytes",
            27 => "128 megabytes",
            28 => "256 megabytes",
            29 => "512 megabytes",
            30 => "1 gigabyte",
            31 => "2 gigabytes",
            32 => "4 gigabytes",
            33 => "8 gigabytes",
            34 => "16 gigabytes",
            35 => "32 gigabytes",
            36 => "64 gigabytes",
            37 => "128 gigabytes",
            38 => "256 gigabytes",
            39 => "512 gigabytes",
            40 => "1 terabyte",
            41 => "2 terabytes",
            42 => "4 terabytes",
            43 => "8 terabytes",
            44 => "16 terabytes",
            45 => "32 terabytes",
            46 => "64 terabytes",
            47 => "128 terabytes",
            48 => "256 terabytes",
            49 => "512 terabytes",
            50 => "1 petabyte",
            51 => "2 petabytes",
            52 => "4 petabytes",
            53 => "8 petabytes",
            54 => "16 petabytes",
            55 => "32 petabytes",
            56 => "64 petabytes",
            57 => "128 petabytes",
            58 => "256 petabytes",
            59 => "512 petabytes",
            60 => "1 exabyte",
            61 => "2 exabytes",
            62 => "4 exabytes",
            63 => "8 exabytes",
        };
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
    PANIC,
    UNREACHABLE,
};

pub fn define_allocator(comptime options: QuickAllocOptions) type {
    if (options.buckets.len == 0) @panic("must provide at least one allocation bucket");
    const buckets = options.buckets;
    // Sort slabs by size, check their validity
    var idx: usize = 0;
    var temp_slab_buf: [buckets.len]usize = undefined;
    var temp_slab_len: usize = 0;
    outer: while (idx < buckets.len) : (idx += 1) {
        const slab_size = buckets[idx].slab_size.bytes_log2();
        const slab_bytes = buckets[idx].slab_size.bytes();
        if (slab_bytes < std.heap.page_size_min) @panic("all slab sizes must be >= std.heap.page_size_min");
        var sidx: usize = 0;
        while (sidx < temp_slab_len) : (sidx += 1) {
            if (slab_size < temp_slab_buf[sidx]) {
                mem.copyBackwards(usize, temp_slab_buf[sidx + 1 .. temp_slab_len + 1], temp_slab_buf[sidx..temp_slab_len]);
                temp_slab_buf[sidx].* = slab_size;
                temp_slab_len += 1;
                continue :outer;
            }
            if (slab_size == temp_slab_buf[sidx]) continue :outer;
        }
        temp_slab_buf[temp_slab_len] = slab_size;
        temp_slab_len += 1;
    }
    var slab_arr: [temp_slab_len]usize = undefined;
    @memcpy(slab_arr[0..temp_slab_len], temp_slab_buf[0..temp_slab_len]);
    const largest_block_size_log2: usize = buckets[buckets.len - 1].block_size.bytes_log2();
    const smallest_block_size_log2: usize = buckets[0].block_size.bytes_log2();
    const size_count: usize = buckets[buckets.len - 1].block_size.bytes_log2() + 1;
    const slab_count: usize = temp_slab_len;
    const slab_sizes: [slab_count]usize = slab_arr;
    // build table type
    // check buckets and build bucket idx tables
    var block_log2_sizes: [buckets.len]Log2Usize = undefined;
    var block_byte_sizes: [buckets.len]usize = undefined;
    var slab_log2_sizes: [buckets.len]Log2Usize = undefined;
    var slab_byte_sizes: [buckets.len]usize = undefined;
    var slab_modulo: [buckets.len]usize = undefined;
    var blocks_per_slab: [buckets.len]usize = undefined;
    var leftover_blocks_per_slab: [buckets.len]usize = undefined;
    var bucket_to_slab_mapping: [buckets.len]usize = undefined;
    var alloc_size_to_bucket_mapping: [size_count]usize = undefined;
    idx = 0;
    var this_block_size: usize = buckets[idx].block_size.bytes_log2();
    while (idx < buckets.len) : (idx += 1) {
        const this_block_bytes = buckets[idx].block_size.bytes();
        const this_slab_size = buckets[idx].slab_size.bytes_log2();
        const this_slab_bytes = buckets[idx].slab_size.bytes();
        if (this_block_size > this_slab_size) @panic("Cannot have a block size larger than slab_size");
        if (this_block_bytes < @sizeOf(usize)) @panic("Cannot have a block size smaller than @sizeOf(usize)");
        const slab_idx = calc: {
            var s: usize = 0;
            while (s < slab_count) : (s += 1) {
                if (this_slab_size == slab_sizes[s]) break :calc s;
            }
            unreachable;
        };
        block_log2_sizes[idx] = this_block_size;
        block_byte_sizes[idx] = this_block_bytes;
        slab_log2_sizes[idx] = this_slab_size;
        slab_byte_sizes[idx] = this_slab_bytes;
        slab_modulo[idx] = this_block_bytes - 1;
        blocks_per_slab[idx] = this_slab_bytes / this_block_bytes;
        leftover_blocks_per_slab[idx] = blocks_per_slab[idx] - 1;
        bucket_to_slab_mapping[idx] = slab_idx;
        if (idx < buckets.len - 1) {
            const next_size = @intFromEnum(buckets[idx + 1].block_size);
            if (this_block_size >= next_size) @panic("bucket sizes MUST be in sorted order from smallest to largest, and there cannot be 2 buckets of the same size");
            this_block_size = next_size;
        }
    }
    // build alloc_size to bucket_idx mapping
    idx = 0;
    var this_bucket_idx: usize = 0;
    while (idx < size_count) : (idx += 1) {
        if (idx > block_log2_sizes[this_bucket_idx]) this_bucket_idx += 1;
        alloc_size_to_bucket_mapping[idx] = this_bucket_idx;
    }
    const const_block_byte_sizes: [buckets.len]usize = comptime make: {
        var arr: [buckets.len]usize = undefined;
        @memcpy(&arr, &block_byte_sizes);
        break :make arr;
    };
    const const_block_log2_sizes: [buckets.len]Log2Usize = comptime make: {
        var arr: [buckets.len]Log2Usize = undefined;
        @memcpy(&arr, &block_log2_sizes);
        break :make arr;
    };
    const const_slab_byte_sizes: [buckets.len]usize = comptime make: {
        var arr: [buckets.len]usize = undefined;
        @memcpy(&arr, &slab_byte_sizes);
        break :make arr;
    };
    const const_slab_log2_sizes: [buckets.len]Log2Usize = comptime make: {
        var arr: [buckets.len]Log2Usize = undefined;
        @memcpy(&arr, &slab_log2_sizes);
        break :make arr;
    };
    const const_slab_modulo: [buckets.len]usize = comptime make: {
        var arr: [buckets.len]usize = undefined;
        @memcpy(&arr, &slab_modulo);
        break :make arr;
    };
    const const_blocks_per_slab: [buckets.len]usize = comptime make: {
        var arr: [buckets.len]usize = undefined;
        @memcpy(&arr, &blocks_per_slab);
        break :make arr;
    };
    const const_leftover_blocks_per_slab: [buckets.len]usize = comptime make: {
        var arr: [buckets.len]usize = undefined;
        @memcpy(&arr, &leftover_blocks_per_slab);
        break :make arr;
    };
    const const_alloc_size_to_bucket_mapping: [size_count]usize = comptime make: {
        var arr: [size_count]usize = undefined;
        @memcpy(&arr, &alloc_size_to_bucket_mapping);
        break :make arr;
    };
    return struct {
        first_recycled_block_by_bucket: [BUCKET_COUNT]usize = @splat(0),
        recycled_block_count_by_bucket: [BUCKET_COUNT]usize = @splat(0),
        first_brand_new_block_by_bucket: [BUCKET_COUNT]usize = @splat(0),
        brand_new_block_count_by_bucket: [BUCKET_COUNT]usize = @splat(0),
        stats: AllocStats = AllocStats{},
        // first_free_allocation: usize,

        const QuickAlloc = @This();
        const BUCKET_COUNT = buckets.len;
        const ALLOC_ERROR_BEHAVIOR = options.slab_allocation_fail_behavior;
        const BLOCK_BYTES = const_block_byte_sizes;
        const BLOCK_BYTES_LOG2 = const_block_log2_sizes;
        const SLAB_BYTES = const_slab_byte_sizes;
        const SLAB_BYTES_LOG2 = const_slab_log2_sizes;
        const SLAB_MODULO = const_slab_modulo;
        const BLOCKS_PER_SLAB = const_blocks_per_slab;
        const LEFTOVER_BLOCKS_PER_SLAB = const_leftover_blocks_per_slab;
        const ALLOC_SIZE_LOG2_TO_BUCKET_IDX = const_alloc_size_to_bucket_mapping;
        const LARGEST_BLOCK_SIZE_LOG2 = largest_block_size_log2;
        const SMALLEST_BLOCK_SIZE_LOG2 = smallest_block_size_log2;
        const LARGE_ALLOC_BEHAVIOR = options.large_allocation_behavior;
        const PAGE_ALLOC: bool = LARGE_ALLOC_BEHAVIOR == .USE_PAGE_ALLOCATOR;
        const LARGE_ALLOC_HINT = options.hint_large_allocation.to_hint();
        const RECYCLE_HINT = options.hint_buckets_have_free_blocks_that_were_used_in_the_past.to_hint();
        const BRAND_NEW_HINT = options.hint_buckets_have_free_blocks_that_have_never_been_used.to_hint();
        const STAT_LOG_HINT = options.hint_log_usage_statistics.to_hint();
        const TRACK_STATS = options.track_allocation_statistics;
        const MSG_LARGE_ALLOCATION = "Large allocation: largest bucket size is " ++ @tagName(@as(AllocSize, @enumFromInt(LARGEST_BLOCK_SIZE_LOG2))) ++ ", but the requested allocation would require a bucket size of {s}";
        const MSG_LARGE_RESIZE = "Large resize/remap: largest bucket size is " ++ @tagName(@as(AllocSize, @enumFromInt(LARGEST_BLOCK_SIZE_LOG2))) ++ ", but either the old memory allocation ({s}) or the new memory allocation ({s}) would exceed this limit";
        const MSG_LARGE_FREE = "Large free: largest bucket size is " ++ @tagName(@as(AllocSize, @enumFromInt(LARGEST_BLOCK_SIZE_LOG2))) ++ ", but the memory provided to free exceeds this limit: {s}";

        pub const AllocStats = if (!TRACK_STATS) void else struct {
            current_total_memory_allocated: usize = 0,
            largest_total_memory_allocated: usize = 0,
            smallest_allocation_request_ever: usize = math.maxInt(usize),
            largest_allocation_request_ever: usize = 0,
            largest_allocation_request_by_bucket: [BUCKET_COUNT]usize = @splat(0),
            smallest_allocation_request_by_bucket: [BUCKET_COUNT]usize = @splat(math.maxInt(usize)),
            most_blocks_ever_used_by_bucket: [BUCKET_COUNT]usize = @splat(0),
            current_blocks_used_by_bucket: [BUCKET_COUNT]usize = @splat(0),
            most_slabs_ever_used_by_bucket: [BUCKET_COUNT]usize = @splat(0),
            current_slabs_used_by_bucket: [BUCKET_COUNT]usize = @splat(0),
            number_of_attempted_resizes_to_larger_buckets_by_bucket: [BUCKET_COUNT]usize = @splat(0),
            largest_page_allocator_fallback_request_ever: if (PAGE_ALLOC) usize else void = if (PAGE_ALLOC) 0 else void{},
            smallest_page_allocator_fallback_request_ever: if (PAGE_ALLOC) usize else void = if (PAGE_ALLOC) math.maxInt(usize) else void{},
            largest_total_bytes_allocated_from_page_allocator_fallback: if (PAGE_ALLOC) usize else void = if (PAGE_ALLOC) 0 else void{},
            current_total_bytes_allocated_from_page_allocator_fallback: if (PAGE_ALLOC) usize else void = if (PAGE_ALLOC) 0 else void{},
            largest_number_of_page_allocator_fallback_allocations: if (PAGE_ALLOC) usize else void = if (PAGE_ALLOC) 0 else void{},
            current_number_of_page_allocator_fallback_allocations: if (PAGE_ALLOC) usize else void = if (PAGE_ALLOC) 0 else void{},
            largest_attempted_page_allocator_fallback_resize_delta_grow: if (PAGE_ALLOC) usize else void = if (PAGE_ALLOC) 0 else void{},
            largest_attempted_page_allocator_fallback_resize_delta_shrink: if (PAGE_ALLOC) usize else void = if (PAGE_ALLOC) 0 else void{},
        };

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
            const alloc_size_log2 = get_bytes_log2(len, alignment);
            if (TRACK_STATS) {
                if (len < self.stats.smallest_allocation_request_ever) self.stats.smallest_allocation_request_ever = len;
                if (len > self.stats.largest_allocation_request_ever) self.stats.largest_allocation_request_ever = len;
            }
            switch (LARGE_ALLOC_BEHAVIOR) {
                LargeAllocBehavior.USE_PAGE_ALLOCATOR => if (alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(LARGE_ALLOC_HINT);
                    if (TRACK_STATS) {
                        if (len > self.stats.largest_page_allocator_fallback_request_ever) self.stats.largest_page_allocator_fallback_request_ever = len;
                        if (len < self.stats.smallest_page_allocator_fallback_request_ever) self.stats.smallest_page_allocator_fallback_request_ever = len;
                        self.stats.current_number_of_page_allocator_fallback_allocations += 1;
                        if (self.stats.current_number_of_page_allocator_fallback_allocations > self.stats.largest_number_of_page_allocator_fallback_allocations) self.stats.largest_number_of_page_allocator_fallback_allocations = self.stats.current_number_of_page_allocator_fallback_allocations;
                        self.stats.current_total_bytes_allocated_from_page_allocator_fallback += len;
                        if (self.stats.current_total_bytes_allocated_from_page_allocator_fallback > self.stats.largest_total_bytes_allocated_from_page_allocator_fallback) self.stats.largest_total_bytes_allocated_from_page_allocator_fallback = self.stats.current_total_bytes_allocated_from_page_allocator_fallback;
                        self.stats.current_total_memory_allocated += len;
                        if (self.stats.current_total_memory_allocated > self.stats.largest_total_memory_allocated) self.stats.largest_total_memory_allocated = self.stats.current_total_memory_allocated;
                    }
                    return PageAllocator.map(len, alignment);
                },
                LargeAllocBehavior.PANIC => if (alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(.cold);
                    std.debug.panic(MSG_LARGE_ALLOCATION, .{AllocSize.get_name_from_bytes_log2(alloc_size_log2)});
                },
                LargeAllocBehavior.UNREACHABLE => if (alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) unreachable,
            }
            const bucket_idx = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[alloc_size_log2];
            if (self.recycled_block_count_by_bucket[bucket_idx] != 0) {
                @branchHint(RECYCLE_HINT);
                const first_free_address = self.first_recycled_block_by_bucket[bucket_idx];
                const next_free_address: *usize = @ptrFromInt(first_free_address);
                self.first_recycled_block_by_bucket[bucket_idx] = next_free_address.*;
                self.recycled_block_count_by_bucket[bucket_idx] -= 1;
                if (TRACK_STATS) {
                    if (len > self.stats.largest_allocation_request_by_bucket[bucket_idx]) self.stats.largest_allocation_request_by_bucket[bucket_idx] = len;
                    if (len < self.stats.smallest_allocation_request_by_bucket[bucket_idx]) self.stats.smallest_allocation_request_by_bucket[bucket_idx] = len;
                    self.stats.current_blocks_used_by_bucket[bucket_idx] += 1;
                    if (self.stats.current_blocks_used_by_bucket[bucket_idx] > self.stats.most_blocks_ever_used_by_bucket[bucket_idx]) self.stats.most_blocks_ever_used_by_bucket[bucket_idx] = self.stats.current_blocks_used_by_bucket[bucket_idx];
                }
                return @ptrFromInt(first_free_address);
            }
            if (self.brand_new_block_count_by_bucket[bucket_idx] != 0) {
                @branchHint(BRAND_NEW_HINT);
                const first_free_address = self.first_brand_new_block_by_bucket[bucket_idx];
                const next_unused_addr = first_free_address + BLOCK_BYTES[bucket_idx];
                self.first_brand_new_block_by_bucket[bucket_idx] = next_unused_addr;
                self.brand_new_block_count_by_bucket[bucket_idx] -= 1;
                if (TRACK_STATS) {
                    if (len > self.stats.largest_allocation_request_by_bucket[bucket_idx]) self.stats.largest_allocation_request_by_bucket[bucket_idx] = len;
                    if (len < self.stats.smallest_allocation_request_by_bucket[bucket_idx]) self.stats.smallest_allocation_request_by_bucket[bucket_idx] = len;
                    self.stats.current_blocks_used_by_bucket[bucket_idx] += 1;
                    if (self.stats.current_blocks_used_by_bucket[bucket_idx] > self.stats.most_blocks_ever_used_by_bucket[bucket_idx]) self.stats.most_blocks_ever_used_by_bucket[bucket_idx] = self.stats.current_blocks_used_by_bucket[bucket_idx];
                }
                return @ptrFromInt(first_free_address);
            }
            const block_size = BLOCK_BYTES[bucket_idx];
            const new_slab = PageAllocator.map(SLAB_BYTES[bucket_idx], bytes_log2_to_alignment(BLOCK_BYTES_LOG2[bucket_idx])) orelse return null;
            self.first_brand_new_block_by_bucket[bucket_idx] = @intFromPtr(new_slab) + block_size;
            self.brand_new_block_count_by_bucket[bucket_idx] = LEFTOVER_BLOCKS_PER_SLAB[bucket_idx];
            if (TRACK_STATS) {
                if (len > self.stats.largest_allocation_request_by_bucket[bucket_idx]) self.stats.largest_allocation_request_by_bucket[bucket_idx] = len;
                if (len < self.stats.smallest_allocation_request_by_bucket[bucket_idx]) self.stats.smallest_allocation_request_by_bucket[bucket_idx] = len;
                self.stats.current_blocks_used_by_bucket[bucket_idx] += 1;
                if (self.stats.current_blocks_used_by_bucket[bucket_idx] > self.stats.most_blocks_ever_used_by_bucket[bucket_idx]) self.stats.most_blocks_ever_used_by_bucket[bucket_idx] = self.stats.current_blocks_used_by_bucket[bucket_idx];
                self.stats.current_slabs_used_by_bucket[bucket_idx] += 1;
                if (self.stats.current_slabs_used_by_bucket[bucket_idx] > self.stats.most_slabs_ever_used_by_bucket[bucket_idx]) self.stats.most_slabs_ever_used_by_bucket[bucket_idx] = self.stats.current_slabs_used_by_bucket[bucket_idx];
                self.stats.current_total_memory_allocated += SLAB_BYTES[bucket_idx];
                if (self.stats.current_total_memory_allocated > self.stats.largest_total_memory_allocated) self.stats.largest_total_memory_allocated = self.stats.current_total_memory_allocated;
            }
            return new_slab;
        }

        fn resize(self_opaque: *anyopaque, memory: []u8, alignment: mem.Alignment, new_len: usize, ret_addr: usize) bool {
            _ = ret_addr;
            const self: *QuickAlloc = @ptrCast(@alignCast(self_opaque));
            const old_alloc_size_log2 = get_bytes_log2(memory.len, alignment);
            const new_alloc_size_log2 = get_bytes_log2(new_len, alignment);
            switch (LARGE_ALLOC_BEHAVIOR) {
                LargeAllocBehavior.USE_PAGE_ALLOCATOR => if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 or new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(LARGE_ALLOC_HINT);
                    if (TRACK_STATS) {
                        if (old_alloc_size_log2 <= LARGEST_BLOCK_SIZE_LOG2 and new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                            const old_bucket = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[old_alloc_size_log2];
                            self.stats.number_of_attempted_resizes_to_larger_buckets_by_bucket[old_bucket] += 1;
                        } else if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 and new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                            if (new_len > memory.len) {
                                const delta = new_len - memory.len;
                                if (delta > self.stats.largest_attempted_page_allocator_fallback_resize_delta_grow) self.stats.largest_attempted_page_allocator_fallback_resize_delta_grow = delta;
                            }
                            if (new_len < memory.len) {
                                const delta = memory.len - new_len;
                                if (delta > self.stats.largest_attempted_page_allocator_fallback_resize_delta_shrink) self.stats.largest_attempted_page_allocator_fallback_resize_delta_shrink = delta;
                            }
                            if (new_len < self.stats.smallest_page_allocator_fallback_request_ever) self.stats.smallest_page_allocator_fallback_request_ever = new_len;
                            if (new_len > self.stats.largest_page_allocator_fallback_request_ever) self.stats.largest_page_allocator_fallback_request_ever = new_len;
                            const resize_result = PageAllocator.realloc(memory, new_len, false);
                            if (resize_result != null) {
                                if (new_len > memory.len) self.stats.current_total_memory_allocated += (new_len - memory.len);
                                if (new_len < memory.len) self.stats.current_total_memory_allocated -= (memory.len - new_len);
                            }
                            return resize_result != null;
                        }
                        return false;
                    }
                    if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 and new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) return PageAllocator.realloc(memory, new_len, false) != null;
                    return false;
                },
                LargeAllocBehavior.PANIC => if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 or new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(.cold);
                    std.debug.panic(MSG_LARGE_RESIZE, .{ AllocSize.get_name_from_bytes_log2(old_alloc_size_log2), AllocSize.get_name_from_bytes_log2(new_alloc_size_log2) });
                },
                LargeAllocBehavior.UNREACHABLE => if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 or new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) unreachable,
            }
            const old_bucket = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[old_alloc_size_log2];
            const new_bucket = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[new_alloc_size_log2];
            if (TRACK_STATS) {
                if (old_bucket < new_bucket) self.stats.number_of_attempted_resizes_to_larger_buckets_by_bucket[old_bucket] += 1;
                if (old_bucket == new_bucket and new_len > self.stats.largest_allocation_request_by_bucket[old_bucket]) self.stats.largest_allocation_request_by_bucket[old_bucket] = new_len;
                if (old_bucket >= new_bucket and new_len < self.stats.smallest_allocation_request_by_bucket[old_bucket]) self.stats.smallest_allocation_request_by_bucket[old_bucket] = new_len;
            }
            return old_bucket >= new_bucket;
        }

        fn remap(self_opaque: *anyopaque, memory: []u8, alignment: mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
            _ = ret_addr;
            const self: *QuickAlloc = @ptrCast(@alignCast(self_opaque));
            const old_alloc_size_log2 = get_bytes_log2(memory.len, alignment);
            const new_alloc_size_log2 = get_bytes_log2(new_len, alignment);
            switch (LARGE_ALLOC_BEHAVIOR) {
                LargeAllocBehavior.USE_PAGE_ALLOCATOR => if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 or new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(LARGE_ALLOC_HINT);
                    if (TRACK_STATS) {
                        if (old_alloc_size_log2 <= LARGEST_BLOCK_SIZE_LOG2 and new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                            const old_bucket = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[old_alloc_size_log2];
                            self.stats.number_of_attempted_resizes_to_larger_buckets_by_bucket[old_bucket] += 1;
                        } else if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 and new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                            if (new_len > memory.len) {
                                const delta = new_len - memory.len;
                                if (delta > self.stats.largest_attempted_page_allocator_fallback_resize_delta_grow) self.stats.largest_attempted_page_allocator_fallback_resize_delta_grow = delta;
                            }
                            if (new_len < memory.len) {
                                const delta = memory.len - new_len;
                                if (delta > self.stats.largest_attempted_page_allocator_fallback_resize_delta_shrink) self.stats.largest_attempted_page_allocator_fallback_resize_delta_shrink = delta;
                            }
                            if (new_len < self.stats.smallest_page_allocator_fallback_request_ever) self.stats.smallest_page_allocator_fallback_request_ever = new_len;
                            if (new_len > self.stats.largest_page_allocator_fallback_request_ever) self.stats.largest_page_allocator_fallback_request_ever = new_len;
                            const resize_result = PageAllocator.realloc(memory, new_len, true);
                            if (resize_result != null) {
                                if (new_len > memory.len) self.stats.current_total_memory_allocated += (new_len - memory.len);
                                if (new_len < memory.len) self.stats.current_total_memory_allocated -= (memory.len - new_len);
                            }
                            return resize_result;
                        }
                        return null;
                    }
                    if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 and new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) return PageAllocator.realloc(memory, new_len, true);
                    return null;
                },
                LargeAllocBehavior.PANIC => if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 or new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(.cold);
                    std.debug.panic(MSG_LARGE_RESIZE, .{ AllocSize.get_name_from_bytes_log2(old_alloc_size_log2), AllocSize.get_name_from_bytes_log2(new_alloc_size_log2) });
                },
                LargeAllocBehavior.UNREACHABLE => if (old_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2 or new_alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) unreachable,
            }
            const old_bucket = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[old_alloc_size_log2];
            const new_bucket = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[new_alloc_size_log2];
            if (TRACK_STATS) {
                if (old_bucket < new_bucket) self.stats.number_of_attempted_resizes_to_larger_buckets_by_bucket[old_bucket] += 1;
                if (old_bucket == new_bucket and new_len > self.stats.largest_allocation_request_by_bucket[old_bucket]) self.stats.largest_allocation_request_by_bucket[old_bucket] = new_len;
                if (old_bucket >= new_bucket and new_len < self.stats.smallest_allocation_request_by_bucket[old_bucket]) self.stats.smallest_allocation_request_by_bucket[old_bucket] = new_len;
            }
            if (old_bucket >= new_bucket) return memory.ptr;
            return null;
        }

        fn free(self_opaque: *anyopaque, memory: []u8, alignment: mem.Alignment, ret_addr: usize) void {
            _ = ret_addr;
            const self: *QuickAlloc = @ptrCast(@alignCast(self_opaque));
            const alloc_size_log2 = get_bytes_log2(memory.len, alignment);
            switch (LARGE_ALLOC_BEHAVIOR) {
                LargeAllocBehavior.USE_PAGE_ALLOCATOR => if (alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(LARGE_ALLOC_HINT);
                    if (TRACK_STATS) {
                        self.stats.current_number_of_page_allocator_fallback_allocations -= 1;
                        self.stats.current_total_bytes_allocated_from_page_allocator_fallback -= memory.len;
                        self.stats.current_total_memory_allocated -= memory.len;
                    }
                    return PageAllocator.unmap(@alignCast(memory));
                },
                LargeAllocBehavior.PANIC => if (alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) {
                    @branchHint(.cold);
                    std.debug.panic(MSG_LARGE_FREE, .{AllocSize.get_name_from_bytes_log2(alloc_size_log2)});
                },
                LargeAllocBehavior.UNREACHABLE => if (alloc_size_log2 > LARGEST_BLOCK_SIZE_LOG2) unreachable,
            }
            const bucket_idx = ALLOC_SIZE_LOG2_TO_BUCKET_IDX[alloc_size_log2];
            const prev_first_free_address = self.first_recycled_block_by_bucket[bucket_idx];
            const curr_first_free_addr: usize = @intFromPtr(memory.ptr);
            const curr_first_free_ptr: *usize = @ptrFromInt(curr_first_free_addr);
            curr_first_free_ptr.* = prev_first_free_address;
            self.first_recycled_block_by_bucket[bucket_idx] = curr_first_free_addr;
            self.recycled_block_count_by_bucket[bucket_idx] += 1;
            if (TRACK_STATS) {
                self.stats.current_blocks_used_by_bucket[bucket_idx] -= 1;
            }
        }

        // pub fn organize_free_blocks(minimum_allocations_to_keep: usize, maximum_allocations_to_keep: usize) void {}

        pub fn log_usage_statistics(self: *const QuickAlloc, log_buffer: *std.ArrayList(u8), comptime log_name: []const u8) void {
            @branchHint(STAT_LOG_HINT);
            log_buffer.clearRetainingCapacity();
            var log_writer = log_buffer.writer();
            var i: usize = 0;
            log_writer.print("\n[QuickAlloc] Usage Statistics ({s})\n======== FREE MEMORY STATS =========\n   SIZE GROUP | FREE SLABS | FREE BLOCKS | FREE BYTES\n--------------+------------+-------------+--------------------------\n", .{log_name}) catch @panic(FAILED_TO_LOG ++ log_name);
            while (i < BUCKET_COUNT) : (i += 1) {
                const block_bytes_log_2: Log2Usize = BLOCK_BYTES_LOG2[i];
                const block_bytes: usize = BLOCK_BYTES[i];
                const block_count: usize = self.recycled_block_count_by_bucket[i] + self.brand_new_block_count_by_bucket[i];
                const bucket_slab_count = block_count / BLOCKS_PER_SLAB[i];
                log_writer.print("{s: >13} | {d: >10} | {d: >11} | {d: >19} bytes\n", .{ AllocSize.get_name_from_bytes_log2(block_bytes_log_2), bucket_slab_count, block_count, block_count * block_bytes }) catch @panic(FAILED_TO_LOG ++ log_name);
            }
            if (TRACK_STATS) {
                log_writer.print("======== USED MEMORY STATS =========\n", .{}) catch @panic("failed to allocate memory for logging: " ++ log_name);
                log_writer.print("Current total memory allocated: {d} bytes\nLargest total memory allocated: {d} bytes\n", .{ self.stats.current_total_memory_allocated, self.stats.largest_total_memory_allocated }) catch @panic(FAILED_TO_LOG ++ log_name);
                log_writer.print("Smallest allocation ever requested: {d} bytes\nLargest allocation ever requested: {d} bytes\n", .{ if (self.stats.smallest_allocation_request_ever == math.maxInt(usize)) 0 else self.stats.smallest_allocation_request_ever, self.stats.largest_allocation_request_ever }) catch @panic(FAILED_TO_LOG ++ log_name);
                log_writer.print("---- BUCKET STATS ----\n", .{}) catch @panic(FAILED_TO_LOG ++ log_name);
                i = 0;
                while (i < BUCKET_COUNT) : (i += 1) {
                    const local_i = i;
                    const block_bytes_log_2: Log2Usize = BLOCK_BYTES_LOG2[local_i];
                    log_writer.print("  [{s}]\n", .{AllocSize.get_name_from_bytes_log2(block_bytes_log_2)}) catch @panic(FAILED_TO_LOG ++ log_name);
                    log_writer.print("     Smallest single allocation: {d} bytes\n     Largest single allocation: {d} bytes\n", .{ if (self.stats.smallest_allocation_request_by_bucket[i] == math.maxInt(usize)) 0 else self.stats.smallest_allocation_request_by_bucket[i], self.stats.largest_allocation_request_by_bucket[i] }) catch @panic(FAILED_TO_LOG ++ log_name);
                    log_writer.print("     Current blocks used by bucket: {d}\n     Most blocks ever used by bucket: {d}\n", .{ self.stats.current_blocks_used_by_bucket[i], self.stats.most_blocks_ever_used_by_bucket[i] }) catch @panic(FAILED_TO_LOG ++ log_name);
                    log_writer.print("     Current slabs used by bucket: {d}\n     Most slabs ever used by bucket: {d}\n", .{ self.stats.current_slabs_used_by_bucket[i], self.stats.most_slabs_ever_used_by_bucket[i] }) catch @panic(FAILED_TO_LOG ++ log_name);
                    log_writer.print("     Number of attempted resizes to larger buckets: {d}\n", .{self.stats.number_of_attempted_resizes_to_larger_buckets_by_bucket[i]}) catch @panic(FAILED_TO_LOG ++ log_name);
                }
                if (PAGE_ALLOC) {
                    log_writer.print("  [Page Allocator]\n", .{}) catch @panic(FAILED_TO_LOG ++ log_name);
                    log_writer.print("     Smallest single allocation: {d} bytes\n     Largest single allocation: {d} bytes\n", .{ if (self.stats.smallest_page_allocator_fallback_request_ever == math.maxInt(usize)) 0 else self.stats.smallest_page_allocator_fallback_request_ever, self.stats.largest_page_allocator_fallback_request_ever }) catch @panic(FAILED_TO_LOG ++ log_name);
                    log_writer.print("     Current total bytes allocated: {d} bytes\n     Largest total bytes allocated: {d} bytes\n", .{ self.stats.current_total_bytes_allocated_from_page_allocator_fallback, self.stats.largest_total_bytes_allocated_from_page_allocator_fallback }) catch @panic(FAILED_TO_LOG ++ log_name);
                    log_writer.print("     Current number of distinct allocations: {d}\n     Largest number of distinct allocations: {d}\n", .{ self.stats.current_number_of_page_allocator_fallback_allocations, self.stats.largest_number_of_page_allocator_fallback_allocations }) catch @panic(FAILED_TO_LOG ++ log_name);
                    log_writer.print("     Largest attempted resize delta (grow): {d} bytes\n     Largest attempted resize delta (shrink): {d} bytes\n", .{ self.stats.largest_attempted_page_allocator_fallback_resize_delta_grow, self.stats.largest_attempted_page_allocator_fallback_resize_delta_shrink }) catch @panic(FAILED_TO_LOG ++ log_name);
                }
            }
            std.log.info("{s}", .{log_buffer.items[0..log_buffer.items.len]});
        }
    };
}

const FAILED_TO_LOG = "failed to allocate memory for logging: ";

inline fn get_bytes_log2(len: usize, alignment: mem.Alignment) Log2Usize {
    const size_log2 = @max(@bitSizeOf(usize) - @clz(len - 1), @intFromEnum(alignment));
    return @intCast(size_log2);
}

inline fn bytes_log2_to_alignment(val: Log2Usize) mem.Alignment {
    return @enumFromInt(val);
}

test "Does it basically work?" {
    const t = std.testing;
    t.log_level = .err;
    const ListU8 = std.ArrayList(u8);
    var log_buffer = ListU8.init(std.heap.page_allocator);
    const ALLOC_OPTIONS = QuickAllocOptions{
        .buckets = &[_]BucketDef{
            BucketDef{
                .block_size = ._128_B,
                .slab_size = ._4_KB,
            },
            BucketDef{
                .block_size = ._1_KB,
                .slab_size = ._16_KB,
            },
        },
        .track_allocation_statistics = true,
        .hint_log_usage_statistics = .VERY_LIKELY,
        .large_allocation_behavior = .USE_PAGE_ALLOCATOR,
        .hint_large_allocation = .VERY_LIKELY,
    };
    const ALLOC = define_allocator(ALLOC_OPTIONS);
    var quick_alloc = ALLOC{};
    const allocator = quick_alloc.allocator();
    var text_list = ListU8.init(allocator);
    try text_list.append('H');
    try text_list.append('e');
    try text_list.append('l');
    try text_list.append('l');
    try text_list.append('o');
    try text_list.append(' ');
    quick_alloc.log_usage_statistics(&log_buffer, "After append 'Hello ' = 6 total bytes of ListU8");
    try text_list.appendSlice("World!");
    quick_alloc.log_usage_statistics(&log_buffer, "After append 'World!' = 12 total bytes of ListU8");
    try t.expectEqualStrings("Hello World!", text_list.items[0..text_list.items.len]);
    try text_list.ensureTotalCapacity(129);
    try t.expectEqualStrings("Hello World!", text_list.items[0..text_list.items.len]);
    quick_alloc.log_usage_statistics(&log_buffer, "After ensureTotalCapacity(129)");
    text_list.clearAndFree();
    quick_alloc.log_usage_statistics(&log_buffer, "After first clear and free");
    try text_list.ensureTotalCapacity(1025);
    quick_alloc.log_usage_statistics(&log_buffer, "After ensureTotalCapacity(1025)");
    text_list.clearAndFree();
    quick_alloc.log_usage_statistics(&log_buffer, "After second clear and free");
}
