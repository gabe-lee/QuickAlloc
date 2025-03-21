const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const math = std.math;
const Allocator = std.mem.Allocator;
const PageAllocator = std.heap.PageAllocator;

pub const QuickAllocOptions = struct {
    /// The size of the memory region requested from the backing allocator when a specific block size does not have
    /// any more slots left. This size will then be divided into slots of the needed block size and appended to the block size
    /// free list.
    slab_size: usize = 64 * 1024,
    /// The smallest block size an allocation can use, any allocation equal to or smaller than this will use this size
    /// memory block.
    smallest_block_size: usize = 1024,
    /// The largest block size supported by this allocator. Any requested allocation larger than this
    /// will be directly mapped/unmapped by the backing PageAllocator.
    largest_block_size: usize = 64 * 1024,
    /// Controls whther allocations larger than the `largest_block_size` are allowed.
    ///
    /// If true, those alloactions will be handled directly by the backing PageAllocator
    allow_allocations_larger_than_max: bool = true,
    /// If `allow_allocations_larger_than_max` is false, how should attempted large allocations respond
    disallowed_larger_than_max_behavior: ErrorBehavior = .PANIC,
};

pub const ErrorBehavior = enum {
    IGNORE,
    LOG,
    PANIC,
    UNREACHABLE,
};

pub fn define_allocator(comptime options: QuickAllocOptions) type {
    if (options.largest_block_size > options.slab_size) @compileError("`largest_block_size` cannot be larger than `slab_size`");
    if (options.smallest_block_size > options.largest_block_size) @compileError("`smallest_block_size` cannot be larger than `largest_block_size`");
    if (options.smallest_block_size == 0) @compileError("`smallest_block_size` cannot equal 0");
    if (options.largest_block_size == 0) @compileError("`largest_block_size` cannot equal 0");
    if (options.slab_size == 0) @compileError("`slab_size` cannot equal 0");
    if (options.slab_size >> @ctz(options.slab_size) != 1) @compileError("`slab_size` must be a power of 2");
    if (options.smallest_block_size >> @ctz(options.smallest_block_size) != 1) @compileError("`smallest_block_size` must be a power of 2");
    if (options.largest_block_size >> @ctz(options.largest_block_size) != 1) @compileError("`largest_block_size` must be a power of 2");
    if (options.smallest_block_size < @sizeOf(usize)) @compileError("`smallest_block_size` must be greater than or equal to `@sizeOf(usize)`");
    return struct {
        first_free_block_by_size: [SIZE_COUNT]usize = @splat(0),
        free_block_count_by_size: [SIZE_COUNT]usize = @splat(0),

        const QuickAlloc = @This();
        const LOG2_SMALLEST_SIZE = math.log2_int(usize, options.smallest_block_size);
        const LOG2_LARGEST_SIZE = math.log2_int(usize, options.largest_block_size);
        const SIZE_COUNT = LOG2_LARGEST_SIZE - LOG2_SMALLEST_SIZE;
        const SMALLEST_SIZE = options.smallest_block_size;
        const LARGEST_SIZE = options.largest_block_size;
        const SLAB_SIZE = options.slab_size;
        const ALLOW_LARGE: bool = options.allow_allocations_larger_than_max;
        const DISALLOW_LARGE_BEHAVIOR: ErrorBehavior = options.disallowed_larger_than_max_behavior;
        const BLOCK_SIZE: [SIZE_COUNT]usize = calc: {
            const array: [SIZE_COUNT]usize = @splat(0);
            var i: math.Log2Int(usize) = 0;
            while (i < SIZE_COUNT) : (i += 1) {
                array[i].* = options.smallest_block_size << i;
            }
            break :calc array;
        };
        const BLOCKS_PER_SLAB: [SIZE_COUNT]usize = calc: {
            const array: [SIZE_COUNT]usize = @splat(0);
            var i = 0;
            while (i < SIZE_COUNT) : (i += 1) {
                array[i].* = BLOCK_SIZE[i] / SLAB_SIZE;
            }
            break :calc array;
        };
        const EXTRA_BLOCKS_PER_SLAB: [SIZE_COUNT]usize = calc: {
            const array: [SIZE_COUNT]usize = @splat(0);
            var i = 0;
            while (i < SIZE_COUNT) : (i += 1) {
                array[i].* = BLOCKS_PER_SLAB[i] - 1;
            }
            break :calc array;
        };

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
            const size_index = get_size_index(len, alignment);
            if (size_index >= SIZE_COUNT) {
                @branchHint(.unlikely);
                if (!ALLOW_LARGE) return handle_error_return_null(DISALLOW_LARGE_BEHAVIOR, ERROR_LARGE_ALLOCATIONS_NOT_ALLOWED, .{LARGEST_SIZE});
                return PageAllocator.map(len, alignment);
            }

            if (self.free_block_count_by_size[size_index] != 0) {
                @branchHint(.likely);
                const first_free_address = self.remove_address_from_free_list(size_index);
                return @ptrFromInt(first_free_address);
            }

            return self.alloc_new_size_slab(size_index);
        }

        fn alloc_new_size_slab(self: *QuickAlloc, size_index: usize) ?[*]u8 {
            const new_slab_attempt = PageAllocator.map(SLAB_SIZE, mem.Alignment.fromByteUnits(SLAB_SIZE));
            if (new_slab_attempt == null) {
                @branchHint(.unlikely);
                return null;
            }
            const new_slab = new_slab_attempt orelse unreachable;
            var extra_free_blocks = EXTRA_BLOCKS_PER_SLAB[size_index];
            const block_size = BLOCK_SIZE[size_index];
            assert(SLAB_SIZE % block_size == 0);
            if (extra_free_blocks > 0) {
                @branchHint(.likely);
                self.free_block_count_by_size[size_index] = extra_free_blocks;
                const first_free_addr = @intFromPtr(new_slab) + block_size;
                self.first_free_block_by_size[size_index].* = first_free_addr;
                var this_free_addr = first_free_addr;
                var next_free_addr = this_free_addr + block_size;
                while (extra_free_blocks > 1) : (extra_free_blocks -= 1) {
                    @branchHint(.likely);
                    const this_free_ptr: *usize = @ptrFromInt(this_free_addr);
                    this_free_ptr.* = next_free_addr;
                    this_free_addr = next_free_addr;
                    next_free_addr += block_size;
                }
                assert(next_free_addr == @intFromPtr(new_slab) + SLAB_SIZE);
            }
            return new_slab;
        }

        fn resize(self_opaque: *anyopaque, memory: []u8, alignment: mem.Alignment, new_len: usize, ret_addr: usize) bool {
            _ = ret_addr;
            _ = self_opaque;
            const old_size_index = get_size_index(memory.len, alignment);
            const new_size_index = get_size_index(new_len, alignment);
            if (old_size_index >= SIZE_COUNT) {
                @branchHint(.unlikely);
                if (new_size_index < SIZE_COUNT) return false;
                if (!ALLOW_LARGE) return handle_error_return_false(DISALLOW_LARGE_BEHAVIOR, ERROR_LARGE_ALLOCATIONS_NOT_ALLOWED, .{LARGEST_SIZE});
                return PageAllocator.realloc(memory, new_len, false) != null;
            }
            return new_size_index == old_size_index;
        }

        fn remap(self_opaque: *anyopaque, memory: []u8, alignment: mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
            _ = ret_addr;
            const self: *QuickAlloc = @ptrCast(@alignCast(self_opaque));
            const old_size_index = get_size_index(memory.len, alignment);
            const new_size_index = get_size_index(new_len, alignment);
            if (old_size_index >= SIZE_COUNT) {
                @branchHint(.unlikely);
                if (new_size_index < SIZE_COUNT) return null;
                if (!ALLOW_LARGE) return handle_error_return_null(DISALLOW_LARGE_BEHAVIOR, ERROR_LARGE_ALLOCATIONS_NOT_ALLOWED, .{LARGEST_SIZE});
                return PageAllocator.realloc(memory, new_len, true);
            }
            if (self.free_block_count_by_size[new_size_index] != 0) {
                @branchHint(.likely);
                const new_free_address = self.remove_address_from_free_list(new_size_index);
                self.add_address_to_free_list(old_size_index, @intFromPtr(memory.ptr));
                return @ptrFromInt(new_free_address);
            }

            const alloc_attempt = self.alloc_new_size_slab(new_size_index);
            if (alloc_attempt == null) {
                @branchHint(.unlikely);
                return null;
            }
            const new_alloc = alloc_attempt orelse unreachable;

            return new_alloc;
        }

        fn free(self_opaque: *anyopaque, memory: []u8, alignment: mem.Alignment, ret_addr: usize) void {
            _ = ret_addr;
            const self: *QuickAlloc = @ptrCast(@alignCast(self_opaque));
            const size_index = get_size_index(memory.len, alignment);
            if (size_index >= SIZE_COUNT) {
                @branchHint(.unlikely);
                if (!ALLOW_LARGE) return handle_error_return_void(DISALLOW_LARGE_BEHAVIOR, ERROR_LARGE_ALLOCATIONS_NOT_ALLOWED, .{LARGEST_SIZE});
                return PageAllocator.unmap(@alignCast(memory));
            }
            self.add_address_to_free_list(size_index, @intFromPtr(memory.ptr));
        }

        fn add_address_to_free_list(self: *QuickAlloc, size_index: usize, address: usize) void {
            const prev_first_free_old_size_address = self.first_free_block_by_size[size_index];
            const curr_first_free_new_size_ptr: *usize = @ptrFromInt(address);
            curr_first_free_new_size_ptr.* = prev_first_free_old_size_address;
            self.first_free_block_by_size[size_index].* = address;
            self.free_block_count_by_size[size_index].* += 1;
        }

        fn remove_address_from_free_list(self: *QuickAlloc, size_index: usize) usize {
            const first_free_address = self.first_free_block_by_size[size_index];
            const next_free_address: *usize = @ptrFromInt(first_free_address);
            self.first_free_block_by_size[size_index].* = next_free_address.*;
            self.free_block_count_by_size[size_index].* -= 1;
            return first_free_address;
        }

        inline fn get_size_index(len: usize, alignment: mem.Alignment) usize {
            return @max(@bitSizeOf(usize) - @clz(len - 1), @intFromEnum(alignment), SMALLEST_SIZE) - SMALLEST_SIZE;
        }

        pub fn log_free_slots(self: *const QuickAlloc) void {
            @branchHint(.cold);
            var i: usize = 0;
            std.log.info("\n[QuickAlloc] Free Block Report\nSIZE GROUP | FREE BLOCKS | FREE SLABS\n----------+-------------+-----------", .{});
            while (i < SIZE_COUNT) : (i += 1) {
                const block_size = BLOCK_SIZE[i];
                const block_count = self.free_block_count_by_size[i];
                const slab_count = block_count / BLOCKS_PER_SLAB[i];
                std.log.info("{d: >10} | {d: >11} | {d: >10}\n", .{ block_size, block_count, slab_count });
            }
        }
    };
}

const ERROR_LARGE_ALLOCATIONS_NOT_ALLOWED = "Allocations larger than {d} are not allowed with this allocator";

inline fn handle_error_return_null(comptime behavior: ErrorBehavior, msg: []const u8, args: anytype) ?[*]u8 {
    switch (behavior) {
        .IGNORE => return null,
        .LOG => {
            std.log.err(msg, args);
            return null;
        },
        .PANIC => std.debug.panic(msg, args),
        .UNREACHABLE => unreachable,
    }
}

inline fn handle_error_return_void(comptime behavior: ErrorBehavior, msg: []const u8, args: anytype) void {
    switch (behavior) {
        .IGNORE => return,
        .LOG => {
            std.log.err(msg, args);
            return;
        },
        .PANIC => std.debug.panic(msg, args),
        .UNREACHABLE => unreachable,
    }
}

inline fn handle_error_return_false(comptime behavior: ErrorBehavior, msg: []const u8, args: anytype) bool {
    switch (behavior) {
        .IGNORE => return false,
        .LOG => {
            std.log.err(msg, args);
            return false;
        },
        .PANIC => std.debug.panic(msg, args),
        .UNREACHABLE => unreachable,
    }
}
