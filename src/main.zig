const std = @import("std");

// 定义 InputBuffer 结构体
const InputBuffer = struct {
    buffer: ?[]u8,
    buffer_length: usize,
    input_length: isize,

    // 创建新的 InputBuffer 的函数
    pub fn new(allocator: std.mem.Allocator) !*InputBuffer {
        // 分配内存
        const input_buffer = try allocator.create(InputBuffer);

        // 初始化字段
        input_buffer.* = InputBuffer{
            .buffer = null,
            .buffer_length = 0,
            .input_length = 0,
        };

        return input_buffer;
    }

    // 释放资源的函数
    pub fn deinit(self: *InputBuffer, allocator: std.mem.Allocator) void {
        if (self.buffer) |buf| {
            allocator.free(buf);
        }
        allocator.destroy(self);
    }
};

// 打印提示符
fn printPrompt() void {
    std.debug.print("db > ", .{});
}

// 读取输入
fn readInput(input_buffer: *InputBuffer, allocator: std.mem.Allocator) !void {
    var stdin = std.io.getStdIn().reader();

    // 如果buffer已存在，先释放
    if (input_buffer.buffer) |buf| {
        allocator.free(buf);
    }

    // 分配一个初始缓冲区
    var buffer = try allocator.alloc(u8, 100);
    errdefer allocator.free(buffer);

    const line = try stdin.readUntilDelimiterOrEof(buffer, '\n') orelse {
        std.debug.print("Error reading input\n", .{});
        return error.InputError;
    };

    // 调整缓冲区大小为实际读取的大小
    if (line.len < buffer.len) {
        // 在新版Zig中，resize返回布尔值，不是可选类型
        const resized = allocator.resize(buffer, line.len);
        // 如果调整成功
        if (resized) {
            buffer.len = line.len;
        } else {
            // 如果调整失败，创建一个新的正确大小的缓冲区
            const new_buffer = try allocator.alloc(u8, line.len);
            @memcpy(new_buffer, line);
            allocator.free(buffer);
            buffer = new_buffer;
        }
    }

    input_buffer.buffer = buffer;
    input_buffer.buffer_length = buffer.len;
    input_buffer.input_length = @intCast(buffer.len);
}

// 关闭输入缓冲区
fn closeInputBuffer(input_buffer: *InputBuffer, allocator: std.mem.Allocator) void {
    input_buffer.deinit(allocator);
}

pub fn main() !void {
    // 创建内存分配器
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // 创建输入缓冲区
    const input_buffer = try InputBuffer.new(allocator);
    defer closeInputBuffer(input_buffer, allocator);

    while (true) {
        printPrompt();
        try readInput(input_buffer, allocator);

        // 获取buffer的内容
        const buffer_content = input_buffer.buffer.?;

        // 检查是否是退出命令
        if (std.mem.eql(u8, buffer_content, ".exit")) {
            break;
        } else {
            std.debug.print("Unrecognized command '{s}'.\n", .{buffer_content});
        }
    }
}
