const std = @import("std");

const MetaCommandResult = enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
};

const PrepareResult = enum {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
};

const StatementType = enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
};

const Statement = struct {
    type: StatementType,
};

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

fn doMetaCommand(inputBuffer: *InputBuffer, allocator: std.mem.Allocator) MetaCommandResult {
    const buffer_content = inputBuffer.buffer.?;
    if (std.mem.eql(u8, buffer_content, ".exit")) {
        closeInputBuffer(inputBuffer, allocator);
        std.process.exit(0);
    } else {
        return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

fn prepareStatement(inputBuffer: *InputBuffer, statement: *Statement) PrepareResult {
    const buffer_content = inputBuffer.buffer.?;
    if (std.mem.eql(u8, buffer_content[0..6], "insert")) {
        statement.*.type = .STATEMENT_INSERT;
        return .PREPARE_SUCCESS;
    }

    if (std.mem.eql(u8, buffer_content[0..6], "select")) {
        statement.*.type = .STATEMENT_SELECT;
        return .PREPARE_SUCCESS;
    }

    return .PREPARE_UNRECOGNIZED_STATEMENT;
}

fn executeStatement(statement: *Statement) !void {
    switch (statement.type) {
        .Insert => {
            std.debug.print("Insert statement\n", .{});
        },
        .Select => {
            std.debug.print("Select statement\n", .{});
        },
        .UnrecognizedStatement => {
            std.debug.print("Unrecognized statement\n", .{});
        },
    }
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
        if (std.mem.eql(u8, buffer_content[0..1], ".")) {
            switch (doMetaCommand(input_buffer, allocator)) {
                .META_COMMAND_SUCCESS => {
                    std.debug.print("Success\n", .{});
                    continue;
                },
                .META_COMMAND_UNRECOGNIZED_COMMAND => {
                    std.debug.print("Unrecognized command\n", .{});
                    continue;
                },
            }
        }

        var state = Statement{ .type = .STATEMENT_INSERT };
        switch (prepareStatement(input_buffer, &state)) {
            .PREPARE_SUCCESS => {
                std.debug.print("Success\n", .{});
                continue;
            },
            .PREPARE_UNRECOGNIZED_STATEMENT => {
                std.debug.print("Unrecognized statement\n", .{});
                continue;
            },
        }
    }
}
