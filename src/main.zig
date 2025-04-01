const std = @import("std");
const os = std.os;
const fs = std.fs;
const Allocator = std.mem.Allocator;

const COLUMN_USERNAME_SIZE = 32;
const COLUMN_EMAIL_SIZE = 255;
const MAGIC_VALID_ROW = 0xDEADBEEF; // Magic number for valid rows

const Row = struct {
    id: u32,
    magic: u32, // Magic number to identify valid rows
    username: [COLUMN_USERNAME_SIZE]u8,
    email: [COLUMN_EMAIL_SIZE]u8,
};

const Cursor = struct {
    table: *Table,
    row_num: u32,
    end_of_table: bool,
};

// Zig uses compile-time calculations instead of macros
const ID_SIZE = @sizeOf(@TypeOf(@field(Row{ .id = 0, .magic = 0, .username = undefined, .email = undefined }, "id")));
const MAGIC_SIZE = @sizeOf(@TypeOf(@field(Row{ .id = 0, .magic = 0, .username = undefined, .email = undefined }, "magic")));
const USERNAME_SIZE = @sizeOf(@TypeOf(@field(Row{ .id = 0, .magic = 0, .username = undefined, .email = undefined }, "username")));
const EMAIL_SIZE = @sizeOf(@TypeOf(@field(Row{ .id = 0, .magic = 0, .username = undefined, .email = undefined }, "email")));
const ID_OFFSET = 0;
const MAGIC_OFFSET = ID_OFFSET + ID_SIZE;
const USERNAME_OFFSET = MAGIC_OFFSET + MAGIC_SIZE;
const EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE = ID_SIZE + MAGIC_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE = 4096;
const TABLE_MAX_PAGES = 100;
const ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

const ExecuteResult = enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
};

const Pager = struct {
    file: fs.File,
    file_length: u64,
    pages: [TABLE_MAX_PAGES]?[]u8,

    pub fn init(file: fs.File) Pager {
        var pager = Pager{
            .file = file,
            .file_length = 0,
            .pages = [_]?[]u8{null} ** TABLE_MAX_PAGES,
        };

        for (&pager.pages) |*page| {
            page.* = null;
        }

        return pager;
    }
};

const Table = struct {
    num_rows: u64,
    pager: *Pager,

    // Constructor
    pub fn init(pager: *Pager, num_rows: u64) Table {
        const table = Table{
            .num_rows = num_rows,
            .pager = pager,
        };

        return table;
    }
};

const MetaCommandResult = enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND,
};

const PrepareResult = enum {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_STRING_TOO_LONG,
};

const StatementType = enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT,
};

const Statement = struct {
    type: StatementType,
    row_to_insert: Row,
};

// InputBuffer structure
const InputBuffer = struct {
    buffer: ?[]u8,
    buffer_length: usize,
    input_length: isize,

    // Create new InputBuffer
    pub fn new(allocator: std.mem.Allocator) !*InputBuffer {
        const input_buffer = try allocator.create(InputBuffer);

        input_buffer.* = InputBuffer{
            .buffer = null,
            .buffer_length = 0,
            .input_length = 0,
        };

        return input_buffer;
    }

    // Free resources
    pub fn deinit(self: *InputBuffer, allocator: std.mem.Allocator) void {
        if (self.buffer) |buf| {
            allocator.free(buf);
        }
        allocator.destroy(self);
    }
};

fn tableStart(table: *Table, allocator: std.mem.Allocator) !*Cursor {
    const cursor = try allocator.create(Cursor);
    cursor.* = Cursor{
        .table = table,
        .row_num = 0,
        .end_of_table = table.num_rows == 0,
    };
    return cursor;
}

fn tableEnd(table: *Table, allocator: std.mem.Allocator) !*Cursor {
    const cursor = try allocator.create(Cursor);
    cursor.* = Cursor{
        .table = table,
        .row_num = @intCast(table.num_rows),
        .end_of_table = true,
    };
    return cursor;
}

fn printRow(row: *const Row) void {
    const stdout = std.io.getStdOut().writer();

    // Find the actual length of the username (stop at first null byte)
    var username_len: usize = 0;
    while (username_len < row.username.len and row.username[username_len] != 0) {
        username_len += 1;
    }

    // Find the actual length of the email (stop at first null byte)
    var email_len: usize = 0;
    while (email_len < row.email.len and row.email[email_len] != 0) {
        email_len += 1;
    }

    // Print only the actual content, not the null padding
    // Add a newline at the end
    stdout.print("({d}, {s}, {s})\n", .{ row.id, row.username[0..username_len], row.email[0..email_len] }) catch {};
}

fn serializeRow(source: *const Row, destination: [*]u8) void {
    const dest = @as([*]u8, destination);

    // Copy id
    @memcpy(dest[ID_OFFSET .. ID_OFFSET + ID_SIZE], std.mem.asBytes(&source.id));

    // Copy magic
    @memcpy(dest[MAGIC_OFFSET .. MAGIC_OFFSET + MAGIC_SIZE], std.mem.asBytes(&source.magic));

    // Copy username
    @memcpy(dest[USERNAME_OFFSET .. USERNAME_OFFSET + USERNAME_SIZE], std.mem.asBytes(&source.username));

    // Copy email
    @memcpy(dest[EMAIL_OFFSET .. EMAIL_OFFSET + EMAIL_SIZE], std.mem.asBytes(&source.email));
}

fn deserializeRow(source: [*]u8, destination: *Row) void {
    const src = @as([*]u8, source);

    // Copy id
    @memcpy(std.mem.asBytes(&destination.id), src[ID_OFFSET .. ID_OFFSET + ID_SIZE]);

    // Copy magic
    @memcpy(std.mem.asBytes(&destination.magic), src[MAGIC_OFFSET .. MAGIC_OFFSET + MAGIC_SIZE]);

    // Copy username
    @memcpy(std.mem.asBytes(&destination.username), src[USERNAME_OFFSET .. USERNAME_OFFSET + USERNAME_SIZE]);

    // Copy email
    @memcpy(std.mem.asBytes(&destination.email), src[EMAIL_OFFSET .. EMAIL_OFFSET + EMAIL_SIZE]);
}

fn rowSlot(cursor: *Cursor, allocator: std.mem.Allocator) [*]u8 {
    const page_num = cursor.row_num / ROWS_PER_PAGE;
    const page = getPage(cursor.table.pager, @as(u32, page_num), allocator) catch unreachable;

    // Calculate row position
    const row_offset = cursor.row_num % ROWS_PER_PAGE;
    const byte_offset = row_offset * ROW_SIZE;

    // Return pointer to row slot
    return @as([*]u8, @ptrCast(page.ptr)) + byte_offset;
}

fn cursorValue(cursor: *Cursor, allocator: std.mem.Allocator) [*]u8 {
    const row_num = cursor.row_num;
    const page_num = row_num / ROWS_PER_PAGE;
    const page = getPage(cursor.table.pager, @as(u32, page_num), allocator) catch unreachable;
    const row_offset = row_num % ROWS_PER_PAGE;
    const byte_offset = row_offset * ROW_SIZE;

    return @as([*]u8, @ptrCast(page.ptr)) + byte_offset;
}

fn cursorAdvance(cursor: *Cursor) void {
    cursor.row_num += 1;
    if (cursor.row_num >= cursor.table.num_rows) {
        cursor.end_of_table = true;
    }
}

fn cursorReset(cursor: *Cursor) void {
    cursor.row_num = 0;
    cursor.end_of_table = false;
}

fn pagerOpen(filename: []const u8, allocator: std.mem.Allocator) !*Pager {
    const file = try fs.cwd().createFile(
        filename,
        .{ .read = true, .truncate = false, .exclusive = false },
    );
    // 确保函数返回时关闭文件
    errdefer file.close();

    // 获取文件长度
    const file_length = try file.getEndPos();

    // 分配 Pager 结构体内存
    const pager = try allocator.create(Pager);
    errdefer allocator.destroy(pager);

    // 初始化 Pager
    pager.* = Pager{
        .file = file,
        .file_length = @as(u64, file_length),
        .pages = [_]?[]u8{null} ** TABLE_MAX_PAGES,
    };

    return pager;
}

fn getPage(pager: *Pager, page_num: u64, allocator: std.mem.Allocator) ![]u8 {
    if (page_num > TABLE_MAX_PAGES) {
        std.debug.print("Tried to fetch page number out of bounds. {d} > {d}\n", .{ page_num, TABLE_MAX_PAGES });
        return error.PageOutOfRange;
    }

    if (pager.pages[page_num] == null) {
        const page = try allocator.alloc(u8, PAGE_SIZE);
        errdefer allocator.free(page);

        var num_pages = pager.file_length / PAGE_SIZE;

        if (pager.file_length % PAGE_SIZE != 0) {
            num_pages += 1;
        }

        if (page_num < num_pages) {
            // 将文件指针设置到正确的位置
            try pager.file.seekTo(page_num * PAGE_SIZE);

            // 读取数据
            const bytes_read = try pager.file.read(page);

            // 如果没有读取完整页面，剩余部分保持为零
            if (bytes_read < PAGE_SIZE) {
                // 在 Zig 中，新分配的内存已初始化为 0，所以不需要额外操作
            }
        }

        pager.pages[page_num] = page;
    }
    return pager.pages[page_num].?;
}

pub fn dbClose(table: *Table, allocator: std.mem.Allocator) !void {
    var pager = table.pager;
    const num_full_pages = table.num_rows / ROWS_PER_PAGE;

    // 将所有完整的页面写入磁盘并释放内存
    for (0..num_full_pages) |i| {
        const page_num: u32 = @intCast(i);
        // 如果页面未加载到内存，跳过
        if (pager.pages[page_num] == null) {
            continue;
        }
        // 刷新页面到磁盘
        try pagerFlush(pager, page_num, PAGE_SIZE);
        // 释放页面内存
        allocator.free(pager.pages[page_num].?);
        pager.pages[page_num] = null;
    }

    // 可能需要写入文件末尾的部分页面
    // 当切换到 B 树后，这应该不再需要
    const num_additional_rows = table.num_rows % ROWS_PER_PAGE;
    if (num_additional_rows > 0) {
        const page_num: u32 = @intCast(num_full_pages);
        if (pager.pages[page_num] != null) {
            try pagerFlush(pager, page_num, num_additional_rows * ROW_SIZE);
            allocator.free(pager.pages[page_num].?);
            pager.pages[page_num] = null;
        }
    }

    // 关闭文件
    pager.file.close();

    // 释放所有剩余页面的内存
    for (0..TABLE_MAX_PAGES) |i| {
        if (pager.pages[i]) |page| {
            allocator.free(page);
            pager.pages[i] = null;
        }
    }

    // 释放分页器和表的内存
    allocator.destroy(pager);
    allocator.destroy(table);
}

/// 将指定页面刷新到磁盘
fn pagerFlush(pager: *Pager, page_num: u32, size: usize) !void {
    if (pager.pages[page_num] == null) {
        return error.NullPage;
    }

    // 设置文件位置
    try pager.file.seekTo(page_num * PAGE_SIZE);

    // 写入数据
    const bytes_written = try pager.file.write(pager.pages[page_num].?[0..size]);

    // 验证是否写入了所有数据
    if (bytes_written != size) {
        return error.IncompleteWrite;
    }
}

// Create new table
fn dbOpen(filename: []const u8, allocator: std.mem.Allocator) !*Table {
    const pager = try pagerOpen(filename, allocator);

    // Calculate number of rows - assume only complete rows are stored
    // Only properly inserted rows should be considered (no garbage data)
    var num_rows: u64 = 0;
    if (pager.file_length > 0) {
        // Try to load the first page to check for valid rows
        if (pager.file_length >= ROW_SIZE) {
            // At least one row exists
            // Each row has a fixed size, so we can calculate the number of rows
            num_rows = pager.file_length / ROW_SIZE;

            // Ensure we don't exceed max rows
            if (num_rows > TABLE_MAX_ROWS) {
                num_rows = TABLE_MAX_ROWS;
            }
        }
    }

    const table = try allocator.create(Table);
    table.* = Table.init(pager, num_rows);
    return table;
}

// Free table memory
fn freeTable(table: *Table, allocator: std.mem.Allocator) void {
    // Free all allocated pages
    for (table.pager.pages) |page_opt| {
        if (page_opt) |page| {
            allocator.free(page);
        }
    }

    // Free the table itself
    allocator.destroy(table);
}

// Print prompt
fn printPrompt() void {
    const stdout = std.io.getStdOut().writer();
    stdout.print("db > ", .{}) catch {};
}

// Read input
fn readInput(input_buffer: *InputBuffer, allocator: std.mem.Allocator) !void {
    var stdin = std.io.getStdIn().reader();

    // Free existing buffer if exists
    if (input_buffer.buffer) |buf| {
        allocator.free(buf);
    }

    // Allocate initial buffer with enough space for maximum input
    var buffer = try allocator.alloc(u8, 400);
    errdefer allocator.free(buffer);

    const line = stdin.readUntilDelimiterOrEof(buffer, '\n') catch |err| {
        // Free the buffer on error
        allocator.free(buffer);
        input_buffer.buffer = null;
        input_buffer.buffer_length = 0;
        input_buffer.input_length = 0;

        if (err == error.EndOfStream) {
            // Handle EOF gracefully - exit the program
            const stdout = std.io.getStdOut().writer();
            stdout.print("\n", .{}) catch {};
            std.process.exit(0);
        }

        return err;
    };

    // Handle EOF
    if (line == null) {
        // Free the buffer and exit gracefully on EOF
        allocator.free(buffer);
        input_buffer.buffer = null;
        input_buffer.buffer_length = 0;
        input_buffer.input_length = 0;

        const stdout = std.io.getStdOut().writer();
        stdout.print("\n", .{}) catch {};
        std.process.exit(0);
    }

    // Resize buffer to actual read size
    if (line.?.len < buffer.len) {
        const resized = allocator.resize(buffer, line.?.len);
        if (resized) {
            buffer = buffer[0..line.?.len];
        } else {
            const new_buffer = try allocator.alloc(u8, line.?.len);
            @memcpy(new_buffer, line.?);
            allocator.free(buffer);
            buffer = new_buffer;
        }
    }

    input_buffer.buffer = buffer;
    input_buffer.buffer_length = buffer.len;
    input_buffer.input_length = @intCast(buffer.len);
}

// Close input buffer
fn closeInputBuffer(input_buffer: *InputBuffer, allocator: std.mem.Allocator) void {
    input_buffer.deinit(allocator);
}

fn doMetaCommand(inputBuffer: *InputBuffer, table: *Table, allocator: std.mem.Allocator) MetaCommandResult {
    const buffer_content = inputBuffer.buffer.?;
    if (std.mem.eql(u8, buffer_content, ".exit")) {
        closeInputBuffer(inputBuffer, allocator);

        // Make sure to close the database properly - this flushes data to disk
        dbClose(table, allocator) catch |err| {
            std.debug.print("Error closing database: {s}\n", .{@errorName(err)});
        };

        std.process.exit(0);
    } else {
        return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

fn prepareStatement(inputBuffer: *InputBuffer, statement: *Statement) PrepareResult {
    if (inputBuffer.buffer) |buffer_content| {
        if (buffer_content.len >= 6 and std.mem.eql(u8, buffer_content[0..6], "insert")) {
            statement.*.type = .STATEMENT_INSERT;

            var iter = std.mem.tokenizeAny(u8, buffer_content, " ");
            _ = iter.next(); // skip insert

            const id_str = iter.next() orelse return PrepareResult.PREPARE_SYNTAX_ERROR;
            if (id_str.len > 0 and id_str[0] == '-') {
                return PrepareResult.PREPARE_NEGATIVE_ID;
            }
            const id = std.fmt.parseInt(u32, id_str, 10) catch {
                return PrepareResult.PREPARE_SYNTAX_ERROR;
            };

            statement.row_to_insert.id = id;
            const username = iter.next() orelse return .PREPARE_SYNTAX_ERROR;
            if (username.len > statement.row_to_insert.username.len) {
                return .PREPARE_STRING_TOO_LONG;
            }

            @memset(&statement.row_to_insert.username, 0); // Clear the array
            std.mem.copyForwards(u8, &statement.row_to_insert.username, username);

            // Parse email
            const email = iter.next() orelse return .PREPARE_SYNTAX_ERROR;
            if (email.len > statement.row_to_insert.email.len) {
                return .PREPARE_STRING_TOO_LONG;
            }
            @memset(&statement.row_to_insert.email, 0); // Clear the array
            std.mem.copyForwards(u8, &statement.row_to_insert.email, email);

            return .PREPARE_SUCCESS;
        }

        if (buffer_content.len >= 6 and std.mem.eql(u8, buffer_content[0..6], "select")) {
            statement.*.type = .STATEMENT_SELECT;
            return .PREPARE_SUCCESS;
        }
    }

    return .PREPARE_UNRECOGNIZED_STATEMENT;
}

fn executeInsert(statement: *Statement, table: *Table, allocator: std.mem.Allocator) ExecuteResult {
    if (table.num_rows >= TABLE_MAX_ROWS) {
        return ExecuteResult.EXECUTE_TABLE_FULL;
    }

    const row_to_insert = &statement.row_to_insert;
    // Add magic number to mark valid rows
    row_to_insert.magic = MAGIC_VALID_ROW;
    const cursor = tableEnd(table, allocator) catch {
        return ExecuteResult.EXECUTE_TABLE_FULL;
    };
    defer allocator.destroy(cursor);  // 确保在函数结束时释放 cursor 内存

    serializeRow(row_to_insert, cursorValue(cursor, allocator));

    table.num_rows += 1;

    // Flush changes to disk
    // We only need to flush the page that we modified
    const page_num: u32 = @intCast((table.num_rows - 1) / ROWS_PER_PAGE);
    if (table.pager.pages[page_num]) |_| {
        pagerFlush(table.pager, page_num, PAGE_SIZE) catch |err| {
            std.debug.print("Error flushing page: {s}\n", .{@errorName(err)});
        };
    }

    return ExecuteResult.EXECUTE_SUCCESS;
}

fn executeSelect(_: *Statement, table: *Table, allocator: std.mem.Allocator) ExecuteResult {
    const cursor = tableStart(table, allocator) catch {
        return ExecuteResult.EXECUTE_TABLE_FULL;
    };
    defer allocator.destroy(cursor);  // 确保在函数结束时释放 cursor 内存

    var row = Row{
        .id = 0,
        .magic = 0,
        .username = undefined,
        .email = undefined,
    };

    // Get stdout for printing
    const stdout = std.io.getStdOut().writer();

    // Only show rows up to the tracked num_rows value - this is critical
    // to avoid showing garbage data
    while (cursor.row_num < table.num_rows) {
        deserializeRow(cursorValue(cursor, allocator), &row);

        // If we encounter a row without the magic number, skip it
        if (row.magic != MAGIC_VALID_ROW) {
            cursorAdvance(cursor);
            continue;
        }

        // Find the actual length of the username (stop at first null byte)
        var username_len: usize = 0;
        while (username_len < row.username.len and row.username[username_len] != 0) {
            username_len += 1;
        }

        // Find the actual length of the email (stop at first null byte)
        var email_len: usize = 0;
        while (email_len < row.email.len and row.email[email_len] != 0) {
            email_len += 1;
        }
        // Print only the actual content, not the null padding
        stdout.print("({d}, {s}, {s})\n", .{ row.id, row.username[0..username_len], row.email[0..email_len] }) catch {};
        cursorAdvance(cursor);
    }

    return ExecuteResult.EXECUTE_SUCCESS;
}

fn executeStatement(statement: *Statement, table: *Table, allocator: std.mem.Allocator) ExecuteResult {
    const stdout = std.io.getStdOut().writer();

    switch (statement.type) {
        .STATEMENT_INSERT => {
            const result = executeInsert(statement, table, allocator);
            if (result == .EXECUTE_SUCCESS) {
                stdout.print("Executed.\n", .{}) catch {};
            } else if (result == .EXECUTE_TABLE_FULL) {
                stdout.print("Error: Table full.\n", .{}) catch {};
            }
            return result;
        },
        .STATEMENT_SELECT => {
            const result = executeSelect(statement, table, allocator);
            if (result == .EXECUTE_SUCCESS) {
                stdout.print("Executed.\n", .{}) catch {};
            }
            return result;
        },
    }
}

pub fn main() !void {
    // Create memory allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer _ = gpa.deinit();

    // 获取命令行参数
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    // 检查是否提供了足够的参数
    if (args.len < 2) {
        std.log.err("Must provide a database filename.", .{});
        std.process.exit(1);
    }

    // 获取文件名（第一个参数是程序名，第二个参数是我们要的文件名）
    const filename = args[1];

    const table = try dbOpen(filename, allocator);
    defer {
        dbClose(table, allocator) catch |err| {
            std.debug.print("Error closing database: {s}\n", .{@errorName(err)});
        };
    }

    // Create input buffer
    const input_buffer = try InputBuffer.new(allocator);
    defer input_buffer.deinit(allocator);

    while (true) {
        printPrompt();
        try readInput(input_buffer, allocator);

        if (input_buffer.buffer) |buffer_content| {
            // Check if it's a meta command
            if (buffer_content.len > 0 and buffer_content[0] == '.') {
                switch (doMetaCommand(input_buffer, table, allocator)) {
                    .META_COMMAND_SUCCESS => continue,
                    .META_COMMAND_UNRECOGNIZED_COMMAND => {
                        const stdout = std.io.getStdOut().writer();
                        stdout.print("Unrecognized command '{s}'\n", .{buffer_content}) catch {};
                        continue;
                    },
                }
            }

            // Prepare statement
            var statement = Statement{
                .type = .STATEMENT_INSERT,
                .row_to_insert = Row{
                    .id = 0,
                    .magic = 0,
                    .username = undefined,
                    .email = undefined,
                },
            };

            switch (prepareStatement(input_buffer, &statement)) {
                .PREPARE_SUCCESS => {},
                .PREPARE_NEGATIVE_ID => {
                    const stdout = std.io.getStdOut().writer();
                    stdout.print("ID must be positive.\n", .{}) catch {};
                    continue;
                },
                .PREPARE_SYNTAX_ERROR => {
                    const stdout = std.io.getStdOut().writer();
                    stdout.print("Syntax error. Could not parse statement.\n", .{}) catch {};
                    continue;
                },
                .PREPARE_UNRECOGNIZED_STATEMENT => {
                    const stdout = std.io.getStdOut().writer();
                    stdout.print("Unrecognized keyword at start of '{s}'\n", .{buffer_content}) catch {};
                    continue;
                },
                .PREPARE_STRING_TOO_LONG => {
                    const stdout = std.io.getStdOut().writer();
                    stdout.print("String is too long.\n", .{}) catch {};
                    continue;
                },
            }

            // Execute statement
            switch (executeStatement(&statement, table, allocator)) {
                .EXECUTE_SUCCESS => {
                    // Success message is already printed in executeStatement
                },
                .EXECUTE_TABLE_FULL => {
                    // Error message is already printed in executeStatement
                },
            }
        }
    }
}
