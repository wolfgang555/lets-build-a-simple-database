const std = @import("std");

const COLUMN_USERNAME_SIZE = 32;
const COLUMN_EMAIL_SIZE = 255;

const Row = struct {
    id: u32,
    username: [COLUMN_USERNAME_SIZE]u8,
    email: [COLUMN_EMAIL_SIZE]u8,
};

// Zig uses compile-time calculations instead of macros
const ID_SIZE = @sizeOf(@TypeOf(@field(Row{ .id = 0, .username = undefined, .email = undefined }, "id")));
const USERNAME_SIZE = @sizeOf(@TypeOf(@field(Row{ .id = 0, .username = undefined, .email = undefined }, "username")));
const EMAIL_SIZE = @sizeOf(@TypeOf(@field(Row{ .id = 0, .username = undefined, .email = undefined }, "email")));
const ID_OFFSET = 0;
const USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE = 4096;
const TABLE_MAX_PAGES = 100;
const ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

const ExecuteResult = enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
};

const Table = struct {
    num_rows: u32,
    pages: [TABLE_MAX_PAGES]?[]u8, // Use slice instead of *anyopaque for easier memory management

    // Constructor
    pub fn init() Table {
        var table = Table{
            .num_rows = 0,
            .pages = undefined,
        };

        for (&table.pages) |*page| {
            page.* = null;
        }

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

    // Copy username
    @memcpy(dest[USERNAME_OFFSET .. USERNAME_OFFSET + USERNAME_SIZE], std.mem.asBytes(&source.username));

    // Copy email
    @memcpy(dest[EMAIL_OFFSET .. EMAIL_OFFSET + EMAIL_SIZE], std.mem.asBytes(&source.email));
}

fn deserializeRow(source: [*]u8, destination: *Row) void {
    const src = @as([*]u8, source);

    // Copy id
    @memcpy(std.mem.asBytes(&destination.id), src[ID_OFFSET .. ID_OFFSET + ID_SIZE]);

    // Copy username
    @memcpy(std.mem.asBytes(&destination.username), src[USERNAME_OFFSET .. USERNAME_OFFSET + USERNAME_SIZE]);

    // Copy email
    @memcpy(std.mem.asBytes(&destination.email), src[EMAIL_OFFSET .. EMAIL_OFFSET + EMAIL_SIZE]);
}

fn rowSlot(table: *Table, row_num: u32) [*]u8 {
    const page_num = row_num / ROWS_PER_PAGE;

    // Allocate page if needed
    if (table.pages[page_num] == null) {
        var gpa = std.heap.GeneralPurposeAllocator(.{}){};
        const page = gpa.allocator().alloc(u8, PAGE_SIZE) catch @panic("Memory allocation failed");
        table.pages[page_num] = page;
    }

    // Get the page
    const page = table.pages[page_num].?.ptr;

    // Calculate row position
    const row_offset = row_num % ROWS_PER_PAGE;
    const byte_offset = row_offset * ROW_SIZE;

    // Return pointer to row slot
    return page + byte_offset;
}

// Create new table
fn newTable(allocator: std.mem.Allocator) !*Table {
    const table = try allocator.create(Table);
    table.* = Table.init();
    return table;
}

// Free table memory
fn freeTable(table: *Table, allocator: std.mem.Allocator) void {
    // Free all allocated pages
    for (table.pages) |page_opt| {
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

    const line = try stdin.readUntilDelimiterOrEof(buffer, '\n') orelse {
        std.debug.print("Error reading input\n", .{});
        return error.InputError;
    };

    // Resize buffer to actual read size
    if (line.len < buffer.len) {
        const resized = allocator.resize(buffer, line.len);
        if (resized) {
            buffer = buffer[0..line.len];
        } else {
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

// Close input buffer
fn closeInputBuffer(input_buffer: *InputBuffer, allocator: std.mem.Allocator) void {
    input_buffer.deinit(allocator);
}

fn doMetaCommand(inputBuffer: *InputBuffer, table: *Table, allocator: std.mem.Allocator) MetaCommandResult {
    const buffer_content = inputBuffer.buffer.?;
    if (std.mem.eql(u8, buffer_content, ".exit")) {
        closeInputBuffer(inputBuffer, allocator);
        freeTable(table, allocator);
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

fn executeInsert(statement: *Statement, table: *Table) ExecuteResult {
    if (table.num_rows >= TABLE_MAX_ROWS) {
        return ExecuteResult.EXECUTE_TABLE_FULL;
    }

    const row_to_insert = &statement.row_to_insert;
    serializeRow(row_to_insert, rowSlot(table, table.num_rows));

    table.num_rows += 1;

    return ExecuteResult.EXECUTE_SUCCESS;
}

fn executeSelect(_: *Statement, table: *Table) ExecuteResult {
    var i: u32 = 0;
    var row = Row{
        .id = 0,
        .username = undefined,
        .email = undefined,
    };

    while (i < table.num_rows) : (i += 1) {
        deserializeRow(rowSlot(table, i), &row);
        printRow(&row);
    }

    return ExecuteResult.EXECUTE_SUCCESS;
}

fn executeStatement(statement: *Statement, table: *Table) ExecuteResult {
    const stdout = std.io.getStdOut().writer();

    switch (statement.type) {
        .STATEMENT_INSERT => {
            const result = executeInsert(statement, table);
            if (result == .EXECUTE_SUCCESS) {
                stdout.print("Executed.\n", .{}) catch {};
            } else if (result == .EXECUTE_TABLE_FULL) {
                stdout.print("Error: Table full.\n", .{}) catch {};
            }
            return result;
        },
        .STATEMENT_SELECT => {
            const result = executeSelect(statement, table);
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

    const table = try newTable(allocator);
    defer freeTable(table, allocator);

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
            switch (executeStatement(&statement, table)) {
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
