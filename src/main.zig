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
    page_num: u32,
    cell_num: u32,
    end_of_table: bool,
};

// Zig uses compile-time calculations instead of macros
const ID_SIZE = @sizeOf(u32);
const MAGIC_SIZE = @sizeOf(u32);
const USERNAME_SIZE = COLUMN_USERNAME_SIZE;
const EMAIL_SIZE = COLUMN_EMAIL_SIZE;
const ID_OFFSET = 0;
const MAGIC_OFFSET = ID_OFFSET + ID_SIZE;
const USERNAME_OFFSET = MAGIC_OFFSET + MAGIC_SIZE;
const EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE = ID_SIZE + MAGIC_SIZE + USERNAME_SIZE + EMAIL_SIZE - 2; // Subtract 2 bytes for alignment

const PAGE_SIZE = 4096;
const TABLE_MAX_PAGES = 100;

const ExecuteResult = enum {
    EXECUTE_SUCCESS,
    EXECUTE_DUPLICATE_KEY,
    EXECUTE_TABLE_FULL,
};

const NodeType = enum {
    NODE_INTERNAL,
    NODE_LEAF,
};

const NODE_TYPE_SIZE = @sizeOf(u8);
const NODE_TYPE_OFFSET = 0;
const IS_ROOT_SIZE = @sizeOf(u8);
const IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE = @sizeOf(u32);
const PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

const NodeHeader = struct {
    const typeSize = NODE_TYPE_SIZE;
    const typeOffset = NODE_TYPE_OFFSET;
    const isRootSize = IS_ROOT_SIZE;
    const isRootOffset = IS_ROOT_OFFSET;
    const parentPointerSize = PARENT_POINTER_SIZE;
    const parentPointerOffset = PARENT_POINTER_OFFSET;
    const size = COMMON_NODE_HEADER_SIZE;
};

const LEAF_NODE_NUM_CELLS_SIZE = @sizeOf(u32);
const LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

const LEAF_NODE_KEY_SIZE = @sizeOf(u32);
const LEAF_NODE_KEY_OFFSET = 0;
const LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

const ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;

const INTERNAL_NODE_NUM_CHILDREN_SIZE = @sizeOf(u32);
const INTERNAL_NODE_CHILD_SIZE = @sizeOf(u32);
const INTERNAL_NODE_KEY_SIZE = @sizeOf(u32);
const INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_CHILDREN_SIZE;
const INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_HEADER_SIZE;
const INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
const INTERNAL_NODE_MAX_CELLS = 3;

fn leafNodeNumCells(node: [*]u8) u32 {
    return std.mem.readInt(u32, node[LEAF_NODE_NUM_CELLS_OFFSET..][0..4], .little);
}

fn setLeafNodeNumCells(node: [*]u8, num_cells: u32) void {
    std.mem.writeInt(u32, node[LEAF_NODE_NUM_CELLS_OFFSET..][0..4], num_cells, .little);
}

fn leafNodeCell(node: [*]u8, cell_num: u32) [*]u8 {
    const offset = LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
    return node + offset;
}

fn leafNodeKey(node: [*]u8, cell_num: u32) u32 {
    // 直接从行数据中读取ID（第一个字段就是ID）
    const row_value = leafNodeValue(node, cell_num);
    const id = std.mem.readInt(u32, row_value[ID_OFFSET..][0..ID_SIZE], .little);

    // 添加调试输出
    std.debug.print("Debug: Reading key at cell {d}: value = {d}\n", .{ cell_num, id });
    return id;
}

fn setLeafNodeKey(node: [*]u8, cell_num: u32, key: u32) void {
    const cell = leafNodeCell(node, cell_num);
    std.mem.writeInt(u32, cell[0..4], key, .little);
}

fn leafNodeValue(node: [*]u8, cell_num: u32) [*]u8 {
    const cell = leafNodeCell(node, cell_num);
    return cell + LEAF_NODE_KEY_SIZE;
}

fn initialize_leaf_node(node: [*]u8) void {
    setNodeType(node, .NODE_LEAF);
    // Set node is_root flag
    node[IS_ROOT_OFFSET] = 1; // Mark as root by default
    setLeafNodeNumCells(node, 0);
}

const Pager = struct {
    file: fs.File,
    file_length: u64,
    pages: [TABLE_MAX_PAGES]?[]u8,
    num_pages: u32,
    allocator: std.mem.Allocator,

    pub fn init(file: fs.File, allocator: std.mem.Allocator) Pager {
        var pager = Pager{
            .file = file,
            .file_length = 0,
            .pages = [_]?[]u8{null} ** TABLE_MAX_PAGES,
            .allocator = allocator,
        };

        for (&pager.pages) |*page| {
            page.* = null;
        }

        return pager;
    }
};

const Table = struct {
    root_page_num: u32,
    pager: *Pager,

    // Constructor
    pub fn init(pager: *Pager) Table {
        const table = Table{
            .pager = pager,
            .root_page_num = 0,
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
        .page_num = table.root_page_num,
        .cell_num = 0,
        .end_of_table = false,
    };

    const root_node = try getPage(table.pager, table.root_page_num, allocator);
    const num_cells = leafNodeNumCells(root_node);
    cursor.end_of_table = num_cells == 0;

    return cursor;
}

fn tableFind(table: *Table, key: u32, allocator: std.mem.Allocator) !*Cursor {
    const root_page_num = table.root_page_num;
    const root_node = try getPage(table.pager, root_page_num, allocator);

    if (getNodeType(root_node) == .NODE_LEAF) {
        return leafNodeFind(table, root_page_num, key, allocator);
    } else {
        std.debug.print("Need to implement searching an internal node\n", .{});
        std.process.exit(1);
    }
}

fn leafNodeFind(table: *Table, page_num: u32, key: u32, allocator: std.mem.Allocator) !*Cursor {
    const node = try getPage(table.pager, page_num, allocator);
    const num_cells = leafNodeNumCells(node);

    const cursor = try allocator.create(Cursor);
    cursor.* = Cursor{
        .table = table,
        .page_num = page_num,
        .cell_num = 0,
        .end_of_table = false,
    };

    // Binary search
    var min_index: u32 = 0;
    var one_past_max_index: u32 = num_cells;

    while (min_index < one_past_max_index) {
        const index = (min_index + one_past_max_index) / 2;
        const key_at_index = leafNodeKey(node, index);

        if (key == key_at_index) {
            cursor.cell_num = index;
            return cursor;
        }

        if (key < key_at_index) {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    cursor.cell_num = min_index;
    return cursor;
}

fn getNodeType(node: [*]u8) NodeType {
    const value = std.mem.readInt(u8, node[NODE_TYPE_OFFSET..][0..1], .little);
    return @as(NodeType, @enumFromInt(value));
}

fn setNodeType(node: [*]u8, node_type: NodeType) void {
    std.mem.writeInt(u8, node[NODE_TYPE_OFFSET..][0..1], @intFromEnum(node_type), .little);
}

fn printRow(row: *const Row) void {
    var buffer: [512]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buffer);
    const stdout = &writer.interface;

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
    stdout.flush() catch {};
}

fn leafNodeInsert(cursor: *Cursor, key: u32, value: *Row) void {
    const node = getPage(cursor.table.pager, cursor.page_num, cursor.table.pager.allocator) catch {
        return;
    };
    const num_cells = leafNodeNumCells(node);
    std.debug.print("Debug: Current number of cells: {}\n", .{num_cells});

    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        std.debug.print("Debug: Leaf node is full, need to split\n", .{});
        leafNodeSplitAndInsert(cursor, key, value);
        return;
    }

    // Find the correct position to insert
    const insert_index = leafNodeFindChild(node, key);
    std.debug.print("Debug: Inserting at index: {}\n", .{insert_index});

    // Make room for new cell by shifting existing cells right
    var i: u32 = num_cells;
    while (i > insert_index) : (i -= 1) {
        @memcpy(leafNodeCell(node, i)[0..LEAF_NODE_CELL_SIZE], leafNodeCell(node, i - 1)[0..LEAF_NODE_CELL_SIZE]);
    }

    // Update number of cells
    setLeafNodeNumCells(node, num_cells + 1);
    std.debug.print("Debug: Updated number of cells to: {}\n", .{num_cells + 1});

    // Write key
    setLeafNodeKey(node, insert_index, key);
    std.debug.print("Debug: Written key: {}\n", .{key});

    // Serialize row data into value part of node
    serializeRow(value, leafNodeValue(node, insert_index));
    std.debug.print("Debug: Serialized row data\n", .{});

    // Verify the data was written correctly
    var read_row: Row = undefined;
    deserializeRow(leafNodeValue(node, insert_index), &read_row);
    std.debug.print("Debug: Verifying inserted data - id: {}, magic: {x}\n", .{ read_row.id, read_row.magic });
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

fn rowSlot(cursor: *Cursor, allocator: std.mem.Allocator) ?[*]u8 {
    const page_num = cursor.page_num;
    const page = getPage(cursor.table.pager, @as(u32, page_num), allocator) catch {
        return null;
    };

    // Calculate cell position
    const cell_offset = cursor.cell_num * LEAF_NODE_CELL_SIZE;
    const byte_offset = LEAF_NODE_HEADER_SIZE + cell_offset;

    // Return pointer to cell value
    return &page[byte_offset + LEAF_NODE_KEY_SIZE];
}

fn cursorValue(cursor: *Cursor, allocator: std.mem.Allocator) ?[*]u8 {
    const page_num = cursor.page_num;
    const page = getPage(cursor.table.pager, @as(u32, page_num), allocator) catch {
        return null;
    };
    return leafNodeValue(page, cursor.cell_num);
}

fn cursorAdvance(cursor: *Cursor) void {
    const page_num = cursor.page_num;
    const node = getPage(cursor.table.pager, page_num, cursor.table.pager.allocator) catch {
        cursor.end_of_table = true;
        return;
    };

    cursor.cell_num += 1;

    // Check if we've reached the end of the cells in this node
    const num_cells = leafNodeNumCells(node);
    if (cursor.cell_num >= num_cells) {
        cursor.end_of_table = true;
        return;
    }
}

fn cursorReset(cursor: *Cursor) void {
    cursor.cell_num = 0;
    cursor.end_of_table = false;
}

fn pagerOpen(filename: []const u8, allocator: std.mem.Allocator) !*Pager {
    // Open the file with read and write permissions, don't truncate, create if doesn't exist
    const file = try fs.cwd().createFile(
        filename,
        .{ .read = true, .truncate = false, .mode = 0o644 },
    );
    // 确保函数返回时关闭文件
    errdefer file.close();

    // 获取文件长度
    const file_length = try file.getEndPos();
    std.debug.print("Opened database file with length: {d}\n", .{file_length});

    // 分配 Pager 结构体内存
    const pager = try allocator.create(Pager);
    errdefer allocator.destroy(pager);

    // 计算页面数量
    const num_pages = if (file_length == 0) 0 else if (file_length / PAGE_SIZE > std.math.maxInt(u32)) {
        std.debug.print("Database file is too large.\n", .{});
        std.process.exit(1);
    } else @as(u32, @intCast(file_length / PAGE_SIZE));

    // 初始化 Pager
    pager.* = Pager{
        .file = file,
        .file_length = @as(u64, file_length),
        .pages = [_]?[]u8{null} ** TABLE_MAX_PAGES,
        .num_pages = num_pages,
        .allocator = allocator,
    };

    if (file_length > 0 and file_length % PAGE_SIZE != 0) {
        std.debug.print("Db file is not a whole number of pages. Corrupt file.\n", .{});
        std.process.exit(1);
    }

    std.debug.print("Initialized pager with {d} pages\n", .{pager.num_pages});
    return pager;
}

fn getPage(pager: *Pager, page_num: u32, allocator: std.mem.Allocator) ![*]u8 {
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

            // 打印调试信息
            std.debug.print("Debug: Read page {d} from disk, bytes_read: {d}\n", .{ page_num, bytes_read });
            std.debug.print("Debug: Raw page data:\n", .{});
            for (0..PAGE_SIZE) |i| {
                if (i % 16 == 0) {
                    std.debug.print("\n{d:0>4}: ", .{i});
                }
                std.debug.print("{x:0>2} ", .{page[i]});
            }
            std.debug.print("\n", .{});
        } else {
            // 如果是新页面，初始化为叶子节点
            initialize_leaf_node(@as([*]u8, @ptrCast(page.ptr)));
        }

        pager.pages[page_num] = page;

        // Add this part to update num_pages when accessing a new page beyond current count
        if (page_num >= pager.num_pages) {
            pager.num_pages = page_num + 1;
        }
    }

    return @as([*]u8, @ptrFromInt(@intFromPtr(pager.pages[page_num].?.ptr)));
}

pub fn dbClose(table: *Table, allocator: std.mem.Allocator) !void {
    var pager = table.pager;

    // 将所有完整的页面写入磁盘并释放内存
    for (0..table.pager.num_pages) |i| {
        const page_num: u32 = @intCast(i);
        // 如果页面未加载到内存，跳过
        if (pager.pages[page_num] == null) {
            continue;
        }
        // 刷新页面到磁盘
        pagerFlush(pager, page_num) catch |err| {
            std.debug.print("Error flushing page {d}: {s}\n", .{ page_num, @errorName(err) });
            return err;
        };
        // 释放页面内存
        allocator.free(pager.pages[page_num].?);
        pager.pages[page_num] = null;
    }

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
fn pagerFlush(pager: *Pager, page_num: u32) !void {
    if (pager.pages[page_num] == null) {
        std.debug.print("Error: Attempted to flush null page {d}\n", .{page_num});
        return error.NullPage;
    }

    // Set file position
    try pager.file.seekTo(page_num * PAGE_SIZE);

    // Write data
    const bytes_written = try pager.file.write(pager.pages[page_num].?[0..PAGE_SIZE]);

    // Verify all data was written
    if (bytes_written != PAGE_SIZE) {
        std.debug.print("Error: Failed to write complete page. Expected {d} bytes, wrote {d} bytes.\n", .{ PAGE_SIZE, bytes_written });
        return error.IncompleteWrite;
    }

    // Print page content for debugging
    std.debug.print("Debug: Flushing page {d} content:\n", .{page_num});
    const node = @as([*]u8, @ptrCast(pager.pages[page_num].?.ptr));
    const num_cells = leafNodeNumCells(node);
    std.debug.print("Number of cells: {d}\n", .{num_cells});
    std.debug.print("Node type: {d}, is_root: {d}\n", .{ node[NODE_TYPE_OFFSET], node[IS_ROOT_OFFSET] });

    // Print raw data for debugging
    std.debug.print("Debug: Raw page data:\n", .{});
    for (0..PAGE_SIZE) |i| {
        if (i % 16 == 0) {
            std.debug.print("\n{d:0>4}: ", .{i});
        }
        std.debug.print("{x:0>2} ", .{node[i]});
    }
    std.debug.print("\n", .{});

    // Print each cell's content for debugging
    for (0..num_cells) |i| {
        const key = leafNodeKey(node, @intCast(i));
        std.debug.print("Cell {d}: key={d}\n", .{ i, key });
    }

    std.debug.print("Successfully flushed page {d} to disk\n", .{page_num});
}

// Create new table
fn dbOpen(filename: []const u8, allocator: std.mem.Allocator) !*Table {
    const pager = try pagerOpen(filename, allocator);
    const table = try allocator.create(Table);
    table.* = Table.init(pager);

    if (pager.num_pages == 0) {
        // New database file. Initialize page 0 as leaf node.
        const root_node = try getPage(pager, 0, allocator);
        initialize_leaf_node(root_node);
        // Set file length to one page
        pager.file_length = PAGE_SIZE;
        pager.num_pages = 1;
    } else {
        // Existing database file. Get the number of rows from the root node.
        const root_node = try getPage(pager, 0, allocator);
        const num_cells = leafNodeNumCells(root_node);
        std.debug.print("Opened existing database with {d} rows\n", .{num_cells});

        // Verify root node validity
        if (root_node[NODE_TYPE_OFFSET] != @intFromEnum(NodeType.NODE_LEAF)) {
            std.debug.print("Error: Root node is not a leaf node\n", .{});
            return error.InvalidNodeType;
        }

        // Verify that the root node is marked as root
        if (root_node[IS_ROOT_OFFSET] != 1) {
            std.debug.print("Error: Root node is not marked as root\n", .{});
            return error.InvalidRootNode;
        }

        // Verify the validity of each cell
        for (0..num_cells) |i| {
            const value = leafNodeValue(root_node, @intCast(i));
            var row = Row{
                .id = 0,
                .magic = 0,
                .username = undefined,
                .email = undefined,
            };
            deserializeRow(value, &row);
            if (row.magic != MAGIC_VALID_ROW) {
                std.debug.print("Error: Invalid row at index {d}\n", .{i});
                return error.InvalidRow;
            }
        }

        // Print the contents of each cell for debugging
        std.debug.print("Debug: Printing all cells in root node:\n", .{});
        for (0..num_cells) |i| {
            const value = leafNodeValue(root_node, @intCast(i));
            var row = Row{
                .id = 0,
                .magic = 0,
                .username = undefined,
                .email = undefined,
            };
            deserializeRow(value, &row);
            std.debug.print("Cell {d}: id={d}, magic={x}, username={s}, email={s}\n", .{
                i,
                row.id,
                row.magic,
                row.username[0..@min(10, row.username.len)],
                row.email[0..@min(10, row.email.len)],
            });
        }
    }

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
    var buffer: [256]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buffer);
    const stdout = &writer.interface;
    stdout.print("db > ", .{}) catch {};
    stdout.flush() catch {};
}

// Read input
fn readInput(input_buffer: *InputBuffer, allocator: std.mem.Allocator) !void {
    var read_buffer: [512]u8 = undefined;
    var stdin_reader = std.fs.File.stdin().reader(&read_buffer);
    const stdin = &stdin_reader.interface;

    // Free existing buffer if exists
    if (input_buffer.buffer) |buf| {
        allocator.free(buf);
    }

    // Allocate initial buffer with enough space for maximum input
    var buffer = try allocator.alloc(u8, 400);
    errdefer allocator.free(buffer);

    const line = stdin.takeDelimiterExclusive('\n') catch |err| {
        // Free the buffer on error
        allocator.free(buffer);
        input_buffer.buffer = null;
        input_buffer.buffer_length = 0;
        input_buffer.input_length = 0;

        if (err == error.EndOfStream) {
            // Handle EOF gracefully - exit the program
            var stdout_buffer: [256]u8 = undefined;
            var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
            const stdout = &stdout_writer.interface;
            stdout.print("\n", .{}) catch {};
            stdout.flush() catch {};
            std.process.exit(0);
        }

        return err;
    };

    // Toss the delimiter
    stdin.toss(1);

    // Copy line data to buffer
    const len = @min(line.len, buffer.len);
    @memcpy(buffer[0..len], line[0..len]);

    input_buffer.buffer = buffer;
    input_buffer.buffer_length = len;
    input_buffer.input_length = @intCast(len);
}

// Close input buffer
fn closeInputBuffer(input_buffer: *InputBuffer, allocator: std.mem.Allocator) void {
    input_buffer.deinit(allocator);
}

fn printConstants() void {
    var buffer: [512]u8 = undefined;
    var writer = std.fs.File.stdout().writer(&buffer);
    const stdout = &writer.interface;

    stdout.print("Constants:\n", .{}) catch {};
    stdout.print("ROW_SIZE: {d}\n", .{ROW_SIZE}) catch {};
    stdout.print("COMMON_NODE_HEADER_SIZE: {d}\n", .{COMMON_NODE_HEADER_SIZE}) catch {};
    stdout.print("LEAF_NODE_HEADER_SIZE: {d}\n", .{LEAF_NODE_HEADER_SIZE}) catch {};
    stdout.print("LEAF_NODE_CELL_SIZE: {d}\n", .{LEAF_NODE_CELL_SIZE}) catch {};
    stdout.print("LEAF_NODE_SPACE_FOR_CELLS: {d}\n", .{LEAF_NODE_SPACE_FOR_CELLS}) catch {};
    stdout.print("LEAF_NODE_MAX_CELLS: {d}\n", .{LEAF_NODE_MAX_CELLS}) catch {};
    stdout.flush() catch {};
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
    } else if (std.mem.eql(u8, buffer_content, ".btree")) {
        var stdout_buffer: [512]u8 = undefined;
        var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
        const stdout = &stdout_writer.interface;
        stdout.print("Tree:\n", .{}) catch {};
        stdout.flush() catch {};
        const node = getPage(table.pager, 0, allocator) catch |err| {
            std.debug.print("Error getting page: {s}\n", .{@errorName(err)});
            return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND;
        };

        printLeafNode(node);
        return MetaCommandResult.META_COMMAND_SUCCESS;
    } else if (std.mem.eql(u8, buffer_content, ".constants")) {
        printConstants();
        return MetaCommandResult.META_COMMAND_SUCCESS;
    } else {
        return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

fn printLeafNode(node: [*]u8) void {
    var stdout_buffer: [512]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;
    const num_cells = leafNodeNumCells(node);

    // 打印标题
    stdout.print("leaf (size {d})\n", .{num_cells}) catch {};

    // 直接打印每个节点的实际键值
    for (0..num_cells) |i| {
        const key = leafNodeKey(node, @intCast(i));
        std.debug.print("Debug: Printing key for cell {d}: {d}\n", .{ i, key });
        stdout.print("  - {d} : {d}\n", .{ i, key }) catch {};
    }
    stdout.flush() catch {};
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
    const row_to_insert = &statement.row_to_insert;
    row_to_insert.magic = MAGIC_VALID_ROW;

    const cursor = tableFind(table, row_to_insert.id, allocator) catch {
        return ExecuteResult.EXECUTE_TABLE_FULL;
    };
    defer allocator.destroy(cursor);

    const node = getPage(table.pager, cursor.page_num, allocator) catch |err| {
        std.debug.print("Error getting page: {s}\n", .{@errorName(err)});
        return ExecuteResult.EXECUTE_TABLE_FULL;
    };

    const num_cells = leafNodeNumCells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        return ExecuteResult.EXECUTE_TABLE_FULL;
    }

    const key_to_insert = row_to_insert.id;
    const existing_key = leafNodeKey(node, cursor.cell_num);
    if (existing_key == key_to_insert) {
        return ExecuteResult.EXECUTE_DUPLICATE_KEY;
    }

    leafNodeInsert(cursor, row_to_insert.id, row_to_insert);

    pagerFlush(table.pager, cursor.page_num) catch |err| {
        std.debug.print("Error flushing page: {s}\n", .{@errorName(err)});
        return ExecuteResult.EXECUTE_TABLE_FULL;
    };

    return ExecuteResult.EXECUTE_SUCCESS;
}

fn executeSelect(_: *Statement, table: *Table, allocator: std.mem.Allocator) ExecuteResult {
    const cursor = tableStart(table, allocator) catch {
        return ExecuteResult.EXECUTE_TABLE_FULL;
    };
    defer allocator.destroy(cursor);

    var row = Row{
        .id = 0,
        .magic = 0,
        .username = undefined,
        .email = undefined,
    };

    var stdout_buffer: [512]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    while (!cursor.end_of_table) {
        const value = cursorValue(cursor, allocator) orelse {
            cursorAdvance(cursor);
            continue;
        };

        deserializeRow(value, &row);

        if (row.magic != MAGIC_VALID_ROW) {
            cursorAdvance(cursor);
            continue;
        }

        // 打印行数据
        var username_len: usize = 0;
        while (username_len < row.username.len and row.username[username_len] != 0) {
            username_len += 1;
        }

        var email_len: usize = 0;
        while (email_len < row.email.len and row.email[email_len] != 0) {
            email_len += 1;
        }

        stdout.print("({d}, {s}, {s})\n", .{ row.id, row.username[0..username_len], row.email[0..email_len] }) catch {};
        cursorAdvance(cursor);
    }
    stdout.flush() catch {};

    return ExecuteResult.EXECUTE_SUCCESS;
}

fn executeStatement(statement: *Statement, table: *Table, allocator: std.mem.Allocator) ExecuteResult {
    var stdout_buffer: [256]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;

    switch (statement.type) {
        .STATEMENT_INSERT => {
            const result = executeInsert(statement, table, allocator);
            if (result == .EXECUTE_SUCCESS) {
                stdout.print("Executed.\n", .{}) catch {};
            } else if (result == .EXECUTE_DUPLICATE_KEY) {
                stdout.print("Error: Duplicate key.\n", .{}) catch {};
            } else if (result == .EXECUTE_TABLE_FULL) {
                stdout.print("Error: Table full.\n", .{}) catch {};
            }
            stdout.flush() catch {};
            return result;
        },
        .STATEMENT_SELECT => {
            const result = executeSelect(statement, table, allocator);
            if (result == .EXECUTE_SUCCESS) {
                stdout.print("Executed.\n", .{}) catch {};
            }
            stdout.flush() catch {};
            return result;
        },
    }
}

fn leafNodeSplitAndInsert(cursor: *Cursor, key: u32, value: *Row) void {
    // Get the old node
    _ = getPage(cursor.table.pager, cursor.page_num, cursor.table.pager.allocator) catch {
        std.debug.print("Failed to get old page in split\n", .{});
        return;
    };

    // Create a new node
    const new_page_num = getUnusedPageNum(cursor.table.pager);
    const new_node = getPage(cursor.table.pager, new_page_num, cursor.table.pager.allocator) catch {
        std.debug.print("Failed to get new page in split\n", .{});
        return;
    };

    // Initialize the new node
    initialize_leaf_node(new_node);

    // For now, just insert the row directly into the new node
    // In a real B-tree implementation, we would split the cells between the nodes

    // Set cursor to new node
    cursor.page_num = new_page_num;
    cursor.cell_num = 0;

    // Insert directly using standard insertion
    // Write key
    setLeafNodeKey(new_node, 0, key);

    // Serialize row data into value part of node
    serializeRow(value, leafNodeValue(new_node, 0));

    // Update number of cells
    setLeafNodeNumCells(new_node, 1);

    std.debug.print("Split: Inserted key {} into new node {}\n", .{ key, new_page_num });
}

fn leafNodeGetMaxKey(node: [*]u8) u32 {
    const num_cells = leafNodeNumCells(node);
    if (num_cells == 0) return 0;
    return leafNodeKey(node, num_cells - 1);
}

fn internalNodeGetMaxKey(node: []u8) u32 {
    const num_cells = internalNodeNumKeys(node);
    if (num_cells == 0) return 0;
    return std.mem.readIntLittle(u32, internalNodeKey(node, num_cells - 1));
}

fn internalNodeParent(node: [*]u8) u32 {
    var buffer: [4]u8 align(4) = undefined;
    @memcpy(buffer[0..], node[PARENT_POINTER_OFFSET..][0..4]);
    return @as(*u32, @ptrCast(&buffer)).*;
}

fn internalNodeNumKeys(node: [*]u8) u32 {
    var buffer: [4]u8 align(4) = undefined;
    @memcpy(buffer[0..], node[INTERNAL_NODE_NUM_KEYS_OFFSET..][0..4]);
    return @as(*u32, @ptrCast(&buffer)).*;
}

fn internalNodeKey(node: [*]u8, key_num: u32) [*]u8 {
    return @as([*]u8, @ptrCast(&node[INTERNAL_NODE_HEADER_SIZE + key_num * INTERNAL_NODE_CELL_SIZE]));
}

fn internalNodeChild(node: [*]u8, child_num: u32) [*]u8 {
    return @as([*]u8, @ptrCast(&node[INTERNAL_NODE_HEADER_SIZE + child_num * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_KEY_SIZE]));
}

fn leafNodeFindChild(node: [*]u8, key: u32) u32 {
    const num_cells = leafNodeNumCells(node);
    std.debug.print("Debug: Finding child for key: {}, num_cells: {}\n", .{ key, num_cells });

    // Find the correct position to insert the key
    var i: u32 = 0;
    while (i < num_cells) : (i += 1) {
        const current_key = leafNodeKey(node, i);
        if (key <= current_key) {
            std.debug.print("Debug: Found insertion position at index: {}\n", .{i});
            return i;
        }
    }

    // If we get here, insert at the end
    std.debug.print("Debug: Inserting at end (index: {})\n", .{num_cells});
    return num_cells;
}

fn getUnusedPageNum(pager: *Pager) u32 {
    return pager.num_pages;
}

fn tableEnd(table: *Table, allocator: std.mem.Allocator) !*Cursor {
    const cursor = try allocator.create(Cursor);
    cursor.* = Cursor{
        .table = table,
        .page_num = table.root_page_num,
        .cell_num = 0,
        .end_of_table = false,
    };

    const root_node = try getPage(table.pager, table.root_page_num, allocator);
    const num_cells = leafNodeNumCells(root_node);
    cursor.cell_num = num_cells;
    cursor.end_of_table = true;

    return cursor;
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
            // Create stdout writer for error messages
            var stdout_buffer: [512]u8 = undefined;
            var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
            const stdout = &stdout_writer.interface;

            // Check if it's a meta command
            if (buffer_content.len > 0 and buffer_content[0] == '.') {
                switch (doMetaCommand(input_buffer, table, allocator)) {
                    .META_COMMAND_SUCCESS => continue,
                    .META_COMMAND_UNRECOGNIZED_COMMAND => {
                        stdout.print("Unrecognized command '{s}'\n", .{buffer_content}) catch {};
                        stdout.flush() catch {};
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
                    stdout.print("ID must be positive.\n", .{}) catch {};
                    stdout.flush() catch {};
                    continue;
                },
                .PREPARE_SYNTAX_ERROR => {
                    stdout.print("Syntax error. Could not parse statement.\n", .{}) catch {};
                    stdout.flush() catch {};
                    continue;
                },
                .PREPARE_UNRECOGNIZED_STATEMENT => {
                    stdout.print("Unrecognized keyword at start of '{s}'\n", .{buffer_content}) catch {};
                    stdout.flush() catch {};
                    continue;
                },
                .PREPARE_STRING_TOO_LONG => {
                    stdout.print("String is too long.\n", .{}) catch {};
                    stdout.flush() catch {};
                    continue;
                },
            }

            // Execute statement
            switch (executeStatement(&statement, table, allocator)) {
                .EXECUTE_SUCCESS => {
                    // Success message is already printed in executeStatement
                },
                .EXECUTE_DUPLICATE_KEY => {},
                .EXECUTE_TABLE_FULL => {
                    // Error message is already printed in executeStatement
                },
            }
        }
    }
}
