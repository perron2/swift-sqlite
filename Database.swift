import Foundation

private let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

class Database {
    let file: String

    struct TransactionContext {
        private let db: Database
        private let name: String
        var cancelled = false

        init(db: Database) {
            self.db = db
            self.name = db.quote(UUID().uuidString)
        }

        fileprivate func begin() throws {
            try db.execute("savepoint \(name)")
        }

        fileprivate func commit() throws {
            if !cancelled {
                try db.execute("release savepoint \(name)")
            }
        }

        fileprivate mutating func rollback() throws {
            if !cancelled {
                try db.execute("rollback to savepoint \(name)")
                cancelled = true
            }
        }

        mutating func cancel() {
            try! rollback()
        }
    }

    init(name: String) throws {
        let path = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first!
        file = path + "/" + name
        if sqlite3_open_v2(file, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nil) != SQLITE_OK {
            throw DatabaseError(db!)
        }
    }

    deinit {
        close()
    }

    func close() {
        if db != nil {
            sqlite3_close(db)
            db = nil
        }
    }

    var schemaVersion: Int {
        return try! Int(selectInteger("pragma schema_version"))
    }

    var userVersion: Int {
        get {
            return try! Int(selectInteger("pragma user_version"))
        }
        set {
            try! execute("pragma user_version = \(newValue)")
        }
    }

    var lastInsertID: Int64 {
        return sqlite3_last_insert_rowid(db)
    }

    var affectedRows: Int {
        return Int(sqlite3_changes(db))
    }

    var changes: Int {
        return Int(sqlite3_changes(db))
    }

    var totalChanges: Int {
        return Int(sqlite3_total_changes(db))
    }

    var foreignKeysEnabled: Bool = false {
        didSet {
            if foreignKeysEnabled != oldValue {
                let value = foreignKeysEnabled ? "on" : "off"
                try! execute("pragma foreign_keys = \(value)")
            }
        }
    }

    var writeAheadLogEnabled: Bool {
        get {
            if let mode = try! selectString("pragma journal_mode") {
                return mode == "wal"
            }
            return false
        }
        set(value) {
            let mode = value ? "wal" : "delete"
            try! execute("pragma journal_mode = \(mode)")
        }
    }

    static func drop(_ name: String) {
        let path = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true).first!
        let file = path + "/" + name
        do {
            try FileManager.default.removeItem(atPath: file)
        } catch {
            print("Cannot drop database \(file): \(error)")
        }
    }

    func quote(_ str: String) -> String {
        return "'" + str.replacingOccurrences(of: "'", with: "''") + "'"
    }

    func transaction(_ block: () throws -> Void) throws {
        var context = TransactionContext(db: self)
        try context.begin()
        do {
            try block()
            try context.commit()
        } catch {
            try context.rollback()
            throw error
        }
    }

    func transaction(_ block: (inout TransactionContext) throws -> Void) throws -> Bool {
        var context = TransactionContext(db: self)
        try context.begin()
        do {
            try block(&context)
            if context.cancelled {
                return false
            }
            try context.commit()
            return true
        } catch {
            try context.rollback()
            throw error
        }
    }

    func applyWriteAheadLog() {
        try! execute("pragma wal_checkpoint(truncate)")
    }

    func selectInteger(_ sql: String) throws -> Int64 {
        let stmt = try createStmt(db!, sql)
        defer { sqlite3_finalize(stmt) }

        if sqlite3_step(stmt) == SQLITE_ROW {
            return sqlite3_column_int64(stmt, 0)
        } else {
            throw DatabaseError(db!)
        }
    }

    func selectString(_ sql: String) throws -> String? {
        let stmt = try createStmt(db!, sql)
        defer { sqlite3_finalize(stmt) }

        if sqlite3_step(stmt) == SQLITE_ROW {
            return String(cString: UnsafePointer(sqlite3_column_text(stmt, Int32(0))))
        } else {
            throw DatabaseError(db!)
        }
    }

    func selectDate(_ sql: String) throws -> Date? {
        if let str = try selectString(sql) {
            return dateFromString(str)
        }
        return nil
    }

    func prepare(_ sql: String) throws -> DatabaseStatement {
        return try DatabaseStatement(db!, sql)
    }

    func execute(_ sql: String) throws {
        if sqlite3_exec(db, sql, nil, nil, nil) != SQLITE_OK {
            throw DatabaseError(db!)
        }
    }

    func insert(into table: String, values: ContentValues) throws -> Int64 {
        var names = ""
        var params = ""
        for (index, value) in values.values.enumerated() {
            if index > 0 {
                names += ", "
                params += ", "
            }
            names += value.name
            params += ":" + value.name
        }
        let sql = "insert into \(table) (\(names)) values (\(params))"
        let stmt = try prepare(sql)
        for value in values.values {
            switch value.value {
            case nil:
                stmt.bindNull(value.name)
            case let val as String:
                stmt.bind(value.name, val)
            case let val as Int32:
                stmt.bind(value.name, val)
            case let val as Int64:
                stmt.bind(value.name, val)
            case let val as Double:
                stmt.bind(value.name, val)
            default:
                stmt.bind(value.name, String(describing: value.value!))
            }
        }
        try stmt.execute()
        return lastInsertID
    }

    func update(into table: String, values: ContentValues, where whereClause: String, with whereArgs: ContentValue...) throws -> Int {
        return try update(into: table, values: values, where: whereClause, with: whereArgs)
    }

    func update(into table: String, values: ContentValues, where whereClause: String, with whereArgs: [ContentValue]) throws -> Int {
        var params = ""
        for (index, value) in values.values.enumerated() {
            if index > 0 {
                params += ", "
            }
            params += value.name + "=:" + value.name
        }
        let sql = "update \(table) set \(params) where \(whereClause)"
        let stmt = try prepare(sql)
        values.add(whereArgs)
        for value in values.values {
            switch value.value {
            case nil:
                stmt.bindNull(value.name)
            case let val as String:
                stmt.bind(value.name, val)
            case let val as Int32:
                stmt.bind(value.name, val)
            case let val as Int64:
                stmt.bind(value.name, val)
            case let val as Double:
                stmt.bind(value.name, val)
            default:
                stmt.bind(value.name, String(describing: value.value!))
            }
        }
        try stmt.execute()
        return affectedRows
    }

    func save(into table: String, values: ContentValues, where whereClause: String, with whereArgs: ContentValue...) throws -> Int64? {
        let affectedRows = try update(into: table, values: values, where: whereClause, with: whereArgs)
        if affectedRows == 0 {
            return try insert(into: table, values: values)
        }
        return nil
    }

    private var db: OpaquePointer? = nil
}

class DatabaseStatement {
    private var db: OpaquePointer
    private var stmt: OpaquePointer? = nil

    fileprivate init(_ db: OpaquePointer, _ sql: String) throws {
        self.db = db

        if sqlite3_prepare_v2(db, sql, -1, &stmt, nil) != SQLITE_OK {
            throw DatabaseError(db)
        }
    }

    deinit {
        sqlite3_finalize(stmt)
    }

    func reset() {
        sqlite3_reset(stmt)
    }

    func bindNull(_ name: String) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")
        sqlite3_bind_null(stmt, index)
    }

    func bind(_ name: String, _ value: Int?) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")
        
        if let value = value {
            #if CGFLOAT_IS_DOUBLE
                sqlite3_bind_int64(stmt, index, sqlite3_int64(value))
            #else
                sqlite3_bind_int(stmt, index, Int32(value))
            #endif
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }

    func bind(_ name: String, _ value: Int32?) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")

        if let value = value {
            sqlite3_bind_int(stmt, index, Int32(value))
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }

    func bind(_ name: String, _ value: Int64?) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")

        if let value = value {
            sqlite3_bind_int64(stmt, index, sqlite3_int64(value))
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }

    func bind(_ name: String, _ value: Double?) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")

        if let value = value {
            sqlite3_bind_double(stmt, index, value)
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }

    func bind(_ name: String, _ value: Bool?) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")

        if let value = value {
            sqlite3_bind_int(stmt, index, value ? 1 : 0)
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }

    func bind(_ name: String, _ value: String?) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")

        if let value = value {
            sqlite3_bind_text(stmt, index, value, -1, SQLITE_TRANSIENT)
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }

    func bind(_ name: String, _ value: Date?) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")

        if let value = value {
            sqlite3_bind_text(stmt, index, dateToString(value), -1, SQLITE_TRANSIENT)
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }

    func execute() throws {
        if sqlite3_step(stmt) != SQLITE_DONE {
            throw DatabaseError(db)
        }
        sqlite3_reset(stmt)
    }

    func query() -> DatabaseRows {
        return DatabaseRows(stmt!)
    }

    func string() throws -> String? {
        let result = sqlite3_step(stmt)
        if result == SQLITE_DONE {
            return nil
        } else if result == SQLITE_ROW {
            if sqlite3_column_type(stmt, Int32(0)) == SQLITE_NULL {
                return nil
            } else {
                return String(cString: UnsafePointer(sqlite3_column_text(stmt, Int32(0))))
            }
        } else {
            throw DatabaseError(db)
        }
    }

    func int() throws -> Int64 {
        if sqlite3_step(stmt) == SQLITE_ROW {
            return sqlite3_column_int64(stmt, 0)
        } else {
            throw DatabaseError(db)
        }
    }
}

class DatabaseRows {
    init(_ stmt: OpaquePointer) {
        self.stmt = stmt
    }

    deinit {
        close()
    }

    func next() throws -> Bool {
        let res = sqlite3_step(stmt)
        if res == SQLITE_DONE {
            close()
            return false
        } else if res == SQLITE_ROW {
            return true
        } else {
            throw DatabaseError(sqlite3_db_handle(stmt))
        }
    }

    func rewind() {
        sqlite3_reset(stmt)
    }

    func close() {
        if !closed {
            sqlite3_reset(stmt)
            closed = true
        }
    }

    func index(_ column: String) -> Int {
        if let index = nameToIndex[column] {
            return index
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return 0
    }

    func null(_ column: Int) -> Bool {
        return sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL
    }

    func null(_ column: String) -> Bool {
        if let index = nameToIndex[column] {
            return null(index)
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return false
    }

    func int(_ column: Int) -> Int? {
        if sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL {
            return nil
        } else {
            return Int(sqlite3_column_int64(stmt, Int32(column)))
        }
    }

    func int(_ column: String) -> Int? {
        if let index = nameToIndex[column] {
            return int(index)
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return nil
    }

    func double(_ column: Int) -> Double? {
        if sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL {
            return nil
        } else {
            return sqlite3_column_double(stmt, Int32(column))
        }
    }

    func double(_ column: String) -> Double? {
        if let index = nameToIndex[column] {
            return double(index)
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return nil
    }

    func bool(_ column: Int) -> Bool? {
        if sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL {
            return nil
        } else {
            return sqlite3_column_int(stmt, Int32(column)) == 1
        }
    }

    func bool(_ column: String) -> Bool? {
        if let index = nameToIndex[column] {
            return bool(index)
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return nil
    }

    func string(_ column: Int) -> String? {
        if sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL {
            return nil
        } else {
            return String(cString: UnsafePointer(sqlite3_column_text(stmt, Int32(column))))
        }
    }

    func string(_ column: String) -> String? {
        if let index = nameToIndex[column] {
            return string(index)
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return nil
    }

    func date(_ column: Int) -> Date? {
        if sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL {
            return nil
        } else {
            let str = String(cString: sqlite3_column_text(stmt, Int32(column)))
            return dateFromString(str)
        }
    }

    func date(_ column: String) -> Date? {
        if let index = nameToIndex[column] {
            return date(index)
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return nil
    }

    // MARK:- Private

    private var stmt: OpaquePointer
    private var closed: Bool = false

    private lazy var nameToIndex: [String:Int] = {
        var map = [String:Int]()
        let count = Int(sqlite3_column_count(self.stmt))
        for i in 0..<count {
            let name = String(cString: sqlite3_column_name(self.stmt, Int32(i))) as String?
            map[name!] = i
        }
        return map
    }()
}

struct ContentValue {
    init(_ name: String, _ value: Any?) {
        self.name = name
        self.value = value
    }

    let name: String
    let value: Any?
}

class ContentValues {
    fileprivate var values = [ContentValue]()

    func clear() {
        values.removeAll()
    }

    func putNull(_ name: String) {
        values.append(ContentValue(name, nil))
    }

    func put(_ name: String, _ value: String?) {
        if let value = value {
            values.append(ContentValue(name, value))
        } else {
            values.append(ContentValue(name, nil))
        }
    }

    func put(_ name: String, _ value: Int?) {
        if let value = value {
            #if CGFLOAT_IS_DOUBLE
            values.append(ContentValue(name, Int64(value)))
            #else
            values.append(ContentValue(name, Int32(value)))
            #endif
        } else {
            values.append(ContentValue(name, nil))
        }
    }

    func put(_ name: String, _ value: Int32?) {
        if let value = value {
            values.append(ContentValue(name, value))
        } else {
            values.append(ContentValue(name, nil))
        }
    }

    func put(_ name: String, _ value: Int64?) {
        if let value = value {
            values.append(ContentValue(name, value))
        } else {
            values.append(ContentValue(name, nil))
        }
    }

    func put(_ name: String, _ value: Double?) {
        if let value = value {
            values.append(ContentValue(name, value))
        } else {
            values.append(ContentValue(name, nil))
        }
    }

    func put(_ name: String, _ value: Bool?) {
        if let value = value {
            let value: Int32 = value ? 1 : 0
            values.append(ContentValue(name, value))
        } else {
            values.append(ContentValue(name, nil))
        }
    }

    func put(_ name: String, _ value: Date?) {
        if let value = value {
            values.append(ContentValue(name, dateToString(value)))
        } else {
            values.append(ContentValue(name, nil))
        }
    }

    func add(_ args: [ContentValue]) {
        values += args
    }
}

class DatabaseError: Error, CustomStringConvertible {
    private(set) var code: Int
    private(set) var message: String

    init(_ db: OpaquePointer) {
        code = Int(sqlite3_errcode(db))
        message = String(cString: sqlite3_errmsg(db))
    }

    var description: String {
        return "\(message) (error code: \(code))"
    }
}

private func createStmt(_ db: OpaquePointer, _ sql: String) throws -> OpaquePointer {
    var stmt: OpaquePointer? = nil
    if sqlite3_prepare_v2(db, sql, -1, &stmt, nil) != SQLITE_OK {
        throw DatabaseError(db)
    }
    return stmt!
}

private func quote(_ str: String) -> String {
    return str.replacingOccurrences(of: "'", with: "''")
}

private func dateToString(_ date: Date) -> String {
    return dateFormatter.string(from: date)
}

private func dateFromString(_ str: String) -> Date? {
    return dateFormatter.date(from: str)
}

private var dateFormatter: DateFormatter = {
    let formatter = DateFormatter()
    formatter.locale =  Locale(identifier: "en_US_POSIX")
    formatter.timeZone = TimeZone(abbreviation: "UTC")
    formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
    return formatter
}()
