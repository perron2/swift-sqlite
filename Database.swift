import Foundation

private let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

class Database {
    let file: String

    struct TransactionContext {
        fileprivate var db: Database
        fileprivate var cancelled: Bool

        mutating func cancel() {
            if !cancelled {
                try! db.execute("rollback transaction")
            }
            self.cancelled = true
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
        try execute("begin transaction")
        do {
            try block()
            try execute("commit transaction")
        } catch {
            try execute("rollback transaction")
            throw error
        }
    }

    func transaction(_ block: (inout TransactionContext) throws -> Void) throws -> Bool {
        var context = TransactionContext(db: self, cancelled: false)
        try execute("begin transaction")
        do {
            try block(&context)
            if context.cancelled {
                return false
            }
            try execute("commit transaction")
            return true
        } catch {
            try execute("rollback transaction")
            throw error
        }
    }

    func savepoint(_ name: String = UUID().uuidString, block: () throws -> Void) throws {
        let name = quote(name)
        let savepoint = "savepoint \(name)"
        try execute(savepoint)
        do {
            try block()
            try execute("release \(savepoint)")
        } catch {
            try execute("rollback to \(savepoint)")
            throw error
        }
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

    func prepare(_ sql: String) throws -> DatabaseStatement {
        return try DatabaseStatement(db!, sql)
    }

    func execute(_ sql: String) throws {
        if sqlite3_exec(db, sql, nil, nil, nil) != SQLITE_OK {
            throw DatabaseError(db!)
        }
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
            sqlite3_bind_int(stmt, index, Int32(value))
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

    func close() {
        if !closed {
            sqlite3_reset(stmt)
            closed = true
        }
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
