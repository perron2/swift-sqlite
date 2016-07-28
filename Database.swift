import Foundation

private let SQLITE_STATIC = unsafeBitCast(0, sqlite3_destructor_type.self)
private let SQLITE_TRANSIENT = unsafeBitCast(-1, sqlite3_destructor_type.self)

class Database {
    let file: String
    
    struct TransactionContext {
        private var db: Database
        private var cancelled: Bool
        
        mutating func cancel() {
            if !cancelled {
                try! db.execute("rollback transaction")
            }
            self.cancelled = true
        }
    }
    
    init(name: String) throws {
        let path = NSSearchPathForDirectoriesInDomains(.DocumentDirectory, .UserDomainMask, true).first!
        file = path + "/" + name
        if sqlite3_open(file, &db) != SQLITE_OK {
            throw DatabaseError(db)
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
    
    static func drop(name: String) {
        let path = NSSearchPathForDirectoriesInDomains(.DocumentDirectory, .UserDomainMask, true).first!
        let file = path + "/" + name
        do {
            try NSFileManager.defaultManager().removeItemAtPath(file)
        } catch {
            print("Cannot drop database \(file): \(error)")
        }
    }
    
    func quote(str: String) -> String {
        return "'" + str.stringByReplacingOccurrencesOfString("'", withString: "''") + "'"
    }
    
    func transaction(block: () throws -> Void) throws {
        try execute("begin transaction")
        do {
            try block()
            try execute("commit transaction")
        } catch {
            try execute("rollback transaction")
            throw error
        }
    }
    
    func transaction(block: (inout TransactionContext) throws -> Void) throws -> Bool {
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
    
    func savepoint(name: String = NSUUID().UUIDString, block: () throws -> Void) throws {
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
    
    func selectInteger(sql: String) throws -> Int64 {
        let stmt = try createStmt(db, sql)
        defer { sqlite3_finalize(stmt) }
        
        if sqlite3_step(stmt) == SQLITE_ROW {
            return sqlite3_column_int64(stmt, 0)
        } else {
            throw DatabaseError(db)
        }
    }
    
    func prepare(sql: String) throws -> DatabaseStatement {
        return try DatabaseStatement(db, sql)
    }
    
    func execute(sql: String) throws {
        if sqlite3_exec(db, sql, nil, nil, nil) != SQLITE_OK {
            throw DatabaseError(db)
        }
    }
    
    private var db: COpaquePointer = nil
}

class DatabaseStatement {
    private var db: COpaquePointer
    private var stmt: COpaquePointer = nil
    
    private init(_ db: COpaquePointer, _ sql: String) throws {
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
    
    func bindNull(name: String) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")
        sqlite3_bind_null(stmt, index)
    }
    
    func bind(name: String, _ value: Int?) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")
        
        if let value = value {
            sqlite3_bind_int(stmt, index, Int32(value))
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }
    
    func bind(name: String, _ value: Double?) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")
        
        if let value = value {
            sqlite3_bind_double(stmt, index, value)
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }
    
    func bind(name: String, _ value: Bool?) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")
        
        if let value = value {
            sqlite3_bind_int(stmt, index, value ? 1 : 0)
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }
    
    func bind(name: String, _ value: String?) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")
        
        if let value = value {
            sqlite3_bind_text(stmt, index, value, -1, SQLITE_TRANSIENT)
        } else {
            sqlite3_bind_null(stmt, index)
        }
    }
    
    func bind(name: String, _ value: NSDate?) {
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
        return DatabaseRows(stmt)
    }
    
    func string() throws -> String? {
        let result = sqlite3_step(stmt)
        if result == SQLITE_DONE {
            return nil
        } else if result == SQLITE_ROW {
            if sqlite3_column_type(stmt, Int32(0)) == SQLITE_NULL {
                return nil
            } else {
                return String.fromCString(UnsafePointer(sqlite3_column_text(stmt, Int32(0))))
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
    init(_ stmt: COpaquePointer) {
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
    
    func null(column: Int) -> Bool {
        return sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL
    }
    
    func null(column: String) -> Bool {
        if let index = nameToIndex[column] {
            return null(index)
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return false
    }
    
    func int(column: Int) -> Int? {
        if sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL {
            return nil
        } else {
            return Int(sqlite3_column_int64(stmt, Int32(column)))
        }
    }
    
    func int(column: String) -> Int? {
        if let index = nameToIndex[column] {
            return int(index)
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return nil
    }
    
    func double(column: Int) -> Double? {
        if sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL {
            return nil
        } else {
            return sqlite3_column_double(stmt, Int32(column))
        }
    }
    
    func double(column: String) -> Double? {
        if let index = nameToIndex[column] {
            return double(index)
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return nil
    }
    
    func bool(column: Int) -> Bool? {
        if sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL {
            return nil
        } else {
            return sqlite3_column_int(stmt, Int32(column)) == 1
        }
    }
    
    func bool(column: String) -> Bool? {
        if let index = nameToIndex[column] {
            return bool(index)
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return nil
    }
    
    func string(column: Int) -> String? {
        if sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL {
            return nil
        } else {
            return String.fromCString(UnsafePointer(sqlite3_column_text(stmt, Int32(column))))
        }
    }
    
    func string(column: String) -> String? {
        if let index = nameToIndex[column] {
            return string(index)
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return nil
    }
    
    func date(column: Int) -> NSDate? {
        if sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL {
            return nil
        } else {
            if let str = String.fromCString(UnsafePointer(sqlite3_column_text(stmt, Int32(column)))) {
                return dateFromString(str)
            }
            return nil
        }
    }
    
    func date(column: String) -> NSDate? {
        if let index = nameToIndex[column] {
            return date(index)
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return nil
    }

    // MARK:- Private

    private var stmt: COpaquePointer
    private var closed: Bool = false

    private lazy var nameToIndex: [String:Int] = {
        var map = [String:Int]()
        let count = Int(sqlite3_column_count(self.stmt))
        for i in 0..<count {
            let name = String.fromCString(sqlite3_column_name(self.stmt, Int32(i))) as String!
            map[name] = i
        }
        return map
    }()
}

class DatabaseError : ErrorType, CustomStringConvertible {
    private(set) var code: Int
    private(set) var message: String
    
    init(_ db: COpaquePointer) {
        code = Int(sqlite3_errcode(db))
        message = String.fromCString(sqlite3_errmsg(db))!
    }
    
    var description: String {
        return "\(message) (error code: \(code))"
    }
}

private func createStmt(db: COpaquePointer, _ sql: String) throws -> COpaquePointer {
    var stmt: COpaquePointer = nil
    if sqlite3_prepare_v2(db, sql, -1, &stmt, nil) != SQLITE_OK {
        throw DatabaseError(db)
    }
    return stmt
}

private func quote(str: String) -> String {
    return str.stringByReplacingOccurrencesOfString("'", withString: "''")
}

private func dateToString(date: NSDate) -> String {
    return dateFormatter.stringFromDate(date)
}

private func dateFromString(str: String) -> NSDate? {
    return dateFormatter.dateFromString(str)
}

private var dateFormatter: NSDateFormatter = {
    let formatter = NSDateFormatter()
    formatter.locale =  NSLocale(localeIdentifier: "en_US_POSIX")
    formatter.timeZone = NSTimeZone(abbreviation: "UTC")
    formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
    return formatter
}()
