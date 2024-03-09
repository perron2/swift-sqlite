import Foundation

private let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

class Database {
    let file: URL

    init(file: URL) throws {
        self.file = file
        try open()
    }

    deinit {
        close()
    }

    func open() throws {
        if sqlite3_open_v2(file.path, &_handle, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nil) != SQLITE_OK {
            throw DatabaseError(_handle!)
        }
    }

    func close() {
        if _handle != nil {
            sqlite3_close(_handle)
            _handle = nil
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
        return sqlite3_last_insert_rowid(handle)
    }

    var affectedRows: Int {
        return Int(sqlite3_changes(handle))
    }

    var changes: Int {
        return Int(sqlite3_changes(handle))
    }

    var totalChanges: Int {
        return Int(sqlite3_total_changes(handle))
    }

    var foreignKeysEnabled = false {
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

    func upgradeSchema(targetVersion: Int, upgradeToVersion: (Int) throws -> Void) throws {
        let foreignKeys = foreignKeysEnabled
        foreignKeysEnabled = false
        defer { foreignKeysEnabled = foreignKeys }
        try transaction {
            var version = userVersion
            if version == 0 {
                try upgradeToVersion(0)
                userVersion = targetVersion
            } else {
                while version < targetVersion {
                    version += 1
                    try upgradeToVersion(version)
                    userVersion = version
                }
            }
        }
    }

    @discardableResult
    func transaction<T>(_ block: () throws -> T) throws -> T {
        return try serializeWriteAccess {
            let insideTransaction = transactionDepth > 0
            transactionDepth += 1
            let savepoint = "s\(transactionDepth)"
            try execute(insideTransaction ? "savepoint \(savepoint)" : "begin")
            do {
                let result = try block()
                try execute(insideTransaction ? "release savepoint \(savepoint)" : "commit")
                transactionDepth -= 1
                return result
            } catch {
                try execute(insideTransaction ? "rollback to savepoint \(savepoint)" : "rollback")
                transactionDepth -= 1
                throw error
            }
        }
    }

    func quote(_ str: String) -> String {
        return "'" + str.replacingOccurrences(of: "'", with: "''") + "'"
    }

    func applyWriteAheadLog() {
        try! execute("pragma wal_checkpoint(truncate)")
    }

    func selectInteger(_ sql: String) throws -> Int64 {
        let stmt = try createStmt(handle, sql)
        defer { sqlite3_finalize(stmt) }

        if sqlite3_step(stmt) == SQLITE_ROW {
            return sqlite3_column_int64(stmt, 0)
        } else {
            throw DatabaseError(handle)
        }
    }

    func selectInteger(_ sql: String, _ params: Param...) throws -> Int64 {
        let stmt = try prepare(sql)
        stmt.bind(params)
        return try stmt.int()
    }

    func selectString(_ sql: String) throws -> String? {
        let stmt = try createStmt(handle, sql)
        defer { sqlite3_finalize(stmt) }

        if sqlite3_step(stmt) == SQLITE_ROW {
            return String(cString: UnsafePointer(sqlite3_column_text(stmt, Int32(0))))
        } else {
            throw DatabaseError(handle)
        }
    }

    func selectString(_ sql: String, _ params: Param...) throws -> String? {
        let stmt = try prepare(sql)
        stmt.bind(params)
        return try stmt.string()
    }

    func selectDate(_ sql: String) throws -> Date? {
        if let str = try selectString(sql) {
            return dateFromString(str)
        }
        return nil
    }

    func prepare(_ sql: String) throws -> DatabaseStatement {
        return try DatabaseStatement(handle, sql)
    }

    func execute(_ sql: String) throws {
        try serializeWriteAccess {
            if sqlite3_exec(handle, sql, nil, nil, nil) != SQLITE_OK {
                throw DatabaseError(handle)
            }
        }
    }

    func execute(_ sql: String, _ params: Param...) throws {
        try serializeWriteAccess {
            let stmt = try prepare(sql)
            stmt.bind(params)
            try stmt.execute()
        }
    }

    func query(_ sql: String, _ params: Param...) throws -> DatabaseRows {
        let stmt = try prepare(sql)
        stmt.bind(params)
        return stmt.query()
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
        return try serializeWriteAccess {
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
    }

    func update(into table: String, values: ContentValues, where whereClause: String, with whereArgs: ContentValue...) throws -> Int {
        return try serializeWriteAccess {
            return try update(into: table, values: values, where: whereClause, with: whereArgs)
        }
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
        return try serializeWriteAccess {
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
                    case let val as Float:
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
    }

    func save(into table: String, values: ContentValues, where whereClause: String, with whereArgs: ContentValue...) throws -> Int64? {
        return try serializeWriteAccess {
            let affectedRows = try update(into: table, values: values, where: whereClause, with: whereArgs)
            if affectedRows == 0 {
                return try insert(into: table, values: values)
            }
            return nil
        }
    }

    func upsert(into table: String, values: ContentValues, idName: String) throws -> Int64 {
        return try serializeWriteAccess {
            if let value = values[idName] as? NSNumber {
                let id = value.int64Value
                let count = try update(into: table, values: values, where: "\(idName) = \(id)")
                if count == 0 {
                    return try insert(into: table, values: values)
                }
                return id
            }
            return try insert(into: table, values: values)
        }
    }

    private var _handle: OpaquePointer?
    private var handle: OpaquePointer {
        if _handle == nil {
            try! open()
        }
        return _handle!
    }

    private let semaphore = DispatchSemaphore(value: 1)
    private var transactionDepth = 0

    private func serializeWriteAccess<T>(_ block: () throws -> T) throws -> T {
        let semaphoreAcquired = Thread.current.threadDictionary["acquired"] as? Bool ?? false
        defer {
            if !semaphoreAcquired {
                Thread.current.threadDictionary["acquired"] = false
                print("SEMAPHORE RELEASED in \(Thread.current)")
                semaphore.signal()
            }
        }
        if !semaphoreAcquired {
            Thread.current.threadDictionary["acquired"] = true
            semaphore.wait()
            print("SEMAPHORE ACQUIRED in \(Thread.current)")
        }
        return try block()
    }
}

class DatabaseStatement {
    private var db: OpaquePointer
    private var stmt: OpaquePointer?
    private(set) var closed = false

    fileprivate init(_ db: OpaquePointer, _ sql: String) throws {
        self.db = db
        if sqlite3_prepare_v2(db, sql, -1, &stmt, nil) != SQLITE_OK {
            throw DatabaseError(db)
        }
    }

    deinit {
        close()
    }

    func close() {
        if !closed {
            sqlite3_finalize(stmt)
            closed = true
        }
    }

    func reset() {
        if !closed {
            sqlite3_reset(stmt)
        }
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

    func bind(_ name: String, _ value: Float?) {
        let index = sqlite3_bind_parameter_index(stmt, ":" + name)
        assert(index > 0, "Invalid parameter name \"\(name)\"")

        if let value = value {
            sqlite3_bind_double(stmt, index, Double(value))
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

    func bind(_ params: [Param]) {
        for param in params {
            let name = param.name
            let value = param.value
            switch value {
                case let value as Int: bind(name, value)
                case let value as Int32: bind(name, value)
                case let value as Int64: bind(name, value)
                case let value as Float: bind(name, value)
                case let value as Double: bind(name, value)
                case let value as Bool: bind(name, value)
                case let value as String: bind(name, value)
                case let value as Date: bind(name, value)
                default:
                    let type = String(describing: value)
                    let value = value.debugDescription
                    fatalError("Cannot bind value \(value) of type \(type) to placeholder \"\(name)\"")
            }
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

    private(set) var hasRow = false

    @discardableResult
    func next() throws -> Bool {
        let res = sqlite3_step(stmt)
        if res == SQLITE_DONE {
            close()
            return false
        } else if res == SQLITE_ROW {
            hasRow = true
            return true
        } else {
            throw DatabaseError(sqlite3_db_handle(stmt))
        }
    }

    func rewind() {
        sqlite3_reset(stmt)
        hasRow = false
    }

    func close() {
        hasRow = false
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

    func int<T>(_ column: Int, convert: (Int) -> T?) -> T? {
        if let value = int(column) {
            return convert(value)
        }
        return nil
    }

    func int<T>(_ column: String, convert: (Int) -> T?) -> T? {
        if let value = int(column) {
            return convert(value)
        }
        return nil
    }

    func int64(_ column: Int) -> Int64? {
        if sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL {
            return nil
        } else {
            return sqlite3_column_int64(stmt, Int32(column))
        }
    }

    func int64(_ column: String) -> Int64? {
        if let index = nameToIndex[column] {
            return int64(index)
        }
        assertionFailure("Invalid column name \"\(column)\"")
        return nil
    }

    func int64<T>(_ column: Int, convert: (Int64) -> T?) -> T? {
        if let value = int64(column) {
            return convert(value)
        }
        return nil
    }

    func int64<T>(_ column: String, convert: (Int64) -> T?) -> T? {
        if let value = int64(column) {
            return convert(value)
        }
        return nil
    }

    func float(_ column: Int) -> Float? {
        if sqlite3_column_type(stmt, Int32(column)) == SQLITE_NULL {
            return nil
        } else {
            return Float(sqlite3_column_double(stmt, Int32(column)))
        }
    }

    func float(_ column: String) -> Float? {
        if let index = nameToIndex[column] {
            return float(index)
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

    func string<T>(_ column: Int, convert: (String) throws -> T?) rethrows -> T? {
        if let value = string(column) {
            return try convert(value)
        }
        return nil
    }

    func string<T>(_ column: String, convert: (String) throws -> T?) rethrows -> T? {
        if let value = string(column) {
            return try convert(value)
        }
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

    // MARK: - Private

    private var stmt: OpaquePointer
    private var closed = false

    private lazy var nameToIndex: [String: Int] = {
        var map = [String: Int]()
        let count = Int(sqlite3_column_count(self.stmt))
        for i in 0..<count {
            let name = String(cString: sqlite3_column_name(self.stmt, Int32(i))) as String?
            map[name!] = i
        }
        return map
    }()
}

struct Param {
    init(_ name: String, _ value: Any?) {
        self.name = name
        self.value = value
    }

    let name: String
    let value: Any?
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

    func put(_ name: String, _ value: Float?) {
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

    subscript(name: String) -> Any? {
        for value in values {
            if value.name == name {
                return value.value
            }
        }
        return nil
    }
}

class DatabaseError: Error, CustomStringConvertible {
    let code: Int
    let message: String

    init(_ db: OpaquePointer) {
        code = Int(sqlite3_errcode(db))
        message = String(cString: sqlite3_errmsg(db))
    }

    var description: String {
        return "\(message) (error code: \(code))"
    }
}

private func createStmt(_ db: OpaquePointer, _ sql: String) throws -> OpaquePointer {
    var stmt: OpaquePointer?
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
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.timeZone = TimeZone(abbreviation: "UTC")
    formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
    return formatter
}()
