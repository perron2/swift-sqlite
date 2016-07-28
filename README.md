SQLite Database
===============
A lightweight Swift wrapper around the C SQLite library.

Example
-------
```swift
do {
    let db = try Database(name: "your.db")
    defer { db.close() }
    db.foreignKeysEnabled = true

    let stmt = try db.prepare("select name, age from persons where age > :age")
    stmt.bind("age", 40)

    let rows = stmt.query()
    while try rows.next() {
        print(rows.string("name"))
        print(rows.int("age"))
    }
} catch {
    print(error)
}
```

Installation
------------
Add the files to your project and make sure you have a bridging header file that includes the standard SQLite header file:

```c
#ifndef Bridging_Header_h
#define Bridging_Header_h

#include <sqlite3.h>

#endif /* Bridging_Header_h */
```

Add the SQLite framework to your project:

1. Open the **General** settings tab of your target.
2. Under **Linked Frameworks and Libraries** click the add button.
3. Select **libsqlite3.0.tbd**