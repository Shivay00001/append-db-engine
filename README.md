# Append-Only Database Engine

A minimal, production-grade embedded append-only database engine written in Go with MVCC (Multi-Version Concurrency Control), Write-Ahead Logging (WAL), and snapshot isolation.

## Features

- **MVCC**: Multi-version concurrency control for concurrent read/write access
- **WAL**: Write-ahead logging for crash recovery and durability
- **Snapshot Isolation**: Transactions see a consistent snapshot of the database
- **CRC32 Integrity**: Data integrity verification using CRC32 checksums
- **Compaction**: Garbage collection of old versions to reclaim space
- **Prefix Scanning**: Efficient iteration over keys with a shared prefix
- **JSON Export**: Full database export to JSON format
- **Thread-Safe**: Safe for concurrent access from multiple goroutines

## Installation

```bash
go get github.com/Shivay00001/append-db-engine
```

## Usage

```go
package main

import (
    "fmt"
    aodb "github.com/Shivay00001/append-db-engine"
)

func main() {
    // Open database
    db, err := aodb.Open("./mydb", false)
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Start a transaction
    txn, _ := db.Begin()
    txn.Put([]byte("user:1"), []byte(`{"name": "Alice"}`))
    txn.Commit()

    // Read data
    value, _ := db.Get([]byte("user:1"))
    fmt.Println(string(value))
}
```

## Testing

```bash
go test -v -race ./...
```

## License

MIT
