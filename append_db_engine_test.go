package aodb

import (
	"os"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "aodb_test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestOpenAndClose(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenReadOnly(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, err := Open(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	txn, _ := db2.Begin()
	err = txn.Put([]byte("key"), []byte("val"))
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != ErrReadOnly {
		t.Fatalf("expected ErrReadOnly, got %v", err)
	}
}

func TestPutGetCommit(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if err := txn.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatal(err)
	}

	// Read within transaction
	val, err := txn.Get([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "world" {
		t.Fatalf("expected 'world', got '%s'", val)
	}

	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Read outside transaction
	val, err = db.Get([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "world" {
		t.Fatalf("expected 'world', got '%s'", val)
	}
}

func TestDeleteKey(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Put a key
	txn, _ := db.Begin()
	txn.Put([]byte("k1"), []byte("v1"))
	txn.Commit()

	// Delete it
	txn2, _ := db.Begin()
	txn2.Delete([]byte("k1"))
	txn2.Commit()

	// Should not be found
	_, err = db.Get([]byte("k1"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write initial value
	txn1, _ := db.Begin()
	txn1.Put([]byte("key"), []byte("v1"))
	txn1.Commit()

	// Start read transaction
	txnRead, _ := db.Begin()

	// Write new value in another transaction
	txn2, _ := db.Begin()
	txn2.Put([]byte("key"), []byte("v2"))
	txn2.Commit()

	// Read transaction should still see old value
	val, _ := txnRead.Get([]byte("key"))
	if string(val) != "v1" {
		t.Fatalf("snapshot isolation broken: expected 'v1', got '%s'", val)
	}
}

func TestScan(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	txn, _ := db.Begin()
	txn.Put([]byte("user:1"), []byte("Alice"))
	txn.Put([]byte("user:2"), []byte("Bob"))
	txn.Put([]byte("item:1"), []byte("Widget"))
	txn.Commit()

	count := 0
	db.Scan([]byte("user:"), func(key, value []byte) error {
		count++
		return nil
	})

	if count != 2 {
		t.Fatalf("expected 2 user keys, got %d", count)
	}
}

func TestCompact(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create multiple versions
	for i := 0; i < 5; i++ {
		txn, _ := db.Begin()
		txn.Put([]byte("key"), []byte("value"))
		txn.Commit()
	}

	stats := db.Stats()
	if stats["total_versions"].(int) != 5 {
		t.Fatalf("expected 5 versions, got %v", stats["total_versions"])
	}

	// Compact to 2 versions
	db.Compact(2)

	stats = db.Stats()
	if stats["total_versions"].(int) != 2 {
		t.Fatalf("expected 2 versions after compact, got %v", stats["total_versions"])
	}
}

func TestRollback(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	txn, _ := db.Begin()
	txn.Put([]byte("key"), []byte("value"))
	txn.Rollback()

	_, err = db.Get([]byte("key"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after rollback, got %v", err)
	}
}

func TestWALRecovery(t *testing.T) {
	dir := tempDir(t)

	// Write data
	db, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	txn, _ := db.Begin()
	txn.Put([]byte("persist"), []byte("me"))
	txn.Commit()
	db.Close()

	// Reopen and verify recovery
	db2, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	val, err := db2.Get([]byte("persist"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "me" {
		t.Fatalf("expected 'me', got '%s'", val)
	}
}

func TestCRCIntegrity(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("test_key")
	value := []byte("test_value")

	txn, _ := db.Begin()
	txn.Put(key, value)
	txn.Commit()

	// Verify key is not corrupted after CRC calculation
	result, _ := db.Get(key)
	if string(result) != "test_value" {
		t.Fatalf("key corruption detected")
	}
}

func TestExport(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	txn, _ := db.Begin()
	txn.Put([]byte("key"), []byte("value"))
	txn.Commit()

	data, err := db.Export()
	if err != nil {
		t.Fatal(err)
	}

	if len(data) == 0 {
		t.Fatal("export returned empty data")
	}
}

func TestDoubleClose(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != ErrDatabaseClosed {
		t.Fatalf("expected ErrDatabaseClosed, got %v", err)
	}
}

func TestBeginAfterClose(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	_, err = db.Begin()
	if err != ErrDatabaseClosed {
		t.Fatalf("expected ErrDatabaseClosed, got %v", err)
	}
}
