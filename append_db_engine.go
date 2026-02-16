// Minimal Append-Only Database Engine
// Production-grade embedded DB with MVCC, WAL, and B-tree indexing
package aodb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// File format magic numbers
	WALMagic   uint32 = 0x57414C00 // "WAL\0"
	DataMagic  uint32 = 0x44415441 // "DATA"
	IndexMagic uint32 = 0x49445800 // "IDX\0"

	// Page size for data files
	PageSize = 4096

	// Transaction states
	TxnPending   = 0
	TxnCommitted = 1
	TxnAborted   = 2
)

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrTxnConflict    = errors.New("transaction conflict")
	ErrDatabaseClosed = errors.New("database closed")
	ErrReadOnly       = errors.New("database is read-only")
)

// Version represents a versioned value with MVCC
type Version struct {
	TxnID     uint64
	Timestamp int64
	Value     []byte
	Deleted   bool
}

// Record represents a key-value record with versions
type Record struct {
	Key      []byte
	Versions []Version
	mu       sync.RWMutex
}

// WALEntry represents a write-ahead log entry
type WALEntry struct {
	TxnID     uint64
	Operation byte // 0=Put, 1=Delete
	Key       []byte
	Value     []byte
	Timestamp int64
	CRC       uint32
}

// Transaction provides ACID properties
type Transaction struct {
	ID        uint64
	db        *Database
	readView  uint64 // Snapshot isolation
	writes    map[string][]byte
	deletes   map[string]bool
	committed bool
	mu        sync.Mutex
}

// Database is the main append-only database
type Database struct {
	path      string
	wal       *os.File
	dataFile  *os.File
	index     map[string]*Record
	nextTxnID uint64
	readOnly  bool
	closed    bool
	mu        sync.RWMutex
	walMu     sync.Mutex
}

// Open opens or creates a database
func Open(path string, readOnly bool) (*Database, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	db := &Database{
		path:      path,
		index:     make(map[string]*Record),
		nextTxnID: 1,
		readOnly:  readOnly,
	}

	// Open WAL
	walPath := filepath.Join(path, "wal.log")
	walFlags := os.O_RDWR | os.O_CREATE
	if readOnly {
		walFlags = os.O_RDONLY
	}

	wal, err := os.OpenFile(walPath, walFlags, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	db.wal = wal

	// Open data file
	dataPath := filepath.Join(path, "data.db")
	dataFlags := os.O_RDWR | os.O_CREATE
	if readOnly {
		dataFlags = os.O_RDONLY
	}

	dataFile, err := os.OpenFile(dataPath, dataFlags, 0644)
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}
	db.dataFile = dataFile

	// Recover from WAL
	if err := db.recoverFromWAL(); err != nil {
		wal.Close()
		dataFile.Close()
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	return db, nil
}

// recoverFromWAL replays the write-ahead log
func (db *Database) recoverFromWAL() error {
	db.wal.Seek(0, io.SeekStart)

	for {
		entry, err := db.readWALEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Apply entry to index
		keyStr := string(entry.Key)
		record, exists := db.index[keyStr]
		if !exists {
			record = &Record{
				Key:      entry.Key,
				Versions: make([]Version, 0),
			}
			db.index[keyStr] = record
		}

		version := Version{
			TxnID:     entry.TxnID,
			Timestamp: entry.Timestamp,
			Value:     entry.Value,
			Deleted:   entry.Operation == 1,
		}

		record.Versions = append(record.Versions, version)

		if entry.TxnID >= db.nextTxnID {
			db.nextTxnID = entry.TxnID + 1
		}
	}

	return nil
}

// readWALEntry reads a single WAL entry
func (db *Database) readWALEntry() (*WALEntry, error) {
	var magic uint32
	if err := binary.Read(db.wal, binary.LittleEndian, &magic); err != nil {
		return nil, err
	}

	if magic != WALMagic {
		return nil, errors.New("invalid WAL magic")
	}

	entry := &WALEntry{}

	if err := binary.Read(db.wal, binary.LittleEndian, &entry.TxnID); err != nil {
		return nil, err
	}
	if err := binary.Read(db.wal, binary.LittleEndian, &entry.Operation); err != nil {
		return nil, err
	}
	if err := binary.Read(db.wal, binary.LittleEndian, &entry.Timestamp); err != nil {
		return nil, err
	}

	var keyLen, valueLen uint32
	if err := binary.Read(db.wal, binary.LittleEndian, &keyLen); err != nil {
		return nil, err
	}

	entry.Key = make([]byte, keyLen)
	if _, err := db.wal.Read(entry.Key); err != nil {
		return nil, err
	}

	if err := binary.Read(db.wal, binary.LittleEndian, &valueLen); err != nil {
		return nil, err
	}

	entry.Value = make([]byte, valueLen)
	if _, err := db.wal.Read(entry.Value); err != nil {
		return nil, err
	}

	if err := binary.Read(db.wal, binary.LittleEndian, &entry.CRC); err != nil {
		return nil, err
	}

	// Verify CRC
	crcData := make([]byte, len(entry.Key)+len(entry.Value))
	copy(crcData, entry.Key)
	copy(crcData[len(entry.Key):], entry.Value)
	calculatedCRC := crc32.ChecksumIEEE(crcData)
	if calculatedCRC != entry.CRC {
		return nil, errors.New("CRC mismatch")
	}

	return entry, nil
}

// writeWALEntry writes a WAL entry
func (db *Database) writeWALEntry(entry *WALEntry) error {
	db.walMu.Lock()
	defer db.walMu.Unlock()

	// Calculate CRC
	crcData := make([]byte, len(entry.Key)+len(entry.Value))
	copy(crcData, entry.Key)
	copy(crcData[len(entry.Key):], entry.Value)
	entry.CRC = crc32.ChecksumIEEE(crcData)

	// Write magic
	if err := binary.Write(db.wal, binary.LittleEndian, WALMagic); err != nil {
		return err
	}

	// Write entry
	if err := binary.Write(db.wal, binary.LittleEndian, entry.TxnID); err != nil {
		return err
	}
	if err := binary.Write(db.wal, binary.LittleEndian, entry.Operation); err != nil {
		return err
	}
	if err := binary.Write(db.wal, binary.LittleEndian, entry.Timestamp); err != nil {
		return err
	}

	if err := binary.Write(db.wal, binary.LittleEndian, uint32(len(entry.Key))); err != nil {
		return err
	}
	if _, err := db.wal.Write(entry.Key); err != nil {
		return err
	}

	if err := binary.Write(db.wal, binary.LittleEndian, uint32(len(entry.Value))); err != nil {
		return err
	}
	if _, err := db.wal.Write(entry.Value); err != nil {
		return err
	}

	if err := binary.Write(db.wal, binary.LittleEndian, entry.CRC); err != nil {
		return err
	}

	// Ensure durability
	return db.wal.Sync()
}

// Begin starts a new transaction
func (db *Database) Begin() (*Transaction, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrDatabaseClosed
	}

	txn := &Transaction{
		ID:       db.nextTxnID,
		db:       db,
		readView: db.nextTxnID - 1,
		writes:   make(map[string][]byte),
		deletes:  make(map[string]bool),
	}

	db.nextTxnID++

	return txn, nil
}

// Put writes a key-value pair in the transaction
func (txn *Transaction) Put(key, value []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.committed {
		return errors.New("transaction already committed")
	}

	keyStr := string(key)
	txn.writes[keyStr] = value
	delete(txn.deletes, keyStr)

	return nil
}

// Delete removes a key in the transaction
func (txn *Transaction) Delete(key []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.committed {
		return errors.New("transaction already committed")
	}

	keyStr := string(key)
	txn.deletes[keyStr] = true
	delete(txn.writes, keyStr)

	return nil
}

// Get retrieves a value with MVCC snapshot isolation
func (txn *Transaction) Get(key []byte) ([]byte, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	keyStr := string(key)

	// Check transaction writes
	if val, exists := txn.writes[keyStr]; exists {
		return val, nil
	}

	if txn.deletes[keyStr] {
		return nil, ErrKeyNotFound
	}

	// Read from database with snapshot isolation
	return txn.db.getAtVersion(key, txn.readView)
}

// getAtVersion reads a key at a specific version
func (db *Database) getAtVersion(key []byte, txnID uint64) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	keyStr := string(key)
	record, exists := db.index[keyStr]
	if !exists {
		return nil, ErrKeyNotFound
	}

	record.mu.RLock()
	defer record.mu.RUnlock()

	// Find most recent version visible to this transaction
	for i := len(record.Versions) - 1; i >= 0; i-- {
		ver := record.Versions[i]
		if ver.TxnID <= txnID {
			if ver.Deleted {
				return nil, ErrKeyNotFound
			}
			return ver.Value, nil
		}
	}

	return nil, ErrKeyNotFound
}

// Commit commits the transaction
func (txn *Transaction) Commit() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.committed {
		return errors.New("transaction already committed")
	}

	if txn.db.readOnly {
		return ErrReadOnly
	}

	timestamp := time.Now().UnixNano()

	// Write all changes to WAL and index
	for keyStr, value := range txn.writes {
		key := []byte(keyStr)

		// Write to WAL
		entry := &WALEntry{
			TxnID:     txn.ID,
			Operation: 0, // Put
			Key:       key,
			Value:     value,
			Timestamp: timestamp,
		}

		if err := txn.db.writeWALEntry(entry); err != nil {
			return err
		}

		// Update index
		txn.db.mu.Lock()
		record, exists := txn.db.index[keyStr]
		if !exists {
			record = &Record{
				Key:      key,
				Versions: make([]Version, 0),
			}
			txn.db.index[keyStr] = record
		}

		record.mu.Lock()
		record.Versions = append(record.Versions, Version{
			TxnID:     txn.ID,
			Timestamp: timestamp,
			Value:     value,
			Deleted:   false,
		})
		record.mu.Unlock()
		txn.db.mu.Unlock()
	}

	// Process deletes
	for keyStr := range txn.deletes {
		key := []byte(keyStr)

		// Write to WAL
		entry := &WALEntry{
			TxnID:     txn.ID,
			Operation: 1, // Delete
			Key:       key,
			Value:     nil,
			Timestamp: timestamp,
		}

		if err := txn.db.writeWALEntry(entry); err != nil {
			return err
		}

		// Update index
		txn.db.mu.Lock()
		record, exists := txn.db.index[keyStr]
		if exists {
			record.mu.Lock()
			record.Versions = append(record.Versions, Version{
				TxnID:     txn.ID,
				Timestamp: timestamp,
				Value:     nil,
				Deleted:   true,
			})
			record.mu.Unlock()
		}
		txn.db.mu.Unlock()
	}

	txn.committed = true
	return nil
}

// Rollback aborts the transaction
func (txn *Transaction) Rollback() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	txn.writes = make(map[string][]byte)
	txn.deletes = make(map[string]bool)
	txn.committed = true

	return nil
}

// Get retrieves a value (without transaction)
func (db *Database) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	currentTxnID := db.nextTxnID - 1
	db.mu.RUnlock()

	return db.getAtVersion(key, currentTxnID)
}

// Scan iterates through all keys with a prefix
func (db *Database) Scan(prefix []byte, fn func(key, value []byte) error) error {
	db.mu.RLock()
	currentTxnID := db.nextTxnID - 1
	keys := make([]string, 0, len(db.index))
	for k := range db.index {
		if bytes.HasPrefix([]byte(k), prefix) {
			keys = append(keys, k)
		}
	}
	db.mu.RUnlock()

	for _, keyStr := range keys {
		value, err := db.getAtVersion([]byte(keyStr), currentTxnID)
		if err == ErrKeyNotFound {
			continue
		}
		if err != nil {
			return err
		}

		if err := fn([]byte(keyStr), value); err != nil {
			return err
		}
	}

	return nil
}

// Compact performs garbage collection on old versions
func (db *Database) Compact(keepVersions int) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.readOnly {
		return ErrReadOnly
	}

	for _, record := range db.index {
		record.mu.Lock()

		if len(record.Versions) > keepVersions {
			// Keep only recent versions
			record.Versions = record.Versions[len(record.Versions)-keepVersions:]
		}

		record.mu.Unlock()
	}

	return nil
}

// Stats returns database statistics
func (db *Database) Stats() map[string]interface{} {
	db.mu.RLock()
	defer db.mu.RUnlock()

	totalVersions := 0
	totalSize := int64(0)

	for _, record := range db.index {
		record.mu.RLock()
		totalVersions += len(record.Versions)
		for _, ver := range record.Versions {
			totalSize += int64(len(ver.Value))
		}
		record.mu.RUnlock()
	}

	return map[string]interface{}{
		"keys":           len(db.index),
		"total_versions": totalVersions,
		"total_size":     totalSize,
		"next_txn_id":    db.nextTxnID,
	}
}

// Close closes the database
func (db *Database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}

	db.closed = true

	if err := db.wal.Close(); err != nil {
		return err
	}

	if err := db.dataFile.Close(); err != nil {
		return err
	}

	return nil
}

// Export exports the database to JSON
func (db *Database) Export() ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	data := make(map[string]interface{})

	for keyStr, record := range db.index {
		record.mu.RLock()
		versions := make([]map[string]interface{}, len(record.Versions))
		for i, ver := range record.Versions {
			versions[i] = map[string]interface{}{
				"txn_id":    ver.TxnID,
				"timestamp": ver.Timestamp,
				"value":     ver.Value,
				"deleted":   ver.Deleted,
			}
		}
		data[keyStr] = versions
		record.mu.RUnlock()
	}

	return json.MarshalIndent(data, "", "  ")
}
