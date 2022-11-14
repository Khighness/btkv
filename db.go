package btkv

import (
	"io"
	"os"
	"sync"
)

// @Author KHighness
// @Update 2022-11-14

// DB 数据库结构体
type DB struct {
	indexes map[string]int64 // 哈希索引 <key, offset>
	dbFile  *DBFile          // 数据文件
	dirPath string           // 数据目录
	mu      sync.RWMutex
}

// Open 开启一个数据库实例
func Open(dirPath string) (*DB, error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	dbFile, err := NewDBFile(dirPath)
	if err != nil {
		return nil, err
	}

	db := &DB{
		indexes: make(map[string]int64),
		dbFile:  dbFile,
		dirPath: dirPath,
	}

	db.loadIndexesFromFile()
	return db, nil
}

// Set 写入数据
func (db *DB) Set(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.dbFile.Offset
	entry := NewEntry(key, value, SET)
	err = db.dbFile.Write(entry)

	db.indexes[string(key)] = offset
	return
}

// Get 读取数据
func (db *DB) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	offset, ok := db.indexes[string(key)]
	if !ok {
		return
	}

	var e *Entry
	e, err = db.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if e != nil {
		val = e.Value
	}
	return
}

func (db *DB) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	_, ok := db.indexes[string(key)]
	if !ok {
		return
	}

	e := NewEntry(key, nil, DEL)
	err = db.dbFile.Write(e)
	if err != nil {
		return
	}

	delete(db.indexes, string(key))
	return
}

// Merge 合并数据文件
func (db *DB) Merge() error {
	if db.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*Entry
		offset       int64
	)

	// 收集新的数据条目
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	// 新文件替换旧文件
	if len(validEntries) > 0 {
		mergeDBFile, err := NewMergeDBFile(db.dirPath)
		if err != nil {
			return err
		}
		defer os.Remove(mergeDBFile.File.Name())

		for _, entry := range validEntries {
			writeOff := mergeDBFile.Offset
			if err := mergeDBFile.Write(entry); err != nil {
				return err
			}

			db.indexes[string(entry.Key)] = writeOff
		}

		dbFileName := db.dbFile.File.Name()
		db.dbFile.File.Close()
		os.Remove(dbFileName)

		mergeDBFileName := mergeDBFile.File.Name()
		mergeDBFile.File.Close()
		os.Rename(mergeDBFileName, db.dirPath+string(os.PathSeparator)+FileName)

		db.dbFile = mergeDBFile
	}

	return nil
}

func (db *DB) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}

	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}

		db.indexes[string(e.Key)] = offset

		if e.Mark == DEL {
			delete(db.indexes, string(e.Key))
		}

		offset += e.GetSize()
	}
}
