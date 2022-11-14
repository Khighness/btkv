package btkv

import (
	"os"
)

// @Author KHighness
// @Update 2022-11-14

const (
	FileName      = "bt.kv"
	MergeFileName = "bt.mg"
)

// DBFile 数据库文件结构体
type DBFile struct {
	File   *os.File
	Offset int64
}

func newInternal(fileName string) (*DBFile, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}

	return &DBFile{Offset: stat.Size(), File: file}, nil
}

// NewDBFile 创建一个新的数据库文件
func NewDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + FileName
	return newInternal(fileName)
}

// NewDBFile 创建一个合并的数据库文件
func NewMergeDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + MergeFileName
	return newInternal(fileName)
}

// Read 从DBFile的指定非偏移量开始读取数据
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	buf := make([]byte, entryHeaderSize)
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}

	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}

	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}
	return
}

// Write 向DBFile追加数据
func (df *DBFile) Write(e *Entry) error {
	buf, err := Encode(e)
	if err != nil {
		return err
	}
	_, err = df.File.WriteAt(buf, df.Offset)
	df.Offset += e.GetSize()
	return nil
}
