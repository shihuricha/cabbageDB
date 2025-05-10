package bitcask

import (
	"bytes"
	"cabbageDB/logger"
	"cabbageDB/util"
	"encoding/gob"
	"fmt"
	"github.com/google/btree"
	"io"
	"os"
	"path/filepath"
)

// Pos代表在文件中的偏移量
// Len表示Value的长度
type ValueOffset struct {
	Pos uint64
	Len uint32
}

// KeyDir里面的一个元素,对应序列化到日志的数据结构Key->(ValuePos,ValueLen)
type ByteItem struct {
	Key   []byte
	Value *ValueOffset
}

// 将ByteItem转为原始Key->Value
type ByteMap struct {
	Key   []byte
	Value []byte
}

func (bi *ByteItem) Less(than btree.Item) bool {
	other := than.(*ByteItem)
	return bytes.Compare(bi.Key, other.Key) < 0
}

// BitCask的追加日志文件
type Log struct {
	Path string
	File *os.File
}

// 打开一个日志文件，如果文件不存在则创建一个。在文件关闭之前对其持有排他锁，如果锁已被持有则返回错误
func NewLog(path string) (*Log, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create file: %w", err)
	}

	if err = LockFileNonBlocking(file); err != nil {
		return nil, fmt.Errorf("get lockfile err: %w", err)
	}

	return &Log{
		Path: path,
		File: file,
	}, nil
}

// 扫描日志文件创建keydir
// 如果遇到从日志读取具体值时出错,可能是由不完整的写入引起,则截断
// 如果遇见其他错误,则报panic,阻止继续运行
func (log *Log) buildKeyDir() (*btree.BTree, error) {
	var lenBuf = make([]byte, 4)
	keyDir := btree.New(2)

	file := log.File

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileLen := fileInfo.Size()

	pos, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	for pos < fileLen {

		_, err = file.Read(lenBuf)

		if err != nil {
			if err == io.EOF {
				os.Truncate(log.File.Name(), pos)
				return keyDir, nil
			}
			return nil, err
		}
		var keyLen uint32
		err = util.ByteToInt(lenBuf, &keyLen)
		if err != nil {
			return nil, err
		}

		// 读取valuelen 可能是-1
		_, err = file.Read(lenBuf)
		if err != nil {
			if err == io.EOF {
				os.Truncate(log.File.Name(), pos)
				return keyDir, nil
			}
			return nil, err
		}
		var valueLenOrTombstone int32
		err = util.ByteToInt(lenBuf, &valueLenOrTombstone)
		if err != nil {
			return nil, err
		}

		// 读取key值
		key := make([]byte, keyLen)
		_, err = file.Read(key)
		if err != nil {
			if err == io.EOF {
				os.Truncate(log.File.Name(), pos)
				return keyDir, nil
			}
			return nil, err
		}
		valuePos := pos + 4 + 4 + int64(keyLen)

		if valueLenOrTombstone > 0 {
			// 跳过value seek时如果大于文件长度并不会报错
			file.Seek(int64(valueLenOrTombstone), io.SeekCurrent)

			if valuePos+int64(valueLenOrTombstone) > fileLen {
				os.Truncate(log.File.Name(), pos)
				return keyDir, nil
			}
		}
		byteItem := &ByteItem{
			Key: key,
			Value: &ValueOffset{
				Pos: uint64(valuePos),
			},
		}
		if valueLenOrTombstone > 0 {
			byteItem.Value.Len = uint32(valueLenOrTombstone)
			keyDir.ReplaceOrInsert(byteItem)
			pos = valuePos + int64(valueLenOrTombstone)
		} else {
			byteItem.Value.Len = 0
			keyDir.Delete(byteItem)
			pos = valuePos
		}

	}

	return keyDir, nil
}

// 从日志文件中读取value
func (log *Log) ReadValue(valuePos uint64, valueLen uint32) (buffer []byte, err error) {
	buffer = make([]byte, valueLen)
	_, err = log.File.Seek(int64(valuePos), io.SeekStart)
	if err != nil {
		return buffer, err
	}
	_, err = log.File.Read(buffer)
	return buffer, err
}

// 将Key->Value值追加到日志文件中，Value为nil值表示墓碑条目。它返回条目的位置和长度。
func (log *Log) writeEntry(key, value []byte) (uint64, uint32) {
	keyLen := uint32(len(key))
	valueLen := uint32(len(value))
	valueLenOrTombstone := int32(-1)
	if value != nil {
		valueLenOrTombstone = int32(len(value))
	}
	itemLen := 4 + 4 + keyLen + valueLen
	file := log.File
	pos, _ := file.Seek(0, io.SeekEnd)

	keyLenByte := util.BinaryToByte(keyLen)
	file.Write(keyLenByte)
	valueLenByte := util.BinaryToByte(valueLenOrTombstone)
	file.Write(valueLenByte)

	file.Write(key)
	file.Write(value)
	file.Sync()

	return uint64(pos), itemLen
}

// 这是一个非常简化的BitCask
// BitCask将键值对写入一个追加型的日志文件,并在内存中保持Key->(ValuePos,ValueLen)的映射关系
// 删除一个键时,会在日志追加一条表示删除的特殊标记
type BitCask struct {
	Log    *Log
	KeyDir *btree.BTree
}

// 自定义序列化方法（仅保存 Log.Path）
func (bc *BitCask) GobEncode() ([]byte, error) {
	// 定义临时结构体，仅包含需要序列化的字段
	tmp := struct {
		LogPath string // 只保留 Log 的 Path 字段
	}{
		LogPath: bc.Log.Path, // 提取 Path
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(tmp); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// 自定义反序列化方法（仅恢复 Log.Path）
func (bc *BitCask) GobDecode(data []byte) error {
	// 定义临时结构体，仅读取 Log.Path
	tmp := struct {
		LogPath string
	}{}

	decoder := gob.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&tmp); err != nil {
		return err
	}

	// 重建 Log 结构（其他字段如 File 初始化为零值）
	bc.Log = &Log{
		Path: tmp.LogPath,
		File: nil, // File 不序列化，需后续手动初始化
	}

	// KeyDir 不反序列化，需后续手动重建
	bc.KeyDir = nil

	return nil
}

// 创建一个Bitcask,并自动压缩
func NewCompact(path string, GarbageRatioThreshold float64) *BitCask {
	bitCask := NewBitCask(path)
	status := bitCask.Status()
	GarbageRatio := float64(status.GarbageDiskSize) / float64(status.TotalDiskSize)
	if status.GarbageDiskSize > 0 && GarbageRatio >= GarbageRatioThreshold {
		logger.Info("start compact")
		if err := bitCask.Compact(); err != nil {
			panic(err)
		}
	}
	return bitCask
}

// 打开或者创建一个BitCask
// 读取到不完整条目时需截断
func NewBitCask(path string) *BitCask {
	log, err := NewLog(path)
	if err != nil {
		panic(err)
	}

	keyDir, err := log.buildKeyDir()
	if err != nil {
		panic(err)
	}
	return &BitCask{
		Log:    log,
		KeyDir: keyDir,
	}
}

func (bitCask *BitCask) Compact() (err error) {

	type value struct {
		valueByte []byte
		valueLen  uint32
	}

	type itemMap struct {
		key   []byte
		value *value
	}

	tmpItemList := make([]*itemMap, 0, bitCask.KeyDir.Len())

	bitCask.KeyDir.Ascend(func(i btree.Item) bool {
		item := i.(*ByteItem)
		valueByte, err1 := bitCask.Log.ReadValue(item.Value.Pos, item.Value.Len)
		if err1 != nil {
			return false
		}
		tmpItem := &itemMap{
			key: item.Key,
			value: &value{
				valueByte: valueByte,
				valueLen:  item.Value.Len,
			},
		}
		tmpItemList = append(tmpItemList, tmpItem)
		return true
	})

	err = os.Truncate(bitCask.Log.File.Name(), 0)
	if err != nil {
		return err
	}
	_, err = bitCask.Log.File.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	for _, tmpItem := range tmpItemList {
		pos, itemLen := bitCask.Log.writeEntry(tmpItem.key, tmpItem.value.valueByte)
		bitCask.KeyDir.ReplaceOrInsert(&ByteItem{
			Key: tmpItem.key,
			Value: &ValueOffset{
				Pos: pos + uint64(itemLen) - uint64(tmpItem.value.valueLen),
				Len: tmpItem.value.valueLen,
			},
		})
	}
	return err

}

func (bitCask *BitCask) Set(key, value []byte) {
	pos, itemLen := bitCask.Log.writeEntry(key, value)
	valueLen := uint32(len(value))

	valuePos := pos + uint64(itemLen) - uint64(valueLen)
	bitCask.KeyDir.ReplaceOrInsert(&ByteItem{
		Key: key,
		Value: &ValueOffset{
			Pos: valuePos,
			Len: valueLen,
		},
	})
}

func (bitCask *BitCask) Get(key []byte) []byte {
	valueOffset := bitCask.KeyDir.Get(&ByteItem{Key: key})
	var valueByte []byte
	if valueOffset != nil {
		byteItem := valueOffset.(*ByteItem)
		valueByte, _ = bitCask.Log.ReadValue(byteItem.Value.Pos, byteItem.Value.Len)
	}
	return valueByte
}

func (bitCask *BitCask) Delete(key []byte) {
	bitCask.KeyDir.Delete(&ByteItem{
		Key: key,
	})
	bitCask.Log.writeEntry(key, nil)
}

func (bitCask *BitCask) Scan(from, to []byte) []*ByteMap {

	byteMapList := []*ByteMap{}

	// 标志 避免一直循环
	needStop := false

	bitCask.KeyDir.Ascend(func(i btree.Item) bool {
		item := i.(*ByteItem)

		// 如果to为nil 只需要判断key大于from即可
		// bytes.Compare(item.Key, from) item>from 是1 item<from是-1
		value, err := bitCask.Log.ReadValue(item.Value.Pos, item.Value.Len)
		if err != nil {
			return false
		}

		if to == nil && bytes.Compare(item.Key, from) <= 0 {
			byteMapList = append(byteMapList, &ByteMap{Key: item.Key, Value: value})
		} else if bytes.Compare(item.Key, from) >= 0 && bytes.Compare(item.Key, to) <= 0 {
			byteMapList = append(byteMapList, &ByteMap{Key: item.Key, Value: value})
			needStop = true
		} else {
			if needStop {
				return false
			}
		}

		return true
	})
	return byteMapList
}

func (bitCask *BitCask) ScanPrefix(prefix []byte) []*ByteMap {
	to := make([]byte, len(prefix))
	endSum := 3

	// 只有前缀
	if len(prefix) == 2 {
		endSum = 1
	}
	// 只有版本 Version
	if len(prefix) == 10 {
		endSum = 8
	}

	copy(to, prefix)
	flag := false

	for i := len(to) - endSum; i >= 0; i-- {
		if to[i] != 0xff {
			to[i]++
			flag = true
			break
		}
	}
	if !flag {
		return bitCask.Scan(prefix, nil)
	}
	return bitCask.Scan(prefix, to)

}

func (bitCask *BitCask) Status() *Status {
	keys := uint64(bitCask.KeyDir.Len())
	size := uint64(0)
	bitCask.KeyDir.Ascend(func(i btree.Item) bool {
		item := i.(*ByteItem)
		size = size + uint64(len(item.Key)) + uint64(item.Value.Len)
		return true
	})
	stat, _ := bitCask.Log.File.Stat()
	totalDiskSize := uint64(stat.Size())
	liveDiskSize := size + 8*keys
	garbageDiskSize := totalDiskSize - liveDiskSize
	return &Status{
		Name:            "bitcask",
		Keys:            keys,
		Size:            size,
		TotalDiskSize:   totalDiskSize,
		GarbageDiskSize: garbageDiskSize,
		LiveDiskSize:    liveDiskSize,
		FileName:        bitCask.FileName(),
	}
}

func (bitCask *BitCask) FlushFile() {
	bitCask.Log.File.Sync()
}

func (bitCask *BitCask) FileName() string {
	path, _ := filepath.Abs(bitCask.Log.File.Name())
	return path
}

func init() {
	gob.Register(&BitCask{})
}
