package log

import (
	"cabbageDB/bitcask"
	"cabbageDB/logger"
	"cabbageDB/util"
)

type Index uint64
type Term uint64

const (
	LogKeyPrefix      byte = 0x02
	EntryPrefix       byte = 0x02
	TermVotePrefix    byte = 0x03
	CommitIndexPrefix byte = 0x04
)

// 一个日志条目
type Entry struct {
	Index   Index  `json:"index"`   //条目索引
	Term    Term   `json:"term"`    //条目被添加时所在的任期
	Command []byte `json:"command"` //状态机命令
}

// 一个Raft Log
type RaftLog struct {
	Engine      Engine //底层存储引擎
	LastIndex   Index  //最后存储条目的索引
	LastTerm    Term   //最后存储条目的任期编号
	CommitIndex Index  //最后提交条目的索引
	CommitTerm  Term   //最后提交条目的任期编号
}

func NewRaftLog(engine Engine) *RaftLog {
	byteMapList := engine.ScanPrefix([]byte{LogKeyPrefix, EntryPrefix})

	var lastIndex Index
	var lastTerm Term
	if len(byteMapList) > 0 {

		var byteMap *bitcask.ByteMap
		if len(byteMapList) > 1 {
			// 当只有前缀的时候由于在ScanPrefix里面他会将前缀+1 即将2,2变成2,3 并且scan是包括左右边界 因此需要倒数第二个
			byteMap = byteMapList[len(byteMapList)-2]
		} else {
			byteMap = byteMapList[len(byteMapList)-1]
		}

		lastEntry := DecodeEntry(byteMap.Key, byteMap.Value)
		lastIndex, lastTerm = lastEntry.Index, lastEntry.Term
	} else {
		lastIndex, lastTerm = 0, 0
	}

	commitEntryByte := engine.Get([]byte{LogKeyPrefix, CommitIndexPrefix})
	commitEntry := DecodeCommitEntry(commitEntryByte)
	commitIndex, commitTerm := commitEntry.Index, commitEntry.Term
	return &RaftLog{
		Engine:      engine,
		LastIndex:   lastIndex,
		LastTerm:    lastTerm,
		CommitIndex: commitIndex,
		CommitTerm:  commitTerm,
	}

}

func (log *RaftLog) SetTerm(term Term, votedFor NodeID) {
	termByte := util.BinaryToByte(uint64(term))
	votedForByte := util.BinaryToByte(votedFor)
	termByte = append(termByte, votedForByte...)

	log.Engine.Set([]byte{LogKeyPrefix, TermVotePrefix}, termByte)
}

func (log *RaftLog) Get(index Index) *Entry {
	keyByte := append([]byte{LogKeyPrefix, EntryPrefix}, util.BinaryToByte(uint64(index))...)
	valueByte := log.Engine.Get(keyByte)
	if len(valueByte) == 0 {
		return nil
	}
	return log.DecodeEntryValue(index, valueByte)
}
func (log *RaftLog) Commit(index Index) Index {
	if index < log.CommitIndex {
		logger.Info("Commit index regression ", log.CommitIndex, " -> ", index)
		return 0
	}
	entry := log.Get(index)
	if entry == nil {
		logger.Info("Can't commit non-existant index ", index)
		return 0
	}
	log.Engine.Set([]byte{LogKeyPrefix, CommitIndexPrefix}, util.BinaryStructToByte(entry))
	log.CommitIndex = entry.Index
	log.CommitTerm = entry.Term
	return index

}

func (log *RaftLog) GetLastIndex() (Index, Term) {
	return log.LastIndex, log.LastTerm
}
func (log *RaftLog) Status() *bitcask.Status {
	return log.Engine.Status()
}

func (log *RaftLog) GetCommitIndex() (Index, Term) {
	return log.CommitIndex, log.CommitTerm
}

func (log *RaftLog) GetTerm() (Term, NodeID) {
	valueByte := log.Engine.Get([]byte{LogKeyPrefix, TermVotePrefix})
	if len(valueByte) == 0 {
		return 0, 0
	}
	var term uint64
	util.ByteToInt(valueByte[:8], &term)
	var nodeID uint8
	util.ByteToInt(valueByte[8:], &nodeID)

	return Term(term), nodeID
}

func (log *RaftLog) Scan(from, to Index, includeFrom, includeTo bool) []*Entry {

	if !includeFrom {
		from += 1
	}
	if !includeTo {
		to -= 1
	}

	fromKey := append([]byte{LogKeyPrefix, EntryPrefix}, util.BinaryToByte(uint64(from))...)
	toKey := append([]byte{LogKeyPrefix, EntryPrefix}, util.BinaryToByte(uint64(to))...)

	items := log.Engine.Scan(fromKey, toKey)
	entryList := make([]*Entry, len(items))
	for i, item := range items {
		entryList[i] = log.DecodeEntry(item.Key, item.Value)
	}
	return entryList

}

func (log *RaftLog) DecodeEntry(key []byte, value []byte) *Entry {

	if key[0] == LogKeyPrefix && key[1] == EntryPrefix {
		var index uint64
		util.ByteToInt(key[2:], &index)
		return log.DecodeEntryValue(Index(index), value)

	} else {
		logger.Info("Invalid key error")
		return nil
	}

}

func (log *RaftLog) DecodeEntryValue(index Index, value []byte) *Entry {
	if len(value) == 0 {
		return &Entry{
			Index:   index,
			Term:    0,
			Command: []byte{},
		}
	}
	var term uint64
	util.ByteToInt(value[:8], &term)
	return &Entry{
		Index:   index,
		Term:    Term(term),
		Command: value[8:],
	}
}

func (log *RaftLog) Append(term Term, command []byte) Index {
	index := log.LastIndex + 1
	indexByte := append([]byte{LogKeyPrefix, EntryPrefix}, util.BinaryToByte(uint64(index))...)

	valueByte := util.BinaryToByte(uint64(term))
	log.Engine.Set(indexByte, append(valueByte, command...))
	//log.Engine.FlushFile()
	log.LastIndex = index
	log.LastTerm = term
	return index
}
func (log *RaftLog) Has(index Index, term Term) bool {
	entry := log.Get(index)
	if entry == nil {
		return false
	}
	if entry.Term == term {
		return true
	}
	if index == 0 && term == 0 {
		return true
	}
	return false
}
func (log *RaftLog) Splice(entries []*Entry) Index {
	enlen := len(entries)
	if enlen == 0 {
		return log.LastIndex
	}
	lastentry := entries[enlen-1]
	lastIndex := lastentry.Index
	lastTerm := lastentry.Term

	scan := log.Scan(entries[0].Index, lastIndex, true, true)

	// 如果在引擎里需要删掉已处理的
	for _, entry := range scan {
		if entry.Term != entries[0].Term {
			break
		}
		entries = entries[1:]
	}

	for _, e := range entries {
		valueByte := append(util.BinaryToByte(uint64(e.Term)), e.Command...)
		log.Engine.Set(append([]byte{LogKeyPrefix, EntryPrefix}, util.BinaryToByte(uint64(e.Index))...), valueByte)
	}
	for index := log.LastIndex + 1; index <= log.LastIndex; index++ {
		keyByte := append([]byte{LogKeyPrefix, EntryPrefix}, util.BinaryToByte(uint64(index))...)
		log.Engine.Delete(keyByte)
	}

	//log.Engine.FlushFile()
	log.LastIndex = lastIndex
	log.LastTerm = lastTerm
	return log.LastIndex
}

type Engine interface {
	Delete(key []byte)
	Get(key []byte) []byte
	Scan(from, to []byte) []*bitcask.ByteMap
	ScanPrefix(prefix []byte) []*bitcask.ByteMap
	Set(key, value []byte)
	Status() *bitcask.Status
	FlushFile()
	FileName() string
}

func DecodeEntry(key, value []byte) *Entry {

	var index, term uint64
	util.ByteToInt(key[2:], &index)
	util.ByteToInt(value[:8], &term)

	entry := Entry{
		Command: value[8:],
		Term:    Term(term),
		Index:   Index(index),
	}
	return &entry
}

// 从日志获取的Value中解码一个条目
func DecodeCommitEntry(value []byte) *Entry {
	entry := Entry{}

	util.ByteToStruct(value, &entry)
	return &entry
}
