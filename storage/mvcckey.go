package storage

import (
	"bytes"
	"cabbageDB/util"
	"encoding/gob"
)

type Key interface {
	MVCCEncode() []byte
}

type Version uint64

func DecodeKey(key []byte) Key {
	if len(key) == 0 {
		return nil
	}

	if key[0] != MVCCKeyPrefix {
		return nil
	}

	switch key[1] {
	case NextVersionPrefix:
		return &NextVersion{}
	case TxnActivePrefix:
		txnActive := TxnActive{}
		var versionNum uint64
		util.ByteToInt(key[2:], &versionNum)
		txnActive.Version = Version(versionNum)
		return &txnActive
	case TxnActiveSnapshotPrefix:
		txnActiveSnap := TxnActiveSnapshot{}
		var versionNum uint64
		util.ByteToInt(key[2:], &versionNum)
		txnActiveSnap.Version = Version(versionNum)
		return &txnActiveSnap
	case TxnWritePrefix:
		txnWrite := TxnWrite{}
		var versionNum uint64
		util.ByteToInt(key[2:10], &versionNum)
		txnWrite.Version = Version(versionNum)
		txnWrite.Key = key[10:]
		return &txnWrite
	case VersionedPrefix:
		version := Versioned{}
		version.KeyPrefix = [2]byte{key[2], key[3]}
		var versionNum uint64
		util.ByteToInt(key[4:12], &versionNum)
		version.Version = Version(versionNum)
		version.Key = key[12:]
		return &version
	case UnVersionedPrefix:
		unVersion := UnVersioned{}
		unVersion.Key = key[2:]
		return &unVersion
	}

	return nil
}

const (
	MVCCKeyPrefix           byte = 0x03
	NextVersionPrefix       byte = 0x02
	TxnActivePrefix         byte = 0x03
	TxnActiveSnapshotPrefix byte = 0x04
	TxnWritePrefix          byte = 0x05
	VersionedPrefix         byte = 0x06
	UnVersionedPrefix       byte = 0x07
)

type NextVersion struct{}

func (n *NextVersion) MVCCEncode() []byte {
	return []byte{MVCCKeyPrefix, NextVersionPrefix}
}

type TxnActive struct {
	Version Version
}

func (t *TxnActive) MVCCEncode() []byte {
	return append([]byte{MVCCKeyPrefix, TxnActivePrefix}, util.BinaryToByte(uint64(t.Version))...)
}

type TxnActiveSnapshot struct {
	Version Version
}

func (t *TxnActiveSnapshot) MVCCEncode() []byte {
	return append([]byte{MVCCKeyPrefix, TxnActiveSnapshotPrefix}, util.BinaryToByte(uint64(t.Version))...)
}

type TxnWrite struct {
	Version Version
	Key     []byte
}

func (t *TxnWrite) MVCCEncode() []byte {
	versionByte := util.BinaryToByte(uint64(t.Version))
	valueByte := append(versionByte, t.Key...)
	return append([]byte{MVCCKeyPrefix, TxnWritePrefix}, valueByte...)
}

type Versioned struct {
	KeyPrefix [2]byte
	Version   Version
	Key       []byte
}

// 这里自定义编码 避免因为长度问题,导致遍历失败
func (v *Versioned) MVCCEncode() []byte {
	byte1 := append([]byte{MVCCKeyPrefix, VersionedPrefix}, v.KeyPrefix[:]...)
	byte1 = append(byte1, util.BinaryToByte(uint64(v.Version))...)
	byte1 = append(byte1, v.Key...)
	return byte1
}

type UnVersioned struct {
	Key []byte
}

func (u *UnVersioned) MVCCEncode() []byte {
	return append([]byte{MVCCKeyPrefix, UnVersionedPrefix}, u.Key...)
}

func GobReg() {

	gob.Register(&Versioned{})
	gob.Register(&UnVersioned{})
	gob.Register(Version(0))
	gob.Register(&NextVersion{})
	gob.Register(&TxnActive{})
	gob.Register(&TxnActiveSnapshot{})
	gob.Register(&TxnWrite{})
	gob.Register(&TransactionState{})
	gob.Register(&VersionHashSet{})

	// 预先编码保证顺序不变
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(&Versioned{})
	_ = enc.Encode(&UnVersioned{})
	_ = enc.Encode(Version(0))
	_ = enc.Encode(&NextVersion{})
	_ = enc.Encode(&TxnActive{})
	_ = enc.Encode(&TxnActiveSnapshot{})
	_ = enc.Encode(&TxnWrite{})
	_ = enc.Encode(&TransactionState{})
	_ = enc.Encode(&VersionHashSet{})

}
