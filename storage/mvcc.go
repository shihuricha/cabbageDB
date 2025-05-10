package storage

import (
	"cabbageDB/bitcask"
	"cabbageDB/log"
	"cabbageDB/logger"
	"cabbageDB/util"
	"errors"
	"math"
	"sync"
)

type MVCC struct {
	Engine log.Engine
	Mu     sync.Mutex
}

func (mvcc *MVCC) Resume(state *TransactionState) *MVCCTransactionEngine {
	return MVCCTxnEngineResume(mvcc.Engine, state)
}

type Status struct {
	Version    uint64
	ActiveTxns uint64
	Storage    *bitcask.Status
}

func (mvcc *MVCC) Status() *Status {
	engine := mvcc.Engine
	nextVersion := NextVersion{}
	versionByte := engine.Get(nextVersion.MVCCEncode())
	var version uint64
	if len(versionByte) == 0 {
		version = 0
	} else {
		err := util.ByteToInt(versionByte, &version)
		if err != nil {
			logger.Info("err:", err.Error())
		}
	}
	txnActive := TxnActive{}
	activeTxns := uint64(len(engine.ScanPrefix(txnActive.MVCCEncode())))
	return &Status{
		Version:    version,
		ActiveTxns: activeTxns,
		Storage:    engine.Status(),
	}
}

func (mvcc *MVCC) GetUnversioned(key []byte) []byte {
	mvcc.Mu.Lock()
	defer mvcc.Mu.Unlock()
	versioned := &UnVersioned{Key: key}
	return mvcc.Engine.Get(versioned.MVCCEncode())
}

func (mvcc *MVCC) SetUnversioned(key, value []byte) {
	mvcc.Mu.Lock()
	defer mvcc.Mu.Unlock()
	versioned := &UnVersioned{Key: key}
	mvcc.Engine.Set(versioned.MVCCEncode(), value)
}

func (mvcc *MVCC) Begin() *MVCCTransactionEngine {
	return MVCCTxnEngineBegin(mvcc.Engine)
}

func (mvcc *MVCC) BeginReadOnly() *MVCCTransactionEngine {
	return MVCCTxnEngineBeginReadOnly(mvcc.Engine, 0)
}

func (mvcc *MVCC) BeginAsOf(version Version) *MVCCTransactionEngine {
	return MVCCTxnEngineBeginReadOnly(mvcc.Engine, version)
}

func NewMVCC(engine log.Engine) *MVCC {
	return &MVCC{
		Engine: engine,
		Mu:     sync.Mutex{},
	}
}

type MVCCTransactionEngine struct {
	Engine log.Engine
	St     *TransactionState
}

func (mvccTxnEngine *MVCCTransactionEngine) Version() Version {
	return mvccTxnEngine.St.Version
}

func (mvccTxnEngine *MVCCTransactionEngine) ReadOnly() bool {
	return mvccTxnEngine.St.ReadOnly
}

func (mvccTxnEngine *MVCCTransactionEngine) Set(keyPrefix [2]byte, key []byte, value []byte) error {
	return mvccTxnEngine.WriteVersion(keyPrefix, key, value)
}

func (mvccTxnEngine *MVCCTransactionEngine) DDLSet(key []byte, value []byte) bool {
	mvccTxnEngine.Engine.Set(key, value)
	return true
}
func (mvccTxnEngine *MVCCTransactionEngine) DDLGet(key []byte) []byte {
	return mvccTxnEngine.Engine.Get(key)
}
func (mvccTxnEngine *MVCCTransactionEngine) DDLDelete(key []byte) {
	mvccTxnEngine.Engine.Delete(key)
}

func (mvccTxnEngine *MVCCTransactionEngine) DDLScanPrefix(keyPrefix [2]byte) []*bitcask.ByteMap {
	return mvccTxnEngine.Engine.ScanPrefix(keyPrefix[:])
}

func (mvccTxnEngine *MVCCTransactionEngine) ScanPrefix(keyPrefix [2]byte) []*bitcask.ByteMap {
	from := &Versioned{
		KeyPrefix: keyPrefix,
		Version:   0,
		Key:       []byte{},
	}
	to := &Versioned{
		KeyPrefix: keyPrefix,
		Version:   mvccTxnEngine.St.Version + 1,
		Key:       []byte{},
	}

	itemList := mvccTxnEngine.Engine.Scan(from.MVCCEncode(), to.MVCCEncode())
	itemListResp := []*bitcask.ByteMap{}
	for i := len(itemList) - 1; i >= 0; i-- {
		versionKey := DecodeKey(itemList[i].Key)
		if v, ok := versionKey.(*Versioned); ok {
			if mvccTxnEngine.St.IsVisible(v.Version) {
				itemListResp = append(itemListResp, itemList[i])
			}
		}
	}
	return itemListResp
}
func (mvccTxnEngine *MVCCTransactionEngine) Delete(keyPrefix [2]byte, key []byte) bool {

	from := &Versioned{KeyPrefix: keyPrefix, Version: 0, Key: key}
	to := &Versioned{KeyPrefix: keyPrefix, Version: math.MaxInt, Key: key}
	versionList := mvccTxnEngine.Engine.Scan(from.MVCCEncode(), to.MVCCEncode())
	for _, item := range versionList {
		versioned, ok := DecodeKey(item.Key).(*Versioned)
		if !ok {
			continue
		}
		if slicesEqual(versioned.Key, key) {
			mvccTxnEngine.Engine.Delete(item.Key)
		}
	}

	return true
}
func (mvccTxnEngine *MVCCTransactionEngine) Get(keyPrefix [2]byte, key []byte) []byte {
	from := &Versioned{
		KeyPrefix: keyPrefix,
		Key:       key,
		Version:   0,
	}
	to := &Versioned{
		KeyPrefix: keyPrefix,
		Key:       key,
		Version:   mvccTxnEngine.St.Version,
	}

	itemList := mvccTxnEngine.Engine.Scan(from.MVCCEncode(), to.MVCCEncode())

	for i := len(itemList) - 1; i >= 0; i-- {
		versionKey := DecodeKey(itemList[i].Key)
		v, ok := versionKey.(*Versioned)
		if !ok {
			continue
		}

		if mvccTxnEngine.St.IsVisible(v.Version) {
			if slicesEqual(v.Key, key) {
				return itemList[i].Value
			}
		}

	}
	return []byte{}
}

func slicesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (mvccTxnEngine *MVCCTransactionEngine) WriteVersion(keyPrefix [2]byte, key []byte, value []byte) error {
	if mvccTxnEngine.St.ReadOnly {
		return nil
	}

	version := mvccTxnEngine.St.Version + 1

	for tmpVersion, _ := range mvccTxnEngine.St.Active {
		if tmpVersion < version {
			version = tmpVersion
		}
	}
	from := &Versioned{KeyPrefix: keyPrefix, Version: version, Key: key}
	to := &Versioned{KeyPrefix: keyPrefix, Version: math.MaxInt, Key: key}
	versionList := mvccTxnEngine.Engine.Scan(from.MVCCEncode(), to.MVCCEncode())

	for _, v := range versionList {
		versionByte := DecodeKey(v.Key)
		if versioned, ok := versionByte.(*Versioned); ok {
			if !mvccTxnEngine.St.IsVisible(versioned.Version) {
				return errors.New("Error Serialization")
			}
		}
	}

	txnWrite := &TxnWrite{
		Version: mvccTxnEngine.St.Version,
		Key:     append(keyPrefix[:], key...),
	}
	mvccTxnEngine.Engine.Set(txnWrite.MVCCEncode(), []byte{'1'})

	versioned := &Versioned{
		KeyPrefix: keyPrefix,
		Key:       key,
		Version:   mvccTxnEngine.St.Version,
	}

	mvccTxnEngine.Engine.Set(versioned.MVCCEncode(), value)

	return nil
}

func (mvccTxnEngine *MVCCTransactionEngine) Commit() bool {
	if mvccTxnEngine.St.ReadOnly {
		return true
	}
	fromTxnWrite := TxnWrite{
		Version: mvccTxnEngine.St.Version,
	}
	toTxnWrite := TxnWrite{
		Version: mvccTxnEngine.St.Version + 1,
	}
	removeKV := mvccTxnEngine.Engine.Scan(fromTxnWrite.MVCCEncode(), toTxnWrite.MVCCEncode())
	for _, v := range removeKV {
		mvccTxnEngine.Engine.Delete(v.Key)
	}
	txnActive := TxnActive{
		Version: mvccTxnEngine.St.Version,
	}
	mvccTxnEngine.Engine.Delete(txnActive.MVCCEncode())

	return true
}

func (mvccTxnEngine *MVCCTransactionEngine) RollBack() bool {
	if mvccTxnEngine.St.ReadOnly {
		return true
	}

	fromTxnWrite := TxnWrite{
		Version: mvccTxnEngine.St.Version,
	}
	toTxnWrite := TxnWrite{
		Version: mvccTxnEngine.St.Version + 1,
	}

	scan := mvccTxnEngine.Engine.Scan(fromTxnWrite.MVCCEncode(), toTxnWrite.MVCCEncode())
	for _, v := range scan {
		key := DecodeKey(v.Key)
		if txnWrite, ok := key.(*TxnWrite); ok {
			versionByte := &Versioned{
				Version:   mvccTxnEngine.St.Version,
				Key:       txnWrite.Key[2:],
				KeyPrefix: [2]byte{txnWrite.Key[0], txnWrite.Key[1]},
			}
			mvccTxnEngine.Engine.Delete(versionByte.MVCCEncode())
		} else {
			return false
		}
		mvccTxnEngine.Engine.Delete(v.Key)
	}

	txnActive := TxnActive{
		Version: mvccTxnEngine.St.Version,
	}
	mvccTxnEngine.Engine.Delete(txnActive.MVCCEncode())
	return true
}

func MVCCTxnEngineBegin(engine log.Engine) *MVCCTransactionEngine {
	nextVersion := NextVersion{}
	versionByte := engine.Get(nextVersion.MVCCEncode())
	var versionValue uint64
	versionValue = 1
	if len(versionByte) == 0 {
		versionValue = uint64(1)
	} else {
		_ = util.ByteToInt(versionByte, &versionValue)
	}
	newVersionByte := util.BinaryToByte(versionValue + 1)
	engine.Set(nextVersion.MVCCEncode(), newVersionByte)

	active := ScanActive(engine)
	if len(active) > 0 {
		txnActiveSnap := TxnActiveSnapshot{Version: Version(versionValue)}
		engine.Set(txnActiveSnap.MVCCEncode(), util.BinaryStructToByte(&active))
	}
	txnActive := TxnActive{
		Version: Version(versionValue),
	}
	engine.Set(txnActive.MVCCEncode(), []byte{'1'})
	return &MVCCTransactionEngine{
		Engine: engine,
		St: &TransactionState{
			Version:  Version(versionValue),
			ReadOnly: false,
			Active:   active,
		},
	}
}

func MVCCTxnEngineBeginReadOnly(engine log.Engine, asOf Version) *MVCCTransactionEngine {
	nextVersion := NextVersion{}
	versionByte := engine.Get(nextVersion.MVCCEncode())
	var versionValue uint64
	if len(versionByte) == 0 {
		versionValue = 1
	} else {
		_ = util.ByteToInt(versionByte, &versionValue)
	}
	var active VersionHashSet
	if asOf != 0 {
		if uint64(asOf) >= versionValue {
			logger.Info(" specified version>last version,return last data")
		}
		versionValue = uint64(asOf)
		txnActiveSnap := TxnActiveSnapshot{
			Version: Version(versionValue),
		}
		txnActiveSnapVersionByte := engine.Get(txnActiveSnap.MVCCEncode())
		snapVersion := DecodeKey(txnActiveSnapVersionByte)
		if _, ok := snapVersion.(*TxnActiveSnapshot); ok {
			util.ByteToStruct(txnActiveSnapVersionByte, &active)
		}
	} else {
		active = ScanActive(engine)

	}

	return &MVCCTransactionEngine{Engine: engine, St: &TransactionState{
		Version:  Version(versionValue),
		ReadOnly: true,
		Active:   active,
	}}
}

func ScanActive(session log.Engine) VersionHashSet {
	active := VersionHashSet{}
	txnActive := TxnActive{}
	scan := session.ScanPrefix(txnActive.MVCCEncode())
	for _, v := range scan {
		txnActiveInterface := DecodeKey(v.Key)

		txnActive1, ok := txnActiveInterface.(*TxnActive)
		if ok {
			active[txnActive1.Version] = struct{}{}
		}
	}
	return active
}

func MVCCTxnEngineResume(engine log.Engine, state *TransactionState) *MVCCTransactionEngine {
	active := TxnActive{state.Version}
	value := engine.Get(active.MVCCEncode())
	if !state.ReadOnly && len(value) == 0 {
		logger.Info("No active transaction at version ", state.Version)
		return nil
	}
	return &MVCCTransactionEngine{
		Engine: engine,
		St:     state,
	}
}
