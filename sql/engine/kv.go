package engine

import (
	"cabbageDB/log"
	"cabbageDB/logger"
	"cabbageDB/sql"
	"cabbageDB/sql/catalog"
	"cabbageDB/sql/expr"
	"cabbageDB/storage"
	"cabbageDB/util"
	"errors"
)

const (
	KeyPrefix      byte = 0x05
	TableKeyPrefix byte = 0x02
	IndexKeyPrefix byte = 0x03
	RowKeyPrefix   byte = 0x04
)

type TableKey struct {
	TableName string
}

type IndexKey struct {
	TableName  string
	IndexName  string
	IndexValue *sql.ValueData
}

type RowKey struct {
	TableName       string
	PrimaryKeyValue *sql.ValueData
}

type KV struct {
	KVStore *storage.MVCC
}

func NewKV(engine log.Engine) *KV {
	return &KV{
		KVStore: storage.NewMVCC(engine),
	}
}

func (kv *KV) GetMetaData(key []byte) []byte {
	return kv.KVStore.GetUnversioned(key)
}

func (kv *KV) SetMetaData(key, value []byte) {
	kv.KVStore.SetUnversioned(key, value)
}

func (kv *KV) Resume(state *storage.TransactionState) catalog.Transaction {
	return NewTransactionEngine(kv.KVStore.Resume(state))
}

func (kv *KV) Begin() catalog.Transaction {
	return NewTransactionEngine(kv.KVStore.Begin())
}

func (kv *KV) BeginReadOnly() catalog.Transaction {
	return NewTransactionEngine(kv.KVStore.BeginReadOnly())
}

func (kv *KV) BeginAsOf(version uint64) catalog.Transaction {
	return NewTransactionEngine(kv.KVStore.BeginAsOf(storage.Version(version)))
}

func (kv *KV) Session() *catalog.Session {

	return nil
}

type TransactionEngine struct {
	Txn *storage.MVCCTransactionEngine
}

func NewTransactionEngine(txn *storage.MVCCTransactionEngine) *TransactionEngine {
	return &TransactionEngine{
		Txn: txn,
	}
}

func (txnEngine *TransactionEngine) Version() uint64 {
	return uint64(txnEngine.Txn.Version())
}

func (txnEngine *TransactionEngine) ReadOnly() bool {
	return txnEngine.Txn.ReadOnly()
}
func (txnEngine *TransactionEngine) Commit() bool {
	return txnEngine.Txn.Commit()
}

func (txnEngine *TransactionEngine) Rollback() bool {
	return txnEngine.Txn.RollBack()
}
func (txnEngine *TransactionEngine) Create(tableName string, row []*sql.ValueData) error {
	// 这里很关键
	table, err := txnEngine.MustReadTable(tableName)
	if err != nil {
		return err
	}
	err = table.ValiDateRow(row, txnEngine)
	if err != nil {
		return err
	}
	id := table.GetRowKey(row)
	rowData, err := txnEngine.Read(tableName, id)
	if len(rowData) != 0 {
		return errors.New("tableName: " + tableName + " is exits primary key " + row[0].String())
	}
	if err != nil {
		return err
	}

	rowKey := RowKey{
		TableName:       table.Name,
		PrimaryKeyValue: id,
	}

	err = txnEngine.Txn.Set([2]byte{KeyPrefix, RowKeyPrefix}, util.BinaryStructToByte(&rowKey), util.BinaryStructToByte(&row))
	if err != nil {
		return err
	}
	for i, column := range table.Columns {
		if !column.Index {
			continue
		}
		index := txnEngine.IndexLoad(table.Name, column.Name, row[i])
		index[id] = struct{}{}
		txnEngine.IndexSave(table.Name, column.Name, row[i], index)

	}
	return nil
}
func (txnEngine *TransactionEngine) IndexSave(tableName string, columnName string, value *sql.ValueData, index catalog.ValueHashSet) {
	indexKey := &IndexKey{
		TableName:  tableName,
		IndexName:  columnName,
		IndexValue: value,
	}
	if len(index) == 0 {
		txnEngine.Txn.Delete([2]byte{KeyPrefix, IndexKeyPrefix}, util.BinaryStructToByte(indexKey))
	} else {
		txnEngine.Txn.Set([2]byte{KeyPrefix, IndexKeyPrefix}, util.BinaryStructToByte(indexKey), util.BinaryStructToByte(&index))
	}

}

func (txnEngine *TransactionEngine) IndexLoad(tableName string, indexName string, indexValue *sql.ValueData) catalog.ValueHashSet {

	indexKey := &IndexKey{
		TableName:  tableName,
		IndexName:  indexName,
		IndexValue: indexValue,
	}

	value := txnEngine.Txn.Get([2]byte{KeyPrefix, IndexKeyPrefix}, util.BinaryStructToByte(indexKey))

	valueHashSet := catalog.ValueHashSet{}
	util.ByteToStruct(value, &valueHashSet)
	return valueHashSet
}

func (txnEngine *TransactionEngine) Delete(tableName string, id *sql.ValueData) error {
	table, err := txnEngine.MustReadTable(tableName)
	if err != nil {
		return err
	}

	type ColumnIndex struct {
		Index int
		Name  string
	}

	for _, v := range txnEngine.TableReferences(tableName, true) {
		t, err1 := txnEngine.MustReadTable(v.TableName)
		if err1 != nil {
			return err1
		}
		references := make([]ColumnIndex, len(v.ColumnReferences))
		for _, refrenceName := range v.ColumnReferences {
			index, err2 := table.GetColumnIndex(refrenceName)
			if err2 != nil {
				return err2
			}
			if index != -1 {
				references = append(references, ColumnIndex{
					Index: index,
					Name:  refrenceName,
				})
			}
		}
		scan, err1 := txnEngine.Scan(t.Name, nil)
		if err1 != nil {
			return err1
		}
		for _, row := range scan {
			for _, columnIndex := range references {

				if sql.EqualValue(row[columnIndex.Index], id) && table.Name != t.Name || !sql.EqualValue(id, table.GetRowKey(row)) {
					return errors.New("Primary Key " + id.String() + " is referenced by table" + t.Name + " column " + columnIndex.Name)
				}

			}
		}
	}

	indexs := []*catalog.Column{}
	for _, v := range table.Columns {
		if v.Index {
			indexs = append(indexs, v)
		}
	}
	if len(indexs) > 0 {
		row, err1 := txnEngine.Read(tableName, id)
		if err1 != nil {
			return err1
		}
		for i, column := range indexs {
			index := txnEngine.IndexLoad(tableName, column.Name, row[i])
			delete(index, id)
			txnEngine.IndexSave(tableName, column.Name, row[i], index)
		}
	}

	txnEngine.Txn.Delete([2]byte{KeyPrefix, RowKeyPrefix}, util.BinaryStructToByte(&RowKey{tableName, id}))

	return nil
}
func (txnEngine *TransactionEngine) Read(tableName string, row *sql.ValueData) ([]*sql.ValueData, error) {
	valueByte := txnEngine.Txn.Get([2]byte{KeyPrefix, RowKeyPrefix}, util.BinaryStructToByte(&RowKey{tableName, row}))
	var RowValue []*sql.ValueData
	util.ByteToStruct(valueByte, &RowValue)

	return RowValue, nil
}

func (txnEngine *TransactionEngine) ReadIndex(tableName string, columnName string, value *sql.ValueData) (catalog.ValueHashSet, error) {
	table, err := txnEngine.MustReadTable(tableName)
	if err != nil {
		return nil, err
	}
	column, err1 := table.GetColumn(columnName)
	if err1 != nil {
		return nil, err1
	}
	if column != nil && !column.Index {
		return nil, errors.New("No index on " + tableName + "." + columnName)
	}
	return txnEngine.IndexLoad(tableName, columnName, value), nil

}

func (txnEngine *TransactionEngine) Scan(tableName string, filter expr.Expression) ([][]*sql.ValueData, error) {

	var rowList [][]*sql.ValueData
	table, err := txnEngine.MustReadTable(tableName)
	if table.Name == "" {
		return nil, err
	}
	itemList := txnEngine.Txn.ScanPrefix([2]byte{KeyPrefix, RowKeyPrefix})
	// 后续如果需要排序 在这里修改顺序
	orderedKeys := []string{}
	versionedRows := make(map[string]struct {
		version storage.Version
		row     []*sql.ValueData
	})

	for i := len(itemList) - 1; i >= 0; i-- {
		versiondIter := storage.DecodeKey(itemList[i].Key)
		versiond, _ := versiondIter.(*storage.Versioned)
		rowKey := RowKey{}
		util.ByteToStruct(versiond.Key, &rowKey)
		if rowKey.TableName != tableName {
			continue
		}
		var row []*sql.ValueData
		keyStr := string(versiond.Key)

		util.ByteToStruct(itemList[i].Value, &row)
		if len(row) == 0 {
			return nil, errors.New("Engine Not Found Row Value")
		}
		if filter != nil {
			valueData := filter.Evaluate(row)
			if valueData == nil {
				return nil, errors.New("Filter Row Value Error")
			}
			if !(valueData.Type == sql.BoolType && valueData.Value == true) {
				continue
			}
		}

		currentEntry, exists := versionedRows[keyStr]
		// 仅保留更高版本的数据
		if !exists || versiond.Version > currentEntry.version {
			if !exists {
				orderedKeys = append(orderedKeys, keyStr)
			}
			versionedRows[keyStr] = struct {
				version storage.Version
				row     []*sql.ValueData
			}{
				version: versiond.Version,
				row:     row,
			}
		}

	}

	for _, key := range orderedKeys {
		entry := versionedRows[key]
		rowList = append(rowList, entry.row)
	}

	return rowList, nil
}

func (txnEngine *TransactionEngine) ScanIndex(tableName string, columnName string) ([]*catalog.IndexValue, error) {
	var indexValue []*catalog.IndexValue
	table, err := txnEngine.MustReadTable(tableName)
	if err != nil {
		return nil, err
	}
	column, err1 := table.GetColumn(columnName)
	if err1 != nil {
		return nil, err1
	}
	if column == nil || !column.Index {
		logger.Info("No index for " + table.Name + "." + columnName)
	}
	itemList := txnEngine.Txn.ScanPrefix([2]byte{KeyPrefix, IndexKeyPrefix})
	for _, item := range itemList {
		var indexKey IndexKey
		util.ByteToStruct(item.Key[2:], &indexKey)
		if indexKey.TableName == tableName {
			var hashSet storage.VersionHashSet
			util.ByteToStruct(item.Value, &hashSet)
			indexValue = append(indexValue, &catalog.IndexValue{
				Value:        indexKey.IndexValue,
				ValueHashSet: hashSet,
			})
		}
	}

	return indexValue, nil
}
func (txnEngine *TransactionEngine) Update(tableName string, id *sql.ValueData, row []*sql.ValueData) error {
	table, err := txnEngine.MustReadTable(tableName)
	if err != nil {
		return err
	}

	rowValue := table.GetRowKey(row)
	if id.Type != rowValue.Type || id.Type != rowValue.Type {
		err = txnEngine.Delete(tableName, id)
		if err != nil {
			return nil
		}
		err = txnEngine.Create(tableName, row)
		if err != nil {
			return nil
		}
		return nil
	}

	indexes := []*catalog.Column{}
	for _, column := range table.Columns {
		if column.Index {
			indexes = append(indexes, column)
		}
	}

	if len(indexes) > 0 {
		// id是主键
		old, err1 := txnEngine.Read(table.Name, id)
		if err1 != nil {
			return err1
		}
		for i, column := range indexes {
			if old[i] == row[i] {
				continue
			}
			index := txnEngine.IndexLoad(table.Name, column.Name, old[i])
			delete(index, id)
			txnEngine.IndexSave(table.Name, column.Name, row[i], index)
			index = txnEngine.IndexLoad(table.Name, column.Name, row[i])
			index[id] = struct{}{}
			txnEngine.IndexSave(table.Name, column.Name, row[i], index)
		}
	}
	table.ValiDateRow(row, txnEngine)
	err = txnEngine.Txn.Set([2]byte{KeyPrefix, RowKeyPrefix}, util.BinaryStructToByte(&RowKey{
		TableName:       table.Name,
		PrimaryKeyValue: id,
	}), util.BinaryStructToByte(&row))
	if err != nil {
		return err
	}
	return nil
}

func (txnEngine *TransactionEngine) CreateTable(table *catalog.Table) error {
	if txnEngine.ReadTable(table.Name).Name != "" {
		return errors.New("Table " + table.Name + " already exists")
	}
	err := table.ValiDate(txnEngine)
	if err != nil {
		return err
	}

	key := append([]byte{KeyPrefix, TableKeyPrefix}, util.BinaryStructToByte(&TableKey{
		TableName: table.Name,
	})...)

	txnEngine.Txn.DDLSet(key, util.BinaryStructToByte(table))
	return nil
}

func (txnEngine *TransactionEngine) DeleteTable(tableName string) error {
	table, err := txnEngine.MustReadTable(tableName)
	if err != nil {
		return err
	}
	if len(txnEngine.TableReferences(table.Name, false)) > 0 {
		t := txnEngine.TableReferences(table.Name, false)[0]
		return errors.New("Table " + table.Name + " is referenced by table " + t.TableName + " column " + t.ColumnReferences[0])
	}
	scan, err1 := txnEngine.Scan(table.Name, nil)
	if err1 != nil {
		return err
	}
	for _, v := range scan {
		txnEngine.Delete(table.Name, table.GetRowKey(v))
	}

	indexScan := txnEngine.Txn.ScanPrefix([2]byte{KeyPrefix, IndexKeyPrefix})
	for _, index := range indexScan {

		storageKey := storage.DecodeKey(index.Key)
		version, ok := storageKey.(*storage.Versioned)
		if !ok {
			continue
		}

		if len(version.Key) == 0 {
			continue
		}

		var indexKey IndexKey
		util.ByteToStruct(version.Key, &indexKey)

		if indexKey.TableName != "" && (indexKey.TableName == tableName) {
			txnEngine.Txn.Delete(version.KeyPrefix, version.Key)
		}
	}

	tableKey := append([]byte{KeyPrefix, TableKeyPrefix}, util.BinaryStructToByte(&TableKey{
		TableName: tableName,
	})...)
	txnEngine.Txn.DDLDelete(tableKey)

	return nil
}

func (txnEngine *TransactionEngine) ReadTable(tableName string) *catalog.Table {

	key := append([]byte{KeyPrefix, TableKeyPrefix}, util.BinaryStructToByte(&TableKey{TableName: tableName})...)
	tableByte := txnEngine.Txn.DDLGet(key)

	table := catalog.Table{}
	util.ByteToStruct(tableByte, &table)

	return &table
}

func (txnEngine *TransactionEngine) ScanTables() []*catalog.Table {
	items := txnEngine.Txn.DDLScanPrefix([2]byte{KeyPrefix, TableKeyPrefix})
	tables := []*catalog.Table{}
	for _, v := range items {
		table := catalog.Table{}
		util.ByteToStruct(v.Value, &table)
		if table.Name != "" {
			tables = append(tables, &table)
		}
	}

	return tables
}

func (txnEngine *TransactionEngine) MustReadTable(tableName string) (*catalog.Table, error) {
	table := txnEngine.ReadTable(tableName)

	if table.Name == "" {
		return nil, errors.New("Table " + tableName + " does not exist")
	}
	return table, nil
}

func (txnEngine *TransactionEngine) TableReferences(tableName string, withSelf bool) []*catalog.TableReferences {
	tables := txnEngine.ScanTables()
	var tableReferences []*catalog.TableReferences
	for _, table := range tables {
		if withSelf || table.Name != tableName {
			tableReference := catalog.TableReferences{
				TableName:        table.Name,
				ColumnReferences: []string{},
			}
			for _, column := range table.Columns {
				if column.Reference == nil {
					continue
				}
				if column.Reference.TableName == tableName {
					tableReference.ColumnReferences = append(tableReference.ColumnReferences, column.Name)
					tableReferences = append(tableReferences, &tableReference)
				}
			}

		}
	}

	return tableReferences
}
