package engine

import (
	"bytes"
	"cabbageDB/log"
	"cabbageDB/sql"
	"cabbageDB/sql/catalog"
	"cabbageDB/sql/expr"
	"cabbageDB/storage"
	"cabbageDB/util"
	"encoding/gob"
	"errors"
)

const (
	QueryPrefix           = 0x06
	QueryStatusPrefix     = 0x02
	QueryReadPrefix       = 0x03
	QueryReadIndexPrefix  = 0x04
	QueryScanPrefix       = 0x05
	QueryScanIndexPrefix  = 0x06
	QueryScanTablesPrefix = 0x07
	QueryReadTablePrefix  = 0x08
)

type Query interface {
	QueryEncode() []byte
}

type QueryStatus struct{}

func (q *QueryStatus) QueryEncode() []byte {
	return []byte{QueryPrefix, QueryStatusPrefix}
}

type QueryRead struct {
	Txn       *storage.TransactionState
	TableName string
	ID        *sql.ValueData
}

func (q *QueryRead) QueryEncode() []byte {
	return append([]byte{QueryPrefix, QueryReadPrefix}, util.BinaryStructToByte(q)...)
}

type QueryReadIndex struct {
	Txn       *storage.TransactionState
	TableName string
	Column    string
	Value     *sql.ValueData
}

func (q *QueryReadIndex) QueryEncode() []byte {
	return append([]byte{QueryPrefix, QueryReadIndexPrefix}, util.BinaryStructToByte(q)...)
}

type QueryScan struct {
	Txn       *storage.TransactionState
	TableName string
	Filter    expr.Expression
}

func (q *QueryScan) QueryEncode() []byte {
	return append([]byte{QueryPrefix, QueryScanPrefix}, util.BinaryStructToByte(q)...)
}

type QueryScanIndex struct {
	Txn       *storage.TransactionState
	TableName string
	Column    string
}

func (q *QueryScanIndex) QueryEncode() []byte {
	return append([]byte{QueryPrefix, QueryScanIndexPrefix}, util.BinaryStructToByte(q)...)
}

type QueryScanTables struct {
	Txn *storage.TransactionState
}

func (q *QueryScanTables) QueryEncode() []byte {
	return append([]byte{QueryPrefix, QueryScanTablesPrefix}, util.BinaryStructToByte(q)...)
}

type QueryReadTable struct {
	Txn       *storage.TransactionState
	TableName string
}

func (q *QueryReadTable) QueryEncode() []byte {
	return append([]byte{QueryPrefix, QueryReadTablePrefix}, util.BinaryStructToByte(q)...)
}

type Client struct {
	Tx chan<- log.RaftMessage
}

func (c *Client) Mutate(mutation Mutation, returnTsFlag bool) (*storage.TransactionState, error) {
	mutationByte := mutation.MutationEncode()
	resp := c.Execute(&log.RaftMutate{
		Command: mutationByte,
	})

	if mutate, ok := resp.(*log.RaftMutate); ok {
		txnState := storage.TransactionState{}
		if returnTsFlag {
			util.ByteToStruct(mutate.Command, &txnState)
		}
		return &txnState, nil
	}

	if err, ok := resp.(*log.RaftError); ok {
		return nil, err.Errmsg
	}

	return nil, errors.New("Mutate Invaild return")
}
func (c *Client) Query(query Query) ([]byte, error) {
	resp := c.Execute(&log.RaftQuery{
		Command: query.QueryEncode(),
	})

	if q, ok := resp.(*log.RaftQuery); ok {
		return q.Command, nil
	}

	if err, ok := resp.(*log.RaftError); ok {
		return nil, err.Errmsg
	}

	return nil, errors.New("Query Invaild return")
}

func (c *Client) Execute(request log.Request) log.Response {
	responseChan := make(chan log.Response, 1)
	c.Tx <- log.RaftMessage{
		Request:    request,
		ResponseTx: responseChan,
	}

	return <-responseChan
}
func (c *Client) Status() *log.NodeStatus {
	resp := c.Execute(&log.RaftStatus{})
	if v, ok := resp.(*log.RaftStatus); ok {
		return v.Status
	}
	return nil
}

func NewClient(tx chan<- log.RaftMessage) *Client {
	return &Client{
		Tx: tx,
	}
}

type Raft struct {
	Client *Client
}

func (r *Raft) Status() *catalog.Status {

	resp, _ := r.Client.Query(&QueryStatus{})
	status := storage.Status{}
	util.ByteToStruct(resp, &status)

	return &catalog.Status{
		RaftStatus: r.Client.Status(),
		MVCCStatus: &status,
	}
}

type ClientTransaction struct {
	Client *Client
	State  *storage.TransactionState
}

func NewClientTransaction(client *Client, readOnly bool, asOf uint64) *ClientTransaction {
	state, _ := client.Mutate(&Begin{
		ReadOnly: readOnly,
		AsOf:     asOf,
	}, true)
	return &ClientTransaction{
		Client: client,
		State:  state,
	}
}

func (c *ClientTransaction) Version() uint64 {
	return uint64(c.State.Version)
}

func (c *ClientTransaction) ReadOnly() bool {
	return c.State.ReadOnly
}
func (c *ClientTransaction) Commit() bool {
	c.Client.Mutate(&Commit{Txn: c.State}, false)
	return true
}
func (c *ClientTransaction) Rollback() bool {
	c.Client.Mutate(&Rollback{Txn: c.State}, false)
	return true
}
func (c *ClientTransaction) Create(table string, row []*sql.ValueData) error {
	_, err := c.Client.Mutate(&Create{
		Txn:   c.State,
		Table: table,
		Row:   row,
	}, false)
	if err != nil {
		return err
	}
	return nil

}
func (c *ClientTransaction) Delete(table string, id *sql.ValueData) error {
	_, err := c.Client.Mutate(&Delete{
		Txn:   c.State,
		Table: table,
		Value: id,
	}, false)
	if err != nil {
		return err
	}
	return nil
}

func (c *ClientTransaction) Read(table string, id *sql.ValueData) ([]*sql.ValueData, error) {
	resp, err := c.Client.Query(&QueryRead{
		Txn:       c.State,
		TableName: table,
		ID:        id,
	})
	if err != nil {
		return nil, err
	}

	var data []*sql.ValueData
	util.ByteToStruct(resp, &data)
	return data, nil
}
func (c *ClientTransaction) ReadIndex(table string, column string, value *sql.ValueData) (catalog.ValueHashSet, error) {
	resp, err := c.Client.Query(&QueryReadIndex{
		Txn:       c.State,
		TableName: table,
		Column:    column,
		Value:     value,
	})
	if err != nil {
		return nil, err
	}
	var data catalog.ValueHashSet
	util.ByteToStruct(resp, &data)
	return data, nil
}
func (c *ClientTransaction) Scan(table string, filter expr.Expression) ([][]*sql.ValueData, error) {
	resp, err := c.Client.Query(&QueryScan{
		Txn:       c.State,
		TableName: table,
		Filter:    filter,
	})
	if err != nil {
		return nil, err
	}

	var data [][]*sql.ValueData
	util.ByteToStruct(resp, &data)
	return data, nil
}
func (c *ClientTransaction) ScanIndex(table string, column string) ([]*catalog.IndexValue, error) {
	resp, err := c.Client.Query(&QueryScanIndex{
		Txn:       c.State,
		TableName: table,
		Column:    column,
	})
	if err != nil {
		return nil, err
	}
	var data []*catalog.IndexValue
	util.ByteToStruct(resp, &data)
	return data, nil
}
func (c *ClientTransaction) Update(table string, id *sql.ValueData, row []*sql.ValueData) error {
	_, err := c.Client.Mutate(&Update{
		Txn:   c.State,
		Table: table,
		ID:    id,
		Row:   row,
	}, false)
	if err != nil {
		return err
	}
	return nil
}

func (c *ClientTransaction) CreateTable(table *catalog.Table) error {
	_, err := c.Client.Mutate(&CreateTable{
		Txn:    c.State,
		Schema: table,
	}, false)
	if err != nil {
		return err
	}
	return nil
}
func (c *ClientTransaction) DeleteTable(tableName string) error {
	_, err := c.Client.Mutate(&DeleteTable{
		Txn:   c.State,
		Table: tableName,
	}, false)
	if err != nil {
		return err
	}
	return nil
}
func (c *ClientTransaction) ReadTable(tableName string) *catalog.Table {
	resp, _ := c.Client.Query(
		&QueryReadTable{
			Txn:       c.State,
			TableName: tableName,
		})

	var table catalog.Table
	util.ByteToStruct(resp, &table)
	return &table
}
func (c *ClientTransaction) ScanTables() []*catalog.Table {
	resp, _ := c.Client.Query(
		&QueryScanTables{
			Txn: c.State,
		})

	data := []*catalog.Table{}
	util.ByteToStruct(resp, &data)
	return data
}
func (c *ClientTransaction) MustReadTable(tableName string) (*catalog.Table, error) {
	table := c.ReadTable(tableName)
	if table == nil || table.Name == "" {
		return nil, errors.New("Table " + tableName + " does not exist")
	}

	return table, nil
}
func (c *ClientTransaction) TableReferences(tableName string, withSelf bool) []*catalog.TableReferences {
	tables := c.ScanTables()
	var tableReferences []*catalog.TableReferences
	for _, table := range tables {
		if withSelf || table.Name != tableName {
			tableReference := catalog.TableReferences{
				TableName:        tableName,
				ColumnReferences: make([]string, len(table.Columns)),
			}
			for _, column := range table.Columns {
				if column.Reference.TableName == tableName {
					tableReference.ColumnReferences = append(tableReference.ColumnReferences, column.Name)
				}
				tableReferences = append(tableReferences, &tableReference)
			}

		}
	}

	return tableReferences

}

func (r *Raft) Begin() catalog.Transaction {
	return NewClientTransaction(r.Client, false, 0)
}

func (r *Raft) BeginReadOnly() catalog.Transaction {
	return NewClientTransaction(r.Client, true, 0)
}

func (r *Raft) BeginAsOf(version uint64) catalog.Transaction {
	return NewClientTransaction(r.Client, true, version)
}

func (r *Raft) Session() *catalog.Session {
	return &catalog.Session{
		Engine: r,
		Txn:    nil,
	}
}

func NewRaft(tx chan<- log.RaftMessage) *Raft {
	return &Raft{
		Client: NewClient(tx),
	}
}

type State struct {
	Engine       *KV
	AppliedIndex uint64
}

func NewState(engine log.Engine) *State {

	KVStorageengine := NewKV(engine)
	appliedIndexByte := KVStorageengine.GetMetaData([]byte("appliedIndex"))
	var appliedIndex uint64
	util.ByteToInt(appliedIndexByte, &appliedIndex)
	return &State{
		Engine:       KVStorageengine,
		AppliedIndex: appliedIndex,
	}
}

func (s *State) GetAppliedIndex() uint64 {
	return s.AppliedIndex
}

func (s *State) Apply(entry *log.Entry) ([]byte, error) {
	if uint64(entry.Index) != s.AppliedIndex+1 {
		panic("entry index not after applied index")
	}

	result, err := s.Mutate(entry.Command)
	s.AppliedIndex = uint64(entry.Index)
	indexByte := util.BinaryToByte(uint64(entry.Index))
	s.Engine.SetMetaData([]byte("appliedIndex"), indexByte)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (s *State) Query(command []byte) ([]byte, error) {
	if len(command) == 0 || command[0] != QueryPrefix {
		return nil, errors.New("query command protocol validation failed: invalid packet header")
	}
	switch command[1] {
	case QueryReadPrefix:
		read := QueryRead{}
		util.ByteToStruct(command[2:], &read)
		row, err := s.Engine.Resume(read.Txn).Read(read.TableName, read.ID)
		if err != nil {
			return nil, err
		}
		return util.BinaryStructToByte(&row), nil
	case QueryReadIndexPrefix:
		readIndex := QueryReadIndex{}
		util.ByteToStruct(command[2:], &readIndex)

		valueSet, err := s.Engine.Resume(readIndex.Txn).ReadIndex(readIndex.TableName, readIndex.Column, readIndex.Value)
		if err != nil {
			return nil, err
		}
		return util.BinaryStructToByte(&valueSet), nil
	case QueryScanPrefix:
		scan := QueryScan{}
		util.ByteToStruct(command[2:], &scan)
		rowList, err := s.Engine.Resume(scan.Txn).Scan(scan.TableName, scan.Filter)
		if err != nil {
			return nil, err
		}
		return util.BinaryStructToByte(&rowList), nil
	case QueryScanIndexPrefix:
		scanIndex := QueryScanIndex{}
		util.ByteToStruct(command[2:], &scanIndex)
		indexValue, err := s.Engine.Resume(scanIndex.Txn).ScanIndex(scanIndex.TableName, scanIndex.Column)
		if err != nil {
			return nil, err
		}
		return util.BinaryStructToByte(&indexValue), nil
	case QueryStatusPrefix:
		return util.BinaryStructToByte(s.Engine.KVStore.Status()), nil
	case QueryReadTablePrefix:
		readTable := QueryReadTable{}
		util.ByteToStruct(command[2:], &readTable)

		table := s.Engine.Resume(readTable.Txn).ReadTable(readTable.TableName)
		return util.BinaryStructToByte(table), nil
	case QueryScanTablesPrefix:
		scanTables := QueryScanTables{}
		util.ByteToStruct(command[2:], &scanTables)

		tables := s.Engine.Resume(scanTables.Txn).ScanTables()
		return util.BinaryStructToByte(&tables), nil
	}
	return nil, errors.New("query command protocol validation failed: invalid packet header")
}

func (s *State) Mutate(command []byte) ([]byte, error) {

	if len(command) == 0 || command[0] != MutationPrefix {
		return nil, errors.New("command is nil")
	}
	switch command[1] {
	case BeginPrefix:
		begin := Begin{}
		util.ByteToStruct(command[2:], &begin)
		var txn catalog.Transaction
		if !begin.ReadOnly {
			txn = s.Engine.Begin()
		} else if begin.AsOf != 0 {
			txn = s.Engine.BeginAsOf(begin.AsOf)
		} else {
			txn = s.Engine.BeginReadOnly()
		}

		txnState, ok := txn.(*TransactionEngine)
		if ok {
			return util.BinaryStructToByte(txnState.Txn.St), nil
		}
		return command, nil
	case CommitPrefix:
		commit := Commit{}
		util.ByteToStruct(command[2:], &commit)
		s.Engine.Resume(commit.Txn).Commit()
		return command, nil
	case RollBackPrefix:
		rollback := Rollback{}
		util.ByteToStruct(command[2:], &rollback)
		s.Engine.Resume(rollback.Txn).Rollback()
		return command, nil
	case CreatePrefix:
		create := Create{}
		util.ByteToStruct(command[2:], &create)
		err := s.Engine.Resume(create.Txn).Create(create.Table, create.Row)
		if err != nil {
			return nil, err
		}
		return command, nil
	case DeletePrefix:
		delete1 := Delete{}
		util.ByteToStruct(command[2:], &delete1)
		err := s.Engine.Resume(delete1.Txn).Delete(delete1.Table, delete1.Value)
		if err != nil {
			return nil, err
		}
		return command, nil
	case UpdatePrefix:
		update := Update{}
		util.ByteToStruct(command[2:], &update)
		err := s.Engine.Resume(update.Txn).Update(update.Table, update.ID, update.Row)
		if err != nil {
			return nil, err
		}
		return command, nil
	case CreateTablePrefix:
		createTable := CreateTable{}
		util.ByteToStruct(command[2:], &createTable)
		err := s.Engine.Resume(createTable.Txn).CreateTable(createTable.Schema)
		if err != nil {
			return nil, err
		}
		return command, nil
	case DeleteTablePrefix:
		deleteTable := DeleteTable{}
		util.ByteToStruct(command[2:], &deleteTable)
		err := s.Engine.Resume(deleteTable.Txn).DeleteTable(deleteTable.Table)
		if err != nil {
			return nil, err
		}
		return command, nil
	}

	return nil, errors.New("mutate command protocol validation failed: invalid packet header")
}

const (
	MutationPrefix    byte = 0x04
	BeginPrefix       byte = 0x02
	CommitPrefix      byte = 0x03
	RollBackPrefix    byte = 0x04
	CreatePrefix      byte = 0x05
	DeletePrefix      byte = 0x06
	UpdatePrefix      byte = 0x07
	CreateTablePrefix byte = 0x08
	DeleteTablePrefix byte = 0x09
)

type Mutation interface {
	MutationEncode() []byte
}

type Begin struct {
	ReadOnly bool
	AsOf     uint64
}

func (b *Begin) MutationEncode() []byte {
	return append([]byte{MutationPrefix, BeginPrefix}, (util.BinaryStructToByte(b))...)
}

type Commit struct {
	Txn *storage.TransactionState
}

func (c *Commit) MutationEncode() []byte {
	return append([]byte{MutationPrefix, CommitPrefix}, (util.BinaryStructToByte(c))...)
}

type Rollback struct {
	Txn *storage.TransactionState
}

func (r *Rollback) MutationEncode() []byte {
	return append([]byte{MutationPrefix, RollBackPrefix}, (util.BinaryStructToByte(r))...)
}

type Create struct {
	Txn   *storage.TransactionState
	Table string
	Row   []*sql.ValueData //现在需要关注下目前Row是只有一行的数据,需要关注下后续为啥这么做
}

func (c *Create) MutationEncode() []byte {
	return append([]byte{MutationPrefix, CreatePrefix}, util.BinaryStructToByte(c)...)
}

type Delete struct {
	Txn   *storage.TransactionState
	Table string
	Value *sql.ValueData
}

func (d *Delete) MutationEncode() []byte {

	return append([]byte{MutationPrefix, DeletePrefix}, util.BinaryStructToByte(d)...)
}

type Update struct {
	Txn   *storage.TransactionState
	Table string
	ID    *sql.ValueData
	Row   []*sql.ValueData
}

func (u *Update) MutationEncode() []byte {
	return append([]byte{MutationPrefix, UpdatePrefix}, util.BinaryStructToByte(u)...)
}

type CreateTable struct {
	Txn    *storage.TransactionState
	Schema *catalog.Table
}

func (c *CreateTable) MutationEncode() []byte {
	return append([]byte{MutationPrefix, CreateTablePrefix}, util.BinaryStructToByte(c)...)
}

type DeleteTable struct {
	Txn   *storage.TransactionState
	Table string
}

func (d *DeleteTable) MutationEncode() []byte {
	return append([]byte{MutationPrefix, DeleteTablePrefix}, util.BinaryStructToByte(d)...)
}

func GobReg() {
	gob.Register(&Begin{})
	gob.Register(&Commit{})
	gob.Register(&Rollback{})
	gob.Register(&Create{})
	gob.Register(&Delete{})
	gob.Register(&Update{})
	gob.Register(&CreateTable{})
	gob.Register(&DeleteTable{})
	gob.Register(&ClientTransaction{})
	gob.Register(&RowKey{})
	gob.Register(&TableKey{})
	gob.Register(&IndexKey{})
	gob.Register(&RowKey{})
	gob.Register([]interface{}{})
	gob.Register([]*sql.ValueData{})
	gob.Register(&TransactionEngine{})

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(&Begin{})
	_ = enc.Encode(&Commit{})
	_ = enc.Encode(&Rollback{})
	_ = enc.Encode(&Create{})
	_ = enc.Encode(&Delete{})
	_ = enc.Encode(&Update{})
	_ = enc.Encode(&CreateTable{})
	_ = enc.Encode(&DeleteTable{})
	_ = enc.Encode(&ClientTransaction{})
	_ = enc.Encode(&RowKey{})
	_ = enc.Encode(&TableKey{})
	_ = enc.Encode(&IndexKey{})
	_ = enc.Encode([]interface{}{})
	_ = enc.Encode([]*sql.ValueData{})
	_ = enc.Encode(&TransactionEngine{})
}
