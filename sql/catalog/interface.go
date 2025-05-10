package catalog

import (
	"bytes"
	"cabbageDB/log"
	"cabbageDB/sql"
	"cabbageDB/sql/expr"
	"cabbageDB/sqlparser/ast"
	"cabbageDB/sqlparser/parser"
	"cabbageDB/storage"
	"cabbageDB/util"
	"encoding/gob"
	"errors"
)

type ValueHashSet = map[*sql.ValueData]struct{}

type Engine interface {
	Begin() Transaction
	BeginReadOnly() Transaction
	BeginAsOf(version uint64) Transaction
	Session() *Session
}

type Transaction interface {
	Catalog
	Version() uint64
	ReadOnly() bool
	Commit() bool
	Rollback() bool
	Create(table string, row []*sql.ValueData) error
	Delete(table string, id *sql.ValueData) error
	Read(table string, id *sql.ValueData) ([]*sql.ValueData, error)
	ReadIndex(table string, column string, value *sql.ValueData) (ValueHashSet, error)
	Scan(table string, filter expr.Expression) ([][]*sql.ValueData, error)
	ScanIndex(table string, column string) ([]*IndexValue, error)
	Update(table string, id *sql.ValueData, row []*sql.ValueData) error
}

type Session struct {
	Engine Engine
	Txn    Transaction
}

func (s *Session) Execute(query string) (ResultSet, error) {
	p := parser.Parser{}
	p.Reset(query)
	ret := p.ParseSQL()
	if ret != 0 {
		if p.ErrorMsg != nil {
			return nil, p.ErrorMsg
		}
		return nil, errors.New("parse sql error")
	}

	for _, result := range p.Result {
		switch v := result.(type) {
		case *ast.BeginStmt:
			if s.Txn != nil {
				return nil, errors.New("Already in a transaction")
			}

			if v.ReadOnly == true {
				if v.AsOf == 0 {
					txn := s.Engine.BeginReadOnly()
					resultSet := &BeginResultSet{Version: txn.Version(), ReadOnly: true}
					s.Txn = txn
					return resultSet, nil
				} else {
					txn := s.Engine.BeginAsOf(v.AsOf)
					resultSet := &BeginResultSet{Version: v.AsOf, ReadOnly: true}
					s.Txn = txn
					return resultSet, nil
				}
			} else {
				if v.AsOf != 0 {
					return nil, errors.New("Can't start read-write transaction in a given version")
				}
				txn := s.Engine.Begin()
				resultSet := &BeginResultSet{Version: txn.Version(), ReadOnly: false}
				s.Txn = txn
				return resultSet, nil
			}
		case *ast.CommitStmt:
			if s.Txn == nil {
				return nil, errors.New("Not in a transaction")
			}
			txn := s.Txn
			s.Txn = nil
			version := txn.Version()
			txn.Commit()
			return &CommitResultSet{Version: version}, nil
		case *ast.RollbackStmt:
			if s.Txn == nil {
				return nil, errors.New("Not in a transaction")
			}
			txn := s.Txn
			s.Txn = nil
			version := txn.Version()
			txn.Rollback()
			return &RollbackResultSet{Version: version}, nil
		case *ast.ExplainStmt:

			node, err := s.ReadWithTxn(func(txn Transaction) (any, error) {
				plan, err1 := Build(v.Stmt, txn)
				if err1 != nil {
					return nil, err1
				}
				return plan.Optimize(txn).Node, nil
			})
			if err != nil {
				return nil, err
			}

			return &ExplainResultSet{
				NodeInfo: FormatNode(node.(Node), 1),
			}, nil
		case *ast.SelectStmt:
			if s.Txn != nil {
				plan, err := Build(result, s.Txn)
				if err != nil {
					return nil, err
				}
				resultset, err1 := plan.Optimize(s.Txn).Execute(s.Txn)
				if err1 != nil {
					return nil, err1
				}
				return resultset, nil
			}

			txn := s.Engine.BeginReadOnly()
			plan, err := Build(result, txn)
			if err != nil {
				return nil, err
			}
			resultset, err1 := plan.Optimize(txn).Execute(txn)
			if err1 != nil {
				return nil, err1
			}
			txn.Rollback()
			return resultset, nil
		default:
			if s.Txn != nil {
				plan, err := Build(result, s.Txn)
				if err != nil {
					return nil, err
				}

				return plan.Optimize(s.Txn).Execute(s.Txn)
			}
			txn := s.Engine.Begin()
			plan, err := Build(result, txn)
			if err != nil {
				return nil, err
			}
			resultset, err1 := plan.Optimize(txn).Execute(txn)
			if err1 != nil {
				txn.Rollback()
				return nil, err1
			}
			txn.Commit()
			return resultset, nil
		}

	}

	return nil, nil
}

func (s *Session) ReadWithTxn(f func(transaction Transaction) (any, error)) (any, error) {
	if s.Txn != nil {
		return f(s.Txn)
	}
	txn := s.Engine.BeginReadOnly()
	res, err := f(txn)
	if err != nil {
		return nil, err
	}
	txn.Rollback()
	return res, nil
}

type IndexValue struct {
	Value        interface{}
	ValueHashSet storage.VersionHashSet
}

type Status struct {
	RaftStatus *log.NodeStatus
	MVCCStatus *storage.Status
}

const (
	ResultSetPrefix            = 0x09
	BeginResultSetPrefix       = 0x02
	CommitResultSetPrefix      = 0x03
	RollbackResultSetPrefix    = 0x04
	CreateResultSetPrefix      = 0x05
	DeleteResultSetPrefix      = 0x06
	CreateTableResultSetPrefix = 0x07
	DropTableResultSetPrefix   = 0x08
	QueryResultSetPrefix       = 0x09
	ExplainResultSetPrefix     = 0x10
)

type ResultSet interface {
	ResultSetEncode() []byte
}

func GetReusltSetPrefix(result ResultSet) byte {
	switch result.(type) {
	case *BeginResultSet:
		return BeginResultSetPrefix
	case *CommitResultSet:
		return CommitResultSetPrefix
	case *RollbackResultSet:
		return RollbackResultSetPrefix
	case *CreateResultSet:
		return CreateResultSetPrefix
	case *DeleteResultSet:
		return DeleteResultSetPrefix
	case *CreateTableResultSet:
		return CreateTableResultSetPrefix
	case *DropTableResultSet:
		return DropTableResultSetPrefix
	case *QueryResultSet:
		return QueryResultSetPrefix
	case *ExplainResultSet:
		return ExplainResultSetPrefix
	}
	return ResultSetPrefix
}

type BeginResultSet struct {
	Version  uint64
	ReadOnly bool
}

func (b *BeginResultSet) ResultSetEncode() []byte {
	return append([]byte{ResultSetPrefix, BeginResultSetPrefix}, util.BinaryStructToByte(b)...)
}

type CommitResultSet struct {
	Version uint64
}

func (c *CommitResultSet) ResultSetEncode() []byte {
	return append([]byte{ResultSetPrefix, CommitResultSetPrefix}, util.BinaryStructToByte(c)...)

}

type RollbackResultSet struct {
	Version uint64
}

func (r *RollbackResultSet) ResultSetEncode() []byte {
	return append([]byte{ResultSetPrefix, RollbackResultSetPrefix}, util.BinaryStructToByte(r)...)
}

type CreateResultSet struct {
	Count int
}

func (c *CreateResultSet) ResultSetEncode() []byte {
	return append([]byte{ResultSetPrefix, CreateResultSetPrefix}, util.BinaryStructToByte(c)...)
}

type DeleteResultSet struct {
	Count int
}

func (d *DeleteResultSet) ResultSetEncode() []byte {
	return append([]byte{ResultSetPrefix, DeleteResultSetPrefix}, util.BinaryStructToByte(d)...)
}

type UpdateResultSet struct {
	Count int
}

func (u *UpdateResultSet) ResultSetEncode() []byte {
	return append([]byte{ResultSetPrefix, DeleteResultSetPrefix}, util.BinaryStructToByte(u)...)
}

type CreateTableResultSet struct {
	Name string
}

func (c *CreateTableResultSet) ResultSetEncode() []byte {
	return append([]byte{ResultSetPrefix, CreateTableResultSetPrefix}, util.BinaryStructToByte(c)...)
}

type DropTableResultSet struct {
	Name string
}

func (d *DropTableResultSet) ResultSetEncode() []byte {
	return append([]byte{ResultSetPrefix, DropTableResultSetPrefix}, util.BinaryStructToByte(d)...)
}

type QueryResultSet struct {
	Columns []string
	Rows    [][]*sql.ValueData
}

func (q *QueryResultSet) ResultSetEncode() []byte {
	return append([]byte{ResultSetPrefix, QueryResultSetPrefix}, util.BinaryStructToByte(q)...)

}

type ExplainResultSet struct {
	NodeInfo string
}

func (e *ExplainResultSet) ResultSetEncode() []byte {
	return append([]byte{ResultSetPrefix, ExplainResultSetPrefix}, util.BinaryStructToByte(e)...)

}

func GobReg() {
	//gob.Register(&ReferenceField{})

	gob.Register(&BeginResultSet{})
	gob.Register(&CommitResultSet{})
	gob.Register(&RollbackResultSet{})
	gob.Register(&CreateResultSet{})
	gob.Register(&DeleteResultSet{})
	gob.Register(&UpdateResultSet{})
	gob.Register(&CreateTableResultSet{})
	gob.Register(&DropTableResultSet{})
	gob.Register(&QueryResultSet{})
	gob.Register(&ExplainResultSet{})
	gob.Register(&sql.ValueData{})
	gob.Register([][]*sql.ValueData{})
	gob.Register(ValueHashSet{})

	gob.Register(&AggregationNode{})
	gob.Register(&CreateTableNode{})
	gob.Register(&NothingNode{})
	gob.Register(&DeleteNode{})
	gob.Register(&DropTableNode{})
	gob.Register(&FilterNode{})
	gob.Register(&HashJoinNode{})
	gob.Register(&IndexLookupNode{})
	gob.Register(&InsertNode{})
	gob.Register(&KeyLookupNode{})
	gob.Register(&LimitNode{})
	gob.Register(&NestedLoopJoinNode{})
	gob.Register(&OffsetNode{})
	gob.Register(&OrderNode{})
	gob.Register(&ProjectionNode{})
	gob.Register(&ScanNode{})
	gob.Register(&UpdateNode{})

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	//_ = enc.Encode(&ReferenceField{})

	_ = enc.Encode(&BeginResultSet{})
	_ = enc.Encode(&CommitResultSet{})
	_ = enc.Encode(&RollbackResultSet{})
	_ = enc.Encode(&CreateResultSet{})
	_ = enc.Encode(&DeleteResultSet{})
	_ = enc.Encode(&UpdateResultSet{})
	_ = enc.Encode(&CreateTableResultSet{})
	_ = enc.Encode(&DropTableResultSet{})
	_ = enc.Encode(&QueryResultSet{})
	_ = enc.Encode(&ExplainResultSet{})
	_ = enc.Encode(&sql.ValueData{})
	_ = enc.Encode([][]*sql.ValueData{})
	_ = enc.Encode(ValueHashSet{})

	_ = enc.Encode(&AggregationNode{})
	_ = enc.Encode(&CreateTableNode{})
	_ = enc.Encode(&NothingNode{})
	_ = enc.Encode(&DeleteNode{})
	_ = enc.Encode(&DropTableNode{})
	_ = enc.Encode(&FilterNode{})
	_ = enc.Encode(&HashJoinNode{})
	_ = enc.Encode(&IndexLookupNode{})
	_ = enc.Encode(&InsertNode{})
	_ = enc.Encode(&KeyLookupNode{})
	_ = enc.Encode(&LimitNode{})
	_ = enc.Encode(&NestedLoopJoinNode{})
	_ = enc.Encode(&OffsetNode{})
	_ = enc.Encode(&OrderNode{})
	_ = enc.Encode(&ProjectionNode{})
	_ = enc.Encode(&ScanNode{})
	_ = enc.Encode(&UpdateNode{})

}
