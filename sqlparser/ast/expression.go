package ast

import (
	"bytes"
	"cabbageDB/sql"
	"encoding/gob"
)

type Order struct {
	Expr Expression
	Desc bool
}

type FromItem interface {
	fromItem()
}

type FromItemTable struct {
	Name  string
	Alias string
}

func (f *FromItemTable) fromItem() {}

type JoinType int

const (
	CrossJoin JoinType = iota + 1
	InnerJoin
	LeftJoin
	RightJoin
)

type FromItemJoinTable struct {
	Left      FromItem
	Right     FromItem
	Type      JoinType
	Predicate Expression
}

func (f *FromItemJoinTable) fromItem() {}

// 表达式
type ExprAS struct {
	Expr Expression
	As   string
}

type ExprColumn struct {
	ColumnName string
	Expr       Expression
}

type Expression interface {
	expression()
}

type Field struct {
	TableName  string
	ColumnName string
}

func (f *Field) expression() {
}

type ColumnIdx struct {
	Index int
}

func (c *ColumnIdx) expression() {
}

//type DataType int
//
//const (
//	Null DataType = iota
//	Bool
//	Int
//	Float
//	String
//)

// Type
// 0 Null
// 1 Bool
// 2 Int
// 3 Float
// 4 String
type Literal struct {
	Type  sql.DataType
	Value interface{}
}

func (l *Literal) expression() {
}

type Function struct {
	FuncName string
	Args     []Expression
}

func (f *Function) expression() {
}

type Operation struct {
	Operation OperationType
}

func (o *Operation) expression() {
}

type OperationType interface {
	operationType()
}

type AndOper struct {
	L Expression
	R Expression
}

func (a *AndOper) operationType() {
}
func (a *AndOper) expression() {
}

type NotOper struct {
	L Expression
}

func (n *NotOper) operationType() {

}
func (n *NotOper) expression() {
}

type OrOper struct {
	L Expression
	R Expression
}

func (o *OrOper) operationType() {

}
func (o *OrOper) expression() {
}

type EqualOper struct {
	L Expression
	R Expression
}

func (e *EqualOper) operationType() {

}
func (e *EqualOper) expression() {
}

type GreaterThanOper struct {
	L Expression
	R Expression
}

func (g *GreaterThanOper) operationType() {

}
func (g *GreaterThanOper) expression() {
}

type GreaterThanOrEqualOper struct {
	L Expression
	R Expression
}

func (g *GreaterThanOrEqualOper) operationType() {

}
func (g *GreaterThanOrEqualOper) expression() {
}

type IsNullOper struct {
	L Expression
}

func (i *IsNullOper) operationType() {

}
func (i *IsNullOper) expression() {
}

type LessThanOper struct {
	L Expression
	R Expression
}

func (l *LessThanOper) operationType() {

}
func (l *LessThanOper) expression() {
}

type LessThanOrEqualOper struct {
	L Expression
	R Expression
}

func (g *LessThanOrEqualOper) operationType() {

}

func (g *LessThanOrEqualOper) expression() {
}

type NotEqualOper struct {
	L Expression
	R Expression
}

func (n *NotEqualOper) operationType() {
}

func (n *NotEqualOper) expression() {
}

type AddOper struct {
	L Expression
	R Expression
}

func (a *AddOper) operationType() {
}
func (a *AddOper) expression() {
}

type AssertOper struct {
	L Expression
}

func (a *AssertOper) operationType() {
}

func (a *AssertOper) expression() {
}

type DivideOper struct {
	L Expression
	R Expression
}

func (d *DivideOper) operationType() {
}
func (d *DivideOper) expression() {
}

type ExponentiateOper struct {
	L Expression
	R Expression
}

func (e *ExponentiateOper) operationType() {
}
func (e *ExponentiateOper) expression() {
}

type FactorialOper struct {
	L Expression
}

func (f *FactorialOper) operationType() {
}
func (f *FactorialOper) expression() {
}

type ModuloOper struct {
	L Expression
	R Expression
}

func (m *ModuloOper) operationType() {
}
func (m *ModuloOper) expression() {
}

type MultiplyOper struct {
	L Expression
	R Expression
}

func (m *MultiplyOper) operationType() {
}
func (m *MultiplyOper) expression() {
}

type NegateOper struct {
	L Expression
}

func (n *NegateOper) operationType() {
}
func (n *NegateOper) expression() {
}

type SubtractOper struct {
	L Expression
	R Expression
}

func (s *SubtractOper) operationType() {
}

func (s *SubtractOper) expression() {
}

type LikeOper struct {
	L Expression
	R Expression
}

func (l *LikeOper) operationType() {
}
func (l *LikeOper) expression() {
}

func GobReg() {

	gob.Register(&Literal{})
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(&Literal{})
}
