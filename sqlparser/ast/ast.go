package ast

import (
	"cabbageDB/sql"
)

type Column struct {
	Name       string
	ColumnType sql.DataType
	PrimaryKey bool
	Nullable   bool
	Default    Expression
	Unique     bool
	Index      bool
	References *Field
}

type ColumnOptionType int

const (
	NOTNULL ColumnOptionType = iota
	NULL
	PRIMARYKEY
	DEFAULT
	UNIQUE
)

type ColumnOption struct {
	Type  ColumnOptionType
	Value interface{}
}

type ConstraintType int

const (
	PRIMARYKEYConstraint ConstraintType = iota
	UNIQUEKEYConstraint
	KEYConstraint
	FORREGINKEYConstraint
)

type Constraint struct {
	Type          ConstraintType
	IndexName     string
	ColumnName    string
	TableName     string
	SubColumnName string
}
