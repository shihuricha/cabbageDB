package parser

import (
	"cabbageDB/sqlparser/ast"
	"errors"
)

var Sqlerr error

func handleColumn(columnOptionList []*ast.ColumnOption, column *ast.Column) error {
	for _, columnOption := range columnOptionList {
		if columnOption == nil {
			continue
		}
		switch columnOption.Type {
		case ast.NULL:
			column.Nullable = true
		case ast.NOTNULL:
			column.Nullable = false
		case ast.PRIMARYKEY:
			column.PrimaryKey = true
		case ast.DEFAULT:
			column.Default = columnOption.Value.(ast.Expression)
		case ast.UNIQUE:
			column.Unique = true
		}
	}
	return nil
}

func handleConstraint(columns []*ast.Column, c *ast.Constraint) error {
	if c == nil {
		return nil
	}

	flag := false
	for _, col := range columns {
		if c.Type == ast.FORREGINKEYConstraint && c.SubColumnName == col.Name {
			flag = true
			if c.TableName == "" {
				return errors.New("References Table Name is empty")
			}
			if c.ColumnName == "" {
				return errors.New("References Column Name is empty")
			}
			col.References = &ast.Field{
				TableName:  c.TableName,
				ColumnName: c.ColumnName,
			}
		}

		if col.Name == c.ColumnName {
			flag = true
			switch c.Type {
			case ast.PRIMARYKEYConstraint:
				col.PrimaryKey = true
			case ast.UNIQUEKEYConstraint:
				col.Unique = true
			case ast.KEYConstraint:
				col.Index = true
			}
		}
	}
	if flag == false {
		return errors.New("not found column " + c.ColumnName)
	}
	return nil
}

func getUint64FromNUM(num interface{}) uint64 {
	switch v := num.(type) {
	case int64:
		return uint64(v)
	case uint64:
		return v
	}
	return 0
}

func valiDate(columns []*ast.Column) error {
	hasPrimaryKey := false
	columnNames := make(map[string]struct{})
	for _, col := range columns {
		if _, ok := columnNames[col.Name]; ok {
			return errors.New("column " + col.Name + " alread exists")
		}
		columnNames[col.Name] = struct{}{}

		if col.PrimaryKey {
			if hasPrimaryKey {
				// 发现第二个主键时，立即报错并指明列名
				return errors.New("multiple primary keys defined (column " + col.Name + ")")
			}
			hasPrimaryKey = true // 标记已存在主键
		}
	}
	if !hasPrimaryKey {
		// 如果没有主键
		return errors.New("must be have a primary key")
	}
	return nil
}
