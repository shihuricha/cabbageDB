package catalog

import (
	"cabbageDB/sql"
	"errors"
	"fmt"
	"strings"
)

type Table struct {
	Name    string
	Columns []*Column
}

func (t *Table) ValiDateRow(row []*sql.ValueData, txn Transaction) error {
	if len(row) != len(t.Columns) {
		return errors.New("Invalid row size for table")
	}
	pk := t.GetRowKey(row)
	for i, column := range t.Columns {
		err := column.ValidateValue(t, pk, row[i], txn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) GetColumn(columnName string) (*Column, error) {
	for _, column := range t.Columns {
		if column.Name == columnName {
			return column, nil
		}
	}
	return nil, errors.New("Column " + columnName + " not found in table " + t.Name)
}

func (t *Table) GetColumnIndex(name string) (int, error) {
	for i, column := range t.Columns {
		if column.Name == name {
			return i, nil
		}
	}

	return -1, errors.New("Column " + name + " not found in table " + t.Name)
}

func (t *Table) ValiDate(txn Transaction) error {
	if t.Columns == nil || len(t.Columns) == 0 {
		//todo 在这里需要输出错误信息 要不使用log呢,或者专门一个错误通道呢？
		return errors.New("table columns is nil")
	}

	count := 0
	for _, column := range t.Columns {
		if column.PrimaryKey {
			count++
		}
	}
	if count != 1 {

		return errors.New("No primary key in table")
	}
	for _, column := range t.Columns {
		err := column.Validate(t, txn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) GetPrimaryKey() *Column {
	for _, column := range t.Columns {
		if column.PrimaryKey {
			return column
		}
	}
	return nil
}

func (t *Table) GetRowKey(row []*sql.ValueData) *sql.ValueData {

	for i, column := range t.Columns {
		if column.PrimaryKey {
			return row[i]
		}
	}
	return nil
}
func (t *Table) String() string {
	// 1. 收集列定义
	var columns []string
	for _, col := range t.Columns {
		columns = append(columns, col.String())
	}

	// 2. 收集表级约束（外键、索引）
	var constraints []string
	for _, col := range t.Columns {
		if constraint := col.ConstraintString(); constraint != "" {
			for _, line := range strings.Split(constraint, "\n") {
				if trimmed := strings.TrimSpace(line); trimmed != "" {
					constraints = append(constraints, trimmed)
				}
			}
		}
	}

	// 3. 合并列和约束，确保约束在列之后
	var parts []string
	parts = append(parts, columns...)
	parts = append(parts, constraints...)

	// 4. 生成最终 SQL
	return fmt.Sprintf(
		"CREATE TABLE %s (\n  %s\n);",
		t.Name,
		strings.Join(parts, ",\n  "),
	)
}

type ReferenceField struct {
	TableName  string
	ColumnName string
}

// 这里的DataType 先这么写,可以在sql解析的时候强制他的类型
type Column struct {
	Name       string
	DataType   sql.DataType
	PrimaryKey bool
	NullAble   bool
	Default    *sql.ValueData
	Unique     bool
	//Reference  string
	Index     bool
	Reference *ReferenceField
}

func (c *Column) Validate(table *Table, txnEngine Transaction) error {
	if c.PrimaryKey && c.NullAble {
		return errors.New("Primary key " + c.Name + " cannot be nullable")
	}
	if c.PrimaryKey && !c.Unique {
		return errors.New("Primary key " + c.Name + " cannot be unique")
	}

	if c.Default != nil {

		dataType := sql.DecodeDataType(c.Default.Value)

		// 只有当Default为NULL 并且c.NullAble的时候才通过
		if dataType == sql.NullType {
			if !c.NullAble {
				return errors.New("Can't use NULL as default value for non-nullable column " + c.Name)
			}
		} else if c.DataType != dataType {
			return errors.New("Default value for column " + c.Name + " has datatype " + c.DataType.String() + ", must be " + dataType.String())
		}

	}

	if c.Reference == nil {
		return nil
	}

	var target *Table
	if c.Reference.TableName != table.Name {
		target = txnEngine.ReadTable(c.Reference.TableName)
	} else {
		target = table
	}
	if target.Name == "" {
		return errors.New("Referenced Table " + c.Reference.TableName + " is not exist")
	}

	index, err := target.GetColumnIndex(c.Reference.ColumnName)
	if err != nil {
		return nil
	}
	if index == -1 {
		return errors.New("Referenced Column " + c.Reference.ColumnName + " is not exist Table" + c.Reference.TableName)
	}

	if !target.Columns[index].Index && !target.Columns[index].PrimaryKey {
		return errors.New("Referenced Column " + c.Reference.ColumnName + " Must Be Primary Key or Index")
	}

	//if c.DataType != target.GetPrimaryKey().DataType {
	//	return errors.New("Referenced primary key dataType is not equal")
	//}

	if c.DataType != target.Columns[index].DataType {
		return errors.New("Referenced primary key dataType is not equal")
	}
	return nil

}

func (c *Column) ValidateValue(table *Table, pk *sql.ValueData, value *sql.ValueData, txn Transaction) error {

	datatype := sql.DecodeDataType(value.Value)
	switch datatype {
	case sql.NullType:
		if !c.NullAble {
			return errors.New("NULL value not allowed for column:" + c.Name)
		}
	case sql.IntType, sql.BoolType, sql.StringType, sql.FloatType:
		if datatype != c.DataType {
			return errors.New("Invalid datatype " + datatype.String() + " for " + c.DataType.String() + " column " + c.Name)
		}
	}

	if c.Reference != nil {

		// 获取主键值
		rowData, err := txn.Scan(c.Reference.TableName, nil)
		if err != nil {
			return err
		}
		if len(rowData) == 0 {
			return errors.New("Referenced column value " + value.String() + "in table " + c.Reference.TableName + " does not exist")
		}

		// 获取t
		t := txn.ReadTable(c.Reference.TableName)
		index, err1 := t.GetColumnIndex(c.Reference.ColumnName)
		if err1 != nil {
			return err1
		}
		if index == -1 {
			return errors.New("Referenced Column " + c.Reference.ColumnName + " is not exist Table" + c.Reference.TableName)
		}
		if t.Columns[index].Index {
			valueData, err2 := txn.ReadIndex(t.Name, c.Reference.ColumnName, value)
			if err2 != nil {
				return err2
			}
			if len(valueData) == 0 {
				return errors.New("Referenced Column Value " + value.String() + " is not exist Table " + c.Reference.TableName)
			}
		}
		if t.Columns[index].PrimaryKey {
			flag := false
			for _, row := range rowData {
				if sql.EqualValue(value, t.GetRowKey(row)) {
					flag = true
				}
			}
			if !flag {
				return errors.New("Referenced Column Value " + value.String() + " is not exist Table " + c.Reference.TableName)
			}
		}
	}

	if c.Unique && !c.PrimaryKey && value != nil {
		//index, err := table.GetColumnIndex(c.Name)
		//if err != nil {
		//	return err
		//}
		scan, err := txn.ReadIndex(table.Name, c.Name, value)
		if err != nil {
			return err
		}

		if len(scan) != 0 {
			return errors.New("Unique value " + value.String() + " already exists for column " + c.Name)
		}
		//for _, row := range scan {
		//	if sql.EqualValue(row[index], value) && !sql.EqualValue(table.GetRowKey(row), pk) {
		//		return errors.New("Unique value " + value.String() + " already exists for column " + c.Name)
		//	}
		//}
	}
	return nil

}

func (c *Column) ConstraintString() string {
	var constraints []string
	// 外键约束
	if c.Reference != nil {
		constraints = append(constraints, fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s(%s)",
			c.Name, c.Reference.TableName, c.Reference.ColumnName))
	}
	// 索引约束（表级）
	if !c.Unique && c.Index {
		constraints = append(constraints, fmt.Sprintf("KEY %s (%s)",
			c.Name, c.Name))
	}
	return strings.Join(constraints, "\n")
}

func (c *Column) String() string {
	var builder strings.Builder

	// 基础列名和类型
	builder.WriteString(fmt.Sprintf("%s %s", c.Name, c.DataType.String()))

	// 主键约束
	if c.PrimaryKey {
		builder.WriteString(" PRIMARY KEY")
	}

	// 非空约束 (主键默认隐含 NOT NULL，故不重复添加)
	if !c.NullAble && !c.PrimaryKey {
		builder.WriteString(" NOT NULL")
	}

	// 默认值
	if c.Default != nil {
		builder.WriteString(fmt.Sprintf(" DEFAULT %v", c.Default))
	}

	// 唯一约束 (主键隐含唯一性，故不重复添加)
	if c.Unique && !c.PrimaryKey {
		builder.WriteString(" UNIQUE")
	}

	return builder.String()
}

type Catalog interface {
	CreateTable(table *Table) error
	DeleteTable(tableName string) error
	ReadTable(tableName string) *Table
	ScanTables() []*Table
	MustReadTable(tableName string) (*Table, error)
	TableReferences(tableName string, withSelf bool) []*TableReferences
}

type TableReferences struct {
	TableName        string
	ColumnReferences []string
}
