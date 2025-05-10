package catalog

import (
	"cabbageDB/sql"
	"cabbageDB/sql/expr"
	"errors"
)

type InsertExec struct {
	Table   string
	Columns []string
	Rows    [][]expr.Expression
}

func PadRow(table *Table, row []*sql.ValueData) []*sql.ValueData {

	if len(row) > len(table.Columns) {
		return nil
	}
	for _, defaule := range table.Columns[len(row)-1:] {
		if defaule.Default != nil {
			row = append(row, defaule.Default)
		}

	}
	return row
}

func (i *InsertExec) Execute(txn Transaction) (ResultSet, error) {
	table, err := txn.MustReadTable(i.Table)
	if err != nil {
		return nil, err
	}
	count := 0
	for _, exprs := range i.Rows {
		var row []*sql.ValueData
		for _, expr := range exprs {
			row = append(row, expr.Evaluate(nil))
		}

		if len(i.Columns) == 0 {
			row = PadRow(table, row)
		} else {
			row, err = MakeRow(table, i.Columns, row)
			if err != nil {
				return nil, err
			}
		}
		err = txn.Create(i.Table, row)
		if err != nil {
			return nil, err
		}
		count += 1
	}
	return &CreateResultSet{
		Count: count,
	}, nil
}

func MakeRow(table *Table, columns []string, values []*sql.ValueData) ([]*sql.ValueData, error) {
	if len(columns) != len(values) {
		return nil, errors.New("Column and value counts do not match")
	}
	inputs := make(map[string]*sql.ValueData)
	for i := range values {
		_, err := table.GetColumn(columns[i])
		if err != nil {
			return nil, err
		}

		inputs[columns[i]] = values[i]

	}
	row := []*sql.ValueData{}
	for _, column := range table.Columns {
		if v, ok := inputs[column.Name]; ok {
			row = append(row, v)
			continue
		} else if column.Default != nil {
			row = append(row, column.Default)
		}

	}

	return row, nil
}

type UpdateExec struct {
	Table       string
	Source      Executor
	Expressions []*UpdateExpr
}

func (u *UpdateExec) Execute(txn Transaction) (ResultSet, error) {
	resultSet, err := u.Source.Execute(txn)
	if err != nil {
		return nil, err
	}

	switch v := resultSet.(type) {
	case *QueryResultSet:
		table, err1 := txn.MustReadTable(u.Table)
		if err1 != nil {
			return nil, err
		}
		updated := make(map[*sql.ValueData]struct{})
		for i := range v.Rows {
			id := table.GetRowKey(v.Rows[i])
			if _, ok := updated[id]; ok {
				continue
			}
			for _, expr := range u.Expressions {
				v.Rows[i][expr.Index] = expr.Expr.Evaluate(v.Rows[i])
			}
			err2 := txn.Update(table.Name, id, v.Rows[i])
			if err2 != nil {
				return nil, err2
			}
			updated[id] = struct{}{}
		}
		return &UpdateResultSet{
			Count: len(updated),
		}, nil
	}
	return nil, errors.New("HashJoinExec Invalid return ResultSet")

}

type DeteleExec struct {
	Table  string
	Source Executor
}

func (d *DeteleExec) Execute(txn Transaction) (ResultSet, error) {
	table, err := txn.MustReadTable(d.Table)
	if err != nil {
		return nil, err
	}
	count := 0
	resultSet, err := d.Source.Execute(txn)
	if err != nil {
		return nil, err
	}

	switch v := resultSet.(type) {
	case *QueryResultSet:
		for i := range v.Rows {
			err = txn.Delete(table.Name, table.GetRowKey(v.Rows[i]))
			if err != nil {
				return nil, err
			}
			count += 1
		}
		return &DeleteResultSet{
			Count: count,
		}, nil
	}
	return nil, errors.New("DeteleExec Invalid return ResultSet")
}
