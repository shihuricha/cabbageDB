package catalog

import (
	"cabbageDB/sql"
	"cabbageDB/sql/expr"
)

type ScanExec struct {
	Table  string
	Filter expr.Expression
}

func (s *ScanExec) Execute(txn Transaction) (ResultSet, error) {
	table, err := txn.MustReadTable(s.Table)
	if err != nil {
		return nil, err
	}
	columns := make([]string, len(table.Columns))
	for i := range table.Columns {
		columns[i] = table.Columns[i].Name
	}
	rows, err1 := txn.Scan(s.Table, s.Filter)
	if err1 != nil {
		return nil, err1
	}
	return &QueryResultSet{
		Columns: columns,
		Rows:    rows,
	}, nil

}

type KeyLookupExec struct {
	Table string
	Keys  []*sql.ValueData
}

func (k *KeyLookupExec) Execute(txn Transaction) (ResultSet, error) {
	table, err := txn.MustReadTable(k.Table)
	if err != nil {
		return nil, err
	}

	rows := [][]*sql.ValueData{}
	for _, key := range k.Keys {
		value, err1 := txn.Read(k.Table, key)
		if err1 != nil {
			return nil, err1
		}
		rows = append(rows, value)
	}
	columns := []string{}
	for _, column := range table.Columns {
		columns = append(columns, column.Name)
	}
	return &QueryResultSet{
		Columns: columns,
		Rows:    rows,
	}, nil
}

type IndexLookupExec struct {
	Table  string
	Column string
	Values []*sql.ValueData
}

func (i *IndexLookupExec) Execute(txn Transaction) (ResultSet, error) {
	table, err := txn.MustReadTable(i.Table)
	if err != nil {
		return nil, err
	}
	pks := make(map[*sql.ValueData]struct{})
	for _, value := range i.Values {
		idx, err1 := txn.ReadIndex(i.Table, i.Column, value)
		if err1 != nil {
			return nil, err1
		}
		for i1, _ := range idx {
			pks[i1] = struct{}{}
		}
	}

	rows := [][]*sql.ValueData{}
	for i1, _ := range pks {
		value, err1 := txn.Read(i.Table, i1)
		if err1 != nil {
			return nil, err1
		}
		rows = append(rows, value)
	}
	colums := []string{}
	for i1 := range table.Columns {
		colums = append(colums, table.Columns[i1].Name)
	}

	return &QueryResultSet{
		Columns: colums,
		Rows:    rows,
	}, nil
}

type NothingExec struct {
}

func (n *NothingExec) Execute(txn Transaction) (ResultSet, error) {
	return &QueryResultSet{
		Columns: []string{},
		Rows:    make([][]*sql.ValueData, 1),
	}, nil
}
