package catalog

import (
	"cabbageDB/sql"
	"cabbageDB/sql/expr"
	"errors"
	"sort"
)

type FilterExec struct {
	Source    Executor
	Predicate expr.Expression
}

func (f *FilterExec) Execute(txn Transaction) (ResultSet, error) {
	resultSet, err := f.Source.Execute(txn)
	if err != nil {
		return nil, err
	}
	switch v := resultSet.(type) {

	case *QueryResultSet:

		var filteredRows [][]*sql.ValueData

		for _, row := range v.Rows {
			result := f.Predicate.Evaluate(row)
			if result == nil {
				continue
			}
			switch result.Type {
			case sql.BoolType:
				if result.Value == true {
					filteredRows = append(filteredRows, row)
				}

			}
		}

		return &QueryResultSet{
			Columns: v.Columns,
			Rows:    filteredRows,
		}, nil
	}
	return nil, errors.New("FilterExec Invalid return ResultSet")
}

type ProjectionExec struct {
	Source      Executor
	Expressions []*ExprAs
}

func (p *ProjectionExec) Execute(txn Transaction) (ResultSet, error) {

	resultSet, err := p.Source.Execute(txn)
	if err != nil {
		return nil, err
	}
	switch v := resultSet.(type) {

	case *QueryResultSet:
		columns := []string{}

		exprs := []expr.Expression{}
		for _, expr1 := range p.Expressions {
			ex, ok := expr1.Expr.(*expr.Field)
			if ok && expr1.As == "" && len(v.Columns) > ex.Index {
				columns = append(columns, v.Columns[ex.Index])
			} else if expr1.As != "" {
				columns = append(columns, expr1.As)
			} else {
				columns = append(columns, "")
			}
			exprs = append(exprs, expr1.Expr)

		}
		rows := [][]*sql.ValueData{}
		for i := range v.Rows {
			row := []*sql.ValueData{}
			for i1 := range exprs {
				row = append(row, exprs[i1].Evaluate(v.Rows[i]))
			}
			rows = append(rows, row)
		}

		return &QueryResultSet{
			Columns: columns,
			Rows:    rows,
		}, nil

	}
	return nil, errors.New("ProjectionExec Invalid return ResultSet")
}

type LimitExec struct {
	Source Executor
	Limit  int
}

func (l *LimitExec) Execute(txn Transaction) (ResultSet, error) {

	resultSet, err := l.Source.Execute(txn)
	if err != nil {
		return nil, err
	}
	switch v := resultSet.(type) {

	case *QueryResultSet:
		if l.Limit > len(v.Rows) {
			l.Limit = len(v.Rows)
		}
		return &QueryResultSet{
			Columns: v.Columns,
			Rows:    v.Rows[:l.Limit],
		}, nil
	}
	return nil, errors.New("LimitExec Invalid return ResultSet")
}

type OffsetExec struct {
	Source Executor
	Offset int
}

func (o *OffsetExec) Execute(txn Transaction) (ResultSet, error) {
	resultSet, err := o.Source.Execute(txn)
	if err != nil {
		return nil, err
	}

	switch v := resultSet.(type) {
	case *QueryResultSet:
		if o.Offset > len(v.Rows) {
			o.Offset = len(v.Rows)
		}

		return &QueryResultSet{
			Columns: v.Columns,
			Rows:    v.Rows[o.Offset:],
		}, nil
	}
	return nil, errors.New("OffsetExec Invalid return ResultSet")
}

type OrderExec struct {
	Source Executor
	Orders []*DirectionExpr
}

type Item struct {
	Row    []*sql.ValueData
	Values []*sql.ValueData
}

func (o *OrderExec) Execute(txn Transaction) (ResultSet, error) {
	resultSet, err := o.Source.Execute(txn)
	if err != nil {
		return nil, err
	}
	switch v := resultSet.(type) {
	case *QueryResultSet:
		items := []*Item{}
		for i := range v.Rows {
			values := []*sql.ValueData{}
			for _, exprAs := range o.Orders {
				values = append(values, exprAs.Expr.Evaluate(v.Rows[i]))
			}
			items = append(items, &Item{
				Row:    v.Rows[i],
				Values: values,
			})
		}

		sorter := Sorter{
			items:  items,
			orders: o.Orders,
		}
		sort.Sort(sorter)

		result := make([][]*sql.ValueData, len(items))
		for i, item := range items {
			result[i] = item.Row
		}

		return &QueryResultSet{
			Columns: v.Columns,
			Rows:    result,
		}, nil

	}
	return nil, errors.New("OrderExec Invalid return ResultSet")

}

type Sorter struct {
	items  []*Item
	orders []*DirectionExpr
}

func (s Sorter) Len() int {
	return len(s.items)
}

func (s Sorter) Swap(i, j int) {
	s.items[i], s.items[j] = s.items[j], s.items[i]
}

func (s Sorter) Less(i, j int) bool {
	a, b := s.items[i], s.items[j]
	for idx, order := range s.orders {
		valA := a.Values[idx]
		valB := b.Values[idx]

		res, comparable1 := valA.Compare(valB)
		if !comparable1 {
			continue // 跳过不可比的条件
		}

		if res == 0 {
			continue // 继续下一个排序条件
		}

		// 根据方向调整结果
		if order.Desc {
			res = -res
		}
		return res < 0
	}
	return false // 所有条件均相等
}
