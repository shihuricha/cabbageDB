package catalog

import (
	"cabbageDB/sql"
	"cabbageDB/sql/expr"
	"errors"
)

type NestedLoopJoinExec struct {
	Left      Executor
	Right     Executor
	Predicate expr.Expression
	Outer     bool
}

func (n *NestedLoopJoinExec) Execute(txn Transaction) (ResultSet, error) {
	leftResult, err1 := n.Left.Execute(txn)
	if err1 != nil {
		return nil, err1
	}
	rightResult, err2 := n.Right.Execute(txn)
	if err2 != nil {
		return nil, err2
	}

	leftQuery, ok1 := leftResult.(*QueryResultSet)
	rightQuery, ok2 := rightResult.(*QueryResultSet)
	if !ok1 || !ok2 {

		return nil, errors.New("HashJoinExec Invalid return ResultSet")
	}
	columns := append(leftQuery.Columns, rightQuery.Columns...)
	nestRow := &NestedLoopRowsExec{
		Left:       leftQuery.Rows,
		Right:      rightQuery.Rows,
		Predicate:  n.Predicate,
		Outer:      n.Outer,
		RightWidth: len(rightQuery.Columns),
	}
	return &QueryResultSet{
		Rows:    GetNestedLoopRowsExecRows(nestRow),
		Columns: columns,
	}, nil
}

type NestedLoopRowsExec struct {
	Left       [][]*sql.ValueData
	Right      [][]*sql.ValueData
	Predicate  expr.Expression
	RightWidth int
	Outer      bool
}

func GetNestedLoopRowsExecRows(n *NestedLoopRowsExec) [][]*sql.ValueData {
	var results [][]*sql.ValueData
	rightEmpty := make([]*sql.ValueData, n.RightWidth)
	for i := range rightEmpty {
		rightEmpty[i] = &sql.ValueData{
			Type: sql.NullType,
		}
	}
	for leftRow := range n.Left {
		hasRightMatches := false

		for _, rightRow := range n.Right {
			combinedRow := append(n.Left[leftRow], rightRow...)
			if n.Predicate != nil {
				value := n.Predicate.Evaluate(combinedRow)
				if value == nil {
					continue
				}
				if !(value.Type == sql.BoolType && value.Value == true) {
					continue
				}
			}
			results = append(results, combinedRow)
			hasRightMatches = true
		}

		// 当左表的某一行在右表中没有匹配（hasRightMatches == false）
		// 且是外连接（outer == true）时，将左表行（leftRow）与 rightEmpty 合并，生成一个“左行 + 右空值”的完整行，表示右表无匹配。
		if n.Outer && !hasRightMatches {
			nullRow := append(n.Left[leftRow], rightEmpty...)
			results = append(results, nullRow)
		}
	}
	return results

}

type HashJoinExec struct {
	Left       Executor
	LeftField  int
	Right      Executor
	RightField int
	Outer      bool
}

func (h *HashJoinExec) Execute(txn Transaction) (ResultSet, error) {
	leftResult, err1 := h.Left.Execute(txn)
	if err1 != nil {
		return nil, err1
	}
	rightResult, err2 := h.Right.Execute(txn)
	if err2 != nil {
		return nil, err2
	}

	leftQuery, ok1 := leftResult.(*QueryResultSet)
	rightQuery, ok2 := rightResult.(*QueryResultSet)
	if !ok1 || !ok2 {
		return nil, errors.New("HashJoinExec Invalid return ResultSet")
	}

	right := make(map[string][][]*sql.ValueData)
	for i := range rightQuery.Rows {
		keyStr := encodeKey(rightQuery.Rows[i][h.RightField])
		right[keyStr] = append(right[keyStr], rightQuery.Rows[i])
	}

	columns := append(leftQuery.Columns, rightQuery.Columns...)

	newRows := [][]*sql.ValueData{}

	for i := range leftQuery.Rows {
		row := leftQuery.Rows[i]
		keyStr := encodeKey(leftQuery.Rows[i][h.LeftField])
		if hit, ok := right[keyStr]; ok {

			for _, rightRow := range hit {
				newRow := append(row, rightRow...)
				newRows = append(newRows, newRow)
			}

		} else if h.Outer {

			empty := make([]*sql.ValueData, len(rightQuery.Columns))
			for i1 := range empty {
				empty[i1] = &sql.ValueData{
					Type: sql.NullType,
				}
			}
			newRow := append(leftQuery.Rows[i], empty...)
			newRows = append(newRows, newRow)
		}
	}
	return &QueryResultSet{
		Columns: columns,
		Rows:    newRows,
	}, nil
}
