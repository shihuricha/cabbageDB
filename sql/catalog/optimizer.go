package catalog

import (
	"cabbageDB/sql"
	"cabbageDB/sql/expr"
	"cabbageDB/sqlparser/ast"
)

type Optimizer interface {
	OptimizerIter(Node) Node
}

type ConstantFolder struct {
}

func (c *ConstantFolder) OptimizerIter(node Node) Node {

	node = NodeTransform(node, func(node1 Node) Node {
		return node1
	}, func(node1 Node) Node {
		return NodeTransformExpressions(node1, func(expression1 expr.Expression) expr.Expression {
			if !expr.Contains(expression1, func(expression2 expr.Expression) bool {
				if _, ok := expression2.(*expr.Field); ok {
					return ok
				}
				return false
			}) {
				return &expr.Constant{
					Value: expression1.Evaluate(nil),
				}
			}
			return expression1
		}, func(expression1 expr.Expression) expr.Expression {
			return expression1
		})
	})
	return node
}

type FilterPushdown struct {
}

func (f *FilterPushdown) OptimizerIter(node Node) Node {
	node = NodeTransform(node, func(node1 Node) Node {
		switch v := node1.(type) {
		case *FilterNode:
			remainder := f.PushDown(v.Predicate, v.Source)
			if remainder != nil {
				return &FilterNode{
					Source:    v.Source,
					Predicate: remainder,
				}
			} else {
				return &FilterNode{
					Source: v.Source,
					Predicate: &expr.Constant{Value: &sql.ValueData{
						Type:  sql.BoolType,
						Value: true,
					}},
				}
			}
		case *NestedLoopJoinNode:
			predicate := f.PushDownJoin(v.Predicate, v.Left, v.Right, v.LeftSize, v.Type)
			return &NestedLoopJoinNode{
				Left:      v.Left,
				LeftSize:  v.LeftSize,
				Right:     v.Right,
				Predicate: predicate,
				Outer:     v.Outer,
				Type:      v.Type,
			}
		}
		return node1

	}, func(node1 Node) Node {
		return node1
	})
	return node
}

func (f *FilterPushdown) PushDown(expression expr.Expression, target Node) expr.Expression {
	switch v := target.(type) {
	case *ScanNode:
		newFilter := v.Filter
		if newFilter != nil {
			expression = &expr.And{
				L: expression,
				R: newFilter,
			}
		}
		v.Filter = expression
		return nil
	case *NestedLoopJoinNode:
		newPred := v.Predicate
		if newPred != nil {
			expression = &expr.And{
				L: expression,
				R: newPred,
			}
		}
		v.Predicate = expression
		return nil
	case *FilterNode:
		v.Predicate = &expr.And{
			L: &expr.Constant{Value: &sql.ValueData{
				Type: sql.NullType,
			}},
			R: v.Predicate,
		}
		return nil
	default:
		return expression
	}
}
func (f *FilterPushdown) PushDownJoin(predicate expr.Expression, left Node, right Node, boundary int, joinType ast.JoinType) expr.Expression {
	cnf := expr.IntoCnfList(predicate)
	pushLeft, cnf := partitionCNF(cnf, func(expr1 expr.Expression) bool {
		if field, ok := expr1.(*expr.Field); ok {
			return field.Index >= boundary
		}
		return false
	})
	// 可能会出现既不符合左推又不符合右推的情况
	pushRight, cnf := partitionCNF(cnf, func(expr1 expr.Expression) bool {
		if field, ok := expr1.(*expr.Field); ok {
			return field.Index < boundary
		}
		return false
	})
	for i := range cnf {

		v, ok := cnf[i].(*expr.Equal)
		if !ok {
			continue
		}

		v1, ok1 := v.L.(*expr.Field)
		v2, ok2 := v.R.(*expr.Field)

		if !(ok1 && ok2) {
			continue
		}
		var l, r int
		var ln, rn *expr.ColumnField
		if v1.Index <= v2.Index {
			l = v1.Index
			ln = v1.ColumnField
			r = v2.Index
			rn = v2.ColumnField
		} else {
			l = v2.Index
			ln = v2.ColumnField
			r = v1.Index
			rn = v1.ColumnField
		}

		flag := false
		for i1 := range pushLeft {
			lvals := expr.AsLookup(pushLeft[i1], l)
			if lvals != nil {
				flag = true
				pushRight = append(pushRight, expr.FromLookup(r, rn, lvals))
			}
		}
		if flag {
			continue
		}
		for i1 := range pushRight {
			lvals := expr.AsLookup(pushRight[i1], r)
			if lvals != nil {
				pushLeft = append(pushLeft, expr.FromLookup(l, ln, lvals))
			}
		}

	}

	pushLeftExpr := expr.FromCnfList(&pushLeft)
	if joinType != ast.LeftJoin && pushLeftExpr != nil {
		remainder := f.PushDown(pushLeftExpr, left)
		if remainder != nil {
			cnf = append(cnf, remainder)
		}
	}

	pushRightExpr := expr.FromCnfList(&pushRight)
	if joinType != ast.RightJoin && pushRightExpr != nil {
		pushRightExpr = expr.Transform(pushRightExpr, func(expr1 expr.Expression) expr.Expression {
			if v, ok := expr1.(*expr.Field); ok {
				return &expr.Field{
					Index:       v.Index - boundary,
					ColumnField: v.ColumnField,
				}
			}
			return expr1
		}, func(expr expr.Expression) expr.Expression {
			return expr
		})

		remainder := f.PushDown(pushRightExpr, right)
		if remainder != nil {
			cnf = append(cnf, remainder)
		}
	}
	return expr.FromCnfList(&cnf)
}

// 核心分区逻辑
func partitionCNF(cnf []expr.Expression, fn expr.ExprFnBool) (push []expr.Expression, remaining []expr.Expression) {
	for _, expr1 := range cnf {
		// 检查是否包含右侧字段的引用
		containsRight := expr.Contains(expr1, fn)

		// 如果不包含右侧字段，加入 push
		if !containsRight {
			push = append(push, expr1)
		} else {
			remaining = append(remaining, expr1)
		}
	}
	return
}

type IndexLookupOpt struct {
	Catalog Catalog
}

func (i *IndexLookupOpt) WrapCnf(node Node, cnf *[]expr.Expression) Node {
	predicate := expr.FromCnfList(cnf)
	if predicate != nil {
		return &FilterNode{
			Source:    node,
			Predicate: predicate,
		}
	}
	return node
}

func (i *IndexLookupOpt) OptimizerIter(node Node) Node {

	nodex := NodeTransform(node, func(node1 Node) Node {
		return node1
	}, func(node1 Node) Node {
		switch v := node1.(type) {
		case *ScanNode:
			if v.Filter == nil {
				return node1
			}

			table, err := i.Catalog.MustReadTable(v.TableName)
			if err != nil {
				return node1
			}
			columns := table.Columns
			pk := -1
			for i1 := range columns {
				if columns[i1].PrimaryKey {
					pk = i1
				}
			}
			if pk == -1 {
				return node1
			}
			cnf := expr.IntoCnfList(v.Filter)
			for i2 := range cnf {
				keys := expr.AsLookup(cnf[i2], pk)
				if len(keys) != 0 {
					newCnf := make([]expr.Expression, 0, len(cnf)-1)
					newCnf = append(newCnf, cnf[:i2]...)
					newCnf = append(newCnf, cnf[i2+1:]...)
					cnf = newCnf // 安全替换原切片
					return i.WrapCnf(&KeyLookupNode{
						TableName: v.TableName,
						Alias:     v.Alias,
						Keys:      keys,
					}, &cnf)
				}

				for i3, column := range columns {
					if !column.Index {
						continue
					}
					if i2 >= len(cnf) {
						continue
					}
					values := expr.AsLookup(cnf[i2], i3)
					newCnf := make([]expr.Expression, 0, len(cnf)-1)
					newCnf = append(newCnf, cnf[:i2]...)
					newCnf = append(newCnf, cnf[i2+1:]...)
					cnf = newCnf
					if values != nil {
						return i.WrapCnf(&IndexLookupNode{
							Table:      v.TableName,
							Alias:      v.Alias,
							ColumnName: column.Name,
							Values:     values,
						}, &cnf)
					}
				}
			}

			return &ScanNode{
				TableName: v.TableName,
				Alias:     v.Alias,
				Filter:    v.Filter,
			}
		}
		return node1
	},
	)

	return nodex

}

// 谓词化简
type NoopCleaner struct {
}

func (n *NoopCleaner) OptimizerIter(node Node) Node {
	node = NodeTransform(node, func(node1 Node) Node {
		return NodeTransformExpressions(node1, func(expr1 expr.Expression) expr.Expression {
			switch v := expr1.(type) {
			case *expr.And:
				v1, ok1 := v.L.(*expr.Constant)
				v2, ok2 := v.R.(*expr.Constant)

				if !ok1 && !ok2 {
					return expr1
				}

				if (v1.Value.Type == sql.BoolType && v1.Value.Value == false) ||
					v1.Value.Type == sql.NullType {
					return &expr.Constant{
						Value: &sql.ValueData{
							Type:  sql.BoolType,
							Value: false,
						},
					}
				}

				if (v2.Value.Type == sql.BoolType && v2.Value.Value == false) ||
					v2.Value.Type == sql.NullType {
					return &expr.Constant{
						Value: &sql.ValueData{
							Type:  sql.BoolType,
							Value: false,
						},
					}
				}

				if v1.Value.Type == sql.BoolType && v1.Value.Value == true {
					return v.R
				}

				if v2.Value.Type == sql.BoolType && v2.Value.Value == true {
					return v.L
				}

			case *expr.Or:
				v1, ok1 := v.L.(*expr.Constant)
				v2, ok2 := v.R.(*expr.Constant)

				if !ok1 && !ok2 {
					return expr1
				}

				if (v1.Value.Type == sql.BoolType && v1.Value.Value == false) ||
					(v1.Value.Type == sql.NullType) {
					return v.R
				}
				if (v2.Value.Type == sql.BoolType && v2.Value.Value == false) ||
					(v2.Value.Type == sql.NullType) {
					return v.L
				}

				if (v1.Value.Type == sql.BoolType && v1.Value.Value == true) ||
					(v2.Value.Type == sql.BoolType && v2.Value.Value == true) {
					return &expr.Constant{
						Value: &sql.ValueData{
							Type:  sql.BoolType,
							Value: true,
						},
					}
				}

			}
			return expr1
		}, func(expr1 expr.Expression) expr.Expression {
			return expr1
		})
	}, func(node1 Node) Node {
		v, ok := node1.(*FilterNode)
		if !ok {
			return node1
		}
		switch v2 := v.Predicate.(type) {
		case *expr.Constant:
			if v2.Value.Type == sql.BoolType && v2.Value.Value == true {
				return v.Source
			}
		default:
			return &FilterNode{Source: v.Source, Predicate: v.Predicate}
		}
		return node1
	})
	return node

}

type JoinType struct {
}

func (j *JoinType) OptimizerIter(node Node) Node {
	return NodeTransform(node, func(node1 Node) Node {
		v, ok := node1.(*NestedLoopJoinNode)
		if !ok {
			return node1
		}
		v1, ok1 := v.Predicate.(*expr.Equal)
		if !ok1 {
			return node1
		}

		v2, ok2 := v1.L.(*expr.Field)
		v3, ok3 := v1.R.(*expr.Field)
		if !ok2 && !ok3 {
			return &NestedLoopJoinNode{
				Left:      v.Left,
				LeftSize:  v.LeftSize,
				Right:     v.Right,
				Predicate: &expr.Equal{L: v1.L, R: v1.R},
				Outer:     v.Outer,
			}
		}
		var leftFiled, rightField *expr.Field
		if v2.Index < v.LeftSize {
			leftFiled = &expr.Field{
				Index:       v2.Index,
				ColumnField: v2.ColumnField,
			}
			rightField = &expr.Field{
				Index:       v3.Index - v.LeftSize,
				ColumnField: v3.ColumnField,
			}
		} else {
			leftFiled = &expr.Field{
				Index:       v3.Index,
				ColumnField: v3.ColumnField,
			}
			rightField = &expr.Field{
				Index:       v2.Index - v.LeftSize,
				ColumnField: v2.ColumnField,
			}
		}
		return &HashJoinNode{
			Left:       v.Left,
			LeftField:  leftFiled,
			Right:      v.Right,
			RightField: rightField,
			Outer:      v.Outer,
		}
	}, func(node1 Node) Node {
		return node1
	})
}
