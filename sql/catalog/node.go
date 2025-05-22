package catalog

import (
	"cabbageDB/sql"
	"cabbageDB/sql/expr"
	"cabbageDB/sqlparser/ast"
	"encoding/json"
	"fmt"
	"strings"
)

type Node interface {
	NodeIter()
}

type NothingNode struct {
}

func (n *NothingNode) NodeIter() {

}

type AggregationNode struct {
	Source     Node
	Aggregates []Aggregate
}

func (a *AggregationNode) NodeIter() {

}

type CreateTableNode struct {
	Schema *Table
}

func (c *CreateTableNode) NodeIter() {

}

type DeleteNode struct {
	TableName string
	Source    Node
}

func (d *DeleteNode) NodeIter() {
}

type DropTableNode struct {
	TableName string
}

func (d *DropTableNode) NodeIter() {

}

type FilterNode struct {
	Source    Node
	Predicate expr.Expression
}

func (f *FilterNode) NodeIter() {

}

type HashJoinNode struct {
	Left       Node
	LeftField  *expr.Field
	Right      Node
	RightField *expr.Field
	Outer      bool
}

func (h *HashJoinNode) NodeIter() {

}

type CoverIndexNode struct {
	Table   string
	Alias   string
	Columns []string
	Values  [][]*sql.ValueData
}

func (c *CoverIndexNode) NodeIter() {

}

type IndexLookupNode struct {
	Table      string
	Alias      string
	ColumnName string
	Values     []*sql.ValueData
}

func (i *IndexLookupNode) NodeIter() {

}

type InsertNode struct {
	TableName   string
	ColumnNames []string
	Expressions [][]expr.Expression
}

func (i *InsertNode) NodeIter() {

}

type KeyLookupNode struct {
	TableName string
	Alias     string
	Keys      []*sql.ValueData
}

func (k *KeyLookupNode) NodeIter() {

}

type LimitNode struct {
	Source Node
	Limit  int
}

func (l *LimitNode) NodeIter() {

}

type NestedLoopJoinNode struct {
	Left      Node
	LeftSize  int
	Right     Node
	Predicate expr.Expression
	Outer     bool
	Type      ast.JoinType
}

func (n *NestedLoopJoinNode) NodeIter() {

}

type OffsetNode struct {
	Source Node
	Num    int
}

func (o *OffsetNode) NodeIter() {

}

type OrderNode struct {
	Source Node
	Orders []*DirectionExpr
}
type DirectionExpr struct {
	Expr expr.Expression
	Desc bool
}

func (o *OrderNode) NodeIter() {

}

type ProjectionNode struct {
	Source      Node
	Expressions []*ExprAs
}
type ExprAs struct {
	Expr expr.Expression
	As   string
}

func (p *ProjectionNode) NodeIter() {

}

type ScanNode struct {
	TableName string
	Alias     string
	Filter    expr.Expression
}

func (s *ScanNode) NodeIter() {

}

type UpdateNode struct {
	TableName   string
	Source      Node
	Expressions []*UpdateExpr
}

func (u *UpdateNode) NodeIter() {

}

type UpdateExpr struct {
	Index int
	Name  string
	Expr  expr.Expression
}

type Plan struct {
	Node Node
}

func (p *Plan) Execute(txn Transaction) (ResultSet, error) {
	return ExecBuild(p.Node).Execute(txn)
}

func Build(statement ast.Stmt, catalog Catalog) (*Plan, error) {
	planner := &Planner{
		Catalog: catalog,
	}
	return planner.Build(statement)
}

func (p *Plan) Optimize(catalog Catalog) *Plan {
	root := p.Node
	folder := &ConstantFolder{}
	root = folder.OptimizerIter(root)
	pushDown := &FilterPushdown{}
	root = pushDown.OptimizerIter(root)
	indexLookup := &IndexLookupOpt{
		Catalog: catalog,
	}
	root = indexLookup.OptimizerIter(root)
	noopCleaner := &NoopCleaner{}
	root = noopCleaner.OptimizerIter(root)
	joinType := &JoinType{}
	root = joinType.OptimizerIter(root)
	p.Node = root
	return p
}

type Aggregate int

const (
	Average Aggregate = iota
	Count
	Max
	Min
	Sum
)

func (a Aggregate) String() string {
	switch a {
	case Average:
		return "average"
	case Count:
		return "count"
	case Max:
		return "max"
	case Min:
		return "min"
	case Sum:
		return "sum"
	default:
		return ""
	}

}

type NodeFn func(Node) Node

func NodeTransform(node Node, before NodeFn, after NodeFn) Node {
	node = before(node)
	switch v := node.(type) {
	case *CreateTableNode, *DropTableNode, *IndexLookupNode, *InsertNode, *KeyLookupNode, *NothingNode, *ScanNode:
		node = node
	case *AggregationNode:
		node = &AggregationNode{
			Source:     NodeTransform(v.Source, before, after),
			Aggregates: v.Aggregates,
		}
	case *DeleteNode:
		node = &DeleteNode{
			TableName: v.TableName,
			Source:    NodeTransform(v.Source, before, after),
		}
	case *FilterNode:
		node = &FilterNode{
			Source:    NodeTransform(v.Source, before, after),
			Predicate: v.Predicate,
		}
	case *HashJoinNode:
		node = &HashJoinNode{
			Left:       NodeTransform(v.Left, before, after),
			LeftField:  v.LeftField,
			Right:      NodeTransform(v.Right, before, after),
			RightField: v.RightField,
			Outer:      v.Outer,
		}
	case *LimitNode:
		node = &LimitNode{
			Source: NodeTransform(v.Source, before, after),
			Limit:  v.Limit,
		}
	case *NestedLoopJoinNode:
		node = &NestedLoopJoinNode{
			Left:      NodeTransform(v.Left, before, after),
			LeftSize:  v.LeftSize,
			Right:     NodeTransform(v.Right, before, after),
			Predicate: v.Predicate,
			Outer:     v.Outer,
			Type:      v.Type,
		}
	case *OffsetNode:
		node = &OffsetNode{
			Source: NodeTransform(v.Source, before, after),
			Num:    v.Num,
		}
	case *OrderNode:
		node = &OrderNode{
			Source: NodeTransform(v.Source, before, after),
			Orders: v.Orders,
		}
	case *ProjectionNode:
		node = &ProjectionNode{
			Source:      NodeTransform(v.Source, before, after),
			Expressions: v.Expressions,
		}
	case *UpdateNode:
		node = &UpdateNode{
			TableName:   v.TableName,
			Source:      NodeTransform(v.Source, before, after),
			Expressions: v.Expressions,
		}
	}
	node = after(node)
	return node
}

func NodeTransformExpressions(node Node, before expr.ExprFn, after expr.ExprFn) Node {
	switch v := node.(type) {
	case *AggregationNode, *CreateTableNode, *DeleteNode, *DropTableNode, *HashJoinNode, *IndexLookupNode, *KeyLookupNode,
		*LimitNode, *NothingNode, *OffsetNode:
		node = node
	case *FilterNode:
		if v.Predicate != nil {
			node = &FilterNode{
				Source:    v.Source,
				Predicate: expr.Transform(v.Predicate, before, after),
			}
		}
	case *InsertNode:
		var exprs1 [][]expr.Expression
		for i := range v.Expressions {
			var exprs2 []expr.Expression
			for i2 := range v.Expressions[i] {
				exprs2 = append(exprs2, expr.Transform(v.Expressions[i][i2], before, after))
			}
			exprs1 = append(exprs1, exprs2)
		}
		node = &InsertNode{
			TableName:   v.TableName,
			ColumnNames: v.ColumnNames,
			Expressions: exprs1,
		}
	case *OrderNode:
		var exprs []*DirectionExpr
		for i := range v.Orders {
			exprs = append(exprs, &DirectionExpr{
				Expr: expr.Transform(v.Orders[i].Expr, before, after),
				Desc: v.Orders[i].Desc,
			})
		}
	case *NestedLoopJoinNode:
		if v.Predicate != nil {
			node = &NestedLoopJoinNode{
				Left:      v.Left,
				LeftSize:  v.LeftSize,
				Right:     v.Right,
				Predicate: expr.Transform(v.Predicate, before, after),
				Outer:     v.Outer,
				Type:      v.Type,
			}
		}
	case *ProjectionNode:
		var exprs []*ExprAs

		for i := range v.Expressions {
			exprs = append(exprs, &ExprAs{
				Expr: expr.Transform(v.Expressions[i].Expr, before, after),
				As:   v.Expressions[i].As,
			})
		}
		node = &ProjectionNode{
			Source:      v.Source,
			Expressions: exprs,
		}
	case *ScanNode:
		if v.Filter != nil {
			node = &ScanNode{
				TableName: v.TableName,
				Alias:     v.Alias,
				Filter:    expr.Transform(v.Filter, before, after),
			}
		}
	case *UpdateNode:
		var exprs []*UpdateExpr
		for i := range v.Expressions {
			exprs = append(exprs, &UpdateExpr{
				Expr:  expr.Transform(v.Expressions[i].Expr, before, after),
				Name:  v.Expressions[i].Name,
				Index: v.Expressions[i].Index,
			})
		}
		node = &UpdateNode{
			TableName:   v.TableName,
			Source:      v.Source,
			Expressions: exprs,
		}

	}
	return node
}

func FormatNode(node Node, indentLevel int) string {
	indent := strings.Repeat("---> ", indentLevel)
	var builder strings.Builder

	switch n := node.(type) {
	case *AggregationNode:
		data := map[string]interface{}{"Aggregates:": n.Aggregates}
		jsonData, _ := json.Marshal(data)
		builder.WriteString(fmt.Sprintf("%sAggregationNode %s\n", indent, jsonData))
		if n.Source != nil {
			builder.WriteString(FormatNode(n.Source, indentLevel+1))
		}
	case *CreateTableNode:
		data := map[string]interface{}{"Table:": n.Schema}
		jsonData, _ := json.Marshal(data)
		builder.WriteString(fmt.Sprintf("%sCreateTableNode %s\n", indent, jsonData))
	case *DeleteNode:
		data := map[string]interface{}{"TableName:": n.TableName}
		jsonData, _ := json.Marshal(data)
		builder.WriteString(fmt.Sprintf("%sDeleteNode %s\n", indent, jsonData))
		if n.Source != nil {
			builder.WriteString(FormatNode(n.Source, indentLevel+1))
		}
	case *DropTableNode:
		data := map[string]interface{}{"TableName:": n.TableName}
		jsonData, _ := json.Marshal(data)
		builder.WriteString(fmt.Sprintf("%sDropTableNode %s\n", indent, jsonData))
	case *FilterNode:
		data := map[string]interface{}{"Predicate:": n.Predicate}
		jsonData, _ := json.Marshal(data)
		builder.WriteString(fmt.Sprintf("%sFilterNode %s\n", indent, jsonData))
		if n.Source != nil {
			builder.WriteString(FormatNode(n.Source, indentLevel+1))
		}
	case *HashJoinNode:
		data := map[string]interface{}{
			"JoinType": func() string {
				if n.Outer {
					return "LEFT OUTER JOIN"
				}
				return "INNER JOIN"
			}(),
			"LeftField":  n.LeftField,
			"RightField": n.RightField,
		}
		jsonData, _ := json.Marshal(data)
		builder.WriteString(fmt.Sprintf("%sHashJoinNode %s\n", indent, jsonData))

		// 处理左子树
		if n.Left != nil {
			builder.WriteString(fmt.Sprintf("%s---> LEFT:\n", indent))
			builder.WriteString(FormatNode(n.Left, indentLevel+1))
		}

		// 处理右子树
		if n.Right != nil {
			builder.WriteString(fmt.Sprintf("%s---> RIGHT:\n", indent))
			builder.WriteString(FormatNode(n.Right, indentLevel+1))
		}
	case *CoverIndexNode:
		jsonData, _ := json.Marshal(n)
		builder.WriteString(fmt.Sprintf("%sCoverIndexNode%s\n", indent, jsonData))
	case *IndexLookupNode:
		jsonData, _ := json.Marshal(n)
		builder.WriteString(fmt.Sprintf("%sIndexLookupNode%s\n", indent, jsonData))
	case *InsertNode:
		jsonData, _ := json.Marshal(n)
		builder.WriteString(fmt.Sprintf("%sInsertNode%s\n", indent, jsonData))
	case *KeyLookupNode:
		jsonData, _ := json.Marshal(n)
		builder.WriteString(fmt.Sprintf("%sKeyLookupNode%s\n", indent, jsonData))
	case *LimitNode:
		data := map[string]interface{}{"Limit:": n.Limit}
		jsonData, _ := json.Marshal(data)
		builder.WriteString(fmt.Sprintf("%sLimitNode %s\n", indent, jsonData))
		if n.Source != nil {
			builder.WriteString(FormatNode(n.Source, indentLevel+1))
		}
	case *NestedLoopJoinNode:
		jsonData, _ := json.Marshal(n)
		builder.WriteString(fmt.Sprintf("%sNestedLoopJoinNode%s\n", indent, jsonData))
	case *OffsetNode:
		data := map[string]interface{}{"Offset:": n.Num}
		jsonData, _ := json.Marshal(data)
		builder.WriteString(fmt.Sprintf("%sOffsetNode %s\n", indent, jsonData))
		if n.Source != nil {
			builder.WriteString(FormatNode(n.Source, indentLevel+1))
		}
	case *OrderNode:
		data := map[string]interface{}{"Orders:": n.Orders}
		jsonData, _ := json.Marshal(data)
		builder.WriteString(fmt.Sprintf("%sOrderNode %s\n", indent, jsonData))
		if n.Source != nil {
			builder.WriteString(FormatNode(n.Source, indentLevel+1))
		}
	case *ProjectionNode:
		data := map[string]interface{}{"Expressions:": n.Expressions}
		jsonData, _ := json.Marshal(data)
		builder.WriteString(fmt.Sprintf("%sProjectionNode %s\n", indent, jsonData))
		if n.Source != nil {
			builder.WriteString(FormatNode(n.Source, indentLevel+1))
		}
	case *ScanNode:
		jsonData, _ := json.Marshal(n)
		builder.WriteString(fmt.Sprintf("%sScanNode %s\n", indent, jsonData))
	case *UpdateNode:
		data := map[string]interface{}{"TableName": n.TableName, "Expressions:": n.Expressions}
		jsonData, _ := json.Marshal(data)
		builder.WriteString(fmt.Sprintf("%sUpdateNode %s\n", indent, jsonData))
	case *NothingNode:
		builder.WriteString(fmt.Sprintf("%sNothingNode \n", indent))
	}

	return builder.String()
}
