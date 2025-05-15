package catalog

import (
	"cabbageDB/sql"
	"cabbageDB/sql/expr"
	"cabbageDB/sqlparser/ast"
	"errors"
)

type Planner struct {
	Catalog Catalog
}

func (p *Planner) Build(statement ast.Stmt) (*Plan, error) {

	node, err := p.BuildStatement(statement)
	if err != nil {
		return nil, err
	}
	return &Plan{
		Node: node,
	}, nil
}

func (p *Planner) BuildStatement(statement ast.Stmt) (Node, error) {
	switch v := statement.(type) {
	case *ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt:
		return nil, errors.New("Unexpected transaction statement")
	case *ast.ExplainStmt:
		return nil, errors.New("Unexpected explain statement")
	case *ast.CreateStmt:
		var columns []*Column
		for _, c := range v.Columns {
			//关于nullable的处理没有做
			var defaultValue *sql.ValueData

			nullable := true //默认为true

			// 如果显示 即c.Nullabl 为true 为空
			// 如果是主键为 flase

			if !c.Nullable || c.PrimaryKey {
				nullable = false // 如果显式设置为可以为false 以及
			}

			if c.Default == nil {
				if nullable {
					defaultValue = &sql.ValueData{
						Type: sql.NullType,
					}
				}
			} else {
				defaultValue = EvaluateConstant(c.Default)
				if defaultValue == nil {
					return nil, errors.New(c.Name + " can't use this default value")
				}
			}

			column := Column{
				Name:       c.Name,
				DataType:   c.ColumnType,
				PrimaryKey: c.PrimaryKey,
				NullAble:   nullable,
				Default:    defaultValue,
				Index:      c.Index && !c.PrimaryKey,
				Unique:     c.Unique || c.PrimaryKey,
			}

			if c.Unique {
				column.Index = true
			}

			if c.References != nil {
				column.Reference = &ReferenceField{
					TableName:  c.References.TableName,
					ColumnName: c.References.ColumnName,
				}
			}

			columns = append(columns, &column)

		}

		return &CreateTableNode{
			Schema: &Table{
				Name:    v.Name,
				Columns: columns,
			},
		}, nil
	case *ast.DropTableStmt:
		return &DropTableNode{
			TableName: v.TableName,
		}, nil
	case *ast.DeleteStmt:
		table, err := p.Catalog.MustReadTable(v.TableName)
		if err != nil {
			return nil, err
		}
		scope := FormTable(table)
		return &DeleteNode{
			TableName: v.TableName,
			Source: &ScanNode{
				TableName: v.TableName,
				Alias:     "",
				Filter:    BuildExpression(scope, v.Where),
			},
		}, nil
	case *ast.InsertStmt:
		var expressions [][]expr.Expression
		for _, columnV := range v.Values {
			var exprs []expr.Expression
			for _, expr := range columnV {
				exprs = append(exprs, BuildExpression(SetConstant(), expr))
			}
			expressions = append(expressions, exprs)
		}
		return &InsertNode{
			TableName:   v.TableName,
			ColumnNames: v.Columns,
			Expressions: expressions,
		}, nil
	case *ast.UpdateStmt:
		table, err := p.Catalog.MustReadTable(v.TableName)
		if err != nil {
			return nil, err
		}
		scope := FormTable(table)
		var expressions []*UpdateExpr
		for _, expr := range v.Set {
			updateExpr := UpdateExpr{
				Index: scope.Resolve("", expr.ColumnName),
				Name:  expr.ColumnName,
				Expr:  BuildExpression(scope, expr.Expr),
			}
			expressions = append(expressions, &updateExpr)
		}

		return &UpdateNode{
			TableName: v.TableName,
			Source: &ScanNode{
				TableName: v.TableName,
				Alias:     "",
				Filter:    BuildExpression(scope, v.Where),
			},
			Expressions: expressions,
		}, nil
	case *ast.SelectStmt:
		var node Node
		scope := NewScope()
		var err error
		if len(v.From) != 0 {
			node, err = p.BuildFromClause(scope, v.From)
		} else {
			node = &NothingNode{}
		}
		if err != nil {
			return nil, err
		}

		if v.Where != nil {
			node = &FilterNode{
				Source:    node,
				Predicate: BuildExpression(scope, v.Where),
			}
		}

		hidden := 0
		if len(v.Select) == 1 && v.Select[0].Expr == nil {
			// 兼容下select *
			v.Select = nil
		}
		num := 0

		if v.Select != nil {
			if v.Having != nil {

				v.Having, num = InjectHidden(v.Having, &v.Select)
				hidden = hidden + num
			}
			for i := range v.Order {
				v.Order[i].Expr, num = InjectHidden(v.Order[i].Expr, &v.Select)
				hidden = hidden + num
			}

			aggregates := ExtractAggregates(v.Select)
			groups := ExtractGroups(v.Select, v.GroupBy, len(aggregates))
			if len(aggregates) > 0 || len(groups) > 0 {
				node = BuildAggregation(scope, node, groups, aggregates)
			}

			var expressions []*ExprAs
			allFieldIdx := 0
			for _, expr1 := range v.Select {

				if v1, ok := expr1.Expr.(*ast.Field); ok && v1.ColumnName == "" {

					if table1, ok1 := scope.Tables[v1.TableName]; ok1 {
						for i := range table1.Columns {
							field := &expr.Field{
								Index: allFieldIdx,
								ColumnField: &expr.ColumnField{
									TbaleName:  v1.TableName,
									ColumnName: table1.Columns[i].Name,
								},
							}
							needExpr := &ExprAs{
								Expr: field,
							}
							expressions = append(expressions, needExpr)
							allFieldIdx++
						}
						continue
					}

				}

				transExpr := BuildExpression(scope, expr1.Expr)
				if transExpr == nil {
					return nil, errors.New("sql err please check sql")
				}

				expressions = append(expressions, &ExprAs{
					Expr: BuildExpression(scope, expr1.Expr),
					As:   expr1.As,
				})
			}
			scope.Project(expressions)
			node = &ProjectionNode{
				Source:      node,
				Expressions: expressions,
			}
		}

		if v.Having != nil {
			node = &FilterNode{
				Source:    node,
				Predicate: BuildExpression(scope, v.Having),
			}
		}

		if len(v.Order) != 0 {
			var orders []*DirectionExpr
			for _, v1 := range v.Order {

				orders = append(orders,
					&DirectionExpr{
						Expr: BuildExpression(scope, v1.Expr),
						Desc: v1.Desc,
					})
			}

			node = &OrderNode{
				Source: node,
				Orders: orders,
			}
		}

		if v.Offset != nil {
			value := EvaluateConstant(v.Offset)
			if value.Type == sql.IntType && value.Value.(int64) >= 0 {
				node = &OffsetNode{
					Source: node,
					Num:    int(value.Value.(int64)),
				}
			}
		}

		if v.Limit != nil {
			value := EvaluateConstant(v.Limit)
			if value.Type == sql.IntType && value.Value.(int64) >= 0 {
				node = &LimitNode{
					Source: node,
					Limit:  int(value.Value.(int64)),
				}
			}
		}
		if hidden > 0 {
			var exprs []*ExprAs
			for i := 0; i < scope.Len()-hidden; i++ {
				exprs = append(exprs, &ExprAs{
					Expr: &expr.Field{Index: i},
				})
			}
			node = &ProjectionNode{
				Source:      node,
				Expressions: exprs,
			}
		}
		return node, nil
	}

	return nil, errors.New("unknown statement")
}

func InjectHidden(expr1 ast.Expression, selectExprPtr *[]*ast.ExprAS) (ast.Expression, int) {
	selectExpr := *selectExprPtr
	for i, exprAs := range selectExpr {
		if equalExpr(expr1, exprAs.Expr) {
			expr1 = &ast.ColumnIdx{i}
			continue
		}

		if exprAs.As != "" {
			expr1 = ast.Transform(expr1, func(expression ast.Expression) ast.Expression {
				v, ok := expression.(*ast.Field)
				if !ok {
					return expression
				}
				if v.TableName == "" && v.ColumnName == exprAs.As {
					return &ast.ColumnIdx{
						Index: i,
					}
				}
				return expression
			}, func(expression ast.Expression) ast.Expression {
				return expression
			})
		}
	}
	hidden := 0
	expr1 = ast.Transform(expr1, func(expression ast.Expression) ast.Expression {
		if v, ok := expression.(*ast.Function); ok {
			if AggregateFromName(v.FuncName) != -1 {
				if idx, ok2 := v.Args[0].(*ast.ColumnIdx); ok2 {
					if IsAggregate(selectExpr[idx.Index].Expr) {
						return nil
					}
				}
				selectExpr = append(selectExpr, &ast.ExprAS{
					Expr: expression,
				})
				hidden += 1
				return &ast.ColumnIdx{
					Index: len(selectExpr) - 1,
				}
			}
		}
		if _, ok := expression.(*ast.Field); ok {
			selectExpr = append(selectExpr, &ast.ExprAS{
				Expr: expression,
			})
			hidden += 1
			return &ast.ColumnIdx{
				Index: len(selectExpr) - 1,
			}
		}
		return expression

	}, func(expression ast.Expression) ast.Expression {
		return expression
	})
	*selectExprPtr = selectExpr
	return expr1, hidden
}

func EvaluateConstant(expr ast.Expression) *sql.ValueData {
	return BuildExpression(SetConstant(), expr).Evaluate([]*sql.ValueData{})
}

func (p *Planner) BuildFromClause(scope *Scope, from []ast.FromItem) (Node, error) {

	node, err := p.BuildFromItem(scope, from[0])
	if err != nil {
		return nil, err
	}
	for _, item := range from[1:] {
		right, err1 := p.BuildFromItem(scope, item)
		if err1 != nil {
			return nil, err1
		}
		node = &NestedLoopJoinNode{
			Left:     node,
			LeftSize: scope.Len(),
			Right:    right,
			Outer:    false,
		}
	}
	return node, nil
}

func (p *Planner) BuildFromItem(scope *Scope, item ast.FromItem) (Node, error) {
	switch v := item.(type) {
	case *ast.FromItemTable:
		name := v.Alias
		if name == "" {
			name = v.Name
		}
		table, err := p.Catalog.MustReadTable(v.Name)
		if err != nil {
			return nil, err
		}
		scope.AddTable(
			name,
			table,
		)
		return &ScanNode{TableName: v.Name, Alias: v.Alias, Filter: nil}, nil
	case *ast.FromItemJoinTable:
		if v.Type == ast.RightJoin {
			v.Right, v.Left = v.Left, v.Right
		}
		left, err := p.BuildFromItem(scope, v.Left)
		if err != nil {
			return nil, err
		}
		leftSize := scope.Len()
		right, err := p.BuildFromItem(scope, v.Right)
		if err != nil {
			return nil, err
		}
		predicate := BuildExpression(scope, v.Predicate)
		var outer bool
		if v.Type == ast.LeftJoin || v.Type == ast.RightJoin {
			outer = true
		}
		var node Node

		node = &NestedLoopJoinNode{
			Left:      left,
			LeftSize:  leftSize,
			Right:     right,
			Predicate: predicate,
			Outer:     outer,
		}

		return node, nil

	}
	return nil, errors.New("BuildFromItem Invalid return Node")
}

func BuildExpression(scope *Scope, expr1 ast.Expression) expr.Expression {
	switch v := expr1.(type) {
	case *ast.Literal:
		return &expr.Constant{
			Value: &sql.ValueData{
				Type:  v.Type,
				Value: v.Value,
			},
		}
	case *ast.ColumnIdx:
		return &expr.Field{
			Index:       v.Index,
			ColumnField: scope.GetLabel(v.Index),
		}
	case *ast.Field:

		index := scope.Resolve(v.TableName, v.ColumnName)
		if index == -1 {
			return nil
		}
		return &expr.Field{
			Index: index,
			ColumnField: &expr.ColumnField{
				TbaleName:  v.TableName,
				ColumnName: v.ColumnName,
			},
		}
	case *ast.Function:
		return nil
	case *ast.Operation:
		switch v1 := v.Operation.(type) {
		case *ast.AndOper:
			return &expr.And{
				L: BuildExpression(scope, v1.L),
				R: BuildExpression(scope, v1.R),
			}
		case *ast.NotOper:
			return &expr.Not{
				L: BuildExpression(scope, v1.L),
			}
		case *ast.OrOper:
			return &expr.Or{
				L: BuildExpression(scope, v1.L),
				R: BuildExpression(scope, v1.R),
			}
		case *ast.EqualOper:
			return &expr.Equal{
				L: BuildExpression(scope, v1.L),
				R: BuildExpression(scope, v1.R),
			}
		case *ast.GreaterThanOper:
			return &expr.GreaterThan{
				L: BuildExpression(scope, v1.L),
				R: BuildExpression(scope, v1.R),
			}
		case *ast.GreaterThanOrEqualOper:
			return &expr.Or{
				L: &expr.GreaterThan{
					L: BuildExpression(scope, v1.L),
					R: BuildExpression(scope, v1.R),
				},
				R: &expr.Equal{
					L: BuildExpression(scope, v1.L),
					R: BuildExpression(scope, v1.R),
				},
			}
		case *ast.IsNullOper:
			return &expr.IsNull{
				L: BuildExpression(scope, v1.L),
			}
		case *ast.LessThanOper:
			return &expr.LessThan{
				L: BuildExpression(scope, v1.L),
				R: BuildExpression(scope, v1.R),
			}
		case *ast.LessThanOrEqualOper:
			return &expr.Or{
				L: &expr.LessThan{
					L: BuildExpression(scope, v1.L),
					R: BuildExpression(scope, v1.R),
				},
				R: &expr.Equal{
					L: BuildExpression(scope, v1.L),
					R: BuildExpression(scope, v1.R),
				},
			}
		case *ast.LikeOper:
			return &expr.Like{
				L: BuildExpression(scope, v1.L),
				R: BuildExpression(scope, v1.R),
			}
		case *ast.NotEqualOper:
			return &expr.Not{
				L: &expr.Equal{
					L: BuildExpression(scope, v1.L),
					R: BuildExpression(scope, v1.R),
				},
			}
		case *ast.AssertOper:
			return &expr.Assert{
				L: BuildExpression(scope, v1.L),
			}
		case *ast.AddOper:
			return &expr.Add{
				L: BuildExpression(scope, v1.L),
				R: BuildExpression(scope, v1.R),
			}
		case *ast.DivideOper:
			return &expr.Divide{
				L: BuildExpression(scope, v1.L),
				R: BuildExpression(scope, v1.R),
			}
		case *ast.ExponentiateOper:
			return &expr.Exponentiate{
				L: BuildExpression(scope, v1.L),
				R: BuildExpression(scope, v1.R),
			}
		case *ast.FactorialOper:
			return &expr.Factorial{
				L: BuildExpression(scope, v1.L),
			}
		case *ast.ModuloOper:
			return &expr.Modulo{
				L: BuildExpression(scope, v1.L),
				R: BuildExpression(scope, v1.R),
			}
		case *ast.MultiplyOper:
			return &expr.Multiply{
				L: BuildExpression(scope, v1.L),
				R: BuildExpression(scope, v1.R),
			}
		case *ast.NegateOper:
			return &expr.Negate{
				L: BuildExpression(scope, v1.L),
			}
		case *ast.SubtractOper:
			return &expr.Subtract{
				L: BuildExpression(scope, v1.L),
				R: BuildExpression(scope, v1.R),
			}
		}
	}
	return nil

}

type Scope struct {
	Constant    bool
	Tables      map[string]*Table
	Columns     []*expr.ColumnField
	Qualified   map[string]int
	UnQualified map[string]int
	Ambiguous   map[string]struct{}
}

func NewScope() *Scope {
	return &Scope{
		Constant:    false,
		Tables:      make(map[string]*Table),
		Columns:     []*expr.ColumnField{},
		Qualified:   make(map[string]int),
		UnQualified: make(map[string]int),
		Ambiguous:   make(map[string]struct{}),
	}
}

func SetConstant() *Scope {
	scope := NewScope()
	scope.Constant = true
	return scope
}

func FormTable(table *Table) *Scope {
	scope := NewScope()
	scope.AddTable(table.Name, table)
	return scope
}

func (s *Scope) AddTable(tableName string, table *Table) {
	if s.Constant {
		return
	}

	if _, ok := s.Tables[tableName]; ok {
		return
	}
	for _, c := range table.Columns {
		s.AddColumn(tableName, c.Name)
	}
	s.Tables[tableName] = table
}

func (s *Scope) AddColumn(tableName string, label string) {
	if label != "" {
		if tableName != "" {
			s.Qualified[tableName+";"+label] = len(s.Columns)
		}

		if _, ok := s.Ambiguous[label]; !ok {
			if _, ok1 := s.UnQualified[label]; !ok1 {
				s.UnQualified[label] = len(s.Columns)
			} else {
				delete(s.UnQualified, label)
				s.Ambiguous[label] = struct{}{}
			}

		}
	}
	s.Columns = append(s.Columns, &expr.ColumnField{
		TbaleName:  tableName,
		ColumnName: label,
	})

}

func (s *Scope) GetColumn(index int) *expr.ColumnField {
	if s.Constant {
		return nil
	}
	return s.Columns[index]
}

func (s *Scope) GetLabel(index int) *expr.ColumnField {
	columnField := s.GetColumn(index)
	if columnField.ColumnName != "" && columnField.ColumnName != "" {
		return columnField
	}
	return nil
}

func (s *Scope) Resolve(tableName string, columnName string) int {
	if s.Constant {
		return -1
	}
	if tableName != "" {
		if _, ok := s.Tables[tableName]; !ok {
			return -1
		}
		if v, ok := s.Qualified[tableName+";"+columnName]; ok {
			return v
		} else {
			return -1
		}
	} else if _, ok := s.Ambiguous[columnName]; ok {
		return -1
	} else {
		if v, ok1 := s.UnQualified[columnName]; ok1 {
			return v
		} else {
			return -1
		}
	}
}

func (s *Scope) Len() int {
	return len(s.Columns)
}

func (s *Scope) Project(expressions []*ExprAs) {
	if s.Constant {
		return
	}

	scope := NewScope()
	scope.Tables = s.Tables
	for _, exprAs := range expressions {
		if exprAs.As != "" {
			scope.AddColumn("", exprAs.As)
			continue
		}

		filed, ok := exprAs.Expr.(*expr.Field)
		if !ok {
			// 占位
			scope.AddColumn("", "")

			continue
		}

		if filed.ColumnField == nil && len(scope.Columns) >= filed.Index {
			f := s.Columns[filed.Index]
			scope.AddColumn(f.TbaleName, f.ColumnName)
			continue
		}

		if filed.ColumnField.ColumnName != "" && filed.ColumnField.TbaleName != "" {
			scope.AddColumn(filed.ColumnField.TbaleName, filed.ColumnField.ColumnName)
			continue
		}

		if filed.ColumnField.ColumnName != "" && filed.ColumnField.TbaleName == "" {
			index, ok1 := s.UnQualified[filed.ColumnField.ColumnName]
			if ok1 {
				v := s.Columns[index]
				scope.AddColumn(v.TbaleName, v.ColumnName)
			}
			continue
		}

	}
	s.Qualified = scope.Qualified
	s.Ambiguous = scope.Ambiguous
	s.UnQualified = scope.UnQualified
	s.Columns = scope.Columns

}

func AggregateFromName(name string) Aggregate {
	switch name {
	case "avg":
		return Average
	case "count":
		return Count
	case "min":
		return Min
	case "max":
		return Max
	case "sum":
		return Sum
	}
	return -1
}

func IsAggregate(expr ast.Expression) bool {
	return ast.Contains(expr, func(expr2 ast.Expression) bool {
		if v, ok := expr2.(*ast.Function); ok {
			if AggregateFromName(v.FuncName) != -1 {
				return true
			}
			return false
		}
		return false
	})
}

type AggregateExpr struct {
	Aggregate Aggregate
	Expr      ast.Expression
}

func ExtractAggregates(exprAss []*ast.ExprAS) []*AggregateExpr {
	var agg []*AggregateExpr
	for i := range exprAss {
		exprAss[i].Expr = ast.Transform(exprAss[i].Expr, func(expression ast.Expression) ast.Expression {
			if v, ok := expression.(*ast.Function); ok && len(v.Args) == 1 {

				aggregate := AggregateFromName(v.FuncName)

				if aggregate != -1 {
					agg = append(agg, &AggregateExpr{
						Aggregate: aggregate,
						Expr:      v.Args[0],
					})
					v.Args = v.Args[1:]
					return &ast.ColumnIdx{
						len(agg) - 1,
					}
				} else {
					return exprAss[i].Expr
				}

			}
			return expression
		}, func(expression ast.Expression) ast.Expression {
			return expression
		})
	}

	return agg
}

func ExtractGroups(exprs []*ast.ExprAS, groupBy []ast.Expression, offset int) []*ast.ExprAS {
	var groups []*ast.ExprAS
	for _, g := range groupBy {
		flag1 := false
		if v, ok := g.(*ast.Field); ok && v.TableName == "" {

			for i := range exprs {
				if exprs[i].As == v.ColumnName {
					groupLen := len(groups)
					// 保存原表达式到groups
					groups = append(groups, &ast.ExprAS{
						Expr: exprs[i].Expr,
						As:   exprs[i].As,
					})
					// 替换表达式里的内容
					exprs[i].Expr = &ast.ColumnIdx{
						Index: offset + groupLen,
					}
					flag1 = true
					break
				}
			}
		}
		if flag1 {
			continue
		}

		flag2 := false

		for i := range exprs {
			if equalFieldExpr(exprs[i].Expr, g) {
				groupLen := len(groups)
				groups = append(groups, &ast.ExprAS{
					Expr: exprs[i].Expr,
					As:   exprs[i].As,
				})
				exprs[i].Expr = &ast.ColumnIdx{
					Index: offset + groupLen,
				}
				flag2 = true
				break
			}
		}
		if flag2 {
			continue
		}
		groups = append(groups, &ast.ExprAS{
			Expr: g,
		})
	}
	for i := range groups {
		if IsAggregate(groups[i].Expr) {
			return nil
		}
	}
	return groups
}

func BuildAggregation(scope *Scope, source Node, groups []*ast.ExprAS, aggregations []*AggregateExpr) Node {
	aggregates := []Aggregate{}
	var exprAss []*ExprAs
	for _, aggr := range aggregations {
		aggregates = append(aggregates, aggr.Aggregate)
		exprAss = append(exprAss, &ExprAs{
			Expr: BuildExpression(scope, aggr.Expr),
		})
	}

	for _, group := range groups {
		exprAss = append(exprAss, &ExprAs{
			Expr: BuildExpression(scope, group.Expr),
			As:   group.As,
		})
	}

	// 创建临时投影列表
	tmpProjections := make([]*ExprAs, len(exprAss))
	for i, e := range exprAss {
		if i < len(aggregates) {
			// 临时投影为 Null，但不影响原始表达式
			tmpProjections[i] = &ExprAs{Expr: &expr.Constant{
				Value: &sql.ValueData{
					Type: sql.NullType,
				},
			}}
		} else {
			tmpProjections[i] = e
		}
	}
	scope.Project(tmpProjections) // 仅用于字段解析
	return &AggregationNode{
		Source:     &ProjectionNode{Source: source, Expressions: exprAss},
		Aggregates: aggregates,
	}
}

func equalExpr(expr1, expr2 ast.Expression) bool {
	if expr1 == nil || expr2 == nil {
		return false
	}
	switch expr1.(type) {
	case *ast.Field:
		return equalFieldExpr(expr1, expr2)
	case *ast.Function:
		return equalFuncExpr(expr1, expr2)
	default:
		return false
	}
}

func equalFieldExpr(expr1, expr2 ast.Expression) bool {
	e1, ok1 := expr1.(*ast.Field)
	e2, ok2 := expr2.(*ast.Field)
	if !ok1 || !ok2 {
		return false
	}
	if e1.ColumnName == e2.ColumnName && e1.TableName == e2.TableName {
		return true
	}
	return false
}

func equalFuncExpr(expr1, expr2 ast.Expression) bool {
	e1, ok1 := expr1.(*ast.Function)
	e2, ok2 := expr2.(*ast.Function)
	if !ok1 || !ok2 {
		return false
	}
	if len(e1.Args) != len(e2.Args) {
		return false
	}

	if e1.FuncName != e2.FuncName {
		return false
	}
	for i := range e1.Args {
		return equalFieldExpr(e1.Args[i], e2.Args[i])
	}
	return true
}
