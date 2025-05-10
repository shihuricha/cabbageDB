package catalog

type Executor interface {
	Execute(txc Transaction) (ResultSet, error)
}

func ExecBuild(node Node) Executor {
	switch v := node.(type) {
	case *AggregationNode:
		return &AggregationExec{
			Source:       ExecBuild(v.Source),
			Aggregates:   v.Aggregates,
			Accumulators: make(map[string][]Accumulator),
		}
	case *CreateTableNode:
		return &CreateTableExec{Table: v.Schema}
	case *DeleteNode:
		return &DeteleExec{Table: v.TableName, Source: ExecBuild(v.Source)}
	case *DropTableNode:
		return &DropTableExec{Table: v.TableName}
	case *FilterNode:
		return &FilterExec{Source: ExecBuild(v.Source), Predicate: v.Predicate}
	case *HashJoinNode:
		return &HashJoinExec{
			Left:       ExecBuild(v.Left),
			LeftField:  v.LeftField.Index,
			Right:      ExecBuild(v.Right),
			RightField: v.RightField.Index,
			Outer:      v.Outer,
		}
	case *IndexLookupNode:
		return &IndexLookupExec{
			Table:  v.Table,
			Column: v.ColumnName,
			Values: v.Values,
		}
	case *InsertNode:
		return &InsertExec{
			Table:   v.TableName,
			Columns: v.ColumnNames,
			Rows:    v.Expressions,
		}

	case *KeyLookupNode:
		return &KeyLookupExec{
			Table: v.TableName,
			Keys:  v.Keys,
		}
	case *LimitNode:
		return &LimitExec{
			Source: ExecBuild(v.Source),
			Limit:  v.Limit,
		}
	case *NestedLoopJoinNode:
		return &NestedLoopJoinExec{
			Left:      ExecBuild(v.Left),
			Right:     ExecBuild(v.Right),
			Predicate: v.Predicate,
			Outer:     v.Outer,
		}
	case *NothingNode:
		return &NothingExec{}
	case *OffsetNode:
		return &OffsetExec{
			Source: ExecBuild(v.Source),
			Offset: v.Num,
		}
	case *OrderNode:
		return &OrderExec{
			Source: ExecBuild(v.Source),
			Orders: v.Orders,
		}
	case *ProjectionNode:
		return &ProjectionExec{
			Source:      ExecBuild(v.Source),
			Expressions: v.Expressions,
		}
	case *ScanNode:
		return &ScanExec{
			Table:  v.TableName,
			Filter: v.Filter,
		}
	case *UpdateNode:
		return &UpdateExec{
			Table:       v.TableName,
			Source:      ExecBuild(v.Source),
			Expressions: v.Expressions,
		}
	}
	return nil
}
