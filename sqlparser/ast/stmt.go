package ast

type Stmt interface {
	StmtIter()
}

type BeginStmt struct {
	ReadOnly bool
	AsOf     uint64
}

func (b *BeginStmt) StmtIter() {
}

type CreateStmt struct {
	Name    string
	Columns []*Column
}

func (c *CreateStmt) StmtIter() {
}

type SelectStmt struct {
	Select  []*ExprAS
	From    []FromItem
	Where   Expression
	GroupBy []Expression
	Having  Expression
	Order   []*Order
	Offset  Expression
	Limit   Expression
}

func (s *SelectStmt) StmtIter() {
}

type CommitStmt struct {
}

func (c *CommitStmt) StmtIter() {
}

type RollbackStmt struct {
}

func (r *RollbackStmt) StmtIter() {
}

type ExplainStmt struct {
	Stmt Stmt
}

func (e *ExplainStmt) StmtIter() {
}

type DropTableStmt struct {
	TableName string
}

func (d *DropTableStmt) StmtIter() {
}

type DeleteStmt struct {
	TableName string
	Where     Expression
}

func (d *DeleteStmt) StmtIter() {
}

type InsertStmt struct {
	TableName string
	Columns   []string
	Values    [][]Expression
}

func (i *InsertStmt) StmtIter() {
}

type UpdateStmt struct {
	TableName string
	Set       []*ExprColumn
	Where     Expression
}

func (u *UpdateStmt) StmtIter() {
}
