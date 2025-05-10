package expr

import (
	"bytes"
	"cabbageDB/logger"
	"cabbageDB/sql"
	"encoding/gob"
	"fmt"
	"math"
	"regexp"
	"strings"
)

type Expression interface {
	expression()
	Evaluate([]*sql.ValueData) *sql.ValueData
}

type Constant struct {
	Value *sql.ValueData
}

func (c *Constant) Evaluate(row []*sql.ValueData) *sql.ValueData {
	return c.Value
}

func (c *Constant) expression() {}

type Field struct {
	Index       int
	ColumnField *ColumnField
}
type ColumnField struct {
	TbaleName  string
	ColumnName string
}

func (f *Field) expression() {}
func (f *Field) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if len(row) == 0 || f.Index >= len(row) {
		return nil
	}
	return row[f.Index]
}

type And struct {
	L Expression
	R Expression
}

func (a *And) expression() {}
func (a *And) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if a.L == nil || a.R == nil {
		logger.Info("And Evaluate nil")
		return nil
	}
	lhs := a.L.Evaluate(row)
	rhs := a.R.Evaluate(row)

	if lhs == nil || rhs == nil {
		logger.Info("And Sub Evaluate nil")
		return nil
	}

	if lhs.Type == sql.BoolType && rhs.Type == sql.BoolType {
		return &sql.ValueData{
			Type:  sql.BoolType,
			Value: (lhs.Value.(bool) && rhs.Value.(bool)),
		}
	}
	if lhs.Type == sql.BoolType && rhs.Type == sql.NullType ||
		rhs.Type == sql.BoolType && lhs.Type == sql.NullType {
		if !lhs.Value.(bool) || !rhs.Value.(bool) {
			return &sql.ValueData{
				Type:  sql.BoolType,
				Value: false,
			}
		}
		if lhs.Value.(bool) || rhs.Value.(bool) {
			return &sql.ValueData{
				Type: sql.NullType,
			}
		}
	}
	if lhs.Type == sql.NullType && rhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}
	return nil

}

type Not struct {
	L Expression
}

func (n *Not) expression() {}
func (n *Not) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if n.L == nil {
		logger.Info("Not Evaluate nil")
		return nil
	}
	lhs := n.L.Evaluate(row)

	if lhs == nil {
		logger.Info("Not Sub Evaluate nil")
		return nil
	}

	if lhs.Type == sql.BoolType {
		return &sql.ValueData{
			Type:  sql.BoolType,
			Value: lhs.Value,
		}
	}
	if lhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}
	return nil
}

type Or struct {
	L Expression
	R Expression
}

func (o *Or) expression() {}
func (o *Or) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if o.L == nil || o.R == nil {
		logger.Info("Or Evaluate nil")
		return nil
	}
	lhs := o.L.Evaluate(row)
	rhs := o.R.Evaluate(row)

	if lhs == nil || rhs == nil {
		logger.Info("Or Sub Evaluate nil")
		return nil
	}

	if lhs.Type == sql.BoolType && rhs.Type == sql.BoolType {
		return &sql.ValueData{
			Type:  sql.BoolType,
			Value: (lhs.Value.(bool) || rhs.Value.(bool)),
		}
	}

	if lhs.Type == sql.BoolType && rhs.Type == sql.NullType {
		if lhs.Value.(bool) {
			return &sql.ValueData{
				Type:  sql.BoolType,
				Value: true,
			}
		}
	}
	if rhs.Type == sql.BoolType && lhs.Type == sql.NullType {
		if rhs.Value.(bool) {
			return &sql.ValueData{
				Type: sql.NullType,
			}
		}
	}

	if lhs.Type == sql.NullType && rhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}
	return nil

}

type Equal struct {
	L Expression
	R Expression
}

func (e *Equal) expression() {}
func (e *Equal) Evaluate(row []*sql.ValueData) *sql.ValueData {

	if e.L == nil || e.R == nil {
		logger.Info("Equal Evaluate nil")
		return nil
	}
	lhs := e.L.Evaluate(row)
	rhs := e.R.Evaluate(row)

	if lhs == nil || rhs == nil {
		logger.Info("Equal Sub Evaluate nil")
		return nil
	}

	if lhs.Type == sql.NullType || rhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}
	if lhs.Type == rhs.Type {
		return &sql.ValueData{
			Type:  sql.BoolType,
			Value: lhs.Value == rhs.Value,
		}
	}
	return &sql.ValueData{
		Type: sql.NullType,
	}
}

type GreaterThan struct {
	L Expression
	R Expression
}

func (g *GreaterThan) expression() {}
func (g *GreaterThan) Evaluate(row []*sql.ValueData) *sql.ValueData {

	if g.L == nil || g.R == nil {
		logger.Info("GreaterThan Evaluate nil")
		return nil
	}
	lhs := g.L.Evaluate(row)
	rhs := g.R.Evaluate(row)

	if lhs == nil || rhs == nil {
		logger.Info("GreaterThan Sub Evaluate nil")
		return nil
	}

	if lhs.Type == sql.NullType || rhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}
	if lhs.Type == rhs.Type {

		if lhs.Type == sql.IntType {
			return &sql.ValueData{
				Type:  sql.BoolType,
				Value: lhs.Value.(int64) > rhs.Value.(int64),
			}
		}

		if lhs.Type == sql.FloatType {
			return &sql.ValueData{
				Type:  sql.BoolType,
				Value: lhs.Value.(float64) > rhs.Value.(float64),
			}
		}

		if lhs.Type == sql.StringType {
			return &sql.ValueData{
				Type:  sql.BoolType,
				Value: false,
			}
		}
		if lhs.Type == sql.NullType {
			return &sql.ValueData{
				Type: sql.NullType,
			}
		}

	} else {
		if lhs.Type == sql.IntType && rhs.Type == sql.FloatType {
			return &sql.ValueData{
				Type:  sql.BoolType,
				Value: float64(lhs.Value.(int64)) > rhs.Value.(float64),
			}
		}
		if lhs.Type == sql.FloatType && rhs.Type == sql.IntType {
			return &sql.ValueData{
				Type:  sql.BoolType,
				Value: lhs.Value.(float64) > float64(rhs.Value.(int64)),
			}
		}

	}
	return nil
}

type IsNull struct {
	L Expression
}

func (i *IsNull) expression() {}
func (i *IsNull) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if i.L == nil {
		logger.Info("IsNull Evaluate nil")
		return nil
	}
	rhs := i.L.Evaluate(row)

	if rhs == nil {
		logger.Info("IsNull Sub Evaluate nil")
		return nil
	}
	if rhs.Type == sql.NullType {
		return &sql.ValueData{
			Type:  sql.BoolType,
			Value: true,
		}
	} else {
		return &sql.ValueData{
			Type:  sql.BoolType,
			Value: false,
		}
	}
}

type LessThan struct {
	L Expression
	R Expression
}

func (l *LessThan) expression() {}
func (l *LessThan) Evaluate(row []*sql.ValueData) *sql.ValueData {

	if l.L == nil || l.R == nil {
		logger.Info("LessThan Evaluate nil")
		return nil
	}
	lhs := l.L.Evaluate(row)
	rhs := l.R.Evaluate(row)

	if lhs == nil || rhs == nil {
		logger.Info("LessThan Sub Evaluate nil")
		return nil
	}

	if lhs.Type == sql.NullType || rhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}
	if lhs.Type == rhs.Type {

		if lhs.Type == sql.IntType {
			return &sql.ValueData{
				Type:  sql.BoolType,
				Value: lhs.Value.(int64) < rhs.Value.(int64),
			}
		}

		if lhs.Type == sql.FloatType {
			return &sql.ValueData{
				Type:  sql.BoolType,
				Value: lhs.Value.(float64) < rhs.Value.(float64),
			}
		}

		if lhs.Type == sql.StringType {
			return &sql.ValueData{
				Type:  sql.BoolType,
				Value: false,
			}
		}

	} else {
		if lhs.Type == sql.IntType && rhs.Type == sql.FloatType {
			return &sql.ValueData{
				Type:  sql.BoolType,
				Value: float64(lhs.Value.(int64)) < rhs.Value.(float64),
			}
		}
		if lhs.Type == sql.FloatType && rhs.Type == sql.IntType {
			return &sql.ValueData{
				Type:  sql.BoolType,
				Value: lhs.Value.(float64) < float64(rhs.Value.(int64)),
			}
		}

	}
	return nil
}

type Add struct {
	L Expression
	R Expression
}

func (a *Add) expression() {}
func (a *Add) Evaluate(row []*sql.ValueData) *sql.ValueData {

	if a.L == nil || a.R == nil {
		logger.Info("Add Evaluate nil")
		return nil
	}
	lhs := a.L.Evaluate(row)
	rhs := a.R.Evaluate(row)

	if lhs == nil || rhs == nil {
		logger.Info("Add Sub Evaluate nil")
		return nil
	}

	if lhs.Type == sql.StringType || rhs.Type == sql.StringType ||
		lhs.Type == sql.BoolType || rhs.Type == sql.BoolType {
		return nil
	}

	if lhs.Type == sql.NullType || rhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}

	if lhs.Type == rhs.Type {

		if lhs.Type == sql.IntType {
			return &sql.ValueData{
				Type:  sql.IntType,
				Value: lhs.Value.(int64) + rhs.Value.(int64),
			}
		}

		if lhs.Type == sql.FloatType {
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: lhs.Value.(float64) + rhs.Value.(float64),
			}
		}
	} else {
		if lhs.Type == sql.IntType && rhs.Type == sql.FloatType {
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: float64(lhs.Value.(int64)) + rhs.Value.(float64),
			}
		}
		if lhs.Type == sql.FloatType && rhs.Type == sql.IntType {
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: lhs.Value.(float64) + float64(rhs.Value.(int64)),
			}
		}
	}
	return nil
}

type Assert struct {
	L Expression
}

func (a *Assert) expression() {}
func (a *Assert) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if a.L == nil {
		logger.Info("Assert Evaluate nil")
		return nil
	}
	lhs := a.L.Evaluate(row)

	if lhs.Type == sql.FloatType {
		return &sql.ValueData{
			Type:  sql.FloatType,
			Value: lhs.Value,
		}
	}
	if lhs.Type == sql.IntType {
		return &sql.ValueData{
			Type:  sql.IntType,
			Value: lhs.Value,
		}
	}
	if lhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}
	return nil
}

type Divide struct {
	L Expression
	R Expression
}

func (d *Divide) expression() {}
func (d *Divide) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if d.L == nil || d.R == nil {
		logger.Info("Divide Evaluate nil")
		return nil
	}
	lhs := d.L.Evaluate(row)
	rhs := d.R.Evaluate(row)

	if lhs == nil || rhs == nil {
		logger.Info("Divide Sub Evaluate nil")
		return nil
	}

	if lhs.Type == sql.StringType || rhs.Type == sql.StringType ||
		lhs.Type == sql.BoolType || rhs.Type == sql.BoolType {
		return nil
	}

	if lhs.Type == sql.NullType || rhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}

	if lhs.Value == 0 || rhs.Value == 0 {
		return nil
	}

	if lhs.Type == rhs.Type {

		if lhs.Type == sql.IntType {

			return &sql.ValueData{
				Type:  sql.IntType,
				Value: lhs.Value.(int64) / rhs.Value.(int64),
			}
		}

		if lhs.Type == sql.FloatType {
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: lhs.Value.(float64) / rhs.Value.(float64),
			}
		}
	} else {
		if lhs.Type == sql.IntType && rhs.Type == sql.FloatType {
			if int(rhs.Value.(float64)) == 0 {
				return nil
			}
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: float64(lhs.Value.(int64)) / rhs.Value.(float64),
			}
		}
		if lhs.Type == sql.FloatType && rhs.Type == sql.IntType {
			if int(lhs.Value.(float64)) == 0 {
				return nil
			}
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: lhs.Value.(float64) / float64(rhs.Value.(int64)),
			}
		}
	}

	return nil
}

type Exponentiate struct {
	L Expression
	R Expression
}

func (e *Exponentiate) expression() {}
func (e *Exponentiate) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if e.L == nil || e.R == nil {
		logger.Info("Exponentiate Evaluate nil")
		return nil
	}
	lhs := e.L.Evaluate(row)
	rhs := e.R.Evaluate(row)

	if lhs == nil || rhs == nil {
		logger.Info("Exponentiate Sub Evaluate nil")
		return nil
	}

	if lhs.Type == sql.StringType || rhs.Type == sql.StringType ||
		lhs.Type == sql.BoolType || rhs.Type == sql.BoolType {
		return nil
	}

	if lhs.Type == sql.NullType || rhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}
	if lhs.Value == 0 || rhs.Value == 0 {
		return nil
	}

	if lhs.Type == rhs.Type {

		if lhs.Type == sql.IntType {

			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: math.Pow(float64(lhs.Value.(int64)), float64(rhs.Value.(int64))),
			}
		}

		if lhs.Type == sql.FloatType {
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: math.Pow(lhs.Value.(float64), rhs.Value.(float64)),
			}
		}
	} else {
		if lhs.Type == sql.IntType && rhs.Type == sql.FloatType {
			if int(rhs.Value.(float64)) == 0 {
				return nil
			}
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: math.Pow(float64(lhs.Value.(int64)), rhs.Value.(float64)),
			}
		}
		if lhs.Type == sql.FloatType && rhs.Type == sql.IntType {
			if int(lhs.Value.(float64)) == 0 {
				return nil
			}
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: math.Pow(lhs.Value.(float64), float64(rhs.Value.(int64))),
			}
		}
	}
	return nil

}

type Factorial struct {
	L Expression
}

func (f *Factorial) expression() {}
func (f *Factorial) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if f.L == nil {
		logger.Info("Factorial Evaluate nil")
		return nil
	}
	lhs := f.L.Evaluate(row)
	if lhs == nil || lhs.Value == 0 {
		return nil
	}
	if lhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}
	if lhs.Type == sql.IntType {
		num := lhs.Value.(int64)
		result := 0
		for i := 2; i <= int(num); i++ {
			result *= i
		}
		return &sql.ValueData{
			Type:  sql.IntType,
			Value: result,
		}
	}
	return nil
}

type Modulo struct {
	L Expression
	R Expression
}

func (m *Modulo) expression() {}
func (m *Modulo) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if m.L == nil || m.R == nil {
		logger.Info("Modulo Evaluate nil")
		return nil
	}
	lhs := m.L.Evaluate(row)
	rhs := m.R.Evaluate(row)

	if lhs == nil || rhs == nil {
		logger.Info("Modulo Sub Evaluate nil")
		return nil
	}

	if lhs.Type == sql.StringType || rhs.Type == sql.StringType ||
		lhs.Type == sql.BoolType || rhs.Type == sql.BoolType {
		return nil
	}

	if lhs.Value == 0 || rhs.Value == 0 {
		return nil
	}

	if lhs.Type == sql.NullType || rhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}

	if lhs.Type == rhs.Type && lhs.Type == sql.IntType {
		return &sql.ValueData{
			Type:  sql.IntType,
			Value: lhs.Value.(int64) % rhs.Value.(int64),
		}
	}
	return nil

}

type Multiply struct {
	L Expression
	R Expression
}

func (m *Multiply) expression() {}
func (m *Multiply) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if m.L == nil || m.R == nil {
		logger.Info("Multiply Evaluate nil")
		return nil
	}
	lhs := m.L.Evaluate(row)
	rhs := m.R.Evaluate(row)

	if lhs == nil || rhs == nil {
		logger.Info("Multiply Sub Evaluate nil")
		return nil
	}

	if lhs.Type == sql.StringType || rhs.Type == sql.StringType ||
		lhs.Type == sql.BoolType || rhs.Type == sql.BoolType {
		return nil
	}

	if lhs.Value == 0 || rhs.Value == 0 {
		return nil
	}

	if lhs.Type == sql.NullType || rhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}

	if lhs.Type == rhs.Type {
		if lhs.Type == sql.IntType {
			return &sql.ValueData{
				Type:  sql.IntType,
				Value: lhs.Value.(int64) * rhs.Value.(int64),
			}
		}

		if lhs.Type == sql.FloatType {
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: lhs.Value.(float64) * rhs.Value.(float64),
			}
		}

	} else {
		if lhs.Type == sql.IntType && rhs.Type == sql.FloatType {
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: float64(lhs.Value.(int64)) * rhs.Value.(float64),
			}
		}
		if lhs.Type == sql.FloatType && rhs.Type == sql.IntType {
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: lhs.Value.(float64) * float64(rhs.Value.(int64)),
			}
		}

	}
	return nil
}

type Negate struct {
	L Expression
}

func (n *Negate) expression() {}
func (n *Negate) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if n.L == nil {
		logger.Info("Negate Evaluate nil")
		return nil
	}
	lhs := n.L.Evaluate(row)

	if lhs == nil {
		logger.Info("Negate Sub Evaluate nil")
		return nil
	}

	if lhs.Type == sql.IntType {
		return &sql.ValueData{
			Type:  sql.IntType,
			Value: -lhs.Value.(int64),
		}
	}
	if lhs.Type == sql.FloatType {
		return &sql.ValueData{
			Type:  sql.IntType,
			Value: -lhs.Value.(float64),
		}
	}
	return nil
}

type Subtract struct {
	L Expression
	R Expression
}

func (s *Subtract) expression() {}

func (s *Subtract) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if s.L == nil || s.R == nil {
		logger.Info("Subtract Evaluate nil")
		return nil
	}
	lhs := s.L.Evaluate(row)
	rhs := s.R.Evaluate(row)

	if lhs == nil || rhs == nil {
		logger.Info("Negate Sub Evaluate nil")
		return nil
	}

	if lhs.Type == sql.StringType || rhs.Type == sql.StringType ||
		lhs.Type == sql.BoolType || rhs.Type == sql.BoolType {
		return nil
	}

	if lhs.Type == sql.NullType || rhs.Type == sql.NullType {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}

	if lhs.Type == rhs.Type {

		if lhs.Type == sql.IntType {
			return &sql.ValueData{
				Type:  sql.IntType,
				Value: lhs.Value.(int64) - rhs.Value.(int64),
			}
		}

		if lhs.Type == sql.FloatType {
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: lhs.Value.(float64) - rhs.Value.(float64),
			}
		}
	} else {
		if lhs.Type == sql.IntType && rhs.Type == sql.FloatType {
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: float64(lhs.Value.(int64)) - rhs.Value.(float64),
			}
		}
		if lhs.Type == sql.FloatType && rhs.Type == sql.IntType {
			return &sql.ValueData{
				Type:  sql.FloatType,
				Value: lhs.Value.(float64) - float64(rhs.Value.(int64)),
			}
		}
	}
	return nil
}

type Like struct {
	L Expression
	R Expression
}

func (l *Like) expression() {}
func (l *Like) Evaluate(row []*sql.ValueData) *sql.ValueData {
	if l.L == nil || l.R == nil {
		logger.Info("Like Evaluate nil")
		return nil
	}
	lhs := l.L.Evaluate(row)
	rhs := l.R.Evaluate(row)

	if lhs == nil || rhs == nil {
		logger.Info("Like Sub Evaluate nil")
		return nil
	}

	if (lhs.Type == sql.NullType && rhs.Type == sql.StringType) ||
		(rhs.Type == sql.NullType && lhs.Type == sql.StringType) {
		return &sql.ValueData{
			Type: sql.NullType,
		}
	}

	if lhs.Type == rhs.Type && lhs.Type == sql.StringType {
		rhsString := rhs.Value.(string)
		lhsString := lhs.Value.(string)

		escaped := regexp.QuoteMeta(rhsString)
		escaped = strings.ReplaceAll(escaped, `%`, `.*`)
		escaped = strings.ReplaceAll(escaped, `_`, `.`)
		regexPattern := fmt.Sprintf("^%s$", escaped)
		matched, _ := regexp.MatchString(regexPattern, lhsString)

		return &sql.ValueData{
			Type:  sql.BoolType,
			Value: matched,
		}

	}
	return nil

}

func IntoNnf(expr Expression) Expression {
	return Transform(expr, func(expression Expression) Expression {
		if v, ok := expression.(*Not); ok {
			switch v2 := v.L.(type) {
			case *And:
				return &Or{L: &Not{L: v2.L}, R: &Not{L: v2.R}}
			case *Or:
				return &And{L: &Not{L: v2.L}, R: &Not{L: v2.L}}
			case *Not:
				return v2.L
			default:
				return expression
			}
		}
		return expression
	}, func(expression Expression) Expression {
		return expression
	})

}

func IntoCnf(expr Expression) Expression {
	return Transform(IntoNnf(expr), func(expression Expression) Expression {
		if v, ok := expression.(*Or); ok {
			if v1, ok1 := v.L.(*And); ok1 {
				return &And{L: &Or{L: v1.L, R: v1.R}, R: v.R}
			}
			if v2, ok2 := v.R.(*And); ok2 {
				return &And{L: v2.L, R: &Or{L: v2.L, R: v2.R}}
			}
			return &Or{L: v.L, R: v.R}

		}
		return expression

	}, func(expression Expression) Expression {
		return expression
	})
}

func IntoCnfList(expr Expression) []Expression {
	var cnf []Expression
	stack := []Expression{IntoCnf(expr)}
	for len(stack) != 0 {
		expr1 := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		switch v := expr1.(type) {
		case *And:
			stack = append(stack, v.R)
			stack = append(stack, v.L)
		default:
			cnf = append(cnf, expr1)
		}
	}

	return cnf
}

func IntoDnf(expr Expression) Expression {
	return Transform(IntoNnf(expr), func(expression Expression) Expression {
		if v, ok := expression.(*And); ok {
			if v1, ok1 := v.L.(*Or); ok1 {
				return &Or{L: &And{L: v1.L, R: v.R}, R: &And{L: v1.R, R: v.R}}
			}
			if v2, ok2 := v.R.(*And); ok2 {
				return &Or{L: &And{L: v.L, R: v2.R}, R: &And{L: v.L, R: v2.R}}
			}
			return &And{L: v.L, R: v.R}

		}
		return expression

	}, func(expression Expression) Expression {
		return expression
	})
}

func IntoDnfList(expr Expression) []Expression {
	var dnf []Expression
	stack := []Expression{IntoDnf(expr)}
	expr1 := stack[len(stack)-1]
	stack = stack[:len(stack)-1]
	switch v := expr1.(type) {
	case *And:
		stack = append(stack, v.R)
		stack = append(stack, v.L)
	default:
		dnf = append(dnf, expr1)
	}
	return dnf
}

// 让内部切片影响外部切片的长度和底层数组
func FromCnfList(cnfListPtr *[]Expression) Expression {
	cnfList := *cnfListPtr
	if len(cnfList) == 0 {
		return nil
	}

	cnf := cnfList[len(cnfList)-1]
	cnfList = cnfList[:len(cnfList)-1]
	for i := range cnfList {
		cnf = &And{
			L: cnf,
			R: cnfList[i],
		}
	}
	*cnfListPtr = cnfList
	return cnf
}

func FromDnfList(dnfList []Expression) Expression {
	if len(dnfList) == 0 {
		return nil
	}

	dnf := dnfList[len(dnfList)-1]
	dnfList = dnfList[:len(dnfList)-1]
	for i := range dnfList {
		dnf = &And{
			L: dnf,
			R: dnfList[i],
		}
	}
	return dnf
}

func AsLookup(expr Expression, field int) []*sql.ValueData {
	switch v := expr.(type) {
	case *Equal:
		v1, ok1 := v.L.(*Field)
		v2, ok2 := v.R.(*Constant)
		if ok1 && ok2 && v1.Index == field {
			return []*sql.ValueData{v2.Value}
		}
		v3, ok3 := v.L.(*Constant)
		v4, ok4 := v.R.(*Field)
		if ok3 && ok4 && v4.Index == field {
			return []*sql.ValueData{v3.Value}
		}
		return nil
	case *IsNull:
		v1, ok1 := v.L.(*Field)
		if ok1 && v1.Index == field {
			return []*sql.ValueData{&sql.ValueData{Type: sql.NullType}}
		}
		return nil
	case *Or:
		v1 := AsLookup(v.L, field)
		v2 := AsLookup(v.R, field)
		if v1 != nil && v2 != nil {
			v1 = append(v1, v2...)
			return v1
		}
		return nil
	default:
		return nil
	}
}

func FromLookup(field int, label *ColumnField, values []*sql.ValueData) Expression {
	if len(values) == 0 {
		return &Equal{
			L: &Field{Index: field, ColumnField: label},
			R: &Constant{Value: &sql.ValueData{Type: sql.NullType}},
		}
	}

	var exprs []Expression
	for i := range values {
		exprs = append(exprs, &Equal{
			L: &Field{
				Index:       field,
				ColumnField: label,
			},
			R: &Constant{
				Value: values[i],
			},
		})
	}
	return FromDnfList(exprs)
}

func GobReg() {
	gob.Register(&Constant{})
	gob.Register(&Field{})
	gob.Register(&ColumnField{})
	gob.Register(&And{})
	gob.Register(&Not{})
	gob.Register(&Or{})
	gob.Register(&Equal{})
	gob.Register(&GreaterThan{})
	gob.Register(&IsNull{})
	gob.Register(&LessThan{})
	gob.Register(&Add{})
	gob.Register(&Assert{})
	gob.Register(&Divide{})
	gob.Register(&Exponentiate{})
	gob.Register(&Factorial{})
	gob.Register(&Modulo{})
	gob.Register(&Multiply{})
	gob.Register(&Negate{})
	gob.Register(&Subtract{})
	gob.Register(&Like{})

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(&Constant{})
	_ = enc.Encode(&Field{})
	_ = enc.Encode(&ColumnField{})
	_ = enc.Encode(&And{})
	_ = enc.Encode(&Not{})
	_ = enc.Encode(&Or{})
	_ = enc.Encode(&Equal{})
	_ = enc.Encode(&GreaterThan{})
	_ = enc.Encode(&IsNull{})
	_ = enc.Encode(&LessThan{})
	_ = enc.Encode(&Add{})
	_ = enc.Encode(&Assert{})
	_ = enc.Encode(&Divide{})
	_ = enc.Encode(&Exponentiate{})
	_ = enc.Encode(&Factorial{})
	_ = enc.Encode(&Modulo{})
	_ = enc.Encode(&Multiply{})
	_ = enc.Encode(&Negate{})
	_ = enc.Encode(&Subtract{})
	_ = enc.Encode(&Like{})

}
