package ast

type ExprFnBool func(expression Expression) bool
type ExprFn func(expression Expression) Expression

func Contains(expr Expression, fn ExprFnBool) bool {
	return !Walk(expr, func(expr Expression) bool {
		return !fn(expr)
	})
}

func Walk(expr Expression, visitor ExprFnBool) bool {
	visitorBool := visitor(expr)

	var exprBool bool
	switch v := expr.(type) {
	case *AddOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *AndOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *DivideOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *EqualOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *ExponentiateOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *GreaterThanOrEqualOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *GreaterThanOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *LessThanOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *LessThanOrEqualOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *LikeOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *ModuloOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *MultiplyOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *NotEqualOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *OrOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *SubtractOper:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *AssertOper:
		exprBool = Walk(v.L, visitor)
	case *FactorialOper:
		exprBool = Walk(v.L, visitor)
	case *IsNullOper:
		exprBool = Walk(v.L, visitor)
	case *NegateOper:
		exprBool = Walk(v.L, visitor)
	case *NotOper:
		exprBool = Walk(v.L, visitor)
	case *Function:
		for _, fn := range v.Args {
			if !Walk(fn, visitor) {
				return false
			}
		}
		exprBool = true
	case *Literal, *Field, *ColumnIdx:
		exprBool = true

	}

	return visitorBool && exprBool
}

func ReplaceWith(expr Expression, fn ExprFn) Expression {
	return fn(expr)
}

func Transform(expr Expression, before ExprFn, after ExprFn) Expression {
	expr = before(expr)
	switch v := expr.(type) {
	case *Operation:
		switch v1 := v.Operation.(type) {
		case *AddOper:
			expr = &Operation{Operation: &AddOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}

		case *AndOper:
			expr = &Operation{Operation: &AndOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *DivideOper:
			expr = &Operation{Operation: &DivideOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *EqualOper:
			expr = &Operation{Operation: &EqualOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *ExponentiateOper:
			expr = &Operation{Operation: &ExponentiateOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *GreaterThanOrEqualOper:
			expr = &Operation{Operation: &GreaterThanOrEqualOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *GreaterThanOper:
			expr = &Operation{Operation: &GreaterThanOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
			}}
		case *LessThanOper:
			expr = &Operation{Operation: &LessThanOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *LessThanOrEqualOper:
			expr = &Operation{Operation: &LessThanOrEqualOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *LikeOper:
			expr = &Operation{Operation: &LikeOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *ModuloOper:
			expr = &Operation{Operation: &ModuloOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *MultiplyOper:
			expr = &Operation{Operation: &MultiplyOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *NotEqualOper:
			expr = &Operation{Operation: &NotEqualOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *OrOper:
			expr = &Operation{Operation: &OrOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *SubtractOper:
			expr = &Operation{Operation: &SubtractOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				}),
				R: ReplaceWith(v1.R, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *AssertOper:
			expr = &Operation{Operation: &SubtractOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *FactorialOper:
			expr = &Operation{Operation: &FactorialOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *NegateOper:
			expr = &Operation{Operation: &NegateOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		case *NotOper:
			expr = &Operation{Operation: &NotOper{
				L: ReplaceWith(v1.L, func(expression Expression) Expression {
					return Transform(expression, before, after)
				})},
			}
		}

	case *Function:
		var exprs []Expression
		for _, fn := range v.Args {
			exprs = append(exprs, ReplaceWith(fn, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}))
		}
		expr = &Function{
			FuncName: v.FuncName,
			Args:     exprs,
		}

	}

	return after(expr)
}
