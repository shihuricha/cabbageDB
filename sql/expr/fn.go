package expr

type ExprFnBool func(expression Expression) bool
type ExprFn func(expression Expression) Expression

func Contains(expr Expression, fn ExprFnBool) bool {
	return !Walk(expr, func(expr1 Expression) bool {
		return !fn(expr1)
	})
}

func Walk(expr Expression, visitor ExprFnBool) bool {
	visitorBool := visitor(expr)

	var exprBool bool
	switch v := expr.(type) {
	case *Add:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *And:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *Divide:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *Equal:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *Exponentiate:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *GreaterThan:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *LessThan:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *Like:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *Modulo:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *Multiply:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *Or:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *Subtract:
		exprBool = Walk(v.L, visitor) && Walk(v.R, visitor)
	case *Assert:
		exprBool = Walk(v.L, visitor)
	case *Factorial:
		exprBool = Walk(v.L, visitor)
	case *IsNull:
		exprBool = Walk(v.L, visitor)
	case *Negate:
		exprBool = Walk(v.L, visitor)
	case *Not:
		exprBool = Walk(v.L, visitor)
	case *Constant, *Field:

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
	case *Add:
		expr = &Add{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
			R: ReplaceWith(v.R, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}

	case *And:
		expr = &And{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
			R: ReplaceWith(v.R, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *Divide:
		expr = &Divide{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
			R: ReplaceWith(v.R, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *Equal:
		expr = &Equal{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
			R: ReplaceWith(v.R, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *Exponentiate:
		expr = &Exponentiate{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
			R: ReplaceWith(v.R, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *GreaterThan:
		expr = &GreaterThan{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
			R: ReplaceWith(v.R, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *LessThan:
		expr = &LessThan{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
			R: ReplaceWith(v.R, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *Like:
		expr = &Like{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
			R: ReplaceWith(v.R, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *Modulo:
		expr = &Modulo{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
			R: ReplaceWith(v.R, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *Multiply:
		expr = &Multiply{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
			R: ReplaceWith(v.R, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *Or:
		expr = &Or{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
			R: ReplaceWith(v.R, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *Subtract:
		expr = &Subtract{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
			R: ReplaceWith(v.R, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *Assert:
		expr = &Assert{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *Factorial:
		expr = &Factorial{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *IsNull:
		expr = &IsNull{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *Negate:
		expr = &Negate{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}
	case *Not:
		expr = &Not{
			L: ReplaceWith(v.L, func(expression Expression) Expression {
				return Transform(expression, before, after)
			}),
		}

	}

	return after(expr)
}
