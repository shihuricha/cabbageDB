%{
package parser

import (
	"myfirstdb/sqlparser/ast"
	"myfirstdb/sqlparser/model"
	"myfirstdb/sql"
)

%}

%union {
	ident string
    item interface{}
    statement ast.Stmt
    expr ast.Expression
}


%token	<ident>
	/*yy:token "%c"     */	identifier      "identifier"
	/*yy:token "\"%c\"" */	stringLit       "string literal"

    andand                  "&&"
    oror                    "||"
    as                      "AS"
    selectKwd		"SELECT"
	from			"FROM"
	not			"NOT"
	is			"IS"
	null        "NULL"
	like			"LIKE"
	limit			"LIMIT"
	trueKwd        "TRUE"
	falseKwd       "FALSE"
	avg         "AVG"
	count        "COUNT"
	sum         "SUM"
	maxKwd         "MAX"
	minKwd         "MIN"
	or           "OR"
    cross        "CROSS"
    join         "JOIN"
    left        "LEFT"
    right       "RIGHT"
    outer       "OUTER"
    where       "WHERE"
    by          "BY"
    group       "GROUP"
    order       "ORDER"
    asc         "ASC"
    desc        "DESC"
    having      "HAVING"
    offset      "OFFSET"
    on          "ON"
    and         "AND"
    ifkwd          "IF"
	neq		"!="
	neqSynonym	"<>"
    exists    "EXISTS"
    create      "CREATE"
    table       "TABLE"


	eq		"="
	ge		">="
	le		"<="
	inner 			"INNER"
	char    "CHAR"
    varchar     "VARCHAR"
    text    "TEXT"
    boolean "BOOLEAN"
    boolkwd    "BOOL"
    float   "FLOAT"
    double  "Double"
    intkwd     "Int"
    integer        "Integer"
    primary "PRIMARY"
    key     "KEY"
    defaultKwd  "DEFAULT"
    unique  "UNIQUE"

    set "SET"
    update "UPDATE"
    into "INTO"
    valueKwd "VALUE"
    values "VALUES"
    insert "INSERT"
    start "START"
    begin "BEGIN"
    transaction "TRANSACTION"
    read "READ"
    only "ONLY"
    write "WRITE"
    commit "COMMIT"
    explain "EXPLAIN"
    rollback "ROLLBACK"
    drop     "DROP"
    delete   "DELETE"
    foreign    "FOREIGN"
    references           "REFERENCES"


%token	<item>
	/*yy:token "1.%d"   */	floatLit        "floating-point literal"
    /*yy:token "%d"     */	intLit          "integer literal"


%type	<statement>
	SelectStmt			"SELECT statement"
	Statement           "statement"
	CreateTableStmt     "CREATE statement"
	UpdateStmt          "UPDATE statement"
	InsertIntoStmt
    BeginStmt
    CommitStmt          "COMMIT statement"
    RollbackStmt        "ROLLBACK statement"
    ExplainStmt
    DropTableStmt
    DeleteStmt

%type	<expr>
	Literal				"literal value"
	Expression			"expression"
	BoolPri				"boolean primary expression"
	PredicateExpr			"Predicate expression factor"
	BitExpr				"bit expression"
	SimpleExpr			"simple expression"
	SimpleIdent			"Simple Identifier expression"
    SumExpr             "sumExpr expression"

%type   <item>
    OuterJoin           "outer"
	CrossJoin		"Cross join"
	InnerJoin       "Iner join"
	Field				"field expression"
	FieldList			"field expression list"
	SelectStmtFieldList		"SELECT statement field list"
	EscapedTableRef 		"escaped table reference"
	FieldAsName			"Field alias name"
	FieldAsNameOpt			"Field alias name opt"
	TableRef 			"table reference"
	TableRefs 			"table references"
	TableRefsClause			"Table references clause"
	TableFactor 			"table factor"
	TableName			"Table name"
	TableAsName			"table alias name"
	TableAsNameOpt 			"table alias name optional"
	JoinTable 			"join table"
	JoinType			"join type"

	WhereClause		"WHERE clause"
	WhereClauseOptional	"Optional WHERE clause"
	SelectStmtGroup			"SELECT statement optional GROUP BY clause"
	GroupByClause			"GROUP BY clause"
	Order				"ORDER BY clause optional collation specification"
	OrderBy				"ORDER BY clause"
	ByItem				"BY item"
	OrderByOptional			"Optional ORDER BY clause optional"
	ByList				"BY list"
	HavingClause			"HAVING clause"
	SelectStmtLimit			"SELECT statement optional LIMIT clause"
	OffsetOpt           "OFFSET option"
	StatementList			"statement list"
    ExpressionList          "expression list"

    IfNotExists
    ColumnList
    ColumnDef
    TableConstraint
    ColumnName
    ColumnType
    FieldLen
    LengthNum
    Version
    NUM
    ColumnOptions
    ColumnOption
    IdxName
    IdxNameOpt

    SetColumn
    SetColumnList

    ValuesList
    RowValue
    ValuesOpt
    Values
    ColumnNameList
    ColumnNameListOpt
    ReadOpt
    FromOpt
    LikeOperation

%type	<ident>
    logAnd			"logical and operator"
    logOr			"logical or operator"
    notEq           "logical noteq operator"
	Identifier			"identifier or unreserved keyword"
    ValueSym         "Value or Values"
    IntoOpt
    TransactionOpt

%precedence lowerThanStringLitToken
%precedence stringLit
%precedence lowerThanSetKeyword
%left   join inner cross left right
%left   tableRefPriority
%precedence on
%left 	oror or
%left 	andand and
%left 	eq ge le neq neqSynonym '>' '<' is like
%left 	'|'
%left 	'&'
%left 	'-' '+'
%left 	'*' '/' '%'
%left 	'^'
%left   neg
%right 	not

%precedence '('
%precedence ','

%%
root:
	StatementList

StatementList:
    Statement
    {
        if $1 !=nil {
            yylex.(*Parser).Result = append(yylex.(*Parser).Result, $1)
        }
    }
|   StatementList ';' Statement
    {
		if $3 != nil {
            yylex.(*Parser).Result = append(yylex.(*Parser).Result, $3)
		}
    }

Statement:
    SelectStmt
|   CreateTableStmt
|   UpdateStmt
|   InsertIntoStmt
|   BeginStmt
|   CommitStmt
|   RollbackStmt
|   ExplainStmt
|   DropTableStmt
|   DeleteStmt

Identifier:
identifier

FieldList:
    Field
    {
        var fieldList []*ast.ExprAS
		field := $1.(*ast.ExprAS)
		$$ = append(fieldList,field)
    }
|	FieldList ',' Field
    {
        fieldList1 := $1.([]*ast.ExprAS)
        field := $3.(*ast.ExprAS)
		$$ = append(fieldList1, field)
	}

Field:
	'*'
	{
		$$ = &ast.ExprAS{Expr: nil}
	}
|	Identifier '.' '*'
	{
		field := &ast.Field{TableName:model.LowStr($1)}
		$$ = &ast.ExprAS{Expr: field}
	}
|	Expression FieldAsNameOpt
    {
        expr := $1.(ast.Expression)
        asName := $2.(string)
        $$ = &ast.ExprAS{Expr: expr,As:asName}
    }

FieldAsNameOpt:
	/* EMPTY */
	{
		$$ = ""
	}
|	FieldAsName
	{
		$$ = $1
	}

FieldAsName:
	Identifier
	{
		$$ = $1
	}
|	"AS" Identifier
	{
		$$ = $2
	}
|	stringLit
	{
		$$ = $1
	}
|	"AS" stringLit
	{
		$$ = $2
	}


SelectStmtFieldList:
    FieldList
    {
        $$ = $1
    }

FromOpt:
    {
        var  fromList []ast.FromItem
        $$ = fromList
    }
|   "FROM" TableRefsClause
    {
        $$ = $2
    }


SelectStmt:
    "SELECT" SelectStmtFieldList FromOpt
	WhereClauseOptional SelectStmtGroup HavingClause OrderByOptional
	SelectStmtLimit OffsetOpt
	{
	    st := &ast.SelectStmt{
	        Select:$2.([]*ast.ExprAS),
	        From:$3.([]ast.FromItem),
	    }
	    if $4 != nil{
	        st.Where = $4.(ast.Expression)
	    }
	    if $5 != nil{
	        st.GroupBy = $5.([]ast.Expression)
	    }
	    if $6 != nil{
	        st.Having = $6.(ast.Expression)
	    }
	    if $7 != nil{
	        st.Order = $7.([]*ast.Order)
	    }
	    if $8 != nil{
	        st.Limit = $8.(ast.Expression)
	    }
	    if $9 != nil{
	        st.Offset = $9.(ast.Expression)
	    }
	    $$ = st
	}

Expression:
    Expression logOr Expression %prec oror
    {
        $$ = &ast.Operation{
            Operation:&ast.OrOper{L: $1, R: $3},
        }
    }
|	Expression logAnd Expression %prec andand
	{
		$$ = &ast.Operation{
            Operation:&ast.AndOper{L: $1, R: $3},
		}
	}
|	"NOT" Expression %prec not
	{
        $$ = &ast.Operation{
            Operation:&ast.NotOper{L: $2},
        }
	}
|	BoolPri


BoolPri:
    BoolPri "IS" "NULL"
    {
        $$ = &ast.Operation{
            Operation:&ast.IsNullOper{L:$1},
        }
    }
|   BoolPri "=" PredicateExpr %prec eq
	{
        $$ = &ast.Operation{
            Operation:&ast.EqualOper{L: $1, R: $3},
        }
	}
|	BoolPri  '>' PredicateExpr %prec '>'
    {
        $$ = &ast.Operation{
            Operation: &ast.GreaterThanOper{L: $1, R: $3},
        }
	}
|	BoolPri  ">=" PredicateExpr %prec ge
	{
        $$ = &ast.Operation{
            Operation: &ast.GreaterThanOrEqualOper{L: $1, R: $3},
        }
	}
|	BoolPri  '<' PredicateExpr %prec '<'
	{
        $$ = &ast.Operation{
            Operation: &ast.LessThanOper{L: $1, R: $3},
        }
	}
|	BoolPri  "<=" PredicateExpr %prec le
	{
        $$ = &ast.Operation{
            Operation:&ast.LessThanOrEqualOper{L: $1, R: $3},
        }
	}
|   BoolPri  notEq PredicateExpr
	{
        $$ = &ast.Operation{
            Operation:&ast.NotEqualOper{L: $1, R: $3},
        }
	}
|	PredicateExpr

LikeOperation:
    Identifier '%'
    {
        $$ = &ast.Literal{Type:sql.StringType,Value:$1+"%"}
    }
|   '%' Identifier '%'
    {
        $$ = &ast.Literal{Type:sql.StringType,Value:"%"+$2+"%"}
    }

PredicateExpr:
    BitExpr "LIKE" LikeOperation %prec like
    {
        $$ = &ast.Operation{
            Operation:&ast.LikeOper{L: $1,R:$3.(ast.Expression)},
        }
    }
|   BitExpr

BitExpr:
    BitExpr '+' BitExpr %prec '+'
    {
        $$ = &ast.Operation{
            Operation:&ast.AddOper{L: $1,R:$3},
        }
    }
|	BitExpr '-' BitExpr %prec '-'
	{
        $$ = &ast.Operation{
            Operation:&ast.SubtractOper{L: $1, R: $3},
        }
	}
|   BitExpr '*' BitExpr %prec '*'
    {
        $$ = &ast.Operation{
            Operation:&ast.MultiplyOper{L: $1, R: $3},
        }
    }
|   BitExpr '/' BitExpr %prec '/'
    {
        $$ = &ast.Operation{
            Operation:&ast.DivideOper{L: $1, R: $3},
        }
    }
|	BitExpr '%' BitExpr %prec '%'
	{
        $$ = &ast.Operation{
            Operation: &ast.ModuloOper{L: $1, R: $3},
        }
	}
|	BitExpr '^' BitExpr %prec '^'
	{
        $$ = &ast.Operation{
            Operation: &ast.ExponentiateOper{L: $1, R: $3},
        }
	}
|	'!' BitExpr %prec neg
	{
        $$ = &ast.Operation{
            Operation: &ast.FactorialOper{L: $2,},
        }
	}
|   SimpleExpr


SimpleIdent:
	Identifier
	{
		$$ =  &ast.Field{ColumnName:model.LowStr($1)}
	}
|	Identifier '.' Identifier
	{
		$$ = &ast.Field{
		    TableName:model.LowStr($1),
		    ColumnName:model.LowStr($3),
		}
	}

Literal:
    "NULL"
    {
        $$ =  &ast.Literal{Type:sql.NullType,}
    }
|	floatLit
	{
		$$ =  &ast.Literal{Type:sql.FloatType,Value:$1}
	}
|	intLit
	{
		$$ = &ast.Literal{Type:sql.IntType,Value:$1}
	}
|	stringLit %prec lowerThanStringLitToken
	{
		$$ = &ast.Literal{Type:sql.StringType,Value:$1}
	}
|   "TRUE"
	{
		$$ =  &ast.Literal{Type:sql.BoolType,Value:true}
	}
|   "FALSE"
	{
		$$ =  &ast.Literal{Type:sql.BoolType,Value:false}
	}

SimpleExpr:
    SimpleIdent
|	Literal
|	SumExpr
    {
        $$ = $1.(ast.Expression)
    }

SumExpr:
	"AVG" '(' Expression ')'
    {
        $$ = &ast.Function{FuncName:model.LowStr("AVG"),Args:[]ast.Expression{$3}}
    }
|	"COUNT" '(' Expression ')'
	{
		$$ = &ast.Function{FuncName:model.LowStr("COUNT"), Args: []ast.Expression{$3}}
	}
|	"COUNT" '(' '*' ')'
	{
	    expr := &ast.Literal{Type: sql.BoolType,Value:true}
		$$ = &ast.Function{FuncName: model.LowStr("COUNT"), Args: []ast.Expression{expr}}
	}
|	"SUM" '(' Expression ')'
    {
        $$ = &ast.Function{FuncName:model.LowStr("SUM"),Args:[]ast.Expression{$3}}
	}
|	"MAX" '(' Expression ')'
    {
        $$ = &ast.Function{FuncName:model.LowStr("MAX"),Args:[]ast.Expression{$3}}
    }
|	"MIN" '(' Expression ')'
    {
        $$ = &ast.Function{FuncName:model.LowStr("MIN"),Args:[]ast.Expression{$3}}
    }


logOr:
"||" | "OR"

logAnd:
"&&" | "AND"

notEq:
"!=" | "<>"

TableRefsClause:
    {
        var  fromList []ast.FromItem
        $$=fromList
    }
|   TableRefs
    {
        $$=$1
    }


TableRefs:
    EscapedTableRef
    {
        var fromList []ast.FromItem
        from := $1.(ast.FromItem)
        $$ = append(fromList,from)
    }
|	TableRefs ',' EscapedTableRef
	{
	    fromList1 := $1.([]ast.FromItem)
        var fromList2 []ast.FromItem
        from := $3.(ast.FromItem)
	    fromList2 = append(fromList2,from)
	    $$ = append(fromList1,fromList2...)
	}

EscapedTableRef:
	TableRef %prec lowerThanSetKeyword
	{
		$$ = $1
	}

TableRef:
	TableFactor
	{
		$$ = $1
	}
|	JoinTable
	{
		$$ = $1
	}

TableFactor:
    TableName TableAsNameOpt
    {
        $$ = &ast.FromItemTable{Name:$1.(string),Alias:$2.(string)}
    }


TableName:
	Identifier
	{
		$$ = model.LowStr($1)
	}

TableAsNameOpt:
	{
		$$ = ""
	}
|	TableAsName
	{
		$$ = $1
	}

TableAsName:
	Identifier
	{
		$$ = model.LowStr($1)
	}
|	"AS" Identifier
	{
		$$ = model.LowStr($2)
	}
JoinTable:
    TableRef CrossJoin TableRef  %prec tableRefPriority
    {
        $$ = &ast.FromItemJoinTable{Left: $1.(ast.FromItem), Right: $3.(ast.FromItem), Type: ast.CrossJoin}
    }
|   TableRef InnerJoin TableRef "ON" Expression
    {
        $$ = &ast.FromItemJoinTable{Left: $1.(ast.FromItem), Right: $3.(ast.FromItem), Type: ast.InnerJoin, Predicate: $5}
    }
|   TableRef OuterJoin TableRef "ON" Expression
    {
        $$ = &ast.FromItemJoinTable{Left: $1.(ast.FromItem), Right: $3.(ast.FromItem), Type: $2.(ast.JoinType), Predicate: $5}
    }


CrossJoin:
    "CROSS" "JOIN"
     {
      $$ = ast.CrossJoin
     }

InnerJoin:
    "INNER" "JOIN"
    {
        $$ = ast.InnerJoin
    }
|   "JOIN"
    {
        $$ = ast.InnerJoin
    }

OuterJoin:
    JoinType "JOIN"
    {
        $$ = $1
    }
|   JoinType "OUTER" "JOIN"
    {
        $$ = $1
    }

JoinType:
	"LEFT"
	{
		$$ = ast.LeftJoin
	}
|	"RIGHT"
	{
		$$ = ast.RightJoin
	}



WhereClauseOptional:
	{
		$$ = nil
	}
|	WhereClause
	{
		$$ = $1
	}

WhereClause:
	"WHERE" Expression
	{
		$$ = $2
	}

SelectStmtGroup:
	/* EMPTY */
	{
		$$ = nil
	}
|	GroupByClause

ExpressionList:
    Expression
    {
        var list  []ast.Expression
        $$ = append(list,$1)
    }
|   ExpressionList ',' Expression
    {
        list1 := $1.([]ast.Expression)
        list2 := $3
        $$ = append(list1,list2)
    }

GroupByClause:
	"GROUP" "BY" ExpressionList
	{
		$$ = $3
	}

ByList:
	ByItem
	{
	    var orderList []*ast.Order
        order := $1.(*ast.Order)
        $$ = append(orderList,order)
	}
|	ByList ',' ByItem
	{
	    list1 := $1.([]*ast.Order)
	    list2 := $3.(*ast.Order)
		$$ = append(list1, list2)
	}

ByItem:
	Expression Order
	{
		$$ = &ast.Order{Expr: $1, Desc: $2.(bool)}
	}

Order:
	/* EMPTY */
	{
		$$ = false // ASC by default
	}
|	"ASC"
	{
		$$ = false
	}
|	"DESC"
	{
		$$ = true
	}

HavingClause:
	{
		$$ = nil
	}
|	"HAVING" Expression
	{
		$$ = $2
	}

OrderByOptional:
	{
		$$ = nil
	}
|	OrderBy
	{
		$$ = $1
	}

OrderBy:
	"ORDER" "BY" ByList
	{
		$$ = $3
	}

SelectStmtLimit:
	{
		$$ = nil
	}
|	"LIMIT" Expression
	{
		$$ = $2
	}

OffsetOpt:
    {
		$$ = nil
    }
|   "OFFSET" Expression
	{
		$$ = $2
	}

CreateTableStmt:
	"CREATE" "TABLE" IfNotExists TableName '(' ColumnList ')'
    {
        var err error
        err =  valiDate($6.([]*ast.Column))
        if err != nil{
            yylex.Error(err.Error())
            return -9
        }


		$$ = &ast.CreateStmt{
			Name:    $4.(string),
			Columns: $6.([]*ast.Column),
		}
    }
IfNotExists:
    /* empty */
    {
        $$ = false
    }
|	"IF" "NOT" "EXISTS"
	{
		$$ = true
	}

ColumnList:
	ColumnDef
	{
        $$ = []*ast.Column{$1.(*ast.Column)}
	}
|	ColumnList ',' ColumnDef
	{
		$$ = append($1.([]*ast.Column), $3.(*ast.Column))
	}
|   ColumnList ',' TableConstraint
    {

        var err error
        err =  handleConstraint($1.([]*ast.Column),$3.(*ast.Constraint))
        if err != nil{
            yylex.Error(err.Error())
            return -8
        }
        $$ = $1.([]*ast.Column)
    }

ColumnDef:
    ColumnName ColumnType ColumnOptions
    {
        var column ast.Column
        var err error
        column.Name = $1.(string)
        column.ColumnType = $2.(sql.DataType)
        column.Nullable = true
        err = handleColumn($3.([]*ast.ColumnOption), &column)
        if err != nil {
            yylex.Error(err.Error())
            return -7
        }
        $$ = &column
    }

ColumnType:
    "CHAR" FieldLen
    {
        $$ = sql.StringType
    }
|   "VARCHAR" FieldLen
    {
        $$ = sql.StringType
    }
|   "TEXT"
    {
        $$ = sql.StringType
    }
|   "BOOLEAN"
    {
        $$ = sql.BoolType
    }
|   "BOOL"
    {
        $$ = sql.BoolType
    }
|   "FLOAT"
    {
        $$ = sql.FloatType
    }
|   "Double"
    {
        $$ = sql.FloatType
    }
|   "Int"
    {
        $$ = sql.IntType
    }
|   "Integer"
    {
        $$ = sql.IntType
    }


ColumnOptions:
    {
        var columnOptions []*ast.ColumnOption
        $$ = columnOptions
    }
|   ColumnOptions ColumnOption
    {
        $$ = append($1.([]*ast.ColumnOption),$2.(*ast.ColumnOption))
    }

ColumnOption:
    "NOT" "NULL"
     {
        option := &ast.ColumnOption{
            Type:ast.NOTNULL,
        }
        $$ = option
     }
|   "NULL"
     {
        option := &ast.ColumnOption{
            Type:ast.NULL,
        }
        $$ = option
     }
|   "PRIMARY" "KEY"
    {
        option := &ast.ColumnOption{
            Type:ast.PRIMARYKEY,
        }
        $$ = option
    }
|   "DEFAULT" Expression
    {
        option := &ast.ColumnOption{
            Type:ast.DEFAULT,
            Value:$2,
        }
        $$ = option
    }
|   "UNIQUE"
    {
        option := &ast.ColumnOption{
            Type:ast.UNIQUE,
        }
        $$ = option
    }



ColumnName:
    Identifier
	{
		$$ = model.LowStr($1)
	}

FieldLen:
	'(' LengthNum ')'
	{
		$$ = int($2.(uint64))
	}

LengthNum:
	NUM
	{
		$$ = getUint64FromNUM($1)
	}
Version:
	NUM
	{
		$$ = getUint64FromNUM($1)
	}
|   {
        $$ = uint64(0)
    }


NUM:
	intLit

IdxName:
   Identifier
	{
		$$ = model.LowStr($1)
	}

IdxNameOpt:
    {
        $$=""
    }
|   Identifier
    {
		$$ = model.LowStr($1)
    }

TableConstraint:
    "PRIMARY" "KEY" IdxNameOpt '(' IdxName ')'
    {
        $$ = &ast.Constraint{Type:ast.PRIMARYKEYConstraint,ColumnName:$5.(string)}
    }
|	"UNIQUE" "KEY" IdxNameOpt '(' IdxName ')' {
		$$ = &ast.Constraint{Type: ast.UNIQUEKEYConstraint, IndexName: $3.(string), ColumnName: $5.(string)}
	}
|	"KEY" IdxNameOpt '(' IdxName ')' {
		$$ = &ast.Constraint{Type: ast.KEYConstraint, IndexName: $2.(string), ColumnName: $4.(string)}
	}
|	"FOREIGN" "KEY" IdxNameOpt '(' ColumnName ')' "REFERENCES" TableName '(' ColumnName ')'
	{
		$$ = &ast.Constraint{
			Type:	ast.FORREGINKEYConstraint,
			IndexName:$3.(string),
			SubColumnName:$5.(string),
			ColumnName:$10.(string),
			TableName:$8.(string),
		}
	}

SetColumn:
	ColumnName eq Expression
	{
		$$ = &ast.ExprColumn{ColumnName: $1.(string), Expr:$3.(ast.Expression)}
	}

SetColumnList:
	SetColumn
	{
		$$ = []*ast.ExprColumn{$1.(*ast.ExprColumn)}
	}
|	SetColumnList ',' SetColumn
	{
		$$ = append($1.([]*ast.ExprColumn), $3.(*ast.ExprColumn))
	}



UpdateStmt:
    "UPDATE" TableName "SET" SetColumnList WhereClauseOptional
    {
        stmt := &ast.UpdateStmt{
            TableName:$2.(string),
            Set:$4.([]*ast.ExprColumn),
        }

        if $5!=nil{
            stmt.Where = $5.(ast.Expression)
        }
        $$ = stmt
    }

IntoOpt:
	{}
|	"INTO"

ColumnNameList:
	ColumnName
	{
		$$ = []string{$1.(string)}
	}
|	ColumnNameList ',' ColumnName
	{
		$$ = append($1.([]string), $3.(string))
	}

ColumnNameListOpt:
	/* EMPTY */
	{
		$$ = []string{}
	}
|	'(' ColumnNameList ')'
	{
		$$ = $2.([]string)
	}


ValueSym:
"VALUE" | "VALUES"

ValuesList:
	RowValue
	{
		$$ = [][]ast.Expression{$1.([]ast.Expression)}
	}
|	ValuesList ',' RowValue
	{
		$$ = append($1.([][]ast.Expression), $3.([]ast.Expression))
	}

RowValue:
	'(' ValuesOpt ')'
	{
		$$ = $2
	}

ValuesOpt:
	{
		$$ = []ast.Expression{}
	}
|	Values

Values:
	Values ',' Expression
	{
		$$ = append($1.([]ast.Expression), $3)
	}
|	Expression
	{
		$$ = []ast.Expression{$1}
	}

InsertIntoStmt:
    "INSERT" IntoOpt TableName ColumnNameListOpt  "VALUES" ValuesList
    {
        stmt := &ast.InsertStmt{
            TableName:$3.(string),
            Columns:$4.([]string),
            Values:$6.([][]ast.Expression),
        }

        $$ = stmt
    }


TransactionOpt:
	{}
|	"TRANSACTION"

ReadOpt:

    "READ" "ONLY"
    {
        $$ = true
    }
|   "READ" "WRITE"
    {
        $$ = false
    }
|   {
        $$ = false
    }

BeginStmt:
    "START" TransactionOpt ReadOpt Version
    {
        stmt := &ast.BeginStmt{
            ReadOnly:$3.(bool),
            AsOf:$4.(uint64),
        }
        $$ = stmt
    }
|   "BEGIN"
    {
        stmt := &ast.BeginStmt{
            ReadOnly:true,
        }
        $$ = stmt
    }

CommitStmt:
    "COMMIT"
    {
        stmt := &ast.CommitStmt{}
        $$ = stmt
    }


RollbackStmt:
    "ROLLBACK"
    {
        stmt := &ast.RollbackStmt{}
        $$ = stmt
    }
ExplainStmt:
    "EXPLAIN" Statement
    {
        stmt := &ast.ExplainStmt{
            Stmt:$2.(ast.Stmt),
        }
        $$ = stmt
    }

DropTableStmt:
    "DROP" "TABLE" TableName
    {
        stmt := &ast.DropTableStmt{
            TableName:$3.(string),
        }
        $$ = stmt
    }
DeleteStmt:
    "DELETE" "FROM" TableName "WHERE" Expression
    {
        stmt := &ast.DeleteStmt{
            TableName:$3.(string),
            Where:$5.(ast.Expression),
        }
        $$ = stmt
    }
%%


