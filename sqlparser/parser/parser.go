package parser

import (
	"cabbageDB/sqlparser/ast"
	"errors"
	"strconv"
)

type Parser struct {
	Scan     *Scanner
	Result   []ast.Stmt
	ErrorMsg error
}

func (p *Parser) Reset(sql string) {
	scan := Scanner{}
	scan.reset(sql)
	p.Scan = &scan
}

func (p *Parser) Lex(lval *yySymType) int {
	tok, pos, lit := p.Scan.scan()

	if tok == identifier {
		tok = handleIdent(lval)
	}

	if tok == identifier {
		if tok1 := p.Scan.isTokenIdentifier(lit, pos.Offset); tok1 != 0 {
			tok = tok1
		}
	}
	if tok == intLit {
		n, _ := strconv.ParseUint(lit, 10, 64)
		lval.item = int64(n)
	}
	if tok == floatLit {
		n, _ := strconv.ParseFloat(lit, 64)
		lval.item = float64(n)
	}
	switch tok {

	case identifier:
		lval.ident = lit
	case stringLit:
		lval.ident = lit
	}

	return tok
}

func (p *Parser) Error(s string) {
	p.ErrorMsg = errors.New(s)
}

func (p *Parser) ParseSQL() int {
	ret := yyParse(p)
	return ret
}
