package parser

import (
	"bytes"
	"unicode"
	"unicode/utf8"
)

type Scanner struct {
	r             reader
	buf           bytes.Buffer
	errs          []error
	identifierDot bool
}

type reader struct {
	s string
	p Pos
	w int
}

type Pos struct {
	Line   int
	Col    int
	Offset int
}

func (s *Scanner) reset(sql string) {
	s.r = reader{s: sql, p: Pos{Line: 1}}
	s.buf.Reset()
	s.errs = s.errs[:0]
}

func (s *Scanner) skipWhitespace() rune {
	return s.r.incAsLongAs(unicode.IsSpace)
}

func (s *Scanner) scan() (tok int, pos Pos, lit string) {
	ch0 := s.r.peek()
	if unicode.IsSpace(ch0) {
		ch0 = s.skipWhitespace()
	}

	pos = s.r.pos()
	if s.r.eof() {
		return 0, pos, ""
	}
	if !s.r.eof() && isIdentExtend(ch0) {
		return scanIdentifier(s)
	}

	node := &ruleTable
	for !(node.childs[ch0] == nil || s.r.eof()) {
		node = node.childs[ch0]
		if node.fn != nil {
			return node.fn(s)
		}
		s.r.inc()
		ch0 = s.r.peek()
	}

	tok, lit = node.token, s.r.data(&pos)
	return
}

func (r *reader) data(from *Pos) string {
	return r.s[from.Offset:r.p.Offset]
}

func scanIdentifier(s *Scanner) (int, Pos, string) {
	pos := s.r.pos()
	s.r.inc()
	s.r.incAsLongAs(isIdentChar)

	return identifier, pos, s.r.data(&pos)
}

var eof = Pos{-1, -1, -1}

func (r *reader) eof() bool {
	// 如果Offset>reader的s长度则到了eof
	return r.p.Offset >= len(r.s)
}

func (r *reader) peek() rune {
	if r.eof() {
		return unicode.ReplacementChar
	}

	// 	s.r.incAsLongAs(isIdentChar) 在这里迭代
	v, w := rune(r.s[r.p.Offset]), 1
	switch {
	case v == 0:
		r.w = w
		return v
	case v >= 0x80:
		v, w = utf8.DecodeRuneInString(r.s[r.p.Offset:])
		if v == utf8.RuneError && w == 1 {
			v = rune(r.s[r.p.Offset])
		}
	}
	r.w = w

	return v
}

func (r *reader) incAsLongAs(fn func(rune) bool) rune {
	for {
		ch := r.peek()
		if !fn(ch) {
			return ch
		}
		if ch == unicode.ReplacementChar && r.eof() {
			return 0
		}
		r.inc()
	}
}

func (r *reader) inc() {
	if r.s[r.p.Offset] == '\n' {
		r.p.Line++
		r.p.Col = 0
	}
	r.p.Offset += r.w
	r.p.Col++
}

func (r *reader) pos() Pos {
	return r.p
}

func (s *Scanner) isTokenIdentifier(lit string, offset int) int {
	if s.r.peek() == '.' {
		return 0
	}
	for idx := offset - 1; idx >= 0; idx-- {
		if s.r.s[idx] == ' ' {
			continue
		} else if s.r.s[idx] == '.' {
			return 0
		}
		break
	}
	buf := &s.buf
	buf.Reset()
	buf.Grow(len(lit))
	data := buf.Bytes()[:len(lit)]

	for i := 0; i < len(lit); i++ {
		c := lit[i]
		if c >= 'a' && c <= 'z' {
			data[i] = c + 'A' - 'a'
		} else {
			data[i] = c
		}
	}

	tokenStr := string(data)
	tok, _ := tokenMap[tokenStr]

	return tok
}

func (p *Parser) GetResult() interface{} {
	ret := yyParse(p)

	return ret
}

func (r *reader) readByte() (ch byte) {
	ch = byte(r.peek())
	if r.eof() {
		return
	}
	r.inc()
	return
}

func (s *Scanner) scanDigits() string {
	pos := s.r.pos()
	s.r.incAsLongAs(isDigit)
	return s.r.data(&pos)
}

func startWithDot(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	if s.identifierDot {
		return int('.'), pos, "."
	}
	if isDigit(s.r.peek()) {
		tok, p, l := s.scanFloat(&pos)
		if tok == identifier {
			return 0, p, l
		}
		return tok, p, l
	}
	tok, lit = int('.'), "."
	return
}

func (r *reader) updatePos(pos Pos) {
	r.p = pos
}

func (s *Scanner) scanFloat(beg *Pos) (tok int, pos Pos, lit string) {
	s.r.updatePos(*beg)
	// float = D1 . D2 e D3
	s.scanDigits()
	ch0 := s.r.peek()
	if ch0 == '.' {
		s.r.inc()
		s.scanDigits()
		ch0 = s.r.peek()
	}
	if isDigit(s.r.peek()) {
		s.scanDigits()
	}
	pos, lit = *beg, s.r.data(beg)
	tok = floatLit
	return
}
