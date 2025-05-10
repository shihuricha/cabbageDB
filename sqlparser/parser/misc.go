package parser

import (
	"bytes"
	"strings"
)

func isIdentChar(ch rune) bool {
	return isLetter(ch) || isDigit(ch) || ch == '_' || ch == '$' || isIdentExtend(ch)
}

func isIdentExtend(ch rune) bool {
	return ch >= 0x80 && ch <= '\uffff'
}
func isLetter(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}
func isDigit(ch rune) bool {
	return (ch >= '0' && ch <= '9')
}

type trieNode struct {
	childs [256]*trieNode
	token  int
	fn     func(s *Scanner) (int, Pos, string)
}

var ruleTable trieNode

func initTokenFunc(str string, fn func(s *Scanner) (int, Pos, string)) {
	for i := 0; i < len(str); i++ {
		c := str[i]
		if ruleTable.childs[c] == nil {
			ruleTable.childs[c] = &trieNode{}
		}
		ruleTable.childs[c].fn = fn
	}
}

func initTokenByte(c byte, tok int) {
	if ruleTable.childs[c] == nil {
		ruleTable.childs[c] = &trieNode{}
	}
	ruleTable.childs[c].token = tok
}

func handleIdent(lval *yySymType) int {
	s := lval.ident

	if !strings.HasPrefix(s, "_") {
		return identifier
	}
	cs, _, err := GetCharsetInfo(s[1:])
	if err != nil {
		return identifier
	}

	lval.ident = cs

	return 0
}

var tokenMap = map[string]int{

	"AND":         and,
	"AS":          as,
	"ASC":         asc,
	"AVG":         avg,
	"BY":          by,
	"COUNT":       count,
	"CROSS":       cross,
	"DESC":        desc,
	"FROM":        from,
	"GROUP":       group,
	"HAVING":      having,
	"INNER":       inner,
	"IS":          is,
	"JOIN":        join,
	"LEFT":        left,
	"LIKE":        like,
	"LIMIT":       limit,
	"NOT":         not,
	"NULL":        null,
	"OFFSET":      offset,
	"ON":          on,
	"OR":          or,
	"ORDER":       order,
	"OUTER":       outer,
	"RIGHT":       right,
	"SELECT":      selectKwd,
	"WHERE":       where,
	"SUM":         sum,
	"MAX":         maxKwd,
	"MIN":         minKwd,
	"CREATE":      create,
	"TABLE":       table,
	"IF":          ifkwd,
	"EXISTS":      exists,
	"CHAR":        char,
	"VARCHAR":     varchar,
	"TEXT":        text,
	"BOOLEAN":     boolean,
	"BOOL":        boolkwd,
	"FLOAT":       float,
	"DOUBLE":      double,
	"INT":         intkwd,
	"INTEGER":     integer,
	"INSERT":      insert,
	"INTO":        into,
	"VALUES":      values,
	"Integer":     integer,
	"PRIMARY":     primary,
	"KEY":         key,
	"DEFAULT":     defaultKwd,
	"UNIQUE":      unique,
	"UPDATE":      update,
	"SET":         set,
	"DELETE":      delete,
	"DROP":        drop,
	"FOREIGN":     foreign,
	"REFERENCES":  references,
	"EXPLAIN":     explain,
	"START":       start,
	"COMMIT":      commit,
	"READ":        read,
	"ONLY":        only,
	"WRITE":       write,
	"BEGIN":       begin,
	"TRANSACTION": transaction,
	"ROLLBACK":    rollback,
	"TRUE":        trueKwd,
	"FALSE":       falseKwd,
}

func init() {
	initTokenByte('(', int('('))
	initTokenByte(')', int(')'))
	initTokenByte(',', int(','))
	initTokenByte('>', int('>'))
	initTokenByte('<', int('<'))
	initTokenByte('+', int('+'))
	initTokenByte('-', int('-'))
	initTokenByte('*', int('*'))
	initTokenByte('/', int('/'))
	initTokenByte('%', int('%'))
	initTokenByte('^', int('^'))

	initTokenByte('=', eq)
	initTokenString(">=", ge)
	initTokenString("<=", le)
	initTokenString("!=", neq)
	initTokenString("<>", neqSynonym)
	//initTokenString("%", wildcard)

	initTokenFunc("0123456789", startWithNumber)
	initTokenFunc("'\"", scanString)
	initTokenFunc(".", startWithDot)

	initTokenFunc("_$ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", scanIdentifier)
}

func initTokenString(str string, tok int) {
	node := &ruleTable
	for _, c := range str {
		if node.childs[c] == nil {
			node.childs[c] = &trieNode{}
		}
		node = node.childs[c]
	}
	node.token = tok
}

func scanString(s *Scanner) (tok int, pos Pos, lit string) {
	tok, pos = stringLit, s.r.pos()
	ending := s.r.readByte()
	s.buf.Reset()
	for !s.r.eof() {
		ch0 := s.r.readByte()
		if ch0 == ending {
			if byte(s.r.peek()) != ending {
				lit = s.buf.String()
				return
			}
			s.r.inc()
			s.buf.WriteByte(ch0)
		} else if ch0 == '\\' {
			if s.r.eof() {
				break
			}
			s.handleEscape(byte(s.r.peek()), &s.buf)
			s.r.inc()
		} else {
			s.buf.WriteByte(ch0)
		}

	}
	tok = 0
	return
}

// handleEscape handles the case in scanString when previous char is '\'.
func (*Scanner) handleEscape(b byte, buf *bytes.Buffer) {
	var ch0 byte
	/*
		\" \' \\ \n \0 \b \Z \r \t ==> escape to one char
		\% \_ ==> preserve both char
		other ==> remove \
	*/
	switch b {
	case 'n':
		ch0 = '\n'
	case '0':
		ch0 = 0
	case 'b':
		ch0 = 8
	case 'Z':
		ch0 = 26
	case 'r':
		ch0 = '\r'
	case 't':
		ch0 = '\t'
	case '%', '_':
		buf.WriteByte('\\')
		ch0 = b
	default:
		ch0 = b
	}
	buf.WriteByte(ch0)
}

func startWithNumber(s *Scanner) (tok int, pos Pos, lit string) {

	pos = s.r.pos()
	tok = intLit
	ch0 := s.r.readByte()

	s.scanDigits()
	ch0 = byte(s.r.peek())
	if ch0 == '.' || ch0 == 'e' || ch0 == 'E' {
		return s.scanFloat(&pos)
	}

	if !s.r.eof() && isIdentChar(rune(ch0)) {
		s.r.incAsLongAs(isIdentChar)
		return identifier, pos, s.r.data(&pos)
	}
	lit = s.r.data(&pos)
	return
}
