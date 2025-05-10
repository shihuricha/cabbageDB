package parser

import (
	"github.com/pkg/errors"
	"strings"
)

type Charset struct {
	Name             string
	DefaultCollation string
	Collations       map[string]*Collation
	Desc             string
	Maxlen           int
}

type Collation struct {
	ID          int
	CharsetName string
	Name        string
	IsDefault   bool
}

var charsets = make(map[string]*Charset)

func GetCharsetInfo(cs string) (string, string, error) {
	c, ok := charsets[strings.ToLower(cs)]
	if !ok {
		return "", "", errors.Errorf("Unknown charset %s", cs)
	}
	return c.Name, c.DefaultCollation, nil
}
