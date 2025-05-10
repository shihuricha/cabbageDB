package gobReg

import (
	"cabbageDB/log"
	"cabbageDB/server"
	"cabbageDB/sql/catalog"
	"cabbageDB/sql/engine"
	"cabbageDB/sql/expr"
	"cabbageDB/sqlparser/ast"
	"cabbageDB/storage"
)

// gob注册结构体并初始化 避免序列化时字节变化
func GobRegMain() {
	storage.GobReg()
	engine.GobReg()
	catalog.GobReg()
	server.GobReg()
	expr.GobReg()
	log.GobReg()
	ast.GobReg()

}
