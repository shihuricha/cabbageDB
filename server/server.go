package server

import (
	"bytes"
	"cabbageDB/log"
	"cabbageDB/logger"
	"cabbageDB/sql"
	"cabbageDB/sql/catalog"
	"cabbageDB/sql/engine"
	"cabbageDB/util"
	"encoding/gob"
	"errors"
	"net"
	"strings"
)

const (
	ClientPrefix     = 0x08
	ExecutePrefix    = 0x02
	GetTablePrefix   = 0x03
	ListTablesPrefix = 0x04
	StatusPrefix     = 0x05
	RespErrPrefix    = 0x06
)

type Server struct {
	Raft         *log.Server
	RaftListener net.Listener
	SQLListener  net.Listener
}

func NewServer(id log.NodeID, peers map[log.NodeID]string, raftLog *log.RaftLog, state log.RaftTxnState) *Server {
	return &Server{
		Raft: log.NewServer(id, peers, raftLog, state),
	}
}

func (s *Server) Listen(sqlAddr string, raftAddr string) {
	sqlListener, err := net.Listen("tcp", sqlAddr)
	if err != nil {
		panic("listen sqladdr " + sqlAddr + " err:" + err.Error())
	}
	raftLitener, err := net.Listen("tcp", raftAddr)
	if err != nil {
		panic("listen raftaddr" + raftAddr + " err:" + err.Error())
	}
	s.SQLListener = sqlListener
	s.RaftListener = raftLitener

}

func (s *Server) Serve() {
	raftChan := make(chan log.RaftMessage, 10)
	sqlEngine := engine.NewRaft(raftChan)

	s.Raft.Serve(s.RaftListener, raftChan)
	s.ServeSQL(s.SQLListener, sqlEngine)
}

func (s *Server) ServeSQL(listener net.Listener, engine *engine.Raft) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Info("ServeSQL err:" + err.Error())
		}

		session := NewSession(engine)
		go session.Handle(conn)

	}
}

type RaftRequest interface {
	requestType()
}
type Execute struct {
	Data string
}

func (*Execute) requestType() {}

type GetTable struct {
	Data string
}

func (*GetTable) requestType() {}

type RespError struct {
	Errmsg string
}

func (*RespError) requestType()  {}
func (*RespError) responseType() {}

type ListTables struct {
	Data []string
}

func (*ListTables) requestType()  {}
func (*ListTables) responseType() {}

type Status struct {
	Data *catalog.Status
}

func (*Status) requestType()  {}
func (*Status) responseType() {}

type RaftResponse interface {
	responseType()
}

type ExecuteResp struct {
	Data catalog.ResultSet
}

func (*ExecuteResp) responseType() {}

type Row struct {
	Data []*sql.ValueData
}

func (*Row) responseType() {}

type GetTableResp struct {
	Data *catalog.Table
}

func (*GetTableResp) responseType() {}

type ClientSession struct {
	Engine *engine.Raft
	SQL    *catalog.Session
}

func NewSession(engine *engine.Raft) *ClientSession {
	return &ClientSession{
		Engine: engine,
		SQL:    engine.Session(),
	}
}

func (s *ClientSession) Handle(conn net.Conn) {
	defer conn.Close()
	for {
		resp, err := s.Request(conn)
		if _, ok := err.(*NetConn); ok {
			break
		}

		prefix := [2]byte{ClientPrefix}
		respByte := []byte{}

		if err != nil {
			prefix[1] = RespErrPrefix
			respByte = util.BinaryStructToByte(&RespError{Errmsg: err.Error()})
			util.SendPrefixMsg(conn, prefix, respByte)
			continue
		}

		switch v := resp.(type) {
		case *ExecuteResp:
			prefix[1] = ExecutePrefix
			respByte = util.BinaryStructToByte(v)
		case *GetTableResp:
			prefix[1] = GetTablePrefix
			respByte = util.BinaryStructToByte(v)
		case *ListTables:
			prefix[1] = ListTablesPrefix
			respByte = util.BinaryStructToByte(v)
		case *Status:
			prefix[1] = StatusPrefix
			respByte = util.BinaryStructToByte(v)
		}

		util.SendPrefixMsg(conn, prefix, respByte)
	}

}

type NetConn struct {
	Err error
}

func (n *NetConn) Error() string {
	return n.Err.Error()
}

func (s *ClientSession) Request(conn net.Conn) (RaftResponse, error) {

	var prefix [2]byte
	_, err := conn.Read(prefix[:])
	if err != nil {
		return nil, &NetConn{
			Err: err,
		}
	}

	if prefix[0] != ClientPrefix {
		return nil, errors.New("conn protocol validation failed: invalid packet header")
	}

	switch prefix[1] {
	case ExecutePrefix:
		execute := Execute{}
		reqByte := util.ReceiveMsg(conn)
		util.ByteToStruct(reqByte, &execute)
		sqlStr := strings.Split(execute.Data, ";")[0]
		result, err1 := s.SQL.Execute(sqlStr + ";")
		if err1 != nil {
			return nil, err1
		}
		return &ExecuteResp{
			Data: result,
		}, nil

	case GetTablePrefix:
		tableNameByte := util.ReceiveMsg(conn)
		tableStr := string(tableNameByte)
		table, err1 := s.SQL.ReadWithTxn(func(txn catalog.Transaction) (any, error) {
			return txn.MustReadTable(strings.ToLower(tableStr))
		})
		if err1 != nil {
			return nil, err1
		}
		return &GetTableResp{
			Data: table.(*catalog.Table),
		}, nil
	case ListTablesPrefix:
		alltables, _ := s.SQL.ReadWithTxn(func(txn catalog.Transaction) (any, error) {
			return txn.ScanTables(), nil
		})
		var tables []string
		for _, table := range alltables.([]*catalog.Table) {
			tables = append(tables, table.Name)
		}
		return &ListTables{
			Data: tables,
		}, nil
	case StatusPrefix:
		return &Status{
			Data: s.Engine.Status(),
		}, nil
	}

	return nil, errors.New("conn protocol validation failed: invalid packet header")

}

func GobReg() {
	gob.Register(&ListTables{})
	gob.Register(&Execute{})
	gob.Register(&ExecuteResp{})
	gob.Register(&Status{})
	gob.Register(&GetTable{})
	gob.Register(&Row{})

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(&ListTables{})
	_ = enc.Encode(&Execute{})
	_ = enc.Encode(&ExecuteResp{})
	_ = enc.Encode(&Status{})
	_ = enc.Encode(&GetTable{})
	_ = enc.Encode(&Row{})

}
