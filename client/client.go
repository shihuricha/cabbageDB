package main

import (
	"cabbageDB/server"
	"cabbageDB/sql/catalog"
	"cabbageDB/util"
	"errors"
	"fmt"
	"net"
)

type Client struct {
	Conn net.Conn
	Txn  *Txn
}

type Txn struct {
	TxnID  uint64
	Status bool
}

func (c *Client) Call(request server.RaftRequest) server.RaftResponse {
	prefix := [2]byte{server.ClientPrefix}
	var err error
	switch v := request.(type) {
	case *server.Execute:
		prefix[1] = server.ExecutePrefix
		reqByte := util.BinaryStructToByte(v)
		err = util.SendPrefixMsg(c.Conn, prefix, reqByte)

	case *server.GetTable:
		prefix[1] = server.GetTablePrefix
		reqByte := []byte(v.Data)
		err = util.SendPrefixMsg(c.Conn, prefix, reqByte)

	case *server.ListTables:
		prefix[1] = server.ListTablesPrefix
		_, err = c.Conn.Write(prefix[:])

	case *server.Status:
		prefix[1] = server.StatusPrefix
		_, err = c.Conn.Write(prefix[:])
	}
	if err != nil {
		fmt.Println("client disconnected, restart client:", err.Error())
		return nil
	}

	var respPrefix [2]byte
	_, _ = c.Conn.Read(respPrefix[:])

	if respPrefix[0] != server.ClientPrefix {
		fmt.Println("protocol validation failed: invalid packet header")
		return nil
	}
	switch respPrefix[1] {
	case server.ExecutePrefix:
		resultSet := server.ExecuteResp{}
		respByte := util.ReceiveMsg(c.Conn)
		util.ByteToStruct(respByte, &resultSet)
		return &resultSet
	case server.ListTablesPrefix:
		resultSet := server.ListTables{}
		respByte := util.ReceiveMsg(c.Conn)
		util.ByteToStruct(respByte, &resultSet)
		return &resultSet
	case server.GetTablePrefix:
		resultSet := server.GetTableResp{}
		respByte := util.ReceiveMsg(c.Conn)
		util.ByteToStruct(respByte, &resultSet)
		return &resultSet
	case server.StatusPrefix:
		resultSet := server.Status{}
		respByte := util.ReceiveMsg(c.Conn)
		util.ByteToStruct(respByte, &resultSet)
		return &resultSet
	case server.RespErrPrefix:
		errResultSet := server.RespError{}
		respByte := util.ReceiveMsg(c.Conn)
		util.ByteToStruct(respByte, &errResultSet)
		return &errResultSet
	}
	return nil
}

func (c *Client) Execute(query string) (catalog.ResultSet, error) {
	resp := c.Call(&server.Execute{
		Data: query,
	})
	if resp == nil {
		return nil, errors.New("server is not responding")
	}
	errMsg, ok := resp.(*server.RespError)
	if ok {
		return nil, errors.New(errMsg.Errmsg)
	}

	resultSet := resp.(*server.ExecuteResp).Data

	switch v := resultSet.(type) {
	case *catalog.QueryResultSet:

		return &catalog.QueryResultSet{
			Columns: v.Columns,
			Rows:    v.Rows,
		}, nil
	case *catalog.BeginResultSet:
		c.Txn = &Txn{
			TxnID:  v.Version,
			Status: v.ReadOnly,
		}
		return resultSet, nil
	default:
		return resultSet, nil
	}
}

func (c *Client) GetTable(table string) (*catalog.Table, error) {
	result := c.Call(&server.GetTable{
		Data: table,
	})
	if v, ok := result.(*server.GetTableResp); ok {
		return v.Data, nil
	}

	errMsg, ok := result.(*server.RespError)
	if ok {
		return nil, errors.New(errMsg.Errmsg)
	}
	return nil, nil
}

func (c *Client) ListTables() []string {
	result := c.Call(&server.ListTables{})
	if v, ok := result.(*server.ListTables); ok {
		return v.Data
	}
	return nil
}

func (c *Client) Status() *catalog.Status {
	result := c.Call(&server.Status{})
	if v, ok := result.(*server.Status); ok {
		return v.Data
	}
	return nil
}
