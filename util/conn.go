package util

import (
	"net"
)

func SendPrefixMsg(conn net.Conn, prefix [2]byte, req []byte) error {
	reqLen := len(req)
	reqLenByte := BinaryToByte(uint64(reqLen))
	reqByte := append(prefix[:], reqLenByte...)
	reqByte = append(reqByte, req...)
	_, err := conn.Write(reqByte)
	return err
}

func ReceiveMsg(conn net.Conn) []byte {
	var msgLen uint64
	tmp := [8]byte{}
	conn.Read(tmp[:])
	ByteToInt(tmp[:], &msgLen)

	cnt := 0
	msgByte := make([]byte, int(msgLen))
	for cnt < int(msgLen) {
		n, _ := conn.Read(msgByte)
		cnt += n
	}
	return msgByte
}
