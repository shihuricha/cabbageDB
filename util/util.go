package util

import (
	"bytes"
	"cabbageDB/logger"
	"encoding/binary"
	"encoding/gob"
	"reflect"
)

func BinaryToByte[T ~uint32 | int32 | uint64 | uint8 | int64](value T) []byte {
	var buffer bytes.Buffer
	err := binary.Write(&buffer, binary.BigEndian, value)
	if err != nil {
		logger.Info("err===>", err.Error())
	}
	return buffer.Bytes()
}

func ByteToInt[T ~uint32 | int32 | uint64 | uint8 | int64](buf []byte, value *T) error {
	err := binary.Read(bytes.NewReader(buf), binary.BigEndian, value)
	return err
}

func BufferAppend(args ...[]byte) []byte {
	var buffer bytes.Buffer
	for _, v := range args {
		buffer.Write(v)
	}
	return buffer.Bytes()
}

func BoolToByte(b bool) byte {
	if b {
		return 0x01
	} else {
		return 0x00
	}
}
func ByteIterToStruct[T any](value []byte, toStruct *T) {
	if len(value) == 0 {
		return
	}
	decoder := gob.NewDecoder(bytes.NewBuffer(value))
	err := decoder.Decode(toStruct)
	if err != nil {
		logger.Info("Byte to Struct "+reflect.TypeOf(toStruct).String()+"err:", err.Error())
	}

}

func ByteToStruct[T any](value []byte, toStruct *T) {
	if len(value) == 0 {
		return
	}
	decoder := gob.NewDecoder(bytes.NewBuffer(value))
	err := decoder.Decode(&toStruct)
	if err != nil {
		logger.Info("Byte to Struct "+reflect.TypeOf(toStruct).String()+"err:", err.Error())
	}

}

func BinaryStructToByte[T any](v *T) []byte {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(v)
	if err != nil {

		logger.Info("Struct to Byte err:", err.Error())
		return nil
	}

	return buffer.Bytes()
}
