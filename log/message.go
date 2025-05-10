package log

import (
	"bytes"
	"cabbageDB/util"
	"encoding/gob"
	"github.com/google/uuid"
)

type Message struct {
	Term  Term
	From  Address
	To    Address
	Event Event
}

type Address = []byte

type RequestID = uuid.UUID

const (
	AddressPrefix   byte = 0x07
	BroadcastPrefix byte = 0x02
	NodePrefix      byte = 0x03
	ClientPrefix    byte = 0x04
)

func GetNodeID(addr Address) NodeID {
	var id NodeID
	if len(addr) > 2 && addr[0] == AddressPrefix && addr[1] == NodePrefix {
		util.ByteToInt(addr[2:], &id)
	}
	return id
}
func SetNodeID(id NodeID) []byte {
	return append([]byte{AddressPrefix, NodePrefix}, util.BinaryToByte(id)...)
}

type Event interface {
	event()
}

type HeartBeat struct {
	CommitIndex Index
	CommitTerm  Term
}

func (*HeartBeat) event() {}

type ConfirmLeader struct {
	CommitIndex  Index
	HasCommitted bool
}

func (*ConfirmLeader) event() {}

type SolicitVote struct {
	LastIndex Index
	LastTerm  Term
}

func (*SolicitVote) event() {}

type GrantVote struct {
}

func (*GrantVote) event() {}

type AppendEntries struct {
	BaseIndex Index
	BaseTerm  Term
	Entries   []*Entry
}

func (*AppendEntries) event() {}

type AcceptEntries struct {
	LastIndex Index
}

func (*AcceptEntries) event() {}

type RejectEntries struct {
}

func (*RejectEntries) event() {}

type ClientRequest struct {
	ID      RequestID
	Request Request
}

func (*ClientRequest) event() {}

type ClientResponse struct {
	ID       RequestID
	Response Response
}

func (*ClientResponse) event() {}

type Request interface {
	requestType() []byte
}

type Response interface {
	responseType() []byte
}

type RaftQuery struct {
	Command []byte
}

func (q *RaftQuery) requestType() []byte {
	return q.Command
}

func (q *RaftQuery) responseType() []byte {
	return q.Command
}

type RaftMutate struct {
	Command []byte
}

func (m *RaftMutate) requestType() []byte {
	return m.Command
}

func (m *RaftMutate) responseType() []byte {
	return m.Command
}

type RaftStatus struct {
	Status *NodeStatus
}

func (s *RaftStatus) requestType() []byte {
	return nil
}

func (s *RaftStatus) responseType() []byte {
	return nil
}

type RaftError struct {
	Errmsg error
}

func (e *RaftError) responseType() []byte {
	return nil
}

type RaftMessage struct {
	Request    Request
	ResponseTx chan<- Response
}

func GobReg() {

	gob.Register(&HeartBeat{})
	gob.Register(&ConfirmLeader{})
	gob.Register(&SolicitVote{})
	gob.Register(&GrantVote{})
	gob.Register(&AppendEntries{})
	gob.Register(&AcceptEntries{})
	gob.Register(&RejectEntries{})
	gob.Register(&ClientRequest{})
	gob.Register(&ClientResponse{})
	gob.Register(&RaftQuery{})
	gob.Register(&RaftMutate{})
	gob.Register(&RaftStatus{})
	gob.Register(&RaftError{})

	// 预先编码保证顺序不变
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(&HeartBeat{})
	_ = enc.Encode(&ConfirmLeader{})
	_ = enc.Encode(&SolicitVote{})
	_ = enc.Encode(&GrantVote{})
	_ = enc.Encode(&AppendEntries{})
	_ = enc.Encode(&AppendEntries{})
	_ = enc.Encode(&RejectEntries{})
	_ = enc.Encode(&ClientRequest{})
	_ = enc.Encode(&RaftQuery{})
	_ = enc.Encode(&RaftMutate{})
	_ = enc.Encode(&RaftStatus{})
	_ = enc.Encode(&RaftError{})
}
