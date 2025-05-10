package log

import (
	"bytes"
	"cabbageDB/util"
	"github.com/google/btree"
)

type RaftTxnState interface {
	GetAppliedIndex() uint64
	Apply(entry *Entry) ([]byte, error)
	Query(command []byte) ([]byte, error)
}

type Instruction interface {
	instruction()
}

type Abort struct {
}

func (*Abort) instruction()  {}
func (*Abort) responseType() {}

type Apply struct {
	Entry *Entry
}

func (*Apply) instruction() {}

type Notify struct {
	ID      RequestID
	Address Address
	Index   Index
}

func (*Notify) instruction() {
}

type Query struct {
	ID      RequestID
	Address Address
	Command []byte
	Term    Term
	Index   Index
	Quorum  uint64
}

func (*Query) instruction() {
}

type Status struct {
	ID      RequestID
	Address Address
	Statue  *NodeStatus
}

func (*Status) instruction() {
}

type Vote struct {
	Term    Term
	Index   Index
	Address Address
}

func (*Vote) instruction() {
}

type AddressValue struct {
	ToAddress Address
	ID        RequestID
}

type Driver struct {
	NodeID  NodeID
	StateRx <-chan Instruction
	NodeTX  chan<- Message
	Notify  map[Index]*AddressValue
	Queries Queries
}
type DriverQuery struct {
	ID      RequestID
	Term    Term
	Address Address
	Command []byte
	Quorum  uint64
	Votes   map[NodeID]struct{}
}

type Queries struct {
	Tree *btree.BTree
}

type QueriesItem struct {
	Key   Index
	Value *btree.BTree
}

func (q *QueriesItem) Less(than btree.Item) bool {
	other := than.(*QueriesItem)
	return q.Key < other.Key
}

type QueriesItemValueItem struct {
	ID    RequestID
	Query DriverQuery
}

func (q *QueriesItemValueItem) Less(than btree.Item) bool {
	other := than.(*QueriesItemValueItem)
	qid := ([16]byte)(q.ID)
	otherid := ([16]byte)(other.ID)
	return bytes.Compare(qid[:], otherid[:]) < 0
}

func (driver *Driver) NotifyApplied(appliedIndex Index, result []byte) {
	if v, ok := driver.Notify[appliedIndex]; ok {
		clientResp := ClientResponse{
			ID:       v.ID,
			Response: &RaftMutate{Command: result},
		}
		driver.Send(v.ToAddress, &clientResp)
	}

}

func (driver *Driver) NotifyAppliedError(appliedIndex Index, err error) {
	if v, ok := driver.Notify[appliedIndex]; ok {
		clientResp := ClientResponse{
			ID:       v.ID,
			Response: &RaftError{Errmsg: err},
		}
		driver.Send(v.ToAddress, &clientResp)
	}

}

func (driver *Driver) QueryRead(appliedIndex Index) []*DriverQuery {
	ready := []*DriverQuery{}
	empty := []*QueriesItem{}

	driver.Queries.Tree.Ascend(func(i btree.Item) bool {
		queryItem := i.(*QueriesItem)
		if queryItem.Key > appliedIndex {
			return false
		}
		queryItem.Value.Ascend(func(i btree.Item) bool {
			valueItem := i.(*QueriesItemValueItem)

			if uint64(len(valueItem.Query.Votes)) >= valueItem.Query.Quorum {
				ready = append(ready, &valueItem.Query)
			}

			if len(ready) > 0 {
				empty = append(empty, queryItem)
			}

			return true
		})

		return false
	})
	for _, queryItem := range empty {
		driver.Queries.Tree.Delete(queryItem)
	}

	return ready
}

// 执行查询
func (driver *Driver) QueryExecute(state RaftTxnState) {
	for _, query := range driver.QueryRead(Index(state.GetAppliedIndex())) {
		result, err := state.Query(query.Command)
		event := ClientResponse{
			ID:       query.ID,
			Response: &RaftQuery{Command: result},
		}
		if err != nil {
			event.Response = &RaftError{Errmsg: err}
		}

		driver.Send(query.Address, &event)
	}
}

func (driver *Driver) Apply(state RaftTxnState, entry *Entry) Index {
	result, err := state.Apply(entry)
	if err != nil {
		driver.NotifyAppliedError(Index(state.GetAppliedIndex()), err)
		driver.QueryExecute(state)
		return Index(state.GetAppliedIndex())
	}
	driver.NotifyApplied(Index(state.GetAppliedIndex()), result)
	driver.QueryExecute(state)
	return Index(state.GetAppliedIndex())
}

func (driver *Driver) Send(to Address, event Event) {
	msg := Message{
		From:  SetNodeID(driver.NodeID),
		To:    to,
		Term:  0,
		Event: event,
	}
	driver.NodeTX <- msg
}

func (driver *Driver) ApplyLog(state RaftTxnState, log *RaftLog) Index {
	appliedIndex := Index(state.GetAppliedIndex())
	commitIndex, _ := log.GetCommitIndex()

	if appliedIndex > commitIndex {
		panic("applied index above commit index")
	}
	if appliedIndex < commitIndex {
		entryList := log.Scan(appliedIndex, commitIndex, false, true)

		for _, entry := range entryList {
			driver.Apply(state, entry)
		}

	}

	return Index(state.GetAppliedIndex())
}

func (driver *Driver) Drive(state RaftTxnState) {
	for instruction := range driver.StateRx {
		driver.Execute(instruction, state)
	}
}

func (driver *Driver) Execute(i Instruction, state RaftTxnState) {
	switch v := i.(type) {
	case *Abort:
		driver.NotifyAbort()
		driver.QueryAbort()
	case *Apply:
		driver.Apply(state, v.Entry)
	case *Notify:
		if v.Index > Index(state.GetAppliedIndex()) {
			driver.Notify[v.Index] = &AddressValue{
				ToAddress: v.Address,
				ID:        v.ID,
			}
		} else {
			event := ClientResponse{
				ID:       v.ID,
				Response: &RaftError{},
			}
			driver.Send(v.Address, &event)
		}
	case *Query:

		queriesItemValue := btree.New(2)
		queriesItemValue.ReplaceOrInsert(&QueriesItemValueItem{
			ID: v.ID,
			Query: DriverQuery{
				ID:      v.ID,
				Term:    v.Term,
				Address: v.Address,
				Quorum:  v.Quorum,
				Votes:   make(map[NodeID]struct{}),
				Command: v.Command,
			},
		})
		driver.Queries.Tree.ReplaceOrInsert(&QueriesItem{
			Key:   v.Index,
			Value: queriesItemValue,
		})
	case *Status:
		v.Statue.ApplyIndex = Index(state.GetAppliedIndex())
		event := ClientResponse{
			ID: v.ID,
			Response: &RaftStatus{
				Status: v.Statue,
			},
		}
		driver.Send(v.Address, &event)
	case *Vote:
		driver.QueryVote(v.Term, v.Index, v.Address)
		driver.QueryExecute(state)
	}
}

// 某地址对某一任期内截止并包含指定提交索引的查询所投的票数
func (driver *Driver) QueryVote(term Term, commitIndex Index, address Address) {
	driver.Queries.Tree.Ascend(func(i btree.Item) bool {
		queryies := i.(*QueriesItem)
		if queryies.Key != commitIndex {
			return false
		}
		queryies.Value.Ascend(func(i btree.Item) bool {
			valueItem := i.(*QueriesItemValueItem)
			if term >= valueItem.Query.Term {
				if address[0] != AddressPrefix || address[1] != NodePrefix {
					return false
				}
				var nodeID uint8
				util.ByteToInt(address[2:], &nodeID)
				valueItem.Query.Votes[nodeID] = struct{}{}
			}
			return true
		})
		return true
	})

}

func (driver *Driver) NotifyAbort() {
	for _, v := range driver.Notify {
		event := ClientResponse{
			ID:       v.ID,
			Response: &RaftError{},
		}
		driver.Send(v.ToAddress, &event)
	}
}

func (driver *Driver) QueryAbort() {
	driver.Queries.Tree.Ascend(func(i btree.Item) bool {
		queryItem := i.(*QueriesItem)
		queryItem.Value.Ascend(func(i btree.Item) bool {
			valueItem := i.(*QueriesItemValueItem)
			event := ClientResponse{
				ID:       valueItem.ID,
				Response: &RaftError{},
			}
			driver.Send(valueItem.Query.Address, &event)
			return true
		})
		return true
	})
}

func NewDriver(nodeID NodeID, stateRx <-chan Instruction, nodeTX chan<- Message) *Driver {

	queries := btree.New(2)
	return &Driver{
		NodeID:  nodeID,
		StateRx: stateRx,
		NodeTX:  nodeTX,
		Notify:  make(map[Index]*AddressValue),
		Queries: Queries{
			Tree: queries,
		},
	}
}
