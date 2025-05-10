package log

import (
	"cabbageDB/logger"
	"math/rand"
	"strconv"
	"time"
)

type NodeID = uint8

// 定义选举超时范围
const (
	ELECTION_TIMEOUT_MIN Ticks = 10
	ELECTION_TIMEOUT_MAX Ticks = 20
)

type Ticks = uint8

const HEARTBEAT_INTERVAL Ticks = 3

// randElectionTimeout 生成一个随机的选举超时时间
func randElectionTimeout() Ticks {
	// 初始化随机数生成器，确保每次运行程序时产生的序列不同
	rand.Seed(time.Now().UnixNano())
	return Ticks(rand.Intn(int(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN)) + int(ELECTION_TIMEOUT_MIN))
}

type NodeInfo struct {
	ID      NodeID
	Peers   map[NodeID]struct{}
	Term    Term
	Log     *RaftLog
	NodeTx  chan<- Message
	StateTx chan<- Instruction
}

type Node interface {
	Step(message Message) Node
	Tick() Node
	Info() *NodeInfo
}

func (info *NodeInfo) Quorum() int {
	return (len(info.Peers)+1)/2 + 1
}

func (info *NodeInfo) Send(to Address, event Event) {
	msg := Message{
		Term:  info.Term,
		From:  SetNodeID(info.ID),
		To:    to,
		Event: event,
	}
	info.NodeTx <- msg

}

func (info *NodeInfo) BecomeFollower(term Term, leader NodeID, votedFor *NodeID) Node {
	if leader != 0 {
		logger.Info("Lost election, following leader " + strconv.Itoa(int(leader)) + " in term " + strconv.Itoa(int(term)))
		id := info.ID
		if votedFor != nil {
			id = *votedFor
		}
		return NewFollower(leader, id, info)
	} else {
		info.Term = term
		info.Log.SetTerm(term, 0)
		return NewFollower(0, 0, info)
	}
}

func (info *NodeInfo) BecomeLeader() Node {
	last_index, _ := info.Log.GetLastIndex()
	node := NewLeader(info.Peers, last_index, info)
	node.HeartBeat()
	node.Propose([]byte{})
	return node
}

func NewNode(id NodeID, peers map[NodeID]struct{}, log *RaftLog, state RaftTxnState, nodeTx chan<- Message) Node {
	stateChan := make(chan Instruction)
	driver := NewDriver(id, stateChan, nodeTx)
	driver.ApplyLog(state, log)
	go driver.Drive(state)
	term, votedFor := log.GetTerm()

	nodeInfo := NodeInfo{
		ID:      id,
		Peers:   peers,
		Term:    term,
		Log:     log,
		NodeTx:  nodeTx,
		StateTx: stateChan,
	}

	if len(nodeInfo.Peers) == 0 {
		if votedFor != id {
			nodeInfo.Term += 1
			nodeInfo.Log.SetTerm(nodeInfo.Term, id)
		}
		lastIndex, _ := nodeInfo.Log.GetLastIndex()

		return NewLeader(make(map[NodeID]struct{}), lastIndex, &nodeInfo)
	} else {
		return NewFollower(0, votedFor, &nodeInfo)
	}

}

type NodeStatus struct {
	Server        NodeID
	Leader        NodeID
	Term          Term
	NodeLastIndex map[NodeID]Index
	CommitIndex   Index
	ApplyIndex    Index
	Storage       string
	StorageSize   uint64
	FileName      string
}
