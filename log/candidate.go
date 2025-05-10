package log

import (
	"cabbageDB/logger"
)

type Candidate struct {
	Votes            map[NodeID]struct{}
	ElectionDuration Ticks
	ElectionTimeout  Ticks
	NodeInfo         *NodeInfo
}

func NewCandidate(nodeInfo *NodeInfo) *Candidate {
	return &Candidate{
		Votes:            make(map[NodeID]struct{}),
		ElectionDuration: 0,
		ElectionTimeout:  randElectionTimeout(),
		NodeInfo:         nodeInfo,
	}
}

func (c *Candidate) Step(msg Message) Node {
	info := c.Info()
	if msg.Term < info.Term && msg.Term > 0 {
		logger.Info("Dropping message from past term ", msg)
		return c
	}
	if msg.Term > info.Term {
		return c.NodeInfo.BecomeFollower(msg.Term, 0, nil).Step(msg)
	}

	switch v := msg.Event.(type) {
	case *GrantVote:
		id := GetNodeID(msg.From)
		if id != 0 {
			c.Votes[id] = struct{}{}
		}
		if len(c.Votes) >= c.NodeInfo.Quorum() {
			return c.NodeInfo.BecomeLeader()
		}
	case *HeartBeat, *AppendEntries:
		id := GetNodeID(msg.From)
		return c.NodeInfo.BecomeFollower(msg.Term, id, nil).Step(msg)
	case *ClientRequest:
		c.NodeInfo.Send(msg.From, &ClientResponse{
			ID:       v.ID,
			Response: &RaftError{},
		})
	}
	return c
}

func (c *Candidate) Tick() Node {
	c.ElectionDuration += 1

	if c.ElectionDuration >= c.ElectionTimeout {
		c = NewCandidate(c.NodeInfo)
		c.Campaign()
	}
	return c
}

func (c *Candidate) Info() *NodeInfo {
	return c.NodeInfo
}

func (c *Candidate) Campaign() {
	c.NodeInfo.Term += 1
	c.Votes[c.NodeInfo.ID] = struct{}{}
	c.NodeInfo.Log.SetTerm(c.NodeInfo.Term, c.NodeInfo.ID)
	lastIndex, lastTerm := c.NodeInfo.Log.GetLastIndex()
	c.NodeInfo.Send([]byte{AddressPrefix, BroadcastPrefix}, &SolicitVote{
		LastIndex: lastIndex,
		LastTerm:  lastTerm,
	})
}
