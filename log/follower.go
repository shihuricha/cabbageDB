package log

import (
	"cabbageDB/logger"
)

type Follower struct {
	Leader          NodeID
	LeaderSeen      Ticks
	ElectionTimeout Ticks
	VotedFor        NodeID
	Forwarded       map[RequestID]struct{}
	NodeInfo        *NodeInfo
}

func (f *Follower) Step(msg Message) Node {

	info := f.NodeInfo
	if msg.Term < info.Term && msg.Term > 0 {
		logger.Info("Dropping message from past term ", msg)
		return f
	}
	if msg.Term > f.NodeInfo.Term {
		return f.NodeInfo.BecomeFollower(msg.Term, 0, &f.VotedFor).Step(msg)
	}
	if f.IsLeader(msg.From) {
		f.LeaderSeen = 0
	}
	switch v := msg.Event.(type) {
	case *HeartBeat:
		if f.Leader == 0 {
			return f.Info().BecomeFollower(msg.Term, GetNodeID(msg.From), &f.VotedFor).Step(msg)
		}
		hasCommited := f.NodeInfo.Log.Has(v.CommitIndex, v.CommitTerm)
		oldCommitIndex, _ := f.NodeInfo.Log.GetCommitIndex()
		if hasCommited && v.CommitIndex > oldCommitIndex {
			f.NodeInfo.Log.Commit(v.CommitIndex)
			scan := f.NodeInfo.Log.Scan(oldCommitIndex+1, v.CommitIndex, true, true)
			for _, entry := range scan {
				f.NodeInfo.StateTx <- &Apply{Entry: entry}
			}
		}

		f.NodeInfo.Send(msg.From, &ConfirmLeader{
			CommitIndex:  v.CommitIndex,
			HasCommitted: hasCommited,
		})
	case *AppendEntries:
		from := GetNodeID(msg.From)
		if f.Leader == 0 {
			f.Leader = from
		} else if f.Leader != from {
			logger.Info("Multiple leaders in term")
			return f
		}
		if v.BaseIndex > 0 && !f.NodeInfo.Log.Has(v.BaseIndex, v.BaseTerm) {
			f.NodeInfo.Send(msg.From, &RejectEntries{})
		} else {
			lastIndex := f.NodeInfo.Log.Splice(v.Entries)
			f.NodeInfo.Send(msg.From, &AcceptEntries{
				LastIndex: lastIndex,
			})
		}
	case *SolicitVote:
		from := GetNodeID(msg.From)
		if f.VotedFor != 0 && from != f.VotedFor {
			return f
		}
		logIndex, logTerm := f.NodeInfo.Log.GetLastIndex()

		if v.LastTerm > logTerm || (v.LastTerm == logTerm && v.LastIndex >= logIndex) {
			f.NodeInfo.Send(msg.From, &GrantVote{})
			f.NodeInfo.Log.SetTerm(f.NodeInfo.Term, from)
			f.VotedFor = from
		}
	case *ClientRequest:
		if msg.From[0] != AddressPrefix || msg.From[1] != ClientPrefix {
			return f
		}
		if f.Leader != 0 {
			f.Forwarded[v.ID] = struct{}{}
			to := SetNodeID(f.Leader)
			f.NodeInfo.Send(to, msg.Event)
		} else {
			f.NodeInfo.Send(msg.From, &ClientResponse{
				ID:       v.ID,
				Response: &RaftError{},
			})
		}
	case *ClientResponse:
		if !f.IsLeader(msg.From) {
			return f
		}

		if status, ok := v.Response.(*RaftStatus); ok {
			status.Status.Server = f.NodeInfo.ID
		}
		if _, ok := f.Forwarded[v.ID]; ok {
			f.NodeInfo.Send([]byte{AddressPrefix, ClientPrefix}, &ClientResponse{
				ID:       v.ID,
				Response: v.Response,
			})
			delete(f.Forwarded, v.ID)
		}

	}

	return f
}

func (f *Follower) IsLeader(from Address) bool {
	id := GetNodeID(from)
	if f.Leader == id {
		return true
	}
	return false
}

func (f *Follower) Info() *NodeInfo {
	return f.NodeInfo
}
func (f *Follower) Tick() Node {
	f.LeaderSeen += 1
	if f.LeaderSeen >= f.ElectionTimeout {
		return f.BecomeCandidate()
	}
	return f
}
func (f *Follower) BecomeCandidate() Node {
	f.AbortForwarded()
	candidate := NewCandidate(f.NodeInfo)
	candidate.Campaign()
	return candidate
}

func NewFollower(leader NodeID, votedFor NodeID, nodeInfo *NodeInfo) Node {
	return &Follower{
		Leader:          leader,
		VotedFor:        votedFor,
		LeaderSeen:      0,
		ElectionTimeout: randElectionTimeout(),
		Forwarded:       make(map[RequestID]struct{}),
		NodeInfo:        nodeInfo,
	}
}

func (f *Follower) AbortForwarded() {
	for id := range f.Forwarded {
		f.NodeInfo.Send([]byte{AddressPrefix, ClientPrefix}, &ClientResponse{
			ID:       id,
			Response: &RaftError{},
		})
	}
}
