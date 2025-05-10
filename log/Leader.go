package log

import (
	"math"
	"sort"
)

type Progress struct {
	Next Index
	Last Index
}

type Leader struct {
	Progress       map[NodeID]*Progress
	SinceHeartBeat Ticks
	NodeInfo       *NodeInfo
}

func (l *Leader) Step(msg Message) Node {
	if msg.Term < l.NodeInfo.Term && msg.Term > 0 {
		return l
	}
	if msg.Term > l.NodeInfo.Term {
		return l.NodeInfo.BecomeFollower(msg.Term, 0, nil).Step(msg)
	}

	switch v := msg.Event.(type) {
	case *ConfirmLeader:
		from := GetNodeID(msg.From)
		l.NodeInfo.StateTx <- &Vote{
			Term:    msg.Term,
			Index:   v.CommitIndex,
			Address: msg.From,
		}
		if !v.HasCommitted {
			l.SendLog(from)
		}
	case *AcceptEntries:
		from := GetNodeID(msg.From)
		l.Progress[from].Last = v.LastIndex
		l.Progress[from].Next = v.LastIndex + 1
		l.MaybeCommit()
	case *RejectEntries:
		from := GetNodeID(msg.From)
		if l.Progress[from].Next > 1 {
			l.Progress[from].Next = l.Progress[from].Next - 1
		}
		l.SendLog(from)
	case *ClientRequest:
		switch req := v.Request.(type) {
		case *RaftQuery:
			commitIndex, _ := l.NodeInfo.Log.GetCommitIndex()
			l.NodeInfo.StateTx <- &Query{
				ID:      v.ID,
				Address: msg.From,
				Command: req.Command,
				Term:    l.NodeInfo.Term,
				Index:   commitIndex,
				Quorum:  uint64(l.NodeInfo.Quorum()),
			}
			l.NodeInfo.StateTx <- &Vote{
				Term:    l.NodeInfo.Term,
				Index:   commitIndex,
				Address: SetNodeID(l.NodeInfo.ID),
			}
			l.HeartBeat()
		case *RaftMutate:
			index := l.Propose(req.Command)
			l.NodeInfo.StateTx <- &Notify{
				ID:      v.ID,
				Address: msg.From,
				Index:   index,
			}
			if len(l.NodeInfo.Peers) == 0 {
				l.MaybeCommit()
			}
		case *RaftStatus:
			engineStatus := l.NodeInfo.Log.Status()

			lastIndexMap := make(map[NodeID]Index)
			for id, progress := range l.Progress {
				lastIndexMap[id] = progress.Last
			}
			lastIndex, _ := l.NodeInfo.Log.GetLastIndex()
			lastIndexMap[l.NodeInfo.ID] = lastIndex

			commitIndex, _ := l.NodeInfo.Log.GetCommitIndex()

			status := NodeStatus{
				Server:        l.NodeInfo.ID,
				Leader:        l.NodeInfo.ID,
				Term:          l.NodeInfo.Term,
				NodeLastIndex: lastIndexMap,
				ApplyIndex:    0,
				Storage:       engineStatus.Name,
				StorageSize:   engineStatus.Size,
				CommitIndex:   commitIndex,
				FileName:      l.NodeInfo.Log.Engine.FileName(),
			}

			l.NodeInfo.StateTx <- &Status{
				ID:      v.ID,
				Address: msg.From,
				Statue:  &status,
			}
		}
	case *ClientResponse:
		if status, ok := v.Response.(*RaftStatus); ok {
			status.Status.Server = l.NodeInfo.ID
		}
		l.NodeInfo.Send([]byte{AddressPrefix, ClientPrefix}, &ClientResponse{
			ID:       v.ID,
			Response: v.Response,
		})

	}

	return l
}
func (l *Leader) Tick() Node {
	l.SinceHeartBeat += 1
	if l.SinceHeartBeat >= HEARTBEAT_INTERVAL {
		l.HeartBeat()
		l.SinceHeartBeat = 0
	}
	return l
}
func (l *Leader) Info() *NodeInfo {
	return l.NodeInfo
}

func (l *Leader) HeartBeat() {
	commitIndex, commitTerm := l.NodeInfo.Log.GetCommitIndex()
	l.NodeInfo.Send([]byte{AddressPrefix, BroadcastPrefix}, &HeartBeat{
		CommitIndex: commitIndex,
		CommitTerm:  commitTerm,
	})
}

func (l *Leader) Propose(command []byte) Index {
	index := l.Info().Log.Append(l.NodeInfo.Term, command)
	for id := range l.NodeInfo.Peers {
		l.SendLog(id)
	}
	return index
}

func (l *Leader) MaybeCommit() Index {
	indexes := []int{}
	for _, v := range l.Progress {
		indexes = append(indexes, int(v.Last))
	}

	lastIndex, _ := l.NodeInfo.Log.GetLastIndex()

	indexes = append(indexes, int(lastIndex))

	sort.Sort(sort.Reverse(sort.IntSlice(indexes)))
	commitIndex := Index(indexes[l.NodeInfo.Quorum()-1])
	if commitIndex == 0 {
		return commitIndex
	}

	prevCommitIndex, _ := l.NodeInfo.Log.GetCommitIndex()
	if commitIndex < prevCommitIndex {
		return prevCommitIndex
	}

	entry := l.NodeInfo.Log.Get(commitIndex)
	if entry != nil && entry.Term != l.NodeInfo.Term {
		return prevCommitIndex
	}
	if commitIndex > prevCommitIndex {
		l.NodeInfo.Log.Commit(commitIndex)
		scan := l.NodeInfo.Log.Scan(prevCommitIndex, commitIndex, false, true)
		for _, entryItem := range scan {
			l.NodeInfo.StateTx <- &Apply{
				Entry: entryItem,
			}
		}

	}
	return commitIndex
}

func (l *Leader) SendLog(peer NodeID) {

	var baseIndex Index
	var baseTerm Term
	if progress, ok := l.Progress[peer]; ok {
		if progress.Next > 1 {
			entry := l.NodeInfo.Log.Get(progress.Next - 1)
			baseIndex, baseTerm = entry.Index, entry.Term
		}
	} else {
		baseIndex, baseTerm = 0, 0
	}

	entires := l.NodeInfo.Log.Scan(baseIndex+1, math.MaxUint64, true, true)
	to := SetNodeID(peer)
	l.NodeInfo.Send(to, &AppendEntries{
		BaseIndex: baseIndex,
		BaseTerm:  baseTerm,
		Entries:   entires,
	})
}

func NewLeader(peers map[NodeID]struct{}, lastIndex Index, nodeInfo *NodeInfo) *Leader {
	next := lastIndex + 1
	progress := make(map[NodeID]*Progress)

	for nodeID := range peers {
		progress[nodeID] = &Progress{
			Next: next,
			Last: 0,
		}
	}
	return &Leader{
		Progress:       progress,
		SinceHeartBeat: 0,
		NodeInfo:       nodeInfo,
	}
}
