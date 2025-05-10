package log

import (
	"cabbageDB/logger"
	"cabbageDB/util"
	"github.com/google/uuid"
	"net"
	"time"
)

type Server struct {
	Node   Node
	Peers  map[NodeID]string
	NodeRx <-chan Message
}

func NewServer(id NodeID, peers map[NodeID]string, log *RaftLog, state RaftTxnState) *Server {
	nodechan := make(chan Message, 10)

	peersHashSet := map[NodeID]struct{}{}
	for peer, _ := range peers {
		peersHashSet[peer] = struct{}{}
	}
	node := NewNode(id, peersHashSet, log, state, nodechan)
	return &Server{
		Node:   node,
		Peers:  peers,
		NodeRx: nodechan,
	}
}

func (s *Server) Serve(listener net.Listener, clientRx <-chan RaftMessage) {
	tcpin := make(chan Message, 10)
	tcpout := make(chan Message, 10)
	go s.TcpReceive(listener, tcpin)
	go s.TcpSender(s.Peers, tcpout)
	go s.EventLoop(s.Node, s.NodeRx, clientRx, tcpin, tcpout)
}

func (s *Server) EventLoop(node Node, nodeRx <-chan Message, clientRx <-chan RaftMessage, tcpRx <-chan Message, tcpTx chan<- Message) {
	timer := time.NewTicker(100 * time.Millisecond)
	requests := make(map[string]chan<- Response)

	for {
		select {
		case <-timer.C:
			node = node.Tick()
		case msg := <-tcpRx:
			// 从另一个节点接收信息
			node = node.Step(msg)
		case msg := <-nodeRx:
			if msg.To[0] != AddressPrefix {
				continue
			}
			switch msg.To[1] {
			case NodePrefix:
				tcpTx <- msg
				// 发送给另一个节点
			case BroadcastPrefix:
				// 广播给config里的节点
				tcpTx <- msg
			case ClientPrefix:
				if event, ok := msg.Event.(*ClientResponse); ok {
					uuidStr := event.ID.String()
					responseTx, ok1 := requests[uuidStr]
					if ok1 {
						responseTx <- event.Response
						delete(requests, uuidStr)
					}
				}
			}
		case clientMsg := <-clientRx:
			newUUID := uuid.New()
			msg := Message{
				From: []byte{AddressPrefix, ClientPrefix},
				To:   SetNodeID(node.Info().ID),
				Term: 0,
				Event: &ClientRequest{
					ID:      newUUID,
					Request: clientMsg.Request,
				},
			}
			node = node.Step(msg)
			requests[newUUID.String()] = clientMsg.ResponseTx
		}
	}
}

func (s *Server) TcpSender(peers map[NodeID]string, outrx <-chan Message) {
	peersTxs := make(map[NodeID]chan<- Message)
	for nodeID, addr := range peers {
		msgchan := make(chan Message, 10)
		peersTxs[nodeID] = msgchan
		go s.TcpSendPeer(addr, msgchan)
	}

	for msg := range outrx {

		to := []NodeID{}

		if msg.To[0] != AddressPrefix {
			continue
		}

		switch msg.To[1] {
		case BroadcastPrefix:
			for nodeID, _ := range peersTxs {
				to = append(to, nodeID)
			}

		case NodePrefix:
			var nodeID NodeID
			util.ByteToInt(msg.To[2:], &nodeID)
			to = append(to, nodeID)
		}

		for _, id := range to {
			peersTxs[id] <- msg
		}
	}
}

func (s *Server) TcpSendPeer(addr string, outrx <-chan Message) {

	var conn net.Conn
	var err error
	for msg := range outrx {
		for i := 0; i < 3; i++ {
			conn, err = net.Dial("tcp", addr)

			if err != nil {
				time.Sleep(1 * time.Second)
				conn = nil
				continue
			}

			defer conn.Close()

			msgByte := util.BinaryStructToByte(&msg)
			err = util.SendPrefixMsg(conn, [2]byte{AddressPrefix, NodePrefix}, msgByte)
			if err == nil {
				break
			}
		}
	}

}

func (s *Server) TcpReceive(listener net.Listener, tcpintx chan<- Message) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Info("TcpReceive err:" + err.Error())
		}

		var preFix [2]byte
		conn.Read(preFix[:])
		if preFix[0] != AddressPrefix && preFix[1] != NodePrefix {
			conn.Close()
			continue
		}
		msgByte := util.ReceiveMsg(conn)
		var message Message
		util.ByteToStruct(msgByte, &message)
		tcpintx <- message
	}
}
