package pubsub

import (
	"encoding/json"
	"io"
	"sync"

	"gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"

	"gx/ipfs/QmRBqJF7hb8ZSpRcMwUt8hNhydWcxGEhtk81HKq6oUwKvs/go-libp2p-peer"
	"gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/host"
	"gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/net"
	"gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/protocol"
)

const DefaultTreeWidth = 2
const DefaultTreeMaxWidth = 5

type TopicManager struct {
	Topics   map[string]*Topic
	topicslk sync.Mutex

	h host.Host
}

func NewTopicManager(h host.Host) *TopicManager {
	return &TopicManager{
		Topics: make(map[string]*Topic),
		h:      h,
	}
}

type Topic struct {
	h host.Host

	sub *subtree

	peers map[peer.ID]net.Stream
	plock sync.Mutex

	id    protocol.ID
	title string

	ctx context.Context

	tm *TopicManager
}

type TreeOpts struct {
	TreeWidth    int
	TreeMaxWidth int
}

func (tm *TopicManager) NewTopic(ctx context.Context, title string, opts ...TreeOpts) *Topic {
	id := protocol.ID(string(tm.h.ID()) + "/" + title)
	t := &Topic{
		h:     tm.h,
		tm:    tm,
		id:    id,
		ctx:   ctx,
		peers: make(map[peer.ID]net.Stream),
		title: title,
		sub:   newSubtree(ctx, tm.h, id),
	}

	if len(opts) > 0 {
		t.sub.treeWidth = opts[0].TreeWidth
		t.sub.treeMaxWidth = opts[0].TreeMaxWidth
	} else {
		t.sub.treeWidth = DefaultTreeWidth
		t.sub.treeMaxWidth = DefaultTreeMaxWidth
	}

	tm.h.SetStreamHandler(t.id, func(s net.Stream) {
		hello, err := readMessage(s)
		if err != nil {
			log.Error(err)
			return
		}

		if hello.Type != Join {
			log.Error("received new stream without join message")
			s.Close()
			return
		}

		err = t.AddPeer(s)
		if err != nil {
			log.Error(err)
			return
		}
	})

	tm.Topics[title] = t

	return t
}

func (t *Topic) Close() error {
	t.h.RemoveStreamHandler(t.id)
	delete(t.tm.Topics, t.title)
	return nil
}

func (t *Topic) AddPeer(s net.Stream) error {
	t.sub.chlock.Lock()
	defer t.sub.chlock.Unlock()
	return t.sub.handleJoin(s, false)
}

func (t *Topic) PublishMessage(mes []byte) error {
	mobj := &Message{
		Type: Data,
		Data: mes,
	}

	// TODO: add signature

	return t.sub.forwardMessage(mobj)
}

func writeMessage(s net.Stream, m *Message) error {
	return json.NewEncoder(s).Encode(m)
}

func readMessage(r io.Reader) (*Message, error) {
	m := new(Message)
	err := json.NewDecoder(r).Decode(m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

type MessageType int

const (
	Data = MessageType(iota)
	Join
	Part
	Update
	State
)

type Message struct {
	Type         MessageType
	Data         []byte   `json:"data,omitempty"`
	Peers        []string `json:"parents,omitempty"`
	TreeWidth    int      `json:"treewidth,omitempty"`
	TreeMaxWidth int      `json:"treemaxwidth,omitempty"`
	NumPeers     int      `json:"numpeers,omitempty"`
}
