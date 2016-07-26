package pubsub

import (
	"fmt"
	"io"
	"sync"

	"gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"

	"gx/ipfs/QmRBqJF7hb8ZSpRcMwUt8hNhydWcxGEhtk81HKq6oUwKvs/go-libp2p-peer"
	"gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/host"
	"gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/net"
	"gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/protocol"
)

type subtree struct {
	children map[peer.ID]*child
	chlock   sync.Mutex

	treeWidth    int
	treeMaxWidth int

	size int

	protoid protocol.ID

	in net.Stream

	h host.Host

	pause chan net.Stream

	ctx context.Context
}

type child struct {
	id       peer.ID
	s        net.Stream
	size     int
	children []string
	chlock   sync.Mutex

	dead bool
}

func (sub *subtree) handleChildMessages(c *child) {
	for {
		m, err := readMessage(c.s)
		if err != nil {
			if err != io.EOF {
				log.Error("error reading message from child: ", err)
			}
			return
		}

		switch m.Type {
		case State:
			c.chlock.Lock()
			c.size = m.NumPeers + 1
			c.children = m.Peers // TODO: convert these back to peer.IDs?
			c.chlock.Unlock()
		case Part:
			c.chlock.Lock()
			c.dead = true
			c.chlock.Unlock()

			if err := sub.redistributeChildren(c); err != nil {
				log.Infof("error redistributing children of %s: %s", c.id, err)
				return
			}

		default:
			log.Error("got weird message from child: ", m)
		}
	}
}

func (sub *subtree) Close() error {
	for _, ch := range sub.children {
		ch.s.Close()
	}

	in := sub.in
	if in != nil {
		partmes := &Message{
			Type: Part,
		}

		err := writeMessage(in, partmes)
		if err != nil {
			log.Error("sending part message to parent: ", err)
		}

		return in.Close()
	}

	return nil
}

func (sub *subtree) joinNewPeer(s net.Stream) error {
	sub.chlock.Lock()
	defer sub.chlock.Unlock()
	return sub.handleJoin(s, false)
}

// handleJoin inserts the given stream into the pubsub tree below this node
// if prio is true, this join has 'priority' meaning more effort will be made to
// join them higher up in the tree (if possible)
// Note: handleJoin should only be called with sub.chlock held
func (sub *subtree) handleJoin(s net.Stream, prio bool) error {
	w := sub.treeWidth
	if prio {
		w = sub.treeMaxWidth
	}

	if len(sub.children) >= w {
		defer s.Close()
		return sub.redirectJoin(s)
	}

	welcome := &Message{
		Type:         Update,
		Peers:        []string{sub.h.ID().Pretty()},
		TreeWidth:    sub.treeWidth,
		TreeMaxWidth: sub.treeMaxWidth,
	}

	err := writeMessage(s, welcome)
	if err != nil {
		s.Close()
		return err
	}

	// if we have a parent, send them an update of our state
	// NOTE: this doesnt always need to be done, multiple state updates
	// could be batched together.
	if sub.in != nil {
		notif := &Message{
			Type:     State,
			Peers:    []string{s.Conn().RemotePeer().Pretty()},
			NumPeers: sub.size,
		}

		if err := writeMessage(sub.in, notif); err != nil {
			return err
		}
	}

	c := &child{s: s, id: s.Conn().RemotePeer()}
	go sub.handleChildMessages(c)

	sub.children[c.id] = c
	return nil
}

func (sub *subtree) redirectJoin(s net.Stream) error {
	if len(sub.children) == 0 {
		return fmt.Errorf("called redirectJoin with no child peers")
	}

	min := 10000000000
	var minc *child
	for _, c := range sub.children {
		c.chlock.Lock()
		if !c.dead && c.size < min {
			min = c.size
			minc = c
		}
		c.chlock.Unlock()
	}

	if minc == nil {
		fmt.Errorf("critical: failed to find child with minimum size")
	}

	minc.chlock.Lock()
	minc.size++
	minc.chlock.Unlock()

	redir := &Message{
		Type:         Update,
		Peers:        []string{minc.s.Conn().RemotePeer().Pretty()},
		TreeWidth:    sub.treeWidth,
		TreeMaxWidth: sub.treeMaxWidth,
	}

	err := writeMessage(s, redir)
	if err != nil {
		return err
	}

	s.Close()
	return nil
}

func (sub *subtree) joinToPeer(ctx context.Context, s net.Stream) error {
	hello := &Message{
		Type: Join,
	}

	err := writeMessage(s, hello)
	if err != nil {
		return err
	}

	m, err := readMessage(s)
	if err != nil {
		return err
	}

	sub.treeWidth = m.TreeWidth
	sub.treeMaxWidth = m.TreeMaxWidth
	// TODO: check these values

	ins, err := sub.joinParents(s, m)
	if err != nil {
		return err
	}

	if ins != s {
		s.Close()
	}

	sub.in = ins
	return nil
}

func translPeerIDs(ps []string) ([]peer.ID, error) {
	var parents []peer.ID
	for _, p := range ps {
		pid, err := peer.IDB58Decode(p)
		if err != nil {
			return nil, err
		}

		parents = append(parents, pid)
	}
	return parents, nil
}

func (sub *subtree) joinParents(itors net.Stream, m *Message) (net.Stream, error) {
	parents, err := translPeerIDs(m.Peers)
	if err != nil {
		return nil, err
	}
	if len(parents) == 0 {
		return nil, fmt.Errorf("received zero parents from initiator")
	}

	var last_err error

	var s net.Stream
	for _, p := range parents {
		if p == itors.Conn().RemotePeer() {
			return itors, nil
		}
		pstr, err := sub.h.NewStream(sub.ctx, sub.protoid, p)
		if err != nil {
			log.Error("failed to connect to pubsub parent: ", err)
			last_err = err
			continue
		}

		err = writeMessage(pstr, &Message{Type: Join})
		if err != nil {
			log.Error("reassign join fail: ", err)
			last_err = err
			continue
		}

		welcome, err := readMessage(pstr)
		if err != nil {
			log.Error("reassign welcome receive fail: ", err)
			last_err = err
			continue
		}

		switch welcome.Type {
		case Update:
			if len(welcome.Peers) == 0 {
				return nil, fmt.Errorf("got welcome update with no parents set")
			}
			if !(len(welcome.Peers) == 1 && welcome.Peers[0] == p.Pretty()) {
				rec_s, err := sub.joinParents(pstr, welcome)
				if err != nil {
					log.Error("recursive joinParents failed: ", err)
					last_err = err
					continue
				}
				pstr.Close()
				pstr = rec_s
			}
		default:
			log.Errorf("%s got bad welcome message from parent: %v", sub.h.ID(), welcome)
			continue
		}

		s = pstr
		break
	}

	if s == nil {
		return nil, fmt.Errorf("could not get connection to tree: ", last_err)
	}

	return s, nil
}

func newSubtree(ctx context.Context, h host.Host, id protocol.ID) *subtree {
	return &subtree{
		children: make(map[peer.ID]*child),
		protoid:  id,
		ctx:      ctx,
		h:        h,
		pause:    make(chan net.Stream),
	}
}

func (sub *subtree) forwardMessage(m *Message) error {
	sub.chlock.Lock()
	defer sub.chlock.Unlock()

	var dead []*child
	for _, c := range sub.children {
		// TODO: in parallel
		c.chlock.Lock()
		ch_dead := c.dead
		c.chlock.Unlock()
		if ch_dead {
			delete(sub.children, c.id)
		}

		err := writeMessage(c.s, m)
		if err != nil {
			dead = append(dead, c)
		}
	}

	// TODO: we could forward this message on to our grandchildren that will otherwise
	// miss it. This would consume a bit more resources on our end, but would provide
	// a lesser chance of dropped messages throughout the system.
	if len(dead) > 0 {
		for _, h := range dead {
			delete(sub.children, h.id)
			if err := sub.redistributeChildren(h); err != nil {
				log.Error(err)
				continue
			}
		}

	}

	return nil
}

func (sub *subtree) redistributeChildren(h *child) error {
	// TODO: in parallel
	for _, child := range h.children {
		pid, err := peer.IDB58Decode(child)
		if err != nil {
			return fmt.Errorf("error decoding peers child ID: %s", err)
		}

		cstr, err := sub.h.NewStream(sub.ctx, sub.protoid, pid)
		if err != nil {
			return fmt.Errorf("error opening stream for tree repair")
		}

		if err := sub.handleJoin(cstr, true); err != nil {
			return fmt.Errorf("repairing child: %s", err)
		}
	}

	return nil
}
