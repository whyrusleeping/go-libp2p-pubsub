package pubsub

import (
	"time"

	"github.com/ipfs/go-libp2p/p2p/net"
	"github.com/ipfs/go-libp2p/p2p/protocol"

	"gx/ipfs/QmY1xNhBfF9xA1pmD8yejyQAyd77K68qNN6JPM1CN2eiRu/go-libp2p-peer"
	"gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"
	logging "gx/ipfs/Qmazh5oNUVsDZTs2g59rq8aYQqwpss8tcUWQzor5sCCEuH/go-log"
)

var SubRepairTimeout = time.Second * 15

var log = logging.Logger("pubsub")

type client struct {
	sub      *subtree
	out      chan []byte
	cancel   func()
	rootPeer peer.ID
	protoid  protocol.ID
}

func (c *client) Messages() <-chan []byte {
	return c.out
}

func (c *client) Close() error {
	defer c.cancel()
	c.sub.h.RemoveStreamHandler(c.protoid)
	return c.sub.Close()
}

func (cli *client) streamHandler(s net.Stream) {
	mes, err := readMessage(s)
	if err != nil {
		log.Error(err)
		s.Close()
		return
	}

	switch mes.Type {
	case Join:
		if err := cli.sub.joinNewPeer(s); err != nil {
			log.Error("error handling peer join: ", err)
		}
	case Update:
		in, err := cli.sub.joinParents(s, mes)
		if err != nil {
			log.Error("tree repair connection failed: ", err)
		}

		select {
		case cli.sub.pause <- in:
		case <-cli.sub.ctx.Done():
			return
		}
	default:
		log.Error("unrecognized message type: ", mes.Type)
	}
}

func (tm *TopicManager) Subscribe(ctx context.Context, itor peer.ID, topic string) (*client, error) {
	ctx, cancel := context.WithCancel(ctx)

	protoid := protocol.ID(string(itor) + "/" + topic)
	s, err := tm.h.NewStream(ctx, protoid, itor)
	if err != nil {
		return nil, err
	}

	sub := newSubtree(ctx, tm.h, protoid)
	sub.in = s

	cli := &client{
		sub:      sub,
		out:      make(chan []byte, 16),
		cancel:   cancel,
		rootPeer: itor,
		protoid:  protoid,
	}

	tm.h.SetStreamHandler(protoid, cli.streamHandler)

	if err := sub.joinToPeer(ctx, s); err != nil {
		return nil, err
	}

	go cli.processMessages()

	return cli, nil
}

func (cli *client) rejoinRoot() error {
	panic("not yet implemented")
}

func (cli *client) processMessages() {
	defer close(cli.out)
	defer cli.sub.in.Close()
	for {
		m, err := readMessage(cli.sub.in)
		if err != nil {
			cli.sub.in = nil
			log.Infof("subscription paused, error on read: %s", err)
			select {
			case <-cli.sub.ctx.Done():
				return
			case cli.sub.in = <-cli.sub.pause:
				log.Info("resumed subscription")
			case <-time.After(SubRepairTimeout):
				log.Error("timed out waiting for subscription to be repaired.")
				if err := cli.rejoinRoot(); err != nil {
					log.Error(err)
					return
				}
			}

			continue
		}

		select {
		case cli.out <- m.Data:
		case <-cli.sub.ctx.Done():
			return
		}

		cli.sub.forwardMessage(m)
	}
}
