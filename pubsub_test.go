package pubsub

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	context "gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"

	pstore "gx/ipfs/QmQdnfvZQuhdT93LNc5bos52wAmdr3G2p6G8teLJMEN32P/go-libp2p-peerstore"
	"gx/ipfs/QmRBqJF7hb8ZSpRcMwUt8hNhydWcxGEhtk81HKq6oUwKvs/go-libp2p-peer"
	host "gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/host"
	mocknet "gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/net/mock"
	testutil "gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/test/util"
)

func makeHosts(t *testing.T, ctx context.Context, count int) []host.Host {
	mn, err := mocknet.FullMeshConnected(ctx, count)
	if err != nil {
		t.Fatal(err)
	}

	return mn.Hosts()
}

func makeNetHosts(t *testing.T, ctx context.Context, count int) []host.Host {
	var out []host.Host
	for i := 0; i < count; i++ {
		h := testutil.GenHostSwarm(t, ctx)
		out = append(out, h)
	}

	return out
}

func connectUp(hs []host.Host) error {
	gpi := func(h host.Host) pstore.PeerInfo {
		return pstore.PeerInfo{
			ID:    h.ID(),
			Addrs: h.Addrs(),
		}
	}

	ctx := context.Background()

	for i := 0; i < len(hs); i++ {
		for j := i + 1; j < len(hs); j++ {
			err := hs[i].Connect(ctx, gpi(hs[j]))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func closeSubs(clis []*client) {
	for _, c := range clis {
		c.Close()
	}
}

func initPubsub(t *testing.T, ctx context.Context, hs []host.Host) (*Topic, []*TopicManager, []*client) {
	var tms []*TopicManager
	for _, h := range hs {
		tms = append(tms, NewTopicManager(h))
	}

	title := "foobar"
	topic := tms[0].NewTopic(ctx, title)

	var subchs []*client
	for _, tm := range tms[1:] {
		subs, err := tm.Subscribe(ctx, hs[0].ID(), title)
		if err != nil {
			t.Fatal(err)
		}
		subchs = append(subchs, subs)
	}
	return topic, tms, subchs
}

func clearWaitingMessages(subs []*client) {
	for _, s := range subs {
		looping := true
		for looping {
			select {
			case _, ok := <-s.Messages():
				if !ok {
					looping = false
				}
			default:
				looping = false
			}
		}
	}
}

func checkSystem(t *Topic, subs []*client, skip map[int]bool, mid int) error {
	if skip == nil {
		skip = make(map[int]bool)
	}

	mes := []byte(fmt.Sprintf("message number %d", mid))

	err := t.PublishMessage(mes)
	if err != nil {
		return err
	}

	for i, ch := range subs {
		if skip[i] {
			continue
		}
		select {
		case data, ok := <-ch.Messages():
			if !ok {
				return fmt.Errorf("data channel closed")
			}
			if !bytes.Equal(mes, data) {
				return fmt.Errorf("wrong data on node %d. expected %q but got %q", i, string(mes), string(data))
			}
		case <-time.After(time.Second * 5):
			return fmt.Errorf("Timeout waiting for peer %d (%s)", i, ch.sub.h.ID())
		}
	}

	return nil
}

func TestBasicPubsub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 4
	hosts := makeNetHosts(t, ctx, count)

	err := connectUp(hosts)
	if err != nil {
		t.Fatal(err)
	}

	topic, _, subchs := initPubsub(t, ctx, hosts)
	defer topic.Close()
	defer closeSubs(subchs)

	for i := 0; i < 10; i++ {
		err = checkSystem(topic, subchs, nil, i)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// tests nodes dropping abruptly (without sending part messages)
func TestNodesDropping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 4
	hosts := makeNetHosts(t, ctx, count)

	err := connectUp(hosts)
	if err != nil {
		t.Fatal(err)
	}

	topic, _, subchs := initPubsub(t, ctx, hosts)
	defer closeSubs(subchs)

	err = checkSystem(topic, subchs, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = hosts[1].Close()
	if err != nil {
		t.Fatal(err)
	}

	err = checkSystem(topic, subchs, map[int]bool{
		0: true,
		2: true,
	}, 1)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)
	clearWaitingMessages(subchs)

	for i := 0; i < 10; i++ {
		err = checkSystem(topic, subchs, map[int]bool{
			0: true,
		}, i+100)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func printTree(t *Topic, subs []*client) {
	find := func(pid peer.ID) *subtree {
		for _, sub := range subs {
			if sub.sub.h.ID() == pid {
				return sub.sub
			}
		}
		panic("child sub not found")
	}
	var ps func(int, *subtree)
	ps = func(d int, s *subtree) {
		for i := 0; i < d; i++ {
			fmt.Print(" ")
		}

		fmt.Printf("%s\n", s.h.ID())
		s.chlock.Lock()
		for _, ch := range s.children {
			chs := find(ch.id)
			ps(d+1, chs)
		}
		s.chlock.Unlock()
	}

	ps(0, t.sub)
}

func TestLowerNodesDropping(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 8
	hosts := makeNetHosts(t, ctx, count)

	err := connectUp(hosts)
	if err != nil {
		t.Fatal(err)
	}

	topic, _, subchs := initPubsub(t, ctx, hosts)
	defer closeSubs(subchs)

	err = checkSystem(topic, subchs, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("killing host %s", hosts[2].ID())
	err = hosts[3].Close()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)

	err = checkSystem(topic, subchs, map[int]bool{
		2: true, // we killed this one
		5: true, // either 5 or 6 will get a dropped message
		6: true, // since dictionary iteration is randomized
	}, 1)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)
	clearWaitingMessages(subchs)

	for i := 0; i < 10; i++ {
		err = checkSystem(topic, subchs, map[int]bool{
			2: true,
		}, i+100)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestNodesDroppingGracefully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 4
	hosts := makeNetHosts(t, ctx, count)

	err := connectUp(hosts)
	if err != nil {
		t.Fatal(err)
	}

	topic, _, subchs := initPubsub(t, ctx, hosts)
	defer closeSubs(subchs)

	err = checkSystem(topic, subchs, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = subchs[0].Close()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 100)

	err = checkSystem(topic, subchs, map[int]bool{
		0: true,
	}, 1)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)
	clearWaitingMessages(subchs)

	for i := 0; i < 10; i++ {
		err = checkSystem(topic, subchs, map[int]bool{
			0: true,
		}, i+100)
		if err != nil {
			t.Fatal(err)
		}
	}
}
