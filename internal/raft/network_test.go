package raft

import "testing"

// =============================================================================
// The harness's own unit tests
// =============================================================================
//
// These call route directly rather than going through a Node, which is what
// makes them the first thing to break when the network's shape changes -- and
// the reason they exist: the fake network is the substrate every other test in
// this package stands on, so a fault here is invisible everywhere and fatal
// everywhere.
//
// route now returns a send-order stamp as its third value (C-15). Callers that
// only care about reachability discard it.

func TestNetworkConnectivity(t *testing.T) {
	c := newCluster(t, 3, 1)

	if _, _, _, ok := c.net.route(0, 1); !ok {
		t.Fatal("fresh cluster should be fully connected")
	}

	c.net.disconnect(0)
	if _, _, _, ok := c.net.route(0, 1); ok {
		t.Error("0 -> 1 should fail after disconnecting 0")
	}
	if _, _, _, ok := c.net.route(1, 0); ok {
		t.Error("1 -> 0 should fail after disconnecting 0 (both directions)")
	}
	if _, _, _, ok := c.net.route(1, 2); !ok {
		t.Error("1 -> 2 should be unaffected by disconnecting 0")
	}

	c.net.reconnect(0)
	if _, _, _, ok := c.net.route(0, 1); !ok {
		t.Error("0 -> 1 should work again after reconnect")
	}
}

func TestNetworkPartition(t *testing.T) {
	c := newCluster(t, 5, 1)

	// Majority {0,1,2} split from minority {3,4}.
	c.net.partition([]int{0, 1, 2}, []int{3, 4})

	if _, _, _, ok := c.net.route(0, 2); !ok {
		t.Error("within-majority link should survive")
	}
	if _, _, _, ok := c.net.route(3, 4); !ok {
		t.Error("within-minority link should survive")
	}
	if _, _, _, ok := c.net.route(2, 3); ok {
		t.Error("across-partition link should be cut")
	}

	c.net.heal()
	if _, _, _, ok := c.net.route(2, 3); !ok {
		t.Error("heal should restore all links")
	}
}

// The seeded rng is what makes a failing test reproducible from its seed alone,
// so the property is worth pinning: identical seeds must produce identical drop
// sequences, and different seeds must not.
//
// It also pins something subtler. Nothing may consume from fn.rng conditionally
// in a way that shifts the stream: the reply-drop roll added in C-15 fires only
// when replyDropRate is above zero, which is why this test still produces the
// same sequence it did before that knob existed.
func TestNetworkDeterminism(t *testing.T) {
	run := func(seed int64) []bool {
		c := newCluster(t, 2, seed)
		c.net.setDropRate(0.5)

		var results []bool
		for i := 0; i < 50; i++ {
			_, _, _, ok := c.net.route(0, 1)
			results = append(results, ok)
		}
		return results
	}

	a, b := run(42), run(42)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed diverged at call %d", i)
		}
	}

	diff := run(43)
	same := true
	for i := range a {
		if a[i] != diff[i] {
			same = false
			break
		}
	}
	if same {
		t.Error("different seeds produced identical drop sequences")
	}
}

// =============================================================================
// C-15 additions
// =============================================================================

// A reply drop must not be a request drop.
//
// The whole value of the knob is that the receiver ACTED and the sender does
// not know. If setReplyDropRate leaked into route, the follower would never
// have appended and the case that exercises mergeEntries against a duplicate
// append would be unreachable again.
func TestReplyDropLosesTheReplyNotTheRequest(t *testing.T) {
	c := newCluster(t, 2, 7)

	c.net.setReplyDropRate(1.0)

	if _, _, _, ok := c.net.route(0, 1); !ok {
		t.Fatal("the request was dropped by a reply-drop rate: the two rolls " +
			"have been wired together, and the follower never gets to act")
	}
	if c.net.replyDeliverable(1, 0) {
		t.Error("a reply-drop rate of 1.0 delivered a reply anyway")
	}
	if got := c.net.drops(); got != 1 {
		t.Errorf("drops = %d, want 1: a discarded reply is a discarded message", got)
	}

	c.net.setReplyDropRate(0)
	if !c.net.replyDeliverable(1, 0) {
		t.Error("a reply-drop rate of 0 discarded a reply")
	}

	// Reachability beats the roll, in that order: a cut return path is not a
	// probabilistic loss and must not consume one.
	c.net.disconnect(1)
	if c.net.replyDeliverable(1, 0) {
		t.Error("a disconnected return path delivered a reply")
	}
}

// Reordering is measured, not configured, so the measurement is what needs
// pinning: a stamp records send order, arrive records delivery order, and a
// count means the two disagreed.
func TestReorderCountIsSendOrderVersusArrivalOrder(t *testing.T) {
	c := newCluster(t, 2, 1)

	_, _, first, ok := c.net.route(0, 1)
	if !ok {
		t.Fatal("setup: a fresh cluster dropped a message")
	}
	_, _, second, ok := c.net.route(0, 1)
	if !ok {
		t.Fatal("setup: a fresh cluster dropped a message")
	}
	if second <= first {
		t.Fatalf("stamps must rise with send order: got %d then %d", first, second)
	}

	// Delivered in the order they were sent.
	c.net.arrive(0, 1, first)
	c.net.arrive(0, 1, second)
	if got := c.net.reorderedCount(); got != 0 {
		t.Errorf("reordered = %d after two in-order deliveries, want 0", got)
	}

	// Delivered backwards.
	_, _, third, _ := c.net.route(0, 1)
	_, _, fourth, _ := c.net.route(0, 1)
	c.net.arrive(0, 1, fourth)
	c.net.arrive(0, 1, third)
	if got := c.net.reorderedCount(); got != 1 {
		t.Errorf("reordered = %d after one inversion, want 1", got)
	}

	// Stamps are per DIRECTED pair. Sharing one counter across the cluster
	// would make every first message on a new link look like an inversion,
	// and the count would measure fan-out rather than reordering.
	_, _, otherWay, _ := c.net.route(1, 0)
	c.net.arrive(1, 0, otherWay)
	if got := c.net.reorderedCount(); got != 1 {
		t.Errorf("reordered = %d after a first delivery on a different pair, "+
			"want 1: the stamps are not per directed pair", got)
	}
}

// A dropped message consumes a stamp it never delivers, leaving a permanent
// hole in the sequence. The hole must be inert -- it can neither invent an
// inversion nor hide one, because it simply never arrives.
func TestADroppedStampNeverCounts(t *testing.T) {
	c := newCluster(t, 2, 11)

	_, _, kept, ok := c.net.route(0, 1)
	if !ok {
		t.Fatal("setup: message dropped before the drop rate was set")
	}

	// This one is discarded on the way out and its stamp is never delivered.
	c.net.setDropRate(1.0)
	if _, _, _, ok := c.net.route(0, 1); ok {
		t.Fatal("setup: a drop rate of 1.0 delivered a message")
	}
	c.net.setDropRate(0)

	_, _, after, ok := c.net.route(0, 1)
	if !ok {
		t.Fatal("setup: message dropped after the drop rate was cleared")
	}

	c.net.arrive(0, 1, kept)
	c.net.arrive(0, 1, after)

	if got := c.net.reorderedCount(); got != 0 {
		t.Errorf("reordered = %d, want 0: the gap left by a dropped stamp was "+
			"counted as an inversion", got)
	}
}
