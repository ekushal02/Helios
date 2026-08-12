package raft

import "testing"

func TestNetworkConnectivity(t *testing.T) {
	c := newCluster(t, 3, 1)

	if _, _, ok := c.net.route(0, 1); !ok {
		t.Fatal("fresh cluster should be fully connected")
	}

	c.net.disconnect(0)
	if _, _, ok := c.net.route(0, 1); ok {
		t.Error("0 -> 1 should fail after disconnecting 0")
	}
	if _, _, ok := c.net.route(1, 0); ok {
		t.Error("1 -> 0 should fail after disconnecting 0 (both directions)")
	}
	if _, _, ok := c.net.route(1, 2); !ok {
		t.Error("1 -> 2 should be unaffected by disconnecting 0")
	}

	c.net.reconnect(0)
	if _, _, ok := c.net.route(0, 1); !ok {
		t.Error("0 -> 1 should work again after reconnect")
	}
}

func TestNetworkPartition(t *testing.T) {
	c := newCluster(t, 5, 1)

	// Majority {0,1,2} split from minority {3,4}.
	c.net.partition([]int{0, 1, 2}, []int{3, 4})

	if _, _, ok := c.net.route(0, 2); !ok {
		t.Error("within-majority link should survive")
	}
	if _, _, ok := c.net.route(3, 4); !ok {
		t.Error("within-minority link should survive")
	}
	if _, _, ok := c.net.route(2, 3); ok {
		t.Error("across-partition link should be cut")
	}

	c.net.heal()
	if _, _, ok := c.net.route(2, 3); !ok {
		t.Error("heal should restore all links")
	}
}

func TestNetworkDeterminism(t *testing.T) {
	run := func(seed int64) []bool {
		c := newCluster(t, 2, seed)
		c.net.setDropRate(0.5)

		var results []bool
		for i := 0; i < 50; i++ {
			_, _, ok := c.net.route(0, 1)
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
