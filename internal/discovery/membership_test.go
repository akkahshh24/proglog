package discovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

// Sets up a cluster with multiple servers and
// checks membership after few changes.
func TestMembership(t *testing.T) {
	// initialize cluster with members
	m, handler := setupMember(t, nil)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)
	require.Eventually(t, func() bool {
		// ! DON'T USE YODA CONDITIONS
		// return 2 == len(handler.joins) &&
		// 	3 == len(m[0].Members()) &&
		// 	0 == len(handler.leaves)
		return len(handler.joins) == 2 && // 2 new members joined
			len(m[0].Members()) == 3 && // total 3 members
			len(handler.leaves) == 0 // 0 members have left
	}, 3*time.Second, 250*time.Millisecond)

	// Make the third member to leave the cluster
	require.NoError(t, m[2].Leave())

	require.Eventually(t, func() bool {
		return len(handler.joins) == 2 &&
			len(m[0].Members()) == 3 &&
			m[0].Members()[2].Status == serf.StatusLeft &&
			len(handler.leaves) == 1
	}, 3*time.Second, 250*time.Millisecond)

	require.Equal(t, fmt.Sprintf("%d", 2), <-handler.leaves)
}

// Sets up a new member under a free port
func setupMember(t *testing.T, members []*Membership) (
	[]*Membership, *handler,
) {
	id := len(members)
	// get a free port
	ports := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{
		"rpc_addr": addr,
	}

	config := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}

	handler := &handler{}
	// check if this member is the cluster's initial member
	// if so initialize joins and leaves channel
	if len(members) == 0 {
		handler.joins = make(chan map[string]string, 3)
		handler.leaves = make(chan string, 3)
	} else {
		config.StartJoinAddrs = []string{
			members[0].BindAddr,
		}
	}

	m, err := New(handler, config)
	require.NoError(t, err)
	members = append(members, m)
	return members, handler
}

// Mock handler
// Implements Handler interface
type handler struct {
	joins  chan map[string]string
	leaves chan string
}

// Puts the new member's id and addr in the joins channel
func (h *handler) Join(id, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}
	return nil
}

// Puts the member's id in the leaves channel
func (h *handler) Leave(id string) error {
	if h.leaves != nil {
		h.leaves <- id
	}
	return nil
}
