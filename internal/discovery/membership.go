package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// It wraps Serf to provide discovery and cluster membership.
type membership struct {
	config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

func New(handler Handler, config config) (*membership, error) {
	m := &membership{
		config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}

	if err := m.setupSerf(); err != nil {
		return nil, err
	}
	return m, nil
}

type config struct {
	nodeName       string            // node's unique identifier across the cluster
	bindAddr       string            // listens on this address
	tags           map[string]string // ? purpose?
	startJoinAddrs []string          // atleast 3 existing node addresses for new nodes to connect to and join the cluster
}

// Creates and configures a Serf instance and starts a goroutine
// to handle Serf's events
func (m *membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.bindAddr)
	if err != nil {
		return err
	}

	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events // receive events when a member joins or leaves the cluster
	config.Tags = m.tags
	config.NodeName = m.nodeName

	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()
	if m.startJoinAddrs != nil {
		_, err = m.serf.Join(m.startJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

type Handler interface {
	join(name, addr string) error
	leave(name string) error
}

// It handles each incoming event according to their type.
func (m *membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				// check if the node is itself, if so don't handle
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}

		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				// check if the node is itself, if so don't handle
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

func (m *membership) handleJoin(member serf.Member) {
	if err := m.handler.join(
		member.Name,
		member.Tags["rpc_addr"],
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *membership) handleLeave(member serf.Member) {
	if err := m.handler.leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// Returns whether the given Serf member is the local member.
func (m *membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Returns the cluster's current members.
func (m *membership) Members() []serf.Member {
	return m.serf.Members()
}

// The specified member leaves the Serf cluster.
func (m *membership) Leave() error {
	return m.serf.Leave()
}

// Logs the given error and message.
func (m *membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
