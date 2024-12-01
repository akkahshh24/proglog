package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// * Exported: used by agent
// It wraps Serf to provide discovery and cluster membership.
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

func New(handler Handler, config Config) (*Membership, error) {
	m := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}

	if err := m.setupSerf(); err != nil {
		return nil, err
	}
	return m, nil
}

// * Exported: used by agent
type Config struct {
	NodeName       string            // node's unique identifier across the cluster
	BindAddr       string            // listens on this address
	Tags           map[string]string // ? purpose?
	StartJoinAddrs []string          // atleast 3 existing node addresses for new nodes to connect to and join the cluster
}

// Creates and configures a Serf instance and starts a goroutine
// to handle Serf's events
func (m *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}

	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events // receive events when a member joins or leaves the cluster
	config.Tags = m.Tags
	config.NodeName = m.NodeName

	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

// It handles each incoming event according to their type.
func (m *Membership) eventHandler() {
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

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(
		member.Name,
		member.Tags["rpc_addr"],
	); err != nil {
		m.logError(err, "failed to join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(
		member.Name,
	); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// Returns whether the given Serf member is the local member.
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Returns the cluster's current members.
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// The specified member leaves the Serf cluster.
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// Logs the given error and message.
func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
