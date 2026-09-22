package repository

import (
	"context"
	"net"
	"strings"
	"time"
)

// TCPBrokerPinger probes message broker TCP reachability.
type TCPBrokerPinger struct {
	addr string
}

// NewTCPBrokerPinger creates a new TCPBrokerPinger from a comma-separated broker list.
func NewTCPBrokerPinger(brokers string) *TCPBrokerPinger {
	parts := strings.Split(brokers, ",")
	addr := strings.TrimSpace(parts[0])
	return &TCPBrokerPinger{addr: addr}
}

// PingBroker attempts a fast TCP connection to verify broker reachability.
func (p *TCPBrokerPinger) PingBroker(ctx context.Context) error {
	if p.addr == "" {
		return nil
	}

	d := net.Dialer{Timeout: 2 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}
