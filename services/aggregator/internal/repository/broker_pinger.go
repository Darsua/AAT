package repository

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"
)

// TCPBrokerPinger probes message broker TCP reachability across configured broker addresses.
type TCPBrokerPinger struct {
	addrs []string
}

// NewTCPBrokerPinger creates a new TCPBrokerPinger from a comma-separated broker list.
func NewTCPBrokerPinger(brokers string) *TCPBrokerPinger {
	var addrs []string
	for _, part := range strings.Split(brokers, ",") {
		if addr := strings.TrimSpace(part); addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return &TCPBrokerPinger{addrs: addrs}
}

// PingBroker verifies TCP reachability against at least one configured broker address.
func (p *TCPBrokerPinger) PingBroker(ctx context.Context) error {
	if len(p.addrs) == 0 {
		return errors.New("no broker addresses configured")
	}

	d := net.Dialer{Timeout: 2 * time.Second}
	var lastErr error
	for _, addr := range p.addrs {
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
	}
	return lastErr
}
