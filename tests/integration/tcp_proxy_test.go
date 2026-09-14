package integration

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TCPChaosProxy is an in-process, zero-dependency TCP proxy designed for chaos testing.
// It proxies traffic between client applications (TaskMQ) and upstream Redis,
// allowing dynamic simulation of hard RST cuts, packet blackholes, and latency injection.
type TCPChaosProxy struct {
	t        *testing.T
	listener net.Listener
	upstream string
	addr     string

	mu        sync.Mutex
	conns     map[net.Conn]net.Conn
	closed    bool
	blackhole atomic.Bool
	delayMs   atomic.Int64
	wg        sync.WaitGroup
}

// StartTCPChaosProxy starts a dynamic TCP proxy on localhost that forwards to upstreamAddr.
func StartTCPChaosProxy(t *testing.T, upstreamAddr string) *TCPChaosProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "failed to bind TCP chaos proxy listener")

	proxy := &TCPChaosProxy{
		t:        t,
		listener: ln,
		upstream: upstreamAddr,
		addr:     ln.Addr().String(),
		conns:    make(map[net.Conn]net.Conn),
	}

	proxy.wg.Add(1)
	go proxy.acceptLoop()

	t.Cleanup(func() {
		proxy.Close()
	})

	return proxy
}

// Addr returns the proxy's listening address (e.g. 127.0.0.1:54321).
func (p *TCPChaosProxy) Addr() string {
	return p.addr
}

// Cut forces an immediate physical disconnect (RST) on all currently active TCP connections.
func (p *TCPChaosProxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for clientConn, upstreamConn := range p.conns {
		if tc, ok := clientConn.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		if tu, ok := upstreamConn.(*net.TCPConn); ok {
			_ = tu.SetLinger(0)
		}
		_ = clientConn.Close()
		_ = upstreamConn.Close()
		delete(p.conns, clientConn)
	}
}

// SetBlackhole enables or disables silent packet drops/freezes (simulating socket read/write timeouts).
func (p *TCPChaosProxy) SetBlackhole(enable bool) {
	p.blackhole.Store(enable)
}

// SetDelay injects artificial latency into packet forwarding.
func (p *TCPChaosProxy) SetDelay(d time.Duration) {
	p.delayMs.Store(d.Milliseconds())
}

// UpstreamAddrs returns the local addresses (as seen by upstream Redis) of all active upstream connections.
func (p *TCPChaosProxy) UpstreamAddrs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	addrs := make([]string, 0, len(p.conns))
	for _, upstreamConn := range p.conns {
		if upstreamConn != nil && upstreamConn.LocalAddr() != nil {
			addrs = append(addrs, upstreamConn.LocalAddr().String())
		}
	}
	return addrs
}

// Close shuts down the proxy listener and terminates all proxied connections.
func (p *TCPChaosProxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	_ = p.listener.Close()
	p.mu.Unlock()

	p.Cut()
	p.wg.Wait()
}

func (p *TCPChaosProxy) acceptLoop() {
	defer p.wg.Done()

	for {
		clientConn, err := p.listener.Accept()
		if err != nil {
			return
		}

		p.mu.Lock()
		if p.closed {
			_ = clientConn.Close()
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()

		p.wg.Add(1)
		go p.handleConnection(clientConn)
	}
}

func (p *TCPChaosProxy) handleConnection(clientConn net.Conn) {
	defer p.wg.Done()

	upstreamConn, err := net.DialTimeout("tcp", p.upstream, 2*time.Second)
	if err != nil {
		_ = clientConn.Close()
		return
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = clientConn.Close()
		_ = upstreamConn.Close()
		return
	}
	p.conns[clientConn] = upstreamConn
	p.mu.Unlock()

	var pairWg sync.WaitGroup
	pairWg.Add(2)

	go func() {
		defer pairWg.Done()
		p.pipe(clientConn, upstreamConn)
	}()

	go func() {
		defer pairWg.Done()
		p.pipe(upstreamConn, clientConn)
	}()

	pairWg.Wait()

	p.mu.Lock()
	delete(p.conns, clientConn)
	p.mu.Unlock()

	_ = clientConn.Close()
	_ = upstreamConn.Close()
}

func (p *TCPChaosProxy) pipe(src, dst net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		if p.blackhole.Load() {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		if d := p.delayMs.Load(); d > 0 {
			time.Sleep(time.Duration(d) * time.Millisecond)
		}

		_ = src.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, err := src.Read(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Continue to check if proxy is closed or blackhole toggled
				p.mu.Lock()
				closed := p.closed
				p.mu.Unlock()
				if closed {
					return
				}
				continue
			}
			return
		}

		if p.blackhole.Load() {
			// Dropping / not forwarding
			time.Sleep(50 * time.Millisecond)
			continue
		}

		_ = dst.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err = dst.Write(buf[:n])
		if err != nil {
			return
		}
	}
}

func TestTCPChaosProxy_Smoke(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	ctx, cancel, _ := RequireRedis(t)
	defer cancel()

	proxy := StartTCPChaosProxy(t, redisTestAddr())
	defer proxy.Close()

	// Connect redis client through proxy
	opts := &redis.Options{
		Addr:        proxy.Addr(),
		DialTimeout: 1 * time.Second,
		ReadTimeout: 1 * time.Second,
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()

	// 1. Initial Ping succeeds
	res, err := rdb.Ping(ctx).Result()
	require.NoError(t, err)
	require.Equal(t, "PONG", res)

	// 2. Cut connections
	proxy.Cut()

	// Next command triggers reconnect transparently
	res, err = rdb.Ping(ctx).Result()
	require.NoError(t, err)
	require.Equal(t, "PONG", res)

	// 3. Blackhole causes timeout
	proxy.SetBlackhole(true)
	bctx, bcancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer bcancel()
	err = rdb.Ping(bctx).Err()
	require.Error(t, err)

	// 4. Resume restores traffic
	proxy.SetBlackhole(false)
	require.Eventually(t, func() bool {
		return rdb.Ping(ctx).Err() == nil
	}, 3*time.Second, 100*time.Millisecond)
}

