package kafkaconn

// proxy_test.go：runtime SOCKS5 代理路由（DBX 宿主注入 runtime.proxy）的传
// 输层测试。核心是内嵌迷你 SOCKS5 服务器（RFC 1928 握手 + 可选 RFC 1929
// 用户名/密码认证）做端到端实拨：明文回环、认证、TLS 包装与 SNI 推导、
// kgo 选项互斥（Dialer 与 DialTLSConfig 并存校验）、种子优先级、指纹敏感
// 性与 lifecycle 解析。全部走 127.0.0.1 回环，不触外网。

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	xproxy "golang.org/x/net/proxy"

	"io.dbx.kafka.plugin/internal/lifecycle"
)

// --- 迷你 SOCKS5 服务器（测试专用 double） -------------------------------------

type socks5TestServer struct {
	listener net.Listener
	// auth 非 nil → 要求用户名/密码认证（RFC 1929）。
	auth *xproxy.Auth
	// forward 非 "" → 无视客户端 CONNECT 地址，固定转发到此目标（SNI 用例：
	// 客户端拨逻辑 broker 域名，服务器只回环可达 TLS 监听）。
	forward string

	mu          sync.Mutex
	connectAddr string // 最近一次 CONNECT 的目标地址（含域名形态）
	authSeen    bool
	authFailed  bool
}

func startSocks5Server(t *testing.T, auth *xproxy.Auth, forward string) *socks5TestServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen socks5: %v", err)
	}
	server := &socks5TestServer{listener: listener, auth: auth, forward: forward}
	go server.serve()
	t.Cleanup(func() { _ = listener.Close() })
	return server
}

func (s *socks5TestServer) addr() string { return s.listener.Addr().String() }

func (s *socks5TestServer) recordedConnectAddr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connectAddr
}

func (s *socks5TestServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

// handle 完成一次 SOCKS5 会话：方法协商 →（可选）认证 → CONNECT → 转发。
// 服务器缺陷一律直接断开（测试 double，无需错误通道）。
func (s *socks5TestServer) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(conn, greeting); err != nil || greeting[0] != 0x05 {
		return
	}
	methods := make([]byte, greeting[1])
	if _, err := io.ReadFull(conn, methods); err != nil {
		return
	}
	const (
		methodNoAuth       = 0x00
		methodUserPass     = 0x02
		methodUnacceptable = 0xFF
	)
	wantAuth := s.auth != nil
	chosen := byte(methodUnacceptable)
	for _, method := range methods {
		if wantAuth && method == methodUserPass {
			chosen = methodUserPass
			break
		}
		if !wantAuth && method == methodNoAuth {
			chosen = methodNoAuth
			break
		}
	}
	if _, err := conn.Write([]byte{0x05, chosen}); err != nil {
		return
	}
	if chosen == methodUnacceptable {
		return
	}
	if wantAuth {
		if !s.handleUserPass(conn) {
			return
		}
	}

	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil || head[1] != 0x01 { // 仅 CONNECT
		return
	}
	var host string
	switch head[3] {
	case 0x01: // IPv4
		ip := make([]byte, 4)
		if _, err := io.ReadFull(conn, ip); err != nil {
			return
		}
		host = net.IP(ip).String()
	case 0x03: // 域名
		length := make([]byte, 1)
		if _, err := io.ReadFull(conn, length); err != nil {
			return
		}
		name := make([]byte, length[0])
		if _, err := io.ReadFull(conn, name); err != nil {
			return
		}
		host = string(name)
	case 0x04: // IPv6
		ip := make([]byte, 16)
		if _, err := io.ReadFull(conn, ip); err != nil {
			return
		}
		host = net.IP(ip).String()
	default:
		return
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBytes); err != nil {
		return
	}
	addr := net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(portBytes))))
	s.mu.Lock()
	s.connectAddr = addr
	s.mu.Unlock()

	target := addr
	if s.forward != "" {
		target = s.forward
	}
	upstream, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		_, _ = conn.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0}) // general failure
		return
	}
	defer func() { _ = upstream.Close() }()
	if _, err := conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0}); err != nil {
		return
	}
	done := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(upstream, conn)
		done <- struct{}{}
	}()
	_, _ = io.Copy(conn, upstream)
	<-done
}

func (s *socks5TestServer) handleUserPass(conn net.Conn) bool {
	head := make([]byte, 2) // VER ULEN
	if _, err := io.ReadFull(conn, head); err != nil {
		return false
	}
	user := make([]byte, head[1])
	if _, err := io.ReadFull(conn, user); err != nil {
		return false
	}
	length := make([]byte, 1)
	if _, err := io.ReadFull(conn, length); err != nil {
		return false
	}
	password := make([]byte, length[0])
	if _, err := io.ReadFull(conn, password); err != nil {
		return false
	}
	s.mu.Lock()
	s.authSeen = true
	ok := string(user) == s.auth.User && string(password) == s.auth.Password
	s.authFailed = !ok
	s.mu.Unlock()
	if _, err := conn.Write([]byte{0x01, map[bool]byte{true: 0x00, false: 0x01}[ok]}); err != nil {
		return false
	}
	return ok
}

// --- TLS 测试栈（CA + 带 SAN 的叶子证书） ---------------------------------------

type proxyTLSStack struct {
	caPEM     string
	leaf      tls.Certificate
	serverTLS *tls.Config
	clientTLS *tls.Config // ServerName 留空：SNI 由代理拨号器按目标地址推导
}

func newProxyTLSStack(t *testing.T, sniHost string) *proxyTLSStack {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "dbx-proxy-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: sniHost},
		DNSNames:     []string{sniHost},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	leaf, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: mustMarshalECKey(t, leafKey)}),
	)
	if err != nil {
		t.Fatalf("leaf keypair: %v", err)
	}
	caPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}))
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(caPEM))
	return &proxyTLSStack{
		caPEM:     caPEM,
		leaf:      leaf,
		serverTLS: &tls.Config{Certificates: []tls.Certificate{leaf}, MinVersion: tls.VersionTLS12},
		clientTLS: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
	}
}

func mustMarshalECKey(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return der
}

// startTLSListener 起一个 TLS 服务，记录握手后的 SNI 并做一次回环读写。
func startTLSListener(t *testing.T, stack *proxyTLSStack) (addr string, serverName func() string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tls: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	var (
		mu   sync.Mutex
		snis string
	)
	go func() {
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			go func(raw net.Conn) {
				defer func() { _ = raw.Close() }()
				tlsConn := tls.Server(raw, stack.serverTLS)
				if err := tlsConn.HandshakeContext(context.Background()); err != nil {
					return
				}
				mu.Lock()
				snis = tlsConn.ConnectionState().ServerName
				mu.Unlock()
				_, _ = io.Copy(tlsConn, tlsConn)
			}(raw)
		}
	}()
	return listener.Addr().String(), func() string {
		mu.Lock()
		defer mu.Unlock()
		return snis
	}
}

// --- 拨号器校验矩阵 --------------------------------------------------------------

func TestRuntimeProxyDialerValidationMatrix(t *testing.T) {
	ok := func(proxy *lifecycle.RuntimeProxy) bool {
		entry := &connEntry{target: connTarget{Proxy: proxy}}
		dial, err := entry.runtimeProxyDialer(nil)
		return err == nil && dial != nil
	}
	if ok(&lifecycle.RuntimeProxy{Type: "socks5", Host: "127.0.0.1", Port: 1080}) != true {
		t.Error("socks5 type should build a dialer")
	}
	if ok(&lifecycle.RuntimeProxy{Type: " SOCKS5H ", Host: "127.0.0.1", Port: 1080}) != true {
		t.Error("socks5h (case/space normalized) should build a dialer")
	}
	// Type 缺省 → Kind 兜底。
	if ok(&lifecycle.RuntimeProxy{Kind: "socks5", Host: "127.0.0.1", Port: 1080}) != true {
		t.Error("kind fallback should build a dialer")
	}
	bad := []struct {
		name  string
		proxy *lifecycle.RuntimeProxy
		want  string
	}{
		{"nil proxy", nil, "runtime proxy is not configured"},
		{"http type", &lifecycle.RuntimeProxy{Type: "http", Host: "p", Port: 8080}, "only socks5"},
		{"missing host", &lifecycle.RuntimeProxy{Type: "socks5", Port: 1080}, "host and port are required"},
		{"port zero", &lifecycle.RuntimeProxy{Type: "socks5", Host: "p"}, "host and port are required"},
		{"port overflow", &lifecycle.RuntimeProxy{Type: "socks5", Host: "p", Port: 65536}, "host and port are required"},
	}
	for _, tc := range bad {
		entry := &connEntry{target: connTarget{Proxy: tc.proxy}}
		_, err := entry.runtimeProxyDialer(nil)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want containing %q", tc.name, err, tc.want)
		}
	}
}

// --- 端到端实拨（回环 SOCKS5） ---------------------------------------------------

func dialVia(t *testing.T, entry *connEntry, address string, tlsConfig *tls.Config) net.Conn {
	t.Helper()
	dialer, err := entry.runtimeProxyDialer(tlsConfig)
	if err != nil {
		t.Fatalf("runtimeProxyDialer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := dialer(ctx, "tcp", address)
	if err != nil {
		t.Fatalf("dial via proxy: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func roundTrip(t *testing.T, conn net.Conn) {
	t.Helper()
	payload := []byte("ping-proxy")
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("roundtrip = %q, want %q", got, payload)
	}
}

func startEcho(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen echo: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()
				_, _ = io.Copy(conn, conn)
			}(conn)
		}
	}()
	return listener.Addr().String()
}

func TestRuntimeProxyDialerPlainEndToEnd(t *testing.T) {
	echoAddr := startEcho(t)
	server := startSocks5Server(t, nil, "")

	entry := &connEntry{target: connTarget{Proxy: &lifecycle.RuntimeProxy{Type: "socks5", Host: "127.0.0.1", Port: mustPort(t, server.addr())}}}
	conn := dialVia(t, entry, echoAddr, nil)
	roundTrip(t, conn)
	if got := server.recordedConnectAddr(); got != echoAddr {
		t.Fatalf("proxy saw CONNECT %q, want %q", got, echoAddr)
	}
}

func TestRuntimeProxyDialerAuthEndToEnd(t *testing.T) {
	echoAddr := startEcho(t)
	auth := &xproxy.Auth{User: "alice", Password: "secret"}
	server := startSocks5Server(t, auth, "")
	proxyPort := mustPort(t, server.addr())

	entry := &connEntry{target: connTarget{Proxy: &lifecycle.RuntimeProxy{Type: "socks5", Host: "127.0.0.1", Port: proxyPort, Username: "alice", Password: "secret"}}}
	roundTrip(t, dialVia(t, entry, echoAddr, nil))
	if !server.authSeen {
		t.Fatal("server never received user/pass auth")
	}
	if server.authFailed {
		t.Fatal("server rejected valid credentials")
	}

	wrong := &connEntry{target: connTarget{Proxy: &lifecycle.RuntimeProxy{Type: "socks5", Host: "127.0.0.1", Port: proxyPort, Username: "alice", Password: "nope"}}}
	dialer, err := wrong.runtimeProxyDialer(nil)
	if err != nil {
		t.Fatalf("dialer: %v", err)
	}
	conn, err := dialer(context.Background(), "tcp", echoAddr)
	if err == nil {
		_ = conn.Close()
		t.Fatal("dial with wrong password should fail")
	}
	if !server.authFailed {
		t.Fatal("server should have recorded the rejected auth attempt")
	}
}

func TestRuntimeProxyDialerTLSWrapsWithDerivedSNI(t *testing.T) {
	const logicalBroker = "db-broker.internal:9092"
	const sniHost = "db-broker.internal"
	stack := newProxyTLSStack(t, sniHost)
	tlsAddr, serverName := startTLSListener(t, stack)
	// forward：代理只回环可达 TLS 监听，客户端拨的是逻辑 broker 域名。
	server := startSocks5Server(t, nil, tlsAddr)

	entry := &connEntry{target: connTarget{Proxy: &lifecycle.RuntimeProxy{Type: "socks5", Host: "127.0.0.1", Port: mustPort(t, server.addr())}}}
	conn := dialVia(t, entry, logicalBroker, stack.clientTLS)
	if got := server.recordedConnectAddr(); got != logicalBroker {
		t.Fatalf("proxy saw CONNECT %q, want hostname form %q (not pre-resolved)", got, logicalBroker)
	}
	roundTrip(t, conn) // TLS 包装后仍可用
	if got := serverName(); got != sniHost {
		t.Fatalf("SNI = %q, want %q (derived from dialed address)", got, sniHost)
	}
}

func mustPort(t *testing.T, addr string) int {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split %q: %v", addr, err)
	}
	value, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("port %q: %v", port, err)
	}
	return value
}

// --- kgo 选项组装（代理路由） ----------------------------------------------------

func TestBuildClientOptsProxyRouteDialerExclusivity(t *testing.T) {
	caPEM := testCAPEM(t)
	entry := &connEntry{
		profile: Profile{
			SecurityProtocol: SecurityProtocolSSL,
			TLSCACert:        caPEM,
			BootstrapServers: []string{"db-broker.internal:9092"},
		},
		target: connTarget{Proxy: &lifecycle.RuntimeProxy{Type: "socks5", Host: "127.0.0.1", Port: 1080}},
	}
	// 回归护栏：代理路由的 Dialer 与 DialTLSConfig 不得并存（kgo config
	// 校验直接拒绝），TLS 包装由代理拨号器自行完成。
	opts, tlsConfig, err := entry.buildClientOptsWithSeeds([]string{"db-broker.internal:9092"}, false)
	if err != nil {
		t.Fatalf("buildClientOptsWithSeeds: %v", err)
	}
	if tlsConfig == nil {
		t.Fatal("tlsConfig = nil, want TLS enabled config")
	}
	// ServerName 留空：SNI 由代理拨号器按每个 advertised broker 地址推导。
	if tlsConfig.ServerName != "" {
		t.Fatalf("tlsConfig.ServerName = %q, want empty for proxy route", tlsConfig.ServerName)
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("kgo.NewClient with proxy dialer: %v (Dialer/DialTLSConfig conflict?)", err)
	}
	client.Close()

	// 探针路径（connection/test）同样必须通过校验。
	probeOpts, _, err := entry.buildClientOptsWithSeeds([]string{"db-broker.internal:9092"}, true)
	if err != nil {
		t.Fatalf("probe build: %v", err)
	}
	probeClient, err := kgo.NewClient(probeOpts...)
	if err != nil {
		t.Fatalf("kgo.NewClient probe path: %v", err)
	}
	probeClient.Close()

	// 对照：直连 + runtime host:port → ServerName 取逻辑 broker 名。
	direct := &connEntry{
		profile: entry.profile,
		target:  connTarget{Host: "127.0.0.1", Port: 19092},
	}
	_, directTLS, err := direct.buildClientOptsWithSeeds([]string{"127.0.0.1:19092"}, false)
	if err != nil {
		t.Fatalf("direct build: %v", err)
	}
	if directTLS.ServerName != "db-broker.internal" {
		t.Fatalf("direct ServerName = %q, want db-broker.internal (logical bootstrap host)", directTLS.ServerName)
	}
}

// --- 种子优先级与指纹 ------------------------------------------------------------

func TestSeedBrokersProxyRouteFallbacks(t *testing.T) {
	// 代理 + bootstrap：保留逻辑 broker 清单（advertised brokers 逐台走代理）。
	withBootstrap := &connEntry{
		profile: Profile{BootstrapServers: []string{"k1:9092", "k2:9092"}},
		target:  connTarget{Host: "127.0.0.1", Port: 19092, Proxy: &lifecycle.RuntimeProxy{Type: "socks5", Host: "p", Port: 1080}},
	}
	if got := withBootstrap.seedBrokers(); len(got) != 2 {
		t.Fatalf("proxy+bootstrap seeds = %v, want logical list", got)
	}
	// 代理 + 无 bootstrap：回落 runtime host:port。
	noBootstrap := &connEntry{
		target: connTarget{Host: "127.0.0.1", Port: 19092, Proxy: &lifecycle.RuntimeProxy{Type: "socks5", Host: "p", Port: 1080}},
	}
	if got := noBootstrap.seedBrokers(); len(got) != 1 || got[0] != "127.0.0.1:19092" {
		t.Fatalf("proxy fallback seeds = %v, want runtime endpoint", got)
	}
	// 直连时 runtime host:port 优先于 bootstrap（Host 管理端点语义）。
	runtimePreferred := &connEntry{
		profile: Profile{BootstrapServers: []string{"k1:9092"}},
		target:  connTarget{Host: "127.0.0.1", Port: 19092},
	}
	if got := runtimePreferred.seedBrokers(); len(got) != 1 || got[0] != "127.0.0.1:19092" {
		t.Fatalf("runtime-preferred seeds = %v, want [127.0.0.1:19092]", got)
	}
}

func TestComputeFingerprintIncludesProxy(t *testing.T) {
	base := Profile{BootstrapServers: []string{"k1:9092"}}
	plain := &connEntry{profile: base}
	proxied := &connEntry{profile: base, target: connTarget{Proxy: &lifecycle.RuntimeProxy{Type: "socks5", Host: "p", Port: 1080}}}
	rotated := &connEntry{profile: base, target: connTarget{Proxy: &lifecycle.RuntimeProxy{Type: "socks5", Host: "p", Port: 1080, Username: "u", Password: "new"}}}

	if plain.computeFingerprint() == proxied.computeFingerprint() {
		t.Fatal("fingerprint ignores proxy route")
	}
	if proxied.computeFingerprint() == rotated.computeFingerprint() {
		t.Fatal("fingerprint ignores proxy credential rotation")
	}
}

func TestLifecycleParseProxyRoundtrip(t *testing.T) {
	parsed, err := lifecycle.Parse([]byte(`{
		"connection": {"id": "c1"},
		"runtime": {
			"host": "broker.internal", "port": 9092,
			"proxy": {"type": "socks5h", "host": "127.0.0.1", "port": 1080, "username": "u", "password": "pw"}
		}
	}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	proxy := parsed.Runtime.Proxy
	if proxy == nil {
		t.Fatal("runtime.proxy missing after parse")
	}
	if proxy.Type != "socks5h" || proxy.Host != "127.0.0.1" || proxy.Port != 1080 || proxy.Username != "u" || proxy.Password != "pw" {
		t.Fatalf("proxy = %+v", proxy)
	}

	plain, err := lifecycle.Parse([]byte(`{"connection":{"id":"c2"},"runtime":{"host":"h","port":9092}}`))
	if err != nil {
		t.Fatalf("parse plain: %v", err)
	}
	if plain.Runtime.Proxy != nil {
		t.Fatalf("no-proxy request parsed proxy = %+v", plain.Runtime.Proxy)
	}
}
