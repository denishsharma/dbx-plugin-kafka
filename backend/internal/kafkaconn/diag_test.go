package kafkaconn

// diag_test.go：connection/test 诊断（issue #28）——探针拨号、摘要行格式、
// seed 来源归类、scheme 前缀剥离，以及端到端失败路径的脱敏断言（凭据与
// TLS 私钥绝不进诊断输出与返回错误）。127.0.0.1:1 恒为 connection refused，
// 不触网、毫秒级失败。

import (
	"bytes"
	"context"
	"crypto/tls"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"io.dbx.kafka.plugin/internal/lifecycle"
)

func TestSeedSourceLabel(t *testing.T) {
	if got := seedSourceLabel(&connEntry{profile: Profile{BootstrapServers: []string{"b:9092"}}}); got != "bootstrap" {
		t.Errorf("bootstrap label = %q", got)
	}
	if got := seedSourceLabel(&connEntry{target: connTarget{Host: "127.0.0.1", Port: 9094}}); got != "runtime" {
		t.Errorf("runtime label = %q", got)
	}
	if got := seedSourceLabel(&connEntry{profile: Profile{ConnectionSource: ConnectionSourceZookeeper}}); got != "zookeeper" {
		t.Errorf("zookeeper label = %q", got)
	}
	if got := seedSourceLabel(&connEntry{target: connTarget{Proxy: &lifecycle.RuntimeProxy{Type: "socks5", Host: "p", Port: 1080}}}); got != "proxy-route" {
		t.Errorf("proxy label = %q", got)
	}
}

func TestSecuritySummary(t *testing.T) {
	if got := securitySummary(Profile{SecurityProtocol: SecurityProtocolPlaintext}); got != "PLAINTEXT" {
		t.Errorf("PLAINTEXT summary = %q", got)
	}
	got := securitySummary(Profile{
		SecurityProtocol: SecurityProtocolSASLSSL,
		SASLMechanism:    SASLMechanismSCRAMSHA512,
	})
	if got != "SASL_SSL/SCRAM-SHA-512 tls=verify" {
		t.Errorf("SASL_SSL summary = %q", got)
	}
	got = securitySummary(Profile{
		SecurityProtocol:      SecurityProtocolSSL,
		TLSInsecureSkipVerify: true,
	})
	if got != "SSL tls=skip-verify" {
		t.Errorf("SSL skip-verify summary = %q", got)
	}
}

func TestTestDiagSummaryLine(t *testing.T) {
	summary := testDiagSummary{
		seeds:      []string{"k1:9092", "k2:9092"},
		seedSource: "bootstrap",
		profile: Profile{
			SecurityProtocol: SecurityProtocolSASLSSL,
			SASLMechanism:    SASLMechanismSCRAMSHA512,
			TLSCACert:        "-----BEGIN CERTIFICATE-----",
		},
		elapsed: 1500 * time.Millisecond,
		status:  "failed",
		err:     `boom "quoted"`,
	}
	line := summary.line([]dialProbe{
		{addr: "k1:9092", tcpMS: 3, tlsMS: 11, err: "dial tcp: refused"},
		{addr: "k2:9092", tcpMS: 2},
	})
	for _, want := range []string{
		"[dbx-plugin-kafka] connection/test",
		"status=failed",
		"seeds=[k1:9092,k2:9092]",
		"seed_source=bootstrap",
		"security=SASL_SSL",
		"sasl=SCRAM-SHA-512",
		"tls=on",
		"ca_cert=true client_cert=false",
		"elapsed=1.5s",
		`err="dial tcp: refused"`,
		`error="boom \"quoted\""`,
	} {
		if !strings.Contains(line, want) {
			t.Errorf("line missing %q:\n%s", want, line)
		}
	}
	if strings.Contains(line, "\n") {
		t.Errorf("summary must stay on one line:\n%s", line)
	}

	okLine := testDiagSummary{
		seeds: []string{"k1:9092"}, seedSource: "runtime",
		profile: Profile{SecurityProtocol: SecurityProtocolPlaintext},
		elapsed: 10 * time.Millisecond, status: "ok", brokers: 3,
	}.line(nil)
	if !strings.Contains(okLine, "status=ok") || !strings.Contains(okLine, "brokers=3") {
		t.Errorf("ok line = %q", okLine)
	}
	if strings.Contains(okLine, "tls=") || strings.Contains(okLine, "sasl=") {
		t.Errorf("PLAINTEXT summary must omit tls/sasl keys: %q", okLine)
	}
}

// TestTestDiagDialerStages 验证探针拨号记录 TCP 阶段并在 TLS 配置存在时
// 完成握手（127.0.0.1:1 拒连 → tcp 阶段带 err；探针落入摘要）。
// TestProbeDialerOptsPassKgoValidation 是 S17 集成矩阵的回归测试：探针路径
// 组装的 opts 追加 kgo.Dialer 后必须仍能通过 kgo 校验（DialTLSConfig 已被
// 省略），TLS 配置交由探针拨号器使用；常规路径再叠 Dialer 则必须被拒绝。
func TestProbeDialerOptsPassKgoValidation(t *testing.T) {
	entry := &connEntry{
		profile: Profile{
			ID:               "c1",
			BootstrapServers: []string{"k1:9092"},
			SecurityProtocol: SecurityProtocolSSL,
		},
	}

	probeOpts, probeTLS, err := entry.buildClientOptsWithSeeds(entry.profile.BootstrapServers, true)
	if err != nil {
		t.Fatalf("probe opts error = %v", err)
	}
	if probeTLS == nil {
		t.Fatal("probe path must return the tls config for the probe dialer")
	}
	probeOpts = append(probeOpts, kgo.Dialer(newTestDiag(nil).dialer(probeTLS)))
	probeClient, err := kgo.NewClient(probeOpts...)
	if err != nil {
		t.Fatalf("probe opts + Dialer rejected by kgo: %v", err)
	}
	probeClient.Close()

	baseOpts, _, err := entry.buildClientOptsWithSeeds(entry.profile.BootstrapServers, false)
	if err != nil {
		t.Fatalf("base opts error = %v", err)
	}
	baseOpts = append(baseOpts, kgo.Dialer(newTestDiag(nil).dialer(probeTLS)))
	if _, err := kgo.NewClient(baseOpts...); err == nil || !strings.Contains(err.Error(), "cannot set both Dialer and DialTLSConfig") {
		t.Errorf("expected kgo to reject Dialer + DialTLSConfig, got %v", err)
	}
}

func TestTestDiagDialerStages(t *testing.T) {
	diag := newTestDiag(&bytes.Buffer{})
	dial := diag.dialer(nil)
	if _, err := dial(context.Background(), "tcp", "127.0.0.1:1"); err == nil {
		t.Fatal("dial to refused port expected error")
	}
	diag.mu.Lock()
	probes := append([]dialProbe{}, diag.probes...)
	diag.mu.Unlock()
	if len(probes) != 1 {
		t.Fatalf("probes = %d, want 1", len(probes))
	}
	probe := probes[0]
	if probe.addr != "127.0.0.1:1" || probe.err == "" || probe.tcpMS < 0 {
		t.Errorf("probe = %+v", probe)
	}

	// TLS 配置存在但目标拒绝：不得 panic，错误带进探针。
	dialTLS := diag.dialer(&tls.Config{MinVersion: tls.VersionTLS12})
	if _, err := dialTLS(context.Background(), "tcp", "127.0.0.1:1"); err == nil {
		t.Fatal("tls dial to refused port expected error")
	}
}

// TestTestFailureDiagnosticsEndToEnd 走完整 Test 失败路径：返回错误带拨号
// 目标摘要且 %w 保留原始错误；sink 收到单行摘要与 kgo WARN；全程无凭据。
func TestTestFailureDiagnosticsEndToEnd(t *testing.T) {
	const password = "s3cret-password-value"
	params, err := lifecycle.Parse([]byte(`{
	  "connection": {
	    "id": "conn-diag",
	    "name": "diag",
	    "external_config": {
	      "bootstrap_servers": "k1:9092",
	      "security_protocol": "SASL_PLAINTEXT",
	      "sasl_mechanism": "PLAIN",
	      "sasl_username": "alice"
	    },
	    "connection_secrets": { "sasl_password": "` + password + `" }
	  },
	  "runtime": { "host": "127.0.0.1", "port": 1 }
	}`))
	if err != nil {
		t.Fatalf("parse params: %v", err)
	}

	sink := &bytes.Buffer{}
	service := NewService()
	service.testDiagSink = sink
	service.testTimeout = 400 * time.Millisecond

	_, testErr := service.Test(context.Background(), params)
	if testErr == nil {
		t.Fatal("Test against refused port expected error")
	}
	out := sink.String()

	// 返回错误：目标摘要 + 原始错误保留（前端分类依赖子串）。
	if !strings.Contains(testErr.Error(), "connect to 127.0.0.1:1 failed after") {
		t.Errorf("error missing target summary: %v", testErr)
	}
	if !strings.Contains(testErr.Error(), "SASL_PLAINTEXT/PLAIN") {
		t.Errorf("error missing protocol summary: %v", testErr)
	}

	// 摘要行：runtime 种子来源 + SASL 机制键。
	if !strings.Contains(out, "status=failed") || !strings.Contains(out, "seeds=[127.0.0.1:1]") {
		t.Errorf("sink missing failed summary:\n%s", out)
	}
	if !strings.Contains(out, "seed_source=runtime") {
		t.Errorf("sink missing seed_source=runtime:\n%s", out)
	}
	if !strings.Contains(out, "sasl=PLAIN") {
		t.Errorf("sink missing sasl mechanism:\n%s", out)
	}
	// kgo 内部 WARN（拨号失败带地址）应经 BasicLogger 落同一 sink。
	if !strings.Contains(out, "[dbx-plugin-kafka:kgo]") {
		t.Errorf("sink missing kgo WARN line:\n%s", out)
	}

	// 红线：凭据不出现在诊断输出与返回错误中。
	for _, blob := range []string{out, testErr.Error()} {
		if strings.Contains(blob, password) {
			t.Errorf("diagnostics leaked sasl password:\n%s", blob)
		}
	}
}
