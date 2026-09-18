package kafkaconn

// diag.go：connection/test 的连接诊断（issue #28：测试失败时用户只见泛化
// 「超时」——sidecar 此前零日志，且与宿主默认 10s 连接超时同刻竞争，无法
// 区分卡在 TCP / TLS / SASL / Metadata 哪个阶段，也无法看出实际拨号的是
// runtime 端点还是 bootstrap 列表）。
//
// Test 路径向同一 sink 输出两类脱敏诊断：
//   - 一行键值摘要（seeds 及其来源、安全协议面、分阶段探针、总耗时、错误）；
//   - franz-go 内部 WARN 日志（拨号失败地址、SASL 认证错误、EOF/TLS 不匹配
//     等提示，kgo logger.go：WARN 覆盖 request failures）。
//
// 宿主会把 sidecar stderr 按行转进宿主日志（runtime.rs spawn_stderr_reader
// 以 log::warn!("[plugin:{id}] …") 落盘），因此无需文件、无需额外权限。
//
// 红线：诊断输出只含地址、键名与耗时；凭据（SASL 密码、client key、token）
// 不落盘；CA/客户端证书只报有无，不报内容。

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// defaultTestTimeout 是 connection/test 的总预算。必须小于宿主默认连接
// 超时（10s，连接级可配 1–300s）：宿主对 connection/test 的
// invoke_with_timeout 超时时会返回不带任何拨号细节的泛化文案，sidecar 先
// 返回才能把携带 seed 地址与阶段的精确错误带给用户（issue #28）。
const defaultTestTimeout = 8 * time.Second

// testRequestOverhead 是测试路径的单请求 deadline（覆盖 TCP+TLS+SASL+
// metadata 全程）。admin/consume 路径维持 5s；测试探活面对的是冷连接 +
// 完整握手链路，放宽到 7s（仍小于 8s 总预算），避免慢握手集群被 5s 误杀。
const testRequestOverhead = 7 * time.Second

// testDiag 收集一次 connection/test 的拨号探针并输出脱敏摘要。
type testDiag struct {
	sink io.Writer

	mu     sync.Mutex
	probes []dialProbe
}

// dialProbe 是单次拨号尝试的分阶段结果（毫秒；0 = 未走到该阶段）。
type dialProbe struct {
	addr  string
	tcpMS int64
	tlsMS int64
	err   string
}

// newTestDiag 创建诊断器；sink 为 nil 时写 stderr（宿主会转进宿主日志）。
func newTestDiag(sink io.Writer) *testDiag {
	if sink == nil {
		sink = os.Stderr
	}
	return &testDiag{sink: sink}
}

// dialer 返回带探针的拨号器。tlsConfig 非 nil 时由拨号器自行完成 TLS 握手：
// kgo 仅在 dialFn 为空时才应用 DialTLSConfig（client.go validateCfg），设置
// 自定义拨号器后必须把 TLS 一起包进去，否则静默降级为明文。TLS 语义对齐
// kgo 内置路径（Clone config；ServerName 为空时由拨号地址推导 SNI）。
// 代理路由（runtimeProxyDialer）自带 TLS 包装，不走本探针，仅记录摘要。
func (d *testDiag) dialer(tlsConfig *tls.Config) func(context.Context, string, string) (net.Conn, error) {
	base := &net.Dialer{Timeout: 10 * time.Second}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		probe := dialProbe{addr: address}
		start := time.Now()
		conn, err := base.DialContext(ctx, network, address)
		probe.tcpMS = time.Since(start).Milliseconds()
		if err == nil && tlsConfig != nil {
			config := tlsConfig.Clone()
			if config.ServerName == "" {
				if host, _, splitErr := net.SplitHostPort(address); splitErr == nil {
					config.ServerName = strings.Trim(host, "[]")
				}
			}
			tlsStart := time.Now()
			tlsConn := tls.Client(conn, config)
			err = tlsConn.HandshakeContext(ctx)
			probe.tlsMS = time.Since(tlsStart).Milliseconds()
			if err == nil {
				conn = tlsConn
			}
		}
		if err != nil {
			probe.err = err.Error()
			if conn != nil {
				_ = conn.Close()
			}
		}
		d.mu.Lock()
		d.probes = append(d.probes, probe)
		d.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
}

// testDiagSummary 是摘要行的载荷（纯数据，便于单测断言格式）。
type testDiagSummary struct {
	seeds      []string
	seedSource string // bootstrap | runtime | zookeeper | proxy-route
	profile    Profile
	elapsed    time.Duration
	status     string // ok | failed
	brokers    int    // status=ok 时为 broker 数
	err        string
}

// seedSourceLabel 归类实际生效的拨号种子来源（seedBrokers 的语义镜像）。
func seedSourceLabel(entry *connEntry) string {
	switch {
	case entry.target.Proxy != nil:
		return "proxy-route"
	case entry.profile.ConnectionSource == ConnectionSourceZookeeper:
		return "zookeeper"
	case entry.target.Host != "" && entry.target.Port > 0:
		return "runtime"
	default:
		return "bootstrap"
	}
}

// line 渲染单行脱敏摘要（键值风格，与 main.go 的 [dbx-plugin-kafka] 前缀
// 日志同族；kgo 内部日志由 BasicLogger 以 [dbx-plugin-kafka:kgo] 前缀输出）。
func (s testDiagSummary) line(probes []dialProbe) string {
	var b strings.Builder
	b.WriteString("[dbx-plugin-kafka] connection/test")
	fmt.Fprintf(&b, " status=%s", s.status)
	fmt.Fprintf(&b, " seeds=[%s]", strings.Join(s.seeds, ","))
	fmt.Fprintf(&b, " seed_source=%s", s.seedSource)
	fmt.Fprintf(&b, " security=%s", s.profile.SecurityProtocol)
	if s.profile.hasSASL() {
		fmt.Fprintf(&b, " sasl=%s", s.profile.SASLMechanism)
	}
	if s.profile.hasTLS() {
		b.WriteString(" tls=on")
		if s.profile.TLSInsecureSkipVerify {
			b.WriteString(" tls_skip_verify=on")
		}
		fmt.Fprintf(&b, " ca_cert=%t client_cert=%t",
			strings.TrimSpace(s.profile.TLSCACert) != "",
			strings.TrimSpace(s.profile.TLSClientCert) != "")
	}
	fmt.Fprintf(&b, " elapsed=%s", s.elapsed.Round(time.Millisecond))
	if len(probes) > 0 {
		fmt.Fprintf(&b, " probes=[%s]", formatProbes(probes))
	}
	if s.status == "ok" {
		fmt.Fprintf(&b, " brokers=%d", s.brokers)
	}
	if s.err != "" {
		fmt.Fprintf(&b, " error=%q", s.err)
	}
	return b.String()
}

// formatProbes 渲染拨号探针；地址含空格时引号包裹（host:port 本身不含
// 空格，仅防御），错误串统一 %q 防止换行破坏单行格式。
func formatProbes(probes []dialProbe) string {
	parts := make([]string, 0, len(probes))
	for _, probe := range probes {
		part := fmt.Sprintf("addr=%s tcp=%dms", probe.addr, probe.tcpMS)
		if probe.tlsMS > 0 {
			part += fmt.Sprintf(" tls=%dms", probe.tlsMS)
		}
		if probe.err != "" {
			part += fmt.Sprintf(" err=%q", probe.err)
		}
		parts = append(parts, "{"+part+"}")
	}
	return strings.Join(parts, " ")
}

// emit 输出摘要行；写失败静默（诊断不能反噬连接测试本身）。
func (d *testDiag) emit(summary testDiagSummary) {
	d.mu.Lock()
	probes := append([]dialProbe{}, d.probes...)
	d.mu.Unlock()
	_, _ = fmt.Fprintln(d.sink, summary.line(probes))
}

// securitySummary 渲染面向用户的协议面摘要（含进失败错误信息；不含凭据，
// 证书只报校验策略）。
func securitySummary(p Profile) string {
	summary := p.SecurityProtocol
	if p.hasSASL() && p.SASLMechanism != "" {
		summary += "/" + p.SASLMechanism
	}
	if p.hasTLS() {
		if p.TLSInsecureSkipVerify {
			summary += " tls=skip-verify"
		} else {
			summary += " tls=verify"
		}
	}
	return summary
}

// kgoTestLogger 返回测试 client 的 franz-go 日志器（WARN 级，与摘要同 sink，
// 行前缀与摘要行区分开）。kgo 默认 nopLogger，拨号失败/SASL 认证错误等内部
// 细节默认全丢；测试路径打开到 WARN 补齐 Metadata/SASL 阶段的可见性。
func kgoTestLogger(sink io.Writer) kgo.Opt {
	return kgo.WithLogger(kgo.BasicLogger(sink, kgo.LogLevelWarn, func() string {
		return "[dbx-plugin-kafka:kgo] "
	}))
}
