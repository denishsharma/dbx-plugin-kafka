// properties 纯函数单测：Confluent properties 解析与连接表单字段映射。
import { describe, expect, it } from "vitest";
import { parsePropertiesText, propertiesToConnectionForm } from "./properties";

describe("confluent properties", () => {
  it("parses key=value lines with comments, continuations and escapes", () => {
    const properties = parsePropertiesText(`
# comment line
! another comment
bootstrap.servers=dbx-kafka-test\\
:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM\\-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="kafka" password="pass=1";
`);
    expect(properties["bootstrap.servers"]).toBe("dbx-kafka-test:9092");
    expect(properties["security.protocol"]).toBe("SASL_SSL");
    expect(properties["sasl.mechanism"]).toBe("SCRAM-SHA-256");
    expect(properties["sasl.jaas.config"]).toContain('password="pass=1"');
  });

  it("maps properties onto the connection form fields", () => {
    const form = propertiesToConnectionForm(
      parsePropertiesText(`
bootstrap.servers=broker1:9092,broker2:9092
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=ScramLoginModule required username="app" password="secret";
`),
    );
    expect(form).toEqual({
      bootstrapServers: "broker1:9092,broker2:9092",
      securityProtocol: "SASL_SSL",
      saslMechanism: "SCRAM-SHA-256",
      saslUsername: "app",
      saslPassword: "secret",
      tlsInsecureSkipVerify: false,
    });
    const open = propertiesToConnectionForm(parsePropertiesText("bootstrap.servers=b:9092"));
    // key 缺省 = 保持默认校验（不跳过）
    expect(open.tlsInsecureSkipVerify).toBe(false);
    expect(open.saslUsername).toBe("");
    const skipped = propertiesToConnectionForm(parsePropertiesText("ssl.endpoint.identification.algorithm="));
    expect(skipped.tlsInsecureSkipVerify).toBe(true);
    const skippedNone = propertiesToConnectionForm(parsePropertiesText("ssl.endpoint.identification.algorithm=NONE"));
    expect(skippedNone.tlsInsecureSkipVerify).toBe(true);
  });
});
