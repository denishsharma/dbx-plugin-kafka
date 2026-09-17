// messageExport 纯函数单测：CSV（RFC 4180）/TSV/JSON 导出序列化。
import { describe, expect, it } from "vitest";
import { serializeMessagesToCsv, serializeMessagesToJson, serializeMessagesToTsv } from "./messageExport";
import type { KafkaMessage } from "./api";

function b64(text: string): string {
  const bytes = new TextEncoder().encode(text);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function msg(overrides: Partial<KafkaMessage>): KafkaMessage {
  return { topic: "orders", partition: 0, offset: 1, timestamp: 1_700_000_000_000, ...overrides };
}

describe("export serialization", () => {
  it("serializes CSV with RFC 4180 escaping", () => {
    const csv = serializeMessagesToCsv([
      msg({ key: "k,1", valueText: 'say "hi"', headers: { trace: "abc" } }),
    ]);
    expect(csv.split("\r\n")[0]).toBe("topic,partition,offset,timestamp,key,value,headers");
    expect(csv).toContain('"k,1"');
    expect(csv).toContain('"say ""hi"""');
    expect(csv).toContain("trace=abc");
  });

  it("serializes stable JSON with full value text", () => {
    const json = JSON.parse(serializeMessagesToJson([msg({ key: "k", valueBase64: b64("body") })]));
    expect(json).toHaveLength(1);
    expect(json[0]).toMatchObject({ topic: "orders", partition: 0, offset: 1, key: "k", value: "body" });
  });

  // Lane4 打磨：TSV 导出——转义规则与 CSV 不同（无引号包裹，制表符/换行/回车
  // 反斜杠转义，反斜杠自身先转义），列序与 CSV 一致。
  it("serializes TSV with tab/newline escaping and CSV column order", () => {
    const tsv = serializeMessagesToTsv([
      msg({ key: "k\t1", valueText: "line1\nline2\rback\\slash", headers: { trace: "abc" } }),
      msg({ key: "plain", valueText: "no escapes" }),
    ]);
    const [header, first, second] = tsv.split("\r\n");
    expect(header).toBe("topic\tpartition\toffset\ttimestamp\tkey\tvalue\theaders");
    expect(first).toBe("orders\t0\t1\t1700000000000\tk\\t1\tline1\\nline2\\rback\\\\slash\ttrace=abc");
    expect(second).toBe("orders\t0\t1\t1700000000000\tplain\tno escapes\t");
  });
});
