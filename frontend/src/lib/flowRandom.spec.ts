// flowRandom 纯函数单测（F4）：mulberry32 固定向量 / generateAvroRandom /
// expandTemplate 占位符 / matchingSchemaSubjects / 参数夹持。
import { describe, expect, it } from "vitest";
import {
  clampFlowCount,
  clampFlowIntervalMs,
  expandTemplate,
  generateAvroRandom,
  matchingSchemaSubjects,
  mulberry32,
} from "./flowRandom";

describe("mulberry32 (F4 fixed-seed RNG)", () => {
  // 固定向量（照 zstd 预生成向量范式：实现 + 种子 42 的序列锁定，防算法漂移）。
  it("produces the locked sequence for seed 42", () => {
    const rng = mulberry32(42);
    const sequence = [rng(), rng(), rng(), rng(), rng()];
    expect(sequence).toEqual([
      0.6011037519201636,
      0.44829055899754167,
      0.8524657934904099,
      0.6697340414393693,
      0.17481389874592423,
    ]);
  });

  it("is deterministic per seed and stays in [0,1)", () => {
    const left = mulberry32(7);
    const right = mulberry32(7);
    for (let index = 0; index < 32; index += 1) {
      const a = left();
      const b = right();
      expect(a).toBe(b);
      expect(a).toBeGreaterThanOrEqual(0);
      expect(a).toBeLessThan(1);
    }
    expect(mulberry32(8)()).not.toBe(mulberry32(7)());
  });
});

const ORDER_AVRO_SCHEMA = {
  type: "record",
  name: "Order",
  fields: [
    { name: "orderId", type: "string" },
    { name: "amount", type: "double" },
    { name: "quantity", type: "int" },
    { name: "paid", type: "boolean" },
    { name: "status", type: { type: "enum", name: "Status", symbols: ["NEW", "PAID", "CANCELLED"] } },
    { name: "tags", type: { type: "array", items: "string" } },
    { name: "meta", type: { type: "map", values: "string" } },
    { name: "ref", type: ["null", "string"], default: null },
    { name: "day", type: { type: "int", logicalType: "date" } },
    { name: "ts", type: { type: "long", logicalType: "timestamp-millis" } },
    { name: "uid", type: { type: "string", logicalType: "uuid" } },
  ],
};

describe("generateAvroRandom (F4 schema_random)", () => {
  // 固定向量：seed=7 + now=1700000000000 的完整生成值锁定。
  it("matches the locked vector for the Order schema (seed 7)", () => {
    const out = generateAvroRandom(JSON.stringify(ORDER_AVRO_SCHEMA), mulberry32(7), 1_700_000_000_000);
    expect(out).toEqual({
      value:
        '{"orderId":"alpha-155","amount":97.69,"quantity":699,"paid":false,"status":"PAID","tags":["beta-597","delta-332"],"meta":{"k0":"delta-566"},"ref":"alpha-431","day":19675,"ts":1700000000000,"uid":"484f3e32-248c-41e8-9af9-ed025601c567"}',
    });
  });

  it("covers record/array/map/union/enum/fixed and logical types", () => {
    const schema = {
      type: "record",
      name: "All",
      fields: [
        { name: "unionPicksFirstNonNull", type: ["null", "string", "int"] },
        { name: "allNullUnion", type: ["null"] },
        { name: "fixed16", type: { type: "fixed", size: 16, name: "H16" } },
        { name: "dateDays", type: { type: "int", logicalType: "date" } },
        { name: "tsMillis", type: { type: "long", logicalType: "timestamp-millis" } },
        { name: "uid", type: { type: "string", logicalType: "uuid" } },
        { name: "amount", type: { type: "bytes", logicalType: "decimal", precision: 8, scale: 2 } },
      ],
    };
    const out = generateAvroRandom(JSON.stringify(schema), mulberry32(3), 1_700_000_000_000);
    expect("error" in out).toBe(false);
    const value = JSON.parse((out as { value: string }).value) as Record<string, unknown>;
    // union 取非 null 首支（string）；全 null union 只能是 null。
    expect(typeof value.unionPicksFirstNonNull).toBe("string");
    expect(value.allNullUnion).toBeNull();
    // fixed(size 16) → 16 位字符串；date → days 数；timestamp-millis → nowMs。
    expect(String(value.fixed16)).toHaveLength(16);
    expect(value.dateDays).toBe(19675);
    expect(value.tsMillis).toBe(1_700_000_000_000);
    // uuid 形状；decimal 为数值（goavro 侧编码支持度由后端契约管辖）。
    expect(String(value.uid)).toMatch(/^[0-9a-f-]{36}$/);
    expect(typeof value.amount).toBe("number");
  });

  it("reports deterministic errors for invalid schema text", () => {
    expect("error" in generateAvroRandom("not json", mulberry32(1))).toBe(true);
    expect("error" in generateAvroRandom("[]", mulberry32(1))).toBe(true);
    expect(generateAvroRandom("{}", mulberry32(1))).toEqual({ error: 'avro schema is missing "type"' });
    // 未知类型在首次生成时抛错（不产出畸形数据）。
    const out = generateAvroRandom('{"type":"warp"}', mulberry32(1));
    expect("error" in out && out.error).toContain("warp");
  });
});

describe("expandTemplate (F4 template placeholders)", () => {
  // 固定向量：seed=11 + now=1700000000000。
  it("matches the locked vector for all placeholder kinds", () => {
    const out = expandTemplate(
      '{"id":"{uuid}","n":{int:1,9},"f":{float:0,100},"k":"{pick:gold|silver}","now":"{now}"}',
      mulberry32(11),
      1_700_000_000_000,
    );
    expect(out).toBe('{"id":"8899d818-977d-46aa-2246-692996dcc594","n":1,"f":74.09,"k":"silver","now":"2023-11-14T22:13:20.000Z"}');
  });

  it("leaves unknown placeholders and edge cases untouched", () => {
    const rng = mulberry32(1);
    expect(expandTemplate('{"keep":"{nope}","multi":"{int:5,5}","empty":"{pick:}","now":"{now}"}', rng, 0)).toBe(
      '{"keep":"{nope}","multi":"5","empty":"{pick:}","now":"1970-01-01T00:00:00.000Z"}',
    );
    expect(expandTemplate("plain", rng, 0)).toBe("plain");
    // int 边界：min=max 时恒等。
    for (let index = 0; index < 8; index += 1) {
      expect(expandTemplate("{int:3,3}", rng, 0)).toBe("3");
    }
  });
});

describe("matchingSchemaSubjects (F4 subject discovery)", () => {
  it("prefix-matches <topic>-value first and <topic>-key second", () => {
    const subjects = ["order-events-value", "order-events-key", "order-events-v2-value", "other-value"];
    expect(matchingSchemaSubjects("order-events", subjects)).toEqual({
      key: "order-events-key",
      value: "order-events-value",
    });
    expect(matchingSchemaSubjects("order-events", ["order-events-key"])).toEqual({ key: "order-events-key" });
    expect(matchingSchemaSubjects("missing", subjects)).toEqual({});
    expect(matchingSchemaSubjects("", subjects)).toEqual({});
  });
});

describe("flow parameter clamps (F4)", () => {
  it("clamps countPerSend into 1..100 with fallback 1", () => {
    expect(clampFlowCount("1")).toBe(1);
    expect(clampFlowCount(100)).toBe(100);
    expect(clampFlowCount(0)).toBe(1);
    expect(clampFlowCount(101)).toBe(100);
    expect(clampFlowCount("abc")).toBe(1);
    expect(clampFlowCount(undefined)).toBe(1);
    expect(clampFlowCount(-5)).toBe(1);
  });

  it("clamps intervalMs into 250..10000 with fallback 1000", () => {
    expect(clampFlowIntervalMs("1000")).toBe(1000);
    expect(clampFlowIntervalMs(250)).toBe(250);
    expect(clampFlowIntervalMs(249)).toBe(250);
    expect(clampFlowIntervalMs(10001)).toBe(10000);
    expect(clampFlowIntervalMs("")).toBe(1000);
  });
});
