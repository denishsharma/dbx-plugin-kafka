// schemaTree 纯函数单测（F5）：AVRO / JSON Schema → 树模型。
import { describe, expect, it } from "vitest";
import { buildSchemaTree } from "./schemaTree";

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

describe("schema tree model (F5)", () => {
  it("builds a collapsible tree from an AVRO record", () => {
    const root = buildSchemaTree(JSON.stringify(ORDER_AVRO_SCHEMA))!;
    expect(root.type).toBe("record");
    const names = root.children!.map((child) => child.name);
    expect(names).toEqual(["orderId", "amount", "quantity", "paid", "status", "tags", "meta", "ref", "day", "ts", "uid"]);
    const status = root.children!.find((child) => child.name === "status")!;
    expect(status.type).toContain("NEW");
    // union 节点带分支子节点；逻辑类型在类型标注里。
    const ref = root.children!.find((child) => child.name === "ref")!;
    expect(ref.type).toBe("union:string");
    expect(ref.children!.map((child) => child.name)).toEqual(["[0]", "[1]"]);
    const day = root.children!.find((child) => child.name === "day")!;
    expect(day.type).toContain("date");
  });

  it("renders Avro field defaults and JSON Schema required markers", () => {
    const avro = buildSchemaTree('{"type":"record","name":"R","fields":[{"name":"c","type":"string","default":"usd"}]}')!;
    expect(avro.children![0].defaultValue).toBe("usd");

    const json = buildSchemaTree('{"type":"object","properties":{"id":{"type":"string"},"n":{"type":"integer","default":7}},"required":["id"]}')!;
    expect(json.children!.map((child) => child.name)).toEqual(["id", "n"]);
    expect(json.children![0].type).toContain("*");
    expect(json.children![1].defaultValue).toBe("7");
  });

  it("returns null for PROTOBUF text / broken JSON (text + hint path)", () => {
    expect(buildSchemaTree('syntax = "proto3";\\nmessage Order {}')).toBeNull();
    expect(buildSchemaTree("{broken")).toBeNull();
  });
});
