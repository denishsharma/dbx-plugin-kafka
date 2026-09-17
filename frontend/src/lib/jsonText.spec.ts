// jsonText 纯函数单测：headers JSON 校验 + 跨引擎一致的 jsonErrorLine。
import { describe, expect, it } from "vitest";
import { jsonErrorLine, parseHeadersJson } from "./jsonText";

describe("headers json", () => {
  it("accepts string-valued objects only", () => {
    expect(parseHeadersJson('{"trace":"abc"}')).toEqual({ headers: { trace: "abc" } });
    expect(parseHeadersJson("")).toEqual({ headers: {} });
    expect(parseHeadersJson("[1]")).toHaveProperty("error");
    expect(parseHeadersJson('{"n":1}')).toHaveProperty("error");
    expect(parseHeadersJson("{bad")).toHaveProperty("error");
  });
});

describe("json error line (editor linter)", () => {
  it("returns null for valid, empty and whitespace-only text", () => {
    expect(jsonErrorLine('{"a": [1, 2, {"b": null}], "c": "x"}')).toBeNull();
    expect(jsonErrorLine("")).toBeNull();
    expect(jsonErrorLine("   \n\t ")).toBeNull();
  });

  it("locates the first syntax error line across value kinds", () => {
    expect(jsonErrorLine("{bad}")).toBe(1);
    expect(jsonErrorLine('{\n  "a": 1,\n  "b": tru\n}')).toBe(3); // 非法字面量
    expect(jsonErrorLine('{\n  "a": 1\n  "b": 2\n}')).toBe(3); // 缺逗号
    expect(jsonErrorLine("[1, 2,\n3,\n]")).toBe(3); // 数组尾逗号
    expect(jsonErrorLine('{"k": "v"')).toBe(1); // 未闭合
  });

  it("tracks multi-line strings/objects and trailing junk", () => {
    expect(jsonErrorLine('{\n  "a": "line1\nbroken"}')).toBe(2); // 字符串内裸换行
    expect(jsonErrorLine('{"a": 1} extra')).toBe(1); // 尾部多余 token
    expect(jsonErrorLine("123")).toBeNull(); // 顶层标量合法
    expect(jsonErrorLine("1.5e-3")).toBeNull();
    expect(jsonErrorLine("01")).toBe(1); // 前导零 → 多余 token
    expect(jsonErrorLine('{"\\u00zz": 1}')).toBe(1); // 非法 \\u 转义
  });
});
