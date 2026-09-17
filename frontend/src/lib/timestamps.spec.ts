// timestamps 纯函数单测（F6-3/F6-6）：tz 感知格式化 + 日期过滤比较器。
import { describe, expect, it } from "vitest";
import { formatTimestamp, timestampFilterTextComparator, timestampIso } from "./timestamps";

describe("timestamp tz helpers (F6-3)", () => {
  it("formats local vs utc and exposes full ISO for cell titles", () => {
    const ms = Date.UTC(2023, 10, 14, 22, 13, 20);
    expect(formatTimestamp(ms, "utc")).toBe("2023-11-14 22:13:20");
    expect(timestampIso(ms)).toBe("2023-11-14T22:13:20.000Z");
    // local 与 utc 的日期文本按定义逐分量断言（与机器时区无关）。
    const local = formatTimestamp(ms, "local");
    const date = new Date(ms);
    const pad = (value: number) => String(value).padStart(2, "0");
    expect(local).toBe(`${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`);
    // 缺省参数 = local（既有调用面行为不变）；非法值兜底。
    expect(formatTimestamp(ms)).toBe(local);
    expect(formatTimestamp(undefined)).toBe("—");
    expect(timestampIso(undefined)).toBe("");
  });

  it("compares date-filter days after parsing cell text in its tz", () => {
    const filterDay = new Date(2023, 10, 14);
    expect(timestampFilterTextComparator(filterDay, "2023-11-14 08:00:00", "local")).toBe(0);
    expect(timestampFilterTextComparator(filterDay, "2023-11-13 23:59:59", "local")).toBe(-1);
    expect(timestampFilterTextComparator(filterDay, "2023-11-15 00:00:00", "local")).toBe(1);
    expect(timestampFilterTextComparator(filterDay, "garbage", "local")).toBe(1);
    // UTC 文本按 UTC 解析为时刻再取本地日比较。
    const instant = new Date(Date.UTC(2023, 10, 13, 23, 0, 0));
    const cellLocalDay = new Date(instant.getFullYear(), instant.getMonth(), instant.getDate());
    expect(timestampFilterTextComparator(cellLocalDay, "2023-11-13 23:00:00", "utc")).toBe(0);
  });
});
