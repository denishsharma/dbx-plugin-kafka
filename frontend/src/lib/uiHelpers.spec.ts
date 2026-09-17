// @vitest-environment happy-dom
// Phase 3 H 路 UI 纯助手（F6）：copyTextToClipboard 的 clipboard API / execCommand
// 兜底双路径 + debounce 防抖语义（quickFilter 150ms 共用）。
// 自 kafkaModel.spec 迁入：流缓冲 / 消息表上限 / 分区校验 / 即时搜索过滤。
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  appendStreamRows,
  capRows,
  copyTextToClipboard,
  debounce,
  filterMessagesByKeyword,
  MESSAGE_ROWS_MAX,
  partitionInputIssue,
  truncatedValuePreview,
} from "./uiHelpers";
import type { KafkaMessage } from "./api";

function msg(overrides: Partial<KafkaMessage>): KafkaMessage {
  return { topic: "orders", partition: 0, offset: 1, timestamp: 1_700_000_000_000, ...overrides };
}

// navigator.clipboard 是 getter-only（happy-dom 同浏览器），测试用 defineProperty 注入。
function stubClipboard(clipboard: unknown) {
  Object.defineProperty(navigator, "clipboard", { value: clipboard, configurable: true });
}
function stubExecCommand(execCommand: unknown) {
  Object.defineProperty(document, "execCommand", { value: execCommand, configurable: true });
}

describe("copyTextToClipboard (F6-2)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("prefers navigator.clipboard.writeText", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    stubClipboard({ writeText });
    await expect(copyTextToClipboard("hello")).resolves.toBe(true);
    expect(writeText).toHaveBeenCalledWith("hello");
  });

  it("falls back to execCommand when the clipboard API rejects", async () => {
    stubClipboard({ writeText: vi.fn().mockRejectedValue(new Error("denied")) });
    const execCommand = vi.fn().mockReturnValue(true);
    stubExecCommand(execCommand);
    await expect(copyTextToClipboard("fallback")).resolves.toBe(true);
    expect(execCommand).toHaveBeenCalledWith("copy");
  });

  it("uses execCommand when the clipboard API is unavailable", async () => {
    stubClipboard(undefined);
    const execCommand = vi.fn().mockReturnValue(true);
    stubExecCommand(execCommand);
    await expect(copyTextToClipboard("legacy")).resolves.toBe(true);
    expect(execCommand).toHaveBeenCalledWith("copy");
  });

  it("reports failure when both paths fail and ignores empty input", async () => {
    stubClipboard(undefined);
    stubExecCommand(vi.fn().mockReturnValue(false));
    await expect(copyTextToClipboard("nope")).resolves.toBe(false);
    const writeText = vi.fn();
    stubClipboard({ writeText });
    await expect(copyTextToClipboard("")).resolves.toBe(false);
    expect(writeText).not.toHaveBeenCalled();
  });
});

describe("debounce (F6-1 quick filter timing)", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("collapses bursts into a single trailing call", () => {
    const fn = vi.fn();
    const debounced = debounce(fn, 150);
    debounced("a");
    vi.advanceTimersByTime(100);
    debounced("ab");
    vi.advanceTimersByTime(100);
    debounced("abc");
    expect(fn).not.toHaveBeenCalled();
    vi.advanceTimersByTime(150);
    expect(fn).toHaveBeenCalledTimes(1);
    expect(fn).toHaveBeenCalledWith("abc");
  });

  it("cancel drops the pending call", () => {
    const fn = vi.fn();
    const debounced = debounce(fn, 150);
    debounced("x");
    debounced.cancel();
    vi.advanceTimersByTime(500);
    expect(fn).not.toHaveBeenCalled();
  });
});

// -- 以下自 kafkaModel.spec 迁入 -----------------------------------------------

describe("stream buffer", () => {
  it("appends rows, caps at max and counts dropped oldest rows", () => {
    const seed = [{ topic: "t", partition: 0, offset: 0, timestamp: 1 }, { topic: "t", partition: 0, offset: 1, timestamp: 2 }];
    const keep = appendStreamRows(seed, [], 10);
    expect(keep).toEqual({ rows: seed, dropped: 0 });
    const grown = appendStreamRows(seed, [{ topic: "t", partition: 0, offset: 2, timestamp: 3 }], 3);
    expect(grown.rows.map((row) => row.offset)).toEqual([0, 1, 2]);
    expect(grown.dropped).toBe(0);
    const overflow = appendStreamRows(grown.rows, [{ topic: "t", partition: 0, offset: 3, timestamp: 4 }], 3, 0);
    expect(overflow.rows.map((row) => row.offset)).toEqual([1, 2, 3]);
    expect(overflow.dropped).toBe(1);
    const stacked = appendStreamRows(overflow.rows, overflow.rows.slice(), 3, 1);
    expect(stacked.dropped).toBe(1 + 3);
    expect(stacked.rows).toHaveLength(3);
  });
});

describe("message table cap", () => {
  it("returns the same reference and zero dropped when under the cap", () => {
    const rows = [msg({ partition: 0, offset: 0 }), msg({ partition: 0, offset: 1 })];
    const capped = capRows(rows);
    expect(capped.rows).toBe(rows); // 零拷贝语义
    expect(capped).toEqual({ rows, total: 2, dropped: 0 });
  });

  it("trims the head and keeps the newest tail when over the cap", () => {
    const rows = Array.from({ length: 5 }, (_unused, index) => msg({ partition: 0, offset: index }));
    const capped = capRows(rows, 3);
    expect(capped.rows.map((row) => row.offset)).toEqual([2, 3, 4]); // 最新在尾部
    expect(capped.total).toBe(5);
    expect(capped.dropped).toBe(2);
    expect(capped.rows).not.toBe(rows); // 裁剪产出新数组
  });

  it("caps to a single row and handles an empty list", () => {
    const rows = [msg({ partition: 0, offset: 7 }), msg({ partition: 0, offset: 8 })];
    expect(capRows(rows, 1).rows.map((row) => row.offset)).toEqual([8]);
    expect(capRows([], 10)).toEqual({ rows: [], total: 0, dropped: 0 });
    expect(MESSAGE_ROWS_MAX).toBeGreaterThan(0);
  });

  it("keeps short value previews untouched", () => {
    const short = "hello world";
    expect(truncatedValuePreview(short)).toEqual({ text: short, truncated: false });
  });

  it("truncates long values keeping head and tail with a hidden-char marker", () => {
    const text = "A".repeat(20000) + "B".repeat(100) + "C".repeat(20000);
    const preview = truncatedValuePreview(text);
    expect(preview.truncated).toBe(true);
    expect(preview.text.length).toBeLessThan(text.length);
    expect(preview.text.startsWith("A".repeat(100))).toBe(true); // 头部保留
    expect(preview.text.endsWith("C".repeat(100))).toBe(true); // 尾部保留
    expect(preview.text).toMatch(/⋯ \d+ chars ⋯/); // 中段省略标记
  });
});

describe("partitionInputIssue (F6-4 produce partition guard)", () => {
  it("accepts empty (auto) and in-range partitions", () => {
    expect(partitionInputIssue("", 3)).toBeNull();
    expect(partitionInputIssue("  ", 3)).toBeNull();
    expect(partitionInputIssue("0", 3)).toBeNull();
    expect(partitionInputIssue("2", 3)).toBeNull();
  });

  it("rejects non-integers and out-of-range partitions with i18n key fragments", () => {
    expect(partitionInputIssue("-1", 3)).toBe("partitionInvalid");
    expect(partitionInputIssue("1.5", 3)).toBe("partitionInvalid");
    expect(partitionInputIssue("abc", 3)).toBe("partitionInvalid");
    expect(partitionInputIssue("3", 3)).toBe("partitionOutOfRange");
    expect(partitionInputIssue("9", 3)).toBe("partitionOutOfRange");
    // partitionCount 缺省（未拿到 topics/list）不做上界校验。
    expect(partitionInputIssue("9", undefined)).toBeNull();
  });
});

describe("filterMessagesByKeyword (F6-1 stream quick filter)", () => {
  const rows: Array<{ partition: number; offset: number; key: string; valueText: string; headers: Record<string, string> }> = [
    { partition: 0, offset: 1, key: "alpha", valueText: '{"v":1}', headers: { trace: "t1" } },
    { partition: 1, offset: 2, key: "beta", valueText: "plain", headers: {} },
    { partition: 2, offset: 3, key: "gamma", valueText: "gold", headers: {} },
  ];

  it("filters loaded rows across partition/offset/key/value/headers", () => {
    expect(filterMessagesByKeyword(rows, "")).toEqual(rows);
    expect(filterMessagesByKeyword(rows, "  ")).toEqual(rows);
    expect(filterMessagesByKeyword(rows, "alpha")).toEqual([rows[0]]);
    // "2" 同时命中 offset=2 与 partition=2（contains 语义，只看已加载行）。
    expect(filterMessagesByKeyword(rows, "2")).toEqual([rows[1], rows[2]]);
    expect(filterMessagesByKeyword(rows, "beta")).toEqual([rows[1]]);
    expect(filterMessagesByKeyword(rows, "TRACE")).toEqual([rows[0]]);
    expect(filterMessagesByKeyword(rows, "nope")).toEqual([]);
  });
});
