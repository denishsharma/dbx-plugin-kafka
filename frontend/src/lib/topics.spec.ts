// topics 纯函数单测：业务评分排序 / internal 沉底 / 关键字过滤 / 收藏置顶 / lag 聚合。
import { describe, expect, it } from "vitest";
import { filterTopics, isInternalTopicName, scoreTopicName, sortTopics, sortTopicsPinned, sumLag } from "./topics";

describe("topic ranking", () => {
  it("scores business-like names above infra names", () => {
    expect(scoreTopicName("order-events")).toBeGreaterThan(scoreTopicName("connect-offsets"));
  });

  it("detects internal topics and sinks them to the bottom", () => {
    expect(isInternalTopicName("_schemas")).toBe(true);
    const sorted = sortTopics([
      { name: "_internal.state" },
      { name: "zzz-raw" },
      { name: "order-events", isInternal: false },
      { name: "users" },
    ]);
    expect(sorted.map((topic) => topic.name)).toEqual(["order-events", "users", "zzz-raw", "_internal.state"]);
  });

  it("filters topics by keyword", () => {
    const topics = [{ name: "orders" }, { name: "users" }];
    expect(filterTopics(topics, "ORD")).toHaveLength(1);
    expect(filterTopics(topics, "  ")).toHaveLength(2);
  });

  // Lane4 打磨：收藏置顶——先按 sortTopics 排，收藏项稳定提前；收藏项内部
  // 保持同一相对顺序；internal 收藏同样置顶（用户显式收藏优先于沉底）。
  it("pins favorites to the top while keeping the base order (Lane4)", () => {
    const topics = [{ name: "_internal.state" }, { name: "zzz-raw" }, { name: "order-events" }, { name: "users" }];
    expect(sortTopicsPinned(topics, new Set())).toEqual(sortTopics(topics));
    const pinned = sortTopicsPinned(topics, new Set(["users", "_internal.state"]));
    expect(pinned.map((topic) => topic.name)).toEqual(["users", "_internal.state", "order-events", "zzz-raw"]);
    // 不改入参
    expect(topics.map((topic) => topic.name)).toEqual(["_internal.state", "zzz-raw", "order-events", "users"]);
  });
});

describe("lag aggregation", () => {
  it("sums non-negative lags and treats missing as zero", () => {
    expect(sumLag([{ lag: 5 }, { lag: 0 }, {}, { lag: -3 }, { lag: null }])).toBe(5);
  });
});
