/**
 * topic 数据整理纯函数（自 kafkaModel 拆分）：业务评分排序、internal 沉底、
 * 关键字过滤、收藏置顶与分组 lag 求和。
 */

// -- topic ranking（对标 tinyrdm kafkaNormalize.scoreBusinessTopicName）---------

const BUSINESS_TOKENS = new Set([
  "account", "activity", "audit", "cart", "customer", "email", "event", "events", "invoice", "item",
  "message", "messages", "notification", "order", "orders", "payment", "product", "profile", "session",
  "transaction", "user", "users",
]);
const INFRA_TOKENS = new Set([
  "changelog", "command", "config", "connect", "dlq", "heartbeat", "offset", "offsets", "repartition", "retry",
]);

export function scoreTopicName(name: string): number {
  const normalized = String(name || "").toLowerCase();
  const tokens = normalized.split(/[._-]+/).filter(Boolean);
  const business = tokens.reduce((acc, token) => acc + (BUSINESS_TOKENS.has(token) ? 24 : 0), 0);
  const infra = tokens.reduce((acc, token) => acc + (INFRA_TOKENS.has(token) ? 18 : 0), 0);
  const structure = tokens.length > 1 ? 8 : 0;
  const readable = normalized.length >= 6 && /[a-z]/.test(normalized) ? 4 : 0;
  return business + structure + readable - infra;
}

export function isInternalTopicName(name: string): boolean {
  return String(name || "").startsWith("_");
}

export interface TopicSortItem {
  name: string;
  isInternal?: boolean;
}

/**
 * topic 排序：internal 沉底（_ 开头或后端标记），业务 topic 按业务评分
 * 降序、同分按名称字典序。返回新数组，不改入参。
 */
export function sortTopics<T extends TopicSortItem>(topics: T[]): T[] {
  return [...topics].sort((left, right) => {
    const leftInternal = left.isInternal === true || isInternalTopicName(left.name);
    const rightInternal = right.isInternal === true || isInternalTopicName(right.name);
    if (leftInternal !== rightInternal) return leftInternal ? 1 : -1;
    const scoreDelta = scoreTopicName(right.name) - scoreTopicName(left.name);
    if (scoreDelta !== 0) return scoreDelta;
    return left.name.localeCompare(right.name);
  });
}

export function filterTopics<T extends TopicSortItem>(topics: T[], keyword: string): T[] {
  const needle = keyword.trim().toLowerCase();
  if (!needle) return topics;
  return topics.filter((topic) => topic.name.toLowerCase().includes(needle));
}

/**
 * 收藏置顶排序（Lane4 前端打磨）：先按 sortTopics 排（业务评分 + internal
 * 沉底不变），再把收藏项稳定提前。收藏项内部保持同一相对顺序；internal topic
 * 被收藏时同样置顶（用户显式收藏优先于 internal 沉底）。返回新数组，不改入参。
 */
export function sortTopicsPinned<T extends TopicSortItem>(topics: T[], pinnedNames: ReadonlySet<string>): T[] {
  const sorted = sortTopics(topics);
  if (pinnedNames.size === 0) return sorted;
  return [...sorted.filter((topic) => pinnedNames.has(topic.name)), ...sorted.filter((topic) => !pinnedNames.has(topic.name))];
}

// -- lag aggregation ------------------------------------------------------------

export interface LagRow {
  lag?: number | null;
}

/** 分区 lag 求和；缺失/负值按 0 计（Option 语义的展示兜底在后端）。 */
export function sumLag(rows: LagRow[]): number {
  return rows.reduce((acc, row) => acc + Math.max(0, Number(row.lag ?? 0) || 0), 0);
}
