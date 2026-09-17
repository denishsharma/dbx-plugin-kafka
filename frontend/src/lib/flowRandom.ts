/**
 * Flow 测试数据生成纯函数（自 kafkaModel 拆分；F4）：mulberry32 固定种子
 * RNG、Avro 随机 JSON、模板占位符展开、subject 前缀发现、参数夹持。
 */

// -- Flow 随机测试数据生成（F4；固定种子 RNG + Avro 随机 JSON + 模板占位符）---------

/** mulberry32 PRNG：固定种子 → 固定序列（spec 固定向量断言，照 zstd 向量范式）。 */
export type RandomSource = () => number;

export function mulberry32(seed: number): RandomSource {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Flow 参数夹持：countPerSend 1..100（默认 1）、intervalMs 250..10000（默认 1000）。 */
export function clampFlowCount(value: unknown, fallback = 1): number {
  return clampIntInclusive(value, 1, 100, fallback);
}

export function clampFlowIntervalMs(value: unknown, fallback = 1000): number {
  return clampIntInclusive(value, 250, 10000, fallback);
}

function clampIntInclusive(value: unknown, min: number, max: number, fallback: number): number {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

/** topic → 前缀匹配的 key/value subject（`<topic>-key` / `<topic>-value`）。 */
export function matchingSchemaSubjects(topic: string, subjects: string[]): { key?: string; value?: string } {
  const clean = String(topic ?? "").trim();
  const result: { key?: string; value?: string } = {};
  if (!clean) return result;
  for (const subject of subjects) {
    if (subject === `${clean}-key`) result.key = subject;
    else if (subject === `${clean}-value`) result.value = subject;
  }
  return result;
}

const AVRO_WORDS = ["alpha", "beta", "gamma", "delta", "omega"];

function randomInt(rng: RandomSource, min: number, max: number): number {
  return min + Math.floor(rng() * (max - min + 1));
}

function randomString(rng: RandomSource): string {
  return `${AVRO_WORDS[Math.floor(rng() * AVRO_WORDS.length)]}-${randomInt(rng, 100, 999)}`;
}

function randomUuid(rng: RandomSource): string {
  const hex = "0123456789abcdef";
  let out = "";
  for (let index = 0; index < 36; index += 1) {
    if (index === 8 || index === 13 || index === 18 || index === 23) out += "-";
    else if (index === 14) out += "4";
    else out += hex[Math.floor(rng() * 16)];
  }
  return out;
}

function daysSinceEpoch(nowMs: number): number {
  return Math.floor(nowMs / 86400000);
}

/**
 * AVRO schema 文本 → 随机 JSON 值（F4 schema_random）。递归覆盖
 * record/array/map/union（非 null 首支）/enum/fixed/int/long/float/double/
 * boolean/string/bytes + 逻辑类型 date（days 数）/timestamp-millis（unix ms
 * 数）/uuid/decimal（数值，最优尽力）。解析失败/空 schema 返回 { error }。
 */
export function generateAvroRandom(schemaText: string, rng: RandomSource, nowMs: number = Date.now()): { value: string } | { error: string } {
  let parsed: unknown;
  try {
    parsed = JSON.parse(schemaText);
  } catch {
    return { error: "schema is not valid JSON" };
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return { error: "avro schema must be a JSON object" };
  }
  if ((parsed as Record<string, unknown>).type === undefined) {
    return { error: "avro schema is missing \"type\"" };
  }
  try {
    // 顶层直接传整个 schema 节点（{type:"record",…} / {type:"long",logicalType:…}）。
    return { value: JSON.stringify(generateAvroValue(parsed, rng, nowMs, 0)) };
  } catch (cause) {
    return { error: cause instanceof Error ? cause.message : String(cause) };
  }
}

const AVRO_GENERATION_DEPTH_MAX = 16;

function generateAvroValue(raw: unknown, rng: RandomSource, nowMs: number, depth: number): unknown {
  if (depth > AVRO_GENERATION_DEPTH_MAX) throw new Error("avro schema nesting too deep");
  // union：非 null 首支（契约：union（非 null 首支））。
  if (Array.isArray(raw)) {
    const branch = raw.find((entry) => entry !== "null" && !(typeof entry === "object" && entry !== null && (entry as Record<string, unknown>).type === "null"));
    if (branch === undefined) return null;
    return generateAvroValue(branch, rng, nowMs, depth + 1);
  }
  if (typeof raw === "string") {
    return generateAvroPrimitive(raw, rng, nowMs);
  }
  if (typeof raw !== "object" || raw === null) {
    throw new Error(`unsupported avro type: ${String(raw)}`);
  }
  const node = raw as Record<string, unknown>;
  const logical = typeof node.logicalType === "string" ? node.logicalType : "";
  const type = node.type;
  if (typeof type === "string") {
    if (logical) return generateAvroLogical(logical, rng, nowMs, node);
    switch (type) {
      case "record":
      case "error": {
        const fields = Array.isArray(node.fields) ? node.fields : [];
        const record: Record<string, unknown> = {};
        for (const field of fields) {
          const entry = field as Record<string, unknown>;
          record[String(entry.name ?? "?")] = generateAvroValue(entry.type, rng, nowMs, depth + 1);
        }
        return record;
      }
      case "enum": {
        const symbols = Array.isArray(node.symbols) ? node.symbols.map(String) : [];
        if (symbols.length === 0) throw new Error("avro enum has no symbols");
        return symbols[Math.floor(rng() * symbols.length)];
      }
      case "array": {
        return Array.from({ length: randomInt(rng, 1, 3) }, () => generateAvroValue(node.items, rng, nowMs, depth + 1));
      }
      case "map": {
        const map: Record<string, unknown> = {};
        const count = randomInt(rng, 1, 3);
        for (let index = 0; index < count; index += 1) {
          map[`k${index}`] = generateAvroValue(node.values, rng, nowMs, depth + 1);
        }
        return map;
      }
      case "fixed": {
        const size = Number(node.size ?? 0);
        let out = "";
        for (let index = 0; index < Math.max(1, Math.min(size, 64)); index += 1) out += String(randomInt(rng, 0, 9));
        return out;
      }
      default:
        return generateAvroPrimitive(type, rng, nowMs);
    }
  }
  if (Array.isArray(type) || typeof type === "object") {
    return generateAvroValue(type, rng, nowMs, depth + 1);
  }
  throw new Error(`unsupported avro type: ${String(type)}`);
}

function generateAvroPrimitive(type: string, rng: RandomSource, nowMs: number): unknown {
  switch (type) {
    case "null":
      return null;
    case "boolean":
      return rng() < 0.5;
    case "int":
      return randomInt(rng, 0, 999);
    case "long":
      return randomInt(rng, 0, 99999);
    case "float":
    case "double":
      return Math.round(rng() * 10000) / 100;
    case "bytes":
      return String(randomInt(rng, 1000, 9999));
    case "string":
      return randomString(rng);
    default:
      throw new Error(`unsupported avro type: ${type}`);
  }
}

function generateAvroLogical(logical: string, rng: RandomSource, nowMs: number, node: Record<string, unknown>): unknown {
  switch (logical) {
    case "date":
      return daysSinceEpoch(nowMs);
    case "timestamp-millis":
      return nowMs;
    case "timestamp-micros":
      return nowMs * 1000;
    case "time-millis":
      return nowMs % 86400000;
    case "time-micros":
      return (nowMs % 86400000) * 1000;
    case "uuid":
      return randomUuid(rng);
    case "decimal": {
      const scale = Number(node.scale ?? 0);
      const base = randomInt(rng, 1, 99999);
      return scale > 0 ? Math.round(base / Math.pow(10, scale) * Math.pow(10, scale)) / Math.pow(10, scale) : base;
    }
    default:
      // 未知逻辑类型按底层基础类型生成。
      return generateAvroValue(node.type, rng, nowMs, 1);
  }
}

// -- Flow 模板占位符展开（F4 template）----------------------------------------------

const TEMPLATE_INT_PATTERN = /\{int:(-?\d+),(-?\d+)\}/g;
const TEMPLATE_FLOAT_PATTERN = /\{float:(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)\}/g;
const TEMPLATE_PICK_PATTERN = /\{pick:([^}]*)\}/g;

/**
 * JSON 模板占位符展开：{uuid} {now} {int:min,max} {float:min,max} {pick:a|b|c}。
 * nowMs 可注入（spec 固定向量）；未知占位符原样保留。
 */
export function expandTemplate(text: string, rng: RandomSource, nowMs: number = Date.now()): string {
  let out = String(text ?? "");
  out = out.replace(/\{uuid\}/g, () => randomUuid(rng));
  out = out.replace(/\{now\}/g, () => new Date(nowMs).toISOString());
  out = out.replace(TEMPLATE_INT_PATTERN, (_match, min: string, max: string) => String(randomInt(rng, Number(min), Number(max))));
  out = out.replace(TEMPLATE_FLOAT_PATTERN, (_match, min: string, max: string) => {
    const low = Number(min);
    const high = Number(max);
    const value = low + rng() * (high - low);
    return String(Math.round(value * 100) / 100);
  });
  out = out.replace(TEMPLATE_PICK_PATTERN, (_match, choices: string) => {
    const parts = choices.split("|").filter((part) => part.length > 0);
    return parts.length === 0 ? _match : parts[Math.floor(rng() * parts.length)];
  });
  return out;
}
