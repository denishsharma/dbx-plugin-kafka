/**
 * Schema 模板与详情树（自 kafkaModel 拆分）：注册弹窗三段静态模板常量
 * （代码非 i18n）+ AVRO / JSON Schema 树模型（F5）。
 */

/** 注册弹窗「插入模板」三段静态模板（代码常量，不进 i18n）。 */
export const SCHEMA_TEMPLATE_AVRO = `{
  "type": "record",
  "name": "DemoRecord",
  "fields": [
    { "name": "id", "type": "string" },
    { "name": "amount", "type": "double" },
    { "name": "quantity", "type": "int" },
    { "name": "status", "type": { "type": "enum", "name": "Status", "symbols": ["NEW", "PAID", "CANCELLED"] } }
  ]
}`;

export const SCHEMA_TEMPLATE_JSON = `{
  "type": "object",
  "properties": {
    "id": { "type": "string" },
    "amount": { "type": "number" },
    "quantity": { "type": "integer" },
    "status": { "type": "string", "enum": ["NEW", "PAID", "CANCELLED"] }
  },
  "required": ["id"]
}`;

export const SCHEMA_TEMPLATE_PROTOBUF = `syntax = "proto3";

package demo.v1;

message DemoMessage {
  string id = 1;
  double amount = 2;
  int32 quantity = 3;
  string status = 4;
}`;

export type SchemaTemplateFormat = "avro" | "json" | "protobuf";

export function schemaTemplateFor(format: SchemaTemplateFormat): string {
  if (format === "json") return SCHEMA_TEMPLATE_JSON;
  if (format === "protobuf") return SCHEMA_TEMPLATE_PROTOBUF;
  return SCHEMA_TEMPLATE_AVRO;
}

/** schema 树节点（详情区 树/文本 toggle 的递归渲染模型）。 */
export interface SchemaTreeNode {
  /** 字段名 / 分支标注（items、values 等）。 */
  name: string;
  /** 类型标注（record/string/union:…/logicalType 标尾等）。 */
  type: string;
  /** Avro default / JSON Schema default（有才显）。 */
  defaultValue?: string;
  children?: SchemaTreeNode[];
}

/**
 * AVRO / JSON Schema 文本 → 树模型（F5 树视图）。PROTOBUF 与解析失败返回 null
 * （调用方保持文本 + 行内提示）。递归覆盖 record/array/map/union/enum/fixed
 * 与 JSON Schema object/array/enum/default。
 */
export function buildSchemaTree(schemaText: string): SchemaTreeNode | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(schemaText);
  } catch {
    return null;
  }
  return buildSchemaTreeNode("root", parsed, 0);
}

const SCHEMA_TREE_DEPTH_MAX = 16;

function nodeTypeLabel(node: Record<string, unknown>, fallback: string): string {
  const logical = typeof node.logicalType === "string" ? ` (${node.logicalType})` : "";
  return `${fallback}${logical}`;
}

function buildSchemaTreeNode(name: string, raw: unknown, depth: number): SchemaTreeNode | null {
  if (depth > SCHEMA_TREE_DEPTH_MAX) return null;
  // union：取每个分支为子节点（union:第一非 null 分支标注）
  if (Array.isArray(raw)) {
    const children = raw
      .map((branch, index) => buildSchemaTreeNode(`[${index}]`, branch, depth + 1))
      .filter((branch): branch is SchemaTreeNode => branch !== null);
    const first = raw.find((branch) => branch !== "null" && !(typeof branch === "object" && branch !== null && (branch as Record<string, unknown>).type === "null"));
    return { name, type: `union:${avroTypeLabel(first)}`, children };
  }
  if (typeof raw === "string") return { name, type: raw };
  if (typeof raw !== "object" || raw === null) return { name, type: String(raw) };
  const node = raw as Record<string, unknown>;
  const type = node.type;
  const nodeDefault = "default" in node && node.default !== undefined ? stringifyTreeDefault(node.default) : undefined;

  if (typeof type === "string") {
    const label = nodeTypeLabel(node, type);
    if (type === "record" || type === "error") {
      const fields = Array.isArray(node.fields) ? node.fields : [];
      const children = fields
        .map((field) => {
          const entry = field as Record<string, unknown>;
          const child = buildSchemaTreeNode(String(entry.name ?? "?"), entry, depth + 1);
          return child;
        })
        .filter((child): child is SchemaTreeNode => child !== null);
      return { name, type: label, children, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    if (type === "enum") {
      const symbols = Array.isArray(node.symbols) ? node.symbols.map(String) : [];
      return { name, type: symbols.length > 0 ? `enum[${symbols.join("|")}]` : "enum", ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    if (type === "array") {
      const child = buildSchemaTreeNode("items", node.items, depth + 1);
      return { name, type: label, children: child ? [child] : [], ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    if (type === "map") {
      const child = buildSchemaTreeNode("values", node.values, depth + 1);
      return { name, type: label, children: child ? [child] : [], ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    if (type === "fixed") {
      return { name, type: `${label}[${String(node.size ?? "?")}]`, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    if (type === "object") {
      // JSON Schema object：properties → 子节点（required 加 * 标记）。
      const children = propertiesChildren(node, depth);
      if (children) {
        return { name, type: label, children, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
      }
      return { name, type: label, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    // 字段包装（{name, type, default}）与基础类型的内嵌逻辑类型。
    if (node.name !== undefined || node.logicalType !== undefined) {
      return { name: typeof node.name === "string" ? node.name : name, type: label, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
    return { name, type: label, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
  }
  if (Array.isArray(type)) {
    const union = buildSchemaTreeNode(name, type, depth + 1);
    return union ? { ...union, name, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) } : { name, type: "union" };
  }
  if (typeof type === "object" && type !== null) {
    const inner = buildSchemaTreeNode(name, type, depth + 1);
    if (inner) return { ...inner, name, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
  }
  // JSON Schema 形状（type 在子级 / properties / items / enum）。
  const properties = typeof node.properties === "object" && node.properties !== null ? (node.properties as Record<string, unknown>) : null;
  if (properties) {
    const children = propertiesChildren(node, depth);
    if (children) {
      return { name, type: typeof node.type === "string" ? node.type : "object", children, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
    }
  }
  if ("items" in node) {
    const child = buildSchemaTreeNode("items", node.items, depth + 1);
    return { name, type: typeof node.type === "string" ? node.type : "array", children: child ? [child] : [], ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
  }
  if (Array.isArray(node.enum)) {
    const symbols = node.enum.map(String);
    return { name, type: `enum[${symbols.join("|")}]`, ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
  }
  return { name, type: typeof node.type === "string" ? node.type : typeof node.type === "object" ? "object" : String(node.type ?? "?"), ...(nodeDefault !== undefined ? { defaultValue: nodeDefault } : {}) };
}

function avroTypeLabel(raw: unknown): string {
  if (raw === undefined) return "null";
  if (typeof raw === "string") return raw;
  if (typeof raw === "object" && raw !== null) {
    const node = raw as Record<string, unknown>;
    const base = typeof node.type === "string" ? node.type : "record";
    return typeof node.logicalType === "string" ? `${base}(${node.logicalType})` : base;
  }
  return String(raw);
}

/** JSON Schema properties → 子节点（required 键名加 * 标记）；无 properties 返回 null。 */
function propertiesChildren(node: Record<string, unknown>, depth: number): SchemaTreeNode[] | null {
  const properties = typeof node.properties === "object" && node.properties !== null ? (node.properties as Record<string, unknown>) : null;
  if (!properties) return null;
  const required = new Set(Array.isArray(node.required) ? node.required.map(String) : []);
  return Object.entries(properties)
    .map(([key, value]) => {
      const child = buildSchemaTreeNode(key, value, depth + 1);
      if (child && required.has(key)) child.type = `${child.type} *`;
      return child;
    })
    .filter((child): child is SchemaTreeNode => child !== null);
}

function stringifyTreeDefault(value: unknown): string {
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
}
