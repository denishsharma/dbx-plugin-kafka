/**
 * JSON 文本工具（自 kafkaModel 拆分）：headers JSON 对象校验与跨引擎一致的
 * jsonErrorLine（编辑器 linter 用，V8 新版 JSON.parse 报错文案不可靠）。
 */

/** headers 编辑框 JSON 对象校验（值必须是 string；其余键值报错）。 */
export function parseHeadersJson(text: string): { headers: Record<string, string> } | { error: string } {
  const trimmed = text.trim();
  if (!trimmed) return { headers: {} };
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch (cause) {
    return { error: cause instanceof Error ? cause.message : String(cause) };
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return { error: "headers must be a JSON object" };
  }
  const headers: Record<string, string> = {};
  for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
    if (typeof value !== "string") return { error: `header "${key}" must be a string` };
    headers[key] = value;
  }
  return { headers };
}

/**
 * 定位 JSON 首个语法错误所在行（1-based）。合法 JSON、空/纯空白文本返回 null
 * （空态不算错——编辑器 linter 语义）。不解析 JSON.parse 的报错文案：V8 新版
 * 对不少输入不再携带 position、WebKit/Firefox 格式各异，改用内置最小 JSON
 * 扫描器，跨引擎（Electron/Chromium、CI Node）行为一致。
 */
export function jsonErrorLine(text: string): number | null {
  const src = String(text ?? "");
  if (!src.trim()) return null;
  const length = src.length;
  let pos = 0;
  let line = 1;

  function skipWs(): void {
    while (pos < length) {
      const ch = src[pos];
      if (ch === "\n") {
        pos += 1;
        line += 1;
      } else if (ch === " " || ch === "\t" || ch === "\r") {
        pos += 1;
      } else {
        break;
      }
    }
  }

  function matchKeyword(word: string): boolean {
    if (src.startsWith(word, pos)) {
      pos += word.length;
      return true;
    }
    return false;
  }

  function scanString(): boolean {
    pos += 1; // 开引号
    while (pos < length) {
      const ch = src[pos];
      if (ch === '"') {
        pos += 1;
        return true;
      }
      if (ch === "\\") {
        const esc = src[pos + 1];
        if (esc === "u") {
          if (!/^[0-9a-fA-F]{4}$/.test(src.slice(pos + 2, pos + 6))) return false;
          pos += 6;
          continue;
        }
        if (esc === undefined || !"\"\\/bfnrt".includes(esc)) return false;
        pos += 2;
        continue;
      }
      // JSON 字符串内不允许字面控制字符（含裸换行），在此处报错。
      if (ch < " ") return false;
      pos += 1;
    }
    return false; // 未闭合
  }

  function scanNumber(): boolean {
    if (src[pos] === "-") pos += 1;
    if (src[pos] === "0") {
      pos += 1;
    } else if (src[pos]! >= "1" && src[pos]! <= "9") {
      while (pos < length && src[pos] >= "0" && src[pos] <= "9") pos += 1;
    } else {
      return false;
    }
    if (src[pos] === ".") {
      pos += 1;
      if (!(pos < length && src[pos] >= "0" && src[pos] <= "9")) return false;
      while (pos < length && src[pos] >= "0" && src[pos] <= "9") pos += 1;
    }
    if (src[pos] === "e" || src[pos] === "E") {
      pos += 1;
      if (src[pos] === "+" || src[pos] === "-") pos += 1;
      if (!(pos < length && src[pos] >= "0" && src[pos] <= "9")) return false;
      while (pos < length && src[pos] >= "0" && src[pos] <= "9") pos += 1;
    }
    return true;
  }

  function scanObject(): boolean {
    pos += 1; // {
    skipWs();
    if (src[pos] === "}") {
      pos += 1;
      return true;
    }
    for (;;) {
      skipWs();
      if (src[pos] !== '"') return false;
      if (!scanString()) return false;
      skipWs();
      if (src[pos] !== ":") return false;
      pos += 1;
      if (!scanValue()) return false;
      skipWs();
      if (src[pos] === ",") {
        pos += 1;
        continue;
      }
      if (src[pos] === "}") {
        pos += 1;
        return true;
      }
      return false;
    }
  }

  function scanArray(): boolean {
    pos += 1; // [
    skipWs();
    if (src[pos] === "]") {
      pos += 1;
      return true;
    }
    for (;;) {
      if (!scanValue()) return false;
      skipWs();
      if (src[pos] === ",") {
        pos += 1;
        continue;
      }
      if (src[pos] === "]") {
        pos += 1;
        return true;
      }
      return false;
    }
  }

  function scanValue(): boolean {
    skipWs();
    if (pos >= length) return false;
    const ch = src[pos];
    if (ch === "{") return scanObject();
    if (ch === "[") return scanArray();
    if (ch === '"') return scanString();
    if (ch === "-" || (ch >= "0" && ch <= "9")) return scanNumber();
    return matchKeyword("true") || matchKeyword("false") || matchKeyword("null");
  }

  if (!scanValue()) return line;
  skipWs();
  return pos < length ? line : null; // 尾部还有非空白内容 = 多余 token
}
