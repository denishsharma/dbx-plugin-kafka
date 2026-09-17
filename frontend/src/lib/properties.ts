/**
 * Confluent properties 纯函数（自 kafkaModel 拆分）：`key=value` 文本解析、
 * → manifest 连接表单字段映射、导入助手只读键值映射（敏感值掩码）。
 */

export interface ConfluentProperties {
  [key: string]: string;
}

/**
 * Confluent properties 文本解析（`key=value` 行，# / ! 注释，`\` 续行，
 * 值内 `\:=` 等反斜杠转义）。纯解析，不做语义校验。
 */
export function parsePropertiesText(text: string): ConfluentProperties {
  const result: ConfluentProperties = {};
  // 续行：行尾奇数个反斜杠才生效（偶数个是转义的字面反斜杠）。
  const logical: string[] = [];
  let pending = "";
  for (const rawLine of text.split(/\r?\n/)) {
    const line = pending + rawLine;
    const trailing = line.match(/\\+$/)?.[0].length ?? 0;
    if (trailing % 2 === 1) {
      pending = line.slice(0, -1);
      continue;
    }
    pending = "";
    logical.push(line);
  }
  if (pending) logical.push(pending);
  for (const line of logical) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#") || trimmed.startsWith("!")) continue;
    // 第一个未被转义的 = 或 : 为分隔符（\ 后跳过一个字符）。
    let separatorIndex = -1;
    for (let index = 0; index < trimmed.length; index += 1) {
      const character = trimmed[index];
      if (character === "\\") {
        index += 1;
        continue;
      }
      if (character === "=" || character === ":") {
        separatorIndex = index;
        break;
      }
    }
    if (separatorIndex <= 0) continue;
    const key = unescapeProperties(trimmed.slice(0, separatorIndex)).trim();
    const value = unescapeProperties(trimmed.slice(separatorIndex + 1).replace(/^[ \t]+/, "")).trimEnd();
    if (key) result[key] = value;
  }
  return result;
}

function unescapeProperties(input: string): string {
  let output = "";
  for (let index = 0; index < input.length; index += 1) {
    const character = input[index];
    if (character === "\\" && index + 1 < input.length) {
      output += input[index + 1];
      index += 1;
    } else {
      output += character;
    }
  }
  return output;
}

export interface ConnectionFormHints {
  bootstrapServers: string;
  securityProtocol: string;
  saslMechanism: string;
  saslUsername: string;
  saslPassword: string;
  tlsInsecureSkipVerify: boolean;
}

/**
 * Confluent properties → manifest 连接表单字段映射（§4）：
 * bootstrap.servers / security.protocol / sasl.mechanism /
 * sasl.jaas.config（提取 username/password）/ ssl.endpoint.identification.algorithm。
 * 跳过校验仅在 key 显式置空或 none 时为 true；key 缺省保持宿主默认校验。
 */
export function propertiesToConnectionForm(properties: ConfluentProperties): ConnectionFormHints {
  const bootstrap = properties["bootstrap.servers"] ?? "";
  const securityProtocol = (properties["security.protocol"] ?? "").toUpperCase();
  const saslMechanism = (properties["sasl.mechanism"] ?? "").toUpperCase();
  const jaas = properties["sasl.jaas.config"] ?? "";
  const username = jaas.match(/(?:^|\s)username\s*=\s*"([^"]*)"/)?.[1] ?? "";
  const password = jaas.match(/(?:^|\s)password\s*=\s*"([^"]*)"/)?.[1] ?? "";
  const endpointAlgorithm = properties["ssl.endpoint.identification.algorithm"];
  const skipVerify = endpointAlgorithm !== undefined && /^(|none)$/i.test(endpointAlgorithm.trim());
  return {
    bootstrapServers: bootstrap,
    securityProtocol,
    saslMechanism,
    saslUsername: username,
    saslPassword: password,
    tlsInsecureSkipVerify: skipVerify,
  };
}

// -- Confluent properties 导入助手（Phase 2，只读映射展示，不回填不持久化）---------

export const PROPERTY_MASKED_PLACEHOLDER = "••••••";

export interface PropertyMappingRow {
  /** properties 原始键（或带提取标注的子键）。 */
  property: string;
  /** 解析出的值；敏感值以掩码占位，不携带明文。 */
  value: string;
  masked: boolean;
  /** 对应宿主连接表单字段（manifest 连接字段名）。 */
  formField: string;
}

/**
 * Confluent properties → 只读键值映射（Phase 2 导入助手展示用）：
 * bootstrap.servers / security.protocol / sasl.mechanism（含 GSSAPI 提示）/
 * sasl.jaas.config（username/password/principal/keyTab 提取）/
 * schema.registry.url / schema.registry.basic.auth.user.info。
 * 敏感值（密码/secret）一律以掩码占位；纯函数，不触碰 localStorage。
 */
export function buildPropertyMappings(properties: ConfluentProperties): PropertyMappingRow[] {
  const rows: PropertyMappingRow[] = [];
  const push = (property: string, value: string, formField: string, masked = false) => {
    if (value) rows.push({ property, value, masked, formField });
  };
  push("bootstrap.servers", properties["bootstrap.servers"] ?? "", "bootstrap_servers");
  push("security.protocol", (properties["security.protocol"] ?? "").toUpperCase(), "security_protocol");
  const mechanism = (properties["sasl.mechanism"] ?? "").toUpperCase();
  if (mechanism) {
    push("sasl.mechanism", mechanism, mechanism === "GSSAPI" ? "sasl_mechanism (GSSAPI) + kerberos_*" : "sasl_mechanism");
  }
  const jaas = properties["sasl.jaas.config"] ?? "";
  if (jaas) {
    const username = jaas.match(/(?:^|\s)username\s*=\s*"([^"]*)"/)?.[1] ?? "";
    const password = jaas.match(/(?:^|\s)password\s*=\s*"([^"]*)"/)?.[1] ?? "";
    const principal = jaas.match(/(?:^|\s)principal\s*=\s*"([^"]*)"/)?.[1] ?? "";
    const keytab = jaas.match(/(?:^|\s)keyTab\s*=\s*"([^"]*)"/)?.[1] ?? "";
    if (username) push("sasl.jaas.config → username", username, "sasl_username");
    if (password) push("sasl.jaas.config → password", PROPERTY_MASKED_PLACEHOLDER, "sasl_password", true);
    if (principal) push("sasl.jaas.config → principal", principal, "kerberos_principal");
    if (keytab) push("sasl.jaas.config → keyTab", keytab, "kerberos_keytab_path");
  }
  push("sasl.kerberos.service.name", properties["sasl.kerberos.service.name"] ?? "", "kerberos_service_name");
  push("schema.registry.url", properties["schema.registry.url"] ?? "", "sr_url");
  const srAuth = properties["schema.registry.basic.auth.user.info"] ?? "";
  if (srAuth) {
    const separatorIndex = srAuth.indexOf(":");
    if (separatorIndex > 0) {
      push("schema.registry.basic.auth.user.info → user", srAuth.slice(0, separatorIndex), "sr_username");
      push("schema.registry.basic.auth.user.info → secret", PROPERTY_MASKED_PLACEHOLDER, "sr_password", true);
    }
  }
  return rows;
}
