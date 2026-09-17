/**
 * OAUTHBEARER 连接表单联动纯函数（自 kafkaModel 拆分；冻结契约 §12.2.3，
 * json camelCase）：镜像 manifest visible_when 联动链，供表单/spec 共用。
 */

export type OauthTokenSource = "msk_iam" | "static_token";

/** OAUTHBEARER 各字段组可见性（镜像 manifest visible_when 联动链，供 spec/摘要共用）。 */
export interface OauthFormVisibility {
  /** oauth_token_source：security_protocol 含 SASL 且 mechanism=OAUTHBEARER。 */
  oauthTokenSource: boolean;
  /** msk_* 组：token source = msk_iam。 */
  mskFields: boolean;
  /** oauth_static_token：token source = static_token。 */
  staticToken: boolean;
}

const SASL_PROTOCOLS = new Set(["SASL_PLAINTEXT", "SASL_SSL"]);

function normalizeTokenSource(source: string): OauthTokenSource | "" {
  const normalized = String(source ?? "").trim().toLowerCase();
  return normalized === "msk_iam" || normalized === "static_token" ? normalized : "";
}

/** security_protocol × sasl_mechanism × oauth_token_source → 字段组可见性。 */
export function oauthFormVisibility(securityProtocol: string, saslMechanism: string, oauthTokenSource: string): OauthFormVisibility {
  const sasl = SASL_PROTOCOLS.has(String(securityProtocol ?? "").trim().toUpperCase());
  const oauth = String(saslMechanism ?? "").trim().toUpperCase() === "OAUTHBEARER";
  if (!sasl || !oauth) return { oauthTokenSource: false, mskFields: false, staticToken: false };
  const source = normalizeTokenSource(oauthTokenSource);
  return { oauthTokenSource: true, mskFields: source === "msk_iam", staticToken: source === "static_token" };
}

/** OAUTHBEARER 要求 SASL_SSL（否则后端 -32602）：表单 hint 用。 */
export function oauthRequiresSaslSsl(securityProtocol: string, saslMechanism: string): boolean {
  const protocol = String(securityProtocol ?? "").trim().toUpperCase();
  return String(saslMechanism ?? "").trim().toUpperCase() === "OAUTHBEARER" && protocol !== "SASL_SSL";
}
