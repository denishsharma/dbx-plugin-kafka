// oauthForm 纯函数单测（F2）：OAUTHBEARER 表单字段组可见性联动（冻结契约 §12.2.3）。
import { describe, expect, it } from "vitest";
import { oauthFormVisibility, oauthRequiresSaslSsl } from "./oauthForm";

describe("oauth form visibility (F2 frozen field chain)", () => {
  it("reveals oauth_token_source only for SASL protocol + OAUTHBEARER", () => {
    expect(oauthFormVisibility("SASL_SSL", "OAUTHBEARER", "")).toEqual({ oauthTokenSource: true, mskFields: false, staticToken: false });
    expect(oauthFormVisibility("PLAINTEXT", "OAUTHBEARER", "msk_iam")).toEqual({ oauthTokenSource: false, mskFields: false, staticToken: false });
    expect(oauthFormVisibility("SASL_SSL", "SCRAM-SHA-512", "msk_iam")).toEqual({ oauthTokenSource: false, mskFields: false, staticToken: false });
  });

  it("branches msk_* group vs oauth_static_token by token source", () => {
    expect(oauthFormVisibility("SASL_SSL", "OAUTHBEARER", "msk_iam")).toEqual({ oauthTokenSource: true, mskFields: true, staticToken: false });
    expect(oauthFormVisibility("SASL_PLAINTEXT", "OAUTHBEARER", "static_token")).toEqual({ oauthTokenSource: true, mskFields: false, staticToken: true });
    // 未选/非法来源：两组都不可见，仅 token source 选择器可见。
    expect(oauthFormVisibility("SASL_SSL", "OAUTHBEARER", "bogus")).toEqual({ oauthTokenSource: true, mskFields: false, staticToken: false });
  });

  it("requires SASL_SSL for OAUTHBEARER (backend -32602 guard)", () => {
    expect(oauthRequiresSaslSsl("SASL_SSL", "OAUTHBEARER")).toBe(false);
    expect(oauthRequiresSaslSsl("SASL_PLAINTEXT", "OAUTHBEARER")).toBe(true);
    expect(oauthRequiresSaslSsl("SASL_SSL", "PLAIN")).toBe(false);
  });
});
