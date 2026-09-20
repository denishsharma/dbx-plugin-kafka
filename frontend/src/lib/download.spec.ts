// @vitest-environment happy-dom
// download 单测：导出保存的双通道（宿主 saveFile 另存为优先 / Blob 网页下载兜底）
// 与取消语义（用户取消另存为对话框不触发下载、返回 canceled）。
import { beforeEach, describe, expect, it, vi } from "vitest";
import { saveTextFile, type SaveFileOutcome } from "./download";

type SaveFileMock = (options: { fileName?: string; contentType?: string }, data: Uint8Array | ArrayBuffer) => Promise<{ path: string } | null>;

function setBridge(saveFile?: SaveFileMock) {
  (window as unknown as { dbxPlugin: unknown }).dbxPlugin = saveFile ? { saveFile } : {};
}

describe("saveTextFile", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("prefers the host saveFile bridge with UTF-8 bytes", async () => {
    const saveFile = vi.fn<SaveFileMock>().mockResolvedValue({ path: "/tmp/kafka-messages.json" });
    setBridge(saveFile);
    const outcome: SaveFileOutcome = await saveTextFile("kafka-messages.json", "application/json", '{"a":"中"}');
    expect(saveFile).toHaveBeenCalledTimes(1);
    const [options, data] = saveFile.mock.calls[0]!;
    expect(options).toEqual({ fileName: "kafka-messages.json", contentType: "application/json" });
    expect(new TextDecoder().decode(data as Uint8Array)).toBe('{"a":"中"}');
    expect(outcome).toEqual({ mode: "save-as", path: "/tmp/kafka-messages.json" });
  });

  it("reports canceled without anchor download when the user dismisses the save dialog", async () => {
    const createObjectURL = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:mock");
    const anchorClick = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    setBridge(vi.fn<SaveFileMock>().mockResolvedValue(null));
    const outcome = await saveTextFile("a.csv", "text/csv", "a,b");
    expect(outcome).toEqual({ mode: "canceled" });
    expect(createObjectURL).not.toHaveBeenCalled();
    expect(anchorClick).not.toHaveBeenCalled();
  });

  it("falls back to anchor download on old hosts without the saveFile bridge", async () => {
    const createObjectURL = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:mock");
    const anchorClick = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    setBridge(undefined);
    const outcome = await saveTextFile("kafka-stream.tsv", "text/tab-separated-values", "a\tb");
    expect(outcome).toEqual({ mode: "download" });
    const blob = createObjectURL.mock.calls[0]![0] as Blob;
    expect(blob.type).toBe("text/tab-separated-values;charset=utf-8");
    expect(await blob.text()).toBe("a\tb");
    expect(anchorClick).toHaveBeenCalledTimes(1);
  });

  it("falls back to anchor download when the host rejects the save bridge", async () => {
    const createObjectURL = vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:mock");
    const anchorClick = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    setBridge(vi.fn<SaveFileMock>().mockRejectedValue(new Error("Host file saving is unavailable")));
    const outcome = await saveTextFile("value.txt", "text/plain", "body");
    expect(outcome).toEqual({ mode: "download" });
    expect(anchorClick).toHaveBeenCalledTimes(1);
  });
});
