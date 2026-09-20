/**
 * 文件导出保存（收敛三处组件内重复的 downloadText）：优先走宿主
 * `dbxPlugin.saveFile` 桥（桌面端 v0.6.14+：原生另存为对话框 + 宿主写盘；
 * 沙箱 iframe 里的 blob 锚点下载会被 WKWebView 取消），旧宿主/dev host
 * 无此桥（或宿主侧未实现）时回落 Blob URL + anchor 网页下载。
 */

/** saveTextFile 结果：save-as = 宿主另存为已写盘；download = 网页下载已触发；canceled = 用户取消另存为对话框。 */
export type SaveFileOutcome =
  | { mode: "save-as"; path: string }
  | { mode: "download" }
  | { mode: "canceled" };

/** 文本 → UTF-8 字节：走桥的二进制 transfer 通道（512MiB 上限），避开 base64 参数 2MiB 桥载荷上限。 */
function encodeUtf8(text: string): Uint8Array {
  return new TextEncoder().encode(text);
}

/**
 * 保存文本文件：宿主桥可用则另存为（取消返回 canceled，调用方决定是否提示）；
 * 桥缺失或宿主侧报「不可用」时回落网页下载（浏览器/dev host 语义）。
 */
export async function saveTextFile(fileName: string, contentType: string, text: string): Promise<SaveFileOutcome> {
  const api = window.dbxPlugin;
  if (typeof api?.saveFile === "function") {
    try {
      const result = await api.saveFile({ fileName, contentType }, encodeUtf8(text));
      return result?.path ? { mode: "save-as", path: result.path } : { mode: "canceled" };
    } catch {
      // 宿主未实现保存桥（如 Web 宿主）→ 回落网页下载，导出不应因此中断。
    }
  }
  downloadViaAnchor(fileName, contentType, text);
  return { mode: "download" };
}

/** 网页下载兜底：Blob URL + 隐藏 anchor click（宿主沙箱外 / 旧 Host API 1.0 行为）。 */
function downloadViaAnchor(name: string, contentType: string, text: string): void {
  const blob = new Blob([text], { type: `${contentType};charset=utf-8` });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = name;
  anchor.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 10_000);
}
