# 使用指南

## 图形界面

双击 [`启动.cmd`](../启动.cmd)。页面由本机回环地址提供，重复启动会打开已有会话。

- **本地图片文件夹**。选择原始目标文件和候选文件夹。工具递归检查子目录并跳过目标文件本身。支持 JPG、PNG、WebP、BMP、TIFF 和 GIF，动图仅读取第一帧。
- **先试一次本机模拟**。使用合成图和本机 HTTP 接口，演示匹配、缺图、坏图和服务错误。
- **编号范围**。配置自己拥有或有权使用的图片接口，填写包含两端的起止编号。界面提供并发数和请求速率设置，按服务方允许的额度配置。未改动接口模板时，范围请求发往界面显示的默认来源。

编号范围任务运行中，已保存的强匹配会显示在候选列表，供提前复核。扫描结束后可查看最终排序的前 20 项和对照报告。

通过 **停止并保存** 结束当前运行。在 **扫描记录** 中可查看已处理、成功和失败的区间或编号，载入原任务后再次点开始继续。关掉标签页后工作仍会继续。结束后台进程请使用 **退出工具 → 保存并退出**。

## 命令行演示

在两个 PowerShell 窗口中进入仓库目录，并按 [快速开始](../README.md#快速开始) 安装依赖。第一个窗口启动合成接口，等待终端显示地址：

```powershell
.\.venv\Scripts\python.exe -m avatar_compare.mock_service --port 8765
```

在第二个窗口运行：

```powershell
.\.venv\Scripts\python.exe -m avatar_compare --target .\outputs\mock-fixtures\target.png --start 1001 --end 1020 --endpoint 'http://127.0.0.1:8765/avatars/{id}' --source-revision fixture-v1 --rate 20 --state-dir .\outputs\mock-state --output .\outputs\mock-report
```

模拟任务有意包含失败编号。命令会生成报告，并以状态码 `1` 表示部分编号失败。演示结束后，在第一个窗口按 `Ctrl+C` 停止模拟服务。

真实接口需接收 GET 请求并直接返回图片字节。模板含一个 `{id}`，可使用 HTTPS 或本机 HTTP。如果接口需要 Bearer 令牌，用 `--token-env AVATAR_TOKEN` 传入环境变量名，并在运行环境中设置实际令牌。完整参数可用 `python -m avatar_compare --help` 查看。

## 恢复和导出

- **省空间模式**默认使用 `.state-compact`，保留匹配候选、最多 100 项复核样本、处理区间和错误。其余图片与完整分数会清理，换目标通常需要重新请求图片。
- **完整保留和本地模式**默认使用 `.state`，保留更完整的结果供恢复和导出。CLI 自定义 `--state-dir` 后，恢复与导出应使用同一路径。
- **界面数据**位于 `.ui`，包括用于恢复的目标图片副本和界面日志。报告默认写入 `outputs`。扫描记录中的 **含保留原图** 只导出任务库仍保有的图片。

相同目标、来源和缓存批次等条件满足时会复用已有记录。CLI 用 `--retry-errors` 补扫失败项，用 `--refresh-source` 建立新批次重新获取。实际接口的图片可能随时间变化。

完整保留的范围任务也可通过 CLI 导出。将 `RUN_ID` 替换为报告 JSON 中的 `run_id`：

```powershell
.\.venv\Scripts\python.exe -m avatar_compare.ledger --run-id RUN_ID --output .\outputs\full-records
```
