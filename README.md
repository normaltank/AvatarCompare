# AvatarCompare

在本机查找图片副本，并查看支持每项匹配的证据。输入一张目标图，从本地文件夹或具有访问授权的 HTTP 图片源中找出候选，打开报告查看并排图片和对应点。

[快速开始](#快速开始) · [使用指南](docs/usage.md) · [匹配原理](docs/architecture.md) · [全部文档](docs/) · [参与贡献](CONTRIBUTING.md) · [更新记录](CHANGELOG.md)

## 快速开始

1. 下载或克隆本仓库，安装 Python 3.12，并确保终端能运行 `python`。
2. 在 Windows 上双击 [`启动.cmd`](启动.cmd)。首次启动会安装依赖，随后在浏览器打开本机界面。
3. 点击 **先试一次本机模拟**，用合成图片体验完整流程。处理自己的图片时，在 **候选来源** 选择 **本地图片文件夹**，选好目标和文件夹后开始比对。

也可以在 PowerShell 里安装依赖后直接运行 CLI。将示例路径换成你的图片和目录：

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m avatar_compare --target .\target.png --folder .\images --open-report
```

Windows 和 Python 3.12 是已验证的使用环境。依赖范围允许 Python 3.10–3.13。

## 功能

- **查看匹配依据**。区分字节相同、解码像素相同和局部几何证据，可查看截图、裁剪或压缩副本的对应位置。
- **停止后继续**。任务进度和失败记录保存在本机 SQLite 中，支持继续既有任务和补扫失败编号。
- **按需保留**。范围任务可使用省空间模式，仅保留有匹配证据的候选、最多 100 项复核样本和错误记录。
- **导出结果**。输出 HTML 对照报告、CSV 和 JSON。范围任务还可从扫描记录导出保留结果、完成区间和错误。

几何匹配受图像分辨率、可见细节和重复纹理影响。相同头像可以对应多个编号，图片匹配只能作为人工复核的线索。

## 数据与网络

本地图片文件夹模式和合成演示在本机运行。范围模式向所选图片接口请求编号对应的图片，目标图片只在本机用于对照。报告可能包含图片、编号和文件名，分享前请检查其中的内容。[缓存、导出及网络设置](docs/usage.md)

## 许可证

项目原创代码和文档使用 [BSD Zero Clause License（0BSD）](LICENSE)。依赖项沿用各自的许可证。
