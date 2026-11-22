# 参与贡献

欢迎提交问题、修正文档和改进比对结果。提交问题时请提供 Python 和系统版本、使用模式、最小复现步骤、预期及实际结果。用合成图片和本机模拟接口复现问题，提交前检查日志和报告中是否含账号、图片、路径或凭据。

## 准备环境

在 Windows PowerShell 中进入仓库根目录。建议用 Python 3.12 创建虚拟环境：

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## 运行测试

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests
```

测试包括图像匹配、本机 HTTP、范围恢复、存储策略和 Windows 启动器。修改使用方式或命令参数时，请同时更新 [使用指南](docs/usage.md)。提交 PR 时说明复现方式、修改范围和已运行的测试。
