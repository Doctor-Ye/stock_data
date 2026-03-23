# Stock SEC Database

一个本地股票数据库项目，用于：

- 拉取标普 500 成分股列表
- 从 SEC 下载公司 `submissions` 和 `companyfacts`
- 提取近 10 年核心财务指标
- 每日检查 `10-K` / `10-Q` 更新
- 生成更新公告并刷新本地数据

当前仓库提供两套实现：

- `Python + SQLite`：推荐，适合长期运行和查询
- `PowerShell`：无 Python 时的 fallback

## 目录

- `src/StockDb.ps1`: 核心函数
- `scripts/sec_db.py`: Python 主程序
- `scripts/Sync-Full-Python.ps1`: Python 全量初始化
- `scripts/Update-Daily-Python.ps1`: Python 每日增量更新
- `scripts/Register-ScheduledTask-Python.ps1`: Python 计划任务注册
- `scripts/Status-Python.ps1`: 查看数据库状态
- `scripts/Export-Csv-Python.ps1`: 导出表或视图到 CSV
- `scripts/Build-Web-Data.ps1`: 生成网页数据
- `scripts/Serve-Web.ps1`: 启动本地网页服务
- `scripts/Sync-Full.ps1`: 全量初始化
- `scripts/Update-Daily.ps1`: 每日增量更新
- `scripts/Register-ScheduledTask.ps1`: 注册 Windows 计划任务
- `config/settings.example.json`: 配置模板

## 快速开始

1. 复制配置文件：

```powershell
Copy-Item .\config\settings.example.json .\config\settings.json
```

2. 修改 `config/settings.json` 中的 `userAgent`。

SEC 明确要求提供可识别的 `User-Agent`，建议填成：

```text
MyStockDB/1.0 your-email@example.com
```

3. 推荐使用 Python 版本做首次全量同步：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Sync-Full-Python.ps1
```

也可以直接运行：

```powershell
C:\Users\admin\AppData\Local\Programs\Python\Python312\python.exe .\scripts\sec_db.py full-sync
```

4. 每日增量更新：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Update-Daily-Python.ps1
```

5. 注册计划任务：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Register-ScheduledTask-Python.ps1 -DailyTime 08:00
```

6. 如果你想只测试一家公司：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Sync-Full-Python.ps1 -Ticker NVDA -Force
```

## 输出数据

- `data\db\companies.json`: 标普 500 公司主数据
- `data\db\filings.json`: 财报索引
- `data\db\financials_annual.json`: 近 10 年核心财务指标
- `data\db\financials_quarterly.json`: 近 10 年季度核心财务指标
- `data\db\stock_sec.db`: SQLite 数据库
- `data\raw\sec\...`: SEC 原始响应
- `data\announcements\latest.md`: 最近一次更新公告
- `data\announcements\history\...`: 历史公告

## 核心指标

- Revenue
- NetIncome
- OperatingIncome
- TotalAssets
- TotalLiabilities
- ShareholdersEquity
- CashAndEquivalents
- OperatingCashFlow
- CapitalExpenditure
- FreeCashFlow
- DilutedEPS
- SharesOutstanding

## 常用命令

查看状态：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Status-Python.ps1
```

导出最新年度财务：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Export-Csv-Python.ps1 -Table latest_annual_financials -Output .\exports\latest_annuals.csv
```

直接执行 SQL 查询：

```powershell
C:\Users\admin\AppData\Local\Programs\Python\Python312\python.exe .\scripts\sec_db.py query-sql "select ticker, fiscal_year, revenue, net_income from latest_annual_financials order by revenue desc limit 20"
```

生成网页数据并启动本地网页：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Build-Web-Data.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\Serve-Web.ps1 -Port 8080
```

然后访问：

```text
http://127.0.0.1:8080
```

增强版仪表板页面：

```text
http://127.0.0.1:8080/dashboard.html
```

## 注意

- 首次全量同步会发起大量网络请求，耗时较长。
- 该方案依赖 SEC 的 `submissions` 与 `companyfacts` JSON 接口，不解析 PDF 或 HTML 报表正文。
- 标普 500 成分股列表默认抓取 Wikipedia 页面表格；财报和财务数据来自 SEC。
- Python 版本使用标准库和 SQLite，不额外依赖第三方包。
## Universe Rules

- Default universe = S&P 500 + US-listed equities above `universeMinMarketCapUsd` + all ADRs + `config/additional_companies.json` manual overrides.
- Large-cap screening uses latest SEC share count multiplied by the latest market quote.
- ADR support includes annual forms `20-F`, `20-F/A`, `40-F`, and `40-F/A`.
- Partial sync is supported via `--limit`, and checkpoint resume is supported via `--resume`, which continues after `sync_state.last_processed_ticker`.
- Recommended workflow:
- `python .\scripts\sec_db.py refresh-companies` to rebuild and stage the universe only
- `powershell -ExecutionPolicy Bypass -File .\scripts\Sync-Full-Python.ps1 -Resume -Limit 100` to sync staged companies in batches
- To keep running batches until the staged universe is done, use `powershell -ExecutionPolicy Bypass -File .\scripts\Sync-Staged-Until-Complete.ps1 -BatchSize 100`

## GitHub Pages

- To publish the current static dashboard to an external site, mirror `web/` into `docs/`:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Sync-Web-Docs.ps1
```

- The repo includes `.github/workflows/deploy-pages.yml`, which deploys `docs/` to GitHub Pages on every push to `main`.
- After the first push, set Pages source to `GitHub Actions` in the repository settings if it is not already enabled.
