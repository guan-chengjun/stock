# A股看盘工具（网页版同花顺风格）

纯行情查看终端，**无任何交易功能**。单文件前端 `index.html`（原生 JS + Tailwind CDN + ECharts CDN，双击即可打开），FastAPI 后端封装 [zzshare](https://pypi.org/project/zzshare/) 数据接口，并内置腾讯/新浪公开行情作为限流容错备用源。

---

## 一、功能一览

| 模块 | 说明 |
| --- | --- |
| 股票搜索 | 代码 / 名称 / 拼音首字母模糊匹配，键盘上下选择、回车直达，防抖请求 |
| 自选股 | 点击报价区 ☆ 加入/移除，本地 `localStorage` 持久化，批量报价刷新 |
| 报价面板 | 最新价、涨跌额/幅、开高低收、成交量额、换手率、市盈率、涨跌停、总/流通市值 |
| 分时图 | 价格线 + 均价线 + 昨收基准 + 红绿量柱，十字光标 tooltip |
| K 线图 | 日 K（前复权/后复权/不复权）、分钟 K（1/5/15/30/60 分），蜡烛图 + MA5/10/20 + 成交量 + 缩放 |
| 五档盘口 | 买卖五档价量、量能条、最新价居中 |
| 成交明细 | 逐笔时间/价格/手数/金额/买卖性质（红买绿卖灰中性） |
| 涨幅榜 | 涨幅 / 成交额 / 换手率三种排序，点击行直达个股 |
| 基本面 | 行业、上市日期、PE、总/流通市值、主营业务 |
| 容错 | 全局 loading、错误遮罩 + 手动重试、Toast 提示、连接状态指示、自动退避 |

配色遵循 A 股惯例：**红涨绿跌**，深色同花顺风格，响应式（桌面三栏 / 移动抽屉）。

---

## 二、环境要求

- Python 3.10 及以上（开发验证环境为 Python 3.14 + Windows 11）
- 现代浏览器（Chrome / Edge / Firefox），需联网加载 Tailwind 与 ECharts CDN
- zzshare **匿名即可用**，但限流较严；如需更高配额可配置 Token（见下文）

---

## 三、安装与启动

### 1. 安装依赖

```bash
# 建议使用虚拟环境
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
# source .venv/bin/activate

pip install -r requirements.txt
```

### 2. 启动后端

```bash
# 方式一：直接运行（推荐）
python main.py

# 方式二：uvicorn 启动
uvicorn main:app --host 127.0.0.1 --port 8000
```

启动成功后：

- API 服务：<http://127.0.0.1:8000>
- 自动接口文档（Swagger）：<http://127.0.0.1:8000/docs>
- 健康检查：<http://127.0.0.1:8000/api/health>

### 3. 打开前端

直接**双击 `index.html`** 即可（`file://` 协议，后端已开启 CORS，无需额外静态服务器）。

> 可选：如需以 http 方式访问，可在项目目录执行 `python -m http.server 5500`，再访问 <http://localhost:5500>。

### 4.（可选）配置 zzshare Token

匿名访问频率上限较低。前往 zzshare 官网个人中心免费获取 Token 后，通过环境变量注入：

```bash
# Windows PowerShell
$env:ZZSHARE_TOKEN="你的token"
python main.py

# Windows CMD
set ZZSHARE_TOKEN=你的token
python main.py

# macOS / Linux
ZZSHARE_TOKEN=你的token python main.py
```

---

## 四、后端接口文档

统一前缀 `http://127.0.0.1:8000/api`。

| 方法 | 路径 | 参数 | 说明 |
| --- | --- | --- | --- |
| GET | `/search` | `q`（必填）, `limit` | 股票代码/名称搜索 |
| GET | `/spot` | `code` 或 `codes`（逗号分隔，≤30） | 单票/批量实时报价，含五档 |
| GET | `/timeline` | `code` | 当日分时（价格/均价/量/额/涨跌幅） |
| GET | `/daily` | `code`, `adjust=qfq\|hfq\|none`, `limit`(20–1000) | 日 K 历史 |
| GET | `/minute` | `code`, `freq=1min\|5min\|15min\|30min\|60min`, `count`(20–1000) | 分钟 K |
| GET | `/rank` | `sort_by=pct_chg\|amount\|turnover_rate\|change`, `order=desc\|asc`, `limit` | 排行榜 |
| GET | `/basic` | `code` | 基本面资料 |
| GET | `/trades` | `code` | 逐笔成交明细 |
| GET | `/health` | — | 服务健康状态 |

**统一约定**

- 成交量单位统一为 **手**（1 手 = 100 股），成交额单位为 **元**，前端自动格式化为万/亿。
- 出错时返回非 200：`400` 参数非法（秒回）、`503` 上游限流（带 `Retry-After` 头与 `retry_after` 字段）、`502` 数据源失败。
- 响应内 `source` 字段标识实际数据源（`zzshare` / `tencent` / `akshare`）。

---

## 五、限流防护与容错设计（重要）

zzshare 匿名接口会返回 `429`（频率限制，`Retry-After` 可达数十秒）甚至临时 `401`。本项目做了多层防护，**请勿短时间高频切换股票或狂点刷新**：

1. **后端 TTL 缓存**：全市场快照 10s、单票报价 8s、分时 15s、排行榜 15s、日 K 12 小时、基本面 24 小时，相同请求直接命中缓存。
2. **单飞（single-flight）**：同一时刻相同请求只放行一个出站调用，其余共享结果。
3. **出站节流闸**：所有外部 HTTP 调用共享最小间隔（默认 0.8s，可用环境变量 `UPSTREAM_MIN_GAP` 调整）。
4. **SDK 重试归零**：zzshare SDK 内部 429 重试被关闭，快速失败后交给备用源与前端处理，避免请求长时间挂起。
5. **多源容错**：
   - 单票报价优先复用「全市场快照」，未命中再走腾讯 `qt.gtimg.cn`；
   - 分时 zzshare 失败自动切腾讯 `web.ifzq.gtimg.cn`；
   - 逐笔成交使用腾讯 `getMingxi`；
   - 基本面使用 akshare（东财源），限流时返回 503 由前端展示重试。
6. **前端节流 + 退避**：请求发车最小间隔 80ms；遇 503 按 `Retry-After` 退避（封顶 4s）自动重试一次，失败展示错误遮罩与「重试」按钮；页面隐藏时暂停轮询。
7. **启动预热**：后端启动时后台预拉股票列表与全市场快照（失败不影响启动，首次访问自动重试）。

---

## 六、目录结构

```
stock/
├── index.html          # 单文件前端（双击打开）
├── main.py             # FastAPI 后端（zzshare 封装 + 备用源 + 缓存/节流）
├── requirements.txt    # Python 依赖
└── README.md           # 本文档
```

---

## 七、常见问题

**Q：页面显示「无法连接后端」？**
A：确认 `main.py` 已启动且监听 8000 端口，访问 <http://127.0.0.1:8000/api/health> 应返回 `{"status":"ok",...}`。

**Q：排行榜/基本面一直转圈或提示限流？**
A：匿名额度被短时用尽。后端已自动缓存与退避，等待几十秒到几分钟后点「重试」即可；长期使用建议配置 `ZZSHARE_TOKEN`。

**Q：报价显示「来源:tencent」？**
A：说明 zzshare 当前限流，后端已自动切换到腾讯行情备用源，数据仍可正常查看。

**Q：数据是实时的吗？**
A：盘中为接近实时的快照（受接口刷新与本地缓存 TTL 影响，约数秒级延迟），盘后为收盘数据。本工具仅用于看盘，不构成投资建议。

**Q：端口 8000 被占用？**
A：修改 `main.py` 末尾 `uvicorn.run(..., port=xxxx)`，并同步修改 `index.html` 顶部的 `API_BASE`。

---

## 八、免责声明

本项目仅用于技术学习与个人行情查看，**不提供任何证券交易功能**，数据来自公开第三方接口，可能存在延迟、缺失或错误，不对任何投资决策负责。
