# 数据连接器踩坑清单

> 适用读者：想把外部数据源（A股 Tushare、加密货币 CCXT、期货 TqSdk、QuantAxis、自建投研共享）接入 czsc 的开发者。
>
> 最后更新：2026/06/14
>
> 来源：本目录 4 个 connector 的实测（≥2026-06-14）。

`czsc/connectors/` 目录下有 4 个连接器，对应 4 类数据来源：

| 模块                       | 数据源                 | 协议 / SDK              | 是否走 `DataClient` |
| -------------------------- | ---------------------- | ----------------------- | ------------------- |
| [`ts_connector.py`](../../czsc/connectors/ts_connector.py)           | Tushare Pro            | HTTPS POST + token      | ✅ 是            |
| [`ccxt_connector.py`](../../czsc/connectors/ccxt_connector.py)       | 加密货币交易所（CCXT 抽象） | CCXT 库 / Binance 等 | ❌ 否             |
| [`tq_connector.py`](../../czsc/connectors/tq_connector.py)           | 天勤 TqSdk（期货）     | 私有 TCP/WS             | ❌ 否             |
| [`local_data.py`](../../czsc/connectors/local_data.py)               | CZSC 投研共享数据      | 本地 parquet/csv         | ❌ 否（纯离线）      |
| [`qa_connector.py`](../../czsc/connectors/qa_connector.py)           | QuantAxis 内网 webserver | HTTP GET                | ❌ 否             |
| [`ddb_connector.py`](../../czsc/connectors/ddb_connector.py)         | DolphinDB 实例（ddbq）    | dolphindb 协议          | ❌ 否（纯本地）    |

> **CLAUDE.md 提醒**：`czsc.connectors.*` 模块都是 **PyO3 边界胶水**（网络拉取 + 字段映射），不是"全工程统一数据接口"。下游统一在 `czsc.RawBar` + `CZSC` 处，收敛在 `format_*_kline()` 几条函数里。

---

## 1. `ts_connector` —— Tushare

### 1.1 单位换算

`ts_connector.format_kline`（[czsc/connectors/ts_connector.py:24-58](../../czsc/connectors/ts_connector.py#L24-L58)）按 freq 分两套单位：

| freq        | `vol` 来源      | `amount` 来源     | ×倍数                |
| ----------- | -------------- | ----------------- | -------------------- |
| `Freq.D`    | `daily.vol` = 手 | `daily.amount` = 千元 | `vol × 100`、`amount × 1000` |
| `Freq.F*`（分钟）| `stk_mins.vol` = 股 | `stk_mins.amount` = 元 | 不变             |

注意 Tushare 这是**对日线单独做单位换算**——A 股日线在 Tushare 上以"手 / 千元"做带宽压缩，写到 czsc 的 `RawBar` 时还原成 "股 / 元"。

### 1.2 dt 字段分流

```python
dt_key = "trade_time" if "分钟" in freq.value else "trade_date"
```

靠 `freq.value` 是否含「分钟」分两种 key —— 不含（`Freq.D/W/M/S/Y`）走 `trade_date`；含（`Freq.F*`）走 `trade_time`。

**坑**：`Freq.W.value = "周线"`、`Freq.M.value = "月线"` ——**不含 "分钟"**，所以会被这条路引导到 `trade_date`。Tushare 周/月线接口的字段名也是 `trade_date`，所以**当前踩不到**—— 但如果哪天 Tushare 周/月线改成 `trade_week`，这里会"静默错位"。

`Freq.S.value = "季线"` 这条在 Tushare 上没原生接口，但路由分支不会报错——它会**用错字段名**。

### 1.3 停牌日容错

```python
vol = int(record["vol"] * 100) if record["vol"] > 0 else 0
```

`> 0` 是兜底：Tushare 日线对停牌日会返回 `NaN / NaN / 0` 三种形态，**没有 `> 0` 拦截**会让 `NaN * 100 = NaN`，`int(NaN)` 抛 `ValueError`。

### 1.4 `set_url_token` 持久化

```python
czsc.set_url_token(token=TUSHARE_TOKEN, url="http://api.tushare.pro")
```

用 `md5(url)[:8]` 作 hash_key，存到 `~/{hash}.txt`。一次配置全局生效，且 `DataClient` 启动时自动读取。**新换 URL 还得再调一次**——`md5` 用 URL 做 hash，换 URL 自然要新文件。

### 1.5 `freq.value` 是中文

`Freq.F30.value == "30分钟"`、`Freq.D.value == "日线"`。这是 `strum(serialize = "30分钟")` 决定的，**Rust variant 名 ≠ value**。如果写 if-elif 分支区分频率，必须用 `freq.value`，不能用 `freq.name`（那是 `"F30"`/`"D"`）。

---

## 2. `ccxt_connector` —— 加密货币

### 2.1 默认接入币安期货

```python
__get_exchange("币安期货")   # → ccxt.binanceusdm()
__get_exchange("币安现货")   # → ccxt.binance()
```

国内网络需 2 个环境变量：

| 变量            | 默认值                  | 说明                    |
| ------------- | -------------------- | --------------------- |
| `USE_PROXY`   | `"0"`（关）              | 设 `"1"` 启用代理 |
| `HTTP_PROXY`/`HTTPS_PROXY` | `http://127.0.0.1:10808` | 仅当 `USE_PROXY=1` 时生效 |

> 国内 v2ray 用户：把端口改成 10809（HTTPS）或 1081（SOCKS5 后挂 HTTP 转换）。

### 2.2 `time.sleep` 反爬退避

`__binance_fetch_ohlcv` 内部走 `ccxt` 拉数据，遇 `429 / 418 / 500` 自动重试 + 递增退避。**用户视角不需要懂**，但跑高频拉取（多年 1min 数据）时，时间预估上要 ×2-3。

---

## 3. `tq_connector` —— 天勤 TqSdk

### 3.1 鉴权经 `TqAuth`

```python
api = TqApi(TqKq(), auth=TqAuth("username", "password"))
```

`TqKq()` 是快期模拟账号（免费）。其它实时行情 / 实盘注册依赖天勤账户体系。

### 3.2 `format_kline` 时区敏感

`tq_connector.format_kline`（[czsc/connectors/tq_connector.py:21](../../czsc/connectors/tq_connector.py#L21)）用 `row["datetime"]` 进行 `datetime.fromtimestamp(ns / 1e9) + timedelta(minutes=1)` —— **TqSdk 给的是 ns Unix 时间戳，正好指向每根分钟线的"未来 1 分钟"**，所以 `+timedelta(minutes=1)`. 否则 `dt` 会指"前一根线的结束"而非"本根线的开始"，缠论的 K 线对齐就错了。

### 3.3 `resample_bars` 必须显式 `base_freq`

`get_raw_bars` 末尾注释说"必须显式声明，否则 resample_bars 会用默认 `Freq.F1` 误标 `RawBar.freq` 导致 Rust 端 lookup 错 wz"——这是天勤 / 其它 connector 一旦用 `czsc.resample_bars` 重采样都需要**显式**传 `base_freq=` 的提示。

---

## 4. `local_data` —— CZSC 投研共享

### 4.1 必须设置 `czsc_research_cache`

```python
cache_path = os.environ.get("czsc_research_cache", r"D:\CZSC投研数据")
if not os.path.exists(cache_path):
    raise ValueError(f"请设置环境变量 czsc_research_cache...")
```

在 import 阶段（模块顶层）就 `raise ValueError`，**没环境变量直接 `import czsc.connectors.local_data` 就炸**。这是设计——逼用户必须下载数据。

### 4.2 数据按目录分 groups

```python
def get_groups():
    return ["A股主要指数", "A股场内基金", "中证500成分股", "期货主力"]
```

`get_raw_bars` / `get_symbols` 都接 group name，按目录读 parquet。**离线、无网络**，是 QA 团队组装好的"投研包"。

---

## 5. `qa_connector` —— QuantAxis webserver（**主要踩坑项**）

针对内网 `http://192.168.50.11:8010` 的 QuantAxis 服务，归纳出开发过程中发现的 5 个**真实坑**。

### 5.1 KLINE_URL 字符串模板不能加双花括号

```python
# ❌ 错误：模块顶层 f-string 把 {code}、{market} 视为变量名，
#    启动时找不到这些变量，会留字面 '{code}' 等在 URL 里。
#    服务端拿到 ?code={code}&market={market}... 当作 pandas 解析，500 / 空 dict。
KLINE_URL = f"{QA_DOMAIN}/marketdata/fetcher?code={{code}}&market={{market}}&..."

# ❌ 同样错：上面写 {code}、{market} 看似"双花括号",
#    但实际顶层 f-string 执行时会把单 {code} 求值失败当作字面保留。
#    后果：URL 里残留 `{code}` 字符。

# ✅ 正确：模块顶层不展开，运行时 KLINE_URL.format(...) 才填。
KLINE_URL = (
    f"{QA_DOMAIN}/marketdata/fetcher"
    "?code={code}&market={market}&start={start}&end={end}"
    "&frequence={frequence}&source={source}"
)
```

> 这一类 Python f-string 双花括号坑非常隐蔽——编辑器看起来 `{{...}}` 与 `{...}` 几乎一样，但运行行为差 100 倍。

### 5.2 服务端 `result == {}` 而非 `[]`

请求合法但"无数据"时：

```json
{"status": 200, "result": {}}      // 不是 [], 是 dict！
{"status": 200, "result": [...]}   // 有数据时是 list[dict]
```

`_get_qa_data` 必须先 `isinstance(rows, list)` 校验——直接 `pd.DataFrame({})` 会得到 **空** 的 DataFrame，与"成功获取 0 行"混淆但不影响后续 `_format_qa_kline`（有 `if kline.empty: return []` 兜底）。

### 5.3 `code` 不接受后缀，但 `symbol` 必须带

```python
# ❌ 给服务端 .SZ/.SH 后缀 → result: {}
{
    "code": "000001.SZ",
    "market": "stock_cn",
}

# ✅ 后缀由 market 字段表达
{
    "code": "000001",
    "market": "stock_cn",
}
```

**解决方案**：[`get_raw_bars`](../../czsc/connectors/qa_connector.py#L260-L296) 内：

```python
ts_code = full_symbol.split(".")[0]   # "000001.SZ#E" → "000001"
```

`"000001.SZ#E"` / `"000300.SH#I"` 这种**带 asset 后缀的"新协议" symbol** 内部被解析后只传给服务端 `code=000001, market=stock_cn/...`。

### 5.4 分钟级查询必须"跨日"

```python
# ❌ 同日（start ~ end 同一天）→ result: {}
params = {"start": "2024-01-02 09:30:00", "end": "2024-01-02 11:30:00", "frequence": "5min"}

# ✅ 跨日（end 在 start 之后若干天）→ 有数据
params = {"start": "2024-12-01 09:30:00", "end": "2024-12-15 00:00:00", "frequence": "5min"}
```

为透明处理，[`get_raw_bars`](../../czsc/connectors/qa_connector.py#L260-L296) 内部对 **freq != Day** 自动把 `end` 扩展到次日 00:00：

```python
if frequence == "day":
    end_iso = edt_ts.replace(hour=15, minute=0).strftime("%Y-%m-%d %H:%M:%S")
else:
    end_iso = (edt_ts + pd.Timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
```

注意 `start_iso` 在分钟级是 `09:30:00`、在日线是 `00:00:00`——和 A 股开盘时段对齐。

### 5.5 日线 / 分钟级字段名差异

| 字段         | 日线 day          | 分钟级 (5min/15min/30min/60min) |
| ----------- | ------------------- | ------------------------------- |
| 时间         | `date`、`datetime`、`date_stamp` 都有 | 只有 `datetime`               |
| 成交量       | `vol == volume` (两字段同义)        | **只有 `volume`**（无 `vol` 字段）|
| 成交额       | `amount`            | `amount`                          |
| 多出字段     | （无）              | `type`（如 `"5min"` / `"30min"`）|

```python
# ✅ _format_qa_kline 的字段自适应选择
time_col = "datetime" if "datetime" in kline.columns else "date"
vol_col = "vol" if "vol" in kline.columns else "volume"
```

不在源头做 cancate、merge，直接两条分支；如果以后服务端休掉 day 的 `volume` 字段（仅留 `vol`），

### 5.6 QA 接口 `frequnence` ≠ 其它后端的 `freq`

QA webserver 定义 6 种频率：`1min / 5min / 15min / 30min / 60min / day`，统统用字符串参数 `frequence=` 拼进 URL。

但是⚠️：**当前 server 上 `1min` 始终返回空**（与 quantaxis 历史注释"2024-09-04 开始有数据"冲突）。

```python
FREQ_QA_MAP = {
    Freq.F5:  "5min",
    Freq.F15: "15min",
    Freq.F30: "30min",
    Freq.F60: "60min",
    Freq.D:   "day",
}
# Freq.F1 / F2 / F3 / ... 走 NotImplementedError 拒绝
```

### 5.7 日线 `vol/amount` 单位与 Tushare 不同

| 源       | vol 单位  | amount 单位 |
| -------- | ----------- | ----------- |
| Tushare 日线 | 手 | 千元  |
| **QA 日线**  | **手** | **元**  |
| Tushare 分钟 | 股 | 元      |
| **QA 分钟**  | **手** | **元**  |

QA 两套都是**手 / 元**。Tushare 日线 `vol × 100`、`amount × 1000` 后才是股 / 元。

[`_format_qa_kline`](../../czsc/connectors/qa_connector.py#L188-L277) 按"**原始单位**"约定：保持传原状不换算。如果你的策略逻辑跨源比较（计量单位必须一致），那么 QA 不适合。

### 5.8 不是 7 个 feature，是 5 个

上面列举了 5.1~5.7 共 **7** 小节，其中 5.5 包含**2**个子点；汇总表：

| 节号    | 症状                                                          | 修复位置 |
| ------ | ------------------------------------------------------------ | -------- |
| **5.1** | URL 含字面 `{code}`，底层需 KLINE_URL.format 而非 module-top-level | 模块顶部 |
| **5.2** | 服务端“空数据”响应 `result=={}` 不是 `[]`                     | `_get_qa_data` |
| **5.3** | `code` 不接受 `.SZ/.SH` 后缀                                | `get_raw_bars` 自动剥后缀 |
| **5.4** | 分钟级查询必须"跨日"，同日返回空                              | `get_raw_bars` 自动扩 end |
| **5.5** | 日线有 `vol/volume`，分钟级只有 `volume`；时间列不同           | `_format_qa_kline` 自适应 |
| **5.6** | `1min` 不在服务端，返回空；czsc 主动拒绝                       | `FREQ_QA_MAP` |
| **5.7** | QA 与 Tushare 日线单位口径不同                                  | `_format_qa_kline` 注释 |

---

## 6. 加速踩坑发现与预防 — 建议几点

1. **测试中接直纯本地函数**（`is_index / is_hkstock / get_start_date / is_future`）无需打网络，00% unit coverage。
2. **插桩调试 parser 完整响应**：在 `_get_qa_data` / 类似入口处加 `logger.debug(f"raw response: {r.text[:300]}")` 能避免“看不到载荷猜不到为什么空列表”。
3. **双接口运行对比**：同一个 symbol / freq 在 ts_connector + qa_connector 上 shapes 是否一致（基本一致是指报式对齐，代替手写断言）。

---

## 6. `ddb_connector` —— DolphinDB 实例（ddbq）

针对内网 `192.168.50.12:8848`（admin/123456）的 DolphinDB 实例 `ddbq`。

### 6.1 数据表与代码格式分裂

| 数据         | dfs 路径                                | 表名        | code 格式            | 时间列       |
| ------------ | --------------------------------------- | ----------- | ------------------- | ------------ |
| 日线         | `dfs://day_level_joinquant`              | `get_price` | **聚宽** (`000001.XSHE`) | `time`        |
| 1 分钟       | `dfs://xc/tushare/min`                   | `min1`      | **Tushare** (`SZ000001`) | `trade_time`   |
| 5 分钟       | `dfs://xc/tushare/min`                   | `min5`      | **Tushare** (`SZ000001`) | `trade_time`   |

⚠️ **日线和分钟线 code 字段格式不一致**——这是历史接法，必须在内部转换：

```python
# 用户输入：聚宽格式
get_raw_bars("000001.XSHE", Freq.D, ...)   # → 服务端 raw 直查
get_raw_bars("000001.XSHE", Freq.F5, ...)  # → 内部 to_tushare_code() → "SZ000001" 后查
```

两个 helper 在源码里 [`ddb_connector.py`](../../czsc/connectors/ddb_connector.py)：

```python
def to_tushare_code(order_book_id):  # "000001.XSHE" → "SZ000001"
def from_tushare_code(ts_code):       # "SZ000001"   → "000001.XSHE"
```

### 6.2 字段名差异

| 字段             | 日线 `get_price` | 分钟级 `min1` / `min5`  |
| ---------------- | ----------------- | ------------------------- |
| 时间             | `time`            | `trade_time`              |
| OHLC             | `open/close/low/high` | `open/close/high/low`     |
| 成交量           | **`volume`**      | `vol`                     |
| 成交额           | **`money`**       | `amount`                  |
| 其它独有         | `factor / high_limit / low_limit / avg / pre_close / paused / open_interest` | （无）|

⚠️ 日线字段名与 ts_connector / qa_connector **不同**——不做单位换算，按"原始单位"保留。

### 6.3 日期字面量必须带点

DolphinDB SQL 中 `>= 20250101` 不被解析，必须写成 `>= 2025.01.01`：

```sql
-- ❌ 不工作
status_code select …
where time >= 20250101;
-- ✅ 必须
where time >= 2025.01.01;
```

[`_to_iso()`](../../czsc/connectors/ddb_connector.py) 内部统一把 `YYYYMMDD` / `YYYY-MM-DD` 转 `YYYY.MM.DD`。

### 6.4 分钟级客户端重采样 F30/F60

服务端只有 min1 / min5，**不存在 min30 / min60**：

```python
FREQ_DDB_TBL = {
    Freq.F30: Same as Freq.F5   # → 客户端 resample_bars
    Freq.F60: Same as Freq.F5   # → 客户端 resample_bars
}
```

[`get_raw_bars`](../../czsc/connectors/ddb_connector.py#L260-L280) 内对 F30/F60 显式走 5min base 重采样。

### 6.5 vol/amount 单位

⚠️ 与 `qa_connector` 约定一致：保留**原始单位**，不做 `×100 / ×1000`。如果跨源对比，须用户自己在调用方对齐。

> **口径对比表**：
> | 源                       | vol 单位      | amount 单位 |
> | ----------------------- | ------------- | ----------- |
> | Tushare 日线            | 手（×100 = 股） | 千元（×1000） |
> | Tushare 分钟                | 股              | 元            |
> | QA 日线 / 分钟             | 手              | 元            |
> | **DDB 日线 / 分钟**         | **股**（待数据核实）| **元**      |

### 6.6 安装与依赖

```toml
# pyproject.toml
dependencies = [
    ...
    "dolphindb>=1.0.1",
]
```

随后 `uv sync --python 3.13` 装依赖。DolphinDB PIP 包会再带 `future / pydantic / pydantic-core / typing-inspection`。

### 6.7 全局 SessionPool（线程安全）

```python
class _DDBSessionPool:
    """线程局部 DDB connection pool（每个线程复用 1 个 session）。"""
    def __init__(self) -> None:
        self._tl = threading.local()
    def get(self) -> ddb.session:
        s = getattr(self._tl, "s", None)
        if s is None:
            s = ddb.session()
            s.connect(DDB_HOST, DDB_PORT, DDB_USER, DDB_PASSWORD, ...)
            self._tl.s = s
        return s
```

`dolphindb.session` **多线程不安全**——`SessionPool` 模式确保每个线程拿到独立 session。

### 6.8 涉及的几个事实坑（汇总）

| 序号 | 症状                                                            | 修复位置                                          |
| ---- | ------------------------------------------------------------- | ------------------------------------------------- |
| 6.1  | 日线和分钟线 code 格式分裂                                      | `to_tushare_code / from_tushare_code`             |
| 6.2  | 日线字段名 `volume/money`，分钟是 `vol/amount`                | `_format_ddb_kline` 二分支                                |
| 6.3  | SQL 中 `YYYYMMDD` 不被解析，必须 `YYYY.MM.DD`                | `_to_iso`                                            |
| 6.4  | 服务端只有 min1/min5，F30/F60 走客户端重采样                  | `get_raw_bars` 中的 `if freq in (F30, F60)` 分支               |
| 6.5  | vol/amount 跨源单位不一致                                       | 用户需在调用方统一                                       |
| 6.6  | dolphindb 多线程下 session 非线程安全                           | `_DDBSessionPool`                                      |

---

## 7. 参考脚本

```bash
# 跳起测试用
python -c "
import sys; sys.path.insert(0, '/Users/nuc8/my/project/github/rust/czsc')
from czsc.connectors.qa_connector import _get_qa_data
print(_get_qa_data(
    code='000001', market='stock_cn',
    start='2024-12-01 09:30:00', end='2024-12-15 00:00:00',
    frequence='5min', source='auto',
))
"
```

返回的是 `pd.DataFrame`，会看出、、不同 freq 下的是 5.2 / 5.5 里描述的差异化。
