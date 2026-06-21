"""
QuantAxis 数据源连接器（对内网 QA webserver）

服务端点：
  {QA_DOMAIN}/marketdata/fetcher - K线数据
  {QA_DOMAIN}/codelist           - 标的代码列表

domain 通过环境变量 QA_DOMAIN 配置，默认 http://192.168.50.11:8010。
QA 不可用时，get_symbols() 会降级到 ts_connector.get_symbols()。

本文件为"最小探针"版：
    - 已实现：is_future / is_index / is_hkstock / symbol_market / get_start_date
              这五个纯本地工具函数（旧版 1:1 平移，零网络）
    - 已实现：_get_qa_data / get_qa_code_list HTTP 调用层
    - 已实现：_format_qa_kline / get_raw_bars 主入口骨架
    - TODO：   _format_qa_kline 内的字段映射需根据真实响应再改
              vol / amount / vol_unit 等细节待实际抓数据后定
"""
from __future__ import annotations

import os
import re
from functools import lru_cache
from typing import List

import pandas as pd
import requests

import czsc
from czsc import Freq, RawBar

# --------------------------- 配置 --------------------------- #

DEFAULT_QA_DOMAIN = "http://192.168.50.11:8010"
QA_DOMAIN = os.environ.get("QA_DOMAIN", DEFAULT_QA_DOMAIN)
QA_TIMEOUT = int(os.environ.get("QA_TIMEOUT", "30"))

# ⚠️ 注意：实际格式字符串，{code} 等占位**不能**用 f-string（模块顶层会立即求值
# 把所有占位符去掉成空字符串）。运行时调 .format(...) 二次展开到 URL。
KLINE_URL = (
    f"{QA_DOMAIN}/marketdata/fetcher"
    "?code={code}&market={market}&start={start}&end={end}"
    "&frequence={frequence}&source={source}"
)
CODELIST_URL = f"{QA_DOMAIN}/codelist"

_session = requests.Session()


# --------------------------- 纯本地工具（旧版 1:1 平移）--------------------------- #


def is_future(code: str) -> bool:
    """以字母开头的代码视为期货（如 RB2501）。"""
    return bool(re.match(r"[a-zA-Z]", code[0]))


def is_index(symbol: str) -> bool:
    """判断是否指数（上证 0xxxxx.SH、399xxx.SZ、北证 8 开头 也算）。"""
    if (
        symbol.startswith("0")
        and (symbol.endswith(".XSHG") or symbol.endswith(".SH"))
    ) or (
        symbol.startswith("399")
        and (symbol.endswith(".XSHE") or symbol.endswith(".SZ"))
    ) or symbol.startswith("8"):
        return True
    return False


def is_hkstock(code: str) -> bool:
    """5 位代码视为港股。"""
    return len(code) == 5


def symbol_market(symbol: str) -> str:
    bare = symbol.split(".")[0]
    if len(bare) == 6:
        return "A股"
    if is_future(bare):
        return "期货"
    return "默认"


def get_start_date(end_date, freq: str = "2分钟", gap: int = 1500):
    """根据结束日期、K 线周期和数量计算开始日期（不含节假日精确性，与旧版一致）。"""
    # 一日交易 4 小时
    count = {
        "1分钟": 240, "2分钟": 120, "3分钟": 80,  "4分钟": 60,
        "5分钟": 48,  "6分钟": 40,  "10分钟": 24, "12分钟": 20,
        "15分钟": 16, "20分钟": 12, "30分钟": 8,  "60分钟": 4,
        "120分钟": 2, "日线": 1,   "周线": 0.2,
    }
    if freq not in count:
        raise ValueError(f"不支持的K线周期: {freq}")
    n = count[freq]
    days = gap // n if n >= 1 else gap * (1 / n)
    if n >= 1 and gap % n > 0:
        days += 1
    days = int(days * (1.2 if freq == "周线" else 1.4))
    return pd.to_datetime(end_date) - pd.Timedelta(days=days)


# --------------------------- HTTP 调用底层 --------------------------- #


def _trading_market(symbol: str) -> str:
    """QA webserver 的 market 参数：stock_cn / index_cn / future_cn / stock_hk。

    接受带 #E/#I/#F 后缀的 symbol；自动剥皮再判断。
    """
    base = symbol.split("#")[0] if "#" in symbol else symbol
    if is_index(base):
        return "index_cn"
    if is_future(base):
        return "future_cn"
    if is_hkstock(base):
        return "stock_hk"
    return "stock_cn"


def _get_qa_data(
    code: str,
    market: str,
    start: str,
    end: str,
    frequence: str,
    source: str = "auto",
) -> pd.DataFrame:
    """调 QA webserver 拉 K 线 df；失败或空数据返回空 df。

    实测响应（2026-06-14 192.168.50.11:8010）：
        URL: /marketdata/fetcher?code=...&market=...&start=...&end=...&frequence=...&source=...
        成功: {status:200, result:[{date, code, open, high, low, close, vol, amount,
                                     date_stamp, volume, datetime}, ...]}
               其中 vol == volume（同字段重复），amount 单位是「元」
        失败 / 错 frequence: {status:200, result:{}} （空 dict）
        网络错:                requests.RequestException

    """
    url = KLINE_URL.format(
        code=code, market=market, start=start,
        end=end, frequence=frequence, source=source,
    )
    try:
        res = _session.get(url, timeout=QA_TIMEOUT)
    except requests.RequestException as exc:
        if hasattr(czsc, "logger"):
            czsc.logger.warning(f"QA webserver 不可达: {QA_DOMAIN} -> {exc}")
        return pd.DataFrame()
    if res.status_code != 200:
        return pd.DataFrame()
    payload = res.json() or {}
    # 实测 payload["result"] 在「无数据」时是空 dict {}；在「有数据」时是 list
    rows = payload.get("result") if isinstance(payload, dict) else payload
    if not isinstance(rows, list) or not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


@lru_cache(maxsize=1)
def get_qa_code_list() -> pd.DataFrame:
    """拉 QA codelist 缓存为 DataFrame，columns=[code/name/market]。失败返回空 df。"""
    try:
        res = _session.get(CODELIST_URL)
    except requests.RequestException:
        return pd.DataFrame()
    if res.status_code != 200:
        return pd.DataFrame()
    payload = res.json() or {}
    rows = payload.get("result") if isinstance(payload, dict) else payload
    out = []
    for c in rows or []:
        try:
            code, name, market = c.split("/", 2)
            out.append({"code": code, "name": name, "market": market})
        except (ValueError, AttributeError):
            continue
    return pd.DataFrame(out).set_index("code") if out else pd.DataFrame()


# --------------------------- 字段映射：QA -> RawBar（待实测）--------------------------- #


def _format_qa_kline(kline: pd.DataFrame, freq: Freq, ts_code: str) -> List[RawBar]:
    """QA webserver 返回的 df -> list[czsc.RawBar]。

    实测字段（单位约定：QA 返回原始值，照搬到 RawBar，不做 ts_connector 那套 ×100 / ×1000）：
        date        "2024-01-02"          字符串日期
        datetime    "2024-01-02 00:00:00" 字符串时间戳
        date_stamp  1704124800.0         Unix 秒（备用，不进 RawBar）
        code        "000001"              标的代码
        open/close/high/low              float OHLC，单位元
        vol == volume  1,158,366         成交量，**单位「手」**（不是股；非 Tushare 口径）
        amount      1,075,742,208        成交额，**单位「元」**（不是千元；非 Tushare 口径）

    「QA 日线 vol(手) / amount(元)」 与「Tushare 日线 vol(手) / amount(千元)」的差别：
        Tushare 是把 amount / 1000 压缩为千元；QA 是把 vol 保持为手，把 amount 保持为元。
        本函数按用户要求严格保留原始单位，调用方做分析时按 QA 口径解读。

    dt 选 datetime（带时分秒），对齐 ts_connector：日线时时分秒全是 00:00:00。
    """
    if kline.empty:
        return []
    # 时间列：日线有 date + datetime，分钟级只有 datetime
    time_col = "datetime" if "datetime" in kline.columns else "date"
    # 成交量列：日线 vol == volume，分钟级只有 volume
    vol_col = "vol" if "vol" in kline.columns else "volume"
    kline = kline.sort_values(time_col, ascending=True, ignore_index=True)
    bars = []
    for i, row in enumerate(kline.to_dict("records")):
        bar = RawBar(
            symbol=ts_code,
            dt=pd.to_datetime(row[time_col]),
            id=i,
            freq=freq,
            open=float(row["open"]),
            close=float(row["close"]),
            high=float(row["high"]),
            low=float(row["low"]),
            vol=float(row[vol_col]),     # 日线=vol，分钟级=volume
            amount=float(row["amount"]),  # 单位元
        )
        bars.append(bar)
    return bars


# --------------------------- 公开主入口 --------------------------- #


# QA webserver 实测支持的频率（≥2026-06-14 抓样）。
# 注意：
#   - frequence="1min" 在 server 上始终返回空（即使 2024-12 的范围也是空）。
#   - frequence="5min/15min/30min/60min" 跨日范围才有数据，单日返回空。
FREQ_QA_MAP = {
    Freq.F5:  "5min",
    Freq.F15: "15min",
    Freq.F30: "30min",
    Freq.F60: "60min",
    Freq.D:   "day",
}
# 不支持的 freq（按实测 white-list 表；Freq.F1 在 QA 上不通）


def get_raw_bars(
    symbol: str,
    freq,
    sdt: str,
    edt: str,
    fq: str = "后复权",
    raw_bar: bool = True,
    **kwargs,
) -> List[RawBar]:
    """主入口：从 QA webserver 读取 K 线并转 list[RawBar]。

    symbol 协议与 ts_connector 一致："<ts_code>#<asset>"
    freq 接受 czsc.Freq 或字符串（"30分钟" / "日线"）。
    fq: "前复权" / "后复权" / "不复权"——QA 当前未实现，留待升级。

    ⚠️ QA server 对「单自然日内」的分钟级查询全部返回 {"result":{}}；
       实测必须给「跨日」的 start/end 才能拿到数据（5min 跨日 480 行 = 2 周 × 5 × 16 段）。
       本函数对 start/end **同自然日**自动扩展到 end+1 天再发请求。

    ⚠️ QA webserver code 字段只接受 6 位数「000001」，不接受扩展「000001.SZ」；
       market 字段已经体现交易所信息，调用方如果在 symbol 后挂了 #E，会改成
       ts_code=000001, market=stock_cn。本函数自动做这一步字符串处理。

    ⚠️ 1min 在 QA webserver 上始终 {"result":{}}，与 quantaxis 旧注释「2024-09-04
       起有数据」不符，本次接入不支持 Freq.F1。
    """
    full_symbol, asset = symbol.split("#") if "#" in symbol else (symbol, "E")
    # QA 不认 .SH/.SZ：剥后缀、保留 6 位
    ts_code = full_symbol.split(".")[0]
    freq = freq if isinstance(freq, Freq) else Freq(freq)

    frequence = FREQ_QA_MAP.get(freq)
    if frequence is None:
        raise NotImplementedError(
            f"qa_connector.get_raw_bars 不支持 freq={freq!r}；"
            f"目前已识别: F5/F15/F30/F60/D"
        )

    # market 推断要看带后缀的原始 symbol（QA 仅按交易所分类，但需去除 #E/#I 后缀再判断）
    market = _trading_market(full_symbol)

    sdt_ts = pd.to_datetime(sdt)
    edt_ts = pd.to_datetime(edt)
    # 日线：end 当天 15:00；分钟线：end 扩展到次日 00:00（必须跨日才有数据）
    if frequence == "day":
        start_iso = sdt_ts.strftime("%Y-%m-%d 00:00:00")
        end_iso = edt_ts.replace(hour=15, minute=0).strftime("%Y-%m-%d %H:%M:%S")
    else:
        start_iso = sdt_ts.strftime("%Y-%m-%d 09:30:00")
        end_iso = (edt_ts + pd.Timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

    df = _get_qa_data(
        code=ts_code, market=market,
        start=start_iso, end=end_iso,
        frequence=frequence, source="auto",
    )
    if df.empty:
        return []

    if raw_bar:
        return _format_qa_kline(df, freq, ts_code)
    return df


# --------------------------- 静态 symbol 字典（旧版 1:1 保留）--------------------------- #


_QA_STOCKS_MAP = {
    "A股主要指数": [
        "880823",
        "000905.SH", "000016.SH", "000300.SH",
        "000001.SH", "000852.SH",
        "399001.SZ", "399006.SZ",
        "399376.SZ", "399377.SZ",
        "399317.SZ", "399303.SZ",
    ],
    "A股场内基金": [
        "512880.SH", "518880.SH", "515880.SH", "513050.SH",
        "512690.SH", "512660.SH", "512400.SH", "512010.SH",
        "512000.SH", "510900.SH", "510300.SH", "510500.SH",
        "510050.SH",
        "159992.SZ", "159985.SZ", "159981.SZ", "159949.SZ", "159915.SZ",
    ],
    "check": [
        "513130", "513330", "513050", "513120",
        "518880", "510900", "513310", "588000", "511380",
        "159561", "159941", "159819",
    ],
}


def get_symbols(step: str = "all") -> list[str]:
    """保留旧版的静态 symbol 字典（QA server 不可达时仍可用）。

    step ∈ {"all", "A股主要指数", "A股场内基金", "check", "stock", "train", "valid"}
        - "stock/train/valid" 依赖 QA codelist；QA 不可达时降级为空 list（加 warning）。
    """
    static = _QA_STOCKS_MAP
    if step in static:
        return list(static[step])
    if step == "all":
        return static["A股主要指数"] + static["A股场内基金"]
    # 动态类（依赖 QA codelist）
    codelist = get_qa_code_list()
    if codelist.empty:
        czsc.logger.warning(f"QA codelist 不可达，step='{step}' 返回 []") if hasattr(czsc, "logger") else None
        return []
    stocks = codelist.index.tolist()
    if step == "stock":
        return stocks
    if step == "train":
        return stocks[:200]
    if step == "valid":
        return stocks[200:600]
    raise ValueError(f"qa_connector.get_symbols 不支持 step='{step}'；"
                     f"已知步骤: {list(static) + ['all', 'stock', 'train', 'valid']}")
