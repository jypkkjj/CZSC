"""DolphinDB 数据源连接器。

针对内网 ddbq 实例：
  - 日线：dfs://day_level_joinquant -> get_price（聚宽格式 code）
  - 1min ：dfs://xc/tushare/min   -> min1（Tushare 格式 code）
  - 5min ：dfs://xc/tushare/min   -> min5（Tushare 格式 code）

⚠️ 与 ts_connector / qa_connector 同时存在的差异（来源：本地 ddbq 实测）：
    - 日线 / 分钟线 **code 格式不一致**——日线 = 聚宽（"000001.XSHE"），分钟线 = Tushare（"SZ000001"）。
      对外 symbol 统一以聚宽格式为准；内部 fetch 分钟线时使用 from_tushare_code() 转换。
    - 日线字段为 volume/money（不是 vol/amount）；分钟线字段名是 vol/amount。
    - 日线表多出 factor / high_limit / low_limit / avg / pre_close / paused / open_interest 八个字段。
    - 当前示例 1min 表里出现的代码如 SH501046 是货币 ETF（场内基金），单一标的；
      没像其它服务端有升降频、利率分位数等事件，仅作为参考。

⚠️ 单位口径（与 qa_connector 一致）：
    - vol/amount 保留 **原始单位**，不做 ×100 / ×1000。
    - 调用方如果跨源对比，须自行对齐单位。

⚠️ freq 支持：
    - 仅实测 F1 / F5 / F30 / F60 / D 服务端可达；Freq.F15/F10 等「服务端无该频」的
      freq 直接 NotImplementedError（不过分猜测服务端行为）。
"""

from __future__ import annotations

import os
import threading
from typing import List, Optional

import pandas as pd

import dolphindb as ddb

import czsc
from czsc import Freq, RawBar

# --------------------------- 配置 --------------------------- #

DEFAULT_DDB_HOST = "192.168.50.12"
DEFAULT_DDB_PORT = 8848
DEFAULT_DDB_USER = "admin"
DEFAULT_DDB_PASSWORD = "123456"

DDB_HOST = os.environ.get("DDB_HOST", DEFAULT_DDB_HOST)
DDB_PORT = int(os.environ.get("DDB_PORT", str(DEFAULT_DDB_PORT)))
DDB_USER = os.environ.get("DDB_USER", DEFAULT_DDB_USER)
DDB_PASSWORD = os.environ.get("DDB_PASSWORD", DEFAULT_DDB_PASSWORD)

# DolphinDB dfs 路径与表
DDB_DAY_DB = "dfs://day_level_joinquant"
DDB_DAY_TBL = "get_price"
DDB_MIN_DB = "dfs://xc/tushare/min"
DDB_MIN1_TBL = "min1"
DDB_MIN5_TBL = "min5"

# freq <-> tbl name（注意 F15 / F10 等不在服务端用；走 NotImplementedError）
FREQ_DDB_TBL = {
    Freq.F1:  (DDB_MIN_DB, DDB_MIN1_TBL),
    Freq.F5:  (DDB_MIN_DB, DDB_MIN5_TBL),
    Freq.F15: (DDB_MIN_DB, DDB_MIN5_TBL),  # 服务端没 min15；15min 走客户端重采样 5min
    Freq.F30: (DDB_MIN_DB, DDB_MIN5_TBL),  # 同上；30min 走客户端重采样 5min
    Freq.F60: (DDB_MIN_DB, DDB_MIN5_TBL),  # 同上；60min 走客户端重采样 5min
    Freq.D:   (DDB_DAY_DB, DDB_DAY_TBL),
}

# freq -> 服务端表里「时间列的名字」，客户端用来构造 RawBar.dt
TIME_COL = {
    Freq.F1:  "trade_time",
    Freq.F5:  "trade_time",
    Freq.F15: "trade_time",
    Freq.F30: "trade_time",
    Freq.F60: "trade_time",
    Freq.D:   "time",
}

# 分钟级客户端重采样目标：F30 / F60 通常用 5min base_freq 重采样
FREQ_TO_BASE_FREQ = {
    Freq.F1:  "1分钟",
    Freq.F5:  "5分钟",
    Freq.F30: "5分钟",
    Freq.F60: "5分钟",
    Freq.D:   "日线",
}


# --------------------------- code 格式转换 --------------------------- #


def to_tushare_code(order_book_id: str) -> str:
    """聚宽 → Tushare。

    >>> to_tushare_code("000001.XSHE")
    'SZ000001'
    >>> to_tushare_code("510300.XSHG")
    'SH510300'
    """
    code, exchange = order_book_id.split(".")
    prefix = "SZ" if exchange == "XSHE" else "SH"
    return prefix + code


def from_tushare_code(ts_code: str) -> str:
    """Tushare → 聚宽。

    >>> from_tushare_code("SZ000001")
    '000001.XSHE'
    >>> from_tushare_code("SH510300")
    '510300.XSHG'
    """
    prefix = ts_code[:2]
    code = ts_code[2:]
    exchange = "XSHE" if prefix == "SZ" else "XSHG"
    return f"{code}.{exchange}"


# --------------------------- 全局 DDB 会话管理 --------------------------- #


class _DDBSessionPool:
    """线程局部 DDB connection pool（每个线程复用 1 个 session）。"""
    def __init__(self) -> None:
        self._tl = threading.local()

    def get(self) -> ddb.session:
        s = getattr(self._tl, "s", None)
        if s is None:
            s = ddb.session()
            s.connect(DDB_HOST, DDB_PORT, DDB_USER, DDB_PASSWORD,
                      readTimeout=30, writeTimeout=30)
            self._tl.s = s
        return s


_pool = _DDBSessionPool()


def _run_ddb(sql: str, *, params: Optional[dict] = None) -> pd.DataFrame:
    """执行 ddb SQL，返回 pd.DataFrame。"""
    s = _pool.get()
    if params:
        for k, v in params.items():
            if isinstance(v, list):
                s.upload({k: v})
        # 把 {placeholders} 替换为真实变量（dolphindb Python 客户端的 !$VAR 语法）
        for k, v in params.items():
            sql = sql.replace("{" + k + "}", str(v).replace("'", "\\'"))
    return s.run(sql)


def _fetch_raw_kline(symbol: str, freq: Freq, sdt: str, edt: str) -> pd.DataFrame:
    """拉 DDB → 返回 raw df。失败返回空 df。"""
    if freq not in FREQ_DDB_TBL:
        raise NotImplementedError(f"ddb_connector 不支持 freq={freq!r}")

    db, tbl = FREQ_DDB_TBL[freq]
    time_col = TIME_COL[freq]

    # 日线和分钟线使用不同的 code 格式
    if freq == Freq.D:
        # 日线：symbol 已经是聚宽格式
        jq_code = symbol
        sdt_ddb = _to_iso(sdt, end_of_day=False)
        edt_ddb = _to_iso(edt, end_of_day=True)
        sql = f"""
        select *
        from loadTable("{db}", "{tbl}")
        where code = "{jq_code}"
          and {time_col} >= {sdt_ddb}
          and {time_col} <  {edt_ddb}
        order by {time_col}
        """
    else:
        # 分钟线：symbol 是聚宽 → 转 Tushare
        ts_code = to_tushare_code(symbol)
        # 分钟线需要用户传入完整 ISO 时间戳（yyyy-MM-dd HH:mm:ss）
        # sdt/edt 在这里必须已经是 datetime。允许多种格式
        sdt_iso = _to_iso(sdt, end_of_day=False)
        edt_iso = _to_iso(edt, end_of_day=True)
        sql = f"""
        select * from loadTable("{db}", "{tbl}")
        where code = "{ts_code}"
          and {time_col} >= {sdt_iso}T09:30:00
          and {time_col} <= {edt_iso}T15:00:00
        order by {time_col}
        """

    try:
        df = _run_ddb(sql)
    except Exception as exc:
        czsc.log.warn(f"DDB 查询失败: {exc}") if hasattr(czsc, "log") else None
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    return df


def _to_iso(s: str, end_of_day: bool = False) -> str:
    """'YYYYMMDD' / 'YYYY-MM-DD' → DolphinDB SQL 用 'YYYY.MM.DD' 形式。

    DolphinDB 的 SQL 中，日期/时间字面量接受：
        - 2026.01.02
        - 2026.01.02T09:30:00
    本函数把字符串 / datetime 转成前者。
    """
    ts = pd.to_datetime(s)
    return ts.strftime("%Y.%m.%d")


# --------------------------- 字段映射：DDB -> RawBar --------------------------- #


def _format_ddb_kline(kline: pd.DataFrame, freq: Freq, ts_code: str) -> List[RawBar]:
    """DDB df → list[czsc.RawBar]。

    日线字段：time, code, open, close, low, high, volume, money, factor, ...
    分钟字段：trade_time, code, open, close, high, low, vol, amount

    单位口径：保留原始值，不做 ×100 / ×1000（与 qa_connector 约定一致）。
    """
    if kline.empty:
        return []
    time_col = TIME_COL[freq]
    kline = kline.sort_values(time_col, ascending=True, ignore_index=True)
    bars = []
    for i, row in enumerate(kline.to_dict("records")):
        if freq == Freq.D:
            vol = float(row.get("volume", 0.0) or 0.0)
            amount = float(row.get("money", 0.0) or 0.0)
        else:
            vol = float(row.get("vol", 0.0) or 0.0)
            amount = float(row.get("amount", 0.0) or 0.0)
        bar = RawBar(
            symbol=ts_code,
            dt=pd.to_datetime(row[time_col]),
            id=i,
            freq=freq,
            open=float(row["open"]),
            close=float(row["close"]),
            high=float(row["high"]),
            low=float(row["low"]),
            vol=vol,
            amount=amount,
        )
        bars.append(bar)
    return bars


# --------------------------- 公开主入口 --------------------------- #


def get_raw_bars(
    symbol: str,
    freq,
    sdt: str,
    edt: str,
    fq: str = "后复权",
    raw_bar: bool = True,
    **kwargs,
) -> List[RawBar]:
    """主入口：DDB → list[RawBar]。

    Args:
        symbol:   一律以**聚宽格式**输入，例如 "000001.XSHE" / "510300.XSHG"
        freq:     "日线" / "30分钟" / czsc.Freq
        sdt/edt:  "YYYYMMDD" / "YYYY-MM-DD" / "YYYY-MM-DD HH:MM:SS"
        fq:       复权类型（DDB 服务端未实现，留待升级）
        raw_bar:  True=list[RawBar] / False=pd.DataFrame

    ⚠️ freq=F30/F60 在服务端没原生表；先拉 5min，由 czsc.resample_bars 客户端重采样。
       ref: C 端 doc.md「rust _native ta 端 仅实时重采样」
    """
    symbol = symbol if symbol.endswith((".XSHE", ".XSHG")) else from_tushare_code(symbol)
    freq = freq if isinstance(freq, Freq) else Freq(freq)

    df = _fetch_raw_kline(symbol, freq, sdt, edt)
    if df.empty:
        return []

    # F15 / F30 / F60 客户端重采样（服务端只有 min1 / min5 表）
    if freq in (Freq.F15, Freq.F30, Freq.F60):
        # czsc.resample_bars 期望 df 含 dt + symbol 列。ddb min 表的列名是 code/trade_time。
        # 同时把 symbol 重写为聚宽格式，保持与日线 / 分钟 native 输出（_format_ddb_kline 一致）。
        df2 = df.rename(columns={"trade_time": "dt"})
        df2["symbol"] = symbol                       # 强制用聚宽格式「对外统一」
        # 保留 OHLC + vol + amount；其它列会被 resample_bars 忽略
        return czsc.resample_bars(
            df2, target_freq=freq, raw_bars=True, base_freq="5分钟",
        )

    if raw_bar:
        return _format_ddb_kline(df, freq, ts_code=symbol)
    return df


def get_symbols(step: str = "all") -> list[str]:
    """列固定一组聚宽格式的 symbol。仅作 demo 用。

    真实业务里应该从 ddb 里 loadTable("dfs://...", "stock_list") 取。
    """
    return ["000001.XSHE", "600000.XSHG", "510300.XSHG", "159660.XSHE"]
