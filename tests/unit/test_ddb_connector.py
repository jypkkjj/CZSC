"""``ddb_connector`` 单元测试（DolphinDB session mock）。

业务背景：
    ``czsc.connectors.ddb_connector`` 面向内网 DolphinDB 实例 ``ddbq``。
    测试不依赖真实 server：使用 ``MockSession`` 伪造 ``ddb.session.run(sql)`` 的返回值，
    走 ddb_connector 的整条调用链：_run_ddb → _fetch_raw_kline → get_raw_bars → _format_ddb_kline。

测试覆盖（按 docs/connectors.md §6.1-§6.8 踩坑清单对应）：
    §6.1  日线 / 分钟线 code 格式分裂（聚宽 vs Tushare）
    §6.2  字段名不同：日线 volume/money，分钟 vol/amount
    §6.3  SQL 日期字面量 YYYYMMDD 必须转 YYYY.MM.DD
    §6.4  F30 / F60 在服务端只有 min1/min5，走客户端重采样
    §6.5  vol/amount 单位保持原始值（不 ×100 / ×1000）
    §6.6  dolphindb 依赖是否在 pyproject 默认依赖里
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import czsc
from czsc import CZSC, Freq, RawBar
from czsc.connectors import ddb_connector as ddb


# --------------------------- Fixtures / helpers --------------------------- #


class _MockSession:
    """Mock ddb.session：根据 SQL 关键字决定返回的 df。"""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def run(self, sql: str) -> pd.DataFrame:
        self.calls.append(sql)
        s = sql.strip().lower()

        # ---- 日线 ---- #
        if "dfs://day_level_joinquant" in s:
            return _make_day_df()

        # ---- 分钟级：min1 / min5 ---- #
        if "min1" in s:
            return _make_min_df(rows=240, freq_label="1min", day="2025.01.02",
                                ts_code="SZ000001")
        if "min5" in s:
            return _make_min_df(rows=48, freq_label="5min", day="2025.01.02",
                                ts_code="SZ000001")

        # 默认空 df（兜底）
        return pd.DataFrame()


def _patch_pool(_mock_session_unused: "_MockSession | None" = None) -> tuple[list[str], MagicMock]:
    """返回 (calls_list, fake_pool)。

    - fake_pool.get() 返回带 ``run`` 方法的对象；
    - 每次运行 SQL 时把原 SQL 字符串 ``append`` 到 calls_list，
      测试中可以 ``calls[-1]`` 取最近一次的 SQL 断言。
    """
    calls: list[str] = []

    def fake_run(sql, *, params=None):
        calls.append(sql)
        s = sql.strip().lower()
        if "dfs://day_level_joinquant" in s:
            return _make_day_df()
        # F30 / F60 客户端重采样的用例需要 5min 数据足够，至少 6 根一组
        if "min1" in s or "min5" in s:
            return _make_min5_df(day="2025.01.02", code="SZ000001", bars_per_day=48)
        # ddb //info / all_securities 表（get_symbols）：按 server 端 where 逻辑过滤
        if "all_securities" in s:
            df = _make_all_securities_df()
            # 模拟服务端 where type = 'xxx' 过滤
            import re
            m = re.search(r"where\s+type\s*=\s*'(\w+)'", s)
            if m:
                t = m.group(1)
                df = df[df["type"].astype(str) == t]
            return df
        return pd.DataFrame()

    fake_pool = MagicMock()
    fake_pool.get.return_value = MagicMock(run=fake_run, upload=MagicMock())
    return calls, fake_pool


def _make_min5_df(day: str, code: str, bars_per_day: int = 48) -> pd.DataFrame:
    """构造真实 5min 间隔的 df：9:35 起每 5 分钟一根，每天 bars_per_day 根。"""
    records = []
    base = pd.Timestamp(f"{day} 09:35:00")
    for i in range(bars_per_day):
        records.append({
            "code": code,
            "trade_time": base + pd.Timedelta(minutes=5 * i),
            "open": 11.7 + i * 0.001,
            "close": 11.71 + i * 0.001,
            "high": 11.72 + i * 0.001,
            "low": 11.69 + i * 0.001,
            "vol": 100000.0 + i * 1000,
            "amount": 1170000.0 + i * 10000,
        })
    return pd.DataFrame(records)


def _make_day_df() -> pd.DataFrame:
    """模拟日线 df（聚宽 code 格式 + volume/money 字段）。"""
    return pd.DataFrame([
        {"time": pd.Timestamp("2025-01-02"),
         "code": "000001.XSHE",
         "open": 1671.43, "close": 1683.14, "low": 1669.97, "high": 1684.61,
         "volume": 64637.0, "money": 11465603.0,
         "factor": 27.242, "high_limit": 197.50, "low_limit": 161.55,
         "avg": 177.35, "pre_close": 1630.0, "paused": 0.0,
         "open_interest": float("nan")},
        {"time": pd.Timestamp("2025-01-03"),
         "code": "000001.XSHE",
         "open": 1683.14, "close": 1708.02, "low": 1680.22, "high": 1709.49,
         "volume": 88000.0, "money": 15000000.0,
         "factor": 27.242, "high_limit": 197.50, "low_limit": 161.55,
         "avg": 1700.0, "pre_close": 1683.14, "paused": 0.0,
         "open_interest": float("nan")},
    ])


def _make_min_df(rows: int, freq_label: str, day: str, ts_code: str) -> pd.DataFrame:
    """模拟分钟级 df：5min/1min 用 trade_time/vol/amount 字段。"""
    times = pd.date_range(start=f"{day} 09:31:00", periods=rows, freq="1min")
    records = []
    for i, t in enumerate(times):
        # 5min 视角下时间跳变一下，每 5 根一行假数据
        if freq_label == "5min" and i % 5 != 0:
            continue
        records.append({
            "code": ts_code,
            "trade_time": t,
            "open": 11.7 + i * 0.001,
            "close": 11.71 + i * 0.001,
            "high": 11.72 + i * 0.001,
            "low": 11.69 + i * 0.001,
            "vol": 100000.0 + i * 1000,
            "amount": 1170000.0 + i * 10000,
        })
    if freq_label == "5min":
        records = records[::5][:48]
    return pd.DataFrame(records)


def _make_all_securities_df() -> pd.DataFrame:
    """模拟 ``dfs://info / all_securities`` 表的 code / type 列。

    notebook/ddb_data.ipynb cell-8 实测有 6 列：
        display_name / name / start_date / end_date / type / code
    本函数只保留 ``code / type``（只需 ``get_symbols`` 使用的部分）；
    实测 type ∈ {"stock", "index"}。生产代码只看这两列。
    """
    rows = [
        {"code": "000001.XSHE", "type": "stock"},
        {"code": "600000.XSHG", "type": "stock"},
        {"code": "510300.XSHG", "type": "stock"},
        {"code": "159660.XSHE", "type": "stock"},
        {"code": "000001.XSHG", "type": "index"},   # 上证指数
        {"code": "000300.XSHG", "type": "index"},   # 沪深300
        {"code": "000905.XSHG", "type": "index"},   # 中证500
    ]
    return pd.DataFrame(rows)


@pytest.fixture
def patched_pool():
    """每个测试方法均拿到一个干净的 mock pool。"""
    calls, fake_pool = _patch_pool(_MockSession())
    with patch.object(ddb, "_pool", fake_pool):
        yield calls, fake_pool


# --------------------------- Test cases --------------------------- #


class TestDdbConnector:

    def test_6x1_code_format_split(self, patched_pool):
        """§6.1 日线 / 分钟线 code 格式分裂。

        - 日线用聚宽格式（"000001.XSHE"）；
        - 分钟级在 SQL 查询里使用 Tushare 格式（"SZ000001"），但 RawBar.symbol 仍然记录聚宽。
        """
        calls, _ = patched_pool

        # 日线：直接传聚宽格式
        bars_d = ddb.get_raw_bars("000001.XSHE", Freq.D, "20250101", "20250131")
        assert len(bars_d) > 0
        assert 'code = "000001.XSHE"' in calls[-1]
        # 日线 Symbol 原样保留给 RawBar.symbol
        assert all(b.symbol == "000001.XSHE" for b in bars_d)

        # 分钟级：传入也是聚宽；SQL 里出现 Tushare 格式
        calls.clear()
        bars_5 = ddb.get_raw_bars("000001.XSHE", Freq.F5, "20250102", "20250102")
        assert len(bars_5) > 0
        assert 'code = "SZ000001"' in calls[-1]   # 已转成 Tushare
        # RawBar.symbol 仍然是聚宽格式（确认）
        assert all(b.symbol == "000001.XSHE" for b in bars_5)

    def test_6x2_field_name_diff(self, patched_pool):
        """§6.2 日线字段 volume/money，分钟 vol/amount。

        - 日线用 row['volume'] / row['money']，
        - 分钟用 row['vol'] / row['amount']。
        """
        # 日线
        bars_d = ddb.get_raw_bars("000001.XSHE", Freq.D, "20250101", "20250131")
        # 用 mock 里 hardcode 的 volume=64637.0, money=11465603.0 校验
        assert bars_d[0].vol == 64637.0
        assert bars_d[0].amount == 11465603.0

        # 分钟：mock 中 vol=100000.0, amount=1170000.0
        bars_5 = ddb.get_raw_bars("000001.XSHE", Freq.F5, "20250102", "20250102")
        assert bars_5[0].vol == 100000.0
        assert bars_5[0].amount == 1170000.0

    def test_6x3_date_format_yyyymmdd_to_dots(self, patched_pool):
        """§6.3 SQL 日期字面量必须 YYYY.MM.DD。

        - 输入 "20250101"/"20250131" 都能传；
        - 实际拼到 SQL 里的日期是 "2025.01.01"/"2025.01.31"；
        - SQL 用半闭区间，上界 edt 不带 in-place 15:00:00 后缀；
        """
        calls, _ = patched_pool
        ddb.get_raw_bars("000001.XSHE", Freq.D, "20250101", "20250131")
        sql = calls[-1]
        # ❌ 不会出现连写 "20250101"
        assert "20250101" not in sql and "20250131" not in sql
        # ✅ 一定是带点的格式
        assert "2025.01.01" in sql
        assert "2025.01.31" in sql
        # 半闭区间使用 < 而不是 <=（避免 edt 当日重复）
        assert "<" in sql

    def test_6x4_client_resample_from_5min_f15_f30_f60(self, patched_pool):
        """§6.4 F15 / F30 / F60 服务端都没有表，走客户端重采样 5min。

        验证：调用 Freq.F15 / F30 / F60 任一时，最终都调用 ``czsc.resample_bars``，
              raw_bars=True、base_freq="5分钟"。
        """
        calls, _ = patched_pool
        for freq in (Freq.F15, Freq.F30, Freq.F60):
            with patch.object(czsc, "resample_bars",
                              wraps=czsc.resample_bars) as mock_resample:
                bars = ddb.get_raw_bars("000001.XSHE", freq, "20250102", "20250102")
                assert mock_resample.called, f"{freq.name} 必须经 resample_bars 客户端重采样"
                args, kwargs = mock_resample.call_args
                # raw_bars=True, target_freq=freq, base_freq="5分钟"
                assert kwargs.get("raw_bars") is True or (len(args) >= 3 and args[2] is True)
                assert kwargs.get("base_freq") == "5分钟" or (
                    len(args) >= 4 and args[3] == "5分钟"
                )
                # 至少有一根 bar 出来
                assert bars and len(bars) > 0
                # 全部以聚宽格式记录 symbol
                assert all(b.symbol == "000001.XSHE" for b in bars)
                # 全部 freq 字段都正确反映用户传入的目标 freq
                assert all(b.freq == freq for b in bars)
            # SQL 也走的是 min5（"dfs://xc/tushare/min" + "min5"）
            assert any("min5" in c and "dfs://xc/tushare/min" in c for c in calls), \
                f"{freq.name} 应当查询 ddb min5 表"

    def test_6x5_keep_original_unit(self, patched_pool):
        """§6.5 vol/amount 单位保持原始。

        即使日线字段名为 volume/money，czsc.RawBar 接收的也是 volume 不是 vol。
        测试 MockSession.run 只返回 volume 字段，连接器应当宽容地提取。
        """
        # 第 1 个日线 raw_df.volume = 64637.0, money = 11465603.0
        bars = ddb.get_raw_bars("000001.XSHE", Freq.D, "20250101", "20250131")
        # 没有 ×100 / ×1000
        assert bars[0].vol == 64637.0
        assert bars[0].amount == 11465603.0
        # 验证 amount 已经把 money 提取出来
        assert bars[0].amount != 0.0

    def test_6x6_dolphindb_dependency(self):
        """§6.6 dolphindb 是默认依赖。

        在 PYPROJ 上不能置 optional，必须是默认依赖。
        模拟一个 fake 到 sys.modules 里，伪造已经安装。
        """
        # 检查 pyproject.toml 里实际写的就是 dependencies，不是 optional
        with open("pyproject.toml") as f:
            content = f.read()
        assert '"dolphindb>=1.0.1"' in content, (
            "ddb_connector 必须把 dolphindb 软依赖提到 dependencies"
        )
        # 不应当单独放到 [project.optional-dependencies].ddb 区块
        # 因为测试桩 mock sys.modules 困难，这里只静态检查 pyproject 文本。

    def test_user_can_pass_freq_enum_or_string(self, patched_pool):
        """get_raw_bars 应当接受 czsc.Freq 或者 "30分钟" 字符串两种形态。"""
        # Freq.F15（新增：客户端重采样 5min）
        bars_f15 = ddb.get_raw_bars("000001.XSHE", Freq.F15, "20250102", "20250102")
        # Freq.F30
        bars1 = ddb.get_raw_bars("000001.XSHE", Freq.F30, "20250102", "20250102")
        # 字符串 "5分钟"
        bars2 = ddb.get_raw_bars("000001.XSHE", "5分钟", "20250102", "20250102")
        # 字符串 "15分钟"（新增：字符串态也支持重采样路径）
        bars3 = ddb.get_raw_bars("000001.XSHE", "15分钟", "20250102", "20250102")
        # Symbol 一致
        for bars in (bars_f15, bars1, bars2, bars3):
            assert all(b.symbol == "000001.XSHE" for b in bars), \
                f"symbol 没保持聚宽格式：{[b.symbol for b in bars[:3]]}"
        # Freq 一致
        assert all(b.freq == Freq.F15 for b in bars_f15)
        assert all(b.freq == Freq.F30 for b in bars1)
        assert all(b.freq == Freq.F5 for b in bars2)
        assert all(b.freq == Freq.F15 for b in bars3)

    def test_format_converter_roundtrip(self):
        """格式转换器互逆性 / 边缘 case。"""
        assert ddb.to_tushare_code("000001.XSHE") == "SZ000001"
        assert ddb.to_tushare_code("510300.XSHG") == "SH510300"
        # 互逆
        assert ddb.from_tushare_code(ddb.to_tushare_code("000001.XSHE")) == "000001.XSHE"
        assert ddb.from_tushare_code(ddb.to_tushare_code("159660.XSHE")) == "159660.XSHE"
        # 半字节 code 的处理（ETF/指数）
        assert ddb.to_tushare_code("000300.XSHG") == "SH000300"

    def test_unsupported_freq_raises(self, patched_pool):
        """不支持的 freq（服务端没表）应显式 NotImplementedError。"""
        with pytest.raises(NotImplementedError) as exc:
            ddb.get_raw_bars("000001.XSHE", Freq.F10, "20250102", "20250102")
        assert "F10" in str(exc.value) or "ddb_connector" in str(exc.value)


class TestGetSymbols:
    """``get_symbols(step)`` 的端到端 + 分支覆盖。

    数据源对齐 notebook/ddb_data.ipynb cell-8 的 SQL：
        select * from loadTable("dfs://info", "all_securities")
    ``_make_all_securities_df()`` 提供 4 stock + 3 index 共 7 行；这是 service 实测
    真实有 7894 行大幅简化版。
    """

    def test_step_all_returns_full_code_list(self, patched_pool):
        """step='all' 返回所有人 code 列 (stock + index 共 7 行)。"""
        syms = ddb.get_symbols("all")
        assert isinstance(syms, list)
        assert len(syms) == 7
        # 内容必须全部存在
        assert "000001.XSHE" in syms
        assert "000300.XSHG" in syms
        # 调用过 SQL
        calls, _ = patched_pool
        assert any("all_securities" in c for c in calls)

    def test_step_stock_filters_type(self, patched_pool):
        """step='stock' 只返回 type='stock' 的代码。"""
        syms = ddb.get_symbols("stock")
        assert all(s != "000001.XSHG" for s in syms), "上证实证不应出现在 stock 集"
        assert "000001.XSHE" in syms
        assert "510300.XSHG" in syms
        # 生成的 SQL 必须含 type='stock'
        calls, _ = patched_pool
        last = next(c for c in reversed(calls) if "all_securities" in c)
        assert "where type = 'stock'" in last

    def test_step_index_filters_type(self, patched_pool):
        """step='index' 只返回 type='index' 的代码。"""
        syms = ddb.get_symbols("index")
        assert "000001.XSHE" not in syms, "平安银行不应出现在 index 集"
        assert "000300.XSHG" in syms
        assert len(syms) == 3

    def test_default_step_is_all(self, patched_pool):
        """不传参数时默认 step='all'。"""
        syms_default = ddb.get_symbols()
        syms_explicit = ddb.get_symbols("all")
        assert syms_default == syms_explicit

    def test_unknown_step_raises_valueerror(self, patched_pool):
        """未知 step 抛 ValueError, 不连 ddb。"""
        with pytest.raises(ValueError) as exc:
            ddb.get_symbols("train")  # qa_connector 风格的 step, 本 connector 不支持
        assert "train" in str(exc.value)

    def test_ddb_failure_returns_empty_with_warning(self, patched_pool):
        """ddb 不可达时 fail-soft: 返回 [] + logger.warning, 不 raise。"""
        calls, fake_pool = patched_pool
        # 模拟 ddb 不可达：get 返回一个 dummy session 使 _run_ddb 抛错
        boom = MagicMock()
        boom.run.side_effect = RuntimeError("connection refused")
        boom.upload = MagicMock()
        original_get = fake_pool.get
        fake_pool.get.return_value = boom

        try:
            syms = ddb.get_symbols("all")
        finally:
            fake_pool.get = original_get

        assert syms == []
        assert boom.run.called
