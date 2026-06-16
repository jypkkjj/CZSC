"""教程示例信号 my_atr_band_V250101 的 Python 集成测试。

对应教程：[docs/add-a-new-signal.md](../../docs/add-a-new-signal.md)

测试目标:
    1. `czsc.signals.list --json` 能列出该信号（CLI 已经在 registry.rs 单测验证过）
    2. 走 `CzscSignals(bg)(...)` 的「单次 update_signals」模式能算出非空信号
    3. `generate_czsc_signals` 大批量输出后, 信号结果列在 DataFrame 中存在,
       且末值 ∈ {"低位", "中位", "高位", "其他"}
    4. 信号命名空间/分类/模板字段都符合宏注解
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from czsc import Freq, format_standard_kline
from czsc.mock import generate_symbol_kines


@pytest.fixture(scope="module")
def _bars_30m() -> list[Any]:
    """与 [tests/test_lightweight_signals.py] 的 _bars_demo 完全一致, 复用 fixture 风格。"""
    df = generate_symbol_kines("000001", "30分钟", "20230101", "20230601", seed=42)
    return format_standard_kline(df, freq=Freq.F30)


class TestRegistration:
    """第一关: 注册表能看到这个信号。"""

    def test_listed_in_native_registry(self):
        import czsc._native as native

        all_kline = native.list_all_signals()
        mine = [s for s in all_kline if s["name"] == "my_atr_band_V250101"]
        assert len(mine) == 1
        entry = mine[0]
        assert entry["category"] == "kline"
        assert entry["namespace"] == "my"
        assert entry["param_template"] == "{freq}_D{di}ATR{timeperiod}_Z{lookback}_Z分位V250101"

    def test_template_matches_macro(self):
        """param_template 必须与宏 #[signal(template = "...")] 完全一致。"""
        import czsc._native as native

        mine = next(
            s for s in native.list_all_signals() if s["name"] == "my_atr_band_V250101"
        )
        # 注意: 大括号 {} 在 JSON 字符串中不被转义
        assert mine["param_template"] == "{freq}_D{di}ATR{timeperiod}_Z{lookback}_Z分位V250101"


class TestGenerateBatch:
    """第二关: 走 generate_czsc_signals 端到端跑通。"""

    def test_signal_column_appears_and_valid(self, _bars_30m):
        from czsc.traders import generate_czsc_signals

        df = generate_czsc_signals(
            _bars_30m,
            signals_config=[{"name": "my_atr_band_V250101", "freq": "30分钟"}],
            df=True,
        )
        # 列名按 Signal.key() 规约: 只保留前 3 段 (k1_k2_k3), 过滤掉"任意"。
        # 这里 k1="30分钟", k2="D1ATR14Z60", k3="Z分位V250101"。
        expected_col_prefix = "30分钟_D1ATR14Z60_Z分位V250101"
        sig_cols = [c for c in df.columns if c == expected_col_prefix]
        assert sig_cols, (
            f"找不到新信号输出列, 实际前几列 = {list(df.columns)[:8]!r}"
        )
        col = sig_cols[0]
        vals = df[col].dropna()
        assert not vals.empty, "信号列全是 NaN"
        # Signal.value() = "v1_v2_v3_score", 这里 v2=v3="任意", score=0。
        # 所以合法值形如 "低位_任意_任意_0" / "中位_任意_任意_0" / "高位_任意_任意_0"。
        unique_vals = set(vals.unique())
        expected_v1s = {"低位_任意_任意_0", "中位_任意_任意_0", "高位_任意_任意_0", "其他_任意_任意_0"}
        unexpected = unique_vals - expected_v1s
        assert not unexpected, (
            f"出现了非预期的 value: {unexpected!r} (全量: {unique_vals!r})"
        )
        # 至少出现一种主分类, 不应只有 "其他"
        active = unique_vals - {"其他_任意_任意_0"}
        assert active, "30 分钟 mock 数据 5 个月, 至少应触发一次 主分类信号"

    def test_signal_column_has_three_segments(self, _bars_30m):
        """信号 key 规约为 3 段: {freq}_{diff}_Z分位V250101。过滤掉 v2/v3="任意" 段。"""
        from czsc.traders import generate_czsc_signals

        df = generate_czsc_signals(
            _bars_30m,
            signals_config=[{"name": "my_atr_band_V250101", "freq": "30分钟"}],
            df=True,
        )
        sig_cols = [c for c in df.columns if "Z分位V250101" in c]
        assert sig_cols, f"找不到带 Z分位V250101 的列, 列 = {list(df.columns)[:5]}"
        col = sig_cols[0]
        parts = col.split("_")
        assert len(parts) == 3, f"信号列名应规约为 3 段 (k1_k2_k3): {col!r}"
        assert parts[0] == "30分钟"
        assert parts[1] == "D1ATR14Z60"
        assert parts[2] == "Z分位V250101"


class TestCLIDiscoverability:
    """第三关: CLI doc/locate 都能看到。"""

    def test_cli_doc_returns_entry(self, _bars_30m):
        from typer.testing import CliRunner

        from czsc.cli import app

        runner = CliRunner()
        r = runner.invoke(
            app, ["signals", "doc", "my_atr_band_V250101", "--json"]
        )
        assert r.exit_code == 0, r.output
        import json
        d = json.loads(r.stdout)
        assert d["name"] == "my_atr_band_V250101"
        assert d["param_template"] == "{freq}_D{di}ATR{timeperiod}_Z{lookback}_Z分位V250101"
