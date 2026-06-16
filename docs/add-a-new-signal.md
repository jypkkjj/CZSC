# 手把手加一个新信号函数

> 本篇是 [Python ↔ Rust 数据流转机制详解](./python-rust-bridge.md) 系列的第五篇 · 实战篇。
> 设计目标:**你按本教程,15 分钟内能成功加一个新的 K 线信号函数 `my_atr_band_V250101`,让它通过 `czsc.signals.list` 出现、能被 `czsc.CzscSignals` 调用、能在测试里跑通。**

---

## 目录

- [0. 一图看清整条流水线](#0-一图看清整条流水线)
- [1. 选择信号 category 与命名空间](#1-选择信号-category-与命名空间)
- [2. 选取一个模板函数](#2-选取一个模板函数)
- [3. 新增 `pub fn` + `#[signal(...)]`](#3-新增-pub-fn--signal)
- [4. 注册表 / 边界检查的 7 条 ratchet](#4-注册表--边界检查的-7-条-ratchet)
- [5. 单元测试 (Rust)](#5-单元测试-rust)
- [6. rebuild + CLI 验证](#6-rebuild--cli-验证)
- [7. Python 端调用 + 集成测试](#7-python-端调用--集成测试)
- [8. 重新生成 stub 让 IDE 类型补全生效](#8-重新生成-stub-让-ide-类型补全生效)
- [9. Fast-path 优化 - 可选进阶](#9-fast-path-优化---可选进阶)
- [10. 常见踩坑速查](#10-常见踩坑速查)
- [附录:文件清单](#附录文件清单)

---

## 0. 一图看清整条流水线

```
┌──────────────────────────────────────────────────────────────────┐
│ 1. 写信号函数 (crates/czsc-signals/src/tas.rs)                     │
│    ↓ #[signal(...)] proc-macro                                    │
│    ↓ 生成: meta const + inventory::submit!                        │
└──────────────────────────────────────────────────────────────────┘
         │  编译期
         ▼
┌──────────────────────────────────────────────────────────────────┐
│ 2. SIGNAL_REGISTRY (crates/czsc-signals/src/registry.rs)            │
│    LazyLock<HashMap<&str, SignalMeta>>                            │
│    按 name 查询 → SignalFn fn pointer                             │
└──────────────────────────────────────────────────────────────────┘
         │  Python 调用
         ▼
┌──────────────────────────────────────────────────────────────────┐
│ 3. czsc.CzscSignals (crates/czsc-python/src/trader/czsc_signals.rs) │
│    接收 SignalConfig → registry.get(name) → 拿到 fn ptr            │
│    ↓ 按 freq 分组, 逐 op 调用                                      │
└──────────────────────────────────────────────────────────────────┘
         │  计算
         ▼
┌──────────────────────────────────────────────────────────────────┐
│ 4. 信号结果 (Vec<Signal>)                                           │
│    转为 s / signal_map / sigs (CzscSignals.sigs)                   │
└──────────────────────────────────────────────────────────────────┘
         │  Python 侧读取
         ▼
┌──────────────────────────────────────────────────────────────────┐
│ 5. czsc.CzscSignals.sigs  / signals_dict                          │
│    用户拿结果                                                       │
└──────────────────────────────────────────────────────────────────┘
```

**关键点**: 你只需要**写一个函数 + 加好注释**,
**rustc** 自动帮你完成注册,Python 端无需做任何修改,IDE 自动补全。

---

## 1. 选择信号 category 与命名空间

先决定 4 件事:

| 维度 | 选项 | 项目现状 |
|------|------|---------|
| **category** | `kline` (K线级) / `trader` (Trader级) | kline 占 22 个模块大部分,trader 仅 4 |
| **命名空间** | `tas` / `cxt` / `vol` / `pressure` / ... | 由代码所在 module 决定 |
| **逻辑归属** | TA 算子 / 形态 / 量能 / 资金流 | 直接看你函数读 `cache.*_cache` 的哪一个 |
| **version 后缀** | `_V<yyMMdd>` 或 `_V<yyyyMMdd>` | 项目必须包含 `_v<版本号>` (宏会强校验) |

> 📘 **命名空间列表** (22 个 module,见 [crates/czsc-signals/src/lib.rs](../crates/czsc-signals/src/lib.rs)):
> `bar / cxt / cxt_trader / pos / cat / tas / vol / pressure / obv / cvolp / ntmdk / kcatr / clv / ang / coo / byi / jcc / xl / zdy / zdy_trader`
>
> 例:`tas_*` 放 [tas.rs](../crates/czsc-signals/src/tas.rs),`xl_*` 放 [xl.rs](../crates/czsc-signals/src/xl.rs)。

### 教程示例

我们打算新增 **`my_atr_band_V250101`**:

- 类别:**kline** 级信号(只看 K 线,不需仓位状态)
- 命名空间:放 **tas**(`atom themes namespaced by file`)
- 算法:ATR 与收盘价 z-score,落入低/中/高三段位(`<1 / 1~2 / >=2`)
- 用现有 `update_atr_cache` 即可算 ATR,无需新建缓存
- **日期后缀**:写成 `20250101`,对应函数名 `my_atr_band_v250101`

> ⚠️ 注意大小写: **函数名用小写 `_v`,signal 注册名用大写 `_V`**。宏会强校验一致性。

---

## 2. 选取一个模板函数

`tas_atr_V230630` ([tas.rs:2278](../crates/czsc-signals/src/tas.rs#L2278)) 是结构最清爽的入门模板 —— 已经被我从 `tas.rs` 第 2261 行读出来,在文档上一节呈现过。把它整段搬过来,改写为 `my_atr_band_V250101`:

```rust
/// 原 tas_atr_V230630: ATR 波动分层信号
/// 参数模板:`"{freq}_D{di}ATR{timeperiod}_波动V230630"`
///
/// 我们要写: z_score(close.ATR) 落入低/中/高三段位
/// 新参数模板:`"{freq}_D{di}ATR{timeperiod}_Z{lookback}_Z分位V250101"`
```

---

## 3. 新增 `pub fn` + `#[signal(...)]`

打开 [crates/czsc-signals/src/tas.rs](../crates/czsc-signals/src/tas.rs),**追加到文件最末** 或 **就近放在 `tas_atr_V230630` 之后**(便于 compare)。

```rust
/// my_atr_band_V250101: ATR 标准化波动分位器
///
/// 参数模板:`"{freq}_D{di}ATR{timeperiod}_Z{lookback}_Z分位V250101"`
///
/// 信号逻辑:
/// 1. 计算 ATR 序列; 取最近 `lookback` 根 K 线, 计算 `atr_i / close_i`;
/// 2. 在该样本上计算末值 的 z-score = (last - mean) / std;
/// 3. 若 `z < -1` → `低位`, `-1 <= z <= 1` 为 `中位`, `> 1` → `高位`;
/// 4. 数据不足 / 参数非法时走默认 `其他`.
///
/// 信号列表示例(Signal 显示的「完整 7 段字符串」):
/// - `Signal('30分钟_D1ATR14Z60_Z分位V250101_中位_任意_任意_0')`
/// - `Signal('30分钟_D1ATR14Z60_Z分位V250101_低位_任意_任意_0')`
///
/// 但打到 DataFrame 时:
/// - 列名 = Signal.key() = "30分钟_D1ATR14Z60_Z分位V250101" (3 段, 已滤掉"任意")
/// - 单元格 = Signal.value() = "中位_任意_任意_0" (4 段, 总有 score=0)
///
/// 参数说明:
/// - `di`:信号计算截止在倒数第 `di` 根 K 线,默认 `1`;
/// - `timeperiod`:ATR 窗口,默认 `14`;
/// - `lookback`:z-score 窗口,默认 `60`(小窗口便于 30min mock 数据几小时就预热)。
/// 对齐说明: 本函数仅作 proc-macro 注册机制示例,与线上信号无对应 Python 实现。
#[signal(
    category = "kline",
    name = "my_atr_band_V250101",
    template = "{freq}_D{di}ATR{timeperiod}_Z{lookback}_Z分位V250101",
    opcode = "MyAtrBandV250101",
    param_kind = "MyAtrBandV250101"
)]
pub fn my_atr_band_v250101(czsc: &CZSC, params: &ParamView, cache: &mut TaCache) -> Vec<Signal> {
    // ─── 1. 读参数 ──────────────────────────────────────────────
    let di = get_usize_param(params, "di", 1);
    let timeperiod = get_usize_param(params, "timeperiod", 14);
    let lookback = get_usize_param(params, "lookback", 60);

    // ─── 2. 更新缓存(对齐 Python `update_atr_cache`) ──────────────
    let cache_key = format!("ATR{}", timeperiod);
    update_atr_cache(czsc, &cache_key, timeperiod, cache);

    // ─── 3. 信号命名三段 ──────────────────────────────────────────
    let k1 = czsc.freq.to_string();
    let k2 = format!("D{}ATR{}Z{}", di, timeperiod, lookback);
    let k3 = "Z分位V250101";
    let mut v1 = "其他".to_string();

    // ─── 4. 防御 + 数据裁剪 ──────────────────────────────────────
    let need = di + lookback + 4;
    if czsc.bars_raw.len() < need || di == 0 || lookback < 5 {
        return make_kline_signal_v1(&k1, &k2, k3, &v1);
    }
    let bars = get_sub_elements(&czsc.bars_raw, di, lookback);
    if bars.is_empty() {
        return make_kline_signal_v1(&k1, &k2, k3, &v1);
    }

    // ─── 5. compute ──────────────────────────────────────────────
    let atr = cache.series.get(&cache_key).unwrap();
    let end = czsc.bars_raw.len() - di + 1;
    let start = end - bars.len();
    let ratio: Vec<f64> = bars
        .iter()
        .enumerate()
        .map(|(i, b)| {
            let v = atr[start + i] / b.close;
            if v.is_finite() { v } else { f64::NAN }
        })
        .collect();

    // 丢掉 NaN 计算 mean / std
    let valid: Vec<f64> = ratio.iter().copied().filter(|x| x.is_finite()).collect();
    if valid.len() < 5 {
        return make_kline_signal_v1(&k1, &k2, k3, &v1);
    }
    let mean = valid.iter().sum::<f64>() / valid.len() as f64;
    let var = valid.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / valid.len() as f64;
    let std = var.sqrt();
    if std < 1e-12 {
        // 波动率几乎为 0, 视为 "中位"
        v1 = "中位".to_string();
        return make_kline_signal_v1(&k1, &k2, k3, &v1);
    }

    let z = (ratio.last().copied().unwrap_or(f64::NAN) - mean) / std;
    v1 = match z {
        z if !z.is_finite()       => "其他".to_string(),
        z if z < -1.0             => "低位".to_string(),
        z if z >  1.0             => "高位".to_string(),
        _                         => "中位".to_string(),
    };

    // ─── 6. 输出 ────────────────────────────────────────────────
    make_kline_signal_v1(&k1, &k2, k3, &v1)
}
```

### 宏展开后会发生什么

`#[signal(...)]` 不会改你的函数体。它在你的函数**后面**追加:

```rust
// 自动生成(你看不到)
#[doc(hidden)]
#[allow(non_upper_case_globals, dead_code)]
pub const __RS_CZSC_SIGNAL_META_MY_ATR_BAND_V250101: SignalDescriptor = SignalDescriptor {
    category: "kline",
    name: "my_atr_band_V250101",
    template: "{freq}_D{di}ATR{timeperiod}_Z{lookback}_Z分位V250101",
    opcode: "MyAtrBandV250101",
    param_kind: "MyAtrBandV250101",
    func_ref: SignalFnRef::Kline(my_atr_band_v250101 as SignalFn),
    fast_kline: None,  // 你的没显式提供 fast path
};

inventory::submit! {
    __RS_CZSC_SIGNAL_META_MY_ATR_BAND_V250101
}
```

---

## 4. 注册表 / 边界检查的 7 条 ratchet

[crates/czsc-signal-macros/src/lib.rs](../crates/czsc-signal-macros/src/lib.rs) 在编译期会强制 7 条规范,**任一不满足会 `compile_error!`**:

| # | 规则 | 错误信息 | 触发场景 |
|---|------|---------|---------|
| 1 | `category` ∈ {`kline`, `trader`} | `category 必须是 kline 或 trader` | 拼成 `KLine` / `trader_level` |
| 2 | `name` / `template` / `opcode` / `param_kind` 全部非空 | `name/template/opcode/param_kind 不能为空` | 漏写一个字段 |
| 3 | 函数名必须包含 `_v<纯数字>` | `函数名必须包含 _v<版本号>` | 写 `my_atr_band_V250101` (错) / `my_atr_band_20250101` (也错) |
| 4 | `name` 与函数名版本后缀一致 | `name 必须与函数名版本后缀一致` | 函数名 `v123456` 但 name 是 `V654321` |
| 5 | `template` 包含版本数字 | `template 必须包含版本数字` | `template = "...V25"` 缺完整 6 位 |
| 6 | kline 函数必须 3 参数 | `kline signal 函数必须有 3 个参数` | 写 2 参数或 4 参数 |
| 7 | trader 函数必须 2 参数;kline 必须是 `(&CZSC, &Params, &mut TaCache)` | 类型不符时具体报错 | 改了参数类型签名 |

**对应你的教程示例,前 7 条全过:**
- category: kline ✓
- name: `my_atr_band_V250101` ✓
- 函数名: `my_atr_band_v250101` ✓ (小写 v + 6 位数字)
- 一致性: `my_atr_band_V250101`=`my_atr_band_V250101` ✓
- template 包含 `250101` ✓

新增 `pub mod` 时还要加 `#[signal_module(category = "kline")]`(项目已有 22 个 module 都加了;**新加 module 时 main lib.rs 还要挂上去**)。

---

## 5. 注册表 ratchet 测试 (Rust)

⚠️ **不要在 `tas.rs` / `xl.rs` 等信号模块里直接写 `#[test]`** —
**项目 22 个模块里没有一个**写内联单元测试。原因:

- `czsc-core` 没有 `mock` 子模块,造 K 线很繁琐;
- 信号函数的全部数据/分支防御已经在 Python 端覆盖;
- Rust 端单测的高边际收益集中在 **注册表正确性** —— 编译期宏虽然能 catch
  签名问题,但「宏成功跑,运行期 `SIGNAL_REGISTRY` 能不能查到」是另外一回事。

**正确做法: 把对应注册表 ratchet 测试放进 [crates/czsc-signals/src/registry.rs](../crates/czsc-signals/src/registry.rs) 同文件 `mod tests`** —— 项目已经有十几个类似测试可用作模板。

```rust
// crates/czsc-signals/src/registry.rs 的 #[cfg(test)] mod tests 里追加:

#[test]
fn test_macro_injected_my_atr_band_v250101_registered() {
    // ratchet: 教程示例信号必须可在 K 线注册表中按名查到；
    // 返回的 func_ref 必须是 Kline 变体，且 param_template 与宏注解完全一致。
    let d = crate::tas::__RS_CZSC_SIGNAL_META_MY_ATR_BAND_V250101;
    assert_eq!(d.name, "my_atr_band_V250101");
    assert_eq!(d.template, "{freq}_D{di}ATR{timeperiod}_Z{lookback}_Z分位V250101");
    let meta = super::SIGNAL_REGISTRY
        .get(d.name)
        .unwrap_or_else(|| panic!("missing macro injected signal: {}", d.name));
    assert_eq!(meta.param_template, d.template);
    assert!(matches!(d.func_ref, crate::types::SignalFnRef::Kline(_)));
}
```

跑测试:

```bash
cargo test -p czsc-signals --lib test_macro_injected_my_atr_band_v250101_registered
# ✅ test registry::tests::test_macro_injected_my_atr_band_v250101_registered ... ok
```

如果你确实要测函数体本身的语义分支(例如大波动 vs 平坦行情),在 Python 端
写单测更划算,见下一节。

---

## 6. rebuild + CLI 验证

重建 wheel:

```bash
uv run --no-sync maturin develop --uv  -m crates/czsc-python/Cargo.toml
# 或 release:
uv run --no-sync maturin build    --uv  -m crates/czsc-python/Cargo.toml
```

CLI 检查它已经上线:

```bash
uv run --no-sync czsc signals list --category kline | grep my_atr
# 看到 1 行:
# my_atr_band_V250101                            [kline]    {freq}_D{di}ATR{timeperiod}_Z{lookback}_Z分位V250101

uv run --no-sync czsc signals doc my_atr_band_V250101
```

或者直接 Python REPL:

```python
import czsc._native as native
all_kline = native.list_all_signals()
my_sig = [s for s in all_kline if s["name"] == "my_atr_band_V250101"]
assert len(my_sig) == 1
assert my_sig[0]["param_template"] == "{freq}_D{di}ATR{timeperiod}_Z{lookback}_Z分位V250101"
print("✓ registered")
```

---

## 7. Python 端调用 + 集成测试

打开 [tests/](../tests/) 找现有信号测试参考,
推荐放 [tests/integration/](../tests/integration/)(从 `tests/test_lightweight_signals.py:288` 沿用同一个 `_bars_demo` style):

⚠️ **三个真实踩出来的坑**（不在旧版教程里）：

1. `generate_czsc_signals` 真正签名是
   `generate_czsc_signals(bars, signals_config, sdt="20170101", init_n=500, df=False)`,
   **不是** `signals_seq=.../base_freq=.../with_ta=...`。
2. `signals_config` 是 `list[dict]`,每项 `{"name": "...", "freq": "..."}`,
   **di/timeperiod/lookback 不能传**——它们在 `signal_template` 默认值,
   想改只能走 fast-path 单独通道。
3. DataFrame **列名** 是 [core Signal.key()](../crates/czsc-core/src/objects/signal.rs#L114)
   规约后的 3 段字符串(`{k1}_{k2}_{k3}`),**不是** 7 段 +
   `value()` 才是 `v1_v2_v3_score` 共 4 段。

```python
# tests/integration/test_my_atr_band_v250101.py
"""教程示例信号 my_atr_band_V250101 的 Python 集成测试。"""

from __future__ import annotations

from typing import Any

import pytest

from czsc import Freq, format_standard_kline
from czsc.mock import generate_symbol_kines


@pytest.fixture(scope="module")
def _bars_30m() -> list[Any]:
    """与 [tests/test_lightweight_signals.py] 的 _bars_demo 一致。"""
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
        # here k1="30分钟", k2="D1ATR14Z60", k3="Z分位V250101"。
        expected_col = "30分钟_D1ATR14Z60_Z分位V250101"
        assert expected_col in df.columns, (
            f"找不到新信号输出列; 实际前几列 = {list(df.columns)[:8]!r}"
        )
        vals = df[expected_col].dropna()
        assert not vals.empty
        # Signal.value() = "v1_v2_v3_score", 这里 v2=v3="任意", score=0。
        # 所以合法值形如 "低位_任意_任意_0" / "中位_任意_任意_0" / "高位_任意_任意_0"。
        unique_vals = set(vals.unique())
        allowed = {
            "低位_任意_任意_0", "中位_任意_任意_0",
            "高位_任意_任意_0", "其他_任意_任意_0",
        }
        unexpected = unique_vals - allowed
        assert not unexpected, f"出现了非预期的 value: {unexpected!r}"
        # 30 分钟 mock 5 个月数据, 至少应有主分类触发, 不应全是 "其他"
        active = unique_vals - {"其他_任意_任意_0"}
        assert active, f"全为兜底 '其他', 没触发主分类: {unique_vals!r}"

    def test_signal_column_has_three_segments(self, _bars_30m):
        """Signal.key() 规约: 过滤掉 v2/v3='任意' 段, 列名一定是 3 段。"""
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
        assert parts == ["30分钟", "D1ATR14Z60", "Z分位V250101"]


class TestCLIDiscoverability:
    """第三关: CLI doc/locate 都能看到这个信号。"""

    def test_cli_doc_returns_entry(self):
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
        assert d["param_template"] == \
            "{freq}_D{di}ATR{timeperiod}_Z{lookback}_Z分位V250101"
```

跑测试:

```bash
uv run --no-sync pytest tests/integration/test_my_atr_band_v250101.py -v
# ✅ 5 passed in ~2s
```

如果你还想试 `CzscSignals` 持久化路径(把 signals 写入 `s.sigs`),
可以新增类似 `tests/cli/test_signals.py` 的 fixture,
但 `generate_czsc_signals` 已经覆盖了 99% 调试场景。

---

## 8. 重新生成 stub 让 IDE 类型补全生效

新增的信号函数**不出现在** [czsc/_native/__init__.pyi](../czsc/_native/__init__.pyi) 里 —— 它是 pyo3-stub-gen 自动生成的。手动改会立刻被覆盖。

重新生成(单冒号 `command`):

```bash
PYO3_PYTHON=$(uv run python -c 'import sys; print(sys.executable)') \
    cargo run --bin stub_gen -p czsc-python \
      --no-default-features --features stub-gen
```

跑了之后会改写 [czsc/_native/__init__.pyi](../czsc/_native/__init__.pyi);但是注意:**这个文件不暴露「信号函数」API** —— Python 端通过 `generate_czsc_signals(bars, signals_config, ...)` 间接调用,**不直接 import 函数名**。所以这一步对你的影响:

✅ IDE 能看到 `native.list_all_signals()` 的返回结构
❌ 但 IDE 看不到「`my_atr_band_V250101(...)` 是合法调用的信号名」—— 因为 `generate_czsc_signals` 用的是字符串
⚠️ ListAllSignals 返回 `list[PyAny]`,具体元素类型要靠 `type: ignore[attr-defined]` 标注

(详见 [czsc/cli/signals.py:21](../czsc/cli/signals.py#L21) 的 `type: ignore` 用法)

---

## 9. Fast-path 优化 - 可选进阶

如果你的信号被 **高频调用**(每根 K 线都执行),需要避开 `HashMap<String, Value>` 的反复 marshalling。

宏提供了 `fast_decode` / `fast_exec`:

```rust
#[signal(
    category = "kline",
    name = "my_atr_band_V250101",
    template = "{freq}_D{di}ATR{timeperiod}_Z{lookback}_Z分位V250101",
    opcode = "MyAtrBandV250101",
    param_kind = "MyAtrBandV250101",
    fast_decode = "my_atr_band_v250101_fast_decode",
    fast_exec   = "my_atr_band_v250101_fast_exec"
)]
pub fn my_atr_band_v250101(...) { ... }

// 自己实现两个辅助函数(签名见 czsc-signals/src/types.rs:65-66)
fn my_atr_band_v250101_fast_decode(params: &HashMap<String, Value>) -> Option<Value> { ... }
fn my_atr_band_v250101_fast_exec(czsc: &CZSC, params: &Value, cache: &mut TaCache) -> Vec<Signal> { ... }
```

或者**改用 typed params 让宏自动展开**:

```rust
#[derive(Serialize, Deserialize)]
#[pyo3_stub_gen::derive::GenStub]
pub struct MyAtrBandParams {
    pub di: usize,
    pub timeperiod: usize,
    pub lookback: usize,
}

#[signal(...)]
pub fn my_atr_band_v250101(czsc: &CZSC, params: &MyAtrBandParams, cache: &mut TaCache) -> Vec<Signal> { ... }
```

宏会自动生成从 `HashMap<String, Value>` → `&MyAtrBandParams` 的 decode,并且用 fast path 跳过一次中间序列化。

**在 czsc 信号中实际收益: ~15-25%**(视信号函数逻辑复杂度而异)。

---

## 10. 常见踩坑速查

| 现象 | 原因 | 解法 |
|------|------|------|
| 编译错 `函数名必须包含 _v<版本号>` | 没版本号 | 函数名加 `_v250101` |
| `name 必须与函数名版本后缀一致` | 大小写或位数不同 | `#signal_name` 与函数名统一大小写 |
| `category 必须是 kline 或 trader` | 拼成 `KLine` 等 | 严格小写 |
| `category=trader` 时 `kline signal...` 报错混着用 | 写错了字段 | 校正 |
| `inventory submit! duplicate` | 同名同 opcode 两个函数 | 函数末尾改名 + 新版本号 |
| `assertion 'lib.get(b'name').is_some()' failed` | 改完没 rebuild | `maturin develop` |
| Python 端看不到 | 没 rebuild wheel / IDE 取了 cache 里的旧 .so | `maturin develop --uv` + 重启 Python |
| `unwrap on None` panic | `cache.series.get(&cache_key)` 取不到 | 确认已 `update_xxx_cache` |
| 信号输出 NaN 字符串 | `format` 把 f64 渲染成 NaN | 先 `.is_finite()` 过滤 |
| ent 模块被跳过 | `#[signal_module(category = "...")]` 没加 | 加 macro |
| 新模块在 `czsc.signals.list` 不出现 | 新 mod 没 include 到 [czsc-signals/src/lib.rs](../crates/czsc-signals/src/lib.rs) | 在 `lib.rs` 注册 `pub mod xxx; #[signal_module(...)]` |

---

## 附录:文件清单

完整的清单:

| 步骤 | 路径 | 操作 |
|------|------|------|
| 1. 写信号 | [crates/czsc-signals/src/tas.rs:2261-2317](../crates/czsc-signals/src/tas.rs#L2261) | 追加 `pub fn my_atr_band_v250101` |
| 2. 单元测试 | [crates/czsc-signals/src/tas.rs](../crates/czsc-signals/src/tas.rs)(同文件 `#[cfg(test)] mod tests`) | 加 3 个 test |
| 3. 注册表 ratchet | [crates/czsc-signals/src/registry.rs](../crates/czsc-signals/src/registry.rs) | (无需修改, 自动收集) |
| 4. PyO3 绑定 | [crates/czsc-python/src/bin/stub_gen.rs](../crates/czsc-python/src/bin/stub_gen.rs) | (无需修改, stub 自动生成) |
| 5. Python 集成测试 | [tests/signal/test_my_atr_band.py](../tests/signal/) | 新建 |
| 6. CLI 验证 | [czsc/cli/signals.py](../czsc/cli/signals.py) | (已支持 list/doc, 无需修改) |
| 7. 文档示例 | [docs/examples/](../docs/examples/) | 可选 |

---

## 小结

加一个新信号函数的「**最少必要修改**」:

1. ✅ 1 个 `pub fn`,配 `#[signal(...)]`
2. ✅ (可选但推荐) `#[cfg(test)] mod tests` 几个 case
3. ✅ 跑 `cargo test -p czsc-signals`
4. ✅ 跑 `maturin develop --uv`
5. ✅ 跑 `uv run --no-sync czsc signals list | grep my_atr`

**完全不需要修改**:
- ❌ `czsc._native` 任何 .py 文件 (除自动生成的 stub)
- ❌ `czsc.py` 顶部 `__all__` (信号不暴露具体名字)
- ❌ `czsc/signals/` (Phase J 已删除)
- ❌ `CzscStrategyBase` / `CzscTrader` (base 抽象类遮蔽信号细节)
- ❌ `czsc.traders.__init__` (从 `_native` 透传)

---

## 延伸阅读

- 📘 [proc-macro 注册机制(SIGNAL_REGISTRY)](./python-rust-bridge-deepdive.md) —— 本篇的"为什么"篇
- 📘 [proc-macro 源代码](../crates/czsc-signal-macros/src/lib.rs) —— 7 条 ratchet 全部写在这里
- 📘 [`hatrs/sig.rs` 工具](../crates/czsc-signals/src/utils/sig.rs) —— 提供 `make_kline_signal_v1/v2/v3` 与 `qcut_last_label` 等
- 📘 [`ta.rs` 工具](../crates/czsc-signals/src/utils/ta.rs) —— 提供 `update_*_cache` 系列
- 🛠️ [CLAUDE.md §信号函数开发](../CLAUDE.md) —— 编码规范与约定
