# Python ↔ Rust 数据流转机制详解

> CZSC 项目采用 **PyO3 + maturin** 实现 Python 与 Rust 的双向互通。本文档从底层机制到实际数据流逐层展开,作为开发与 review 时的参考手册。

> 📘 姊妹篇:
> - [proc-macro 注册机制(SIGNAL_REGISTRY)](./python-rust-bridge-deepdive.md) 讲解 `#[signal]` 如何把 220+ 函数零手工注册到全局表
> - [零拷贝优化(Arrow IPC)](./python-rust-bridge-zerocopy.md) 讲解 `pd.DataFrame` 如何以 IPC bytes + Polars 的方式避免逐列拷贝传给 Rust
> - [GIL 与多线程踩坑实录](./python-rust-bridge-gil.md) 讲解 PyO3 项目里 GIL/线程池/rayon 各种「明明没改代码却卡死」的根因与解法
> - [手把手加一个新信号函数](./add-a-new-signal.md) 实战:从 `pub fn` 到 Python `pytest` 15 分钟走通完整流程

---

## 目录

- [架构总览](#架构总览)
- [构建机制：maturin 把 Rust 编译成 Python 扩展](#构建机制maturin-把-rust-编译成-python-扩展)
- [三种典型数据流转场景](#三种典型数据流转场景)
  - [场景 1：Python 传 pandas DataFrame → Rust](#场景-1python-传-pandas-dataframe--rust)
  - [场景 2：Rust 返回 `#[pyclass]` 对象给 Python 长期持有](#场景-2rust-返回-ppyclass-对象给-python-长期持有)
  - [场景 3：Rust 内部计算，要释放 GIL](#场景-3rust-内部计算要释放-gil)
- [完整调用链示例](#完整调用链示例)
- [类型映射对照表](#类型映射对照表)
- [项目特有的工程约束](#项目特有的工程约束)
- [调试工具](#调试工具)

---

## 架构总览

```
┌──────────────────────────────────────────────────────────┐
│  Python 用户代码                                            │
│    from czsc import CZSC, Freq                            │
│    from czsc.mock import generate_symbol_kines            │
├──────────────────────────────────────────────────────────┤
│  Python 顶层 facade (czsc/__init__.py + 子模块)             │
│    纯透传: from czsc._native import CZSC, Freq, ...      │
├──────────────────────────────────────────────────────────┤
│  PyO3 扩展模块  czsc._native  (由 maturin 构建)            │
│    - #[pyclass] 装饰的 Rust 类型                            │
│    - #[pyfunction] 暴露的 Rust 函数                          │
│    - 自动类型转换 (GIL 管理 + 类型 marshal)                   │
├──────────────────────────────────────────────────────────┤
│  Rust crate (czsc / czsc-core / czsc-signals / czsc-trader)│
│    纯 Rust 实现，无 Python 依赖                               │
└──────────────────────────────────────────────────────────┘
```

---

## 构建机制：maturin 把 Rust 编译成 Python 扩展

`crates/czsc-python/Cargo.toml`（简化）：

```toml
[lib]
name = "czsc_python"
crate-type = ["cdylib"]                    # ← 关键：产生动态库

[dependencies]
pyo3 = { version = "0.22", features = ["extension-module"] }
pyo3-stub-gen = "..."                       # ← 自动生成 .pyi stub
```

`pyproject.toml`（简化）：

```toml
[build-system]
requires = ["maturin>=1.0"]
build-backend = "maturin"

[tool.maturin]
module-name = "czsc._native"               # ← Python import 路径
features = ["pyo3/extension-module"]
```

构建产物在 `.venv/lib/python3.x/site-packages/czsc/_native/` 下：

| 平台 | 文件 |
|------|------|
| Linux | `_native.abi3.so` |
| macOS | `_native.cpython-310-darwin.so` |
| Windows | `_native.cp310-win_amd64.pyd` |

这些 `.so` / `.pyd` 文件就是 Python 通过 `import czsc._native` 加载的扩展模块。

---

## 三种典型数据流转场景

### 场景 1：Python 传 pandas DataFrame → Rust

这是最常见的场景（K 线数据）。

**Python 端调用**：

```python
from czsc.mock import generate_symbol_kines
from czsc import format_standard_kline, Freq

df = generate_symbol_kines("000001", "30分钟", "20240101", "20240105")
#       dt                  open    high    low     close   vol
# 2024-01-02 09:30  9.50   9.55   9.48   9.52   12345
# ...

bars = format_standard_kline(df, freq=Freq.F30)
```

**Rust 端接收**（典型 PyO3 写法）：

```rust
// crates/czsc-python/src/lib.rs 或对应 binding 文件
use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pyfunction]
fn format_standard_kline<'py>(
    py: Python<'py>,
    df: &Bound<'py, PyAny>,        // ← 接收任意 Python 对象
    freq: &str,
) -> PyResult<Vec<RawBar>> {
    // 关键步骤 1：GIL 必须在持有状态下访问 Python 对象
    // 关键步骤 2：从 PyAny 提取字段
    let df_dict: &Bound<PyDict> = df.downcast()?;

    let dt     = df_dict.get_item("dt")?.extract::<Vec<i64>>()?;
    let open   = df_dict.get_item("open")?.extract::<Vec<f64>>()?;
    let high   = df_dict.get_item("high")?.extract::<Vec<f64>>()?;
    let low    = df_dict.get_item("low")?.extract::<Vec<f64>>()?;
    let close  = df_dict.get_item("close")?.extract::<Vec<f64>>()?;
    let vol    = df_dict.get_item("vol")?.extract::<Vec<f64>>()?;

    // 关键步骤 3：组织成 Rust 强类型 Vec<RawBar>
    let bars = dt.iter().zip(open.iter()).map(|(d, o)| RawBar {
        dt: *d, open: *o, /* ... */
    }).collect();

    Ok(bars)
}
```

**原理图解**：

```
Python DataFrame (heap-allocated Python objects)
        │
        │  ① PyO3 拿到 PyAny* 指针（持有 GIL）
        ▼
PyAny (PyO3 中间表示，未类型化)
        │
        │  ② downcast + extract::<Vec<f64>>()
        │    PyO3 内部调用 PyArg_ParseTuple
        │    Python list → Rust Vec<f64> (malloc 重分配，复制)
        ▼
Rust Vec<RawBar> (栈/堆上的原生 Rust 数据)
        │
        │  ③ 内部算法处理（不再有 Python 对象引用，可释放 GIL）
        ▼
Rust 内部算法运行
```

> ⚠️ **性能关键点**：第 ② 步是 **复制到 Rust 堆**（不是零拷贝）。这一步通常占整个调用耗时的 60-80%。如果 DataFrame 很大，要考虑用 Arrow IPC 零拷贝方案。

---

### 场景 2：Rust 返回 `#[pyclass]` 对象给 Python 长期持有

这是 CZSC 核心的运行模式：`CZSC(bars)` 返回一个 Python 对象，后续 Python 反复调用其方法。

**Rust 定义**：

```rust
// crates/czsc-core/src/czsc.rs (简化)
use pyo3::prelude::*;

#[pyclass]
#[derive(Clone)]
pub struct CZSC {
    bars: Vec<RawBar>,
    freq: Freq,
    fxs: Vec<FX>,
    bis: Vec<BI>,
    // ...
}

#[pymethods]
impl CZSC {
    #[new]
    fn new(bars: Vec<RawBar>, freq: Freq) -> PyResult<Self> {
        let mut czsc = CZSC { bars, freq, fxs: vec![], bis: vec![] };
        czsc.recalculate()?;
        Ok(czsc)
    }

    #[getter]
    fn bis(&self) -> Vec<BI> { self.bis.clone() }

    #[getter]
    fn fxs(&self) -> Vec<FX> { self.fxs.clone() }

    fn get_bi_info(&self) -> Vec<BiInfo> { /* ... */ }
}
```

**Python 端使用**：

```python
from czsc import CZSC, Freq

c = CZSC(bars, freq=Freq.F30)
#    ↑ CZSC 是 Python 对象，实际包裹的是 Rust CZSC struct
#      Python 端 c 是个 capsule（PyObject*），指向 Rust heap 上的 CZSC

print(c.bis)            # 触发 #[getter] bis()，克隆 Vec<BI> 转 Python list
print(c.fxs)            # 同上
print(c.get_bi_info())  # 调用 #[pymethods] 方法
```

**生命周期机制**：

```
Python c ──持有──→ PyObject (在 Python 堆上持有引用)
                       │
                       │ 指向
                       ▼
                  Arc<Mutex<...>>  (Rust 端实际数据)
                       │
                       │ 实现 Drop trait
                       ▼
                  当 Python GC 收回 PyObject 时，
                  PyO3 自动调用 CZSC 的 drop()，释放 Rust 堆
```

> ⚠️ **坑点**：如果 Rust 在算法内部保存了 Python 对象引用 (`Py<PyAny>`)，必须正确管理 `Python<'py>` 生命周期；否则会出现 dangling pointer 或 GIL 死锁。

---

### 场景 3：Rust 内部计算，要释放 GIL

**对 CPU 密集型计算必须显式 `py.allow_threads(|| { ... })`**：

```rust
#[pyfunction]
fn generate_czsc_signals<'py>(
    py: Python<'py>,
    bars: Vec<RawBar>,
    signals_seq: Vec<Vec<String>>,
) -> PyResult<Vec<Signal>> {
    // 关键：释放 GIL，让其他 Python 线程可运行
    let result = py.allow_threads(|| {
        // 这段代码可以纯 Rust 跑，不持有 GIL
        // 即使在这里 sleep 或者做大计算，也不会阻塞其他 Python 线程
        heavy_computation(bars, signals_seq)
    });

    Ok(result)
}
```

**重要原则**：

- 调用 `.extract::<T>()`、`.downcast()`、`Bound` 操作时 **必须持有 GIL**
- 纯 Rust 计算（已经是 `Vec<f64>` 之类的原生数据）→ 应该放进 `allow_threads` 内
- 如果不释放 GIL，100% CPU 的 Rust 算法会阻塞 Python 主线程

---

## 完整调用链示例

以 `czsc.traders.generate_czsc_signals(c, signals_seq)` 为例：

```
1. Python 调用
   c               ← PyObject, 内部是 Rust CZSC struct
   signals_seq     ← list[list[str]]
        │
        ▼
2. PyO3 自动类型 marshal
   c        ──→ &CZSC         (零成本，仅指针 + 引用计数)
   sig_seq  ──→ Vec<Vec<String>>  (Python list → 拷贝到 Rust Vec)
        │
        ▼
3. Rust 内部 (py.allow_threads 内)
   - 遍历 signals_seq 调用 #[signal] 注册的函数
   - 每个信号函数接收 RawBar slice 或 CZSC 引用，返回 Signal
   - SIGNAL_REGISTRY 查找 (HashMap<String, fn(...) -> Signal>)
        │
        ▼
4. Rust 计算完毕
        │
        ▼
5. PyO3 重新拿回 GIL
   Vec<Signal> ──→ Python list[Signal] (再次拷贝，但只对返回值)
        │
        ▼
6. Python 拿到结果
   signals = generate_czsc_signals(c, ["Signal1", "Signal2"])
   signals[0].value  ← 触发 #[getter]，返回 Python str/int/float
```

---

## 类型映射对照表

PyO3 提供 `FromPyObject` / `IntoPy` 自动转换核心类型，但要小心性能。

| Python 类型 | Rust 类型 (FromPyObject) | 转换成本 | 说明 |
|------------|------------------------|---------|------|
| `int` | `i64`, `i32`, `usize` | 极低 | 直接复制 |
| `float` | `f64`, `f32` | 极低 | 直接复制 |
| `str` | `String`, `&str` | 低 | UTF-8 校验 + 拷贝 |
| `bytes` | `Vec<u8>`, `&[u8]` | 极低 | 零拷贝（PyBytes 是 buffer protocol） |
| `list[T]` | `Vec<T>` | **高** | 递归拷贝每个元素 |
| `dict[K,V]` | `HashMap<K,V>` / `PyDict` | **高** | 递归拷贝 |
| `pd.DataFrame` | `&PyAny` (手动解析) | **高** | 必须手动 `extract`，PyO3 不识别 DataFrame |
| `np.ndarray` (contiguous) | `&PyArray<T>` (需 `numpy` feature) | **零拷贝** (只读) / 低 (写入) | 仅当 contiguous + 全本机字节序时零拷贝 |
| `None` | `Option<T>` | 极低 | — |
| 自定义 `#[pyclass]` | `&T` / `T` (Clone) | 极低（仅引用计数） | PyO3 自动识别 |

---

## 项目特有的工程约束

翻看 `CLAUDE.md` 第一条宪法，对这套机制有非常严格的要求：

> **同一份输入，Rust 用户和 Python 用户的输出必须 byte-for-byte 一致。Python 端只允许做 ① 纯透传、② 不可避免的 PyO3 边界胶水（如 DataFrame ↔ PyDict 解析）。**

**禁止的"适配层"信号**：

```python
# ❌ 绝对禁止
def my_strategy(czsc_obj, signals):
    if isinstance(czsc_obj, dict):        # ← 多态分支
        czsc_obj = convert(czsc_obj)
    elif isinstance(czsc_obj, CZSC):
        czsc_obj = unwrap(czsc_obj)
    # ...
```

这种工作必须 **下沉到 Rust 端用 enum 表达**，让 Python 端零分支。

**Checklist（review 红线）**：

- ✅ Python 函数体只有 `from czsc._native import xxx; return xxx(...)`
- ✅ 如果有 `&PyAny` 提取，注释清楚是「不可避免的 PyO3 边界胶水」
- ❌ 出现 `isinstance(x, pd.DataFrame): ... elif isinstance(x, list): ...`

---

## 调试工具

几个开发时常用的技巧：

```bash
# 1. 看 PyO3 类型 stub（自动生成的）
cat czsc/_native/__init__.pyi

# 2. 看实际扩展符号
uv run --no-sync python -c "import czsc._native; print(dir(czsc._native))"

# 3. 用 sys.getrefcount 看 Python ↔ Rust 对象引用
uv run --no-sync python -c "
import sys, czsc
from czsc.mock import generate_symbol_kines
from czsc import CZSC, Freq
df = generate_symbol_kines('000001', '30分钟', '20240101', '20240105')
c = CZSC(format_standard_kline(df, freq=Freq.F30), freq=Freq.F30)
print('refs to c:', sys.getrefcount(c))
"

# 4. 卡顿 / 死锁时检查 GIL
#   PyO3 在 allow_threads 内不会响应 KeyboardInterrupt；
#   如果你 ctrl-c 不响应，看看是不是 Python 线程在等待 Rust 释放 GIL。
```

---

## 延伸阅读

```bash
# 信号函数注册宏
ls crates/czsc-signals/src/                        # 22 个 .rs 信号源文件
cat crates/czsc-signal-macros/src/lib.rs           # #[signal] proc-macro 实现

# 核心类型 PyO3 声明
ls crates/czsc-python/src/
grep -rn "#\[pyclass\]\|#\[pyfunction\]" crates/czsc-core/src/

# 类型 stub（自动生成，对应所有 Python 可见的 API）
head -100 czsc/_native/__init__.pyi
```

---

## 姊妹篇索引<a name="姊妹篇索引"></a>

本教程从「Python ↔ Rust 数据流转」的不同角度拆开讲,推荐读法:**本文档先通读,对结构有概念后按需点对应姊妹篇**。下表给一份**角色/难度/何时读**速查。

| # | 文档 | 主题 | 主要回答什么 | 何时读 |
|---|------|------|-------------|--------|
| 0th | 📍 **本文档** | 总览 / 类型映射 / 工程约束 | "PyO3 边界到底怎么 marshal,Python 端的硬规则是什么" | 第一次上手、review 他人 PR |
| 1st | [proc-macro 注册机制(SIGNAL_REGISTRY)](./python-rust-bridge-deepdive.md) | 注册表机制:LazyLock + inventory::submit + fn-pointer 表 | "220+ 信号函数是怎么自动注册、不需手工名单的" | 你要新加信号、新加 enum 类别 |
| 2nd | [零拷贝优化(Arrow IPC)](./python-rust-bridge-zerocopy.md) | DataFrame → Polars 的零拷贝路径,polars 给 arrow-rs 做 shell | "10 万行 K 线怎么传到 Rust 不复制 60 万次" | 你的回测 / 大数据 IO 慢、调试 memory |
| 3rd | [GIL 与多线程踩坑实录](./python-rust-bridge-gil.md) | 项目并发姿态:几乎没有 `allow_threads`,唯一并行点在 `optimize.rs` | "rayon `pool.install(...)` 持有 GIL 启动几万线程怎么补救" | 死锁 / 卡住 / `allow_threads` 引发的随机超时问题 |
| 4th | 🛠️ **[手把手加一个新信号函数](./add-a-new-signal.md)** | 实战:60 行 Rust + 5 个 Python 测试 = 跑通一个真实例子 | "给我一个能跑通的最小完整示例" | 你要新加信号函数,先照抄模板 |

> 共 5 篇,主题互相引用(每篇头部都有快速姊妹篇速链)。
- 🔬 **[proc-macro 注册机制（SIGNAL_REGISTRY）](./python-rust-bridge-deepdive.md)**
- 🚀 **零拷贝优化**（如何用 Arrow IPC 让 DataFrame → Rust 不复制）
