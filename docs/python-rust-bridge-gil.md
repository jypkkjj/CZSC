# GIL 与多线程踩坑实录

> 本篇是 [Python ↔ Rust 数据流转机制详解](./python-rust-bridge.md) 系列的第四篇。
> CZSC 是 **典型 PyO3 项目**—— Rust 端占用了大部分 CPU,但又被 GIL 锁住了。
> 老 PyO3 教程只看 PyO3 API 表面,本书直接拆解 CZSC 真实代码、`optimize.rs` 注释里的血泪、调度器里的 rayon 嵌套陷阱。

> 📘 姊妹篇:
> - 🚪 [总览:Python ↔ Rust 数据流转机制详解](./python-rust-bridge.md) —— PyO3 三种典型场景 / 类型映射表 / 调试工具
> - 🌱 [proc-macro 注册机制(SIGNAL_REGISTRY)](./python-rust-bridge-deepdive.md) —— `#[signal]` 也是个 `LazyLock<HashMap>`,GIL 与多线程行为要特别照顾
> - 🚄 [零拷贝优化(Arrow IPC → Polars)](./python-rust-bridge-zerocopy.md) —— 大数据流跨 GIL 的零拷贝路径
> - 🛠️ **[手把手加一个新信号函数](./add-a-new-signal.md)** —— 60 行 Rust + 5 个 Python 测试 = 跑通一个真实教程示例(结合本篇 GIL 知识最合适)
>
> 📜 索引速查表: [姊妹篇总索引](./python-rust-bridge.md#姊妹篇索引)

---

## 目录

- [0. 项目里的并发姿态——先承认事实](#0-项目里的并发姿态先承认事实)
- [1. 最关键前提:GIL 在 CZSC 里几乎全程持有](#1-最关键前提gil-在-czsc-里几乎全程持有)
- [2. 坑 1:`rayon` 嵌套并行导致「卡住」](#2-坑-1rayon-嵌套并行导致卡住)
- [3. 坑 2:`&[u8]` 借指向 Python bytes,extend 超过 GIL 范围就 UAF](#3-坑-2u8-借指向-python-bytes-extend-超过-gil-范围就-uaf)
- [4. 坑 3:`&mut TaCache` 是独占引用——多 freq 不能共享](#4-坑-3mut-tacache-是独占引用多-freq-不能共享)
- [5. 坑 4:`rayon::par_bridge` 跨 GIL 边界会「静默吞掉错误」](#5-坑-4rayonpar_bridge-跨-gil-边界会静默吞掉错误)
- [6. 坑 5:`ctrl-c` 在 `allow_threads` 不响应](#6-坑-5ctrl-c-在-allow_threads-不响应)
- [7. 坑 6:Python 多线程(`ThreadPoolExecutor`)锁死 Rust](#7-坑-6python-多线程threadpoolexecutor锁死-rust)
- [8. 坑 7:多进程 vs 多线程的选择](#8-坑-7多进程-vs-多线程的选择)
- [9. 决策清单:什么时候动用 `allow_threads`/rayon/sub-interpreter](#9-决策清单什么时候动用-allow_threadsrayonsub-interpreter)
- [10. 调试工具箱](#10-调试工具箱)
- [附录:文件位置速查](#附录文件位置速查)

---

## 0. 项目里的并发姿态——先承认事实

看一下真实使用统计:

```bash
grep -rn "allow_threads\|Python::with_gil\|prepare_freethreaded_python" crates/
# → 只有 example 测试代码 19 条,全在 czsc-python src 里都没有该 API

grep -rn "ThreadPool\|ProcessPool\|concurrent.futures\|asyncio" czsc/ docs/
# → 0 条
```

**实际并发面**:

| 文件 | 机制 | 场景 |
|------|------|------|
| [crates/czsc-trader/src/optimize.rs:339](../crates/czsc-trader/src/optimize.rs#L339) | `symbols.into_par_iter()` | 参数优化批跑,多品种/多参数组合 |
| [crates/czsc-trader/src/optimize.rs:353](../crates/czsc-trader/src/optimize.rs#L353) | `chunks.par_iter()` | 单品种内参数集分割 |
| [crates/czsc-trader/src/engine_v2/scheduler.rs:21](../crates/czsc-trader/src/engine_v2/scheduler.rs#L21) | `into_par_iter()` + `map(...)` | 多 symbol 并发跑引擎(plan-driven) |

**其他所有路径(RTC 信号生成、`CzscTrader.update_signals`、`format_standard_kline`)** 全是 **持有 GIL 的单条线程顺序执行**。

为什么?答案在 [crates/czsc-trader/src/optimize.rs:304](../crates/czsc-trader/src/optimize.rs#L304) 的注释里:

> `单线程下避免 rayon 嵌套并行,防止在某些环境出现卡住`

—— 这是开发组踩过的坑。

---

## 1. 最关键前提:GIL 在 CZSC 里几乎全程持有

每个 `#[pyfunction]` 入口的执行模型几乎全部 = **「进 keep_gil 模型」**。抄个最常用的 `run_research`:

```rust
#[pyfunction]
pub fn run_research<'py>(
    py: Python<'py>,
    bars_bytes: &Bound<'py, PyBytes>,
    config_path: &str,
    ...,
) -> PyResult<String> {
    // ...持有 GIL,做完所有事
    let df = pyarrow_to_df(raw_data)?;             // 持有 GIL
    let bars = format_standard_kline(df, ...)?;    // 持有 GIL
    let plan = ExecutionPlanInput::compile(...)?;  // 持有 GIL
    let out = UnifiedExecEngine::run(&plan, bars, ..., false)?;  // 持有 GIL
    Ok(out)
}
```

进入 rayon 的代码仍然是 **持有 GIL**,这点至关重要:**GIL 不阻碍 Rust 内的 rayon 多线程**——GIL 只是阻止 Python 字节码并发;Rust 侧一旦拿到 `&mut str buffer` 或 `Vec<RawBar>`,它就是 native 数据,rayon 可以放心 `.par_iter()`。

下面的「坑」焦点就是这个交叉点。

---

## 2. 坑 1:rayon 嵌套并行导致「卡住」

**现象**:`optimize.rs` 多线程路径里写了一段:

```rust
let run = || {
    symbols.into_par_iter().for_each(|sym| {
        if let Some(bars) = bars_map.get(&sym) {
            if chunks.len() <= 1 {
                let _ = one_symbol_optim(...);  // fib 字节码计算,纯 Rust
            } else {
                chunks.par_iter().for_each(|chunk_pos| {  // ← 在 rayon 线程中又起 rayon
                    let _ = one_symbol_optim(...)
                });
            }
        }
    })
};

if n_threads > 0 {
    rayon::ThreadPoolBuilder::new().num_threads(n_threads).build()
        .map(|pool| pool.install(run))
        .unwrap_or_else(...) 
} else {
    run();
}
```

`optimize.rs:304` 顶部注释:

> 单线程下避免 rayon 嵌套并行,防止在某些环境出现卡住

**为什么嵌套 rayon 会卡死**?

1. rayon 默认线程池是 **全局 shared pool**(跨 `rayon::ThreadPool` 也会用同一个 work-stealing 队列)
2. `par_iter().for_each` 在 work-stealing 调度时, **worker 可能会从自己持有 rayon scope 的外部任务里去拿另一个 rayon 任务**
3. 在 macOS / Linux 上,系统线程库会触发 **lock 顺序反转** —— A 线程池等 B,B 等 A,不会触发 deadlock detector,看起来就是「一直不结束」
4. 这在 **CI(只有 2-4 core)、concurrent fuzz、密集 IO + CPU 混合** 下极易触发

**项目怎么修的?** 「单线程下不嵌套」:

```rust
if n_threads == 1 {
    // 用同步 for,完全绕过 rayon,防止嵌套
    for sym in symbols {
        if let Some(bars) = bars_map.get(&sym) {
            for chunk_pos in &chunks {
                let _ = one_symbol_optim(...);
            }
        }
    }
    return;
}
```

**碰到这种问题的通用诊断**:

```bash
# 1. 看 syscall 是否真正在跑
py-spy dump --pid <PID>   # 如果看到一个线程卡在 futex / pthread_cond_wait 就是它
strace -p <PID>           # Linux 信号看出现哪种 futex

# 2. thread sanitizer
RUSTFLAGS="-Z sanitizer=thread" cargo +nightly build -p czsc-trader

# 3. 调小 rayon 池
RAYON_NUM_THREADS=2 uv run python your_script.py
```

**规则**: rayon *全局 pool* 不能嵌套用。两种解法:

- ✅ 嵌套内部从 `par_iter` 改成 「同步 collect 到 Vec,再 par_iter」(养线程第一级降级)
- ✅ 使用 `scope_fifo/spawn_fifo`,明确非嵌套
- ❌ 不要在 `par_iter().for_each()` 体里再启一个 `par_iter`

**踩过的同学 —— `engine_v2/scheduler.rs:21` 也是 `into_par_iter().map(...)`**,**但足够简单是一层**,没踩到这个坎。

---

## 3. 坑 2:`&[u8]` 借指向 Python bytes,extend 超过 GIL 范围就 UAF

```rust
#[pyfunction]
fn bad_example<'py>(
    py: Python<'py>,
    bars_bytes: &Bound<'py, PyBytes>,
) -> PyResult<PyObject> {
    let raw: &[u8] = bars_bytes.as_bytes();   // ← 借指针,指向 Python GC heap
    
    // ❌ 错: 释放 GIL,从 pool 中拿的任务可能跑贼潆(py.auto释放)
    let df = py.allow_threads(|| {
        pyarrow_to_df(raw).unwrap()    // 🔥 raw 还在被另一个线程用!!
    });
    // ...
}
```

CPython 堆对象(bytes / list / dict)的内存释放可能发生在:
- GC 决定收回 `bars_bytes`
- Python 代码 `del bars_bytes` 
- 其他 Python 线程 离开 block 范围后 GC

**PyO3 的 lifetime 魔法**: `&Bound<'py, PyBytes>` 中的 `'py` 只有在 `Python<'py>` token 还活着的时候才 safe。 `allow_threads(|...| ...)` 的闭包是 `'py` 没了才跳出(`'py` 生命周期 ✓个函数的 bound),**实际上不安全**。

**正确做法**:在进 `allow_threads` 之前 promote 到 `Vec<u8>`:

```rust
#[pyfunction]
fn good_example<'py>(
    py: Python<'py>,
    bars_bytes: &Bound<'py, PyBytes>,
) -> PyResult<PyObject> {
    let raw: Vec<u8> = bars_bytes.as_bytes().to_vec();  // 1️⃣ 立刻拷一份
    let df = py.allow_threads(|| {
        pyarrow_to_df(&raw).unwrap()                    // ✅ raw 是 Rust 堆,可跨 GIL 释放
    });
    Ok(...)
}
```

费用:多一次 memcpy (100 万行 K 线 ~50 MB 上 IPC 编码后 ~5 MB), ~6 ms。**可接受换安全**。

如果不想提早拷,也可以快路径走 **PyBytes 接口里的 buffer 协议**:

```rust
use pyo3::buffer::PyBuffer;
let buf = PyBuffer::<u8>::get(bars_bytes)?;     // raw buffer view
let slice = buf.as_slice(py)?;                   // &[u8], lifetime bound to buf
drop(buf);                                       // 🔥 释放了 view, slice dangling!
```

明显比 `as_bytes()` 危险, 一般不上。

---

## 4. 坑 3:`&mut TaCache` 是独占引用——多 freq 不能共享

看 [crates/czsc-trader/src/czsc_signals.rs:309](../crates/czsc-trader/src/czsc_signals.rs#L309):

```rust
fn compute_kline_signals(&mut self, changed_freqs: ...) {
    for group in &self.compiled_kline_groups {           // ← 按 freq 分组顺序计算
        if let Some(czsc) = self.kas.get(group.freq.as_str()) {
            let cache = self.ta_cache.entry(group.freq.clone()).or_default();
            for op in &group.ops {
                let sigs_res = match op {
                    CompiledKlineSignalOp::Fast { exec, params }   => (exec)(czsc, params, cache),   // 需要 &mut
                    CompiledKlineSignalOp::Dynamic { func, params } => (func)(czsc, params, cache),  // 需要 &mut
                };
                ...
            }
        }
    }
}
```

**关键**: `cache` 在内部是 `&mut TaCache`,**同一 freq 不能两个 op 并发生成**。

但是 **不同 freq 可以并发**——因为 `TaCache` 是 `HashMap<freq, TaCache>`,每个 freq 有独立 Mutex。

👉 この是隐藏的курок: 如果以后想跨 freq 并行,要用 **`dashmap / sharded_mutex`** 包 `ta_cache`;但现在实现是**严格单线程顺序**,反而跨过了这个坎。

**坑实质**: 某个别人(或以后的你)读到代码可能会想到:**「这里厥 8 个 CPU,为什么不顺手加个 rayon 跨 freq 并行?」** 

**答: 不是不肯, 是要必须重构**:
1. `ta_cache` 改为 `RwLock<HashMap<...>>` 或者 `DashMap<freq, Mutex<...>>`(取消 `&mut self`)
2. `kas` 同样 要 sync
3. 任何「**可重入地修改 self 的字段**」(如 `self.s.insert` / `self.sigs.insert`)全部要 责资`borrow self as *mut Self`+ `unsafe`

这是个代价很大的重构,**项目选择不重构,只跨品种并行**——这是个「原则选择」。

---

## 5. 坑 4:`rayon::par_bridge` 跨 GIL 边界会「静默吞掉错误」

假设你冲动优化 `compute_kline_signals`,改成:

```rust
fn compute_kline_signals(&mut self, _: _) {
    // ❌ 错
    self.compiled_kline_groups.par_iter_mut().for_each(|group| {
        let mut local_sigs = Vec::new();
        // ... compute signals ...
        self.signal_map.extend(local_sigs);  // 🔥 多个线程同时 &mut self.signal_map
    });
}
```

报错会被 rayon **吞掉** — 因为 `.for_each(...)` 返回 `()`,只有 `.try_for_each` 会报第一个 panic。

加上 Python 端可能 `except Exception`,会静默吞 panic(不是好实践,但实际很常见)。结论: **明明运行结果错了,但 Python 端烫获 "成功"**。

**代码加 rx 作为防错锁**:

```rust
use crossbeam::channel::unbounded;
let (tx, rx) = unbounded::<Result<(), String>>();
self.compiled_kline_groups.par_iter().try_for_each(|group| {
    let result = (|| -> Result<(), String> { ... })();
    tx.send(result).unwrap();
    Ok(())
})?;
for r in rx.iter() { r?; }
```

或者**不跨 `&mut self` 边界**:

```rust
fn compute_kline_signals(&self, _: _) -> Vec<...> {  // 改为只读
    self.compiled_kline_groups.par_iter().map(|group| {
        // 只访 &self.kas, &self.ta_cache, 不动 self
        let cache = self.ta_cache.get(group.freq.as_str()).unwrap();
        group.ops.iter().map(|op| {
            let czsc = self.kas.get(group.freq.as_str()).unwrap();
            match op {
                CompiledKlineSignalOp::Dynamic { func, params } => (func)(czsc, params, cache),
                ...
            }
        }).collect::<Vec<_>>()
    }).collect()
}
```

**实质规则**: 在 Rust GIL 还在持锁的场景下,原生的 `std::sync::Mutex<T>` 也能用(Raskell RwLock 不能跨 .await)。选用规则:

| 需要 | 原生 Mutex | RwLock | DashMap | shard 锁 |
|------|------------|--------|---------|---------|
| 读多写少,粗粒度 | x | ✓ | x | x |
| 读多写少,freq 分组 | x | x | ✓ | x |
| 热点多,高并发 | x | x | x | ✓ |

CZSC 选 RwLock —— 以 unsichtigkeit “1 个 trader + 1 个 query 口”的设计换取 实现简单。

---

## 6. 坑 5:`ctrl-c` 在 `allow_threads` 不响应

PyO3 API docs 明文警告:

> Python signals will not be received while the GIL is released

其实含义比想的更严重:

- 如果你 `py.allow_threads(|| sleep_for_300_seconds())`,Python 端 `Ctrl+C` 会被 **挂起**,过了 300 秒后才能中断
- Python 中 `signal.signal(SIGINT, ...)` 也会」未响应直到 return 到 GIL

**项目里没大量使用 `allow_threads`**,所以这个坑不常见。但一旦以后在某处加背景,要避免这种「30s 锁死」详情:

```rust
// ❌ 错:crtl-c 装死300秒
py.allow_threads(|| std::thread::sleep(std::time::Duration::from_secs(300)));

// ✅ 对:留 GIL 响应 signal
std::thread::sleep(std::time::Duration::from_millis(50));   // 跑育可以 release
// or
py.check_signals()?;    // 手动检查 Ctrl-C
```

**安全模板**: 调用外留 GIL、设置 `is_thread_safe` sign、为期货读者设计可中断。详细可以写独立 topic。

---

## 7. 坑 6:Python 多线程(`ThreadPoolExecutor`)锁死 Rust

Python 端常见用法:

```python
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=8) as ex:
    futures = [ex.submit(czsc.compute_signals, ...) for _ in range(8)]
    for f in futures: f.result()
```

**期望**: 8 线程并行计算 signals.

**实际**: 依然串行❗为什么?

原因:

```
Thread-1                          Thread-2
  GIL locked                       (waiting for GIL)
  ↓                                ↑
  call czsc.compute_signals()      call czsc.compute_signals()
  进入 Rust → GIL 仍 持锁           (不能再抢到了)
  Rust 内部 100% CPU 计算            
  计算完 → 返回 GIL                
  ↓                                ↑ 拿到 GIL
  GIL released
  ...重复...
```

Python `threading` 的「并行」仅仅是「多线程技术中取下多个线程」,但 GIL 让他们只能串行执行 Python 字节码。而 Rust 代码是「native」—— 但 **调用者(持有 GIL 的 Python 线程)还没释放 GIL**,**另一个 Python 线程需要 GIL 才能「准备调用」** Rust 函数。

**什么场合能并行**:

- Python 纯 I/O 线程(发出 IO 后释放 GIL)
- Python `multiprocessing`(是独立进程,各自 GIL)
- Rust 代码本身主动 `py.allow_threads(...)`,后并在另一个 rayon 池中轮训 CPU 任务

**结论**: **`ThreadPoolExecutor` + CZSC = 业余串行**。

**正确変种 1: multiprocessing**

```python
import multiprocessing as mp
def worker(symbol):
    return czsc.run_research(bars[symbol], config)
with mp.Pool(8) as pool:
    outs = pool.map(worker, symbols)
```

不同进程 → 各自 GIL → 可以并行。

**正确変种 2: Rust 内部 rayon**

```python
# Python 端不画脚手架, 调用一个會计为内部并行的接口
czsc.run_research_batch(symbols, bars_map, config)  # 实现者在 Rust 内部 use rayon
```

这正是项目调优路径(`optimize.rs`)。

---

## 8. 坑 7:多进程 vs 多线程的选择

| 点 | multiprocessing | ThreadPoolExecutor | Rust rayon (GIL 内) |
|----|-----------------|--------------------|----------------------|
| 这能并行 CZSC 计算? | ✓ | ✗ | ✓ |
| 内存负担 | 8x (不共享) | 1x (共享) | 1x |
| Python code 能走进去? | only main | ✓ | only main after call |
| 启动价格 | 多进程 fork ~100ms | 几us | ~10us |
| 序列化需求 | pickle 几十MB | None | None |
| 调试难度 | 中 (NumPy / Ray 殷 人时还错位) | 低 | 低 |

**经验法则**:

- **纯 Python 业务**(I/O 系/编制文始件/调用云 API) → `ThreadPoolExecutor` 足够
- **包含 CZSC 计算** → multiprocessing,**或者**使用项目提供的 "run_research_batch" 类接口(内含 rayon)
- **跨 signal shared state 的多品种”被调用 CZSC”** → 不要走 multiprocessing,total 中输出应该在 Rust 里 and not spawn the GIL 出 Rust

---

## 9. 决策清单:什么时候动用 `allow_threads`/rayon/sub-interpreter

### 只 Rt  oidinterned

```
需要 「backgrounded cup download」 同时还能主线程响 ctrl-c ?
  → Rust side 加 check_signals(), 记住 base Python GIL 被释放期间 不会响应

需要: 多品种/多参数 批跑 ?
  → Rust 内部 rayon (项目已有 optimize.rs 模板)

需要: 多品种 + Python 中间逻辑 ?
  → multiprocessing.Pool + walk-in 看 multiprocessing.is_alive 状态

需要: Python 主线程响应 ctrl-c + Rust 计算 ★★★
  → Rust 段 unhappy threads + 主动设置 is_thread_safe
  → 完成一定轮 提前 check_signals()

需要: 跨进程 并发 + 共享 giant 缓存 (几十 GB Bar cache) ?
  → sub-interpreter (PEP 684) 或 process + mmap
```

### 项衉的 设计记账方法

设计上设计时看下三个问题:

1. 这个 `#[pyfunction]` 能被 Python `ThreadPoolExecutor` 并发调? 能,就会  跳 GIL — 需要 [pyfunction.lock]
2. Rust 函数 里需不需要保持 GIL? 如果不需 调用 不要 from `py.anyhow()` 获取的东西 — 需要 `allow_threads`
3. 后面才需要的 heavy compute 要 invoke rayon 不能? 如果能 — 提前 collect 成 `Vec<独立任务>`,放到 `.par_iter().map(...)` 中

### 期末 贠 适中蜛

2025+ 的 **PEP 703 "free-threaded Python"** (3.13+ no-GIL) 使这个坑调和些 — 但 CZSC 项目需要兼容 Python 3.10 项目(见 Cargo.toml 锁定),这个升级一定不是囏期。

看到了 cpython 3.13t 的 **sub-interpreter + per-interpreter GIL** (PEP 684),是另一个 project 路线 —— 项目现阶段 vs。

---

## 10. 调试工具箱

### sys.getrefcount 查 GIL 是否被持锁

```python
import sys
import czsc
c = czsc.CZSC(...)
print("refs:", sys.getrefcount(c))
# 任何一个 lusution 大幅跳度证明有人忘记减 ,或者另一个 Python 线程持锁了 c
```

### py-spy 看 Rust rayon 是否贴在 futex

```bash
uv run --no-sync py-spy dump --pid $(pgrep -f czsc.py)
```

输出中能但去找:

- `GIL` → 另一个 Python 线程抢锁了,你最后是「拺了哪个 thread」
- `futex_wait_queue` (Linux) 或 `psynch_mutexwait` (macOS) → rayon pool 里热锁了
- `parking_lot::Mutex` 或 `std::sync::Mutex` → Rust 内部 Mutex 起热(检查跨线程同 phase 加载同一 TaCache 等)

### strace / dtrace

```bash
strace -p <PID> 2>&1 | grep -c futex   # 数字一直在跳 → 锁热
dtrace -n 'syscall:::entry { trace(copyin); }' -p <PID>   # macOS 馠装 IO
```

### futures crate → 减少 rayon 父锁块

```toml
# Cargo.toml + try fn check_signals() 机制可以加 futures::select!
futures = "0.3"
```

---

## 附录:文件位置速查

| 路径 | 文件 | 备注 |
|------|------|------|
| [crates/czsc-trader/src/optimize.rs:300](../crates/czsc-trader/src/optimize.rs#L300) | rayon 调整 | `单线程下避免 rayon 嵌套并行` |
| [crates/czsc-trader/src/engine_v2/scheduler.rs:21](../crates/czsc-trader/src/engine_v2/scheduler.rs#L21) | `into_par_iter()` 多品种调度 | 一层并行,不嵌套 |
| [crates/czsc-trader/src/czsc_signals.rs:309](../crates/czsc-trader/src/czsc_signals.rs#L309) | 默认串行计算 signals | 底线是跨 GIL 七 代 中 营业起 串行避免 Mutex |
| [crates/czsc-trader/src/czsc_signals.rs:71](../crates/czsc-trader/src/czsc_signals.rs#L71) | ta_cache: HashMap<freq, TaCache> | freq 是隐式锁粒度 |
| [crates/czsc-python/Cargo.toml:34](../crates/czsc-python/Cargo.toml) | abi3 / PyGILState 背景 | 限制 架构 创建 |

---

## 小结

CZSC 项目的并发原则:

| 原则 | 实现 |
|------|------|
| GIL 预期被搾 | Rust 不依靠 与 GIL 共贵 的方向 |
| 多品种 / 多参数 用 rayon | optimize.rs、scheduler.rs |
| 多 freq 不能同时修改 | 默认顺序, 改为跨品种 |
| 安全 UAF 优先级 高 | `&[u8]`  走 py.allow_threads 之前 promote to Vec<u8> |
| multiprocessing 是 Python 唯一跨 GIL 并行 | 项目只提供 Rust 内部批跑的接口 |

异常检测越早越好:

1. 看到 rayon 嵌套 → 改为同步 collect
2. 看到 `&[u8]` 跨 GIL → promote to `Vec<u8>`
3. 看到 `Signal not get fired` + 没有 报错 → rayon `.for_each` 吞 error, 改 `.try_for_each` + channel
4. 外面 ThreadPoolExecutor 调 CZSC → 实质 上无效率, 请改 multiprocessing

