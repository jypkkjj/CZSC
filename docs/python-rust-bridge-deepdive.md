# proc-macro 注册机制(SIGNAL_REGISTRY)深入讲解

> 本篇是 [Python ↔ Rust 数据流转机制详解](./python-rust-bridge.md)的姊妹篇,聚焦于一个具体问题:**220+ 信号函数是如何被自动注册到一个全局 `HashMap` 里,运行时又如何按名字查表调用的?**
>
> 📘 姊妹篇:
> - 🚪 [总览:Python ↔ Rust 数据流转机制详解](./python-rust-bridge.md)
> - 🚄 [零拷贝优化(Arrow IPC → Polars)](./python-rust-bridge-zerocopy.md)
> - 🧵 [GIL 与多线程踩坑实录](./python-rust-bridge-gil.md)
> - 🛠️ [手把手加一个新信号函数](./add-a-new-signal.md) ← 本篇知识的最直接实战落地
>
> 📜 索引速查表: [姊妹篇总索引](./python-rust-bridge.md#姊妹篇索引)

---

## 目录

- [问题背景:为什么需要 proc-macro](#问题背景为什么需要-proc-macro)
- [三大支撑技术](#三大支撑技术)
- [端到端的实现路径](#端到端的实现路径)
  - [第 1 步:`#[signal]` 宏替换](#第-1-步signal-宏替换)
  - [第 2 步:`#[signal_module]` 宏校验模块](#第-2-步signal_module-宏校验模块)
  - [第 3 步:`inventory::submit!` + `inventory::collect!`](#第-3-步inventorysubmit--inventorycollect)
  - [第 4 步:`SIGNAL_REGISTRY` 打包](#第-4-步signal_registry-打包)
- [运行时查找与调用](#运行时查找与调用)
- [完整堆栈图:从 `generate_czsc_signals` 到具体函数](#完整堆栈图从-generate_czsc_signals-到具体函数)
- [三层校验 / 三重安全网](#三层校验--三重安全网)
- [Fast-Path 优化与 `param_kind` 注入](#fast-path-优化与-param_kind-注入)
- [阅读源码时的快捷跳转](#阅读源码时的快捷跳转)
- [常见误区与陷阱](#常见误区与陷阱)

---

## 问题背景:为什么需要 proc-macro

**项目原貌**: `crates/czsc-signals/src/` 里有 22 个 `.rs` 文件,FN 数 > 220。每个文件充斥着:

```rust
pub fn tas_ma_base_v221101(czsc: &CZSC, params: &ParamView, cache: &mut TaCache) -> Vec<Signal> { ... }
pub fn tas_ma_round_v221206(czsc: &CZSC, params: &ParamView, cache: &mut TaCache) -> Vec<Signal> { ... }
pub fn tas_macd_base_v221028(...) -> Vec<Signal> { ... }
```

**需求**: 让 Python 调用 `generate_czsc_signals(c, ["tas_ma_base_V221101@$param_template$", ...])` 时能按字符串名字反查到具体 `fn` 指针并执行。

**朴素方案的问题** —— 手工维护一个注册表:

```rust
// ❌ 220 行 add_to_registry!(tas_ma_base_v221101); // 改名字忘了改这里?
lazy_static! {
    static ref REGISTRY: HashMap<&'static str, SignalFn> = {
        let mut m = HashMap::new();
        m.insert("tas_ma_base_V221101", tas_ma_base_v221101 as SignalFn);
        m.insert("tas_ma_round_V221206", tas_ma_round_v221206 as SignalFn);
        // ...220 行 add
        m
    };
}
```

这有 3 个痛点:

| 痛点 | 后果 |
|------|------|
| 函数签名只能手抄 | 漏了/拼错字符串编译能过,运行 `name` miss 时才发现 |
| 重命名时容易忘改注册表 | 编译/运行都不报错,只是 hydration 永远 0 |
| 无法做编译期校验(name 与函数名版本号必须一致) | 历史包袱 `V<yyMMdd>` 拼写漂移 |

**proc-macro 方案的答案**: 让 **注解紧贴定义**, 编译期生成注册代码 + 校验, 运行时靠 `inventory` 自动收集, 零手工维护。

---

## 三大支撑技术

这套机制建立在 3 个 Rust 生态技术之上,先把它们列清楚:

| 技术 | 角色 | 在本项目中的位置 |
|------|------|-----------------|
| [`proc-macro2` + `syn` + `quote`](https://crates.io/crates/syn) | proc-macro 的基础工具链:解析/生成 TokenStream | `crates/czsc-signal-macros/Cargo.toml`([crates.io](https://crates.io)) |
| [`inventory`](https://crates.io/crates/inventory) | **链接器期全局收集点**(类似 C++ 的 init-array / `__attribute__((constructor))`) | 出现在 6 个文件,搜索 `inventory::iter\|inventory::submit\|inventory::collect` |
| `LazyLock<HashMap<...>>` (Rust 1.80+ stable)| 把 inventory 摊平为 O(1) 查询的运行时表 | `crates/czsc-signals/src/registry.rs` 的 `SIGNAL_REGISTRY` |

🔑 **关键洞察**: Rust 没有「反射」,函数地址无法在运行时枚举。`inventory` 通过链接器技巧(`--no-keep-memory`) 让每个 crate 把静态变量挂到全局链表,然后任意下游 crate 可以 `iter()` 出所有点。这是 220+ 函数不需要手工注册的核心。

---

## 端到端的实现路径

### 第 1 步:`#[signal]` 宏替换

开发者写的源头:

```rust
// crates/czsc-signals/src/tas.rs:99
use czsc_signal_macros::signal;

#[signal(
    category   = "kline",
    name       = "tas_ma_base_V221101",
    template   = "{freq}_D{di}{ma_type}#{timeperiod}_分类V221101",
    opcode     = "TasMaBaseV221101",
    param_kind = "TasMaBase",
)]
pub fn tas_ma_base_v221101(
    czsc: &CZSC,
    params: &ParamView,
    cache: &mut TaCache,
) -> Vec<Signal> { ... }
```

宏处理后(`crates/czsc-signal-macros/src/lib.rs:335` 输出的 `quote! {...}`),**编译期实际生成**了 3 件事:

1. **原函数原样保留**(`#vis #sig #block`)—— 你的算法一字不漏
2. **生成 dyn-wrap 函数** (针对 `&ParamView` 类型) , 把字符串化的 `HashMap<String, Value>` 装换成 `ParamView`,然后调用真函数:
   ```rust
   #[doc(hidden)]
   fn __rs_dyn_wrap_tas_ma_base_v221101(
       czsc: &CZSC,
       params: &HashMap<String, Value>,
       cache: &mut TaCache,
   ) -> Vec<Signal> {
       let p = ParamView::new(params);
       tas_ma_base_v221101(czsc, &p, cache)
   }
   ```
   ⚠️ **关键设计**: 注册到 inventory 的永远是 `__rs_dyn_wrap_*` 这个 **胖指针**, 而不是 `tas_ma_base_v221101`。这样统一了所有 signal 的签名 (都接收原始 `HashMap`), 而内部可以是任何具体参数类型 (`ParamView` / `MyParams` / 裸 `HashMap`)。
3. **生成静态信号描述符 + `inventory::submit!`**(注入到 inventory 链表):
   ```rust
   #[doc(hidden)]
   pub const __RS_CZSC_SIGNAL_META_TAS_MA_BASE_V221101: SignalDescriptor = SignalDescriptor {
       category:   "kline",
       name:       "tas_ma_base_V221101",
       template:   "{freq}_D{di}{ma_type}#{timeperiod}_分类V221101",
       opcode:     "TasMaBaseV221101",
       param_kind: "TasMaBase",
       func_ref:   SignalFnRef::Kline(__rs_dyn_wrap_tas_ma_base_v221101 as SignalFn),
       fast_kline: None,
   };

   inventory::submit! {
       __RS_CZSC_SIGNAL_META_TAS_MA_BASE_V221101
   }
   ```

`fast_kline: None` 暂不展开 —— 见下文 [Fast-Path 优化](#fast-path-优化与-param_kind-注入)。

### 第 2 步:`#[signal_module]` 宏校验模块

**模块级**宏,作用是 **守住模块边界**(`crates/czsc-signal-macros/src/lib.rs:360`)。

`crates/czsc-signals/src/lib.rs` 这么用:

```rust
extern crate self as czsc_signals;
use czsc_signal_macros::signal_module;

#[signal_module(category = "kline")]
pub mod tas {
    include!("tas.rs");
}
```

宏做了什么(读取 `lib.rs:388-466`):

- ✅ 检查模块是 **内联模块**(`m.content.is_none()` 时返回 `compile_error!`,防止 `mod foo;` 这种外部模块)
- ✅ 遍历模块的每个 `Item::Fn`,对所有 `pub fn *_v*` 强制要求加上 `#[signal(...)]` 标注
- ✅ **校验签名**:
  - `kline` 类: 必须 3 参数, 类型 `(&CZSC, &Params, &mut TaCache)`
  - `trader` 类: 必须 2 参数, 类型 `(&dyn TraderState, &Params)`
- ✅ **检查模块内重名**: 用 `HashSet` 验证 `attr_name` 与 `attr_opcode` 都不重复
- ⚠️ **不会** 自动给函数加 `#[signal]`,必须手写 —— 这是 spec 强调的「显式优于隐式」

如果违反任意一条,**`cargo build` 直接失败**, 这就是「编译期 ratchet」的来源。

### 第 3 步:`inventory::submit!` + `inventory::collect!`

每个放进 `inventory::submit!` 的静态项, 链接器会把它挂到全局链表里。但要 `iter::<T>()` 能正常工作, **必须在某个 crate 里显式 `inventory::collect!(T)`**(声明对类型 `T` 进行收集)。

本项目把它放在 `crates/czsc-signals/src/lib.rs:116`:

```rust
pub mod types;
inventory::collect!(crate::types::SignalDescriptor);
```

🔑 **关键点**: `inventory::collect!` 必须在某个 crate 的 crate root 出现, 这样收集入口就能感知每个 `submit!` 点(它们可能在 `tas.rs` 里,可能在 `tas::sub_module` 里,等等)。如果没有这一行,**`inventory::iter::<SignalDescriptor>()` 会返回空** —— 而这种 bug 是不会编译报错的,运行起来才会发现「信号突然全不见了」。

> **运行验证**: `tests/parity/` 删之前,会有专门的 ratchet 测试断言 `SIGNAL_REGISTRY.len() >= 220`。现在被分散到 `test_macro_injected_kline_descriptors_registered` 等白名单测试里。

### 第 4 步:`SIGNAL_REGISTRY` 打包

`crates/czsc-signals/src/registry.rs:44-60` 把 inventory 平摊为可查询的 `HashMap`:

```rust
pub static SIGNAL_REGISTRY: LazyLock<HashMap<&'static str, SignalMeta>> = LazyLock::new(|| {
    let mut m: HashMap<&'static str, SignalMeta> = HashMap::new();
    for d in list_generated_signal_descriptors() {
        insert_generated_kline(&mut m, d);
    }
    m
});
```

`LazyLock`(Rust 1.80+ stable)保证 **首次访问时初始化, 线程安全**。这是在 `SIGNAL_REGISTRY.get(name)` 第一次被调用时执行的。

`list_generated_signal_descriptors()` 内部先调用 `inventory::iter::<SignalDescriptor>()` 拿到所有点, 然后:

- **去重 + 排序 + 强校验 name/opcode 全局不重复**(`normalize_generated_signal_descriptors`, `registry.rs:111-132`)
  - 重名 → `panic!("invalid generated signal descriptors: duplicate signal name: ...")`
  - 重 opcode → `panic!("invalid generated signal descriptors: duplicate signal opcode: ...")`
- 按 name 字典序排序

⚠️ 在生产路径上,这个 panic 不会触发(因为 `#[signal_module]` 已挡住模块内的重复)。但全局 opcode 重复(来自不同模块)只能在这层捕获,**所以 panic 范围被刻意控制在这个函数内**, 不让它污染调用栈上层。

---

## 运行时查找与调用

主调用方在 `crates/czsc-trader/src/czsc_signals.rs:144` 等:

```rust
use czsc_signals::registry::SIGNAL_REGISTRY;

let meta = SIGNAL_REGISTRY.get(config.name.as_str())?;
(meta.func)(czsc, params, &mut cache)  // ← 直接调用 fn 指针
```

**调用伪代码**:

```
config.name: "tas_ma_base_V221101"        ← 来自 Python 的策略配置
        │
        │  O(1) HashMap 查找
        ▼
SignalMeta { func: SignalFn, param_template: "...", fast_kline: Option<...> }
        │
        │  meta.func 不需要再 pack —— 它本来就是
        │  fn(&CZSC, &HashMap<String, Value>, &mut TaCache) -> Vec<Signal>
        ▼
(meta.func)(czsc, params, &mut cache)
        │
        ▼
__rs_dyn_wrap_tas_ma_base_v221101(czsc, params, &mut cache)
        │
        │  HashMap → ParamView 转换
        ▼
tas_ma_base_v221101(czsc, &p, cache)     ← 真正的算法
```

**这段流程的性能特征**:

| 阶段 | 成本 |
|------|------|
| `SIGNAL_REGISTRY.get(name)` | 1 次 hash + 等值比较 |
| 函数指针间接调用 | 一次 `mov + jmp`, 现代 CPU 上几乎 0 |
| `HashMap → ParamView` 转换 | 每次都做(用 fast-path 可绕过,见下文) |
| 真函数体 | 取决于信号计算 |

---

## 完整堆栈图:从 `generate_czsc_signals` 到具体函数

```
Python: czsc.traders.generate_czsc_signals(c, [[
    "日线_D1SMA#5_分类V221101_多头_向上_任意_0",
    "60分钟_D1EMA#12_分类V221101_空头_向下_任意_0",
]])
│
▼
crates/czsc-trader/src/czsc_signals.rs:144                 ← PyO3 #[pyfunction]
│   for sig_config in signals_seq:
│       // 用 SIGNAL_REGISTRY.parse 名字 → 函数指针
│       meta = SIGNAL_REGISTRY.get("tas_ma_base_V221101")─────► HashMap 查找
│       (meta.func)(czsc, params, &mut cache)            ─────► fn 指针调用
│
▼
__rs_dyn_wrap_tas_ma_base_v221101(...)         ← #[signal] 宏注入的 wrapper
│   let p = ParamView::new(params);
│   tas_ma_base_v221101(czsc, &p, cache)
│
▼
tas_ma_base_v221101(...)                          ← 你写的算法本体
│   let ma = sma(...);
│   let signal = Signal { ... };
│   vec![signal]
│
▼
Vec<Signal> → PyO3 → Python list
```

---

## 三层校验 / 三重安全网

| 层级 | 触发时机 | 检查内容 | 补丁触发 |
|------|---------|----------|---------|
| **L1: 源 AST** | `cargo build` | 签名、参数、`#[signal]` 是否标注 | `compile_error!` 中断编译 |
| **L2: 模块内去重** | `cargo build` | 同模块内 name/opcode 唯一 | `compile_error!` 中断编译 |
| **L3: 全局 inventory 归并** | 首次访问 `SIGNAL_REGISTRY` | 跨模块 name/opcode 唯一,函数指针就绪 | `panic!` 中断该次调用 |
| **L4: 运行时 ratchet** | `cargo test` | 已知信号名白名单存在(`registry.rs` 的 6 个 `_registry_contains_*` test) | 测试失败 |

设计哲学:**能早报错的尽可能 L1/L2 报,不要把 bug 推到运行期**。

---

## Fast-Path 优化与 `param_kind` 注入

每个 signal 的 `HashMap<String, Value>` 在每次调用都被 `ParamView::new()` 重新解释 —— 对于一个跑批热的 backtest,这不可忽略。

宏默认会为非 `&ParamView` / `&HashMap` 的强类型参数生成 fast-path 包装(见 `crates/czsc-signal-macros/src/lib.rs:198-251`):

```rust
// 由宏自动生成的 fast-path(3 个 fn):
fn __rs_fast_decode_<name>(params: &HashMap<String, Value>) -> Option<Value> {
    // 第一次解释: HashMap → serde_json::Value → 强类型 (#pty,例如 MyParams)
    // 然后再 serde_json::to_value,得到「已经准备好」的可序列化值
}

fn __rs_fast_exec_<name>(czsc: &CZSC, p: &Value, &mut TaCache) -> Vec<Signal> {
    // 之后每根K线都走这里:Value → 强类型 (#pty) → 调用的算法
    // 省掉 ParamView 的运行时查找开销
}
```

不显式指定 `fast_exec` / `fast_decode` 时,宏自动注入 `auto_fast_expr`(强类型参数时才生成)。当指定时(`fast_exec = "tas_my_exec"`, `fast_decode = "tas_my_decode"`),相当于要求宏跳过自动生成,直接用你写的特化版本。

`FastKlineMeta` 挂在 `SignalDescriptor::fast_kline`, 与 `func_ref` 并列。运行期 `czsc_trader::CzscTrader` 优先用 fast-path,有性能差异时 `meta.fast_kline.is_some()` 分支决定走哪条。

🔍 **代码定位**:
- 宏生成 fast-path 的代码块:`crates/czsc-signal-macros/src/lib.rs:198-251`
- 运行时挑选 fast-path 的代码:`SIGNAL_META.func` vs `SIGNAL_META.fast_kline.exec` 的选择(在 `czsc_signals.rs:154` 附近)

---

## 阅读源码时的快捷跳转

```bash
# 1. 宏入口
code crates/czsc-signal-macros/src/lib.rs        # 19-357 行 = #[signal]; 359-470 = #[signal_module]

# 2. SignalDescriptor 类型与 SignalFn 别名
code crates/czsc-signals/src/types.rs            # 106-122 行

# 3. 全局注册表
code crates/czsc-signals/src/registry.rs         # 44-60 行 = SIGNAL_REGISTRY / TRADER_SIGNAL_REGISTRY

# 4. inventory 收集点
crates/czsc-signals/src/lib.rs:116                # inventory::collect!(SignalDescriptor)

# 5. 模块括号包装
crates/czsc-signals/src/lib.rs:12-112             # 22 个 #[signal_module]

# 6. 运行时反查
crates/czsc-trader/src/czsc_signals.rs:144        # SIGNAL_REGISTRY.get(name)
crates/czsc-trader/src/sig_parse.rs:86            # 从信号字符串 ↔ name 的逆向解析
crates/czsc-trader/src/trader.rs:72               # TRADER_SIGNAL_REGISTRY.get(name)

# 7. 验证测试(是真正的 ratchet)
crates/czsc-signals/src/registry.rs:198-457       # 6 个白名单测试
```

---

## 常见误区与陷阱

### 1. 「我加了一个新 fn,但 `SIGNAL_REGISTRY` 找不到」

`#[signal]` 没加?  
或者模块没有 `#[signal_module]` 括号?  
或者参数类型棘手导致宏拒绝展开(看 `cargo build` 输出,有没有 `compile_error!`)?

排查清单:
- ✅ 函数名 `pub fn foo_v123456(...)` 包含 `_v<数字>`
- ✅ 函数名 `name = "foo_V123456"` 带大写 `V`(宏会做 `_v` → `_V` 校验,见 `lib.rs:80-90`,不一致就报错)
- ✅ 模板 `template = "..._V123456"` 包含版本号
- ✅ 模块用了 `#[signal_module(category = "...")]`
- ✅ 参数签名匹配 `category`:`kline` 是 3 参,`trader` 是 2 参

### 2. 「重命名函数后,Python 端 Signal name 突然 miss」

**这是设计的「显式优于隐式」** —— `name = "foo_V123456"` 是注册到 inventory 的字符串,**不会** 与 fn ident 自动同步。改了 fn 名后必须同步改 `name`,否则同名 entry 残留 → 运行 `cargo build` 会在 `#[signal]` 宏校验时拒绝。

### 3. 「为什么 `TRADER_SIGNAL_REGISTRY` 不能复用同一个 `SIGNAL_REGISTRY`?」

因为两者函数指针签名不同:
- `SignalFn = fn(&CZSC, &HashMap, &mut TaCache) -> Vec<Signal>`
- `TraderSignalFn = fn(&dyn TraderState, &HashMap) -> Vec<Signal>`

`SignalDescriptor::func_ref: SignalFnRef` 用枚举区分(`types.rs:95-100`)。两个 `HashMap` 的 key 都是 `&'static str`(名字),所以 `category` 决定它走哪个表。

### 4. 「inventory 没有 `submit!` 项,但 `collect!` 了也能跑?」

能,`iter()` 只会返回空。但意味着你 **注册表永远为空**,调用方全部 `None` —— 这种 bug 是 silent 的。ratchet 测试(白名单)是抵御这种回归的最后兜底。

### 5. 「为什么 `#[signal_module]` 不能写到 `mod foo;` 上?」

外部模块(`mod foo;`)的体内在另一个文件里, `syn::ItemMod.content` 是 `None` —— 宏读不到内部函数,没法做去重与签名校验(`lib.rs:388-394`)。**强制要求内联模块 `(mod foo { ... })`**, 配合 `include!("foo.rs")` 是惯用模式(见 `crates/czsc-signals/src/lib.rs:13-15`)。

### 6. `comment="fast_exec"` / `"fast_decode"` 路径解析失败

`fast_exec = "tas_my_exec"` 中的字符串是 `syn::Path` 解析对象,要求是合法的 Rust 路径。如报错一般是因为没加 `crate::` 前缀,或拼错 fn 名。

---

## 加新信号函数的 cheat-sheet

1. 在 `crates/czsc-signals/src/<category>.rs` 末尾加 `{signal`
2. 函数名遵循 `<namespace>_<logic>_v<yyMMdd>`,`pub`
3. `name`、`template` 与 `version` 严格对应,`template` 包含版本号
4. `cache` 一定要 `&mut TaCache`(就算算法里不用 —— 保持统一的内存接口)
5. `cargo build` 验证编译期校验通过
6. `tests/` 验证策略 / 加一个白名单 `assert!` 到 `registry.rs` 末尾的 test 块中
7. 文档: 在 `docs/public_api.md` 或对应 namespace 文档里补一行举例

---

## 参考资料

- 📦 [`inventory` crate docs.rs](https://docs.rs/inventory)
- 📦 [`syn` crate docs.rs](https://docs.rs/syn)
- 📘 [ Rust Reference: Procedural Macros](https://doc.rust-lang.org/reference/procedural-macros.html)