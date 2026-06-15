# 从源码构建 czsc —— 完整教程

> 适用读者：Python 开发者，希望从源码编译 Rust 扩展、在本地跑通整个 czsc 项目。
>
> 最后更新：2026/06/14

---

## 0. 前置准备（一次性）

### 0.1 工具链清单

| 工具       | 最低版本 | 用途                            | 本项目硬要求                                   |
| ---------- | -------- | ------------------------------- | ---------------------------------------------- |
| `python`   | ≥ 3.10   | 运行 Python 代码                | `requires-python = ">=3.10"`                   |
| `uv`       | ≥ 0.4    | 管理 Python 依赖与虚拟环境      | 推荐用 `uv sync`/`uv run`                      |
| `rustc/cargo` | ≥ 1.74 | 编译 Rust 扩展              | 当前实测：1.91.1                               |
| `maturin`  | ≥ 1.0    | 把 Rust crate 编译成 Python wheel | 本项目**不发布预编译 wheel，必须从源码构建** |
| `xcode-select` (仅 macOS) | 任意 | 提供 CommandLineTools 工具链 | `xcode-select --install`                       |

> PyO3 0.28 + abi3-py310，要求编译期 PYO3_PYTHON 解析到 Python ≥ 3.10。

### 0.2 安装 maturin

任选其一：

```bash
# 方式 A：cargo 装（推荐，编译快，路径：~/.cargo/bin/maturin）
cargo install maturin --locked

# 方式 B：通过 uv 装（路径：~/.local/bin/maturin）
uv tool install maturin
```

验证：

```bash
which maturin        # 应有输出
maturin --version    # 期望 maturin 1.x.x
```

> 第一次 `cargo install` 需要十几分钟；之后增量秒级。

### 0.3 macOS 额外检查

```bash
xcode-select -p
# 期望：/Library/Developer/CommandLineTools
# 或：/Applications/Xcode.app/Contents/Developer
```

未装则：

```bash
xcode-select --install
```

---

## 1. 建虚拟环境 + 装 Python 依赖

### 1.1 进入项目根目录

```bash
cd /Users/nuc8/my/project/github/rust/czsc
```

### 1.2 同步依赖

```bash
# 装全部依赖组合（dev + test + docs）
uv sync --extra all --python 3.13
```

`uv sync` 会做：

1. 解析 `pyproject.toml` + `uv.lock` → 确定精确版本。
2. 在 `.venv/` 创建隔离虚拟环境（默认 `.venv`）。
3. `pip install` 里面所有**纯 Python**依赖。Rust 扩展部分在下一步 `maturin develop` 时处理。

> **CLAUDE.md " `--no-sync` 约定"**：日常跑测试用 `uv run --no-sync …` 跳过 lockfile 解析（省 4-5 秒）。但改了 `pyproject.toml` / `uv.lock` 后必须显式跑一次 `uv sync`。

### 1.3 验证 venv

```bash
ls .venv/bin/python && .venv/bin/python --version
# 期望：Python 3.10+（与 uv 默认选择一致）

uv run python -c "import sys; print(sys.executable)"
# 期望：…/项目根/.venv/bin/python
```

---

## 2. 从源码编译 Rust 扩展

这一步用 maturin 把 `crates/czsc-python` 编译成 `.so / .dylib / .pyd`，注入当前 venv 的 `site-packages/czsc/` 目录，**产物文件名是 `czsc._native.abi3.so`**（不是 `_native/` 子目录里的 `czsc_native.so`）—— 这是 maturin + abi3-py310 的标准命名。

### 2.1 开发模式（日常用）

```bash
uv run --no-sync maturin develop --uv
```

`--uv` 让 maturin 把产物装到当前 `uv` 激活的 `.venv`，而不是再造一份。

输出形如：

```
🔗 Found pyo3 binding in `crates/czsc-python`
📡 Using build profile `dev`
…
```

第一次编译 4-10 分钟（macOS Intel/Apple Silicon 首次冷启动，含 polars 等大依赖的 Rust 部分）。之后改 Rust 源码增量编译秒级。

### 2.2 跳过 stub 生成（可选）

```bash
uv run --no-sync maturin develop --uv --skip-stub
```

`--skip-stub` 跳过 `czsc-python` 里 `stub_gen` 二进制生成 `pyi` 类型 stub 的步骤。日常 Python 调用不关心类型提示时可用。

### 2.3 验证编译产物

```bash
ls .venv/lib/python3.13/site-packages/czsc/_native.abi3.so
# 期望看到：_native.abi3.so 这一个文件（不是 _native/ 目录）

# 替代：用 Python 直接验证
.venv/bin/python -c "import czsc._native; print(czsc._native.__file__)"
# 期望：…/site-packages/czsc/_native.abi3.so
```

### 2.4 Python 端冒烟测试

```bash
uv run --no-sync python -c "
from czsc import CZSC, Freq
from czsc.mock import generate_symbol_kines
from czsc import format_standard_kline

df = generate_symbol_kines('000001', '30分钟', '20240101', '20240105')
print('rows:', len(df), 'cols:', list(df.columns))

bars = format_standard_kline(df, freq=Freq.F30)
print('bars:', len(bars))

c = CZSC(bars)
print('czsc:', c)
print('Symbol:', c.symbol)
print('Freq:', c.freq)
"
```

期望看到 `rows: ~xx`、`bars: xx`、`czsc: CZSC(...)`，且**不报错**。

常见红线：

| 现象                                             | 原因                                       | 解决                                                                           |
| ------------------------------------------------ | ------------------------------------------ | ------------------------------------------------------------------------------ |
| `ImportError: … incompatible architecture`       | wheel 与当前 Python ABI 不匹配             | 重新 `maturin develop --uv`                                                    |
| `ModuleNotFoundError: No module named 'czsc._native'` | maturin 没有把 dylib 拷到 `.venv` | 检查 `ls .venv/lib/python*/site-packages/czsc/_native.abi3.so` 是否存在  |
| `undefined symbol: PyGILState_Release`           | 直接跑 `cargo build --workspace`         | 改走 `maturin develop`，见阶段 7.1                                         |
| cmake 找不到 `ArrowConfig.cmake`、error exit 1   | pyarrow 21.0.0 没有 cp314 wheel，走了 sdist 源码编，要 Arrow C++ | 切到 Python 3.13，见阶段 7.7                                     |

---

## 3. 跑测试

### 3.1 默认测试集（推荐日常）

```bash
uv run --no-sync pytest
```

期望：

```
========= xxx passed, x skipped in y.yy s ==========
```

跳过的是 `@pytest.mark.slow` 标记的耗时测试。

### 3.2 单文件 / 单测试

```bash
# 单文件
uv run --no-sync pytest tests/test_analyze.py -v

# 单测试函数
uv run --no-sync pytest tests/test_analyze.py::test_czsc_basic -v

# 按关键字过滤
uv run --no-sync pytest -k "test_czsc_basic"
```

### 3.3 跑全套（含 `@pytest.mark.slow`）

```bash
uv run --no-sync pytest --run-slow
```

发布前 / CI 时用。

---

## 4. 日常开发循环

### 4.1 改了 Rust 代码

```bash
uv run --no-sync maturin develop --uv     # 增量编译 + 注入 venv
uv run --no-sync pytest                   # 跑测试
```

### 4.2 只改了 Python 代码

```bash
uv run --no-sync pytest   # 不需要重新编译
```

### 4.3 一行串联（最常用）

```bash
uv run --no-sync maturin develop --uv && uv run --no-sync pytest -x
```

> 黄金法则：**别直接跑 `cargo build`**——那条路径会绕开 maturin 的 feature 协商，触发 PyO3 `extension-module` 与具体 libpython 的链接冲突。

---

## 5. 构建可分发的 wheel（发布用）

### 5.1 本机平台 wheel

```bash
uv run --no-sync maturin build --release
```

产物文件名按 PEP 491 拆分格式：

```
czsc-{version}-{python_tag}-{abi_tag}-{platform_tag}.whl
```

实际两条参考：

| 平台          | 文件名                                                                       |
| ------------- | ---------------------------------------------------------------------------- |
| 你的本机（实测，Intel macOS） | `czsc-1.0.0rc8-cp310-abi3-macosx_10_12_x86_64.whl`              |
| Apple Silicon macOS         | `czsc-1.0.0-rc.8-cp310-abi3-macosx_11_0_arm64.whl`               |

字段含义：

| 字段         | 取值示例                          | 含义                                                       |
| ------------ | --------------------------------- | ---------------------------------------------------------- |
| `czsc`       | distribution name                 | `pyproject.toml [project].name`                            |
| `1.0.0-rc.8` | version                           | 从 Cargo workspace `[workspace.package].version` 注入      |
| `cp310`      | python tag                        | 兼容的最低小版本，由 `pyo3/abi3-py310` 决定                |
| `abi3`       | abi tag                           | 走 Python 稳定 ABI，一个 wheel 兼容 Python 3.10 ~ 3.13     |
| `macosx_…_x86_64` / `macosx_11_0_arm64` | platform tag | 当前构建平台；多平台需 cibuildwheel            |

### 5.2 把 `.whl` 装到别处

```bash
uv pip install target/wheels/czsc-1.0.0-rc.8-cp310-abi3-*.whl
```

### 5.3 多平台 wheel（高级）

办不到。需要 CI / Docker 多镜像，用 [cibuildwheel](https://cibuildwheel.readthedocs.io/) 串：

```yaml
# .github/workflows/wheels.yml 节选
- run: python -m pip install cibuildwheel
- run: python -m cibuildwheel --output-dir wheelhouse
```

本机只能产**一个**平台 tag；不实用，仅为发布流水线服务。

---

## 6. 一次到位的 checklist

跑通以下 8 步即可宣布本地环境就绪：

- [ ] `maturin --version` 有输出
- [ ] `uv sync --extra all --python 3.13` 成功（`.venv` 已创建；详见阶段 7.7 关于 Python 3.14 的坑）
- [ ] `ls .venv/lib/python3.13/site-packages/czsc/_native.abi3.so` 看到 maturin 产物（实测文件名是 `_native.abi3.so`，而不是 _native/ 目录里的 `czsc_native.so`）
- [ ] `uv run python -c "from czsc import CZSC"` 不报错
- [ ] 阶段 2.4 的 `format_standard_kline + CZSC(bars)` 冒烟脚本打印正常
- [ ] `uv run --no-sync pytest` 全绿
- [ ] 改了 `crates/czsc-*/src/*.rs` 后 `maturin develop --uv` 触发增量编译
- [ ] （可选）`maturin build --release` 产出 `.whl` 文件

---

## 7. 常见踩坑 FAQ

### 7.1 不要直接 `cargo build --workspace`

表现：链接阶段报 `undefined symbol: PyGILState_Release` 或类似。

根因：本项目根 [Cargo.toml](../../Cargo.toml) 注释里写得很清楚

> cdylib 路径要 `pyo3/extension-module + pyo3/abi3-py310`（不链接具体 libpython），
> stub_gen 路径要裸 `pyo3`（链接具体版本 libpython）。同一 package feature 集不
> 可能同时满足两者，所以用 `[features]` 隔离。

裸跑 `cargo build --workspace` 会让 cargo 把 `extension-module + abi3-py310` 强制用于所有 crate，导致业务 crate 凭空要求"不链接 libpython，但运行时又找不到 `PyGILState_Release` 这种符号"。

**解决办法**：始终用 `maturin develop` 或 `maturin build`，由 maturin 自己决定 feature。

如果你确实要跑 Rust 单元测试，绕开 maturin 的写法是显式传 `PYO3_PYTHON`：

```bash
PYO3_PYTHON=$(uv run python -c "import sys; print(sys.executable)") \
    cargo test -p czsc-core
```

这样 cargo 才能找到 `libpython3.X.dylib/.so` 的具体符号。

### 7.2 改了 `Cargo.toml` 但 maturin 没识别

可能 maturin 用哈希缓存了产物。强制重编：

```bash
uv run --no-sync maturin develop --uv -f       # -f = force

# 或者：仅清这一 crate 的缓存
cargo clean -p czsc-python
uv run --no-sync maturin develop --uv
```

### 7.3 maturin 报 `protoc not found`

本项目 `pyo3-stub-gen = "0.22" + inventory` 不需要 protoc。如果上游某天加了 `tonic` / `prost` 之类子依赖：

```bash
brew install protobuf
```

### 7.4 装过 PyPI 版 czsc 与 maturin develop 冲突

```bash
# 先删 PyPI 版本
uv pip uninstall czsc

# 看指向
uv run python -c "import czsc; print(czsc.__file__)"
# 期望：.venv/lib/python3.13/site-packages/czsc/__init__.py

# 重装源码版本
uv run --no-sync maturin develop --uv
```

### 7.5 maturin 跨架构 / universal2 警告

warning，不影响功能。可以消除但**没有意义**（开发模式够了）：

```bash
uv run --no-sync maturin develop --uv --target universal2-apple-darwin
```

代价：编译时间 ×2，published wheel 体积 ×2。仅发布流水线需要。

### 7.6 chrono / polars / pyo3-stub-gen 版本"撞车"

看根 [Cargo.toml](../../Cargo.toml) 第 19-31 行注释。`polars = 0.52.0` 是因为 polars 0.53 对 chrono 的约束会与 pyo3-stub-gen 冲突。**这是上游问题，不是项目策略。** 解锁要等 polars 放宽 chrono 版本约束。

日常不用管——`uv sync` 自动锁定正确组合，除非你自己改了 `Cargo.toml`。

### 7.7 Python 3.14 上 `pyarrow 21.0.0` 没有 wheel —— **必须降到 3.13**

**症状**：`uv sync --extra all --python 3.14` 卡在 pyarrow 构建阶段。先是几百行 setuptools 的 `SetuptoolsDeprecationWarning`（无害），最后两行致命：

```
CMake Error at CMakeLists.txt:289 (find_package):
  Could not find a package configuration file provided by "Arrow" with any of
  the following names:
    ArrowConfig.cmake / arrow-config.cmake / Arrow.cps / arrow.cps
error: command '/usr/local/bin/cmake' failed with exit code 1
help: `pyarrow` (v21.0.0) was included because `czsc` depends on `pyarrow`
```

装 cmake 之后还会继续撞这堵墙——因为不是 cmake 的问题，是 **Arrow C++ 系统库**没装。

**根因**：pyarrow 21.0.0（项目当前锁定的版本）**不发布** Python 3.14 的预编译 wheel。PyPI 上 pyarrow 21.0.0 的 wheel 矩阵只覆盖：

```
cp39 / cp310 / cp311 / cp312 / cp313
  ✅ macOS arm64 + x86_64
  ✅ manylinux + musllinux
  ✅ win_amd64
cp314 / cp314t：❌ 一个都没有
```

uv 找不到 cp314 wheel 就 **退回 sdist 源码构建**，需要系统装好 Apache Arrow C++ 库（`brew install apache-arrow` 拿到的版本通常与 pyarrow 21 锁的 Arrow C++ 版本不匹配，依然会失败）。

**解决**：**降到 Python 3.13**（或更老的 3.12 / 3.11）：

```bash
uv sync --extra all --python 3.13     # ← 第一次就指定，别用 3.14
uv run --no-sync maturin develop --uv
uv run --no-sync pytest
```

**何时能重新用 3.14**：等上游两条之一解决：
- pyarrow 22.0.0（2026 末预计）发布 cp314 wheel；或
- 本项目 [pyproject.toml](../../pyproject.toml) 把 pyarrow 升到 22+。

【快速核查脚本】可以验证某个指定版本对当前 Python 解释器有没有 wheel：

```bash
uv run --no-sync python -c "
import sys, urllib.request, json
v = '21.0.0'
mp = sys.implementation.cache_tag
data = json.loads(urllib.request.urlopen(f'https://pypi.org/pypi/pyarrow/{v}/json').read())
wheels = [u['filename'] for u in data['urls'] if u['filename'].endswith('.whl')]
match = [w for w in wheels if mp in w]
print('current:', mp)
print('available wheels with this tag:', len(match))
print('first 3:', match[:3])
"
```

如果 `match` 是 0，**马上换 Python 解版器**，别折腾 compiler。

---

## 8. 一行命令总结

| 场景            | 命令                                                          |
| --------------- | ------------------------------------------------------------- |
| 首次完整搭建    | `uv sync --extra all --python 3.13 && uv run --no-sync maturin develop --uv` |
| Rust 改动后      | `uv run --no-sync maturin develop --uv`                       |
| 只改 Python 后   | `uv run --no-sync pytest`                                     |
| 串起来（最强）  | `uv run --no-sync maturin develop --uv && uv run --no-sync pytest -x` |
| 出 wheel（发布）| `uv run --no-sync maturin build --release`                    |

---

## 9. 关联文档

- [CLAUDE.md](../../CLAUDE.md) —— 项目宪法与日常约定
- [Cargo.toml](../../Cargo.toml) —— Rust workspace 顶层配置，看 `pyo3 = "0.28"` 那段注释能加深对 feature 设计的理解
- `docs/migration/` —— 历史迁移说明（不直接相关，但是阅读脉络）
- `docs/examples/` —— 用底层 API 写的示例脚本（构建完之后跑这些）
