# ADR 0001: K 线 Arrow 输出增加 MA/MACD 段 struct 列

- **状态**：已锁定（用户 2026-07-29 grill-with-docs 多轮访谈后落地）
- **范围**：qdata Arrow Flight `do_get` 返回的 Kline RecordBatch

## 背景

qdata 已有 Kline 指标列（MA5/MA10/MA20/MA60/MA120/MA250、MACD、DIF、DEA、Bollinger、jc/sc 布尔列）。下游 quant-lab 在 `lab/chan.py::SegUtil` 里把 jc/sc 布尔列转换成"段"对象（Seg），再消费段的 start/end/high/low/jc/sc/close_min/close_max 做策略。

**问题**：当前段计算在 quant-lab 端做，需要它先全量消费 K 线才能产出段结果，跨服务往返、不可由 qdata 单边优化。

## 决策

qdata 直接在生成 RecordBatch 时为每根 K 线计算"它所属的段"，作为四个 nullable 列输出：
- `ma_segment`: MA5/MA10 段（Struct）
- `macd_segment`: DIF/DEA 段（Struct）
- `ma_recent_seg_bars`: 最近 N 个已结束 MA5/MA10 段结束点 K 线 idx（List<Int64>）
- `macd_recent_seg_bars`: 同上，MACD（List<Int64>）

**N 是运行时可调的**，由 `DatasetTicket.recent_seg_n` 控制；不同 query 可传不同 N。

### Struct 字段（与 quant-lab `Seg` 1:1 对齐）

| 字段 | 类型 | 含义 |
|---|---|---|
| `start` | int64 | 段起点 K 线 idx（= 上一交叉 idx，保证段间衔接） |
| `end` | int64 | 段终点 K 线 idx（上涨段=high_idx，下跌段=low_idx，段内极值） |
| `high` | float64 | 段内 high 极值 |
| `low` | float64 | 段内 low 极值 |
| `jc_idx` | int64 | 金叉 K 线 idx |
| `sc_idx` | int64 | 死叉 K 线 idx |
| `close_min` | float64 | `[start, end]` 闭区间 close 最小值 |
| `close_max` | float64 | `[start, end]` 闭区间 close 最大值 |
| `direction` | utf8 | `"up"` 或 `"down"` |

**段衔接硬约束**：除首段外，`seg_n.start == seg_{n-1}.end`。首段 `start = 0`（系列首个 bar idx）。
- **start** 语义：交叉区间端点（保证段衔接）
- **end** 语义：段内极值 idx（上涨段 high、下跌段 low）— 与 quant-lab `Seg` 1:1

Struct 内部所有字段 nullable=false；Struct 列 nullable=true。

### 段在 K 线上的归属（已锁定，用户原话）

> "jc bar 产生 closed 下跌段，这个 jc bar 填下跌段的信息，反之 sc bar 产生上涨段"

即：
- **jc bar** 填**已结束下跌段**（区间 `[prev_sc, jc_idx]`，direction="down"）
- **sc bar** 填**已结束上涨段**（区间 `[prev_jc, sc_idx]`，direction="up"）
- 非交叉 bar 上 struct 列 NULL
- 进行中段（最后一段未结束）不输出

### 算法

逐 bar O(n) 状态机。每根 bar 上：

1. 读 `jc_arr[i]` / `sc_arr[i]` 决定当前 bar 是否为交叉点。
2. 维护 cross_state（最近一次交叉类型 = up/down/none）和上一交叉点位置。
3. 若当前 bar 是 jc：上一交叉为 sc → 计算 `[prev_sc, i]` 的 down 段，start=prev_sc_idx、end=jc_idx，high/low 取自段内 running 极值，close_min/close_max 从 close 数组扫，写到 `ma_segment[i]`。
4. 若当前 bar 是 sc：上一交叉为 jc → 计算 `[prev_jc, i]` 的 up 段，start=prev_jc_idx、end=sc_idx，写到 `ma_segment[i]`。
5. 更新 state：prev_idx = i（保证下段 start = 本段 end）。

状态机不依赖反向索引，整体 O(n)。

### MACD 段

复用同一状态机，输入为 `macd_jc: Vec<bool>` / `macd_sc: Vec<bool>` + `dif: Vec<f64>` / `dea: Vec<f64>`。

### recent_seg_bars 列

每根 bar 都填 List<Int64>，值为当前 bar 之前（含当前）累计的最近 N 个已结束段"结束点 K 线 idx"。

N 来自 `DatasetTicket.recent_seg_n`，默认 20。

状态机在写 struct 列的同时，额外把当前段结束点 idx 推入 ring buffer；溢出 N 时从尾部弹出最旧的一个。每根 bar 都读取当前快照填 List 值。不足 N 的 bar（系列开头）按实际长度填，下游自行看 length。

### Series 独立性

每个 `(exchange, symbol, freq)` series 内部独立计算段；不跨 series。

## 替代方案与权衡

| 方案 | 否决理由 |
|---|---|
| 沿用 quant-lab `_find_seg_by_indices` 批量算法（O(n²) 最坏） | 与本任务"逐 bar O(n)"指令冲突 |
| 在 Flight 客户端做段计算 | quant-lab 端已有现成实现，但 qdata 单边加列可省下游一次扫表 |
| 同时输出进行中段（end=last_idx） | 与"只输出已结束段"锁定冲突 |
| 不加 struct 列、改加 jc_idx/sc_idx/end_idx/start_idx/high/low/close_min/close_max 八列 | schema 不可读、与 quant-lab Seg 不对位；下游需自己组装 |
| struct 字段命名用语义化（jc_bar_idx 等） | 与 quant-lab 拼写不对齐，下游消费无法 1:1 |
| N=20 硬编码到列名 | 与"20 是可变的"锁定冲突 |

## 影响

- `KlineProcessor` 增加字段：`ma_segment: Vec<Option<SegData>>` / `macd_segment: Vec<Option<SegData>>` / `ma_recent_seg_bars: Vec<Vec<i64>>` / `macd_recent_seg_bars: Vec<Vec<i64>>`
- `indicators()` 末尾追加 `compute_segments(recent_seg_n: usize)` 调用
- `create_record_batch()` 用 `StructArray::from(...)` 和 `ListArray::from_iter_primitive(...)` 构造四列
- `DatasetTicket` 新增 `recent_seg_n: usize` 字段；旧 ticket 反序列化需 `serde(default = "default_20")`
- 下游（quant-lab）可直接 `pa.ipc()` 读 arrow，跳过自建 SegUtil
- 无新依赖（arrow 已在）

## 反向成本

- 段定义若要调整（如改 close_min 区间），需要改 qdata + 重发 arrow schema；下游消费者需同步更新
- struct 列带来 ~80 bytes/row 额外开销；List 列每行 ~32 bytes 头部 + 8 bytes × seg count（按 N=20 满载、100w 行 ≈ 16GB，量化进 qps）
- 调高 N 会线性放大 List 列带宽

## 变更记录

### 2026-07-30：seg 列从 Struct 改为 FixedSizeBinary（行存储）

**动机**：将 11 字段 struct 拆为下游多列读取；改为单列定长 binary 后：
- schema 列数更少、可读性提升
- 下游解析为 `struct.unpack` 一次到位，零拷贝
- 字段对齐与 quant-lab `Seg` 一致性更强（去掉了 11 字段中 padding 的方向字符串，改为 i32 enum）

**变更**（破坏性，下游需同步更新）：
- `ma_segment` / `macd_segment` 类型：`Struct(11 子字段)` → `FixedSizeBinary(88)` (nullable=true)
- non-null bar 上为 88 字节 little-endian 编码；null bar 上为 NULL
- `recent_ma_segment_bars` / `recent_macd_segment_bars` 不变（List<Int64>）

**字节布局**（88 字节）：

| off | 字段 | 类型 | 字节 |
|---|---|---|---|
| 0 | `start` | i64 | 8 |
| 8 | `end` | i64 | 8 |
| 16 | `high` | f64 | 8 |
| 24 | `low` | f64 | 8 |
| 32 | `jc_idx` | i64 | 8 |
| 40 | `sc_idx` | i64 | 8 |
| 48 | `close_min` | f64 | 8 |
| 56 | `close_max` | f64 | 8 |
| 64 | `close_min_idx` | i64 | 8 |
| 72 | `close_max_idx` | i64 | 8 |
| 80 | `direction` | i32 (-1=down, 1=up，sign 表方向) | 4 |
| 84 | _padding_ | u32 | 4 |

**下游解码参考**（Python）：
```python
# 注意：Python `struct` 里 f=f32(4B)，d=f64(8B)，跟 Rust 命名不一致
SEG_FMT = struct.Struct("<qq d d qq d d qq i 4x")  # 88 字节
direction = {-1: "down", 1: "up"}[SEG_FMT.unpack_from(buf)[10]]
```

**影响范围**：
- `src/kline_processor.rs`：`encode_seg` / `build_seg_binary_array` 替代原 `build_seg_struct_array`，`SEG_BYTES=88` 常量集中维护布局
- `test_seg.py`：按新布局解码
- quant-lab / signalview / atlas/klineproxy：原本按 `pa.struct([...])` 取字段的代码需改为 `pa.binary(88)` + `unpack`
- 无新依赖

**反向成本**：seg 字段名不再有，下游 IDE hover 不到字段含义——需在 schema 旁附本节布局表。
