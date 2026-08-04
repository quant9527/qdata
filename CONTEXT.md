# CONTEXT — dataservice

## Glossary

### MA Segment（MA5MA10 段）
由 MA5 与 MA10 的一次"金叉→死叉"或"死叉→金叉"行程定义的段。段结构与 quant-lab `lab/chan.py::Seg` 1:1 对齐。

字段（struct 子字段，全不 nullable）：
- `start: int64` — 段起点 K 线索引（= 上一交叉 idx，保证段间衔接；首段 = 0）
- `end: int64` — 段终点 K 线索引（上涨段=high_idx，下跌段=low_idx，段内极值）
- `high: float64` — 段内最高价
- `low: float64` — 段内最低价
- `jc_idx: int64` — 金叉 K 线索引
- `sc_idx: int64` — 死叉 K 线索引
- `close_min: float64` — `[start, end]` 闭区间内 close 最小值
- `close_max: float64` — `[start, end]` 闭区间内 close 最大值
- `direction: i32` — `-1` = down, `1` = up（sign 表方向）

**段在 K 线上的归属（关键决策）**：段挂在**反向交叉 bar**上——
- **jc bar**（金叉）：填**已结束的下跌段**（即 `[prev_sc, jc_idx]` 区间）。语义：金叉意味着"刚才那段下跌已结束"。
- **sc bar**（死叉）：填**已结束的上涨段**（即 `[prev_jc, sc_idx]` 区间）。语义：死叉意味着"刚才那段上涨已结束"。
- 非交叉 bar 上，struct 列为 NULL。

进行中段不输出（最后一个未结束段不出现在任何 bar 上）。

### MACD Segment（MACD 段）
复用同一段算法，参数为 `fast=dif, slow=dea`。字段同上（jc/sc 仍指"DIF/DEA 金叉死叉"）。

### recent_seg_bars（近期段 K 线索引 List 列）
**N 是运行时可调的**，由 `DatasetTicket.recent_seg_n` 控制，每条 query 可不同。

每根 bar 都填 List<Int64>，值为当前 bar 之前（含当前）累计的最近 N 个已结束段"结束点 K 线 idx"。不足 N 的 bar（系列开头）按实际长度填。

### Series 独立性
每个 `(exchange, symbol, freq)` series 内部独立计算段，不跨 series。

## 数据落地

K 线输出增加四个列：

| 列名 | 类型 | 含义 |
|---|---|---|
| `ma_segment` | FixedSizeBinary(88) (nullable=true) | MA5MA10 段（行存储，仅在 jc/sc bar 上填） |
| `macd_segment` | FixedSizeBinary(88) (nullable=true) | MACD 段（同上） |
| `ma_recent_seg_bars` | List<Int64> (nullable=true) | 最近 N 个已结束 MA5MA10 段的结束点 K 线 idx；N 来自 ticket |
| `macd_recent_seg_bars` | List<Int64> (nullable=true) | 同上，MACD |

**Seg 字节布局（88 字节，little-endian）**：

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

Python 解码示例（test_seg.py）：
```python
# 注意：Python `struct` 里 f=f32(4B)，d=f64(8B)，跟 Rust 命名不一致
SEG_FMT = struct.Struct("<qq d d qq d d qq i 4x")  # 88 字节
fields = SEG_FMT.unpack_from(buf)
direction = {-1: "down", 1: "up"}[fields[10]]
```

FixedSizeBinary 列在每根交叉 bar 上挂 88 字节编码段，其他 bar 上为 NULL。List 列每根 bar 都填。

## DatasetTicket 增量

```rust
pub struct DatasetTicket {
    pub name: String,
    pub tags: Vec<String>,
    pub start_time: i64,
    pub end_time: i64,
    pub kline_reverse: bool,
    pub kline_aggregate: String,
    pub kline_snapshot: bool,
    pub recent_seg_n: usize,   // 新增；默认 20
}
```

## 配套

ADR 0001：`docs/adr/0001-kline-segment-struct.md`