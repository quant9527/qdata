#!/usr/bin/env python3
"""Test segment data for 德明利

ma_segment / macd_segment 现在是 FixedSizeBinary(88) 行存储列：
非交叉 bar 为 NULL，交叉 bar 为 88 字节 little-endian 编码。

布局：
    off  field          type   size
    ──   ─────          ────   ────
     0   start          i64     8
     8   end            i64     8
    16   high           f64     8
    24   low            f64     8
    32   jc_idx         i64     8
    40   sc_idx         i64     8
    48   close_min      f64     8
    56   close_max      f64     8
    64   close_min_idx  i64     8
    72   close_max_idx  i64     8
    80   direction      i32     4   (0 = down, 1 = up)
    84   _padding       u32     4

注意：Python `struct` 里 `f`=f32(4B)，`d`=f64(8B)，与 Rust 的 f32/f64 不一致。
"""
import json
import struct
from typing import NamedTuple

import pyarrow as pa
import pyarrow.flight as flight

client = flight.FlightClient("grpc://127.0.0.1:50001")

ticket_data = {
    "name": "kline_as_301308_1d",
    "tags": ["as_301308_1d"],
    "start_time": 0,
    "end_time": 1999999999999,
    "recent_seg_n": 20,
}
ticket = flight.Ticket(json.dumps(ticket_data).encode())

reader: flight.FlightStreamReader = client.do_get(ticket)
table = reader.read_all()

print(f"Rows: {table.num_rows}, Columns: {table.num_columns}")
print(f"Columns: {table.column_names}")

# 找到 seg 列
seg_cols = [c for c in table.column_names if c in ("ma_segment", "macd_segment")]
print(f"\nSegment columns: {seg_cols}")
for col in seg_cols:
    f = table.field(col)
    print(f"  {col} type = {f.type} nullable = {f.nullable}")

SEG_FMT = struct.Struct("<qq d d qq d d qq i 4x")  # 11 字段 88 字节（最后 4 字节 padding）
# direction: -1=down, 1=up（sign 直接表方向）


class Seg(NamedTuple):
    """88 字节定长 segment 解码结果。字段名跟 Rust 端 1:1。

    注意：direction 保持原始 i32 (0=down, 1=up)，不映射成字符串。
    下游若需要字符串，请用 DIR_NAMES[seg.direction] 自己查。
    """
    start: int
    end: int
    high: float
    low: float
    jc_idx: int
    sc_idx: int
    close_min: float
    close_max: float
    close_min_idx: int
    close_max_idx: int
    direction: int

    @classmethod
    def from_bytes(cls, buf: bytes) -> "Seg":
        return cls(*SEG_FMT.unpack_from(buf))


DIR_NAMES = {0: "down", 1: "up"}


def decode_seg(buf: bytes) -> Seg:
    return Seg(*SEG_FMT.unpack_from(buf))


for col in seg_cols:
    print(f"\n=== {col} ===")
    col_data = table.column(col)
    for i, val in enumerate(col_data):
        if val.as_py() is None:
            continue
        seg = decode_seg(val.as_py())
        print(
            f"  bar[{i}] direction={seg.direction} "
            f"start={seg.start} end={seg.end} "
            f"high={seg.high:.2f} low={seg.low:.2f} "
            f"jc={seg.jc_idx} sc={seg.sc_idx}"
        )

# recent_seg_bars 列保持 List<Int64> 不变
recent_cols = [c for c in table.column_names if "recent" in c]
for col in recent_cols:
    print(f"\n=== {col} (first 5 bars with data) ===")
    col_data = table.column(col)
    count = 0
    for i in range(table.num_rows):
        val = col_data[i].as_py()
        if val and len(val) > 0:
            print(f"  bar[{i}]: {val}")
            count += 1
            if count >= 5:
                break
