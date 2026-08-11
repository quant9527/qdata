use anyhow::Result;
use arrow::array::{
    ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array, ListArray, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, TimeZone, Utc, Weekday};
use std::sync::Arc;

#[derive(Debug, serde::Deserialize, Clone)]
pub struct KlineSeries {
    pub exchange: String,
    pub symbol: String,
    pub freq: String,
    pub timestamps: Vec<i64>,
    pub opens: Vec<f64>,
    pub closes: Vec<f64>,
    pub highs: Vec<f64>,
    pub lows: Vec<f64>,
    pub vols: Vec<f64>,
    pub qvs: Vec<f64>,
    pub end_ts: Vec<i64>,
}

impl KlineSeries {
    /// Create a new KlineDataSeries with the specified parameters
    pub fn new(exchange: String, symbol: String, freq: String) -> Self {
        Self {
            exchange,
            symbol,
            freq,
            timestamps: Vec::new(),
            opens: Vec::new(),
            closes: Vec::new(),
            highs: Vec::new(),
            lows: Vec::new(),
            vols: Vec::new(),
            qvs: Vec::new(),
            end_ts: Vec::new(),
        }
    }

    pub fn from_strs(exchange: &str, symbol: &str, freq: &str) -> Self {
        Self::new(exchange.to_string(), symbol.to_string(), freq.to_string())
    }

    /// Get the length of the data
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    /// Check if the data is empty
    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    /// Reverse kline prices (not the order of data)
    /// This implements the logic similar to:
    /// ```python
    /// def reverse_kline(df):
    ///     # Calculate the global max and min of the entire K-line sequence
    ///     global_max = df['high'].max()
    ///     global_min = df['low'].min()
    ///
    ///     # Use vectorized operations to calculate reversed prices
    ///     df_reversed = df.copy()
    ///     df_reversed['open'] = global_max + global_min - df['open']
    ///     df_reversed['high'] = global_max + global_min - df['low']
    ///     df_reversed['low'] = global_max + global_min - df['high']
    ///     df_reversed['close'] = global_max + global_min - df['close']
    ///
    ///     return df_reversed
    /// ```
    pub fn reverse_prices(&mut self) {
        if self.highs.is_empty() || self.lows.is_empty() {
            return;
        }

        // Calculate global max and min
        let global_max = self.highs.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let global_min = self.lows.iter().cloned().fold(f64::INFINITY, f64::min);

        // Reverse prices using vectorized operations
        for i in 0..self.len() {
            let old_open = self.opens[i];
            self.opens[i] = global_max + global_min - old_open;
            let old_high = self.highs[i];
            let old_low = self.lows[i];
            self.highs[i] = global_max + global_min - old_low;
            self.lows[i] = global_max + global_min - old_high;
            let old_close = self.closes[i];
            self.closes[i] = global_max + global_min - old_close;
        }
    }

    pub fn push_from(&mut self, other: &KlineSeries, idx: usize) {
        self.push(
            other.timestamps[idx], other.opens[idx], other.closes[idx],
            other.highs[idx], other.lows[idx], other.vols[idx],
            other.qvs[idx], other.end_ts[idx],
        );
    }

    /// Push new data to all vectors
    pub fn push(
        &mut self,
        ts: i64,
        open: f64,
        close: f64,
        high: f64,
        low: f64,
        vol: f64,
        qv: f64,
        end_ts: i64,
    ) {
        self.timestamps.push(ts);
        self.opens.push(open);
        self.closes.push(close);
        self.highs.push(high);
        self.lows.push(low);
        self.vols.push(vol);
        self.qvs.push(qv);
        self.end_ts.push(end_ts);
    }
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct FactorRecord {
    pub exchange: String,
    pub symbol: String,
    pub ts: i64,
    pub f: f64,
}

/// 已结束段的内部表示（与 quant-lab `Seg` 字段 1:1）。
/// 字段含义见 CONTEXT.md / ADR 0001。
#[derive(Debug, Clone)]
pub(crate) struct SegData {
    pub start: i64,
    pub end: i64,
    pub high: f64,
    pub low: f64,
    pub jc_idx: i64,
    pub sc_idx: i64,
    pub close_min: f64,
    pub close_max: f64,
    pub close_min_idx: i64,
    pub close_max_idx: i64,
    pub direction: &'static str,
    /// pct_change 在 [start, end]（含端点）的最大/最小值
    pub pct_max: f64,
    pub pct_min: f64,
}

impl SegData {
    /// start / end / high / low 由调用方传入（来自状态机累加极值）。
    /// close_min / close_max / close_min_idx / close_max_idx / pct_max / pct_min
    /// 均从 [start, sc_idx) 区间扫出，覆盖完整段移动（与 quant-lab `lab.chan` 对齐）。
    fn compute(
        jc_idx: i64,
        sc_idx: i64,
        start: i64,
        end: i64,
        high: f64,
        low: f64,
        closes: &[f64],
        pct_change: &[f64],
        direction: &'static str,
    ) -> Self {
        let n = closes.len();
        let close_lo = start.max(0).min(n as i64 - 1) as usize;
        let close_hi = sc_idx.max(0).min(n as i64).max(close_lo as i64 + 1) as usize;

        // close 极值区间：[start, sc_idx)，覆盖完整段移动。
        let close_window = &closes[close_lo..close_hi];
        // 单次遍历同时取极值与极值位置
        let mut close_min = f64::INFINITY;
        let mut close_max = f64::NEG_INFINITY;
        let mut close_min_local: usize = 0;
        let mut close_max_local: usize = 0;
        for (i, &c) in close_window.iter().enumerate() {
            if c < close_min {
                close_min = c;
                close_min_local = i;
            }
            if c > close_max {
                close_max = c;
                close_max_local = i;
            }
        }
        // 与 numpy 一致的 NaN 传播语义（np.max/min 遇 NaN 得 NaN；
        // 客户端拿 NaN 会回退本地计算，保证与生产路径逐点一致）
        fn fold_max_nan(w: &[f64]) -> f64 {
            w.iter().copied().fold(f64::NEG_INFINITY, |a, b| {
                if a.is_nan() || b.is_nan() { f64::NAN } else { a.max(b) }
            })
        }
        fn fold_min_nan(w: &[f64]) -> f64 {
            w.iter().copied().fold(f64::INFINITY, |a, b| {
                if a.is_nan() || b.is_nan() { f64::NAN } else { a.min(b) }
            })
        }
        // pct_max/min: [start, sc_idx) 覆盖完整段移动
        let (pct_max, pct_min) = match pct_change.get(close_lo..close_hi) {
            Some(w) if !w.is_empty() => (fold_max_nan(w), fold_min_nan(w)),
            _ => (f64::NAN, f64::NAN),
        };
        Self {
            start,
            end,
            high,
            low,
            jc_idx,
            sc_idx,
            close_min,
            close_max,
            close_min_idx: close_lo as i64 + close_min_local as i64,
            close_max_idx: close_lo as i64 + close_max_local as i64,
            direction,
            pct_max,
            pct_min,
        }
    }
}

#[derive(Debug)]
pub struct KlineProcessor {
    data: KlineSeries,
    pub qfq_idx: usize,
    pub ind_idx: usize,
    factors: Vec<FactorRecord>,

    // Indicator storage
    ma5: Vec<f64>,
    ma10: Vec<f64>,
    ma20: Vec<f64>,
    ma60: Vec<f64>,
    ma120: Vec<f64>,
    ma250: Vec<f64>,
    ma5ma10_diff: Vec<f64>, // ma5 - ma10差值
    vol5: Vec<f64>,         // 5-day volume moving average
    vol10: Vec<f64>,        // 10-day volume moving average
    ema12: Vec<f64>,
    ema26: Vec<f64>,
    dif: Vec<f64>,
    dea: Vec<f64>,
    macd: Vec<f64>,
    pct_change: Vec<f64>, // 添加涨跌幅存储字段
    // Bollinger Bands
    bb_upper: Vec<f64>,
    bb_middle: Vec<f64>,
    bb_lower: Vec<f64>,
    // Cross indicators
    macd_jc: Vec<bool>,
    macd_sc: Vec<bool>,
    ma5ma10_jc: Vec<bool>,
    ma5ma10_sc: Vec<bool>,
    // 前一根收盘同时低于 MA5/MA10、当前收盘同时站上（cl2b_pair 入口条件之一）
    cross_ma5ma10: Vec<bool>,
    ma5ma60_jc: Vec<bool>,
    ma5ma60_sc: Vec<bool>,
    ma5ma120_jc: Vec<bool>,
    ma5ma120_sc: Vec<bool>,
    ma5ma250_jc: Vec<bool>,
    ma5ma250_sc: Vec<bool>,
    // Track previous values for calculations
    prev_ema12: Option<f64>,
    prev_ema26: Option<f64>,
    prev_dea: Option<f64>,
    current_factor_index: usize,

    // 已结束段（与 quant-lab Seg 1:1）。长度 = data.len()，非交叉 bar 上为 None。
    ma_segment: Vec<Option<SegData>>,
    macd_segment: Vec<Option<SegData>>,
    // 最近 N 个已结束段结束点 K 线 idx 的 ring buffer 快照，每根 bar 一份
    ma_recent_seg_bars: Vec<Vec<i64>>,
    macd_recent_seg_bars: Vec<Vec<i64>>,

    // === 段状态机增量状态 ===
    // MA5MA10
    seg_ma_prev_cross: Option<&'static str>,
    seg_ma_prev_idx: i64,
    seg_ma_prev_seg_end: i64,         // 上一段的 end（即段内极值 idx），保证段衔接
    seg_ma_running_low_idx: i64,
    seg_ma_running_low: f64,
    seg_ma_running_high_idx: i64,
    seg_ma_running_high: f64,
    seg_ma_buf: std::collections::VecDeque<i64>,
    // MACD
    seg_macd_prev_cross: Option<&'static str>,
    seg_macd_prev_idx: i64,
    seg_macd_prev_seg_end: i64,
    seg_macd_running_low_idx: i64,
    seg_macd_running_low: f64,
    seg_macd_running_high_idx: i64,
    seg_macd_running_high: f64,
    seg_macd_buf: std::collections::VecDeque<i64>,
}

impl KlineProcessor {
    pub fn create_record_batch(&self) -> Result<RecordBatch> {
        let ts_len = self.data.len();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("exchange", DataType::Utf8, false),
                Field::new("symbol", DataType::Utf8, false),
                Field::new("freq", DataType::Utf8, false),
                Field::new("timestamp", DataType::Int64, false),
                Field::new("open", DataType::Float64, false),
                Field::new("close", DataType::Float64, false),
                Field::new("high", DataType::Float64, false),
                Field::new("low", DataType::Float64, false),
                Field::new("vol", DataType::Float64, false),
                Field::new("qv", DataType::Float64, false),
                Field::new("end_ts", DataType::Int64, false),
                Field::new("ma5", DataType::Float64, true),
                Field::new("ma10", DataType::Float64, true),
                Field::new("ma20", DataType::Float64, true),
                Field::new("ma60", DataType::Float64, true),
                Field::new("ma120", DataType::Float64, true),
                Field::new("ma250", DataType::Float64, true),
                Field::new("ma5ma10", DataType::Float64, true), // ma5 - ma10差值
                Field::new("vol5", DataType::Float64, true),    // 5-day volume moving average
                Field::new("vol10", DataType::Float64, true),   // 10-day volume moving average
                Field::new("macd", DataType::Float64, true),
                Field::new("dif", DataType::Float64, true),
                Field::new("dea", DataType::Float64, true),
                Field::new("bb_upper", DataType::Float64, true), // Bollinger Bands upper band
                Field::new("bb_middle", DataType::Float64, true), // Bollinger Bands middle band
                Field::new("bb_lower", DataType::Float64, true), // Bollinger Bands lower band
                Field::new("macd_jc", DataType::Boolean, true),
                Field::new("macd_sc", DataType::Boolean, true),
                Field::new("ma5ma10_jc", DataType::Boolean, true),
                Field::new("ma5ma10_sc", DataType::Boolean, true),
                Field::new("cross_ma5ma10", DataType::Boolean, true), // 收盘价上穿 MA5&MA10
                Field::new("ma5ma60_jc", DataType::Boolean, true),
                Field::new("ma5ma60_sc", DataType::Boolean, true),
                Field::new("ma5ma120_jc", DataType::Boolean, true),
                Field::new("ma5ma120_sc", DataType::Boolean, true),
                Field::new("ma5ma250_jc", DataType::Boolean, true),
                Field::new("ma5ma250_sc", DataType::Boolean, true),
                Field::new("pct_change", DataType::Float64, true),
                // MA5MA10 段：行存储定长 binary（仅交叉 bar 有值，non-null = sentinel）
                // 见 build_seg_binary_array / encode_seg 的字段布局（11 字段 = 88 bytes）
                Field::new("ma_segment", DataType::FixedSizeBinary(SEG_BYTES), true),
                // MACD 段：行存储定长 binary（同上）
                Field::new("macd_segment", DataType::FixedSizeBinary(SEG_BYTES), true),
                // 最近 N 个已结束 MA5MA10 段结束点 K 线 idx，每根 bar 一份
                Field::new(
                    "recent_ma_segment_bars",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    true,
                ),
                Field::new(
                    "recent_macd_segment_bars",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    true,
                ),
            ])),
            vec![
                Arc::new(StringArray::from(vec![self.data.exchange.as_str(); ts_len])),
                Arc::new(StringArray::from(vec![self.data.symbol.as_str(); ts_len])),
                Arc::new(StringArray::from(vec![self.data.freq.as_str(); ts_len])),
                Arc::new(Int64Array::from(self.data.timestamps.clone())),
                Arc::new(Float64Array::from(self.data.opens.clone())),
                Arc::new(Float64Array::from(self.data.closes.clone())),
                Arc::new(Float64Array::from(self.data.highs.clone())),
                Arc::new(Float64Array::from(self.data.lows.clone())),
                Arc::new(Float64Array::from(self.data.vols.clone())),
                Arc::new(Float64Array::from(self.data.qvs.clone())),
                Arc::new(Int64Array::from(self.data.end_ts.clone())),
                Arc::new(Float64Array::from(self.ma5.clone())),
                Arc::new(Float64Array::from(self.ma10.clone())),
                Arc::new(Float64Array::from(self.ma20.clone())),
                Arc::new(Float64Array::from(self.ma60.clone())),
                Arc::new(Float64Array::from(self.ma120.clone())),
                Arc::new(Float64Array::from(self.ma250.clone())),
                Arc::new(Float64Array::from(self.ma5ma10_diff.clone())), // ma5 - ma10差值
                Arc::new(Float64Array::from(self.vol5.clone())), // 5-day volume moving average
                Arc::new(Float64Array::from(self.vol10.clone())), // 10-day volume moving average
                Arc::new(Float64Array::from(self.macd.clone())),
                Arc::new(Float64Array::from(self.dif.clone())),
                Arc::new(Float64Array::from(self.dea.clone())),
                Arc::new(Float64Array::from(self.bb_upper.clone())), // Bollinger Bands upper band
                Arc::new(Float64Array::from(self.bb_middle.clone())), // Bollinger Bands middle band
                Arc::new(Float64Array::from(self.bb_lower.clone())), // Bollinger Bands lower band
                Arc::new(BooleanArray::from(self.macd_jc.clone())),
                Arc::new(BooleanArray::from(self.macd_sc.clone())),
                Arc::new(BooleanArray::from(self.ma5ma10_jc.clone())),
                Arc::new(BooleanArray::from(self.ma5ma10_sc.clone())),
                Arc::new(BooleanArray::from(self.cross_ma5ma10.clone())),
                Arc::new(BooleanArray::from(self.ma5ma60_jc.clone())),
                Arc::new(BooleanArray::from(self.ma5ma60_sc.clone())),
                Arc::new(BooleanArray::from(self.ma5ma120_jc.clone())),
                Arc::new(BooleanArray::from(self.ma5ma120_sc.clone())),
                Arc::new(BooleanArray::from(self.ma5ma250_jc.clone())),
                Arc::new(BooleanArray::from(self.ma5ma250_sc.clone())),
                Arc::new(Float64Array::from(self.pct_change.clone())),
                build_seg_binary_array(&self.ma_segment, ts_len),
                build_seg_binary_array(&self.macd_segment, ts_len),
                build_recent_seg_list(&self.ma_recent_seg_bars, ts_len),
                build_recent_seg_list(&self.macd_recent_seg_bars, ts_len),
            ],
        )
        .map_err(|e| e.into())
    }

    /// Create a new KlineProcessor with the specified parameters
    pub fn new(exchange: &str, symbol: &str, freq: &str, factors: Vec<FactorRecord>) -> Self {
        Self {
            data: KlineSeries::from_strs(exchange, symbol, freq),
            qfq_idx: 0,
            ind_idx: 0,
            factors,
            ma5: Vec::new(),
            ma10: Vec::new(),
            ma20: Vec::new(),
            ma60: Vec::new(),
            ma120: Vec::new(),
            ma250: Vec::new(),
            ma5ma10_diff: Vec::new(),
            vol5: Vec::new(),  // 5-day volume moving average
            vol10: Vec::new(), // 10-day volume moving average
            ema12: Vec::new(),
            ema26: Vec::new(),
            dif: Vec::new(),
            dea: Vec::new(),
            macd: Vec::new(),
            pct_change: Vec::new(),
            bb_upper: Vec::new(),  // Bollinger Bands upper band
            bb_middle: Vec::new(), // Bollinger Bands middle band
            bb_lower: Vec::new(),  // Bollinger Bands lower band
            macd_jc: Vec::new(),
            macd_sc: Vec::new(),
            ma5ma10_jc: Vec::new(),
            ma5ma10_sc: Vec::new(),
            cross_ma5ma10: Vec::new(),
            ma5ma60_jc: Vec::new(),
            ma5ma60_sc: Vec::new(),
            ma5ma120_jc: Vec::new(),
            ma5ma120_sc: Vec::new(),
            ma5ma250_jc: Vec::new(),
            ma5ma250_sc: Vec::new(),
            current_factor_index: 0,
            prev_ema12: None,
            prev_ema26: None,
            prev_dea: None,
            ma_segment: Vec::new(),
            macd_segment: Vec::new(),
            ma_recent_seg_bars: Vec::new(),
            macd_recent_seg_bars: Vec::new(),
            seg_ma_prev_cross: None,
            seg_ma_prev_idx: -1,
            seg_ma_prev_seg_end: -1,
            seg_ma_running_low_idx: -1,
            seg_ma_running_low: f64::INFINITY,
            seg_ma_running_high_idx: -1,
            seg_ma_running_high: f64::NEG_INFINITY,
            seg_ma_buf: std::collections::VecDeque::new(),
            seg_macd_prev_cross: None,
            seg_macd_prev_idx: -1,
            seg_macd_prev_seg_end: -1,
            seg_macd_running_low_idx: -1,
            seg_macd_running_low: f64::INFINITY,
            seg_macd_running_high_idx: -1,
            seg_macd_running_high: f64::NEG_INFINITY,
            seg_macd_buf: std::collections::VecDeque::new(),
        }
    }

    /// Load KlineSeries directly into the processor
    /// This method only loads the data without any processing
    pub fn load(mut self, kline_data: KlineSeries) -> Self {
        let len = kline_data.len();
        self.data = kline_data;

        // Pre-allocate capacity for all indicator vectors since their final length
        // will be the same as the kline data length
        self.ma5.reserve(len);
        self.ma10.reserve(len);
        self.ma20.reserve(len);
        self.ma60.reserve(len);
        self.ma120.reserve(len);
        self.ma250.reserve(len);
        self.ma5ma10_diff.reserve(len);
        self.vol5.reserve(len);
        self.vol10.reserve(len);
        self.ema12.reserve(len);
        self.ema26.reserve(len);
        self.dif.reserve(len);
        self.dea.reserve(len);
        self.macd.reserve(len);
        self.pct_change.reserve(len);
        self.bb_upper.reserve(len); // Bollinger Bands upper band
        self.bb_middle.reserve(len); // Bollinger Bands middle band
        self.bb_lower.reserve(len); // Bollinger Bands lower band
        self.macd_jc.reserve(len);
        self.macd_sc.reserve(len);
        self.ma5ma10_jc.reserve(len);
        self.ma5ma10_sc.reserve(len);
        self.cross_ma5ma10.reserve(len);
        self.ma5ma60_jc.reserve(len);
        self.ma5ma60_sc.reserve(len);
        self.ma5ma120_jc.reserve(len);
        self.ma5ma120_sc.reserve(len);
        self.ma5ma250_jc.reserve(len);
        self.ma5ma250_sc.reserve(len);
        self.ma_segment.reserve(len);
        self.macd_segment.reserve(len);
        self.ma_recent_seg_bars.reserve(len);
        self.macd_recent_seg_bars.reserve(len);

        self
    }

    /// Get the length of the data
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn reverse_prices(&mut self) -> &mut Self {
        self.data.reverse_prices();
        self
    }

    pub fn qfq(&mut self) -> &mut Self {
        if self.data.timestamps.is_empty() || self.factors.is_empty() {
            return self;
        }
        let last_idx = self.qfq_idx;
        let last_ts = self.data.timestamps[last_idx];

        // Find the first factor where ts > last_ts
        while self.current_factor_index < self.factors.len()
            && self.factors[self.current_factor_index].ts <= last_ts
        {
            self.current_factor_index += 1;
        }

        if self.current_factor_index > 0 {
            let factor = self.factors[self.current_factor_index - 1].f;
            let last_idx = self.qfq_idx;
            self.data.opens[last_idx] /= factor;
            self.data.closes[last_idx] /= factor;
            self.data.highs[last_idx] /= factor;
            self.data.lows[last_idx] /= factor;
        }
        self
    }

    // ── MA ──
    pub fn compute_ma(&self) -> MaOutput {
        if self.data.closes.is_empty() {
            return MaOutput { ma5: f64::NAN, ma10: f64::NAN, ma20: f64::NAN, ma60: f64::NAN, ma120: f64::NAN, ma250: f64::NAN, ma5ma10_diff: f64::NAN, vol5: f64::NAN, vol10: f64::NAN };
        }
        let len = self.ind_idx + 1;
        let closes = &self.data.closes[..len];
        let vols = &self.data.vols[..len];
        let ma5  = if len >= 5   { closes[len-5..].iter().sum::<f64>() / 5.0   } else { f64::NAN };
        let ma10 = if len >= 10  { closes[len-10..].iter().sum::<f64>() / 10.0  } else { f64::NAN };
        let ma20 = if len >= 20  { closes[len-20..].iter().sum::<f64>() / 20.0  } else { f64::NAN };
        let ma60 = if len >= 60  { closes[len-60..].iter().sum::<f64>() / 60.0  } else { f64::NAN };
        let ma120 = if len >= 120 { closes[len-120..].iter().sum::<f64>() / 120.0 } else { f64::NAN };
        let ma250 = if len >= 250 { closes[len-250..].iter().sum::<f64>() / 250.0 } else { f64::NAN };
        let ma5ma10_diff = if ma5.is_finite() && ma10.is_finite() { ma5 - ma10 } else { f64::NAN };
        let vol5  = if len >= 5  { vols[len-5..].iter().sum::<f64>() / 5.0  } else { f64::NAN };
        let vol10 = if len >= 10 { vols[len-10..].iter().sum::<f64>() / 10.0 } else { f64::NAN };
        MaOutput { ma5, ma10, ma20, ma60, ma120, ma250, ma5ma10_diff, vol5, vol10 }
    }

    fn push_ma(&mut self, o: &MaOutput) {
        self.ma5.push(o.ma5); self.ma10.push(o.ma10); self.ma20.push(o.ma20);
        self.ma60.push(o.ma60); self.ma120.push(o.ma120); self.ma250.push(o.ma250);
        self.ma5ma10_diff.push(o.ma5ma10_diff); self.vol5.push(o.vol5); self.vol10.push(o.vol10);
    }

    // ── MACD ──
    pub fn compute_macd(&self) -> MacdOutput {
        if self.data.closes.is_empty() {
            return MacdOutput { ema12: f64::NAN, ema26: f64::NAN, dif: f64::NAN, dea: f64::NAN, macd: f64::NAN, next_ema12: self.prev_ema12, next_ema26: self.prev_ema26, next_dea: self.prev_dea };
        }
        let close = self.data.closes[self.ind_idx];
        if !close.is_finite() {
            return MacdOutput { ema12: f64::NAN, ema26: f64::NAN, dif: f64::NAN, dea: f64::NAN, macd: f64::NAN, next_ema12: self.prev_ema12, next_ema26: self.prev_ema26, next_dea: self.prev_dea };
        }
        let ema12 = self.prev_ema12.filter(|p| p.is_finite()).map(|p| 2.0/13.0*close + 11.0/13.0*p).unwrap_or(close);
        let ema26 = self.prev_ema26.filter(|p| p.is_finite()).map(|p| 2.0/27.0*close + 25.0/27.0*p).unwrap_or(close);
        let dif = if ema12.is_finite() && ema26.is_finite() { ema12 - ema26 } else { f64::NAN };
        let dea = self.prev_dea.filter(|p| dif.is_finite() && p.is_finite()).map(|p| 2.0/10.0*dif + 8.0/10.0*p).unwrap_or(dif);
        let macd = if dif.is_finite() && dea.is_finite() { (dif - dea) * 2.0 } else { f64::NAN };
        MacdOutput { ema12, ema26, dif, dea, macd, next_ema12: Some(ema12), next_ema26: Some(ema26), next_dea: Some(dea) }
    }

    fn push_macd(&mut self, o: &MacdOutput) {
        self.ema12.push(o.ema12); self.ema26.push(o.ema26);
        self.dif.push(o.dif); self.dea.push(o.dea); self.macd.push(o.macd);
        self.prev_ema12 = o.next_ema12; self.prev_ema26 = o.next_ema26; self.prev_dea = o.next_dea;
    }

    // ── MACD cross ──
    fn compute_macd_cross(&self, current_macd: f64) -> (bool, bool) {
        match self.macd.last().copied() {
            Some(prev) if prev.is_finite() && current_macd.is_finite() =>
                (prev <= 0.0 && current_macd > 0.0, prev >= 0.0 && current_macd < 0.0),
            _ => (false, false),
        }
    }

    // ── MA crosses ──
    fn compute_ma_cross(&self, ma: &MaOutput) -> MaCrossOutput {
        let p5 = self.ma5.last().copied(); let p10 = self.ma10.last().copied();
        let p60 = self.ma60.last().copied(); let p120 = self.ma120.last().copied();
        let p250 = self.ma250.last().copied();
        let (ma5ma10_jc, ma5ma10_sc)   = cross2(p5, p10, ma.ma5, ma.ma10);
        let (ma5ma60_jc, ma5ma60_sc)   = cross2(p5, p60, ma.ma5, ma.ma60);
        let (ma5ma120_jc, ma5ma120_sc) = cross2(p5, p120, ma.ma5, ma.ma120);
        let (ma5ma250_jc, ma5ma250_sc) = cross2(p5, p250, ma.ma5, ma.ma250);
        MaCrossOutput { ma5ma10_jc, ma5ma10_sc, ma5ma60_jc, ma5ma60_sc, ma5ma120_jc, ma5ma120_sc, ma5ma250_jc, ma5ma250_sc }
    }

    fn push_ma_cross(&mut self, o: &MaCrossOutput) {
        self.ma5ma10_jc.push(o.ma5ma10_jc); self.ma5ma10_sc.push(o.ma5ma10_sc);
        self.ma5ma60_jc.push(o.ma5ma60_jc); self.ma5ma60_sc.push(o.ma5ma60_sc);
        self.ma5ma120_jc.push(o.ma5ma120_jc); self.ma5ma120_sc.push(o.ma5ma120_sc);
        self.ma5ma250_jc.push(o.ma5ma250_jc); self.ma5ma250_sc.push(o.ma5ma250_sc);
    }

    // ── 收盘价上穿 MA5&MA10（cl2b_pair cross_ma5ma10 入口）──
    // 前一根收盘同时低于 MA5/MA10，当前收盘同时站上两条均线。
    fn compute_cross_ma5ma10(&self, ma: &MaOutput) -> bool {
        let i = self.ind_idx;
        if i < 1 { return false; }
        let c  = self.data.closes[i];
        let pc = self.data.closes[i - 1];
        match (self.ma5.last().copied(), self.ma10.last().copied()) {
            (Some(p5), Some(p10))
                if c.is_finite() && pc.is_finite()
                    && p5.is_finite() && p10.is_finite()
                    && ma.ma5.is_finite() && ma.ma10.is_finite() =>
                c > ma.ma5 && c > ma.ma10 && pc < p5 && pc < p10,
            _ => false,
        }
    }

    // ── Pct change ──
    fn compute_pct_change(&self) -> f64 {
        if self.data.closes.is_empty() || self.ind_idx < 1 { return f64::NAN; }
        let curr = self.data.closes[self.ind_idx];
        let prev = self.data.closes[self.ind_idx - 1];
        if !curr.is_finite() || !prev.is_finite() || prev == 0.0 { f64::NAN } else { (curr - prev) / prev * 100.0 }
    }

    // ── Bollinger Bands ──
    fn compute_bollinger_bands(&self) -> BollingerOutput {
        const PERIOD: usize = 20; const MULT: f64 = 2.0;
        if self.data.closes.is_empty() { return BollingerOutput { upper: f64::NAN, middle: f64::NAN, lower: f64::NAN }; }
        let len = self.ind_idx + 1;
        let (upper, middle, lower) = if len >= PERIOD {
            let w = &self.data.closes[len - PERIOD..len];
            let mid = w.iter().sum::<f64>() / PERIOD as f64;
            if mid.is_finite() {
                let std = (w.iter().map(|p| (p - mid).powi(2)).sum::<f64>() / PERIOD as f64).sqrt();
                (mid + MULT * std, mid, mid - MULT * std)
            } else { (f64::NAN, f64::NAN, f64::NAN) }
        } else { (f64::NAN, f64::NAN, f64::NAN) };
        BollingerOutput { upper, middle, lower }
    }

    fn push_bollinger(&mut self, o: &BollingerOutput) {
        self.bb_upper.push(o.upper); self.bb_middle.push(o.middle); self.bb_lower.push(o.lower);
    }

    // ── Orchestrator ──
    /// Compute all technical indicators for the current row and push into accumulators.
    /// Each sub-step returns an immutable value so computation is testable in isolation.
    ///
    /// `recent_seg_n`：最近 N 个已结束段结束点 K 线 idx 的窗口大小（0 表示不输出 List 列）。
    pub fn indicators(&mut self, recent_seg_n: usize) -> &mut Self {
        let i = self.ind_idx;

        let ma    = self.compute_ma();
        let macd  = self.compute_macd();
        let (macd_jc, macd_sc) = self.compute_macd_cross(macd.macd);
        let cross = self.compute_ma_cross(&ma);
        let cross_ma5ma10 = self.compute_cross_ma5ma10(&ma);
        let pct   = self.compute_pct_change();
        let bb    = self.compute_bollinger_bands();

        self.push_ma(&ma);
        self.push_macd(&macd);
        self.macd_jc.push(macd_jc);
        self.macd_sc.push(macd_sc);
        self.push_ma_cross(&cross);
        self.cross_ma5ma10.push(cross_ma5ma10);
        self.pct_change.push(pct);
        self.push_bollinger(&bb);

        // ── 段状态机增量更新 ──
        if recent_seg_n > 0 {
            self.update_seg_state_ma(i, recent_seg_n);
            self.update_seg_state_macd(i, recent_seg_n);
        }

        self
    }

    /// Pre-fill segment vectors to the full data length.
    /// Call once before starting the indicators loop.
    pub fn init_segments(&mut self) {
        let n = self.data.len();
        self.ma_segment = vec![None; n];
        self.macd_segment = vec![None; n];
        self.ma_recent_seg_bars = vec![Vec::new(); n];
        self.macd_recent_seg_bars = vec![Vec::new(); n];
    }

    /// 逐 bar O(1) 状态机更新 MA5MA10 交叉段
    fn update_seg_state_ma(&mut self, i: usize, recent_seg_n: usize) {
        let is_jc = self.ma5ma10_jc.get(i).copied().unwrap_or(false);
        let is_sc = self.ma5ma10_sc.get(i).copied().unwrap_or(false);

        let lo = self.data.lows.get(i).copied().unwrap_or(f64::INFINITY);
        let hi = self.data.highs.get(i).copied().unwrap_or(f64::NEG_INFINITY);
        if lo < self.seg_ma_running_low {
            self.seg_ma_running_low = lo;
            self.seg_ma_running_low_idx = i as i64;
        }
        if hi > self.seg_ma_running_high {
            self.seg_ma_running_high = hi;
            self.seg_ma_running_high_idx = i as i64;
        }

        if is_jc && self.seg_ma_prev_cross == Some("sc") {
            // 下跌段：start = 上一段 end（极值 idx），end = 段内 low_idx
            // 首段 prev_seg_end=-1 → start.max(0) 自然退化为 0
            let seg = SegData::compute(
                self.seg_ma_prev_idx,
                i as i64,
                self.seg_ma_prev_seg_end,
                self.seg_ma_running_low_idx,
                self.seg_ma_running_high,
                self.seg_ma_running_low,
                &self.data.closes,
                &self.pct_change,
                "down",
            );
            self.ma_segment[i] = Some(seg);
            push_recent(&mut self.seg_ma_buf, i as i64, recent_seg_n);
        } else if is_sc && self.seg_ma_prev_cross == Some("jc") {
            // 上涨段：start = 上一段 end（极值 idx），end = 段内 high_idx
            let seg = SegData::compute(
                self.seg_ma_prev_idx,
                i as i64,
                self.seg_ma_prev_seg_end,
                self.seg_ma_running_high_idx,
                self.seg_ma_running_high,
                self.seg_ma_running_low,
                &self.data.closes,
                &self.pct_change,
                "up",
            );
            self.ma_segment[i] = Some(seg);
            push_recent(&mut self.seg_ma_buf, i as i64, recent_seg_n);
        }
        self.ma_recent_seg_bars[i] = self.seg_ma_buf.iter().copied().collect();

        if is_jc || is_sc {
            self.seg_ma_prev_cross = Some(if is_jc { "jc" } else { "sc" });
            self.seg_ma_prev_idx = i as i64;
            // 记录本段 end：jc 触发的下跌段 end=low_idx，sc 触发的上涨段 end=high_idx
            self.seg_ma_prev_seg_end = if is_jc {
                self.seg_ma_running_low_idx
            } else {
                self.seg_ma_running_high_idx
            };
            self.seg_ma_running_low_idx = i as i64;
            self.seg_ma_running_low = lo;
            self.seg_ma_running_high_idx = i as i64;
            self.seg_ma_running_high = hi;
        }
    }

    /// 逐 bar O(1) 状态机更新 MACD 交叉段
    fn update_seg_state_macd(&mut self, i: usize, recent_seg_n: usize) {
        let is_jc = self.macd_jc.get(i).copied().unwrap_or(false);
        let is_sc = self.macd_sc.get(i).copied().unwrap_or(false);

        let lo = self.data.lows.get(i).copied().unwrap_or(f64::INFINITY);
        let hi = self.data.highs.get(i).copied().unwrap_or(f64::NEG_INFINITY);
        if lo < self.seg_macd_running_low {
            self.seg_macd_running_low = lo;
            self.seg_macd_running_low_idx = i as i64;
        }
        if hi > self.seg_macd_running_high {
            self.seg_macd_running_high = hi;
            self.seg_macd_running_high_idx = i as i64;
        }

        if is_jc && self.seg_macd_prev_cross == Some("sc") {
            // 下跌段：start = 上一段 end（极值 idx），end = 段内 low_idx
            let seg = SegData::compute(
                self.seg_macd_prev_idx,
                i as i64,
                self.seg_macd_prev_seg_end,
                self.seg_macd_running_low_idx,
                self.seg_macd_running_high,
                self.seg_macd_running_low,
                &self.data.closes,
                &self.pct_change,
                "down",
            );
            self.macd_segment[i] = Some(seg);
            push_recent(&mut self.seg_macd_buf, i as i64, recent_seg_n);
        } else if is_sc && self.seg_macd_prev_cross == Some("jc") {
            // 上涨段：start = 上一段 end（极值 idx），end = 段内 high_idx
            let seg = SegData::compute(
                self.seg_macd_prev_idx,
                i as i64,
                self.seg_macd_prev_seg_end,
                self.seg_macd_running_high_idx,
                self.seg_macd_running_high,
                self.seg_macd_running_low,
                &self.data.closes,
                &self.pct_change,
                "up",
            );
            self.macd_segment[i] = Some(seg);
            push_recent(&mut self.seg_macd_buf, i as i64, recent_seg_n);
        }
        self.macd_recent_seg_bars[i] = self.seg_macd_buf.iter().copied().collect();

        if is_jc || is_sc {
            self.seg_macd_prev_cross = Some(if is_jc { "jc" } else { "sc" });
            self.seg_macd_prev_idx = i as i64;
            // 记录本段 end：jc 触发的下跌段 end=low_idx，sc 触发的上涨段 end=high_idx
            self.seg_macd_prev_seg_end = if is_jc {
                self.seg_macd_running_low_idx
            } else {
                self.seg_macd_running_high_idx
            };
            self.seg_macd_running_low_idx = i as i64;
            self.seg_macd_running_low = lo;
            self.seg_macd_running_high_idx = i as i64;
            self.seg_macd_running_high = hi;
        }
    }

    /// Aggregate kline data based on frequency
    /// For 1w (weekly) and 1M (monthly) frequencies, combine daily data
    pub fn aggregate(self, target_freq: &str) -> Self {
        // Only aggregate for weekly (1w) and monthly (1M) frequencies
        if target_freq != "1w" && target_freq != "1M" {
            return self;
        }

        // Create a new processor for aggregated data
        let mut agg_processor = KlineProcessor::new(
            &self.data.exchange,
            &self.data.symbol,
            target_freq,
            self.factors.clone(),
        );

        // Process data for aggregation
        for i in 0..self.data.len() {
            // Check if current and previous elements are in the same period
            let should_merge = if i > 0 {
                self.in_same_period(i - 1, i, target_freq)
            } else {
                false
            };

            if should_merge {
                // Aggregate with previous data
                let last_idx = agg_processor.data.len() - 1;
                agg_processor.data.highs[last_idx] =
                    agg_processor.data.highs[last_idx].max(self.data.highs[i]);
                agg_processor.data.lows[last_idx] =
                    agg_processor.data.lows[last_idx].min(self.data.lows[i]);
                agg_processor.data.closes[last_idx] = self.data.closes[i];
                agg_processor.data.vols[last_idx] += self.data.vols[i];
                agg_processor.data.qvs[last_idx] += self.data.qvs[i];
                agg_processor.data.end_ts[last_idx] = self.data.end_ts[i];
            } else {
                agg_processor.data.push_from(&self.data, i);
            }
        }

        agg_processor
    }

    /// Check if two data points are in the same aggregation period
    fn in_same_period(&self, prev_idx: usize, curr_idx: usize, freq: &str) -> bool {
        let prev_ts = self.data.timestamps[prev_idx];
        let curr_ts = self.data.timestamps[curr_idx];

        match freq {
            "1w" => {
                // Check if both timestamps are in the same week (Monday to Sunday)
                self.get_week_start(prev_ts) == self.get_week_start(curr_ts)
            }
            "1M" => {
                // Check if both timestamps are in the same month
                self.get_month_number(prev_ts) == self.get_month_number(curr_ts)
            }
            _ => false,
        }
    }

    /// Get the start of the week (Monday) for a given timestamp
    fn get_week_start(&self, timestamp: i64) -> i64 {
        // Convert timestamp to seconds if it's in milliseconds
        let ts_seconds = if timestamp > 1_000_000_000_000 {
            timestamp / 1000
        } else {
            timestamp
        };

        // Convert to DateTime<Utc>
        if let Some(dt) = Utc.timestamp_opt(ts_seconds, 0).single() {
            // Get the Monday of the week (weekday 1 = Monday)
            let weekday = dt.weekday();
            let days_since_monday = match weekday {
                Weekday::Mon => 0,
                Weekday::Tue => 1,
                Weekday::Wed => 2,
                Weekday::Thu => 3,
                Weekday::Fri => 4,
                Weekday::Sat => 5,
                Weekday::Sun => 6,
            };
            let start_of_week = dt - chrono::Duration::days(days_since_monday);
            start_of_week.timestamp()
        } else {
            // Fallback to simple calculation if conversion fails
            // Monday is the start of the week
            let seconds_in_day = 86400;
            let days_since_epoch = ts_seconds / seconds_in_day;
            let day_of_week = (days_since_epoch + 3) % 7; // +3 because 1970-01-01 was Thursday (day 3)
            let monday_offset = if day_of_week == 0 { 6 } else { day_of_week - 1 };
            (days_since_epoch - monday_offset) * seconds_in_day
        }
    }

    /// Get month number from timestamp (year * 12 + month)
    fn get_month_number(&self, timestamp: i64) -> i64 {
        // Convert timestamp to seconds if it's in milliseconds
        let ts_seconds = if timestamp > 1_000_000_000_000 {
            timestamp / 1000
        } else {
            timestamp
        };

        // Convert to DateTime<Utc>
        if let Some(dt) = Utc.timestamp_opt(ts_seconds, 0).single() {
            // Return year * 12 + month (0-based)
            dt.year() as i64 * 12 + dt.month0() as i64
        } else {
            // Fallback to simple calculation if conversion fails
            // Approximate calculation: assume 30.44 days per month on average
            ts_seconds / (30 * 86400)
        }
    }
}

// ── Immutable indicator compute output types ──

pub(crate) struct MaOutput {
    ma5: f64, ma10: f64, ma20: f64, ma60: f64, ma120: f64, ma250: f64,
    ma5ma10_diff: f64, vol5: f64, vol10: f64,
}

pub(crate) struct MacdOutput {
    ema12: f64, ema26: f64, dif: f64, dea: f64, macd: f64,
    next_ema12: Option<f64>, next_ema26: Option<f64>, next_dea: Option<f64>,
}

pub(crate) struct MaCrossOutput {
    ma5ma10_jc: bool, ma5ma10_sc: bool,
    ma5ma60_jc: bool, ma5ma60_sc: bool,
    ma5ma120_jc: bool, ma5ma120_sc: bool,
    ma5ma250_jc: bool, ma5ma250_sc: bool,
}

pub(crate) struct BollingerOutput {
    upper: f64, middle: f64, lower: f64,
}

fn cross2(prev_a: Option<f64>, prev_b: Option<f64>, curr_a: f64, curr_b: f64) -> (bool, bool) {
    match (prev_a, prev_b) {
        (Some(pa), Some(pb)) if pa.is_finite() && pb.is_finite() && curr_a.is_finite() && curr_b.is_finite() =>
            (pa <= pb && curr_a > curr_b, pa >= pb && curr_a < curr_b),
        _ => (false, false),
    }
}

/// 把 idx 推入 ring buffer，超出 n 时弹出最旧。
fn push_recent(buf: &mut std::collections::VecDeque<i64>, idx: i64, n: usize) {
    if n == 0 {
        return;
    }
    buf.push_back(idx);
    while buf.len() > n {
        buf.pop_front();
    }
}

/// Segment 行存储布局常量
/// ─────────────────────────────────────────────────────────
/// 11 个字段按 little-endian 拼接，direction 用 i32 收敛到 4 字节，
/// 末尾 4 字节 padding 对齐到 8 字节，总计 88 字节：
///
///   off  field          type   size
///   ──   ─────          ────   ────
///    0   start          i64     8
///    8   end            i64     8
///   16   high           f64     8
///   24   low            f64     8
///   32   jc_idx         i64     8
///   40   sc_idx         i64     8
///   48   close_min      f64     8
///   56   close_max      f64     8
///   64   close_min_idx  i64     8
///   72   close_max_idx  i64     8
///   80   direction      i32     4   (-1 = down, 1 = up)
///   84   _padding       u32     4   (0)
///   88   pct_max        f64     8   ([start,end] 含端点)
///   96   pct_min        f64     8   ([start,end] 含端点)
///   ──                   total 104（前 88 字节布局不变，老客户端按 88 读前缀即可）
pub(crate) const SEG_BYTES: i32 = 104;

const SEG_OFF_DIRECTION: usize = 80;
const DIR_DOWN: i32 = -1;
const DIR_UP: i32 = 1;

/// 把一个 SegData 编码为 88 字节的定长 buffer（行存储）。
fn encode_seg(seg: &SegData, buf: &mut [u8]) {
    debug_assert_eq!(buf.len(), SEG_BYTES as usize);
    buf.fill(0);
    buf[0..8].copy_from_slice(&seg.start.to_le_bytes());
    buf[8..16].copy_from_slice(&seg.end.to_le_bytes());
    buf[16..24].copy_from_slice(&seg.high.to_le_bytes());
    buf[24..32].copy_from_slice(&seg.low.to_le_bytes());
    buf[32..40].copy_from_slice(&seg.jc_idx.to_le_bytes());
    buf[40..48].copy_from_slice(&seg.sc_idx.to_le_bytes());
    buf[48..56].copy_from_slice(&seg.close_min.to_le_bytes());
    buf[56..64].copy_from_slice(&seg.close_max.to_le_bytes());
    buf[64..72].copy_from_slice(&seg.close_min_idx.to_le_bytes());
    buf[72..80].copy_from_slice(&seg.close_max_idx.to_le_bytes());
    let dir: i32 = if seg.direction == "up" { DIR_UP } else { DIR_DOWN };
    // up=1, down=-1（sign 直接表达多空方向）
    buf[SEG_OFF_DIRECTION..SEG_OFF_DIRECTION + 4].copy_from_slice(&dir.to_le_bytes());
    buf[88..96].copy_from_slice(&seg.pct_max.to_le_bytes());
    buf[96..104].copy_from_slice(&seg.pct_min.to_le_bytes());
}

/// 构造 FixedSizeBinary(SEG_BYTES) 列：非交叉 bar 存 NULL，交叉 bar 存 88 字节编码。
fn build_seg_binary_array(segs: &[Option<SegData>], ts_len: usize) -> ArrayRef {
    // 1. 收集每个 bar 的 bytes：None → 全 0 placeholder（不会被读到，因为下方用 null buffer 屏蔽）；
    //    Some(s) → 真实编码。
    let mut values: Vec<u8> = vec![0u8; ts_len * SEG_BYTES as usize];
    let mut present: Vec<bool> = Vec::with_capacity(ts_len);
    let mut scratch = [0u8; SEG_BYTES as usize];
    for i in 0..ts_len {
        match segs.get(i).and_then(|s| s.as_ref()) {
            Some(s) => {
                encode_seg(s, &mut scratch);
                values[i * SEG_BYTES as usize..(i + 1) * SEG_BYTES as usize]
                    .copy_from_slice(&scratch);
                present.push(true);
            }
            None => {
                present.push(false);
            }
        }
    }
    let nulls = arrow::buffer::NullBuffer::from(present);
    let array = FixedSizeBinaryArray::try_new(
        SEG_BYTES,
        arrow::buffer::Buffer::from_vec(values),
        Some(nulls),
    )
    .expect("seg fixed size binary array");
    Arc::new(array)
}

/// 构造 List<Int64> 列，每行一个变长数组。
fn build_recent_seg_list(bars: &[Vec<i64>], ts_len: usize) -> ArrayRef {
    let mut values: Vec<i64> = Vec::new();
    let mut offsets: Vec<i32> = Vec::with_capacity(ts_len + 1);
    offsets.push(0);
    let mut null_bitmap: Vec<bool> = Vec::with_capacity(ts_len);
    for i in 0..ts_len {
        let row = bars.get(i).cloned().unwrap_or_default();
        // 全 0 长 list 当作 NULL 也没问题——下游可通过 length 0 判定
        if row.is_empty() {
            null_bitmap.push(false);
        } else {
            values.extend(row.iter().copied());
            null_bitmap.push(true);
        }
        offsets.push(values.len() as i32);
    }
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    let values_array: ArrayRef = Arc::new(Int64Array::from(values));
    let offsets_buffer = arrow::buffer::OffsetBuffer::<i32>::new(
        arrow::buffer::ScalarBuffer::from(offsets),
    );
    let nulls = arrow::buffer::NullBuffer::from(null_bitmap);
    Arc::new(
        ListArray::try_new(field, offsets_buffer, values_array, Some(nulls))
            .expect("recent seg list array"),
    )
}

#[cfg(test)]
mod seg_binary_layout_tests {
    use super::*;

    /// 字节布局固定：88 字节基础字段 + 16 字节扩展（pct_max/pct_min）= 104。
    /// 前 88 字节布局不变，下游按 88 读前缀的老客户端不受影响。
    #[test]
    fn seg_bytes_constant_is_104() {
        assert_eq!(SEG_BYTES, 104);
    }

    /// 已知输入 → 字节序列固定（little-endian）。验证与 Python 端
    /// `struct.Struct("<qq d d qq d d qq i 4x")` 拿到的字节完全一致。
    #[test]
    fn encode_seg_matches_python_struct_layout() {
        let seg = SegData {
            start: 0x0000_0000_0000_0001_i64,
            end: 0x0000_0000_0000_0002_i64,
            high: 3.5,
            low: 2.5,
            jc_idx: 0x0000_0000_0000_0003_i64,
            sc_idx: 0x0000_0000_0000_0004_i64,
            close_min: 1.0,
            close_max: 5.0,
            close_min_idx: 0x0000_0000_0000_0005_i64,
            close_max_idx: 0x0000_0000_0000_0006_i64,
            direction: "up",
            pct_max: 7.5,
            pct_min: -1.5,
        };
        let mut buf = [0u8; SEG_BYTES as usize];
        encode_seg(&seg, &mut buf);
        assert_eq!(buf.len(), 104);

        // 用同一个 struct format 在 Rust 端手动解一遍，验证对齐。
        // <qq d d qq d d qq i 4x> 与 Python 一致。
        let fmt = "<qq d d qq d d qq i 4x dd";
        assert_eq!(std::mem::size_of_val(&buf), std::mem::size_of::<u8>() * 104);

        // 手算 expected bytes
        let mut expected = [0u8; 104];
        expected[0..8].copy_from_slice(&1_i64.to_le_bytes());
        expected[8..16].copy_from_slice(&2_i64.to_le_bytes());
        expected[16..24].copy_from_slice(&3.5_f64.to_le_bytes());
        expected[24..32].copy_from_slice(&2.5_f64.to_le_bytes());
        expected[32..40].copy_from_slice(&3_i64.to_le_bytes());
        expected[40..48].copy_from_slice(&4_i64.to_le_bytes());
        expected[48..56].copy_from_slice(&1.0_f64.to_le_bytes());
        expected[56..64].copy_from_slice(&5.0_f64.to_le_bytes());
        expected[64..72].copy_from_slice(&5_i64.to_le_bytes());
        expected[72..80].copy_from_slice(&6_i64.to_le_bytes());
        expected[80..84].copy_from_slice(&1_i32.to_le_bytes()); // up
        // 84..88 留 0（padding）
        expected[88..96].copy_from_slice(&7.5_f64.to_le_bytes());
        expected[96..104].copy_from_slice(&(-1.5_f64).to_le_bytes());
        assert_eq!(buf, expected);

        // 避免 unused 警告：fmt 在文档注释里被引用
        let _ = fmt;
    }

    #[test]
    fn seg_close_min_scans_until_sc_idx() {
        // 下跌段：start=0, end=2（段内价格极点），但 sc_idx=5 才是下一段金叉。
        // close_min / close_max / pct_max / pct_min 均应覆盖完整段移动 [start, sc_idx)，
        // 而不是仅 [start, end]。
        let closes = vec![10.0, 9.0, 8.0, 7.0, 6.0, 11.0];
        let pct_change = vec![0.0, 0.0, 0.0, 5.0, -3.0, 0.0];
        let seg = SegData::compute(
            2, 5, 0, 2,
            10.0, 6.0,
            &closes, &pct_change, "down",
        );
        assert_eq!(seg.close_min, 6.0);
        assert_eq!(seg.close_min_idx, 4);
        assert_eq!(seg.close_max, 10.0);
        assert_eq!(seg.close_max_idx, 0);
        // pct 也应按 [start, sc_idx) = [0, 5) 计算，覆盖 5.0 与 -3.0
        assert_eq!(seg.pct_max, 5.0);
        assert_eq!(seg.pct_min, -3.0);
    }

    #[test]
    fn direction_enum_encoding() {
        // down=-1, up=1（sign 直接表达多空方向）
        let mut buf = [0u8; SEG_BYTES as usize];
        let seg_up = SegData {
            start: 0, end: 0, high: 0.0, low: 0.0,
            jc_idx: 0, sc_idx: 0,
            close_min: 0.0, close_max: 0.0,
            close_min_idx: 0, close_max_idx: 0,
            direction: "up",
            pct_max: 0.0, pct_min: 0.0,
        };
        encode_seg(&seg_up, &mut buf);
        assert_eq!(&buf[80..84], &1_i32.to_le_bytes());

        let seg_down = SegData { direction: "down", ..seg_up };
        encode_seg(&seg_down, &mut buf);
        assert_eq!(&buf[80..84], &(-1_i32).to_le_bytes());
    }
}
