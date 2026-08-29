<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-29 | Updated: 2026-06-29 -->

# qdata - K 线数据服务

## Purpose
**高性能 K 线数据服务**。基于 Rust 实现，通过 gRPC Arrow Flight 协议 (:50001) 提供行情 K 线数据。是 quant-lab 和 atlas 的核心数据来源之一。

## 服务架构

```
TDengine / PostgreSQL (K线原始数据)
        │
        ▼
┌─────────────────────────┐
│  qdata (Rust)      │
│  gRPC Arrow Flight :50001│
│                          │
│  高性能 K 线数据查询      │
│  支持多周期/多标的        │
│  Arrow Flight 列式传输    │
└─────────┬───────────────┘
          │
          ▼
  quant-lab ← 选股引擎需要使用 K 线
  atlas/flow ← K 线代理 pkg/klineproxy/
  signalview ← K 线展示
```

## 关键文件

| 文件 | 描述 |
|------|------|
| `src/` | Rust 源码目录 |
| `Cargo.toml` | Rust 依赖配置 |
| `.github/workflows/` | CI/CD |

## 依赖关系

### 被依赖
- `quant-lab` — 主要调用者（通过 gRPC Flight 获取 K 线）
- `atlas/pkg/klineproxy/` — Go 端的 K 线代理层
- `signalview` — K 线展示

### 依赖
- `ops/dep/` — TDengine / PostgreSQL 数据存储

## 问题排查指引

| 问题 | 排查方向 |
|------|----------|
| K 线数据查询失败 | 检查 qdata 是否运行 (:50001) |
| 数据返回慢 | TDengine 查询性能 / 网络延迟 |
| 某只股票没数据 | 检查该股票在 TDengine/PostgreSQL 中是否有记录 |
| 数据不准确 | 上游数据源 → atlas/consumer K 线入库逻辑 |

## For AI Agents

### Working In This Directory
- Rust 项目，使用 `cargo build` 构建
- gRPC Arrow Flight 协议，跨语言调用方便
- 通常作为容器运行 (`ops/app/compose.yaml`)

<!-- MANUAL: -->
