# data-service
supported freqs: 1d,2h,1h,30m,15m,5m

## 本地运行（推荐）

镜像构建与运行时都基于 `tdengine/tsdb`，libtaos 由官方安装，无需拷贝和补丁：

```bash
docker build -t dataservice:dev .
docker run --rm --network host --user 0 \
    --entrypoint /app/data-service \
    -e TDENGINE_HOST=localhost:6030 \
    -e TDENGINE_DB=kline \
    dataservice:dev
```

`tdengine/tsdb` 的 ENTRYPOINT 默认是 `taosd`，上面用 `--entrypoint` 覆盖为我们编译出的二进制，并用 `--user 0` 绕过 taosd 启动时对 `/var/log/taos` 的写权限检查（dataservice 本身不需要那些路径）。生产部署建议同镜像族跑 taosd 与 dataservice 各一个容器，版本完全对齐（`TAOS_VERSION`）。

## 本地开发（无 Docker）

需要：Rust 1.96 toolchain、本机已运行 TDengine（端口 6030）。dataservice 通过 dlopen 加载 `libtaos.so`，所以本机也需要装一份（deb 包或从镜像拷出）。

```bash
TAOS_VERSION=3.4.1.6

# 1. 提取 libtaos 与客户端配置（rootless 环境）
mkdir -p ~/.local/lib/taos ~/.local/taos/cfg ~/.local/taos/log ~/.local/taos/data
docker create --name tdengine-tmp docker.io/tdengine/tsdb:${TAOS_VERSION} echo
docker cp tdengine-tmp:/usr/local/lib/libtaos.so*        ~/.local/lib/taos/
docker cp tdengine-tmp:/usr/local/lib/libtaosnative.so*  ~/.local/lib/taos/
docker cp tdengine-tmp:/etc/taos/taos.cfg                ~/.local/taos/cfg/
docker rm tdengine-tmp

# 2. 让 dataservice 在 rootless 环境能找到 libtaos 和配置/日志目录
cargo run
```

环境变量（默认值见 `src/main.rs`）：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `TDENGINE_HOST` | `localhost:6030` | 原生 TCP 端口（切换 ws feature 时改 6041） |
| `TDENGINE_USER` | `root` | |
| `TDENGINE_PASS` | `taosdata` | |
| `TDENGINE_DB` | `kline` | 启动后 use_database |
| `FLIGHT_ADDR` | `0.0.0.0:50001` | Arrow Flight 监听 |
| `REDIS_URL` | `redis://127.0.0.1/` | 可选，snapshot 缓存 |
| `TAOS_CFG_DIR` | `/etc/taos` | rootless 跑时改为 `~/.local/taos/cfg` |
| `TAOS_LOG_DIR` | `/var/log/taos` | rootless 跑时改为 `~/.local/taos/log` |
| `TAOS_DATA_DIR` | `/var/lib/taos` | rootless 跑时改为 `~/.local/taos/data` |

> **关于 dlopen 与 PT_GNU_STACK**：libtaos 的 ELF 头里 `PT_GNU_STACK` 标志为 `RWE`，部分现代内核（Fedora、Arch 等默认开启严格 mprotect）会拒绝 dlopen。Debian/Ubuntu 默认内核放行，因此**本机是 Debian 系时不需任何修复**。如果遇到 `cannot enable executable stack as shared object requires`，把 `LD_LIBRARY_PATH` 指向 taos 官方 .deb 安装目录而非手动拷出的副本，或直接用 docker 镜像路径。