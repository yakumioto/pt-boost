# 部署指南（Docker）

承接 [design-overview.md](design-overview.md) 的"部署形态"：pt-boost 是单一 Go 二进制、
Docker 镜像常驻部署；torrentfs-go 是独立部署的另一个服务，pt-boost 只持有它的
`base_url` + 凭证，不托管其进程、不内嵌其代码、不读取或校验它自己的部署配置
（ADR 0001）。本文档只讲 pt-boost 自己这一侧怎么打包、怎么跑。

## 连接已有 torrentfs-go（本地部署）

仓库的 `compose.yaml` 只运行 pt-boost，不会启动或修改 torrentfs-go 的服务配置。

1. 创建被 git 忽略的 `config.local.yaml`，连接已有 torrentfs-go；M-Team 的 `base_url` 必须为 `https://api.m-team.cc`（不要追加 `/api`）。
2. 在被 git 忽略的 `.env` 提供 `MTEAM_TOKEN` 与 `TORRENTFS_PASSWORD`，在配置中用 `${NAME}` 引用。torrentfs 的 `username` 当前仅为兼容配置保留，登录实际只发密码。
3. 配置文件不含明文密钥，可执行 `chmod 644 config.local.yaml` 使镜像内的非 root 用户能够读取。
4. 执行：

```sh
docker compose up -d --build
curl http://127.0.0.1:8081/status
docker compose logs --tail 30
docker compose stop
```

状态接口只绑定宿主机 `127.0.0.1:8081`。容器使用只读根文件系统、禁用提权并移除全部 capabilities；日志按 10MB × 3 轮转。

首次扫描会下载 `.torrent` 并触发配置的主动取片；冷种检测成熟后会删除本池分类下的冷种。不要把已有片库使用的分类配置为热种池分类。未归属本池的已有种子会跳过。

## 永久淘汰与候选人数限制

配置顶层 `state_dir: /data`。候选必须是 **free**（`require_free` 默认 `true`，仅 `discount=FREE` 且促销未过期者入选；促销时间无法解析时按非 free 跳过，不会消耗下载配额）。搜索按 **R18 板块优先**：`mode=all` 在生产环境不含 R18，因此先查 `mode=adult` 再查 `mode=normal`，R18 候选排在前面，非 R18 仍可作为补充。

人数条件：`seeders > seeders_min`、`leechers > leechers_min`；可选上限 `seeders_max`（严格小于，`0` 或省略表示不限制）。当前本地部署已按要求设为 `seeders_max: 0`。人数条件只筛选新增候选，不会立即删除池内已有成员。

`compose.yaml` 将命名卷 `pt-boost-state` 挂载到 `/data`，可在只读根文件系统下由非 root 用户写入。永久淘汰记录和待删除队列按站点名称、分类隔离；写入并同步到磁盘后才请求删除。异步删除成功才确认出池，失败/超时保留队列，启动及扫描时重试，不依赖有新候选。重启与重建容器不会清空名单。热种池及上传采样仍从 torrentfs-go 重建，不保存在本地。

**不要执行 `docker compose down -v`，不要删除数据卷或淘汰记录。** 名单没有到期时间和自动解除机制；丢失数据卷无法保证历史种子不再入池。需要备份 `/data`；修改站点名称或分类会切换名单命名空间。非 Docker 部署默认使用当前工作目录的 `data/`，必须放在稳定、可写的持久路径。

## 冷种上传速率阈值

每站点可配置 `cold_rate_threshold`，省略时默认 **`100KiB/s`（102400 B/s）**，必须为正值。保持原有 `cold_window: 20m`：完整窗口平均上传速率严格低于阈值才判冷，等于阈值不判冷。支持覆盖如 `80KiB/s`、`200KiB/s`；`KB/s` 是十进制，`KiB/s` 是二进制，二者不同。配置更改后需要重启服务，采样窗口会重新积累。

## 周期缓存补片

本地 `target_share_bytes: 128MiB` 用于新种初始取片。每次 2 分钟上传采样后，对本池成员查询 `/status` 的当前驻留缓存（不是累计下载量）与 `/piece-download` 任务状态：严格低于 64 MiB 才尝试补到 128 MiB，只请求缺失 pieces，完整 piece 对齐可能略超目标，小种子取自身大小。

- 已有 `running` 任务不重置；补片按每站点 2 个在途任务预算调度，已有初始取片也占预算。扫描批量落地可能暂时超过预算，此时不再新开补片。
- 外部分类、永久淘汰及待删除成员不补；状态查询失败时本轮不发新任务，避免未知状态下超额并发。
- 失败从 2 分钟指数退避，最多 30 分钟；成功后至少间隔 2 分钟。请求超时也保守占用本轮名额，下一轮重新查状态。
- 补片不重置上传观察窗口，也不解除冷种或永久淘汰判定。日志 `cache refill requested` 代表发起请求，非缓存已补满；`cache maintenance completed` 报告本轮启动数。
- 这不是缓存锁定或带宽硬限制，在线视频仍可挤占共享 LRU。10 个种子的目标合计约 1.25 GiB；持续竞争时会发生重下，需结合日志评估。

## 构建镜像

```sh
docker build -t pt-boost:latest .
```

`Dockerfile` 是两段式构建：`golang:1.26-alpine` 里静态编译
（`CGO_ENABLED=0`），产物只有一个二进制；运行阶段用
`gcr.io/distroless/static-debian12:nonroot`，没有 shell、没有包管理器，
镶带的只有 CA 证书（M-Team 走 HTTPS 需要）和时区数据。镜像体积约
15-20MB，不含构建工具链残留，也绝不会含有 Chromium / 无头浏览器相关的任何
东西（ADR 0002：M-Team 适配器只走标准 HTTP 直连）。

## 运行容器

pt-boost 从不内置默认配置——每个站点的 `api_key`/`category`/`pool_size`
等都是部署相关的，必须自己提供一份 YAML（格式见
[design-overview.md](design-overview.md) 的配置示例）挂载到容器内固定路径
`/etc/pt-boost/config.yaml`：

```sh
docker run -d --name pt-boost \
  -v $(pwd)/config.yaml:/etc/pt-boost/config.yaml:ro \
  -p 8081:8081 \
  pt-boost:latest
```

- 容器启动即是常驻进程（内部调度 goroutine 常驻），不会跑完一轮扫描就退出。
- `-p 8081:8081` 对外暴露 `GET /status`（监听地址可通过配置的
  `status.listen_addr` 覆盖，默认 `:8081`）。
- 配置文件里引用的 `${NAME}` 占位符（例如 `api_key: ${MTEAM_TOKEN}`）会在
  读取配置时从进程环境变量展开（见 `internal/config` 的
  `expandEnv`）——不要把密钥直接写进 YAML 本身，用 `-e MTEAM_TOKEN=...`
  或 `--env-file .env` 注入。
- 镜像的 `ENTRYPOINT` 用 exec form 直接把 `/pt-boost` 作为容器 PID 1
  运行，中间没有 shell。这保证 `docker stop` 发出的 `SIGTERM` 能直达
  `main.go` 的 `signal.NotifyContext`，触发优雅关闭（取消所有站点的扫描
  /冷种检测循环 + 给 `/status` 服务器一个 5 秒的关闭窗口），不需要等到
  Docker 默认的 10 秒优雅期超时后发 `SIGKILL`。

## docker-compose 示例

[`docker-compose.example.yml`](../docker-compose.example.yml)
演示了 pt-boost 容器 + 一个独立的 torrentfs-go 容器一起跑、pt-boost 通过
容器网络访问 torrentfs-go 的 `base_url` 这一网络拓扑。**torrentfs-go 这个
服务是占位符**：torrentfs-go 本身没有发布过官方镜像（只有 nightly 源码），
真实部署时请自己从 <https://github.com/yakumioto/torrentfs-go> 构建/运行它，
pt-boost 这边只需要知道它的 `base_url` + 用户名密码。

用法：

```sh
cp docker-compose.example.yml docker-compose.yml
# 编辑 ./config.yaml（torrentfs.base_url 指向你的 torrentfs-go 服务）
# 编辑 ./.env 提供 config.yaml 里 ${NAME} 占位符引用的密钥
docker compose up -d
```

## 已验证的行为（手动烟雾测试，非自动化测试）

用真实 `docker build` + `docker run`（network host 模式 + 一个模拟
torrentfs-go/M-Team 的本地 fake server）验证过：

- 镜像构建成功，`docker images` 显示约 16.7MB（无构建工具链残留，无
  Chromium）。
- 容器启动瞬间 `GET /status` 返回 `{"last_scan_at":null,"pool":[]}`
  （池还未重建完成，不报错不挂起）；扫描落地候选后正确展示池内容。
- `docker stop` 在约 1 秒内完成（远小于 Docker 默认 10 秒优雅期），容器
  `ExitCode=0`，日志干净打印 `"pt-boost stopped"`，无需 `docker kill`。
