# 部署指南（Docker）

承接 [design-overview.md](design-overview.md) 的"部署形态"：pt-boost 是单一 Go 二进制、
Docker 镜像常驻部署；torrentfs-go 是独立部署的另一个服务，pt-boost 只持有它的
`base_url` + 凭证，不托管其进程、不内嵌其代码、不读取或校验它自己的部署配置
（ADR 0001）。本文档只讲 pt-boost 自己这一侧怎么打包、怎么跑。

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
