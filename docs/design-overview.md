# pt-boost 设计总览

> 本文档是 2026-11-12 需求访谈（grilling）达成共识后的架构快照，供后续实现阶段直接参照。术语定义见 [../GLOSSARY.md](../GLOSSARY.md)，关键决策理由见 [adr/](./adr/)。

## 范围边界

- pt-boost 是一个**纯 HTTP 客户端 + 调度/策略引擎**：不托管 torrentfs-go 进程、不内嵌其代码、不读取/校验/依赖 torrentfs-go 的部署配置（`[cache].capacity`、`backend` 模式、`[experiments.upload].mode` 等一律视为运维方自行决定的黑盒，pt-boost 只依赖其公开 REST API 的行为契约）。
- 第一版支持单站点适配器：M-Team。站点适配通过通用 `Site` 接口扩展，不改动核心调度逻辑。
- M-Team 适配器只走标准 `net/http` 直连，不引入 Chromium/无头浏览器依赖（ADR 0002）。
- torrentfs-go 侧集成方式是 HTTP RESTful 调用（ADR 0001），因为它没有可导出的 Go library API。

## 配置结构（YAML，示意）

```yaml
torrentfs:
  base_url: http://torrentfs:8080
  username: ...
  password: ...

sites:
  - name: mteam
    api_key: ...
    base_url: https://api.m-team.cc/api   # 或 test2.m-team.cc 测试环境
    category: boost-mteam                  # torrentfs-go 分类标签，即池成员凭证
    pool_size: 10                          # 默认 10，可覆盖
    seeders_min: 10
    leechers_min: 50
    scan_interval: 10m
    max_scan_pages: 3
    page_size: 100
    request_interval: 3s                   # 单次请求间隔，留安全余量
    target_share_bytes: 256MiB             # 主动取片的目标份额，pt-boost 自己的策略常量
    cold_window: 20m
    cold_rate_threshold: 50KB/s
```

## 端到端工作流程

### 1. 扫描循环（每个 site 一个 goroutine，周期 = `scan_interval`）

1. 调用 M-Team `POST /api/torrent/search`，`sortField=LEECHERS, sortDirection=DESC`，分页拉取（`page_size` 条/页，最多 `max_scan_pages` 页，页间隔 ≥ `request_interval`）。
2. 客户端过滤：`seeders > seeders_min && leechers > leechers_min`（API 不支持区间参数，必须本地过滤）。
3. 跳过已在池中的候选（与当前池成员的 infohash 比对，池成员来自第 4 步的分类标签重建）。
4. 若池未满（当前成员数 < `pool_size`）：直接按顺序加入候选。
5. 若池已满：触发第 2 步"冷种检测"的最新结果，若存在冷种，用排名最高的候选顶替冷种中最差的一个；否则本轮不做替换。

### 2. 冷种检测（独立轮询，周期建议 2 分钟，覆盖 `cold_window=20m` 窗口）

1. 调用 torrentfs-go `GET /api/v1/torrents`，按 `category == site.category` 过滤，得到当前池成员及其 `UploadedBytes` 累计值。
2. 为每个成员维护进程内存滚动窗口（`{timestamp, UploadedBytes}` 采样序列，只保留最近 `cold_window` 内的样本）。
3. 速率 = 窗口内首尾样本的 `Δbytes / Δt`；若样本覆盖满 `cold_window` 且速率持续 < `cold_rate_threshold`，标记为冷种。
4. 新加入不足 `cold_window` 的成员不参与冷种判定（避免误判刚加入的种子）。

### 3. 新种子落地（选中一个候选后）

1. 调用 M-Team `POST /api/torrent/genDlToken`（**必须 form-encoded**，不能 JSON）拿下载链接。
2. pt-boost 下载 `.torrent` 文件原始字节。
3. 调用 torrentfs-go `POST /api/v1/torrents`（multipart 上传该文件）加入种子，拿到 torrentfs-go 内部 torrent id。
4. 调用 torrentfs-go `PUT /api/v1/torrents/{id}/category`，打上该 site 专属标签（如 `boost-mteam`）——这是池成员资格的唯一凭证，也是进程重启后重建池状态的依据。
5. 调用 torrentfs-go `PUT /api/v1/torrents/{id}/piece-download`，范围 = 从文件头开始、覆盖 `target_share_bytes`（文件小于此值则覆盖全部），一次性触发主动取片，**不经过任何 FUSE 文件读取**。
6. 之后不再对该种子做主动 piece 管理，后续缓存去留交给 torrentfs-go 自身的全局 LRU 淘汰（v1 简化策略，更复杂的"稀有 piece 优先"留给 v2）。

### 4. 淘汰流程（冷种被顶替时）

1. 调用 torrentfs-go `DELETE /api/v1/torrents/{id}`（异步，返回 202 + `operationResponse`，可选轮询 `GET /api/v1/operations/{id}` 确认完成）。
2. 从内存滚动窗口中移除该种子的采样历史。

### 5. 进程重启恢复

- **不引入本地持久化存储**（无 SQLite/BoltDB）。池成员列表 = `GET /api/v1/torrents` 按分类标签过滤的结果，天然持久化在 torrentfs-go 侧。
- 内存滚动窗口清空重新积累，`cold_window`（20 分钟）内自然恢复冷种判定能力，重启代价可接受。

### 6. 认证

- torrentfs-go：`POST /api/v1/auth/login`（用户名密码）换 Bearer token，按 `token_ttl` 滑动过期前刷新。
- M-Team：`x-api-key` header，直连，不经浏览器中转（ADR 0002）。

### 7. 可观测性（v1）

- 结构化日志（JSON/文本可切换）。
- 简单状态查询 HTTP 端点（如 `GET /status`），输出各 site 当前池成员、最近扫描时间、冷种判定详情。
- Prometheus `/metrics` 留作后续迭代，v1 不做。

### 8. 明确排除的"反作弊红线"

- 不使用、不依赖 torrentfs-go 的 `[experiments.upload].mode = inject/amplify`（伪造上传数字），只接受真实 BT 协议层的上传统计；但该配置项本身如何设置，是 torrentfs-go 部署方的职责，不由 pt-boost 校验或强制。

## 部署形态

- pt-boost 编译为单一 Go 二进制，Docker 镜像部署，常驻运行，内部调度 goroutine 常驻（不是 cron 式"跑一轮就退出"）。
- torrentfs-go 是独立部署的另一个服务/容器，pt-boost 只持有其 base_url + 凭证。
