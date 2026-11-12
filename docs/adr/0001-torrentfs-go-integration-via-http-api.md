# pt-boost 通过 torrentfs-go 的 HTTP RESTful API 集成，而非作为 Go library 内嵌

**Status**: accepted

pt-boost 和 torrentfs-go 都是 Go 项目，直觉上会倾向于把 torrentfs-go 作为 Go module 直接 `import` 进 pt-boost 同一进程运行。经确认（m00027），实际选择是：torrentfs-go 作为独立部署、独立生命周期的守护进程运行，pt-boost 仅通过其 HTTP RESTful 接口（增删种子、读取统计、设置分类标签等）与之交互。

这样选择是因为 torrentfs-go 本身设计为一个带 Web 管理界面、FUSE 挂载、Docker/SMB 访问的独立服务，而不是一个"无状态函数库"；把它当独立服务对待，可以独立升级/重启/部署 torrentfs-go，不与 pt-boost 的发布节奏耦合，也不需要把 FUSE 挂载、BT 引擎这些重量级能力编译进 pt-boost 二进制。代价是 pt-boost 必须自行处理网络调用的重试、超时、torrentfs-go 不可用时的降级逻辑，且两者数据模型需要通过 REST API 的 DTO 对齐，而不是共享 Go 类型。

**事后验证（子调研，2026-11-12）**：这不仅是偏好选择，而是硬性技术约束——torrentfs-go 代码库（commit `5936439`）完全没有 `pkg/` 或任何根级可导出包，全部业务逻辑在 `internal/session`、`internal/cache`、`internal/piecestore` 等 `internal/` 包下，Go 语言规则下无法被仓库之外的 module `import`。`cmd/torrentfs/main.go` 是唯一入口，只支持"编译为独立二进制 + TOML 配置文件 + HTTP API"的运行形态。因此"作为 Go library 内嵌"这条路径在没有 fork 并迁移内部包之前根本不可行，HTTP RESTful 集成是当前唯一选项。
