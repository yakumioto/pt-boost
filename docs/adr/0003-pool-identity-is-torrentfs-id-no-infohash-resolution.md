# 热种池的身份就是 torrentfs-go 的 torrent id，pt-boost 不解析 infohash

**Status**: accepted

最初的设计让站点适配器在搜索阶段为每个候选解析 infohash（M-Team 走 `POST /api/torrent/detail`），Scheduler 用它判断"候选是否已在池中"。对生产环境 `api.m-team.cc` 实测后这条路走不通：`/api/torrent/detail` 是详情页展示接口，返回的 `infoHash` 字段恒为 `null`（抽样 8 个种子 8/8 为 null）。

决定：**pt-boost 不获取、不计算候选的 infohash**。`Candidate` 只带站点自己的 torrent id；候选的身份由 torrentfs-go 在添加 `.torrent` 时给出，热种池以 torrentfs-go 的 torrent id 为键。被否决的替代方案是在 pt-boost 里解析 bencode、对 `info` 字典做 SHA1：那需要在搜索阶段就为每个候选下载 `.torrent`，而 infohash 本来就是 torrentfs-go 维护的东西，不该在 pt-boost 再算一遍。

这依赖 torrentfs-go 的两个行为（核实于 torrentfs-go `main` @ `5936439`，`internal/session/manage.go` 的 `addManagedLocked` 与 `newRegistryEntry`）：

1. `POST /api/v1/torrents` **幂等**：重复添加已管理的种子返回成功和同一个 torrent，不报错（只有该种子正在删除时才返回 409）。
2. torrent **id 就是 infohash 的十六进制串**，同一内容永远得到同一个 id。

如果 torrentfs-go 改掉其中任何一条（比如重复添加改成报错，或 id 改成自增/随机），Scheduler 的判重会失效，需要回到这里重新决策。

## 后果

- **轮替顺序是"先添加，后淘汰"**：只有添加之后才知道候选是不是已在池中，所以必须先 add 拿到 id、确认不是重复，再淘汰冷种。反过来做会让一个重复候选白白挤掉一个冷种。添加成功但尚未打分类标签的种子不算池成员，所以池不会真正超容量。
- **别人的种子不碰**：幂等添加意味着候选可能解析到一个 torrentfs-go 早已管理的种子——用户自己的片库（通常没有分类标签），或另一个 site 的池成员。每轮首次落地前 Scheduler 先取一次种子列表；候选解析到的 id 若在本轮之前就已存在且不是本池成员，一律跳过：不打标签、不触发取片、不为它淘汰、不删除。否则它会被纳入池，之后作为冷种被删除。副作用：某次落地在"已添加、未打标签"之间失败留下的无标签种子不会被自动认领，需要人工处理。
- **淘汰失败要回滚**：新种子先于冷种淘汰被添加；若淘汰失败，Scheduler 把刚添加的种子删掉，避免留下无分类标签的孤儿。走到这一步的种子必然是本轮新建的（见上一条），删除是安全的。
- **不维护重启前的状态**：进程内缓存 `站点 torrent id → torrentfs-go id` 让稳态扫描不再重复下载已入池种子的 `.torrent`。它不持久化；重启后首轮扫描里，每个仍然达标的旧成员会多付一次"下载 `.torrent` + 幂等添加"，随后命中池成员被跳过。这是刻意接受的代价。
- 池已满且没有冷种时，本轮不为任何候选下载 `.torrent`。
