# PT Boost

通用 PT 站"寄生做种/刷上传"服务：周期性从 PT 站点 API 发现有上传缺口的种子，驱动 torrentfs-go 按需缓存并提供 piece 读取，借助真实 BT 协议向其他 peer 产生上传流量。首个支持的站点是 M-Team。

## Language

**热种 (Hot Torrent)**:
站点搜索结果中满足人数条件（做种人数(seeders) 高于下限、可选地低于上限，且 下载人数(leechers) 高于下限）且当前为**免费种**的种子，即已有一定做种基数但下载需求更大、存在明显上传缺口，适合被 pt-boost 寄生做种。
_Avoid_: 热门种子、高热度种子（未限定具体阈值字段时不要用"热种"）

**热种池 (Hot Torrent Pool)**:
每个站点配置并发维持的热种集合，容量有上限（默认 10，可按站点覆盖配置）。新候选发现时按滚动替换策略顶替池内的冷种。
_Avoid_: 种子队列、下载队列

**冷种 (Cold Torrent)**:
热种池内某个种子在连续观察窗口（默认 20 分钟）内平均上传速率低于可配置阈值（默认 < 100KiB/s），被判定为已失去上传价值，优先被新候选顶替出池。
_Avoid_: 过期种子、死种

**免费种 (Free Torrent)**:
M-Team `status.discount` 为 `FREE` 且促销尚未到期（`discountEndTime` 为空表示长期有效）的种子，下载不消耗账号下载量。只有免费种才会被选入热种池。
_Avoid_: 优惠种、打折种（`PERCENT_50` 等部分优惠不算免费种）

**R18 板块 (R18 Section)**:
M-Team 搜索 `mode=adult` 对应的成人内容板块，与 `mode=normal` 互斥；生产环境 `mode=all` 不包含该板块。热种候选优先取自该板块，不足时才补充普通板块。
_Avoid_: 成人分类（容易与站点内容分类号混淆）

**永久淘汰 (Permanent Retirement)**:
因失去上传价值而退出热种池后，不再被该站点自动选入热种池的状态；不会随时间流逝或服务重启解除。
_Avoid_: 冷却期、临时禁入（永久淘汰没有自动到期时间）

**做种人数 (Seeders) / 下载人数 (Leechers)**:
M-Team `/torrent/search` 返回的种子热度字段，只能作为排序字段(`sortField=SEEDERS|LEECHERS`)使用，站点 API 不提供区间过滤，区间过滤（seeders/leechers 阈值）必须在 pt-boost 客户端完成。
_Avoid_: 笼统的"上传量"/"下载量"（必须明确是人数字段还是字节量字段）

**站点适配器 (Site Adapter)**:
pt-boost 核心调度逻辑依赖的通用接口（搜索候选热种、获取下载令牌、查询种子详情等），每个 PT 站点提供一份实现；新增站点只需实现该接口，不改动核心调度。
_Avoid_: 站点插件（未正式定为插件化架构前不用"插件"）

**本地分类标签 (Local Category Tag)**:
pt-boost 通过 torrentfs-go 的 RESTful 接口（`PUT /api/v1/torrents/{id}/category`）给自己管理的任务打的分类标签（例如 `boost-mteam`），只存在于 torrentfs-go/pt-boost 本地范围内，不回写、不影响 PT 站点侧的内容分类体系。该标签同时是 pt-boost 重启后"重建某个站点热种池成员"的唯一依据：按分类值过滤 `GET /api/v1/torrents` 的结果即可恢复池状态，因此多站点场景下每个站点必须使用不同的分类值。
_Avoid_: 站点分类、种子分类（这两个词专指 PT 站点 `/torrent/categoryList` 的内容类型，如电影/剧集，与本地标签是两套不同体系）

**全局 Piece 缓存 (Global Piece Cache)**:
torrentfs-go 会话级的单一共享内存 LRU 缓存（`[cache].capacity` 配置，双水位淘汰：高水位 7/8 容量触发批量淘汰至低水位 3/4 容量），所有种子共享同一容量池，没有任何"每个种子独立缓存上限"的机制；这份缓存同时服务本地读取与向其他 peer 的上传(serve)请求，二者共享同一份数据。pt-boost 原始设想的"每个种子最多缓存 256M"必须换算成"全局容量 = 池容量 × 单种子目标份额"（如 10 × 256MiB = 2560MiB），作为软性目标而非硬性隔离。
_Avoid_: per-torrent 缓存、独立缓存（torrentfs-go 不支持这个概念，不要在设计中假设存在）

**连接配额 (Connection Quota)**:
torrentfs-go 里真正按种子独立配置的维度（`[connections].reading_conns`/`uploading_conns`），而不是缓存容量；用来限制某个种子在"正被读取"/"仅做种"两种状态下允许的 peer 连接数上限，彼此没有跨种子的全局预算。
_Avoid_: 与"全局 Piece 缓存"混淆——两者是不同的资源维度

**主动取片 (Piece Download Trigger)**:
torrentfs-go 提供的 `PUT /api/v1/torrents/{id}/piece-download` 接口，可以不经过 FUSE 文件读取、直接触发某个种子某个 range 的 piece 主动下载进全局缓存。这是 pt-boost 驱动"刷上传"的核心机制：把热种的 piece 主动拉进缓存后，该缓存条目即可被其他 peer 的下载请求命中、产生真实上传流量，全程不需要挂载 FUSE 或发起任何文件读取。
_Avoid_: 下载种子、做种（笼统说法容易让人联想到"下载整个文件"或"传统客户端做种"，这里特指按 range 的主动缓存填充）
