# M-Team 适配器只走标准 HTTP 直连，禁止引入 Chromium/无头浏览器依赖

**Status**: accepted

PT-Forward 的 M-Team API 指南提到生产域名 `api.m-team.io`/`api.m-team.cc` 在部分 IP 段会被 nginx 拦截（302→Google），官方参考实现用 rod/Playwright 起一个无头 Chromium 做请求中转来绕过这个风控。用户在 m00044 明确要求：**pt-boost 的 M-Team 适配器只允许标准 HTTP 直连，不允许引入 Chromium 等浏览器依赖**，即使这意味着在某些网络环境下直连可能被拦截。

这是刻意偏离参考实现推荐做法的决定，必须记录，避免后续维护者看到直连在某些环境下失败、照着 PT-Forward 文档"修复"成引入 Chromium。原因：pt-boost 最终以 Docker 形态部署（m00044），引入 Chromium 会大幅增加镜像体积、内存占用和攻击面，且用户判断自己的部署网络环境不需要绕过这层风控。如果未来直连确实在用户的网络环境下被拦截，需要先排查域名选择（`test2.m-team.cc` 测试环境 / `api.m-team.cc` / `api.m-team.io`）、UA、请求头是否贴合文档建议，而不是默认引入浏览器中转。
