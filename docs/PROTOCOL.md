# HydroOJ 出站隧道 —— 协议与部署

学校禁止内网穿透，OJ 只能主动向外发请求。本方案让公网中转 + OJ 端常驻 agent 组成一条
**纯出站**的 HTTP 隧道：学生访问中转域名，中转把请求塞进挂起的 pull 响应里，agent 转发到
本地 HydroOJ，再把响应回传。

```
学生浏览器 --HTTPS--> 中转 (公网)  <--长轮询 pull--  agent (校内, 只出站) --> 127.0.0.1:8888
```

HUSTOJ 换成 HydroOJ 后，以下三点是硬约束，实现里已处理：

| 项 | 为什么必须 | 做法 |
|---|---|---|
| `X-Forwarded-For` = 学生真实 IP | Hydro 有「同 IP 多账号」风控，全走 127.0.0.1 会把全班判成一人 | 中转取自己观测到的 TCP 源地址放进 `ip` 字段；学生自己带来的 `XFF` 在两侧都被丢弃，防伪造 |
| `X-Forwarded-Proto: https` | 否则 Hydro 生成的绝对链接是 `http://`，在 HTTPS 页面里被当混合内容拦掉 | agent 固定写 `https`（可配 `forwarded_proto`） |
| `Host` = 公网域名 | Hydro 靠 `Host` / `X-Forwarded-Host` 生成绝对链接和判断 vhost | agent 把 `Host` 改写为 `public_host`（默认取 relay_url 的 host） |

## 通道模型

不是「每个并发请求占一个通道」，而是**单条 pull 流 + 批量下发**：

- agent 只维持 **1 条**挂起的 `GET /c/pull`，中转最多压 25 秒。
- 期间到达的请求，中转立刻攒成一批（≤64 条）塞进这条已挂起的响应返回。
- agent 拿到批后并发丢给本地工作池（默认 24 线程）去打 `127.0.0.1:8888`。
- 队列只在工作池全忙时才堆，起过载缓冲，不承担延迟。

这样通道数与并发数解耦，agent 重连不丢 slot 状态。

## 端点

| 端点 | 方向 | 说明 |
|---|---|---|
| `POST /c/hello` | agent → 中转 | 握手上报，返回中转侧 limits |
| `GET /c/pull?site=&wait=` | agent → 中转 | 挂起取批；空批 = 超时，agent 立即重挂 |
| `GET /c/body/<rid>` | agent → 中转 | 取大请求体（>256KB 不走 JSON 内联） |
| `POST /c/resp/<rid>` | agent → 中转 | 回传响应；元信息在 `X-Resp-Meta`（base64 JSON），body 走原始字节 |
| `ANY /*` | 学生 → 中转 | 入队等待，超时 504 / 队列满 503 |
| `GET /_stats` | 运维 | JSON 统计，需 `X-Tunnel-Token` |
| `GET /` | 学生 / 运维 | 正常代理到 Hydro；带 `X-Tunnel-Token` 且 agent 离线时才显示状态页 |

所有 `/c/*` 以及诊断接口都要求 `X-Tunnel-Token`，用 `timingSafeEqual` 比对。

## 关键行为

- **ack 确认（投递是否真到达）**：中转把一批交给 pull 响应后，agent 立刻 `POST /c/ack/<rids>` 回报。
  只有收到 ack 才算这批真到手。900ms（`ackTimeoutMs`）没 ack 的条目**放回队首重发**，不是直接判失败。
  这一步专门对付「写进了已被网关静默回收的死连接」——那种情况下 `res.end()` 不报错，
  但 agent 根本收不到，学生只能干等硬超时。
- **快速失败（四级）**：
  1. 请求进来时既没有挂起的 pull、最近 `agentSilenceMs=3000ms` 内也没有 pull/hello/ack 活动
     → 直接 503，几十毫秒返回，不进队列。
  2. 队列有货但没有活 pull：等 `noAgentGraceMs=500ms` 复查一次；若 agent 的 `lastPullAt`
     已冻结超过 `noAgentDeadMs=1500ms`，判定 agent 真没了 → 503。
     **两级判定是必须的**：agent 收到一批后要先 ack、再派发转发任务，这期间本来就没有 pull 挂着。
     早期用「此刻没有活 pull」一刀切判死，实测 60 并发只成 37/60 —— 正常在忙的 agent 被误杀了。
  3. 已投递但 `ackTimeoutMs=900ms` 没 ack → 放回队首，交给第 2 条的看门狗统一裁决。
     ack 超时回调里**绝不能** `failQueue` 整个队列：那会把别的批次里还在正常飞行的请求一起杀掉。
     「agent 是否死了」只能由一处判断。
  4. 兜底：早期版本有 `orphanGraceMs=4000ms` 的逐条孤儿超时，现已由上面几条覆盖，
     仅作最后防线保留。
- **投递前验活**：往一个已死的 socket 投递等于请求凭空蒸发。`flush` 前先 `waiterAlive` 检查，
  已死的 waiter 直接跳过，批次留给下一个 waiter。
- **实测失败耗时**（本地，agent 被 `TerminateProcess` 强杀、不给它发 FIN 的机会）：
  完全离线 0.002~0.07s；刚杀掉后的第 1 个请求约 1.5s（需等 ack 看门狗 + 复查），
  后续请求 0.5s。第一次慢是物理下限：进程被强杀后 TCP 连接不会立刻关闭，
  中转无法当场知道对端已死。把看门狗压到 500ms 以下会误杀真实 OJ 上的慢响应。
- **硬超时**：请求已交给 agent 但 20 秒没回传，返 504。
- **静态缓存**：`GET` 且 `Content-Type` 属于 css/js/图片/字体，或路径命中 `/static/ /components/ /upload/ /public/`，
  进内存 LRU（默认 200MB / 5 分钟）。命中直接返回，不消耗隧道。实测命中 0.23s vs 首次 0.36s。
- **请求体**：原始字节转发，不重新编码。>256KB 走 `/c/body/` 单独拉，避免 JSON base64 膨胀。
- **响应头**：去 hop-by-hop；`Set-Cookie` 多条保留（实测网关 `__wb_rsc` 与 Hydro `sid` 可共存）；
  `content-encoding` 与上游 CRC 冲突时清掉重算长度。
- **压缩**：中转对上游 `Accept-Encoding` 一律剥离，由中转→浏览器这一跳决定压缩，避免双层解压错乱。

## 部署

### 中转（公网，Node ≥18，零依赖）

```bash
cd tunnel-relay
# 1. 生成 token
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
# 2. 写进 config.json 的 sites.<id>.token
node server.js
```

`config.json` 里 token 若是占位符（以 `REPLACE` / `CHANGE` / `your_` 开头），中转会给所有
代理请求返 503 并在首页提示，不会带着假配置跑。

### agent（HydroOJ 所在机器，Python ≥3.8，纯标准库）

```bash
sudo mkdir -p /opt/tunnel-agent
sudo cp agent.py config.json /opt/tunnel-agent/
sudo cp tunnel-agent.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now tunnel-agent
journalctl -u tunnel-agent -f
```

`config.json` 的 `token` 必须和中转一致；`local_port` 按你 Hydro 的实际端口填（默认 8888）。

### 部署在 WorkBuddy 云沙箱时（关键差异）

云沙箱的发布域名前面有一层**网关**，它会做三件影响实现的事，全部已在代码里处理：

1. **sharecode 逐请求校验** —— 不带就返 403「链接不完整」。
   - 浏览器：带 sharecode 访问一次后网关种下 `__wb_rsc` cookie（HttpOnly/Secure/SameSite=None，30 天），
     后续请求靠 cookie 即可，学生不用每次都带。
   - **agent 是程序调用、没有 cookie**，所以必须在每条隧道请求上都附加 `?sharecode=`。
     这就是 agent config 里 `sharecode` 字段存在的原因。**自行部署到 VPS 时该字段留空。**
2. **真实 IP 由网关提供** —— 网关转发时 socket 源地址是 `127.0.0.1`，但会写入：
   - `x-real-ip: <学生真实 IP>`（单值、最可信）
   - `x-forwarded-for: <真实IP>, 10.x.x.x, ::ffff:172.24.0.1`（后面几段是平台内网）
   中转优先取 `x-real-ip`。**已验证学生伪造 `X-Real-IP` 会被网关覆盖成真实出口 IP，无法绕过。**
3. **平台凭据必须剥离** —— 网关给每个请求塞 `Authorization: Bearer <space JWT>`。这是平台租户凭据，
   透到 OJ 端会进 Hydro 日志。中转按 JWT 内容识别并剥离，同时剥掉 `x-wb-*` / `x-space-*` /
   `traceparent` / `stgw-*` 等内部标识，但保留用户自己的 Basic / API token 认证。
   已实测验证：Hydro 端最终只收到 `host / xff / x-real-ip / x-forwarded-proto / x-forwarded-host /
   x-tunnel-via` 这几个业务头。


## 容量与延迟（实测）

### 本机环回（无真实公网 RTT）

| 并发 | 成功率 | p50 | p95 |
|---|---|---|---|
| 10 | 10/10 | 331ms | 347ms |
| 50 | 50/50 | 39ms | 539ms |
| 180 | 180/180 | 141ms | 191ms |

180 并发下直连模拟服务 p95 是 1052ms，经中转反而 191ms —— 因为批处理把突发摊平了。

### 部署到 WorkBuddy 云沙箱后（公网实测）

| 并发 | 成功率 | p50 | p95 |
|---|---|---|---|
| 5 | 5/5 | 749ms | 779ms |
| 20 | 20/20 | 572ms | 705ms |
| 60 | 60/60 | 385ms | 1244ms |

**超时率 0.00%**（91 次请求 0 次超时）。

延迟构成（本机到北京大兴沙箱实测）：TLS 握手 132ms + 隧道两次公网往返 ≈ 400ms。
这是架构固有成本：每个请求要 浏览器→网关→中转→agent→Hydro 再原路回来。
串行单请求约 200ms，并发时因连接复用摊薄。

## 云端长连接实测结论（最关键的一条）

**25 秒长挂起在云沙箱上存活，不会被掐断。** 实测 `curl` 挂满 25 秒返回 `200` 且 JSON 完整。
这是本方案能跑在云沙箱上的前提，已验证。

## 常见故障

| 现象 | 原因 | 处理 |
|---|---|---|
| 首页一直显示状态页 | `token` 是占位符，或 agent 从未连上 | 改 config 重启两端 |
| 全站 503 但 `_stats`（带 token 访问）里 `responses` 在涨 | agent 在跑但本地 Hydro 没起 | 查 `local_port`、`journalctl -u hydro` |
| 登录后跳回 `http://` | `forwarded_proto` 没生效 | 确认 agent config 里是 `https` |
| 图片 403 / Referer 校验失败 | Hydro 的 CSRF/Referer 白名单还写着旧域名 | 在 Hydro 配置里加中转域名 |
| 全班被判同一 IP | `XFF` 没透传 | 确认中转没被前置 CDN 覆盖掉真实源 IP；必要时改 `trust proxy` 层数 |

## 未解决 / 需你确认

1. **WebSocket（`/socket.io/`）目前未走隧道。** Hydro 会自动降级到 HTTP polling，功能正常，
   只是评测状态慢几秒、且每次 polling 占用一次隧道请求。要给 WS 提速，需要在 pull 协议上
   再加一层字节流通道 —— 这是第二阶段，本次未做。
2. **Hydro 的 `X-Forwarded-For` 信任链**：Hydro 读 `app.proxy` 与 HttpServer 配置。若你前面
   还套了 nginx，要保证 agent 打进去的 `XFF` 不会被 nginx 覆盖成 `127.0.0.1`。上线前必须核实
   一次 —— 这个错了就是全班同 IP。
3. **沙箱生命周期未知。** 云沙箱不是长期托管环境，可能被回收/休眠。中转重启会丢内存队列（可接受，
   agent 会自动重连），但如果沙箱整机被回收、域名失效，那就得换 VPS。**生产用建议 VPS**，
   代码零改动。
4. **沙箱无数据库/无持久化。** 中转状态全在进程内存，重启即清空统计与缓存。这是设计选择，
   不是缺陷 —— 但别指望它存历史。

## 诊断端点（已收紧，2026-11-22）

| 端点 | 用途 | 鉴权 |
|---|---|---|
| `GET /_stats` | 各项计数（请求/响应/超时/拒绝/缓存命中/字节数） | **必须**带 `x-tunnel-token: <agent token>`，否则 404 |
| `GET /_hdr` | **已删除**，恒返回 404 | — |
| `GET /` | 状态页，仅 agent 离线或 token 未配置时显示 | **必须**带 `x-tunnel-token`，否则走正常代理转发给 Hydro |

`/_hdr` 原来的设计是把网关这一跳看到的头部原样回显，用来确认真实 IP。但云沙箱网关会给
每个请求塞 `Authorization: Bearer <space JWT>` 和 `Cookie`，原样回显等于把平台凭据送给
任何打开这个路径的学生。转发给 Hydro 的路径本来就会剥掉这类头（`filterReqHeaders`），
诊断路径却漏了 —— 已彻底删除，不再提供任何回显原始请求头的公网接口。

**注意 `sharecode` 不能当鉴权**：学生手里也有它，挡不住任何人。诊断接口只认 agent token。

调用示例：

```bash
curl -s -H 'x-tunnel-token: <agent token>' https://<中转域名>/_stats
```

## 缓存边界（已收紧，2026-11-22）

缓存命中后请求**不再进 Hydro**，因此也绕过了 Hydro 的登录检查 —— 白名单必须从严：

- 只缓存路径以 `/static/`、`/components/`、`/public/` 开头，且响应 `Content-Type`
  是明确静态类型（css/js/图片/字体/wasm）的 200 响应。
- **不缓存** `/upload/`、`/d/`、附件、任意带扩展名的地址（旧版本按「任意扩展名」缓存，
  会把前一个用户的私有附件直接发给后一个人）。
- **不缓存**带 `Cookie` / `Authorization` 的请求，也不缓存带 `Set-Cookie`、
  `Vary: Cookie`、`Cache-Control: private/no-store` 的响应。
- 带 `Range` 的请求一律不缓存。

