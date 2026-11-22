# HydroOJ 出站隧道（outbound tunnel）

**一句话：校内 HydroOJ 一个入站端口都不开，只由 OJ 端主动向外挂请求，校外学生就能用公网域名完整访问 OJ（登录、提交、判题回显、静态资源全走这条隧道）。**

学校禁止内网穿透（frp / cloudflared 等）时适用。从防火墙看，OJ 机器上跑的 agent 和「一台电脑在浏览网页」没区别：三段连接全是**出向**的。

```
 学生浏览器                 公网中转                    学校内网
┌──────────┐   HTTPS 出向   ┌──────────────┐   长挂起   ┌──────────┐  本地 HTTP  ┌──────────┐
│ 刷题页面  │ ────────────▶ │  Node 中转    │ ◀──────── │  agent   │ ──────────▶ │ HydroOJ  │
│          │ ◀──────────── │ server.js    │ ─────────▶ │ 常驻进程 │ ◀────────── │ :8888    │
└──────────┘   响应/推送    └──────────────┘  请求批量   └──────────┘   回传       └──────────┘
                                  ▲
                                  │ 学生访问 https://<公网域名>/?sharecode=<分享码>
```

几条关键事实：

- **不需要改 Hydro，也不需要装插件**；agent 只是把公网请求原样转发给 `127.0.0.1:<Hydro端口>`。
- **不需要学生装任何东西**；学生拿到的就是一个普通网址。
- 中转端已实测：并发 5 / 20 / 60 全部 100% 成功；agent 断开后 0.2～0.4 秒返回 503，不让学生干等。
- 已知限制：`/socket.io/` 走 HTTP polling 降级（评测状态慢几秒）；若 Hydro 前套 nginx，必须确认 nginx 信任并透传 `X-Forwarded-For`，否则全班会被 Hydro 判成同一 IP。

---

## 一、学校机器一键部署

在 **HydroOJ 所在 Linux 机器**上，用 root 执行下面一条命令（把尖括号换成你的值）：

```bash
curl -fsSL --max-time 20 https://cdn.jsdelivr.net/gh/sanhaikeji/hydro-outbound-tunnel@main/install-agent.sh \
  | sudo bash -s -- 8888 '<TOKEN>' '<SHARECODE>'
```

- `8888` 换成 Hydro 实际监听端口。
- `<TOKEN>` / `<SHARECODE>` 是部署凭据，**不写在本仓库里**，见你本地的 `LOCAL.md`（已被 `.gitignore` 忽略，不会上传）。
- 如果 jsDelivr 访问不了，把 URL 换成 `https://raw.githubusercontent.com/sanhaikeji/hydro-outbound-tunnel/main/install-agent.sh`。
- 也可以先把脚本下载下来再跑：`curl -fsSL <上面的URL> -o install-agent.sh && sudo bash install-agent.sh 8888 <TOKEN> <SHARECODE>`。

脚本会自动：检查 Python ≥3.8 → 写 `/opt/tunnel-agent/{agent.py,config.json}` → 检查本地 Hydro 端口 → 注册 systemd 开机自启 → 等握手成功 → 从公网自检一次 → 打印学生访问地址。

**日常只需这四个命令：**

```bash
sudo bash install-agent.sh status      # 看运行状态和最近日志
sudo bash install-agent.sh logs        # 实时日志
sudo bash install-agent.sh uninstall   # 卸载
sudo bash install-agent.sh 8080 <TOKEN> <SHARECODE>   # 换端口重装
```

也可以用环境变量代替后两个参数：

```bash
export TUNNEL_TOKEN='<TOKEN>' TUNNEL_SHARECODE='<SHARECODE>'
sudo -E bash install-agent.sh 8888
```

---

## 二、安全说明（重要）

- `TOKEN` 是 agent 与中转之间的认证密钥。**公开仓库里绝不能写死它**，否则任何人都能注册一个假 agent，把学生引导到仿冒页面。
- `SHARECODE` 是云沙箱分享码，拿到它就能访问你的 OJ。它本来就在发给学生的网址里，但仍不建议提交到仓库。
- 本仓库的 `install-agent.sh` 只接收参数/环境变量；`tunnel-relay/config.example.json` 是脱敏模板；真实值放在被忽略的 `LOCAL.md`。
- 学校机器上如果担心命令历史留下密钥，用环境变量方式，并在装完后 `unset TUNNEL_TOKEN TUNNEL_SHARECODE`。

---

## 三、文件结构

| 文件 | 作用 |
|---|---|
| `install-agent.sh` | **给学校机器用的一键安装脚本**，单文件、内嵌 agent、零 pip 依赖 |
| `tunnel-agent/agent.py` | agent 源码（脚本内嵌的是同一份逻辑），便于审计与二次开发 |
| `tunnel-agent/tunnel-agent.service` | systemd unit 模板 |
| `tunnel-relay/server.js` | 中转服务源码，Node ≥18，零 npm 依赖 |
| `tunnel-relay/config.example.json` | 中转配置模板（脱敏） |
| `docs/PROTOCOL.md` | 协议、部署、云端实测数据与排障 |
| `LOCAL.md` | 本机真实凭据（**.gitignore 忽略，不上传**） |

---

## 四、自己搭 VPS 中转（可选）

云沙箱适合验证，长期运行建议自己的 VPS。代码不用改：

1. VPS 上装 Node ≥18，`node server.js` 启动（配置见 `config.example.json`）。
2. 前面套 nginx 终止 TLS，域名指向 VPS。
3. 装 agent 时把 `RELAY_URL` 指到你的域名、`SHARECODE` 留空：

```bash
export TUNNEL_RELAY_URL='https://oj.example.com' TUNNEL_TOKEN='<你的token>'
sudo -E bash install-agent.sh 8888
```

注意：自建时不需要 `sharecode`，脚本会生成空值，agent 就不会再拼 `?sharecode=`。

---

## 五、发版 / 更新

按维护者约定：**对外发布、推送、覆盖线上之前，先跟负责人确认**。本地改完、验证通过就停下来汇报，等点头再推。

学校机器更新脚本：重新执行第一节那条命令即可（会覆盖 `/opt/tunnel-agent/agent.py` 并重启服务，不影响 Hydro）。

---

## 六、许可

MIT，见 [LICENSE](LICENSE)。
