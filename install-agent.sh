#!/usr/bin/env bash
# HydroOJ 出站隧道 —— OJ 机器一键安装脚本（单文件 / 零外部依赖）
#
# 用法（在 HydroOJ 所在 Linux 机器上，用 root 执行）：
#   bash install-agent.sh 8888 <TOKEN> <SHARECODE>   # 安装（端口按实际 Hydro 改）
#   bash install-agent.sh status                     # 看运行状态
#   bash install-agent.sh logs                       # 看实时日志
#   bash install-agent.sh uninstall                  # 卸载
#
# TOKEN / SHARECODE 也可走环境变量 TUNNEL_TOKEN / TUNNEL_SHARECODE。
# 公开仓库里不写死密钥；学校里可 curl | bash 带参安装，见 README.md。

set -euo pipefail

RELAY_URL="${TUNNEL_RELAY_URL:-https://hydro-tunnel.app.workbuddy.host}"
DIR="/opt/tunnel-agent"
SVC="tunnel-agent"
PY=""

say() { printf '%s\n' "$*"; }
die() { printf '错误: %s\n' "$*" >&2; exit 1; }

CMD="${1:-}"
LOCAL_PORT="8888"
TOKEN="${TUNNEL_TOKEN:-}"
SHARECODE="${TUNNEL_SHARECODE:-}"
if [ -z "$CMD" ] || [[ "$CMD" =~ ^[0-9]+$ ]]; then
  CMD=install
  [ -z "${1:-}" ] || LOCAL_PORT="$1"
  [ -z "${2:-}" ] || TOKEN="$2"
  [ -z "${3:-}" ] || SHARECODE="$3"
fi
if [ "$CMD" = install ]; then
  [[ "$LOCAL_PORT" =~ ^[0-9]+$ ]] || die "端口必须是数字，例如：bash $0 8080 <TOKEN> <SHARECODE>"
  [ "${#TOKEN}" -ge 16 ] || die "缺少 TOKEN（至少 16 字符）：bash $0 $LOCAL_PORT <TOKEN> <SHARECODE>"
  [ -n "$SHARECODE" ] || die "缺少 SHARECODE：bash $0 $LOCAL_PORT <TOKEN> <SHARECODE>"
fi

find_python() {
  PY="$(command -v python3 || true)"
  [ -n "$PY" ] || die "找不到 python3，请先安装 Python 3.8 以上"
  "$PY" -c 'import sys; sys.exit(0 if sys.version_info >= (3,8) else 1)' \
    || die "Python 版本过低，需要 3.8 以上（当前 $("$PY" -V 2>&1)）"
}

write_files() {
  # 目录与密钥文件收紧：token 存在 config.json 里，644 会让同机其他账号读到，
  # 拿到 token 就能注册假 agent 把学生流量劫走。服务以 root 运行，600/750 不影响它读。
  install -d -m 750 "$DIR"
  chmod 750 "$DIR" 2>/dev/null || true

  cat > "$DIR/agent.py" <<'AGENT_PY'
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""OJ 端隧道 agent —— 全程只发出站请求，不接受任何入站连接。"""

import base64
import gzip
import http.client
import json
import os
import queue
import select
import socket
import ssl
import sys
import threading
import time
import traceback
from urllib.parse import quote, urlsplit

DEFAULTS = {
    "relay_url": "",
    "token": "",
    "site": "default",
    "sharecode": "",
    "local_host": "127.0.0.1",
    "local_port": 8888,
    "public_host": "",
    "forwarded_proto": "https",
    "workers": 24,
    "pull_wait": 25,
    "max_retry": 3,
    "verbose": False,
}

HOP_BY_HOP = {
    "connection", "keep-alive", "transfer-encoding", "upgrade", "te",
    "trailer", "content-length",
}
OVERRIDE = {"host", "x-forwarded-for", "x-forwarded-proto", "x-forwarded-host", "x-real-ip"}


def log(msg):
    sys.stderr.write("[agent] %s\n" % msg)
    sys.stderr.flush()


def dbg(msg):
    if CFG["verbose"]:
        log(msg)


def load_cfg():
    path = os.environ.get("TUNNEL_AGENT_CONFIG") or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "config.json"
    )
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        log("找不到配置文件: %s" % path)
        sys.exit(1)
    except Exception as e:
        log("配置文件解析失败: %s" % e)
        sys.exit(1)

    cfg = dict(DEFAULTS)
    cfg.update(raw)
    if not cfg["relay_url"]:
        log("config.json 缺少 relay_url")
        sys.exit(1)
    if len(str(cfg["token"])) < 16:
        log("config.json 的 token 太短（至少 16 字符）")
        sys.exit(1)

    u = urlsplit(cfg["relay_url"])
    cfg["_scheme"] = u.scheme or "https"
    cfg["_netloc"] = u.netloc
    cfg["_base"] = u.path.rstrip("/")
    if not cfg["public_host"]:
        cfg["public_host"] = u.netloc
    cfg["_sc"] = ("sharecode=" + quote(str(cfg["sharecode"]), safe="")) if cfg["sharecode"] else ""
    return cfg


def pack_body(payload):
    return gzip.compress(payload, 6)


def with_sc(cfg, path):
    if not cfg.get("_sc"):
        return path
    if "?" in path:
        return path + "&" + cfg["_sc"]
    return path + "?" + cfg["_sc"]


class ConnPool:
    """按需复用 http.client 连接，避免每次请求都握一次 TLS。"""

    def __init__(self, host, port, use_tls, timeout=60):
        self.host = host
        self.port = port
        self.use_tls = use_tls
        self.timeout = timeout
        self._free = queue.LifoQueue()
        self._lock = threading.Lock()
        self._made = 0

    def _new(self):
        if self.use_tls:
            ctx = ssl.create_default_context()
            c = http.client.HTTPSConnection(self.host, self.port, timeout=self.timeout, context=ctx)
        else:
            c = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        c.connect()
        with self._lock:
            self._made += 1
        return c

    def acquire(self):
        while True:
            try:
                c = self._free.get_nowait()
            except queue.Empty:
                return self._new()
            if self._is_usable(c):
                return c
            try:
                c.close()
            except Exception:
                pass

    @staticmethod
    def _is_usable(conn):
        sock = getattr(conn, "sock", None)
        if sock is None:
            return False
        try:
            if sock.fileno() < 0:
                return False
            r, _, _ = select.select([sock], [], [], 0)
        except (OSError, ValueError):
            return False
        if r:
            return False
        return True

    def release(self, conn, reusable=True):
        if not reusable:
            try:
                conn.close()
            except Exception:
                pass
            return
        if not self._is_usable(conn):
            try:
                conn.close()
            except Exception:
                pass
            return
        self._free.put(conn)

    def request(self, method, path, body=None, headers=None, timeout=None):
        attempts = 2
        for i in range(attempts):
            conn = self.acquire()
            reusable = True
            try:
                if timeout is not None:
                    conn.timeout = timeout
                conn.request(method, path, body=body, headers=headers or {})
                resp = conn.getresponse()
                data = resp.read()
                if resp.will_close:
                    reusable = False
                return resp.status, resp.reason, resp.getheaders(), data
            except (http.client.HTTPException, socket.error, OSError):
                reusable = False
                if i == attempts - 1:
                    raise
                continue
            finally:
                self.release(conn, reusable)


def build_local_headers(item, cfg):
    seen = {}
    for k, v in (item.get("headers") or {}).items():
        lk = k.lower()
        if lk in HOP_BY_HOP or lk in OVERRIDE:
            continue
        seen[lk] = v
    seen["host"] = cfg["public_host"]
    seen["x-forwarded-for"] = item.get("ip") or "0.0.0.0"
    seen["x-real-ip"] = item.get("ip") or "0.0.0.0"
    seen["x-forwarded-proto"] = cfg["forwarded_proto"]
    seen["x-forwarded-host"] = cfg["public_host"]
    seen["x-tunnel-via"] = "relay"
    return seen


def filter_resp_headers(headers):
    out = {}
    for k, v in headers:
        lk = k.lower()
        if lk in HOP_BY_HOP:
            continue
        if lk == "set-cookie":
            out.setdefault("set-cookie", [])
            out["set-cookie"].append(v)
            continue
        out[lk] = v
    return out


def fetch_ref_body(rid, cfg, pool):
    path = with_sc(cfg, "%s/c/body/%s" % (cfg["_base"], rid))
    st, _r, _h, data = pool.request("GET", path, headers={"x-tunnel-token": cfg["token"]})
    if st != 200:
        raise RuntimeError("body fetch failed: HTTP %s" % st)
    return data


def forward_one(item, cfg, resp_pool, ref_pool):
    rid = item["rid"]
    try:
        body = None
        if item.get("bodyRef"):
            body = fetch_ref_body(rid, cfg, ref_pool)
        elif "body" in item:
            body = base64.b64decode(item["body"])

        headers = build_local_headers(item, cfg)
        local = http.client.HTTPConnection(cfg["local_host"], cfg["local_port"], timeout=30)
        try:
            local.request(item["method"], item["path"], body=body, headers=headers)
            resp = local.getresponse()
            payload = resp.read()
            status = resp.status
            hdrs = filter_resp_headers(resp.getheaders())
            hdrs.pop("content-encoding", None)
            hdrs.pop("content-length", None)
        finally:
            try:
                local.close()
            except Exception:
                pass

        meta = base64.b64encode(
            json.dumps({"status": status, "headers": hdrs}, ensure_ascii=False).encode("utf-8")
        ).decode("ascii")
        if len(meta) > 6000:
            hdrs = {"content-type": "text/plain; charset=utf-8"}
            payload = b"tunnel: response headers too large"
            status = 502
            meta = base64.b64encode(
                json.dumps({"status": status, "headers": hdrs}).encode("utf-8")
            ).decode("ascii")

        path_resp = with_sc(cfg, "%s/c/resp/%s" % (cfg["_base"], rid))
        last = None
        for attempt in range(cfg["max_retry"]):
            try:
                st, _r, _h, data = resp_pool.request(
                    "POST", path_resp, body=pack_body(payload),
                    headers={
                        "x-tunnel-token": cfg["token"],
                        "x-resp-meta": meta,
                        "x-resp-encoding": "gzip",
                        "content-type": "application/octet-stream",
                    },
                )
                if st == 200:
                    dbg("resp ok rid=%s status=%s len=%d" % (rid, status, len(payload)))
                    return
                last = "HTTP %s %s" % (st, data[:120])
            except Exception as e:
                last = repr(e)
            time.sleep(0.2 * (attempt + 1))
        log("回传失败 rid=%s: %s" % (rid, last))
    except Exception:
        log("转发失败 rid=%s path=%s\n%s" % (rid, item.get("path"), traceback.format_exc()))
        try:
            meta = base64.b64encode(
                json.dumps({"status": 502, "headers": {"content-type": "text/plain; charset=utf-8"}}).encode("utf-8")
            ).decode("ascii")
            resp_pool.request(
                "POST", with_sc(cfg, "%s/c/resp/%s" % (cfg["_base"], rid)),
                body=pack_body("隧道转发失败：OJ 端无法连接本地服务。".encode("utf-8")),
                headers={"x-tunnel-token": cfg["token"], "x-resp-meta": meta,
                         "x-resp-encoding": "gzip", "content-type": "application/octet-stream"},
            )
        except Exception:
            pass


def hello(cfg, pool):
    path = with_sc(cfg, "%s/c/hello?site=%s" % (cfg["_base"], cfg["site"]))
    info = {"hostname": socket.gethostname(), "workers": cfg["workers"],
            "local": "%s:%s" % (cfg["local_host"], cfg["local_port"]),
            "public_host": cfg["public_host"], "pid": os.getpid()}
    st, _r, _h, data = pool.request(
        "POST", path, body=json.dumps(info).encode("utf-8"),
        headers={"x-tunnel-token": cfg["token"], "content-type": "application/json"},
    )
    if st != 200:
        raise RuntimeError("hello 失败: HTTP %s %s" % (st, data[:200]))
    return json.loads(data.decode("utf-8"))


def ack_items(cfg, pool, rids):
    if not rids:
        return True
    path = with_sc(cfg, "%s/c/ack/%s" % (cfg["_base"], ",".join(rids)))
    try:
        st, _r, _h, _d = pool.request(
            "POST", path, body=b"",
            headers={"x-tunnel-token": cfg["token"], "content-length": "0"},
        )
        return st == 200
    except Exception as e:
        dbg("ack 异常: %r" % (e,))
        return False


def pull_loop(cfg, pool, resp_pool, ref_pool, executor):
    backoff = 1.0
    path_tpl = with_sc(cfg, "%s/c/pull?site=%s&wait=%s" % (cfg["_base"], cfg["site"], cfg["pull_wait"]))
    while True:
        try:
            st, _r, _h, data = pool.request(
                "GET", path_tpl, headers={"x-tunnel-token": cfg["token"]},
                timeout=cfg["pull_wait"] + 15,
            )
            if st == 401:
                log("token 被中转拒绝，请检查两边 config.json 是否一致。停止。")
                os._exit(1)
            if st != 200:
                raise RuntimeError("pull HTTP %s" % st)
            items = json.loads(data.decode("utf-8")).get("items") or []
            backoff = 1.0
            if not items:
                continue
            dbg("pull 拿到 %d 条" % len(items))
            acked = ack_items(cfg, resp_pool, [it["rid"] for it in items if it.get("rid")])
            if not acked:
                log("ack 失败，本批 %d 条交回中转重排" % len(items))
                time.sleep(0.5)
                continue
            for it in items:
                executor(forward_one, it, cfg, resp_pool, ref_pool)
        except Exception as e:
            log("pull 异常: %r，%.1fs 后重试" % (e, backoff))
            time.sleep(backoff)
            backoff = min(backoff * 1.8, 15.0)


def main():
    global CFG
    CFG = load_cfg()
    cfg = CFG
    use_tls = cfg["_scheme"] == "https"
    port = 443 if use_tls else 80
    netloc = cfg["_netloc"]
    host = netloc.split(":")[0]
    if ":" in netloc:
        port = int(netloc.rsplit(":", 1)[1])

    log("relay=%s site=%s local=%s:%s public_host=%s workers=%d" % (
        cfg["relay_url"], cfg["site"], cfg["local_host"], cfg["local_port"],
        cfg["public_host"], cfg["workers"]))

    pull_pool = ConnPool(host, port, use_tls, timeout=cfg["pull_wait"] + 30)
    resp_pool = ConnPool(host, port, use_tls, timeout=60)
    ref_pool = ConnPool(host, port, use_tls, timeout=60)
    try:
        info = hello(cfg, resp_pool)
        log("握手成功: site=%s limits=%s" % (info.get("site"), info.get("limits")))
    except Exception as e:
        log("握手失败: %r（继续重试，中转恢复后会自动接上）" % e)

    from concurrent.futures import ThreadPoolExecutor
    ex = ThreadPoolExecutor(max_workers=cfg["workers"], thread_name_prefix="fw")
    t = threading.Thread(target=pull_loop, args=(cfg, pull_pool, resp_pool, ref_pool, ex.submit),
                         name="pull", daemon=True)
    t.start()
    try:
        while t.is_alive():
            t.join(1)
    except KeyboardInterrupt:
        log("收到 SIGINT，退出")


CFG = DEFAULTS

if __name__ == "__main__":
    main()
AGENT_PY
  chmod 750 "$DIR/agent.py"

  cat > "$DIR/config.json" <<EOF
{
  "relay_url": "$RELAY_URL",
  "token": "$TOKEN",
  "site": "hydro",
  "sharecode": "$SHARECODE",
  "local_host": "127.0.0.1",
  "local_port": $LOCAL_PORT,
  "public_host": "",
  "forwarded_proto": "https",
  "workers": 24,
  "pull_wait": 25,
  "max_retry": 3,
  "verbose": false
}
EOF
  chmod 600 "$DIR/config.json"
}

write_service() {
  cat > "/etc/systemd/system/$SVC.service" <<EOF
[Unit]
Description=HydroOJ Tunnel Agent (outbound-only)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$DIR
ExecStart=$PY $DIR/agent.py
Environment=TUNNEL_AGENT_CONFIG=$DIR/config.json
Environment=PYTHONUNBUFFERED=1
Restart=always
RestartSec=2
Nice=10
IOSchedulingClass=best-effort
IOSchedulingPriority=6
LimitNOFILE=8192
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$SVC

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
}

check_local_port() {
  if "$PY" - "$LOCAL_PORT" <<'PYCHK'
import socket, sys
p = int(sys.argv[1])
s = socket.socket(); s.settimeout(0.5)
try:
    s.connect(("127.0.0.1", p)); sys.exit(0)
except Exception:
    sys.exit(1)
finally:
    s.close()
PYCHK
  then
    say "  [OK] 本地 Hydro 端口 $LOCAL_PORT 正在监听"
  else
    say "  [警告] 本地 127.0.0.1:$LOCAL_PORT 连不上 —— 请先确认 Hydro 真的在上面跑，否则学生访问会 502"
  fi
}

do_install() {
  [ "$(id -u)" -eq 0 ] || die "请用 root 执行：sudo bash $0 $*"
  find_python
  say ">>> 1/5 写文件到 $DIR"
  write_files
  say "  agent.py 与 config.json 已写好（Python: $PY）"

  say ">>> 2/5 检查本地 Hydro"
  check_local_port

  say ">>> 3/5 注册 systemd 服务"
  if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
    write_service
    systemctl enable "$SVC" >/dev/null 2>&1 || true
    systemctl restart "$SVC"
    USE_SYSTEMD=1
  else
    say "  未检测到 systemd，改用后台进程方式（重启机器后需重新执行本脚本）"
    pkill -f "$DIR/agent.py" 2>/dev/null || true
    TUNNEL_AGENT_CONFIG="$DIR/config.json" nohup "$PY" "$DIR/agent.py" >>"$DIR/agent.log" 2>&1 &
    echo $! > "$DIR/agent.pid"
    USE_SYSTEMD=0
  fi

  say ">>> 4/5 等待握手"
  ok=0
  for i in $(seq 1 20); do
    sleep 1
    if [ "$USE_SYSTEMD" -eq 1 ]; then
      if journalctl -u "$SVC" --no-pager -n 200 2>/dev/null | grep -q '握手成功'; then ok=1; break; fi
    else
      if grep -q '握手成功' "$DIR/agent.log" 2>/dev/null; then ok=1; break; fi
    fi
  done
  if [ "$ok" -eq 1 ]; then
    say "  [OK] 已连上云端中转"
  else
    say "  [警告] 20 秒内没看到握手成功，看日志排查：bash $0 logs"
  fi

  say ">>> 5/5 从公网验证一次"
  # 两个坑都要避开：
  #   1. agent 没连上时中转首页也会返回 200，但那是「状态页」，不是真的转发成功；
  #   2. 真实 Hydro 首页可能是 302 跳登录、路径可能 404，那反而说明隧道是通的。
  # 所以判据 = HTTP 码不是 000/503/504，且响应体不是中转状态页。
  code="$(curl -sSL -m 15 -o /tmp/tunnel-check.html -w '%{http_code}' \
    "$RELAY_URL/?sharecode=$SHARECODE" 2>/dev/null || echo 000)"
  say "  公网返回 HTTP $code"
  if case "$code" in 000|503|504) true ;; *) false ;; esac \
     || grep -q '隧道中转状态' /tmp/tunnel-check.html 2>/dev/null; then
    say "  [警告] 隧道没通（可能是状态页或 agent 没接）。先看 bash $0 logs"
  else
    say "  [OK] 隧道通了！把 $RELAY_URL/?sharecode=$SHARECODE 发给学生即可"
  fi

  say ""
  say "完成。以后常用命令："
  say "  bash $0 status      看状态"
  say "  bash $0 logs        看日志"
  say "  bash $0 8080        改本地端口重装"
  say "  bash $0 uninstall   卸载"
}

do_status() {
  if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files "$SVC.service" >/dev/null 2>&1; then
    systemctl status "$SVC" --no-pager -l || true
    say ""
    say "最近日志："
    journalctl -u "$SVC" --no-pager -n 20 || true
  else
    if [ -f "$DIR/agent.pid" ] && kill -0 "$(cat "$DIR/agent.pid")" 2>/dev/null; then
      say "agent 正在运行，PID $(cat "$DIR/agent.pid")"
    else
      say "agent 未运行"
    fi
    tail -n 20 "$DIR/agent.log" 2>/dev/null || true
  fi
  say ""
  say "中转状态（需 agent token，勿外传）："
  say "  curl -s -H 'x-tunnel-token: $TOKEN' '$RELAY_URL/_stats'"
}

do_logs() {
  if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files "$SVC.service" >/dev/null 2>&1; then
    journalctl -u "$SVC" -f
  else
    tail -f "$DIR/agent.log"
  fi
}

do_uninstall() {
  [ "$(id -u)" -eq 0 ] || die "请用 root 执行：sudo bash $0 uninstall"
  if command -v systemctl >/dev/null 2>&1; then
    systemctl stop "$SVC" 2>/dev/null || true
    systemctl disable "$SVC" 2>/dev/null || true
    rm -f "/etc/systemd/system/$SVC.service"
    systemctl daemon-reload
  fi
  pkill -f "$DIR/agent.py" 2>/dev/null || true
  rm -rf "$DIR"
  say "已卸载（$DIR 已删除）"
}

case "$CMD" in
  install)   do_install ;;
  status)    do_status ;;
  logs)      do_logs ;;
  uninstall) do_uninstall ;;
  *)
    say "用法: bash $0 [本地Hydro端口 [TOKEN] [SHARECODE] | status | logs | uninstall]"
    say "  bash $0 8888 <TOKEN> <SHARECODE>   安装（端口按实际 Hydro 改）"
    say "  bash $0 status / logs              查看状态 / 实时日志"
    say "  bash $0 uninstall                  卸载"
    say "  也可用环境变量 TUNNEL_TOKEN / TUNNEL_SHARECODE 代替后两个参数"
    exit 1
    ;;
esac
