#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""OJ 端隧道 agent —— 全程只发出站请求，不接受任何入站连接。

行为：
  1. 握手上报（/c/hello）
  2. 单条长轮询拉批（/c/pull），拿到一批请求后并发转发给本地 HydroOJ
  3. 把响应（status + headers + 原始 body）回传（/c/resp/<rid>）

依赖：仅 Python 3.8+ 标准库。无 pip 依赖。
"""

import base64
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
    "relay_url": "",            # 例: https://oj-tunnel.xxx.link
    "token": "",
    "site": "default",
    "sharecode": "",            # 云沙箱发布链接里的 sharecode；自行部署的 VPS 留空
    "local_host": "127.0.0.1",
    "local_port": 8888,
    "public_host": "",          # 学生看到的域名；留空则用 relay_url 的 host
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

# 这些上行头必须由 agent 自己重写，绝不沿用学生侧的值
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


def with_sc(cfg, path):
    """把 sharecode 并进查询串。

    云沙箱发布出来的域名前面有一层网关，逐请求校验 sharecode，不带就返 403
    「链接不完整」。隧道接口是程序调用、没有浏览器 cookie，所以只能每条都带。
    """
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
            # 池里的连接可能已被对端关掉（网关回 Connection: close 时就是这样）。
            # http.client 不会主动把 sock 置 None，只看引用是查不出来的，
            # 必须真的探一下：select 可读即代表对端已关或已有脏数据。
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
            # 可读 = 有未消费的数据或 EOF，都不该拿来发新请求
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
        """发一个请求。连接被对端判死后自动换一条新连接重试一次。

        长轮询隧道里最常见的事故就是：一条空闲连接躺在池里，网关那头已经把它
        静默回收了。第一次请求必然撞死，但换新连接重发是安全的（pull 幂等，
        resp 回传用 rid 去重）。
        """
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
                # 换新连接再试一次，不睡——立刻重发
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

    # Hydro 靠 Host 生成绝对链接（app.proxy=true 时还会看 X-Forwarded-Host）
    seen["host"] = cfg["public_host"]
    # 真实学生 IP：中转观测到的 TCP 源地址，学生伪装的 XFF 已在两侧丢弃
    seen["x-forwarded-for"] = item.get("ip") or "0.0.0.0"
    seen["x-real-ip"] = item.get("ip") or "0.0.0.0"
    seen["x-forwarded-proto"] = cfg["forwarded_proto"]
    seen["x-forwarded-host"] = cfg["public_host"]
    # 让 Hydro 知道请求是经由代理进来的（不影响业务，便于日志排查）
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
        method = item["method"]
        path = item["path"]

        local = http.client.HTTPConnection(cfg["local_host"], cfg["local_port"], timeout=30)
        try:
            local.request(method, path, body=body, headers=headers)
            resp = local.getresponse()
            payload = resp.read()
            status = resp.status
            hdrs = filter_resp_headers(resp.getheaders())
            # 上游若意外带回压缩/分块标记，必须清掉，否则浏览器端会解不动
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
            # 元信息过大（极端多的 Set-Cookie），回退成纯文本提示，避免中转拒收
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
                    "POST",
                    path_resp,
                    body=payload,
                    headers={
                        "x-tunnel-token": cfg["token"],
                        "x-resp-meta": meta,
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
                json.dumps(
                    {
                        "status": 502,
                        "headers": {"content-type": "text/plain; charset=utf-8"},
                    }
                ).encode("utf-8")
            ).decode("ascii")
            resp_pool.request(
                "POST",
                with_sc(cfg, "%s/c/resp/%s" % (cfg["_base"], rid)),
                body="隧道转发失败：OJ 端无法连接本地服务。".encode("utf-8"),
                headers={
                    "x-tunnel-token": cfg["token"],
                    "x-resp-meta": meta,
                    "content-type": "application/octet-stream",
                },
            )
        except Exception:
            pass


def hello(cfg, pool):
    path = with_sc(cfg, "%s/c/hello?site=%s" % (cfg["_base"], cfg["site"]))
    info = {
        "hostname": socket.gethostname(),
        "workers": cfg["workers"],
        "local": "%s:%s" % (cfg["local_host"], cfg["local_port"]),
        "public_host": cfg["public_host"],
        "pid": os.getpid(),
    }
    st, _r, _h, data = pool.request(
        "POST", path, body=json.dumps(info).encode("utf-8"),
        headers={"x-tunnel-token": cfg["token"], "content-type": "application/json"},
    )
    if st != 200:
        raise RuntimeError("hello 失败: HTTP %s %s" % (st, data[:200]))
    return json.loads(data.decode("utf-8"))


def ack_items(cfg, pool, rids):
    """把一批 rid 确认给中转。返回是否成功。"""
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
    path_tpl = with_sc(
        cfg,
        "%s/c/pull?site=%s&wait=%s" % (cfg["_base"], cfg["site"], cfg["pull_wait"]),
    )
    while True:
        try:
            st, _r, _h, data = pool.request(
                "GET",
                path_tpl,
                headers={"x-tunnel-token": cfg["token"]},
                timeout=cfg["pull_wait"] + 15,
            )
            if st == 401:
                log("token 被中转拒绝，请检查两边 config.json 是否一致。停止。")
                os._exit(1)
            if st != 200:
                raise RuntimeError("pull HTTP %s" % st)
            payload = json.loads(data.decode("utf-8"))
            items = payload.get("items") or []
            backoff = 1.0
            if not items:
                continue
            dbg("pull 拿到 %d 条" % len(items))
            # 先把整批 ack 回去，中转据此确认这批真的到了 agent 手里。
            # 不做这步的话，中转无从区分「已投递」和「写进了死连接」，
            # 只能靠超时兜底，学生白等。
            acked = ack_items(cfg, resp_pool, [it["rid"] for it in items if it.get("rid")])
            if not acked:
                # ack 都没成功，说明这条链此刻不可用，别接这批活了
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
        log("握手失败: %r（继续重试 pull，中转恢复后会自动接上）" % e)

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
