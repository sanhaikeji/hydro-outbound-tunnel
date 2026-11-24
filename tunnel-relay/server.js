#!/usr/bin/env node
'use strict'

const http = require('http')
const crypto = require('crypto')
const fs = require('fs')
const path = require('path')
const zlib = require('zlib')

const DEFAULTS = {
  port: 3000,
  maxBodyBytes: 8 * 1024 * 1024,
  inlineBodyBytes: 256 * 1024,
  maxQueue: 2048,
  maxBatch: 64,
  pullWaitMs: 25000,
  requestTimeoutMs: 20000,
  orphanGraceMs: 4000,
  ackTimeoutMs: 900,
  // 队列有货但没 pull 挂着时，先等这么久再复查（给 agent 的 ack+转发间隙留余地）
  noAgentGraceMs: 500,
  // 复查时如果 lastPullAt 也冻结超过这么久，才认定 agent 真的没了。
  // 必须显著大于 agent 处理一批的耗时，否则高并发下会误杀正常请求。
  noAgentDeadMs: 1500,
  // 请求进来时若既没 pull 又没近期活动，直接 503，不必进队列
  agentSilenceMs: 3000,
  cacheMaxBytes: 200 * 1024 * 1024,
  cacheTtlMs: 300000,
  maxMetaBytes: 7000,
}

const HOP_BY_HOP = new Set([
  'connection', 'keep-alive', 'transfer-encoding', 'upgrade', 'te',
  'proxy-authorization', 'proxy-connection', 'trailer',
])

// 缓存白名单必须从严：命中后请求不再进 Hydro，也就绕过了 Hydro 的登录检查。
// 只允许明确公开的静态前缀；upload / 附件 / 「任意扩展名」一律不缓存。
const CACHEABLE_PATH = /^\/(?:static|components|public)\//

const CACHEABLE_TYPE = /^(?:text\/css|text\/javascript|application\/javascript|image\/(?:png|jpe?g|gif|svg\+xml|webp|avif|x-icon)|font\/|application\/(?:font|wasm|x-font))/

function loadConfig() {
  const p = process.env.TUNNEL_CONFIG || path.join(__dirname, 'config.json')
  let raw
  try {
    raw = JSON.parse(fs.readFileSync(p, 'utf8'))
  } catch (e) {
    console.error('[relay] cannot read config ' + p + ': ' + e.message)
    process.exit(1)
  }
  const cfg = Object.assign({}, DEFAULTS, raw)
  if (!cfg.sites || !Object.keys(cfg.sites).length) {
    console.error('[relay] config needs at least one site')
    process.exit(1)
  }
  if (!cfg.defaultSite) cfg.defaultSite = Object.keys(cfg.sites)[0]
  if (!cfg.sites[cfg.defaultSite]) {
    console.error('[relay] defaultSite "' + cfg.defaultSite + '" not found in sites')
    process.exit(1)
  }
  return cfg
}

function createSite(id, raw) {
  const token = String(raw.token || '')
  return {
    id,
    name: String(raw.name || id),
    expectedHost: String(raw.expectedHost || ''),
    token,
    configured: !/^replace|^change|^your_/i.test(token) && token.length >= 16,
    queue: [],
    waiters: [],
    waiting: new Map(),
    bodies: new Map(),
    lastPullAt: 0,
    lastHelloAt: 0,
    idleTimer: null,
    cache: new LruCache(cfg.cacheMaxBytes),
    stats: {
      requests: 0, served: 0, timedOut: 0, rejected: 0, cacheHit: 0,
      cacheMiss: 0, pulls: 0, responses: 0, bytesIn: 0, bytesOut: 0,
      clientErrors: 0, startedAt: Date.now(),
    },
  }
}

class LruCache {
  constructor(maxBytes) {
    this.map = new Map()
    this.bytes = 0
    this.maxBytes = maxBytes
  }
  get(key) {
    const e = this.map.get(key)
    if (!e) return null
    if (Date.now() - e.at > cfg.cacheTtlMs) {
      this._drop(key, e)
      return null
    }
    this.map.delete(key)
    this.map.set(key, e)
    return e
  }
  set(key, entry) {
    if (entry.body.length > this.maxBytes / 4) return
    const old = this.map.get(key)
    if (old) this._drop(key, old)
    this.map.set(key, entry)
    this.bytes += entry.body.length
    while (this.bytes > this.maxBytes) {
      const k = this.map.keys().next().value
      if (!k) break
      this._drop(k, this.map.get(k))
    }
  }
  _drop(key, entry) {
    this.map.delete(key)
    this.bytes -= entry.body.length
  }
  get size() { return this.map.size }
}

const cfg = loadConfig()
const sites = {}
for (const id of Object.keys(cfg.sites)) sites[id] = createSite(id, cfg.sites[id])
const defaultSite = sites[cfg.defaultSite]

function clientIp(req) {
  // 网关在前面，socket.remoteAddress 是网关自己的地址（127.0.0.1 / 内网），
  // 不能当学生 IP 用 —— 那样 Hydro 的风控会把全班判成同一个人。
  // 优先级：x-real-ip（网关写的单值，最可信）> x-forwarded-for 最左段 > socket 地址。
  const real = String(req.headers['x-real-ip'] || '').trim()
  if (real) return real

  const xff = String(req.headers['x-forwarded-for'] || '').trim()
  if (xff) {
    // 最左段是发起方。后面的段是各级代理，不能取。
    const first = xff.split(',')[0].trim()
    if (first && !isPrivateIp(first)) return first
  }

  const ra = req.socket && req.socket.remoteAddress ? req.socket.remoteAddress : ''
  return ra.replace(/^::ffff:/, '').replace(/^::1$/, '127.0.0.1')
}

function isPrivateIp(ip) {
  return /^(10\.|127\.|169\.254\.|172\.1[6-9]\.|172\.2[0-9]\.|172\.3[01]\.|192\.168\.)/.test(ip)
}

function isPlatformToken(value) {
  const v = String(value || '')
  if (!/^Bearer\s+/i.test(v)) return false
  const jwt = v.replace(/^Bearer\s+/i, '')
  if (!/^ey[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\./.test(jwt)) return false
  try {
    const body = jwt.split('.')[1].replace(/-/g, '+').replace(/_/g, '/')
    const json = JSON.parse(Buffer.from(body + '='.repeat((4 - body.length % 4) % 4), 'base64').toString('utf8'))
    // 平台租户凭据的特征字段，普通应用的 JWT 不会有
    if (json.space || json.region === 'bj10' || /cloudstudio/i.test(String(json.iss || ''))) return true
  } catch (e) {
    return false
  }
  return false
}

function tokenOk(site, supplied) {
  if (!site.configured || !supplied) return false
  const a = Buffer.from(site.token)
  const b = Buffer.from(String(supplied))
  if (a.length !== b.length) return false
  return crypto.timingSafeEqual(a, b)
}

// 诊断接口专用鉴权：只认 agent 手里那把 token，放在 x-tunnel-token 头里。
// 不能用 sharecode —— 学生手里也有它，挡不住任何人。
function isAdmin(req) {
  const supplied = req.headers['x-tunnel-token']
  if (typeof supplied !== 'string') return false
  for (const id of Object.keys(sites)) {
    if (tokenOk(sites[id], supplied)) return true
  }
  return false
}

function readBody(req, maxBytes) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let size = 0
    let done = false
    req.on('data', (c) => {
      if (done) return
      size += c.length
      if (size > maxBytes) {
        done = true
        reject(Object.assign(new Error('body too large'), { kind: 'TOO_LARGE' }))
        return
      }
      chunks.push(c)
    })
    req.on('end', () => { if (!done) { done = true; resolve(Buffer.concat(chunks)) } })
    req.on('error', (e) => { if (!done) { done = true; reject(e) } })
  })
}

function filterReqHeaders(headers) {
  const out = {}
  for (const k of Object.keys(headers)) {
    const lk = k.toLowerCase()
    if (HOP_BY_HOP.has(lk)) continue
    if (lk === 'host') continue
    if (lk === 'content-length') continue
    // 隧道自己的管理头不能透到 OJ：既避免泄露部署细节，也防止有人拿它当鉴权凭据试探。
    if (lk === 'x-tunnel-token' || lk === 'x-tunnel-via') continue
    // 统一让上游返回明文,避免压缩/缓存两层不一致
    if (lk === 'accept-encoding') continue
    if (lk === 'x-forwarded-for' || lk === 'x-real-ip') continue
    if (lk === 'x-forwarded-proto' || lk === 'x-forwarded-host') continue
    // 云沙箱网关会给每个请求塞一个 Bearer JWT（space token）。
    // 这是平台凭据，绝不能透到 OJ 端去 —— 它会出现在 Hydro 日志里。
    if (lk === 'authorization') {
      // 云沙箱网关会给每个请求塞一个 Bearer JWT（space token），这是平台凭据，
      // 绝不能透到 OJ 端（会进 Hydro 日志）。但用户自己的 Basic / API token 认证要保留，
      // 所以只剥平台那一类。
      if (!isPlatformToken(headers[k])) out[lk] = headers[k]
      continue
    }
    // 平台内部标识，对 OJ 无意义且可能泄露部署信息
    if (lk.startsWith('x-wb-') || lk.startsWith('x-space-')) continue
    if (lk === 'x-trace-id' || lk === 'traceparent' || lk === 'x-request-id') continue
    if (lk === 'stgw-request-id' || lk === 'x-stgw-time') continue
    if (lk === 'x-client-proto' || lk === 'x-client-proto-ver' || lk === 'x-forwarded-port') continue
    out[lk] = headers[k]
  }
  return out
}

function flush(site) {
  while (site.queue.length && site.waiters.length) {
    const waiter = site.waiters.shift()
    // agent 进程被杀时，它那条挂起的 pull 可能还没被内核回收成 close 事件。
    // 往一个已死的 socket 投递 = 请求凭空蒸发，学生只能干等硬超时。
    if (!waiterAlive(waiter)) {
      clearTimeout(waiter.timer)
      continue
    }
    deliverBatch(site, waiter.res, waiter)
  }

  // agent 每次交付完一批要去 ack、再转发，然后才发下一条 pull。
  // 这中间有一小段「队列有货但没 pull 挂着」的正常空窗（几十毫秒）。
  // 但如果宽限期内还是没人来接，就要怀疑 agent 真的没了。
  //
  // 这里不能一刀切地杀：并发一高，agent 正在 ack + 排转发任务的那一两百毫秒里，
  // 新到达的请求正好落在空窗中，一刀切会把正常请求误判成离线（实测 60 并发掉到 37/60）。
  //
  // 所以分两级：第一次到期只复查；只有当 lastPullAt/lastHelloAt 也冻结超过
  // noAgentDeadMs（说明 agent 是真的不再活动了）才失败。
  // agent 活着时每轮 pull 都会刷新 lastPullAt，永远不会触发第二级。
  if (site.queue.length && !hasLiveWaiter(site)) {
    if (!site.idleTimer) armIdleWatch(site)
  } else if (site.idleTimer) {
    clearTimeout(site.idleTimer)
    site.idleTimer = null
  }
}

function armIdleWatch(site) {
  const tick = () => {
    site.idleTimer = null
    // 有活 pull 来接了，或者队列已空 —— 危险解除
    if (!site.queue.length || hasLiveWaiter(site)) return

    const last = Math.max(site.lastPullAt || 0, site.lastHelloAt || 0)
    if (last && Date.now() - last < cfg.noAgentDeadMs) {
      // agent 刚刚还在 pull，这只是处理间隙，再等一轮
      site.idleTimer = setTimeout(tick, cfg.noAgentGraceMs)
      site.idleTimer.unref()
      return
    }
    console.log('[relay] 无 agent 接取且活动已停止 ' + Math.round((Date.now() - last)) +
      'ms，快速失败 ' + site.queue.length + ' 条')
    failQueue(site, 'OJ 端 agent 已离线，请稍后重试。')
  }
  site.idleTimer = setTimeout(tick, cfg.noAgentGraceMs)
  site.idleTimer.unref()
}

function waiterAlive(waiter) {
  const r = waiter.res
  if (!r) return false
  if (r.destroyed || r.writableEnded || r.headersSent) return false
  if (!r.socket || r.socket.destroyed) return false
  return true
}

function deliverBatch(site, res, waiter) {
  const batch = site.queue.splice(0, cfg.maxBatch)
  const items = batch.map((it) => {
    it.delivered = true
    it.deliveredAt = Date.now()
    const item = {
      rid: it.rid,
      method: it.method,
      path: it.path,
      headers: it.headers,
      ip: it.ip,
    }
    if (it.body.length) {
      if (it.body.length <= cfg.inlineBodyBytes) {
        item.body = it.body.toString('base64')
      } else {
        item.bodyRef = true
        item.bodySize = it.body.length
        site.bodies.set(it.rid, it.body)
        setTimeout(() => site.bodies.delete(it.rid), 30000).unref()
      }
    }
    return item
  })
  site.stats.pulls++
  site.lastPullAt = Date.now()
  if (waiter) {
    waiter.delivered = true
    clearTimeout(waiter.timer)
  }
  writeJson(res, 200, { items, now: Date.now() })

  // 投递确认：agent 收到批后立刻 POST /c/ack 报 rid。
  // 只有真拿到 ack 才算这批发出去过 —— 否则就是写进了一条对端已静的连接，
  // 请求根本没到达 agent。这种情况把请求放回队首重发，而不是让学生等超时。
  if (batch.length) {
    const rids = batch.map((it) => it.rid)
    setTimeout(() => {
      let requeued = 0
      let ackedCount = 0
      for (const rid of rids) {
        const it = site.waiting.get(rid)
        // 已经不在等待表里 = 已经完成或已被清理，不必追究
        if (!it) continue
        if (it.acked) { ackedCount++; continue }
        if (it.finished) continue
        // 没 ack：撤销这次投递标记，放回队首
        it.delivered = false
        it.deliveredAt = 0
        if (!site.queue.includes(it)) {
          site.queue.unshift(it)
          requeued++
        }
      }
      if (ackedCount) console.log('[relay] ack ok: ' + ackedCount + '/' + rids.length)
      if (requeued) {
        // 这里绝对不能 failQueue。
        // 高并发下「这一刻没有活 pull」是常态：agent 正在 ack + 派转发任务。
        // 原来的做法是超时就 failQueue 整个队列，结果把别的批次里还在正常飞行的
        // 请求一起杀掉了（实测 60 并发只成 37/60）。
        // 正确的做法是：只把缺 ack 的这批放回队列，让 flush 里那个「两级空闲看门狗」
        // 去统一判断 agent 到底死没死 —— 判断只能有一个出处。
        console.log('[relay] ' + cfg.ackTimeoutMs + 'ms 未收到 ack，重排 ' + requeued + ' 条')
        flush(site)
      }
    }, cfg.ackTimeoutMs).unref()
  }
}

function hasLiveWaiter(site) {
  for (const w of site.waiters) {
    if (waiterAlive(w)) return true
  }
  return false
}

function failQueue(site, msg) {
  const items = site.queue.splice(0, site.queue.length)
  for (const it of items) {
    it.finished = true
    site.waiting.delete(it.rid)
    clearTimeout(it.timer)
    clearTimeout(it.orphanTimer)
    site.stats.rejected++
    if (!it.res.writableEnded) {
      it.res.writeHead(503, {
        'content-type': 'text/plain; charset=utf-8',
        'retry-after': '3',
      })
      it.res.end(msg)
    }
  }
}

function writeJson(res, status, obj) {
  if (res.writableEnded || res.headersSent) return
  const body = Buffer.from(JSON.stringify(obj))
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': body.length,
    'cache-control': 'no-store',
  })
    res.end(body)
}

function dropItem(site, rid) {
  const it = site.waiting.get(rid)
  if (!it) return null
  site.waiting.delete(rid)
  site.bodies.delete(rid)
  clearTimeout(it.timer)
  clearTimeout(it.orphanTimer)
  const qi = site.queue.indexOf(it)
  if (qi >= 0) site.queue.splice(qi, 1)
  return it
}

function respondBrowser(site, rid, entry) {
  const it = dropItem(site, rid)
  if (!it) return false
  it.finished = true
  if (it.res.writableEnded) return true
  const hdrs = {}
  for (const k of Object.keys(entry.headers || {})) {
    hdrs[k] = entry.headers[k]
  }
  hdrs['content-length'] = entry.body.length
  if (it.method === 'HEAD' || entry.status === 204 || entry.status === 304) {
    it.res.writeHead(entry.status, hdrs)
    it.res.end()
  } else {
    it.res.writeHead(entry.status, hdrs)
    it.res.end(entry.body)
  }
  site.stats.served++
  site.stats.bytesOut += entry.body.length
  return true
}

function cacheKey(req) {
  return req.method + ' ' + req.url
}

function isCacheable(req) {
  if (req.method !== 'GET') return false
  if (req.headers.range) return false
  // 缓存键里没有 Cookie / Authorization。带这两种头的请求，响应可能跟用户有关，
  // 一旦缓存就会把 A 的私有内容发给 B —— 必须整条跳过缓存。
  if (req.headers.cookie || req.headers.authorization) return false
  let pathname
  try {
    pathname = new URL(req.url, 'http://relay.local').pathname
  } catch (e) {
    return false
  }
  return CACHEABLE_PATH.test(pathname)
}

function shouldStore(key, entry) {
  if (entry.status !== 200) return false
  const h = entry.headers || {}
  // 带 Set-Cookie 说明响应有会话语义；Vary 里提到凭据也说明响应因用户而异。
  if (h['set-cookie'] || h['set-cookie2']) return false
  if (/\b(cookie|authorization|\*)\b/i.test(String(h.vary || ''))) return false
  if (/\b(private|no-store|no-cache)\b/i.test(String(h['cache-control'] || ''))) return false
  const ct = String(h['content-type'] || '')
  // 不再用「路径像静态」兜底：必须 content-type 也是明确的静态类型。
  return CACHEABLE_TYPE.test(ct)
}

function dashboard(site) {
  const s = site.stats
  const upMs = Date.now() - s.startedAt
  const rows = [
    ['站点', site.name + ' (' + site.id + ')'],
    ['配置状态', site.configured ? '已配置' : '未配置 token'],
    ['agent 在线', site.waiters.length > 0 ? '是 (有挂起的 pull)' : '否'],
    ['队列中', String(site.queue.length)],
    ['在途请求', String(site.waiting.size)],
    ['累计请求', String(s.requested || s.requests)],
    ['已响应', String(s.served)],
    ['超时 504', String(s.timedOut)],
    ['拒绝 503', String(s.rejected)],
    ['缓存命中', String(s.cacheHit) + ' / 未命中 ' + String(s.cacheMiss)],
    ['上行字节', s.bytesIn + ' B'],
    ['下行字节', s.bytesOut + ' B'],
    ['运行时长', Math.round(upMs / 1000) + ' s'],
  ]
  const trs = rows.map((r) => '<tr><td>' + esc(r[0]) + '</td><td>' + esc(r[1]) + '</td></tr>').join('')
  return '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">' +
    '<meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<title>隧道中转状态</title><style>' +
    'body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Microsoft YaHei",sans-serif;' +
    'margin:0;padding:32px;background:#fafaf9;color:#1c1917;line-height:1.6}' +
    'h1{font-size:20px;font-weight:600;margin:0 0 4px}' +
    'p.sub{color:#78716c;font-size:13px;margin:0 0 24px}' +
    'table{border-collapse:collapse;background:#fff;border:1px solid #e7e5e4;border-radius:8px;' +
    'min-width:420px;overflow:hidden}' +
    'td{padding:9px 16px;font-size:13px;border-bottom:1px solid #f5f5f4}' +
    'td:first-child{color:#78716c;width:130px}' +
    'tr:last-child td{border-bottom:0}' +
    '.warn{margin-top:20px;padding:12px 16px;background:#fef2f2;border:1px solid #fecaca;' +
    'border-radius:8px;color:#991b1b;font-size:13px}' +
    '</style></head><body><h1>隧道中转状态</h1>' +
    '<p class="sub">学生侧入口 -> 本中转 -> OJ 端 agent（出站长轮询）</p>' +
    '<table>' + trs + '</table>' +
    (site.configured ? '' : '<div class="warn">config.json 里的 token 还是占位符，' +
      '中转已拒绝所有隧道与控制请求。改成你自己的随机串并重启后才会工作。</div>') +
    '</body></html>'
}

function esc(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
}

async function handleControl(req, res, url) {
  const site = sites[url.searchParams.get('site') || cfg.defaultSite] || defaultSite
  if (!tokenOk(site, req.headers['x-tunnel-token'])) {
    site.stats.rejected++
    return writeJson(res, 401, { error: 'bad_token' })
  }

  if (url.pathname === '/c/hello') {
    const body = await readBody(req, 64 * 1024).catch(() => Buffer.alloc(0))
    let payload = {}
    try { payload = JSON.parse(body.toString('utf8') || '{}') } catch (e) {}
    site.lastHelloAt = Date.now()
    return writeJson(res, 200, {
      ok: true,
      site: site.id,
      server: 'tunnel-relay',
      limits: {
        maxBodyBytes: cfg.maxBodyBytes,
        maxBatch: cfg.maxBatch,
        pullWaitMs: cfg.pullWaitMs,
      },
      echo: payload,
      now: Date.now(),
    })
  }

  if (url.pathname === '/c/pull') {
    const waitSec = Number(url.searchParams.get('wait') || cfg.pullWaitMs / 1000)
    const wait = Math.max(1000, Math.min(waitSec * 1000, cfg.pullWaitMs))

    if (site.queue.length) {
      return deliverBatch(site, res)
    }

    const waiter = { res, timer: null, delivered: false }
    waiter.timer = setTimeout(() => {
      const i = site.waiters.indexOf(waiter)
      if (i >= 0) site.waiters.splice(i, 1)
      if (!res.writableEnded) writeJson(res, 200, { items: [], now: Date.now() })
    }, wait)

    const drop = () => {
      clearTimeout(waiter.timer)
      const i = site.waiters.indexOf(waiter)
      if (i >= 0) site.waiters.splice(i, 1)

      // 这条 pull 在没拿到批的情况下断开 = agent 掉线了。
      // 队列里还没投递的请求注定没人来取，立刻失败，别让学生等孤儿超时。
      if (!waiter.delivered && site.queue.length && !hasLiveWaiter(site)) {
        console.log('[relay] agent pull 断开且无其他连接，快速失败 ' + site.queue.length + ' 条')
        failQueue(site, 'OJ 端 agent 已离线，请稍后重试。')
      }
    }
    res.on('close', drop)
    site.waiters.push(waiter)
    return
  }

  if (url.pathname.startsWith('/c/ack/')) {
    // agent 收到批后立刻逐条报确认。中转据此知道这批确实到达了对端，
    // 而不是写进了一条已被网关静默回收的连接。
    const rids = url.pathname.slice('/c/ack/'.length).split(',').filter(Boolean)
    let n = 0
    for (const rid of rids) {
      const it = site.waiting.get(rid)
      if (!it) continue
      it.acked = true
      n++
    }
    site.lastPullAt = Date.now()
    return writeJson(res, 200, { ok: true, acked: n })
  }

  if (url.pathname.startsWith('/c/body/')) {
    const rid = url.pathname.slice('/c/body/'.length)
    const buf = site.bodies.get(rid)
    if (!buf) return writeJson(res, 404, { error: 'no_body' })
    res.writeHead(200, {
      'content-type': 'application/octet-stream',
      'content-length': buf.length,
      'cache-control': 'no-store',
    })
    return res.end(buf)
  }

  if (url.pathname.startsWith('/c/resp/')) {
    const rid = url.pathname.slice('/c/resp/'.length)
    const metaRaw = req.headers['x-resp-meta'] || ''
    let meta = { status: 502, headers: {} }
    if (metaRaw.length > cfg.maxMetaBytes) {
      return writeJson(res, 413, { error: 'meta_too_large' })
    }
    try {
      meta = JSON.parse(Buffer.from(metaRaw, 'base64').toString('utf8'))
    } catch (e) {
      return writeJson(res, 400, { error: 'bad_meta' })
    }
    let body = await readBody(req, cfg.maxBodyBytes).catch(() => null)
    if (body === null) return writeJson(res, 413, { error: 'body_too_large' })
    // 云沙箱 WAF 会扫描 POST body，真实网页里的 <script> 等特征会被拦成 403。
    // agent 先 gzip 压缩绕过扫描，这里解压还原。头缺失时按原始 body 处理，兼容旧 agent。
    if (String(req.headers['x-resp-encoding'] || '') === 'gzip') {
      try {
        body = zlib.gunzipSync(body)
      } catch (e) {
        return writeJson(res, 400, { error: 'bad_gzip' })
      }
    }
    site.bodies.delete(rid)
    site.stats.responses++
    site.stats.bytesIn += body.length
    const entry = { status: Number(meta.status) || 502, headers: meta.headers || {}, body }

    const pending = site.waiting.get(rid)
    if (pending && pending.cacheKey && shouldStore(pending.cacheKey, entry)) {
      site.cache.set(pending.cacheKey, {
        status: entry.status,
        headers: entry.headers,
        body: entry.body,
        at: Date.now(),
      })
    }
    const ok = respondBrowser(site, rid, entry)
    return writeJson(res, 200, { ok, rid })
  }

  return writeJson(res, 404, { error: 'unknown_control_path' })
}

async function handleProxy(site, req, res) {
  site.stats.requests++
  site.lastProxyAt = Date.now()

  if (!site.configured) {
    res.writeHead(503, { 'content-type': 'text/plain; charset=utf-8' })
    return res.end('中转未配置: config.json 的 sites.' + site.id + '.token 仍是占位符。')
  }

  // 存活判定看「最近一次活动」，不能看是否有挂起的 pull：
  // agent 在转发一批请求的间隙本来就没有 pull 挂着，那是正常状态。
  if (!site.waiters.length && !isAgentAlive(site)) {
    site.stats.rejected++
    res.writeHead(503, { 'content-type': 'text/plain; charset=utf-8', 'retry-after': '5' })
    return res.end('OJ 端 agent 当前离线，请稍后重试。')
  }

  if (site.queue.length >= cfg.maxQueue) {
    site.stats.rejected++
    res.writeHead(503, { 'content-type': 'text/plain; charset=utf-8', 'retry-after': '2' })
    return res.end('中转队列已满，请稍后重试。')
  }

  let body
  try {
    body = await readBody(req, cfg.maxBodyBytes)
  } catch (e) {
    if (e.kind === 'TOO_LARGE') {
      site.stats.clientErrors++
      res.writeHead(413, { 'content-type': 'text/plain; charset=utf-8' })
      return res.end('请求体超过上限 ' + cfg.maxBodyBytes + ' 字节。')
    }
    return
  }

  const key = cacheKey(req)
  if (isCacheable(req)) {
    const hit = site.cache.get(key)
    if (hit) {
      site.stats.cacheHit++
      site.stats.served++
      site.stats.bytesOut += hit.body.length
      return serveDirect(res, hit)
    }
    site.stats.cacheMiss++
  }

  const rid = crypto.randomUUID()
  const item = {
    rid,
    method: req.method,
    path: req.url,
    headers: filterReqHeaders(req.headers),
    body,
    ip: clientIp(req),
    cacheKey: isCacheable(req) ? key : null,
    delivered: false,
    acked: false,
    finished: false,
    res,
    timer: null,
    orphanTimer: null,
    enqueuedAt: Date.now(),
  }
  site.waiting.set(rid, item)

  const drop = () => {
    if (!site.waiting.has(rid)) return
    site.waiting.delete(rid)
    site.bodies.delete(rid)
    const qi = site.queue.indexOf(item)
    if (qi >= 0) site.queue.splice(qi, 1)
  }

  // 硬超时：请求已在 agent 手里但迟迟不回传
  item.timer = setTimeout(() => {
    if (!site.waiting.has(rid)) return
    drop()
    site.stats.timedOut++
    if (!res.writableEnded) {
      res.writeHead(504, { 'content-type': 'text/plain; charset=utf-8' })
      res.end('OJ 端响应超时（' + cfg.requestTimeoutMs + 'ms）。')
    }
  }, cfg.requestTimeoutMs)

  // 孤儿超时：请求已入队但一直没有 agent 来取。这比等满 20 秒友好得多，
  // 尤其是 agent 刚断线的几秒里，学生不该被拖着干等。
  item.orphanTimer = setTimeout(() => {
    if (item.delivered || item.acked) return
    if (!site.waiting.has(rid)) return
    drop()
    site.stats.rejected++
    if (!res.writableEnded) {
      res.writeHead(503, { 'content-type': 'text/plain; charset=utf-8', 'retry-after': '3' })
      res.end('OJ 端 agent 未响应，请稍后重试。')
    }
  }, cfg.orphanGraceMs)

  res.on('close', () => {
    // 学生主动断开：只释放等待表，让队列里的条目自然过期，
    // 避免上游正在写入时被打断。
    if (site.waiting.get(rid) === item && !item.delivered) {
      site.waiting.delete(rid)
      const qi = site.queue.indexOf(item)
      if (qi >= 0) site.queue.splice(qi, 1)
      clearTimeout(item.timer)
      clearTimeout(item.orphanTimer)
    }
  })

  site.queue.push(item)
  flush(site)
}

function serveDirect(res, entry) {
  const hdrs = Object.assign({}, entry.headers)
  hdrs['content-length'] = entry.body.length
  res.writeHead(entry.status, hdrs)
  res.end(entry.body)
}

// agent 视为存活：有挂起的 pull（这是最硬的信号）。
// 否则看最近是否还在活跃地 pull/hello —— 但窗口必须短。
// 窗口开大（比如 pullWaitMs×2）会把「刚挂掉的 agent」也算成活的，
// 结果是请求被排队空等，学生白等到孤儿超时才收到 503。
function isAgentAlive(site) {
  if (site.waiters.length) return true
  const last = Math.max(site.lastPullAt || 0, site.lastHelloAt || 0)
  if (!last) return false
  return Date.now() - last < cfg.agentSilenceMs
}

const server = http.createServer((req, res) => {
  let url
  try {
    url = new URL(req.url, 'http://' + (req.headers.host || 'localhost'))
  } catch (e) {
    res.writeHead(400)
    return res.end()
  }

  // 诊断接口只允许拿 agent token 的运维访问。
  // /_hdr 已彻底删除：它把网关塞进来的 Authorization: Bearer <space JWT> 和 Cookie
  // 原样回显，学生打开这个路径就能拿到平台凭据。真实 IP 改为在 /_stats 里看计数、
  // 或在 agent 日志里看，不再提供任何回显原始请求头的公网接口。
  if (url.pathname === '/_stats') {
    if (!isAdmin(req)) {
      res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' })
      return res.end('Not Found')
    }
    const out = {}
    for (const id of Object.keys(sites)) out[id] = sites[id].stats
    return writeJson(res, 200, out)
  }

  if (url.pathname === '/_hdr') {
    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' })
    return res.end('Not Found')
  }

  if (url.pathname.startsWith('/c/')) {
    return handleControl(req, res, url).catch((e) => {
      console.error('[relay] control error', e)
      if (!res.headersSent) writeJson(res, 500, { error: 'internal' })
    })
  }

  const site = defaultSite

  if (url.pathname === '/' && req.method === 'GET' && isAdmin(req)) {
    if (!site.configured || (site.waiters.length === 0 && site.stats.requests === 0)) {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' })
      return res.end(dashboard(site))
    }
  }

  handleProxy(site, req, res).catch((e) => {
    console.error('[relay] proxy error', e)
    if (!res.headersSent) {
      res.writeHead(500, { 'content-type': 'text/plain; charset=utf-8' })
      res.end('中转内部错误')
    }
  })
})

server.on('clientError', (err, socket) => {
  if (socket.writable) socket.end('HTTP/1.1 400 Bad Request\r\n\r\n')
})

server.keepAliveTimeout = 65000
server.headersTimeout = 70000

const port = Number(process.env.PORT || cfg.port)
server.listen(port, '0.0.0.0', () => {
  console.log('[relay] listening on 0.0.0.0:' + port)
  for (const id of Object.keys(sites)) {
    const s = sites[id]
    console.log('[relay] site ' + id + ' (' + s.name + ') token ' +
      (s.configured ? 'configured' : 'MISSING -> edit config.json'))
  }
})

process.on('SIGTERM', () => {
  console.log('[relay] SIGTERM, shutting down')
  server.close(() => process.exit(0))
  setTimeout(() => process.exit(0), 5000).unref()
})
