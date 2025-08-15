/**
 * R2 CDN Edge Cache – Node + TypeScript (path-mirroring + meta-at-root + half-evict LRU)
 * - 数据文件：.cache/files/<upstreamPath>（与远端路径一致）
 * - 元数据：  .cache/<sha1(url)>.meta.json（meta.file 记录本地绝对路径）
 * - LRU：超过 MAX_CACHE_BYTES（默认 100 GiB）时，一次性删到一半容量（按 lastAccess 最老优先）
 * - 命中标记：
 *    - 首次查源站/未命中直透：x-cache: 0
 *    - 本地命中（含 Range 命中、HEAD 命中）：x-cache: node
 */

import crypto from 'node:crypto'
import fs from 'node:fs'
import fsp from 'node:fs/promises'
import http from 'node:http'
import path from 'node:path'
import { PassThrough, pipeline, Readable } from 'node:stream'
import { promisify } from 'node:util'
import { Client, Dispatcher, request as undiciRequest } from 'undici'

const pipe = promisify(pipeline)

// ---- Config ---------------------------------------------------------------
const ORIGIN_BASE = process.env.ORIGIN_BASE?.replace(/\/$/, '') || ''
if (!ORIGIN_BASE) {
  console.error('ERROR: ORIGIN_BASE is required.')
  process.exit(1)
}
const PORT = Number(process.env.PORT || 8787)
const CACHE_DIR = process.env.CACHE_DIR
  ? path.resolve(process.cwd(), process.env.CACHE_DIR)
  : path.resolve(process.cwd(), '.cache')

// 默认 100 GiB，可配
const MAX_CACHE_BYTES = Number(process.env.MAX_CACHE_BYTES || 100 * 1024 * 1024 * 1024)

// 0 = infinite（不做 revalidate）
const CACHE_TTL_SECONDS = Number(process.env.CACHE_TTL_SECONDS || 0)

// Ensure cache dir exists
await fsp.mkdir(CACHE_DIR, { recursive: true })
await fsp.mkdir(path.join(CACHE_DIR, 'files'), { recursive: true })

// ---- Types ----------------------------------------------------------------
interface Meta {
  url: string // Full origin URL
  file: string // Full local file absolute path
  status: number // Status code cached (usually 200)
  headers: Record<string, string> // Headers to replay
  size: number // Exact bytes of data file
  etag?: string
  lastModified?: string
  cachedAt: number // epoch ms
  lastAccess: number // epoch ms
}

// ---- Utility: key + path mapping -----------------------------------------
function keyFor(url: string) {
  return crypto.createHash('sha1').update(url).digest('hex')
}

// 把上游路径映射到本地磁盘路径：.cache/files/<pathname>
function toLocalFilePath(upstreamPath: string) {
  const clean = upstreamPath.replace(/^\/+/, '') // 去掉前导 /
  return path.join(CACHE_DIR, 'files', clean)
}

function metaPathByKey(key: string) {
  return path.join(CACHE_DIR, `${key}.meta.json`)
}

async function readMeta(key: string): Promise<Meta | null> {
  try {
    const buf = await fsp.readFile(metaPathByKey(key), 'utf8')
    return JSON.parse(buf) as Meta
  } catch {
    return null
  }
}
async function writeMeta(key: string, meta: Meta) {
  await fsp.writeFile(metaPathByKey(key), JSON.stringify(meta, null, 2), 'utf8')
}

async function fileExists(p: string) {
  try {
    await fsp.access(p, fs.constants.F_OK)
    return true
  } catch {
    return false
  }
}

function headerSingle(h: string | string[] | undefined): string | undefined {
  if (!h) return undefined
  if (Array.isArray(h)) return h[0]
  return h
}

function pickHeadersForCache(
  h: Record<string, string | string[] | undefined>
): Record<string, string> {
  const keep = [
    'content-type',
    'content-encoding',
    'content-language',
    'cache-control',
    'content-disposition'
  ]
  const out: Record<string, string> = {}
  for (const k of keep) {
    const v = headerSingle(h[k as keyof typeof h])
    if (v) out[k] = v
  }
  return out
}

// ---- FS utils: size + eviction -------------------------------------------
async function dirSizeRecursive(dir: string): Promise<number> {
  let total = 0
  const entries = await fsp.readdir(dir, { withFileTypes: true })
  for (const ent of entries) {
    const p = path.join(dir, ent.name)
    if (ent.isDirectory()) {
      total += await dirSizeRecursive(p)
    } else if (ent.isFile()) {
      const st = await fsp.stat(p)
      total += st.size
    }
  }
  return total
}

async function currentCacheBytes(): Promise<number> {
  // 只统计 data：.cache/files 目录
  return dirSizeRecursive(path.join(CACHE_DIR, 'files'))
}

/**
 * 当 total > MAX 时：
 * 1) 读取所有 meta（含 size、lastAccess）
 * 2) 按 lastAccess 升序（最老在前）排序
 * 3) 依次删除，直到 total <= MAX/2  —— “删除一半最老的文件”的效果
 */
async function enforceBudget() {
  let total = await currentCacheBytes()
  if (total <= MAX_CACHE_BYTES) return

  const metas: Array<{ key: string; file: string; size: number; lastAccess: number }> = []
  const files = await fsp.readdir(CACHE_DIR)
  for (const f of files) {
    if (!f.endsWith('.meta.json')) continue
    const key = f.slice(0, -'.meta.json'.length)
    const meta = await readMeta(key)
    if (meta && typeof meta.size === 'number' && meta.file) {
      metas.push({
        key,
        file: meta.file,
        size: meta.size,
        lastAccess: meta.lastAccess || meta.cachedAt || 0
      })
    }
  }
  metas.sort((a, b) => a.lastAccess - b.lastAccess) // oldest first

  const target = Math.floor(MAX_CACHE_BYTES / 2) // 目标容量：一半
  for (const m of metas) {
    if (total <= target) break
    try {
      await fsp.rm(m.file, { force: true })
    } catch (e) {
      console.warn('[evict] failed to remove data', m.file, e)
    }
    try {
      await fsp.rm(metaPathByKey(m.key), { force: true })
    } catch (e) {
      console.warn('[evict] failed to remove meta', m.key, e)
    }
    total -= m.size
    console.log(`[evict] ${m.key} freed ${m.size} bytes; total≈${total}`)
  }
}

// ---- Origin client --------------------------------------------------------
const origin = new Client(new URL(ORIGIN_BASE).origin, { pipelining: 0 }) as Dispatcher

async function originRequest(
  pathname: string,
  opts: { method: string; headers?: Record<string, string> }
) {
  const url = `${ORIGIN_BASE}${pathname}`
  return undiciRequest(url, {
    method: opts.method as any,
    headers: opts.headers,
    dispatcher: origin
  })
}

// ---- Download de-dupe -----------------------------------------------------
const inflight = new Map<string, Promise<void>>()

/**
 * 下载完整文件到本地并写 meta（后台落盘；常用于首次 Range）
 */
async function downloadAndCache(upstreamPath: string): Promise<void> {
  const url = `${ORIGIN_BASE}${upstreamPath}`
  const key = keyFor(url)
  if (inflight.has(key)) return inflight.get(key)!

  const p = (async () => {
    const { statusCode, headers, body } = await originRequest(upstreamPath, {
      method: 'GET',
      headers: {}
    })

    if (statusCode !== 200) {
      try {
        await pipe(Readable.from(body), new PassThrough())
      } catch {}
      throw Object.assign(new Error(`Origin responded ${statusCode}`), { statusCode })
    }

    const dataFile = toLocalFilePath(upstreamPath)
    await fsp.mkdir(path.dirname(dataFile), { recursive: true })
    const tmp = path.join(path.dirname(dataFile), `.${path.basename(dataFile)}.tmp-${Date.now()}`)
    const out = fs.createWriteStream(tmp)

    await pipe(Readable.from(body), out)

    const st = await fsp.stat(tmp)
    await fsp.rename(tmp, dataFile)

    const meta: Meta = {
      url,
      file: dataFile,
      status: 200,
      headers: pickHeadersForCache(headers as any),
      size: st.size,
      etag: headerSingle((headers as any)['etag']),
      lastModified: headerSingle((headers as any)['last-modified']),
      cachedAt: Date.now(),
      lastAccess: Date.now()
    }
    await writeMeta(key, meta)

    await enforceBudget()
  })().finally(() => inflight.delete(key))

  inflight.set(key, p)
  return p
}

// ---- Revalidate -----------------------------------------------------------
async function shouldRevalidate(meta: Meta): Promise<boolean> {
  if (CACHE_TTL_SECONDS <= 0) return false
  const age = (Date.now() - meta.cachedAt) / 1000
  return age >= CACHE_TTL_SECONDS
}

async function tryRevalidateAndMaybeRefresh(
  meta: Meta,
  upstreamPath: string,
  key: string
): Promise<boolean> {
  try {
    const headers: Record<string, string> = {}
    if (meta.etag) headers['if-none-match'] = meta.etag
    if (meta.lastModified) headers['if-modified-since'] = meta.lastModified

    const {
      statusCode,
      headers: h,
      body
    } = await originRequest(upstreamPath, { method: 'GET', headers })

    if (statusCode === 304) {
      meta.cachedAt = Date.now()
      meta.lastAccess = Date.now()
      await writeMeta(key, meta)
      try {
        await pipe(Readable.from(body), new PassThrough())
      } catch {}
      return true
    }

    if (statusCode === 200) {
      const dataFile = toLocalFilePath(upstreamPath)
      await fsp.mkdir(path.dirname(dataFile), { recursive: true })
      const tmp = path.join(path.dirname(dataFile), `.${path.basename(dataFile)}.tmp-${Date.now()}`)
      const out = fs.createWriteStream(tmp)
      await pipe(Readable.from(body), out)
      const st = await fsp.stat(tmp)
      await fsp.rename(tmp, dataFile)

      const newMeta: Meta = {
        url: meta.url,
        file: dataFile,
        status: 200,
        headers: pickHeadersForCache(h as any),
        size: st.size,
        etag: headerSingle((h as any)['etag']),
        lastModified: headerSingle((h as any)['last-modified']),
        cachedAt: Date.now(),
        lastAccess: Date.now()
      }
      await writeMeta(key, newMeta)
      await enforceBudget()
      return true
    }

    try {
      await pipe(Readable.from(body), new PassThrough())
    } catch {}
    return false
  } catch (e) {
    console.warn('[revalidate] failed', e)
    return false
  }
}

// ---- Proxy helpers --------------------------------------------------------
function setReplayHeaders(res: http.ServerResponse, meta: Meta) {
  res.setHeader('content-length', String(meta.size))
  for (const [k, v] of Object.entries(meta.headers)) res.setHeader(k, v)
  res.setHeader('x-cache', 'node') // 本地命中
}

async function updateLastAccess(key: string, meta: Meta) {
  meta.lastAccess = Date.now()
  await writeMeta(key, meta)
}

async function proxyToOrigin(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  upstreamPath: string,
  headOnly: boolean
) {
  const headers: Record<string, string> = {}
  if (req.headers['range']) headers['range'] = String(req.headers['range'])
  if (req.headers['if-none-match']) headers['if-none-match'] = String(req.headers['if-none-match'])
  if (req.headers['if-modified-since'])
    headers['if-modified-since'] = String(req.headers['if-modified-since'])

  const {
    statusCode,
    headers: h,
    body
  } = await originRequest(upstreamPath, { method: headOnly ? 'HEAD' : 'GET', headers })
  res.statusCode = statusCode
  for (const [hk, hv] of Object.entries(h)) if (typeof hv === 'string') res.setHeader(hk, hv)
  res.setHeader('x-cache', '0') // 未命中直透
  if (headOnly) return void res.end()
  Readable.from(body).pipe(res)
}

/**
 * 首次普通 GET：边回边缓存（并发 de-dupe）
 */
async function proxyFullAndCache(res: http.ServerResponse, upstreamPath: string, key: string) {
  const url = `${ORIGIN_BASE}${upstreamPath}`
  if (inflight.has(key)) {
    // 已有下载在进行：等完成后按命中回放
    await inflight.get(key)!
    const meta = await readMeta(key)
    if (!meta) return void send(res, 500, 'Cache failed')
    setReplayHeaders(res, meta)
    res.statusCode = 200
    return fs.createReadStream(meta.file).pipe(res)
  }

  const p = (async () => {
    const {
      statusCode,
      headers: h,
      body
    } = await originRequest(upstreamPath, { method: 'GET', headers: {} })

    // 上游头转发（覆盖 x-cache）
    res.statusCode = statusCode
    for (const [hk, hv] of Object.entries(h)) if (typeof hv === 'string') res.setHeader(hk, hv)
    res.setHeader('x-cache', '0') // 首次查源站

    if (statusCode !== 200) {
      return Readable.from(body).pipe(res) // 非 200 不缓存
    }

    const dataFile = toLocalFilePath(upstreamPath)
    await fsp.mkdir(path.dirname(dataFile), { recursive: true })
    const tmp = path.join(path.dirname(dataFile), `.${path.basename(dataFile)}.tmp-${Date.now()}`)
    const out = fs.createWriteStream(tmp)

    const tee = new PassThrough()
    const pump = pipe(Readable.from(body), tee)
    tee.pipe(res)
    const writer = pipe(tee, out)

    await Promise.all([pump, writer])

    const st = await fsp.stat(tmp)
    await fsp.rename(tmp, dataFile)

    const meta: Meta = {
      url,
      file: dataFile,
      status: 200,
      headers: pickHeadersForCache(h as any),
      size: st.size,
      etag: headerSingle((h as any)['etag']),
      lastModified: headerSingle((h as any)['last-modified']),
      cachedAt: Date.now(),
      lastAccess: Date.now()
    }
    await writeMeta(key, meta)
    await enforceBudget()
  })().finally(() => inflight.delete(key))

  inflight.set(key, p)
  return p
}

function parseRange(h: string, size: number): Array<{ start: number; end: number }> | null {
  const m = h.match(/^bytes=(.*)$/)
  if (!m) return null
  const parts = m[1]
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
  const ranges: Array<{ start: number; end: number }> = []
  for (const p of parts) {
    const [a, b] = p.split('-')
    let start = a === '' ? NaN : Number(a)
    let end = b === '' ? NaN : Number(b)
    if (Number.isNaN(start)) {
      const n = Number(b)
      if (!Number.isFinite(n)) return null
      if (n <= 0) continue
      start = Math.max(0, size - n)
      end = size - 1
    } else if (Number.isNaN(end)) {
      end = size - 1
    }
    if (start > end || start < 0 || end >= size) return null
    ranges.push({ start, end })
  }
  return ranges
}

async function serveRangeFromDisk(
  res: http.ServerResponse,
  file: string,
  meta: Meta,
  rangeHeader: string
) {
  const size = meta.size
  const ranges = parseRange(rangeHeader, size)
  if (!ranges || ranges.length === 0) {
    res.statusCode = 416
    res.setHeader('content-range', `bytes */${size}`)
    return void res.end()
  }
  const r = ranges[0]
  const chunkSize = r.end - r.start + 1

  res.statusCode = 206
  for (const [k, v] of Object.entries(meta.headers)) res.setHeader(k, v)
  res.setHeader('content-length', String(chunkSize))
  res.setHeader('content-range', `bytes ${r.start}-${r.end}/${size}`)
  res.setHeader('accept-ranges', 'bytes')
  res.setHeader('x-cache', 'node') // Range 命中

  const rs = fs.createReadStream(file, { start: r.start, end: r.end })
  rs.pipe(res)
}

// 首次 Range：边下边回（不阻塞），并后台下载全量落盘
async function proxyRangeAndCache(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  upstreamPath: string,
  key: string,
  downloadPromise: Promise<void>
) {
  try {
    const headers: Record<string, string> = {}
    if (req.headers.range) headers.range = String(req.headers.range)

    const {
      statusCode,
      headers: originHeaders,
      body
    } = await originRequest(upstreamPath, { method: 'GET', headers })

    res.statusCode = statusCode
    for (const [k, v] of Object.entries(originHeaders))
      if (typeof v === 'string') res.setHeader(k, v)
    res.setHeader('x-cache', '0') // 首次走源站

    const tee = new PassThrough()
    pipe(Readable.from(body), tee).catch(console.error)
    tee.pipe(res)

    downloadPromise.catch(console.error)
  } catch (e) {
    console.error('[proxyRangeAndCache]', e)
    return void send(res, 502, 'Bad Gateway')
  }
}

function send(res: http.ServerResponse, code: number, text: string) {
  res.statusCode = code
  res.setHeader('content-type', 'text/plain; charset=utf-8')
  res.end(text)
}

// ---- Server ---------------------------------------------------------------
const server = http.createServer(async (req, res) => {
  try {
    if (!req.url) return void send(res, 400, 'Bad Request')

    const u = new URL(req.url, `http://localhost:${PORT}`)
    const upstreamPath = u.pathname // 例如 /proxy/a/b/c.mp4
    const fullOriginUrl = `${ORIGIN_BASE}${upstreamPath}`
    const key = keyFor(fullOriginUrl)
    const dataFile = toLocalFilePath(upstreamPath) // .cache/files/proxy/a/b/c.mp4
    const rangeHeader = req.headers['range'] as string | undefined
    const hasRange = !!rangeHeader && /^bytes=\d*-\d*(,\d*-\d*)*$/.test(rangeHeader)

    // HEAD → 如果本地存在，用 meta 回放；否则透传到源站
    if (req.method === 'HEAD') {
      if (await fileExists(dataFile)) {
        const meta = await readMeta(key)
        if (!meta) return void send(res, 500, 'Corrupt cache')
        await updateLastAccess(key, meta)
        setReplayHeaders(res, meta) // x-cache: node
        res.statusCode = 200
        return void res.end()
      }
      return proxyToOrigin(req, res, upstreamPath, true) // x-cache: 0
    }

    // 已缓存
    if (await fileExists(dataFile)) {
      const meta = await readMeta(key)
      if (!meta) {
        await fsp.rm(dataFile, { force: true }).catch(() => {})
        return void send(res, 500, 'Corrupt cache')
      }

      // TTL / Revalidation（可选）
      if (await shouldRevalidate(meta)) {
        await tryRevalidateAndMaybeRefresh(meta, upstreamPath, key)
      }

      const freshMeta = (await readMeta(key)) ?? meta
      await updateLastAccess(key, freshMeta)

      if (hasRange) {
        return serveRangeFromDisk(res, freshMeta.file, freshMeta, rangeHeader!)
      }

      // 全量回放（命中）
      setReplayHeaders(res, freshMeta) // x-cache: node
      res.statusCode = 200
      return fs.createReadStream(freshMeta.file).pipe(res)
    }

    // 未缓存
    if (hasRange) {
      // 首次 Range：开后台全量下载；当前请求边下边回
      const downloadPromise = downloadAndCache(upstreamPath)
      return proxyRangeAndCache(req, res, upstreamPath, key, downloadPromise)
    }

    // 首次普通 GET：边回边缓存（不等待）
    await proxyFullAndCache(res, upstreamPath, key)
    return
  } catch (e) {
    console.error(e)
    return void send(res, 404, '404')
  }
})

server.listen(PORT, () => {
  console.log(`R2 Edge Cache listening on http://localhost:${PORT}`)
})
