/* 盯盘神经（场外基金）· 网页版数据桥（Qoder Sites Edge Function / Deno）
 * 与本地 server.js 同源同口径：全部数据来自公开真实接口，禁止模拟/插值。
 * 边缘运行时无法落盘：缓存改为模块级内存 Map（TTL 与本地一致）。
 * 类目规模聚合使用季报披露快照 scale_snapshot.json（真实东财数据汇总，界面标注汇总日期）。
 */
import snapRaw from './scale_snapshot.json' with { type: 'json' };

const SNAP = snapRaw as { generatedAt: string; source: string; dates: string[]; rows: [string, number, number][]; missing: number };
const SCALE = new Map<string, { yi: number; date: string }>();
for (const r of SNAP.rows) SCALE.set(r[0], { yi: r[1], date: SNAP.dates[r[2]] || '' });

const TTL: Record<string, number> = {
  rank: 30 * 60e3,
  search: 7 * 86400e3,
  hold: 12 * 3600e3,
  basic: 7 * 86400e3,
  pz: 12 * 3600e3,
  kl: 20 * 60e3,
  q: 60e3,
  agg: 3600e3,
  fee: 7 * 86400e3
};

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36';
const MOB_UA = 'Mozilla/5.0';
const TX_K_HOSTS = ['https://web.ifzq.gtimg.cn', 'https://proxy.finance.qq.com'];
const MOB = 'https://fundmobapi.eastmoney.com/FundMNewApi';
const MOB_Q = 'deviceid=Wap&plat=Wap&product=EFund&version=2.0.0';

function bjDate(ms: number) { return new Date(ms + 8 * 3600e3).toISOString().slice(0, 10); }
function bjNow() { return new Date(Date.now() + 8 * 3600e3); }
function tradingNow() {
  const n = bjNow(), wd = n.getUTCDay(), m = n.getUTCHours() * 60 + n.getUTCMinutes();
  return wd >= 1 && wd <= 5 && m >= 555 && m <= 905;
}
function num(v: unknown) { const x = parseFloat(String(v)); return Number.isFinite(x) ? x : null; }
function hash(s: string) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; }
  return ('0000000' + h.toString(16)).slice(-8);
}
function log(...a: unknown[]) { console.log('[dpsj]', ...a); }

/* 并发闸：最多 6 个出站请求 */
const gate = { active: 0, queue: [] as (() => void)[], max: 6 };
function acquire(): Promise<void> {
  return new Promise(res => {
    if (gate.active < gate.max) { gate.active++; res(); }
    else gate.queue.push(res);
  });
}
function release() {
  const next = gate.queue.shift();
  if (next) next(); else gate.active--;
}

async function fetchBuf(url: string, headers: Record<string, string> = {}, timeout = 9000): Promise<Uint8Array> {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeout);
  try {
    const r = await fetch(url, { headers: Object.assign({ 'User-Agent': UA, 'Accept': '*/*' }, headers), signal: ctl.signal, redirect: 'follow' });
    if (!r.ok) { await r.body?.cancel(); throw new Error('HTTP ' + r.status + ' ' + url.slice(0, 90)); }
    return new Uint8Array(await r.arrayBuffer());
  } finally { clearTimeout(timer); }
}
const utf8 = (b: Uint8Array) => new TextDecoder('utf-8').decode(b);
async function fetchRetry(urls: string[], headers: Record<string, string> = {}, tries = 3, timeout = 9000): Promise<Uint8Array> {
  let lastErr: unknown;
  for (let i = 0; i < tries; i++) {
    const url = urls[i % urls.length];
    await acquire();
    try { const b = await fetchBuf(url, headers, timeout); release(); return b; }
    catch (e) { release(); lastErr = e; await new Promise(r => setTimeout(r, 200 * (i + 1))); }
  }
  throw lastErr || new Error('fetch failed');
}
async function mobJson(url: string) {
  let lastErr: unknown;
  for (let i = 0; i < 3; i++) {
    try {
      const j = JSON.parse(utf8(await fetchRetry([url], { 'User-Agent': MOB_UA }, 2, 9000)));
      if (j && j.ErrCode === 0) return j;
      lastErr = new Error((j && (j.ErrMsg || j.ErrorMessage)) || '移动接口业务错误');
    } catch (e) { lastErr = e; }
    await new Promise(r => setTimeout(r, 300 * (i + 1)));
  }
  throw lastErr;
}

/* inflight 去重 */
const inflight = new Map<string, Promise<unknown>>();
function dedup<T>(key: string, fn: () => Promise<T>): Promise<T> {
  const hit = inflight.get(key);
  if (hit) return hit as Promise<T>;
  const p = fn().finally(() => inflight.delete(key));
  inflight.set(key, p as Promise<unknown>);
  return p;
}

/* 内存缓存（边缘无磁盘）；超过上限按插入顺序淘汰，避免无界增长 */
const MEM = new Map<string, { at: number; data: unknown }>();
const MEM_MAX = 6000;
function readCache<T>(name: string, ttl: number, fresh?: boolean): T | null {
  if (fresh) return null;
  const hit = MEM.get(name);
  if (hit && Date.now() - hit.at < ttl) return hit.data as T;
  return null;
}
function writeCache(name: string, data: unknown) {
  if (MEM.size >= MEM_MAX) {
    let n = 0;
    for (const k of MEM.keys()) { MEM.delete(k); if (++n >= 1000) break; }
  }
  MEM.set(name, { at: Date.now(), data });
}

/* ---------- 基金全集 ---------- */
const THEMES: [string, string, RegExp][] = [
  ['hk', '港股', /港股|恒生|H股|香港|沪港深|港股通/],
  ['us', '美股', /纳指|纳斯达克|标普|道琼斯|美国|美元|北美/],
  ['kc', '科创', /科创|科技创新/],
  ['cy', '创业板', /创业板|创成长|创蓝筹/],
  ['hs300', '沪深300', /沪深300|300增强|300指数|300ETF/],
  ['zz500', '中证500/1000', /中证500|500增强|中证1000|1000增强|中证2000|国证2000|中证A500|A500/],
  ['div', '红利低波', /红利|股息|低波/],
  ['med', '医药医疗', /医药|医疗|健康|生物|创新药|疫苗|中药/],
  ['cons', '消费白酒', /消费|白酒|食品|饮料|内需|零售|品牌/],
  ['ne', '新能源光伏', /新能源|光伏|碳中和|电池|锂|风电|储能|绿色|环保/],
  ['semi', '半导体芯片', /半导体|芯片|集成电路/],
  ['tech', '科技AI', /科技|人工智能|算力|云计算|大数据|软件|计算机|通信|5G|互联网|数字经济|机器人|智能|电子|信息/],
  ['mil', '军工', /军工|国防|航空航天|船舶/],
  ['fin', '金融地产', /金融|银行|证券|保险|地产|房地产|非银/],
  ['res', '资源周期', /有色|煤炭|钢铁|石油|化工|资源|材料|黄金|贵金属|矿业|农业|养殖|畜牧/],
  ['mfg', '高端制造', /制造|机械|装备|汽车|工业/],
  ['global', '全球海外', /全球|环球|海外|亚太|欧洲|日本|德国|法国|印度|越南|沙特|东南亚|新兴市场/]
];
const TYPE_KEYS: [string, string][] = [['gp', '股票型'], ['hh', '混合型'], ['zs', '指数型'], ['qdii', 'QDII'], ['zq', '债券型'], ['fof', 'FOF'], ['other', '其他']];
function typeKey(t: string) {
  if (!t) return 'other';
  if (t.startsWith('股票')) return 'gp';
  if (t.startsWith('混合')) return 'hh';
  if (t.startsWith('指数') || t.includes('股票指数')) return 'zs';
  if (t.startsWith('QDII')) return 'qdii';
  if (t.startsWith('债券')) return 'zq';
  if (t.startsWith('FOF') || t.includes('FOF')) return 'fof';
  return 'other';
}
function themesOf(name: string) {
  const out: string[] = [];
  for (const [k, , re] of THEMES) if (re.test(name)) out.push(k);
  return out;
}

type Fund = {
  code: string; name: string; pinyin: string; navDate: string;
  nav: number | null; accNav: number | null; chg: number | null;
  w1: number | null; m1: number | null; m3: number | null; m6: number | null;
  y1: number | null; y2: number | null; y3: number | null; ytd: number | null;
  type: string; tk: string; themes: string[];
};
type Univ = { funds: Fund[]; byCode: Map<string, Fund>; navDate: string; builtAt: number };
let UNIV: Univ | null = null;

async function loadRank(fresh?: boolean): Promise<unknown[]> {
  const c = readCache<unknown[]>('rank', TTL.rank, fresh);
  if (c) return c;
  return dedup('rank', async () => {
    const url = 'https://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&rs=&gs=0&sc=zzf&st=desc&pi=1&pn=30000&dx=0';
    const txt = utf8(await fetchRetry([url], { Referer: 'https://fund.eastmoney.com/data/fundranking.html' }, 3, 20000));
    const m = txt.match(/datas:\[([\s\S]*?)\],allRecords/);
    if (!m) throw new Error('排行接口返回格式异常');
    const rows: unknown[] = [];
    for (const mm of m[1].matchAll(/"([^"]*)"/g)) {
      const f = mm[1].split(',');
      if (f.length < 17 || !/^\d{6}$/.test(f[0])) continue;
      rows.push({
        code: f[0], name: f[1], pinyin: f[2], navDate: f[3],
        nav: num(f[4]), accNav: num(f[5]), chg: num(f[6]),
        w1: num(f[7]), m1: num(f[8]), m3: num(f[9]), m6: num(f[10]),
        y1: num(f[11]), y2: num(f[12]), y3: num(f[13]), ytd: num(f[14])
      });
    }
    if (!rows.length) throw new Error('排行接口未解析到基金数据');
    writeCache('rank', rows);
    log('rank loaded:', rows.length);
    return rows;
  });
}
async function loadSearch(fresh?: boolean): Promise<Record<string, string>> {
  const c = readCache<Record<string, string>>('search', TTL.search, fresh);
  if (c) return c;
  return dedup('search', async () => {
    const url = 'https://fund.eastmoney.com/js/fundcode_search.js';
    let txt = utf8(await fetchRetry([url], { Referer: 'https://fund.eastmoney.com/' }, 3, 15000));
    txt = txt.replace(/^\uFEFF/, '').replace(/^var\s+r\s*=\s*/, '').replace(/;\s*$/, '');
    const arr = JSON.parse(txt);
    const map: Record<string, string> = {};
    for (const it of arr) if (it && it[0] && !map[it[0]]) map[it[0]] = it[3] || '';
    writeCache('search', map);
    log('search loaded:', Object.keys(map).length);
    return map;
  });
}
async function ensureUniverse(fresh?: boolean): Promise<Univ> {
  if (UNIV && !fresh && Date.now() - UNIV.builtAt < TTL.rank) return UNIV;
  const [rank, types] = await Promise.all([loadRank(fresh), loadSearch(fresh)]);
  const funds = (rank as Record<string, unknown>[]).map(r => {
    const type = types[String(r.code)] || '';
    return Object.assign({}, r, { type, tk: typeKey(type), themes: themesOf(String(r.name)) }) as Fund;
  });
  const byCode = new Map(funds.map(f => [f.code, f]));
  let navDate = '';
  for (const f of funds) if (f.navDate > navDate) navDate = f.navDate;
  UNIV = { funds, byCode, navDate, builtAt: Date.now() };
  return UNIV;
}

/* ---------- 持仓 ---------- */
function symsOf(gpdm: unknown, newtexch: unknown) {
  const code = String(gpdm || '').trim();
  const tx = String(newtexch || '');
  if (tx === '116') { const h = code.padStart(5, '0'); return { qsym: 'hk' + h, ksym: 'hk' + h, mkt: 'HK' }; }
  if (tx === '105' || tx === '106' || tx === '107') {
    const suf = tx === '105' ? '.OQ' : tx === '106' ? '.N' : '.A';
    return { qsym: 'us' + code, ksym: 'us' + code + suf, mkt: 'US' };
  }
  const c0 = code[0];
  const pre = (c0 === '6' || c0 === '9') ? 'sh' : (c0 === '4' || c0 === '8' || code.startsWith('92')) ? 'bj' : 'sz';
  return { qsym: pre + code, ksym: pre + code, mkt: 'A' };
}
type Stock = { code: string; name: string; w: number; industry: string; qsym: string; ksym: string; mkt: string };
type Hold = { date: string; stocks: Stock[]; coverage: number; method: string; via: string | null; reason: string | null; error?: string };
async function fetchHoldRaw(code: string) {
  const url = MOB + '/FundMNInverstPosition?FCODE=' + code + '&' + MOB_Q;
  const j = await mobJson(url);
  if (!j.Datas) throw new Error('持仓接口无数据 ' + code);
  j.Datas.Expansion = j.Expansion || '';
  return j.Datas;
}
async function getHoldings(code: string, fresh?: boolean): Promise<Hold> {
  const c = readCache<Hold>('hold_' + code, TTL.hold, fresh);
  if (c) return c;
  return dedup('hold_' + code, async () => {
    let d = await fetchHoldRaw(code);
    const orig = d;
    let method = '直接持仓', via: string | null = null;
    if ((!d.fundStocks || !d.fundStocks.length) && d.ETFCODE) {
      via = d.ETFCODE;
      try { d = await fetchHoldRaw(d.ETFCODE); method = '穿透ETF(' + via + ')'; } catch (_e) { d = { fundStocks: [] }; }
    }
    const stocks: Stock[] = [];
    for (const s of (d.fundStocks || [])) {
      const w = num(s.JZBL);
      if (w === null || w <= 0) continue;
      const sy = symsOf(s.GPDM, s.NEWTEXCH);
      stocks.push(Object.assign({ code: s.GPDM, name: s.GPJC, w, industry: s.INDEXNAME === '--' ? '' : (s.INDEXNAME || '') }, sy));
    }
    let reason: string | null = null;
    if (!stocks.length) {
      reason = (orig.fundfofs && orig.fundfofs.length) ? 'FOF持仓为基金，持仓法暂不适用'
        : (orig.fundboods && orig.fundboods.length) ? '最新季报以债券持仓为主，持仓法不适用'
          : '最新季报未披露股票持仓，无法用持仓法估算';
    }
    const cov = stocks.reduce((a, s) => a + s.w, 0);
    const out: Hold = { date: d.Expansion || '', stocks, coverage: Math.round(cov * 100) / 100, method, via, reason };
    writeCache('hold_' + code, out);
    return out;
  });
}

/* ---------- 基础信息 ---------- */
type Basic = { endNav: number | null; scaleDate: string; company: string; manager: string; ftype: string; risk: string; estab: string; buy: number | null; buyOrig: number | null };
async function getBasic(code: string, fresh?: boolean): Promise<Basic> {
  const c = readCache<Basic>('basic_' + code, TTL.basic, fresh);
  if (c) return c;
  return dedup('basic_' + code, async () => {
    const url = MOB + '/FundMNBasicInformation?FCODE=' + code + '&' + MOB_Q;
    const j = await mobJson(url);
    const d = (j && j.Datas) || {};
    const out: Basic = {
      endNav: num(d.ENDNAV), scaleDate: d.FEGMRQ && d.FEGMRQ !== '--' ? d.FEGMRQ : '',
      company: d.JJGS || '', manager: d.JJJL || '', ftype: d.FTYPE || '',
      risk: d.RISKLEVEL || '', estab: d.ESTABDATE || '',
      buy: num(String(d.RATE || '').replace('%', '')), buyOrig: num(String(d.SOURCERATE || '').replace('%', ''))
    };
    writeCache('basic_' + code, out);
    return out;
  });
}

/* ---------- 费率 ---------- */
type Fee = { buy: number | null; buyOrig: number | null; buyTier: string; mgmt: number | null; custody: number | null; service: number | null };
function parseFeePage(h: string): Fee {
  const g = (re: RegExp) => { const m = h.match(re); return m ? parseFloat(m[1]) : null; };
  const out: Fee = {
    buy: null, buyOrig: null, buyTier: '',
    mgmt: g(/管理费率<\/td><td[^>]*>([\d.]+)%/),
    custody: g(/托管费率<\/td><td[^>]*>([\d.]+)%/),
    service: g(/销售服务费率<\/td><td[^>]*>([\d.]+)%/)
  };
  const sec = h.split('申购费率')[1];
  if (sec) {
    const m = sec.match(/<tbody><tr><td[^>]*>([^<]*)<\/td><td[^>]*>([\s\S]*?)<\/td>/);
    if (m) {
      out.buyTier = m[1];
      const so = m[2].match(/<strike[^>]*>([\d.]+)%/);
      const pcts = [...m[2].matchAll(/([\d.]+)%/g)].map(x => parseFloat(x[1]));
      if (so) { out.buyOrig = parseFloat(so[1]); out.buy = pcts.length ? pcts[pcts.length - 1] : null; }
      else if (pcts.length) { out.buy = pcts[0]; out.buyOrig = pcts[0]; }
    }
  }
  return out;
}
async function getFee(code: string, fresh?: boolean): Promise<Fee> {
  const c = readCache<Fee>('fee_' + code, TTL.fee, fresh);
  if (c) return c;
  return dedup('fee_' + code, async () => {
    const url = 'https://fundf10.eastmoney.com/jjfl_' + code + '.html';
    const h = utf8(await fetchRetry([url], { Referer: 'https://fundf10.eastmoney.com/' }, 3, 12000));
    if (!h.includes('管理费率')) throw new Error('费率页结构异常 ' + code);
    const out = parseFeePage(h);
    writeCache('fee_' + code, out);
    return out;
  });
}

/* ---------- pingzhongdata ---------- */
function extractJsVar(txt: string, name: string): any {
  let idx = txt.indexOf(name + ' =');
  if (idx < 0) idx = txt.indexOf(name + '=');
  if (idx < 0) return null;
  let start = -1;
  for (let i = idx + name.length; i < txt.length; i++) {
    const ch = txt[i];
    if (ch === '[' || ch === '{') { start = i; break; }
    if (ch === ';' || ch === '\n') return null;
  }
  if (start < 0) return null;
  const open = txt[start], close = open === '[' ? ']' : '}';
  let depth = 0, inStr = false, esc = false;
  for (let i = start; i < txt.length; i++) {
    const ch = txt[i];
    if (esc) { esc = false; continue; }
    if (ch === '\\') { esc = true; continue; }
    if (inStr) { if (ch === '"') inStr = false; continue; }
    if (ch === '"') { inStr = true; continue; }
    if (ch === open) depth++;
    else if (ch === close) { depth--; if (depth === 0) { try { return JSON.parse(txt.slice(start, i + 1)); } catch (_e) { return null; } } }
  }
  return null;
}
type Pz = { nav: { d: string; nav: number | null; ret: number | null }[]; scale: { dates: string[]; values: (number | null)[] } | null; stockRatio: number | null; ratioDate: string };
async function getPz(code: string, fresh?: boolean): Promise<Pz> {
  const c = readCache<Pz>('pz_' + code, TTL.pz, fresh);
  if (c) return c;
  return dedup('pz_' + code, async () => {
    const url = 'https://fund.eastmoney.com/pingzhongdata/' + code + '.js';
    const txt = utf8(await fetchRetry([url], { Referer: 'https://fund.eastmoney.com/' + code + '.html' }, 3, 20000));
    const trend = extractJsVar(txt, 'Data_netWorthTrend') || [];
    const nav = trend.map((p: any) => ({ d: bjDate(p.x), nav: num(p.y), ret: num(p.equityReturn) })).filter((p: any) => p.d && p.nav !== null);
    const fs = extractJsVar(txt, 'Data_fluctuationScale');
    let scale: Pz['scale'] = null;
    if (fs && fs.categories && Array.isArray(fs.series)) {
      const vals = (fs.series[0] && Array.isArray(fs.series[0].data)) ? fs.series[0].data : fs.series;
      scale = { dates: fs.categories, values: vals.map((v: any) => (v && typeof v === 'object') ? num(v.y) : num(v)) };
    }
    const aa = extractJsVar(txt, 'Data_assetAllocation');
    let stockRatio: number | null = null, ratioDate = '';
    if (aa && aa.series) {
      const s = aa.series.find((x: any) => x.name === '股票占净比');
      if (s && s.data && s.data.length) {
        stockRatio = num(s.data[s.data.length - 1]);
        ratioDate = (aa.categories && aa.categories[aa.categories.length - 1]) || '';
      }
    }
    if (!nav.length) throw new Error('净值历史解析失败 ' + code);
    const out: Pz = { nav, scale, stockRatio, ratioDate };
    writeCache('pz_' + code, out);
    return out;
  });
}

/* ---------- 批量实时行情（腾讯，GBK） ---------- */
type Quote = { name: string; price: number | null; prev: number | null; pct: number | null; time: string };
async function getQuotes(syms: string[], fresh?: boolean): Promise<Record<string, Quote>> {
  const uniq = [...new Set(syms)].sort();
  if (!uniq.length) return {};
  const key = 'q_' + hash(uniq.join(','));
  const c = readCache<Record<string, Quote>>(key, TTL.q, fresh);
  if (c) return c;
  return dedup(key, async () => {
    const out: Record<string, Quote> = {};
    for (let i = 0; i < uniq.length; i += 60) {
      const batch = uniq.slice(i, i + 60);
      try {
        const buf = await fetchRetry(['https://qt.gtimg.cn/q=' + batch.join(',')], {}, 2, 8000);
        const txt = new TextDecoder('gbk').decode(buf);
        for (const m of txt.matchAll(/v_([\w.]+)="([^"]*)"/g)) {
          const f = m[2].split('~');
          if (f.length < 33) continue;
          out[m[1]] = { name: f[1], price: num(f[3]), prev: num(f[4]), pct: num(f[32]), time: f[30] || '' };
        }
      } catch (e) { log('quote batch fail:', (e as Error).message); }
    }
    writeCache(key, out);
    return out;
  });
}

/* ---------- 个股日K ---------- */
type Kl = { bars: { d: string; c: number }[]; rets: Record<string, number>; last: string };
function barsToRets(bars: { d: string; c: number }[]) {
  const rets: Record<string, number> = {};
  for (let i = 1; i < bars.length; i++) {
    if (bars[i - 1].c > 0) rets[bars[i].d] = (bars[i].c / bars[i - 1].c - 1) * 100;
  }
  return rets;
}
function parseTxK(j: any, ksym: string) {
  const data = j && j.data;
  if (!data) return null;
  const node = data[ksym] || data[Object.keys(data)[0]];
  if (!node) return null;
  const rows = node.qfqday || node.day;
  if (!Array.isArray(rows) || !rows.length) return null;
  const bars: { d: string; c: number }[] = [];
  for (const r of rows) {
    const d = String(r[0]).slice(0, 10), c = num(r[2]);
    if (d && c !== null) bars.push({ d, c });
  }
  return bars.length ? bars : null;
}
async function getKline(ksym: string, fresh?: boolean): Promise<Kl> {
  const c = readCache<Kl>('kl_' + ksym, TTL.kl, fresh);
  if (c) return c;
  return dedup('kl_' + ksym, async () => {
    let candidates = [ksym];
    if (ksym.startsWith('us')) {
      const base = ksym.replace(/\.(OQ|N|A)$/, '');
      candidates = [...new Set([ksym, base + '.OQ', base + '.N', base + '.A', base])];
    }
    let bars: { d: string; c: number }[] | null = null;
    // 通用 fqkline 仅支持A股；港/美股须走专用端点（通用端点返回WAF拦截页）
    const kpath = ksym.startsWith('hk') ? 'hkfqkline' : ksym.startsWith('us') ? 'usfqkline' : 'fqkline';
    const khosts = kpath === 'fqkline' ? TX_K_HOSTS : ['https://web.ifzq.gtimg.cn', 'https://ifzq.gtimg.cn'];
    const deadline = Date.now() + 26000; // 边缘单次请求预算，避免候选轮换耗尽运行时
    for (const sym of candidates) {
      if (Date.now() > deadline) break;
      const urls = khosts.map(h => h + '/appstock/app/' + kpath + '/get?param=' + sym + ',day,,,320,qfq');
      try {
        const j = JSON.parse(utf8(await fetchRetry(urls, {}, 2, 8000)));
        bars = parseTxK(j, sym);
        if (bars) break;
      } catch (_e) { /* 试下一候选 */ }
    }
    if (!bars && /^(sh|sz|bj)\d{6}$/.test(ksym) && Date.now() < deadline) {
      try {
        const url = 'https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketData.getKLineData?symbol=' + ksym + '&scale=240&ma=no&datalen=320';
        const arr = JSON.parse(utf8(await fetchRetry([url], { Referer: 'https://finance.sina.com.cn/' }, 2, 8000)));
        if (Array.isArray(arr) && arr.length) {
          bars = arr.map((r: any) => ({ d: String(r.day).slice(0, 10), c: num(r.close) })).filter((b: any) => b.c !== null);
        }
      } catch (_e) { /* 诚实失败 */ }
    }
    if (!bars || bars.length < 2) throw new Error('K线获取失败 ' + ksym);
    const out: Kl = { bars, rets: barsToRets(bars), last: bars[bars.length - 1].d };
    writeCache('kl_' + ksym, out);
    return out;
  });
}

/* ---------- 今日估算 ---------- */
async function estTodayMany(codes: string[], fresh?: boolean) {
  const u = await ensureUniverse(false);
  const holds = await Promise.all(codes.map(async c => {
    try { return await getHoldings(c, fresh); } catch (e) { return { error: (e as Error).message } as Hold; }
  }));
  const qsyms: string[] = [];
  holds.forEach(h => (h.stocks || []).forEach(s => qsyms.push(s.qsym)));
  const quotes = await getQuotes(qsyms, fresh);
  const out: Record<string, unknown> = {};
  codes.forEach((c, i) => {
    const h = holds[i], f = u.byCode.get(c);
    if (!h || h.error) { out[c] = { est: null, reason: (h && h.error) || '持仓获取失败' }; return; }
    if (h.reason) { out[c] = { est: null, reason: h.reason, holdDate: h.date, method: h.method }; return; }
    let est = 0, cov = 0, time = '';
    for (const s of h.stocks) {
      const q = quotes[s.qsym];
      if (!q || q.pct === null) continue;
      est += s.w * q.pct / 100; cov += s.w;
      if (q.time > time) time = q.time;
    }
    out[c] = {
      est: cov > 0 ? Math.round(est * 1000) / 1000 : null,
      cov: Math.round(cov * 100) / 100, holdCov: h.coverage, holdDate: h.date,
      method: h.method, time, navDate: f ? f.navDate : '',
      reason: cov > 0 ? null : '持仓股当前无实时行情'
    };
  });
  return out;
}

/* ---------- 偏差口径：官方 − 估算 ---------- */
function monthCutoff(d: string) {
  const t = new Date(d + 'T00:00:00Z');
  const day = t.getUTCDate();
  t.setUTCMonth(t.getUTCMonth() - 1);
  if (t.getUTCDate() < day) t.setUTCDate(0);
  return t.toISOString().slice(0, 10);
}
function monthDevOf(series: { d: string; ret: number | null; estRet: number | null }[], lastDate: string) {
  const cut = monthCutoff(lastDate);
  let off = 1, est = 1, n = 0, mStart = '', mEnd = '';
  for (const p of series) {
    if (p.d <= cut || p.d > lastDate) continue;
    if (p.ret === null || p.ret === undefined || p.estRet === null || p.estRet === undefined) continue;
    off *= 1 + p.ret / 100; est *= 1 + p.estRet / 100; n++;
    if (!mStart) mStart = p.d;
    mEnd = p.d;
  }
  if (!n) return { devM: null, nM: 0, reasonM: '近1月无可估算交易日' };
  const offM = Math.round((off - 1) * 100000) / 1000, estM = Math.round((est - 1) * 100000) / 1000;
  return { devM: Math.round((offM - estM) * 1000) / 1000, offM, estM, nM: n, mStart, mEnd, reasonM: null };
}
async function yDevMany(codes: string[], fresh?: boolean) {
  const u = await ensureUniverse(false);
  const holds = await Promise.all(codes.map(async c => {
    try { return await getHoldings(c, fresh); } catch (e) { return { error: (e as Error).message } as Hold; }
  }));
  const ksyms = [...new Set(holds.flatMap(h => (h.stocks || []).map(s => s.ksym)))];
  const klMap: Record<string, Kl | null> = {};
  await Promise.all(ksyms.map(async k => {
    try { klMap[k] = await getKline(k, fresh); } catch (_e) { klMap[k] = null; }
  }));
  const pzs = await Promise.all(codes.map(async c => {
    try { return await getPz(c, fresh); } catch (_e) { return null; }
  }));
  const out: Record<string, any> = {};
  codes.forEach((c, i) => {
    const h = holds[i], f = u.byCode.get(c);
    if (!h || h.error) { out[c] = { dev: null, devM: null, reason: (h && h.error) || '持仓获取失败' }; return; }
    if (h.reason) { out[c] = { dev: null, devM: null, reason: h.reason, holdDate: h.date }; return; }
    if (!f || !f.navDate) { out[c] = { dev: null, devM: null, reason: '无最新净值数据' }; return; }
    const D = f.navDate;
    const estByDate = (d: string) => {
      let est = 0, cov = 0;
      for (const s of h.stocks) {
        const kl = klMap[s.ksym];
        const r = kl && kl.rets[d];
        if (r === undefined || r === null) continue;
        est += s.w * r / 100; cov += s.w;
      }
      return cov > 0 ? { est, cov } : null;
    };
    const eD = estByDate(D);
    const base = { holdDate: h.date, method: h.method, navDate: D };
    if (!eD) {
      out[c] = Object.assign(base, { dev: null, estD: null, official: f.chg, covD: 0, reason: '净值日(' + D + ')持仓股无对应行情' });
    } else {
      const estD = Math.round(eD.est * 1000) / 1000;
      out[c] = Object.assign(base, {
        dev: f.chg !== null ? Math.round((f.chg - estD) * 1000) / 1000 : null,
        estD, official: f.chg, covD: Math.round(eD.cov * 100) / 100,
        reason: f.chg === null ? '官方涨幅缺失' : null
      });
    }
    const pz = pzs[i];
    if (!pz) { out[c].devM = null; out[c].reasonM = '净值历史获取失败'; return; }
    const cut = monthCutoff(D);
    const series = pz.nav.filter(p => p.d > cut && p.d <= D).map(p => {
      const e = estByDate(p.d);
      return { d: p.d, ret: p.ret, estRet: e ? Math.round(e.est * 1000) / 1000 : null };
    });
    Object.assign(out[c], monthDevOf(series, D));
  });
  return out;
}

/* ---------- 单基金详情 ---------- */
async function fundDetail(code: string, daysRaw: string | null, fresh?: boolean) {
  const u = await ensureUniverse(false);
  const f = u.byCode.get(code);
  if (!f) { const e = new Error('未知基金代码 ' + code) as Error & { status?: number }; e.status = 404; throw e; }
  const days = Math.min(Math.max(parseInt(String(daysRaw)) || 120, 20), 250);
  const [basic, pz, hold, fee] = await Promise.all([
    getBasic(code, fresh).catch(() => null),
    getPz(code, fresh),
    getHoldings(code, fresh).catch(e => ({ error: (e as Error).message }) as Hold),
    getFee(code, fresh).catch(() => null)
  ]);
  const navAll = pz.nav;
  const win = navAll.slice(-(days + 1));
  const estArr: ({ ret: number | null; nav: number | null; cov: number } | null)[] = win.map(() => null);
  const klines: Record<string, Kl | null> = {};
  let holdErr: string | null = null;
  if (hold && !hold.error && !hold.reason && hold.stocks.length) {
    await Promise.all(hold.stocks.map(async s => {
      try { klines[s.ksym] = await getKline(s.ksym, fresh); } catch (_e) { klines[s.ksym] = null; }
    }));
    let prevEst: number | null = null;
    for (let i = 0; i < win.length; i++) {
      if (i === 0) { estArr[i] = { ret: null, nav: null, cov: 0 }; prevEst = null; continue; }
      const d = win[i].d;
      let est = 0, cov = 0;
      for (const s of hold.stocks) {
        const kl = klines[s.ksym];
        const r = kl && kl.rets[d];
        if (r === undefined || r === null) continue;
        est += s.w * r / 100; cov += s.w;
      }
      if (cov <= 0) { estArr[i] = { ret: null, nav: null, cov: 0 }; prevEst = null; continue; }
      const ret = Math.round(est * 1000) / 1000;
      const anchor = prevEst !== null ? prevEst : win[i - 1].nav;
      const enav = anchor === null ? null : Math.round(anchor * (1 + ret / 100) * 10000) / 10000;
      estArr[i] = { ret, nav: enav, cov: Math.round(cov * 100) / 100 };
      prevEst = enav;
    }
  } else holdErr = (hold && (hold.error || hold.reason)) || '持仓不可用';
  const quotes = hold && hold.stocks ? await getQuotes(hold.stocks.map(s => s.qsym), fresh).catch(() => ({} as Record<string, Quote>)) : {};
  const holdTable = (hold && hold.stocks || []).map(s => {
    const q = quotes[s.qsym];
    return Object.assign({}, s, { pct: q ? q.pct : null, price: q ? q.price : null, contrib: q && q.pct !== null ? Math.round(s.w * q.pct * 10) / 1000 : null });
  });
  let etEst = 0, etCov = 0, etTime = '';
  for (const s of (hold && hold.stocks || [])) {
    const q = quotes[s.qsym];
    if (!q || q.pct === null) continue;
    etEst += s.w * q.pct / 100; etCov += s.w;
    if (q.time > etTime) etTime = q.time;
  }
  const estToday = etCov > 0 ? { est: Math.round(etEst * 1000) / 1000, cov: Math.round(etCov * 100) / 100, time: etTime } : null;
  const rows = win.map((p, i) => ({
    d: p.d, nav: p.nav, ret: p.ret,
    estRet: estArr[i] ? estArr[i]!.ret : null,
    estNav: estArr[i] ? estArr[i]!.nav : null,
    cov: estArr[i] ? estArr[i]!.cov : 0,
    dev: estArr[i] && estArr[i]!.ret !== null && p.ret !== null ? Math.round((p.ret! - estArr[i]!.ret!) * 1000) / 1000 : null
  }));
  const monthDev = monthDevOf(rows, f.navDate || rows[rows.length - 1].d);
  const devs = rows.slice(1).map(r => r.dev).filter(v => v !== null) as number[];
  const stats = {
    nDays: rows.length - 1, nEst: devs.length,
    meanAbsDev: devs.length ? Math.round(devs.reduce((a, b) => a + Math.abs(b), 0) / devs.length * 1000) / 1000 : null,
    maxAbsDev: devs.length ? Math.round(Math.max(...devs.map(Math.abs)) * 1000) / 1000 : null,
    meanDev: devs.length ? Math.round(devs.reduce((a, b) => a + b, 0) / devs.length * 1000) / 1000 : null
  };
  return {
    code, name: f.name, type: f.type, tk: f.tk, themes: f.themes,
    navDate: f.navDate, nav: f.nav, chg: f.chg, m1: f.m1, m3: f.m3, y1: f.y1,
    basic, fee, scale: pz.scale, stockRatio: pz.stockRatio, ratioDate: pz.ratioDate,
    holdDate: hold && hold.date || '', holdCov: hold && hold.coverage || 0,
    holdMethod: hold && hold.method || '', holdReason: holdErr, estToday,
    holdings: holdTable, rows, stats, monthDev, days
  };
}

/* ---------- 筛选 / 分组（网页版自选存浏览器，分组代码由前端随请求提交） ---------- */
function filterFunds(u: Univ, type: string | null, theme: string | null, q: string | null, gset: Set<string> | null) {
  const qq = (q || '').trim().toLowerCase();
  return u.funds.filter(f =>
    (!gset || gset.has(f.code)) &&
    (!type || type === 'all' || f.tk === type) &&
    (!theme || theme === 'all' || f.themes.includes(theme)) &&
    (!qq || f.code.includes(qq) || f.name.toLowerCase().includes(qq) || (f.pinyin || '').toLowerCase().includes(qq))
  );
}
function groupSetOf(group: string | null, gcodes: unknown): Set<string> | null {
  if (!group || group === 'all') return null;
  const arr = Array.isArray(gcodes) ? gcodes : [];
  return new Set(arr.map(x => String(x)).filter(x => /^\d{6}$/.test(x)));
}

/* ---------- 类目规模：季报快照即时汇总 ---------- */
function scaleAgg(u: Univ, type: string | null, theme: string | null, q: string | null, group: string | null, gcodes: unknown) {
  const list = filterFunds(u, type, theme, q, groupSetOf(group, gcodes));
  let sumYi = 0, missing = 0, latestScaleDate = '';
  for (const f of list) {
    const s = SCALE.get(f.code);
    if (s) { sumYi += s.yi; if (s.date > latestScaleDate) latestScaleDate = s.date; }
    else missing++;
  }
  return {
    key: hash([type || 'all', theme || 'all', (q || '').trim().toLowerCase(), group || 'all', String((gcodes as string[] | undefined)?.length || 0)].join('|')),
    total: list.length, done: list.length, sumYi: Math.round(sumYi * 100) / 100, missing,
    running: false, latestScaleDate, finishedAt: Date.now(),
    type: type || 'all', theme: theme || 'all', q: q || '', group: group || 'all',
    snapshotAt: SNAP.generatedAt.slice(0, 10), source: SNAP.source
  };
}

/* ---------- HTTP ---------- */
function sendJson(obj: unknown, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' }
  });
}
function routePath(p: string) {
  // 网关可能以 /functions/v1/app/<接口> 或 /app/<接口> 挂载；本地预览也可能带 /api 前缀
  let s = p;
  s = s.replace(/^\/functions\/v1\/[^/]+(?=\/|$)/, '');
  s = s.replace(/^\/app(?=\/|$)/, '');
  s = s.replace(/^\/api(?=\/|$)/, '');
  if (!s.startsWith('/')) s = '/' + s;
  return s.replace(/\/+$/, '') || '/';
}
function codesOf(v: string | null, cap: number) {
  return (v || '').split(',').map(s => s.trim()).filter(s => /^\d{6}$/.test(s)).slice(0, cap);
}

export async function handler(req: Request): Promise<Response> {
  const u = new URL(req.url);
  const p = routePath(u.pathname);
  if (req.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Methods': 'GET,POST,OPTIONS' } });
  }
  let body: Record<string, any> = {};
  if (req.method === 'POST') {
    try { body = JSON.parse(await req.text()) || {}; } catch (_e) { body = {}; }
  }
  const sp = u.searchParams;
  const get = (k: string): string | null => (body[k] !== undefined && body[k] !== null ? String(body[k]) : sp.get(k));
  const fresh = get('fresh') === '1';
  try {
    if (p === '/' || p === '/health') return sendJson({ ok: true, mode: 'edge', time: Date.now(), tradingNow: tradingNow(), snapshotAt: SNAP.generatedAt });
    if (p === '/meta') {
      const univ = await ensureUniverse(fresh);
      const types = TYPE_KEYS.map(([k, label]) => ({ key: k, label, count: univ.funds.filter(f => f.tk === k).length }));
      const themes = THEMES.map(([k, label]) => ({ key: k, label, count: univ.funds.filter(f => f.themes.includes(k)).length }));
      return sendJson({ fundCount: univ.funds.length, navDate: univ.navDate, types, themes, serverTime: Date.now(), tradingNow: tradingNow(), builtAt: univ.builtAt });
    }
    if (p === '/funds') {
      const univ = await ensureUniverse(fresh);
      const group = get('group') || 'all';
      const gcodes = Array.isArray(body.gcodes) ? body.gcodes : (sp.get('gcodes') ? sp.get('gcodes')!.split(',') : null);
      let list = filterFunds(univ, get('type'), get('theme'), get('q'), groupSetOf(group, gcodes));
      const sort = get('sort') || 'chg', order = get('order') === 'asc' ? 1 : -1;
      const nv = (x: number | null) => x ?? -1e18;
      const cmp: Record<string, (a: Fund, b: Fund) => number> = {
        code: (a, b) => a.code.localeCompare(b.code),
        name: (a, b) => a.name.localeCompare(b.name, 'zh'),
        nav: (a, b) => nv(a.nav) - nv(b.nav),
        chg: (a, b) => nv(a.chg) - nv(b.chg),
        w1: (a, b) => nv(a.w1) - nv(b.w1),
        m1: (a, b) => nv(a.m1) - nv(b.m1),
        m3: (a, b) => nv(a.m3) - nv(b.m3),
        m6: (a, b) => nv(a.m6) - nv(b.m6),
        y1: (a, b) => nv(a.y1) - nv(b.y1),
        ytd: (a, b) => nv(a.ytd) - nv(b.ytd)
      };
      const c = cmp[sort] || cmp.chg;
      list = list.slice().sort((a, b) => order * c(a, b));
      const page = Math.max(parseInt(String(get('page'))) || 1, 1);
      const size = Math.min(Math.max(parseInt(String(get('size'))) || 50, 10), 120);
      const rows = list.slice((page - 1) * size, page * size).map(f => {
        const o = Object.assign({}, f) as Fund & { scaleYi?: number; scaleDate?: string; fee?: Fee };
        const s = SCALE.get(f.code);
        if (s) { o.scaleYi = Math.round(s.yi * 1e4) / 1e4; o.scaleDate = s.date; }
        const fe = readCache<Fee>('fee_' + f.code, TTL.fee, false);
        if (fe) o.fee = fe;
        return o;
      });
      return sendJson({ total: list.length, page, size, navDate: univ.navDate, rows });
    }
    if (p === '/extras') {
      const codes = codesOf(get('codes'), 12);
      if (!codes.length) return sendJson({});
      const out: Record<string, unknown> = {};
      await Promise.all(codes.map(async c => {
        const snap = SCALE.get(c);
        const [b, fe] = await Promise.all([
          snap ? Promise.resolve(null) : getBasic(c, fresh).catch(() => null),
          getFee(c, fresh).catch(() => null)
        ]);
        const scaleYi = snap ? Math.round(snap.yi * 1e4) / 1e4 : (b && b.endNav !== null && b.endNav > 0 ? Math.round(b.endNav / 1e4) / 1e4 : null);
        const scaleDate = snap ? snap.date : (b ? b.scaleDate : '');
        out[c] = {
          scaleYi, scaleDate,
          fee: fe || (b && b.buy !== null && b.buy !== undefined ? { buy: b.buy, buyOrig: b.buyOrig, buyTier: '', mgmt: null, custody: null, service: null } : null),
          reason: scaleYi === null && !fe ? '规模未披露/费率获取失败' : null
        };
      }));
      return sendJson(out);
    }
    if (p === '/est-today' || p === '/ydev') {
      const codes = codesOf(get('codes'), p === '/est-today' ? 24 : 10);
      if (!codes.length) return sendJson({});
      const out = p === '/est-today' ? await estTodayMany(codes, fresh) : await yDevMany(codes, fresh);
      return sendJson(out);
    }
    const fm = p.match(/^\/fund\/(\d{6})$/);
    if (fm) return sendJson(await fundDetail(fm[1], get('days'), fresh));
    if (p === '/scale-agg') {
      const univ = await ensureUniverse(false);
      const group = get('group') || 'all';
      const gcodes = Array.isArray(body.gcodes) ? body.gcodes : (sp.get('gcodes') ? sp.get('gcodes')!.split(',') : null);
      const key = hash([get('type') || 'all', get('theme') || 'all', (get('q') || '').trim().toLowerCase(), group, Array.isArray(gcodes) ? gcodes.slice().sort().join(',') : ''].join('|'));
      const cached = readCache<ReturnType<typeof scaleAgg>>('agg_' + key, TTL.agg, fresh);
      if (cached && cached.total === filterFunds(univ, get('type'), get('theme'), get('q'), groupSetOf(group, gcodes)).length) return sendJson(cached);
      const job = scaleAgg(univ, get('type'), get('theme'), get('q'), group, gcodes);
      writeCache('agg_' + key, job);
      return sendJson(job);
    }
    return sendJson({ error: '未知接口 ' + p }, 404);
  } catch (e) {
    const err = e as Error & { status?: number };
    log('ERR', p, err.message);
    return sendJson({ error: err.message || '服务内部错误' }, err.status || 502);
  }
}