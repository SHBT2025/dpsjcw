/* 盯盘神经（场外基金）· 持仓法净值估算终端 — 零依赖本地数据桥
 * 仅监听 127.0.0.1:8788。所有数据来自公开真实接口，禁止模拟/插值。
 * 估算口径：最新季报前十大重仓股 占净值比 × 个股真实涨跌幅（持仓法，覆盖率有限，详见前端说明）。
 */
'use strict';
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = 8788;
const ROOT = __dirname;
const CACHE_DIR = path.join(ROOT, 'data_cache');
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

const TTL = {
  rank: 30 * 60e3,        // 全市场排行（净值/官方涨幅）
  search: 7 * 86400e3,    // 基金类型字典
  hold: 12 * 3600e3,      // 季报持仓（季度披露）
  basic: 7 * 86400e3,     // 基础信息+规模（季度披露）
  pz: 12 * 3600e3,        // 净值历史/规模走势/资产配置
  kl: 20 * 60e3,          // 个股日K（含当日live bar）
  q: 60e3,                // 批量实时行情
  agg: 24 * 3600e3,       // 类目规模聚合结果
  fee: 7 * 86400e3        // 费率（天天基金f10，变动极少）
};

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36';
const MOB_UA = 'Mozilla/5.0'; // 东财移动接口对完整Chrome UA指纹拦截，短UA实测可用
const TX_K_HOSTS = ['https://web.ifzq.gtimg.cn', 'https://proxy.finance.qq.com'];
const MOB = 'https://fundmobapi.eastmoney.com/FundMNewApi';
const MOB_Q = 'deviceid=Wap&plat=Wap&product=EFund&version=2.0.0';

/* ---------- 基础工具 ---------- */
function bjDate(ms) { return new Date(ms + 8 * 3600e3).toISOString().slice(0, 10); }
function bjNow() { return new Date(Date.now() + 8 * 3600e3); }
function bjToday() { return bjDate(Date.now()); }
function tradingNow() {
  const n = bjNow(), wd = n.getUTCDay(), m = n.getUTCHours() * 60 + n.getUTCMinutes();
  return wd >= 1 && wd <= 5 && m >= 555 && m <= 905; // 9:15-15:05 粗略时段，真实休市以行情时间戳为准
}
function num(v) { const x = parseFloat(v); return Number.isFinite(x) ? x : null; }
function md5(s) { return crypto.createHash('md5').update(s).digest('hex').slice(0, 16); }
function log(...a) { console.log('[' + new Date().toISOString() + ']', ...a); }

/* 并发闸：最多 6 个出站请求 */
const gate = { active: 0, queue: [], max: 6 };
function acquire() {
  return new Promise(res => {
    if (gate.active < gate.max) { gate.active++; res(); }
    else gate.queue.push(res);
  });
}
function release() {
  const next = gate.queue.shift();
  if (next) next(); else gate.active--;
}

function fetchBuf(url, headers = {}, timeout = 12000) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, { headers: Object.assign({ 'User-Agent': UA, 'Accept': '*/*' }, headers), timeout }, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        const loc = res.headers.location.startsWith('http') ? res.headers.location : new URL(res.headers.location, url).href;
        return resolve(fetchBuf(loc, headers, timeout));
      }
      if (res.statusCode !== 200) { res.resume(); return reject(new Error('HTTP ' + res.statusCode + ' ' + url.slice(0, 90))); }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    });
    req.on('timeout', () => { req.destroy(new Error('timeout ' + url.slice(0, 90))); });
    req.on('error', reject);
  });
}
async function fetchRetry(urls, headers = {}, tries = 4, timeout = 12000) {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    const url = urls[i % urls.length];
    await acquire();
    try { const b = await fetchBuf(url, headers, timeout); release(); return b; }
    catch (e) { release(); lastErr = e; await new Promise(r => setTimeout(r, 250 * (i + 1))); }
  }
  throw lastErr || new Error('fetch failed');
}

/* 东财移动JSON接口：短UA + 业务错误码重试 */
async function mobJson(url) {
  let lastErr;
  for (let i = 0; i < 4; i++) {
    try {
      const j = JSON.parse((await fetchRetry([url], { 'User-Agent': MOB_UA }, 2, 12000)).toString('utf8'));
      if (j && j.ErrCode === 0) return j;
      lastErr = new Error((j && (j.ErrMsg || j.ErrorMessage)) || '移动接口业务错误');
    } catch (e) { lastErr = e; }
    await new Promise(r => setTimeout(r, 350 * (i + 1)));
  }
  throw lastErr;
}

/* inflight 去重 */
const inflight = new Map();
function dedup(key, fn) {
  if (inflight.has(key)) return inflight.get(key);
  const p = fn().finally(() => inflight.delete(key));
  inflight.set(key, p);
  return p;
}

/* 磁盘缓存 */
function cachePath(name) { return path.join(CACHE_DIR, name.replace(/[^\w.\-]/g, '_') + '.json'); }
function readCache(name, ttl, fresh) {
  if (fresh) return null;
  try {
    const j = JSON.parse(fs.readFileSync(cachePath(name), 'utf8'));
    if (Date.now() - j.fetchedAt < ttl) return j.data;
  } catch (e) { /* 无缓存或损坏 */ }
  return null;
}
function writeCache(name, data) {
  try { fs.writeFileSync(cachePath(name), JSON.stringify({ fetchedAt: Date.now(), data })); } catch (e) { log('cache write fail', name, e.message); }
}

/* ---------- 排序索引：规模 / 费率 / 1日偏离（全部来自真实缓存，无数据=null） ---------- */
const BASE = new Map();   // code -> {scaleYi, scaleDate, feeF10, feeBasic}
const DEV1 = new Map();   // code -> {navDate, dev, estD, official, covD, reason, holdDate, method}
let baseBuildP = null, dev1BuildP = null;

function basePut(code, b, fe) {
  const n = Object.assign({}, BASE.get(code));
  if (b) {
    n.scaleYi = (b.endNav !== null && b.endNav > 0) ? b.endNav / 1e8 : null;
    n.scaleDate = b.scaleDate || '';
    if (b.buy !== undefined) n.feeBasic = { buy: b.buy, buyOrig: b.buyOrig === undefined ? null : b.buyOrig, buyTier: '', mgmt: null, custody: null, service: null };
  }
  if (fe) n.feeF10 = fe;
  BASE.set(code, n);
}
function baseOf(code) {
  const n = BASE.get(code);
  if (!n) return null;
  return { scaleYi: n.scaleYi, scaleDate: n.scaleDate || '', fee: n.feeF10 || n.feeBasic || null };
}
function sortValue(f, sort) {
  if (sort === 'dev1') {
    const d = dev1Valid(f.code, f.navDate);
    return d && d.dev !== null && d.dev !== undefined ? d.dev : null;
  }
  const b = baseOf(f.code);
  if (!b) return null;
  if (sort === 'scale') return (b.scaleYi === undefined || b.scaleYi === null) ? null : b.scaleYi;
  if (sort === 'fee') return (b.fee && b.fee.buy !== null && b.fee.buy !== undefined) ? b.fee.buy : null;
  return null;
}
function baseCoverage(list, sort) {
  let known = 0, undisclosed = 0, pending = 0;
  for (const f of list) {
    const b = baseOf(f.code);
    const has = sort === 'scale' ? (b && b.scaleYi !== undefined) : (b && b.fee);
    if (!has) { pending++; continue; }
    const v = sort === 'scale' ? b.scaleYi : b.fee.buy;
    if (v === null || v === undefined) undisclosed++; else known++;
  }
  return { known, undisclosed, pending };
}
function chunkedIndex(files, ttlOf, apply, label) {
  return (async () => {
    for (let i = 0; i < files.length; i += 400) {
      for (const f of files.slice(i, i + 400)) {
        try {
          const j = JSON.parse(fs.readFileSync(path.join(CACHE_DIR, f), 'utf8'));
          if (Date.now() - j.fetchedAt > ttlOf(f)) continue;
          apply(f, j.data);
        } catch (e) { /* 无缓存或损坏 */ }
      }
      await new Promise(r => setImmediate(r)); // 让出事件循环，避免建索引时界面卡顿
    }
    log(label, files.length, '个缓存文件');
  })();
}
function ensureBaseIndex() {
  if (!baseBuildP) {
    const files = fs.readdirSync(CACHE_DIR).filter(f => /^(basic|fee)_\d{6}\.json$/.test(f));
    baseBuildP = chunkedIndex(files,
      f => f.startsWith('basic_') ? TTL.basic : TTL.fee,
      (f, d) => {
        if (f.startsWith('basic_')) { if (d.buy !== undefined) basePut(f.slice(6, 12), d, null); }
        else basePut(f.slice(4, 10), null, d);
      }, '规模/费率排序索引就绪:');
  }
  return baseBuildP;
}
function ensureDev1Index() {
  if (!dev1BuildP) {
    const files = fs.readdirSync(CACHE_DIR).filter(f => /^dev1_\d{6}\.json$/.test(f));
    dev1BuildP = chunkedIndex(files, () => Infinity,
      (f, d) => { if (d && d.navDate) DEV1.set(f.slice(5, 11), d); }, '1日偏离排序索引就绪:');
  }
  return dev1BuildP;
}
function dev1Put(code, o) { DEV1.set(code, o); writeCache('dev1_' + code, o); }

/* ---------- 基金全集：排行 + 类型字典 ---------- */
const THEMES = [
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
const TYPE_KEYS = [['gp', '股票型'], ['hh', '混合型'], ['zs', '指数型'], ['qdii', 'QDII'], ['zq', '债券型'], ['fof', 'FOF'], ['other', '其他']];
function typeKey(t) {
  if (!t) return 'other';
  if (t.startsWith('股票')) return 'gp';
  if (t.startsWith('混合')) return 'hh';
  if (t.startsWith('指数') || t.includes('股票指数')) return 'zs';
  if (t.startsWith('QDII')) return 'qdii';
  if (t.startsWith('债券')) return 'zq';
  if (t.startsWith('FOF') || t.includes('FOF')) return 'fof';
  return 'other';
}
function themesOf(name) {
  const out = [];
  for (const [k, , re] of THEMES) if (re.test(name)) out.push(k);
  return out;
}

let UNIV = null; // {funds, byCode, navDate, builtAt}
async function loadRank(fresh) {
  const c = readCache('rank', TTL.rank, fresh);
  if (c) return c;
  return dedup('rank', async () => {
    const url = 'https://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&rs=&gs=0&sc=zzf&st=desc&pi=1&pn=30000&dx=0';
    const txt = (await fetchRetry([url], { Referer: 'https://fund.eastmoney.com/data/fundranking.html' }, 4, 25000)).toString('utf8');
    const m = txt.match(/datas:\[([\s\S]*?)\],allRecords/);
    if (!m) throw new Error('排行接口返回格式异常');
    const rows = [];
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
async function loadSearch(fresh) {
  const c = readCache('search', TTL.search, fresh);
  if (c) return c;
  return dedup('search', async () => {
    const url = 'https://fund.eastmoney.com/js/fundcode_search.js';
    let txt = (await fetchRetry([url], { Referer: 'https://fund.eastmoney.com/' })).toString('utf8');
    txt = txt.replace(/^﻿/, '').replace(/^var\s+r\s*=\s*/, '').replace(/;\s*$/, '');
    const arr = JSON.parse(txt);
    const map = {};
    for (const it of arr) if (it && it[0] && !map[it[0]]) map[it[0]] = it[3] || '';
    writeCache('search', map);
    log('search loaded:', Object.keys(map).length);
    return map;
  });
}
async function ensureUniverse(fresh) {
  if (UNIV && !fresh && Date.now() - UNIV.builtAt < TTL.rank) return UNIV;
  const [rank, types] = await Promise.all([loadRank(fresh), loadSearch(fresh)]);
  const funds = rank.map(r => {
    const type = types[r.code] || '';
    return Object.assign({}, r, { type, tk: typeKey(type), themes: themesOf(r.name) });
  });
  const byCode = new Map(funds.map(f => [f.code, f]));
  let navDate = '';
  for (const f of funds) if (f.navDate > navDate) navDate = f.navDate;
  UNIV = { funds, byCode, navDate, builtAt: Date.now() };
  return UNIV;
}

/* ---------- 持仓（最新季报，ETF联接穿透） ---------- */
function symsOf(gpdm, newtexch) {
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
async function fetchHoldRaw(code) {
  const url = MOB + '/FundMNInverstPosition?FCODE=' + code + '&' + MOB_Q;
  const j = await mobJson(url);
  if (!j.Datas) throw new Error('持仓接口无数据 ' + code);
  j.Datas.Expansion = j.Expansion || '';
  return j.Datas;
}
async function getHoldings(code, fresh) {
  const c = readCache('hold_' + code, TTL.hold, fresh);
  if (c) return c;
  return dedup('hold_' + code, async () => {
    let d = await fetchHoldRaw(code);
    const orig = d;
    let method = '直接持仓', via = null;
    if ((!d.fundStocks || !d.fundStocks.length) && d.ETFCODE) {
      via = d.ETFCODE;
      try { d = await fetchHoldRaw(d.ETFCODE); method = '穿透ETF(' + via + ')'; } catch (e) { d = { fundStocks: [] }; }
    }
    const stocks = [];
    for (const s of (d.fundStocks || [])) {
      const w = num(s.JZBL);
      if (w === null || w <= 0) continue;
      const sy = symsOf(s.GPDM, s.NEWTEXCH);
      stocks.push(Object.assign({ code: s.GPDM, name: s.GPJC, w, industry: s.INDEXNAME === '--' ? '' : (s.INDEXNAME || '') }, sy));
    }
    let reason = null;
    if (!stocks.length) {
      reason = (orig.fundfofs && orig.fundfofs.length) ? 'FOF持仓为基金，持仓法暂不适用'
        : (orig.fundboods && orig.fundboods.length) ? '最新季报以债券持仓为主，持仓法不适用'
          : '最新季报未披露股票持仓，无法用持仓法估算';
    }
    const cov = stocks.reduce((a, s) => a + s.w, 0);
    const out = { date: d.Expansion || '', stocks, coverage: Math.round(cov * 100) / 100, method, via, reason };
    writeCache('hold_' + code, out);
    return out;
  });
}

/* ---------- 基础信息（规模/公司/经理/优惠申购费率） ---------- */
async function getBasic(code, fresh) {
  const c = readCache('basic_' + code, TTL.basic, fresh);
  // 旧缓存缺少 RATE 字段（费率排序需要），视为过期重新请求
  if (c && c.buy !== undefined) return c;
  return dedup('basic_' + code, async () => {
    const url = MOB + '/FundMNBasicInformation?FCODE=' + code + '&' + MOB_Q;
    const j = await mobJson(url);
    const d = (j && j.Datas) || {};
    const out = {
      endNav: num(d.ENDNAV), scaleDate: d.FEGMRQ && d.FEGMRQ !== '--' ? d.FEGMRQ : '',
      company: d.JJGS || '', manager: d.JJJL || '', ftype: d.FTYPE || '',
      risk: d.RISKLEVEL || '', estab: d.ESTABDATE || '',
      buy: num(String(d.RATE || '').replace('%', '')), buyOrig: num(String(d.SOURCERATE || '').replace('%', ''))
    };
    writeCache('basic_' + code, out);
    basePut(code, out, null);
    return out;
  });
}

/* ---------- 费率（天天基金f10费率页：申购优惠/原价 + 管理/托管/销售服务费） ---------- */
function parseFeePage(h) {
  const g = re => { const m = h.match(re); return m ? parseFloat(m[1]) : null; };
  const out = {
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
async function getFee(code, fresh) {
  const c = readCache('fee_' + code, TTL.fee, fresh);
  if (c) return c;
  return dedup('fee_' + code, async () => {
    const url = 'https://fundf10.eastmoney.com/jjfl_' + code + '.html';
    const h = (await fetchRetry([url], { Referer: 'https://fundf10.eastmoney.com/' }, 4, 15000)).toString('utf8');
    if (!h.includes('管理费率')) throw new Error('费率页结构异常 ' + code);
    const out = parseFeePage(h);
    writeCache('fee_' + code, out);
    basePut(code, null, out);
    return out;
  });
}

/* ---------- pingzhongdata：净值全历史 / 规模走势 / 股票仓位 ---------- */
/* 整个文件可能只有一行，用括号配平扫描提取变量值 */
function extractJsVar(txt, name) {
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
    else if (ch === close) { depth--; if (depth === 0) { try { return JSON.parse(txt.slice(start, i + 1)); } catch (e) { return null; } } }
  }
  return null;
}
async function getPz(code, fresh) {
  const c = readCache('pz_' + code, TTL.pz, fresh);
  if (c) return c;
  return dedup('pz_' + code, async () => {
    const url = 'https://fund.eastmoney.com/pingzhongdata/' + code + '.js';
    const txt = (await fetchRetry([url], { Referer: 'https://fund.eastmoney.com/' + code + '.html' }, 4, 25000)).toString('utf8');
    const trend = extractJsVar(txt, 'Data_netWorthTrend') || [];
    const nav = trend.map(p => ({ d: bjDate(p.x), nav: num(p.y), ret: num(p.equityReturn) })).filter(p => p.d && p.nav !== null);
    const fs = extractJsVar(txt, 'Data_fluctuationScale');
    let scale = null;
    if (fs && fs.categories && Array.isArray(fs.series)) {
      const vals = (fs.series[0] && Array.isArray(fs.series[0].data)) ? fs.series[0].data : fs.series;
      scale = { dates: fs.categories, values: vals.map(v => (v && typeof v === 'object') ? num(v.y) : num(v)) };
    }
    const aa = extractJsVar(txt, 'Data_assetAllocation');
    let stockRatio = null, ratioDate = '';
    if (aa && aa.series) {
      const s = aa.series.find(x => x.name === '股票占净比');
      if (s && s.data && s.data.length) {
        stockRatio = num(s.data[s.data.length - 1]);
        ratioDate = (aa.categories && aa.categories[aa.categories.length - 1]) || '';
      }
    }
    if (!nav.length) throw new Error('净值历史解析失败 ' + code);
    const out = { nav, scale, stockRatio, ratioDate };
    writeCache('pz_' + code, out);
    return out;
  });
}

/* ---------- 批量实时行情（腾讯，GBK） ---------- */
async function getQuotes(syms, fresh) {
  const uniq = [...new Set(syms)].sort();
  if (!uniq.length) return {};
  const key = 'q_' + md5(uniq.join(','));
  const c = readCache(key, TTL.q, fresh);
  if (c) return c;
  return dedup(key, async () => {
    const out = {};
    for (let i = 0; i < uniq.length; i += 60) {
      const batch = uniq.slice(i, i + 60);
      try {
        const buf = await fetchRetry(['https://qt.gtimg.cn/q=' + batch.join(',')], {}, 3, 10000);
        const txt = new TextDecoder('gbk').decode(buf);
        for (const m of txt.matchAll(/v_([\w.]+)="([^"]*)"/g)) {
          const f = m[2].split('~');
          if (f.length < 33) continue;
          const price = num(f[3]), prev = num(f[4]), pct = num(f[32]);
          out[m[1]] = { name: f[1], price, prev, pct, time: f[30] || '' };
        }
      } catch (e) { log('quote batch fail:', e.message); }
    }
    writeCache(key, out);
    return out;
  });
}

/* ---------- 个股日K（腾讯qfq主源，新浪A股备用；320根） ---------- */
function barsToRets(bars) {
  const rets = {};
  for (let i = 1; i < bars.length; i++) {
    if (bars[i - 1].c > 0) rets[bars[i].d] = (bars[i].c / bars[i - 1].c - 1) * 100;
  }
  return rets;
}
function parseTxK(j, ksym) {
  const data = j && j.data;
  if (!data) return null;
  const node = data[ksym] || data[Object.keys(data)[0]];
  if (!node) return null;
  const rows = node.qfqday || node.day;
  if (!Array.isArray(rows) || !rows.length) return null;
  const bars = [];
  for (const r of rows) {
    const d = String(r[0]).slice(0, 10), c = num(r[2]);
    if (d && c !== null) bars.push({ d, c });
  }
  return bars.length ? bars : null;
}
async function getKline(ksym, fresh, ttl) {
  const c = readCache('kl_' + ksym, ttl || TTL.kl, fresh);
  if (c) return c;
  return dedup('kl_' + ksym, async () => {
    let candidates = [ksym];
    if (ksym.startsWith('us')) {
      const base = ksym.replace(/\.(OQ|N|A)$/, '');
      candidates = [ksym, base + '.OQ', base + '.N', base + '.A', base];
      candidates = [...new Set(candidates)];
    }
    let bars = null;
    // 通用 fqkline 仅支持A股；港/美股须走专用端点（通用端点返回WAF拦截页）
    const kpath = ksym.startsWith('hk') ? 'hkfqkline' : ksym.startsWith('us') ? 'usfqkline' : 'fqkline';
    const khosts = kpath === 'fqkline' ? TX_K_HOSTS : ['https://web.ifzq.gtimg.cn', 'https://ifzq.gtimg.cn'];
    for (const sym of candidates) {
      const urls = khosts.map(h => h + '/appstock/app/' + kpath + '/get?param=' + sym + ',day,,,320,qfq');
      try {
        const j = JSON.parse((await fetchRetry(urls, {}, 3, 12000)).toString('utf8'));
        bars = parseTxK(j, sym);
        if (bars) break;
      } catch (e) { /* 试下一候选 */ }
    }
    if (!bars && /^(sh|sz|bj)\d{6}$/.test(ksym)) {
      try {
        const url = 'https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketData.getKLineData?symbol=' + ksym + '&scale=240&ma=no&datalen=320';
        const arr = JSON.parse((await fetchRetry([url], { Referer: 'https://finance.sina.com.cn/' })).toString('utf8'));
        if (Array.isArray(arr) && arr.length) {
          bars = arr.map(r => ({ d: String(r.day).slice(0, 10), c: num(r.close) })).filter(b => b.c !== null);
        }
      } catch (e) { /* 诚实失败 */ }
    }
    if (!bars || bars.length < 2) throw new Error('K线获取失败 ' + ksym);
    const out = { bars, rets: barsToRets(bars), last: bars[bars.length - 1].d };
    writeCache('kl_' + ksym, out);
    return out;
  });
}

/* ---------- 估算：今日（实时行情） ---------- */
async function estTodayMany(codes, fresh) {
  const u = await ensureUniverse(false);
  const holds = await Promise.all(codes.map(async c => {
    try { return await getHoldings(c, fresh); } catch (e) { return { error: e.message }; }
  }));
  const qsyms = [];
  holds.forEach(h => (h.stocks || []).forEach(s => qsyms.push(s.qsym)));
  const quotes = await getQuotes(qsyms, fresh);
  const out = {};
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

/* ---------- 偏差口径：官方 − 估算；月偏差 = 近1月同交易日集合的复利涨幅差 ---------- */
function monthCutoff(d) {
  const t = new Date(d + 'T00:00:00Z');
  const day = t.getUTCDate();
  t.setUTCMonth(t.getUTCMonth() - 1);
  if (t.getUTCDate() < day) t.setUTCDate(0); // 月末钳制（03-31 → 02-28）
  return t.toISOString().slice(0, 10);
}
function monthDevOf(series, lastDate) {
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

/* ---------- 估算：最新净值日偏差（1日偏离，排序索引复用） ---------- */
const DEV1_FAIL_RETRY = 30 * 60e3; // 网络/行情类失败短期内复用，避免反复打接口
function dev1Valid(code, navDate) {
  const d = DEV1.get(code);
  if (!d || d.navDate !== navDate) return null;
  if (d.retryAt && Date.now() >= d.retryAt) return null;
  return d;
}
/* skipCached=true 时命中索引的基金不再请求接口（全市场扫描用） */
async function dev1Core(codes, fresh, klTtl, skipCached) {
  const u = await ensureUniverse(false);
  await ensureDev1Index();
  const out = {}, need = [];
  for (const c of codes) {
    const f = u.byCode.get(c);
    if (!fresh && skipCached && f) {
      const d = dev1Valid(c, f.navDate);
      if (d) { out[c] = d; continue; }
    }
    need.push(c);
  }
  const holds = await Promise.all(need.map(async c => {
    try { return await getHoldings(c, fresh); } catch (e) { return { error: e.message }; }
  }));
  const ksyms = [...new Set(holds.flatMap(h => (h.stocks || []).map(s => s.ksym)))];
  const klMap = {};
  await Promise.all(ksyms.map(async k => {
    try { klMap[k] = await getKline(k, fresh, klTtl); } catch (e) { klMap[k] = null; }
  }));
  need.forEach((c, i) => {
    const h = holds[i], f = u.byCode.get(c);
    const D = f ? f.navDate : '';
    let o;
    if (!h || h.error) o = { navDate: D, dev: null, estD: null, official: f ? f.chg : null, reason: (h && h.error) || '持仓获取失败', retryAt: Date.now() + DEV1_FAIL_RETRY };
    else if (h.reason) o = { navDate: D, dev: null, estD: null, official: f ? f.chg : null, reason: h.reason, holdDate: h.date, method: h.method };
    else if (!f || !f.navDate) o = { navDate: '', dev: null, estD: null, official: null, reason: '无最新净值数据' };
    else {
      let est = 0, cov = 0;
      for (const s of h.stocks) {
        const kl = klMap[s.ksym];
        const r = kl && kl.rets[D];
        if (r === undefined || r === null) continue;
        est += s.w * r / 100; cov += s.w;
      }
      const base = { holdDate: h.date, method: h.method, navDate: D, official: f.chg };
      if (cov <= 0) o = Object.assign(base, { dev: null, estD: null, covD: 0, reason: '净值日(' + D + ')持仓股无对应行情', retryAt: Date.now() + DEV1_FAIL_RETRY });
      else {
        const estD = Math.round(est * 1000) / 1000;
        o = Object.assign(base, {
          dev: f.chg !== null ? Math.round((f.chg - estD) * 1000) / 1000 : null,
          estD, covD: Math.round(cov * 100) / 100,
          reason: f.chg === null ? '官方涨幅缺失' : null
        });
      }
    }
    dev1Put(c, o);
    out[c] = o;
  });
  return { out, holds, klMap, u, need };
}
async function dev1Many(codes, fresh, klTtl) {
  return (await dev1Core(codes, fresh, klTtl, true)).out;
}

/* ---------- 估算：最新净值日偏差 + 近1月偏差（历史K线） ---------- */
async function yDevMany(codes, fresh) {
  const core = await dev1Core(codes, fresh, TTL.kl, false);
  const { out, holds, klMap, u } = core;
  const pzs = await Promise.all(codes.map(async c => {
    try { return await getPz(c, fresh); } catch (e) { return null; }
  }));
  codes.forEach((c, i) => {
    const h = holds[i], f = u.byCode.get(c);
    out[c].devM = null;
    if (!h || h.error || h.reason || !f || !f.navDate) { out[c].reasonM = out[c].reason || '不可估算'; return; }
    const D = f.navDate;
    const estByDate = d => {
      let est = 0, cov = 0;
      for (const s of h.stocks) {
        const kl = klMap[s.ksym];
        const r = kl && kl.rets[d];
        if (r === undefined || r === null) continue;
        est += s.w * r / 100; cov += s.w;
      }
      return cov > 0 ? { est, cov } : null;
    };
    const pz = pzs[i];
    if (!pz) { out[c].reasonM = '净值历史获取失败'; return; }
    const cut = monthCutoff(D);
    const series = pz.nav.filter(p => p.d > cut && p.d <= D).map(p => {
      const e = estByDate(p.d);
      return { d: p.d, ret: p.ret, estRet: e ? Math.round(e.est * 1000) / 1000 : null };
    });
    Object.assign(out[c], monthDevOf(series, D));
  });
  return out;
}

/* ---------- 单基金详情：官方净值 × 估算序列 ---------- */
async function fundDetail(code, days, fresh) {
  const u = await ensureUniverse(false);
  const f = u.byCode.get(code);
  if (!f) { const e = new Error('未知基金代码 ' + code); e.status = 404; throw e; }
  days = Math.min(Math.max(parseInt(days) || 120, 20), 250);
  const [basic, pz, hold, fee] = await Promise.all([
    getBasic(code, fresh).catch(() => null),
    getPz(code, fresh),
    getHoldings(code, fresh).catch(e => ({ error: e.message })),
    getFee(code, fresh).catch(() => null)
  ]);
  const navAll = pz.nav;
  const win = navAll.slice(-(days + 1));
  let estArr = win.map(() => null), klines = {}, holdErr = null;
  if (hold && !hold.error && !hold.reason && hold.stocks.length) {
    await Promise.all(hold.stocks.map(async s => {
      try { klines[s.ksym] = await getKline(s.ksym, fresh); } catch (e) { klines[s.ksym] = null; }
    }));
    let prevEst = null;
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
      // 锚定：前一日估算净值；缺口后以昨日官方净值重新锚定（口径已在界面说明）
      const anchor = prevEst !== null ? prevEst : win[i - 1].nav;
      const enav = Math.round(anchor * (1 + ret / 100) * 10000) / 10000;
      estArr[i] = { ret, nav: enav, cov: Math.round(cov * 100) / 100 };
      prevEst = enav;
    }
  } else holdErr = (hold && (hold.error || hold.reason)) || '持仓不可用';
  const quotes = hold && hold.stocks ? await getQuotes(hold.stocks.map(s => s.qsym), fresh).catch(() => ({})) : {};
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
    estRet: estArr[i] ? estArr[i].ret : null,
    estNav: estArr[i] ? estArr[i].nav : null,
    cov: estArr[i] ? estArr[i].cov : 0,
    dev: estArr[i] && estArr[i].ret !== null && p.ret !== null ? Math.round((p.ret - estArr[i].ret) * 1000) / 1000 : null
  }));
  const monthDev = monthDevOf(rows, f.navDate || rows[rows.length - 1].d);
  const devs = rows.slice(1).map(r => r.dev).filter(v => v !== null);
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

/* ---------- 用户数据：自选 + 自定义分类（本机 userdata.json） ---------- */
const USER_FILE = path.join(ROOT, 'userdata.json');
function loadUser() {
  try {
    const j = JSON.parse(fs.readFileSync(USER_FILE, 'utf8'));
    return {
      watch: Array.isArray(j.watch) ? j.watch.filter(c => /^\d{6}$/.test(c)) : [],
      cats: Array.isArray(j.cats) ? j.cats.filter(c => c && c.id && c.name).map(c => ({
        id: String(c.id).slice(0, 24), name: String(c.name).slice(0, 20),
        codes: Array.isArray(c.codes) ? c.codes.filter(x => /^\d{6}$/.test(x)) : []
      })) : []
    };
  } catch (e) { return { watch: [], cats: [] }; }
}
function saveUser(j) {
  const tmp = USER_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(j));
  fs.renameSync(tmp, USER_FILE);
}
function groupSet(group) {
  if (!group || group === 'all') return null;
  const ud = loadUser();
  if (group === 'watch') return new Set(ud.watch);
  const cat = ud.cats.find(c => c.id === group);
  return new Set(cat ? cat.codes : []);
}

/* ---------- 类目规模渐进聚合 ---------- */
const aggJobs = new Map();
let activeAgg = null;
function filterFunds(u, type, theme, q, gset) {
  const qq = (q || '').trim().toLowerCase();
  return u.funds.filter(f =>
    (!gset || gset.has(f.code)) &&
    (!type || type === 'all' || f.tk === type) &&
    (!theme || theme === 'all' || f.themes.includes(theme)) &&
    (!qq || f.code.includes(qq) || f.name.toLowerCase().includes(qq) || (f.pinyin || '').toLowerCase().includes(qq))
  );
}
function aggKey(type, theme, q, group) {
  let base = [type || 'all', theme || 'all', (q || '').trim().toLowerCase(), group || 'all'].join('|');
  if (group && group !== 'all') base += '|' + md5([...groupSet(group)].sort().join(','));
  return md5(base);
}
async function startAgg(type, theme, q, group, force) {
  const u = await ensureUniverse(false);
  const gset = groupSet(group);
  const list = filterFunds(u, type, theme, q, gset);
  const key = aggKey(type, theme, q, group);
  const cached = force ? null : readCache('agg_' + key, TTL.agg, false);
  if (cached && cached.total === list.length) { aggJobs.set(key, cached); return cached; }
  if (activeAgg && activeAgg.running) activeAgg.cancel = true;
  const job = { key, total: list.length, done: 0, sumYi: 0, missing: 0, running: true, cancel: false, startedAt: Date.now(), latestScaleDate: '', type: type || 'all', theme: theme || 'all', q: q || '', group: group || 'all' };
  aggJobs.set(key, job);
  activeAgg = job;
  (async () => {
    for (const f of list) {
      if (job.cancel) { job.running = false; return; }
      try {
        const b = await getBasic(f.code, false);
        if (b && b.endNav !== null && b.endNav > 0) {
          job.sumYi += b.endNav / 1e8;
          if (b.scaleDate > job.latestScaleDate) job.latestScaleDate = b.scaleDate;
        } else job.missing++;
      } catch (e) { job.missing++; }
      job.done++;
    }
    job.running = false;
    job.finishedAt = Date.now();
    job.sumYi = Math.round(job.sumYi * 100) / 100;
    writeCache('agg_' + key, job);
    log('agg done', key, job.done, '/', job.total, job.sumYi, '亿');
  })().catch(e => { job.running = false; job.error = e.message; });
  return job;
}

/* ---------- 1日偏离全市场扫描（排序用：渐进、可暂停、结果落盘缓存） ---------- */
const devJobs = new Map();
let activeDev = null;
let devGen = 0; // 扫描代号：新一轮启动后旧循环立即让位，避免并发循环互相覆盖进度
function devKey(type, theme, q, group) {
  let base = [type || 'all', theme || 'all', (q || '').trim().toLowerCase(), group || 'all'].join('|');
  if (group && group !== 'all') base += '|' + md5([...groupSet(group)].sort().join(','));
  return md5(base);
}
function devCounters(list) {
  let computed = 0, na = 0, failed = 0, unknown = 0;
  for (const f of list) {
    const d = dev1Valid(f.code, f.navDate);
    if (!d) unknown++;
    else if (d.dev !== null && d.dev !== undefined) computed++;
    else if (d.retryAt) failed++;
    else na++;
  }
  return { computed, na, failed, unknown };
}
function devStatus(type, theme, q, group, u) {
  const list = filterFunds(u, type, theme, q, groupSet(group));
  const c = devCounters(list);
  const job = devJobs.get(devKey(type, theme, q, group));
  const base = {
    key: devKey(type, theme, q, group), total: list.length, navDate: u.navDate,
    type: type || 'all', theme: theme || 'all', q: q || '', group: group || 'all'
  };
  if (job) return Object.assign(base, job, c, { done: c.computed + c.na + c.failed });
  return Object.assign(base, c, { done: c.computed + c.na + c.failed, running: false, idle: true });
}
/* 交互请求优先：出站闸占满或有排队时扫描先让路，保证界面不卡 */
async function scanYield() {
  let guard = 0;
  while ((gate.queue.length > 0 || gate.active >= gate.max) && guard++ < 150) await new Promise(r => setTimeout(r, 120));
}
async function startDevScan(type, theme, q, group) {
  const u = await ensureUniverse(false);
  await ensureDev1Index();
  const list = filterFunds(u, type, theme, q, groupSet(group));
  const key = devKey(type, theme, q, group);
  const exist = devJobs.get(key);
  if (exist && exist.running) return exist;
  if (activeDev && activeDev.running && activeDev.key !== key) activeDev.cancel = true;
  const job = {
    key, total: list.length, done: 0, computed: 0, na: 0, failed: 0, unknown: list.length,
    running: true, cancel: false, paused: false, startedAt: Date.now(), navDate: u.navDate,
    type: type || 'all', theme: theme || 'all', q: q || '', group: group || 'all'
  };
  devJobs.set(key, job); activeDev = job;
  const myGen = ++devGen;
  // 净值日不是今天（或已收盘）时只需历史收盘，K线可用较长TTL，避免扫描期间反复重拉
  const klTtl = (u.navDate === bjToday() && tradingNow()) ? TTL.kl : 6 * 3600e3;
  (async () => {
    // 多轮补扫：命中索引的基金跳过请求，只补未知/过期项，直到全覆盖或三轮上限
    for (let pass = 0; pass < 3; pass++) {
      for (let i = 0; i < list.length; i += 12) {
        if (job.cancel || devGen !== myGen) { job.running = false; job.paused = job.cancel; return; }
        await scanYield();
        const batch = list.slice(i, i + 12).map(f => f.code);
        try {
          const r = await dev1Many(batch, false, klTtl);
          for (const c of batch) {
            const d = r[c];
            if (d && d.dev !== null && d.dev !== undefined) job.computed++;
            else if (d && d.retryAt) job.failed++;
            else job.na++;
          }
        } catch (e) { job.failed += batch.length; log('dev scan batch fail:', e.message); }
        job.done += batch.length;
        job.unknown = Math.max(job.total - job.done, 0);
      }
      if (devCounters(list).unknown === 0) break;
    }
    if (devGen === myGen) { job.running = false; job.finishedAt = Date.now(); }
    log('dev1 scan done', key, job.computed, '/', job.total);
  })().catch(e => { job.running = false; job.error = e.message; });
  return job;
}

/* ---------- HTTP 服务 ---------- */
function sendJson(res, obj, status = 200) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' });
  res.end(body);
}
function sendErr(res, e) {
  log('ERR', e.message);
  sendJson(res, { error: e.message || '服务内部错误' }, e.status || 502);
}
const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://127.0.0.1');
  const p = u.pathname;
  if (req.method === 'OPTIONS') { res.writeHead(204, { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Methods': 'GET,PUT,POST,OPTIONS' }); return res.end(); }
  try {
    if (p === '/' || p === '/index.html') {
      const html = fs.readFileSync(path.join(ROOT, 'index.html'));
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
      return res.end(html);
    }
    if (p === '/api/health') return sendJson(res, { ok: true, port: PORT, time: Date.now(), tradingNow: tradingNow() });
    if (p === '/api/meta') {
      const univ = await ensureUniverse(u.searchParams.get('fresh') === '1');
      const types = TYPE_KEYS.map(([k, label]) => ({ key: k, label, count: univ.funds.filter(f => f.tk === k).length }));
      const themes = THEMES.map(([k, label]) => ({ key: k, label, count: univ.funds.filter(f => f.themes.includes(k)).length }));
      return sendJson(res, { fundCount: univ.funds.length, navDate: univ.navDate, types, themes, serverTime: Date.now(), tradingNow: tradingNow(), builtAt: univ.builtAt });
    }
    if (p === '/api/funds') {
      const sp = u.searchParams;
      const univ = await ensureUniverse(sp.get('fresh') === '1');
      const group = sp.get('group') || 'all';
      let list = filterFunds(univ, sp.get('type'), sp.get('theme'), sp.get('q'), groupSet(group));
      const sort = sp.get('sort') || 'chg', order = sp.get('order') === 'asc' ? 1 : -1;
      let sortInfo = null;
      if (sort === 'scale' || sort === 'fee' || sort === 'dev1') {
        if (sort === 'dev1') await ensureDev1Index(); else await ensureBaseIndex();
        // 无数据的基金一律排在末尾（不随升降序翻转），同值按代码稳定排序
        list = list.slice().sort((a, b) => {
          const x = sortValue(a, sort), y = sortValue(b, sort);
          if (x === null && y === null) return a.code < b.code ? -1 : 1;
          if (x === null) return 1;
          if (y === null) return -1;
          return x === y ? (a.code < b.code ? -1 : 1) : order * (x - y);
        });
        if (sort === 'dev1') {
          const job = devJobs.get(devKey(sp.get('type'), sp.get('theme'), sp.get('q'), group));
          sortInfo = Object.assign({ key: 'dev1', total: list.length, running: !!(job && job.running), navDate: univ.navDate }, devCounters(list));
          // 扫描结束仍有缺口（如中途被新扫描顶替）时自动补一轮；用户主动暂停则不打扰
          if (sortInfo.unknown > 0 && !(job && job.running) && !(job && job.paused) && (!job || Date.now() - (job.finishedAt || 0) > 60e3)) {
            startDevScan(sp.get('type'), sp.get('theme'), sp.get('q'), group).catch(e => log('dev scan heal fail:', e.message));
            sortInfo.running = true;
          }
        } else {
          const agg = aggJobs.get(aggKey(sp.get('type'), sp.get('theme'), sp.get('q'), group));
          sortInfo = Object.assign({ key: sort, total: list.length, running: !!(agg && agg.running), sweepDone: agg ? agg.done : 0, sweepTotal: agg ? agg.total : list.length }, baseCoverage(list, sort));
          // 规模/费率来自基础信息缓存：缺口时后台补齐（与底部规模聚合同一条扫描），5分钟内不重复启动
          if (sortInfo.pending > 0 && !(agg && agg.running) && (!agg || Date.now() - (agg.startedAt || 0) > 5 * 60e3)) {
            startAgg(sp.get('type'), sp.get('theme'), sp.get('q'), group, true).catch(e => log('base sweep fail:', e.message));
            sortInfo.running = true;
          }
        }
      } else {
        const cmp = {
          code: (a, b) => a.code.localeCompare(b.code),
          name: (a, b) => a.name.localeCompare(b.name, 'zh'),
          nav: (a, b) => (a.nav ?? -1e18) - (b.nav ?? -1e18),
          chg: (a, b) => (a.chg ?? -1e18) - (b.chg ?? -1e18),
          w1: (a, b) => (a.w1 ?? -1e18) - (b.w1 ?? -1e18),
          m1: (a, b) => (a.m1 ?? -1e18) - (b.m1 ?? -1e18),
          m3: (a, b) => (a.m3 ?? -1e18) - (b.m3 ?? -1e18),
          m6: (a, b) => (a.m6 ?? -1e18) - (b.m6 ?? -1e18),
          y1: (a, b) => (a.y1 ?? -1e18) - (b.y1 ?? -1e18),
          ytd: (a, b) => (a.ytd ?? -1e18) - (b.ytd ?? -1e18)
        }[sort] || ((a, b) => (a.chg ?? -1e18) - (b.chg ?? -1e18));
        list = list.slice().sort((a, b) => order * cmp(a, b));
      }
      const page = Math.max(parseInt(sp.get('page')) || 1, 1);
      const size = Math.min(Math.max(parseInt(sp.get('size')) || 50, 10), 120);
      const rows = list.slice((page - 1) * size, page * size).map(f => {
        const b = readCache('basic_' + f.code, TTL.basic, false);
        const fe = readCache('fee_' + f.code, TTL.fee, false);
        const o = Object.assign({}, f);
        if (b && b.endNav !== null && b.endNav > 0) { o.scaleYi = Math.round(b.endNav / 1e4) / 1e4; o.scaleDate = b.scaleDate; }
        const feeVal = fe || (b && b.buy !== undefined && b.buy !== null ? { buy: b.buy, buyOrig: b.buyOrig === undefined ? null : b.buyOrig, buyTier: '', mgmt: null, custody: null, service: null } : null);
        if (feeVal) o.fee = feeVal;
        return o;
      });
      return sendJson(res, { total: list.length, page, size, navDate: univ.navDate, rows, sortInfo });
    }
    if (p === '/api/userdata') {
      if (req.method === 'GET') return sendJson(res, loadUser());
      if (req.method === 'PUT') {
        let b = '';
        req.on('data', c => { b += c; if (b.length > 1e6) req.destroy(); });
        req.on('end', () => {
          try {
            const j = JSON.parse(b);
            const clean = {
              watch: [...new Set((Array.isArray(j.watch) ? j.watch : []).filter(c => /^\d{6}$/.test(c)))].slice(0, 5000),
              cats: (Array.isArray(j.cats) ? j.cats : []).slice(0, 100).filter(c => c && c.id && c.name).map(c => ({
                id: String(c.id).slice(0, 24), name: String(c.name).slice(0, 20),
                codes: [...new Set((Array.isArray(c.codes) ? c.codes : []).filter(x => /^\d{6}$/.test(x)))].slice(0, 5000)
              }))
            };
            saveUser(clean);
            sendJson(res, { ok: true });
          } catch (e) { sendJson(res, { error: '用户数据格式错误' }, 400); }
        });
        return;
      }
    }
    if (p === '/api/extras') {
      const codes = (u.searchParams.get('codes') || '').split(',').map(s => s.trim()).filter(s => /^\d{6}$/.test(s)).slice(0, 120);
      if (!codes.length) return sendJson(res, {});
      const fresh = u.searchParams.get('fresh') === '1';
      const out = {};
      await Promise.all(codes.map(async c => {
        const [b, fe] = await Promise.all([
          getBasic(c, fresh).catch(() => null),
          getFee(c, fresh).catch(() => null)
        ]);
        out[c] = {
          scaleYi: b && b.endNav !== null && b.endNav > 0 ? Math.round(b.endNav / 1e4) / 1e4 : null,
          scaleDate: b ? b.scaleDate : '',
          fee: fe || (b && b.buy !== null && b.buy !== undefined ? { buy: b.buy, buyOrig: b.buyOrig, buyTier: '', mgmt: null, custody: null, service: null } : null),
          reason: !b && !fe ? '基础信息/费率获取失败' : null
        };
      }));
      return sendJson(res, out);
    }
    if (p === '/api/est-today' || p === '/api/ydev') {
      const codes = (u.searchParams.get('codes') || '').split(',').map(s => s.trim()).filter(s => /^\d{6}$/.test(s)).slice(0, 120);
      if (!codes.length) return sendJson(res, {});
      const fresh = u.searchParams.get('fresh') === '1';
      const out = p === '/api/est-today' ? await estTodayMany(codes, fresh) : await yDevMany(codes, fresh);
      return sendJson(res, out);
    }
    const fm = p.match(/^\/api\/fund\/(\d{6})$/);
    if (fm) {
      const out = await fundDetail(fm[1], u.searchParams.get('days'), u.searchParams.get('fresh') === '1');
      return sendJson(res, out);
    }
    if (p === '/api/scale-agg') {
      const sp = u.searchParams;
      const group = sp.get('group') || 'all';
      const key = aggKey(sp.get('type'), sp.get('theme'), sp.get('q'), group);
      let job = aggJobs.get(key);
      if (!job || (!job.running && (job.cancel || job.error || job.done < job.total || sp.get('fresh') === '1'))) job = await startAgg(sp.get('type'), sp.get('theme'), sp.get('q'), group);
      return sendJson(res, job);
    }
    if (p === '/api/dev1-scan') {
      const sp = u.searchParams;
      const type = sp.get('type'), theme = sp.get('theme'), q = sp.get('q'), group = sp.get('group') || 'all';
      const action = sp.get('action') || 'status';
      const univ = await ensureUniverse(false);
      if (action === 'start') await startDevScan(type, theme, q, group);
      else if (action === 'stop') {
        const job = devJobs.get(devKey(type, theme, q, group));
        if (job && job.running) job.cancel = true;
      }
      return sendJson(res, devStatus(type, theme, q, group, univ));
    }
    sendJson(res, { error: '未知接口 ' + p }, 404);
  } catch (e) { sendErr(res, e); }
});
server.listen(PORT, '127.0.0.1', () => {
  log('盯盘神经数据桥已启动: http://127.0.0.1:' + PORT);
  ensureUniverse(false).then(u => log('基金全集就绪:', u.funds.length, '只, 净值日期', u.navDate)).catch(e => log('初始化失败(首次请求时重试):', e.message));
});