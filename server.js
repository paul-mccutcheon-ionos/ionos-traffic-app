require('dotenv').config({ path: require('path').join(__dirname, '.env') });
const express = require('express');
const fetch   = require('node-fetch');
const path    = require('path');
const bcrypt  = require('bcryptjs');
const dns     = require('dns').promises;
const zlib    = require('zlib');
const { S3Client, CreateBucketCommand, HeadBucketCommand,
        ListObjectsV2Command, GetObjectCommand,
        PutBucketLifecycleConfigurationCommand,
        GetBucketLifecycleConfigurationCommand } = require('@aws-sdk/client-s3');

const app  = express();
const PORT = process.env.PORT || 3000;

const CLOUD_API   = 'https://api.ionos.com/cloudapi/v6';
const BILLING_API = 'https://api.ionos.com/billing';

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ── Meter ID metadata ───────────────────────────────────────────────────────

const TRAFFIC_METER_INFO = {
  CTI1000: { label: '1 GB cumulative traffic inbound',               dir: 'in'  },
  CTO0000: { label: '1 GB cumulative traffic outbound, first 2 TB',  dir: 'out' },
  CTO1100: { label: '1 GB cumulative traffic outbound, next 8 TB',   dir: 'out' },
  CTO1200: { label: '1 GB cumulative traffic outbound, next 40 TB',  dir: 'out' },
};

const S3_METER_INFO = {
  S3TI2100: { label: '1 GB Object Storage external inbound data',                                                dir: 'in'  },
  S3TI2200: { label: '1 GB Object Storage internal inbound data transfer',                                       dir: 'in'  },
  S3TO2100: { label: '1 GB Object Storage common traffic outbound',                                              dir: 'out' },
  S3TO2200: { label: '1 GB Object Storage internal traffic outbound',                                            dir: 'out' },
  S3TO2300: { label: '1 GB Object Storage IONOS national outbound data transfer for contract-owned buckets',     dir: 'out' },
};

// ── Helpers ─────────────────────────────────────────────────────────────────

async function cloudGet(urlPath, token) {
  const r = await fetch(`${CLOUD_API}${urlPath}`, {
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' }
  });
  if (!r.ok) {
    const t   = await r.text();
    const err = new Error(`Cloud API ${urlPath} → ${r.status}: ${t}`);
    err.status = r.status;
    throw err;
  }
  return r.json();
}

async function billingGet(urlPath, token) {
  const r = await fetch(`${BILLING_API}${urlPath}`, {
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' }
  });
  const body = await r.text();
  if (!r.ok) {
    const err = new Error(`Billing API ${urlPath} → ${r.status}: ${body}`);
    err.status = r.status;
    throw err;
  }
  try { return JSON.parse(body); } catch { return {}; }
}

function isPrivateIP(ip) {
  const p = ip.split('.').map(Number);
  if (p.length !== 4 || p.some(isNaN)) return true;
  return p[0] === 10 ||
    (p[0] === 172 && p[1] >= 16 && p[1] <= 31) ||
    (p[0] === 192 && p[1] === 168);
}

async function getDatacenterIPs(datacenterId, token) {
  const ipMap = new Map();
  const [natGws, nlbs, albs, servers] = await Promise.allSettled([
    cloudGet(`/datacenters/${datacenterId}/natgateways?depth=1`, token),
    cloudGet(`/datacenters/${datacenterId}/networkloadbalancers?depth=2`, token),
    cloudGet(`/datacenters/${datacenterId}/applicationloadbalancers?depth=2`, token),
    cloudGet(`/datacenters/${datacenterId}/servers?depth=3`, token),
  ]);
  if (natGws.status === 'fulfilled')
    for (const g of natGws.value?.items || [])
      for (const ip of g.properties?.publicIps || [])
        ipMap.set(ip, { type: 'NAT Gateway', resourceName: g.properties?.name || '', resourceId: g.id || '' });
  if (nlbs.status === 'fulfilled')
    for (const n of nlbs.value?.items || []) {
      const m = { type: 'Network Load Balancer', resourceName: n.properties?.name || '', resourceId: n.id || '' };
      for (const ip of n.properties?.ips || []) ipMap.set(ip, m);
      for (const cidr of n.properties?.lbPrivateIps || []) {
        const ip = cidr.split('/')[0];
        if (ip && !ipMap.has(ip)) ipMap.set(ip, m);
      }
    }
  if (albs.status === 'fulfilled')
    for (const a of albs.value?.items || []) {
      const m = { type: 'Application Load Balancer', resourceName: a.properties?.name || '', resourceId: a.id || '' };
      for (const ip of a.properties?.ips || []) ipMap.set(ip, m);
      for (const cidr of a.properties?.lbPrivateIps || []) {
        const ip = cidr.split('/')[0];
        if (ip && !ipMap.has(ip)) ipMap.set(ip, m);
      }
    }
  if (servers.status === 'fulfilled')
    for (const s of servers.value?.items || []) {
      const sName   = s.properties?.name || '';
      const isK8s   = /nodepool/i.test(sName);
      for (const nic of s.entities?.nics?.items || []) {
        const nicName = nic.properties?.name || nic.id.slice(0, 8);
        const nicIsK8s = isK8s || /^k8s/i.test(nicName);
        for (const ip of nic.properties?.ips || [])
          if (!ipMap.has(ip))
            ipMap.set(ip, {
              type: nicIsK8s ? 'K8s Worker' : 'Server',
              resourceName: sName,
              nicName,
              nicId: nic.id || '',
              lan:   nic.properties?.lan ?? null
            });
      }
    }
  return ipMap;
}

// Try to extract a Map<meterId, {totalBytes}> from various possible billing response shapes
function extractMeters(data) {
  const m = new Map();
  if (!data) return m;

  // Shape A: trafficObj.meters = [{meterId, total|quantity|bytes}, ...]
  if (Array.isArray(data?.trafficObj?.meters)) {
    for (const e of data.trafficObj.meters) {
      const id  = e.meterId || e.id || e.meter;
      const val = e.total ?? e.quantity ?? e.bytes ?? 0;
      if (id) m.set(id, { totalBytes: val });
    }
  }

  // Shape B: trafficObj flat { CTI1000: <bytes>, ... }
  if (m.size === 0 && data?.trafficObj && typeof data.trafficObj === 'object') {
    for (const [k, v] of Object.entries(data.trafficObj))
      if (k !== 'ip' && typeof v === 'number') m.set(k, { totalBytes: v });
  }

  // Shape C: top-level meters array
  if (m.size === 0 && Array.isArray(data?.meters)) {
    for (const e of data.meters) {
      const id  = e.meterId || e.id;
      const val = e.total ?? e.quantity ?? 0;
      if (id) m.set(id, { totalBytes: val });
    }
  }

  // Shape D: top-level flat object with meter-ID-like keys
  if (m.size === 0 && typeof data === 'object') {
    for (const [k, v] of Object.entries(data))
      if (typeof v === 'number' && /^[A-Z0-9]{6,9}$/.test(k)) m.set(k, { totalBytes: v });
  }

  return m;
}

const toGB4   = b => Math.floor((b / 1073741824) * 10000) / 10000;
const toGB2   = b => Math.floor((b / 1073741824) * 100)   / 100;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// ── Routes ──────────────────────────────────────────────────────────────────

app.post('/api/list-ips', async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: 'Bearer token is required.' });
  try {
    const data   = await cloudGet('/datacenters?depth=1', token);
    const allDcs = data?.items || [];
    const settled = await Promise.allSettled(
      allDcs.map(dc =>
        getDatacenterIPs(dc.id, token).then(ipMap => ({
          vdcName:  dc.properties?.name     || dc.id,
          location: dc.properties?.location || '—',
          ips: Array.from(ipMap.entries()).map(([ip, meta]) => ({ ip, deviceType: meta.type, ...meta }))
        }))
      )
    );
    const ipList = [];
    for (const r of settled) {
      if (r.status !== 'fulfilled') continue;
      const { vdcName, location, ips } = r.value;
      for (const e of ips) ipList.push({ ...e, vdcName, location });
    }
    ipList.sort((a, b) =>
      a.vdcName.localeCompare(b.vdcName) ||
      a.ip.localeCompare(b.ip, undefined, { numeric: true })
    );
    res.json({ ips: ipList });
  } catch (err) {
    res.status(err.status || 502).json({ error: err.message });
  }
});

app.post('/api/list-vdcs', async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: 'Bearer token is required.' });
  try {
    const data = await cloudGet('/datacenters?depth=1', token);
    const vdcs = (data?.items || [])
      .map(dc => ({ id: dc.id, name: dc.properties?.name || '—', location: dc.properties?.location || '—' }))
      .sort((a, b) => a.name.localeCompare(b.name));
    res.json({ vdcs });
  } catch (err) {
    res.status(err.status || 502).json({ error: err.message });
  }
});

// Single-IP traffic query
app.post('/api/query-ip', async (req, res) => {
  const { token, contractId, period, targetIp } = req.body;
  if (!token || !contractId || !period || !targetIp)
    return res.status(400).json({ error: 'All fields are required.' });
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(period))
    return res.status(400).json({ error: 'Invalid billing period. Use YYYY-MM.' });
  if (!/^(\d{1,3}\.){3}\d{1,3}$/.test(targetIp))
    return res.status(400).json({ error: 'Invalid IP address format.' });

  const [fqdn, aRecords] = await Promise.all([
    dns.reverse(targetIp).then(h => h[0] || null).catch(() => null),
    fetch(`https://api.hackertarget.com/reverseiplookup/?q=${encodeURIComponent(targetIp)}`)
      .then(r => r.text())
      .then(t => {
        const s = t.trim();
        if (!s || s.startsWith('error') || s.toLowerCase().includes('no dns')) return [];
        return s.split('\n').map(x => x.trim()).filter(Boolean);
      })
      .catch(() => [])
  ]);

  let data;
  try {
    data = await billingGet(
      `/${encodeURIComponent(contractId)}/traffic/${encodeURIComponent(period)}?ip=true`,
      token
    );
  } catch (err) {
    return res.status(err.status || 502).json({ error: err.message });
  }

  const ipList = data?.trafficObj?.ip;
  if (!Array.isArray(ipList))
    return res.status(404).json({ error: 'No IP traffic data found in billing response.' });

  const ipBlock = ipList.find(e => e.ip === targetIp);
  if (!ipBlock)
    return res.status(404).json({ error: `No billing records for IP ${targetIp} in ${period}.` });

  const vdcName = ipBlock.vdcName || '';
  const fqdnLow = (fqdn || '').toLowerCase();
  const vdcLow  = vdcName.toLowerCase();
  const vdcHas  = t => new RegExp(`[.\\s-]${t}([.\\s-]|$)`, 'i').test(vdcName);

  const deviceType =
    vdcHas('nlb') || fqdnLow.includes('nlb')                        ? 'Network Load Balancer'
    : vdcHas('lb')  || /\blb\b/i.test(fqdnLow)                      ? 'Application Load Balancer'
    : vdcHas('nat') || fqdnLow.includes('nat-gw')                   ? 'NAT Gateway'
    : /k8s|kube|kubernetes/i.test(vdcLow + fqdnLow)                 ? 'Kubernetes Node'
    : 'Standard VDC / Cluster';

  const dates        = ipBlock.dates || [];
  const totalInBytes  = dates.reduce((s, d) => s + (d.In  ?? d.in  ?? 0), 0);
  const totalOutBytes = dates.reduce((s, d) => s + (d.Out ?? d.out ?? 0), 0);

  res.json({
    summary: {
      ip: ipBlock.ip, fqdn, aRecords, deviceType, deviceName: vdcName,
      deviceUUID:  ipBlock.vdcUUID || '',
      totalInGB:   toGB2(totalInBytes),
      totalOutGB:  toGB2(totalOutBytes),
    },
    dates: dates.map(d => ({
      date:  d.Date || d.date || d.day || '—',
      inGB:  toGB4(d.In  ?? d.in  ?? 0),
      outGB: toGB4(d.Out ?? d.out ?? 0),
    }))
  });
});

// VDC aggregate traffic query + meter ID breakdown
app.post('/api/query-vdc', async (req, res) => {
  const { token, contractId, period, vdcQuery } = req.body;
  if (!token || !contractId || !period || !vdcQuery)
    return res.status(400).json({ error: 'All fields are required.' });
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(period))
    return res.status(400).json({ error: 'Invalid billing period. Use YYYY-MM.' });

  const query = vdcQuery.trim();
  if (query !== '__ALL__' && query.length < 2)
    return res.status(400).json({ error: 'VDC name must be ≥ 2 characters.' });

  let allDcs;
  try {
    const d = await cloudGet('/datacenters?depth=1', token);
    allDcs  = d?.items || [];
  } catch (err) {
    return res.status(err.status || 502).json({ error: `Cloud API: ${err.message}` });
  }

  let matched;
  if (query === '__ALL__') {
    matched = allDcs;
  } else {
    const isUUID = UUID_RE.test(query);
    matched = allDcs.filter(dc =>
      isUUID
        ? dc.id?.toLowerCase() === query.toLowerCase()
        : dc.properties?.name?.toLowerCase().includes(query.toLowerCase())
    );
    if (!matched.length)
      return res.status(404).json({ error: `No datacenter matched "${query}".` });
  }

  let ipMaps, billingIps, metersData;
  try {
    [ipMaps, billingIps, metersData] = await Promise.all([
      Promise.all(matched.map(dc => getDatacenterIPs(dc.id, token).then(m => ({ dc, ipMap: m })))),
      billingGet(
        `/${encodeURIComponent(contractId)}/traffic/${encodeURIComponent(period)}?ip=true`,
        token
      ).then(d => {
        const list = d?.trafficObj?.ip;
        if (!Array.isArray(list))
          throw Object.assign(new Error('No IP billing data in response.'), { status: 404 });
        return list;
      }),
      billingGet(
        `/${encodeURIComponent(contractId)}/traffic/${encodeURIComponent(period)}`,
        token
      ).catch(() => null)
    ]);
  } catch (err) {
    return res.status(err.status || 502).json({ error: err.message });
  }

  const allIpMeta = new Map();
  for (const { dc, ipMap } of ipMaps) {
    const dcName = dc.properties?.name || dc.id;
    for (const [ip, meta] of ipMap) allIpMeta.set(ip, { ...meta, dcName });
  }

  const matchedBilling = billingIps.filter(e => allIpMeta.has(e.ip));
  if (!matchedBilling.length)
    return res.status(404).json({
      error: `VDC found with ${allIpMeta.size} IP(s) but none had billable traffic in ${period}.`
    });

  const ips = matchedBilling.map(entry => {
    const dates = entry.dates || [];
    const inB   = dates.reduce((s, d) => s + (d.In  ?? d.in  ?? 0), 0);
    const outB  = dates.reduce((s, d) => s + (d.Out ?? d.out ?? 0), 0);
    const meta  = allIpMeta.get(entry.ip) || { type: 'Unknown', resourceName: '' };
    return { ip: entry.ip, deviceType: meta.type, resourceName: meta.resourceName,
             nicName: meta.nicName || '', nicId: meta.nicId || '', lan: meta.lan ?? null,
             totalInGB: toGB2(inB), totalOutGB: toGB2(outB) };
  }).sort((a, b) => b.totalOutGB - a.totalOutGB);

  const dailyMap = new Map();
  for (const entry of matchedBilling)
    for (const d of entry.dates || []) {
      const date = d.Date || d.date || d.day || '—';
      if (!dailyMap.has(date)) dailyMap.set(date, { inB: 0, outB: 0 });
      const b = dailyMap.get(date);
      b.inB  += d.In  ?? d.in  ?? 0;
      b.outB += d.Out ?? d.out ?? 0;
    }
  const dates = Array.from(dailyMap.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, { inB, outB }]) => ({ date, inGB: toGB4(inB), outGB: toGB4(outB) }));

  const totalInGB  = Math.floor(ips.reduce((s, ip) => s + ip.totalInGB,  0) * 100) / 100;
  const totalOutGB = Math.floor(ips.reduce((s, ip) => s + ip.totalOutGB, 0) * 100) / 100;

  // Meter breakdown — try API response first, fall back to tier calculation for traffic meters
  const apiMeters = extractMeters(metersData);

  const buildMeters = ids => {
    const result = {};
    for (const id of ids) {
      const m = apiMeters.get(id);
      result[id] = m
        ? { totalGB: toGB4(m.totalBytes), found: true, source: 'api' }
        : { totalGB: null, found: false, source: 'none' };
    }
    return result;
  };

  const trafficMeters = buildMeters(Object.keys(TRAFFIC_METER_INFO));
  const s3Meters      = buildMeters(Object.keys(S3_METER_INFO));

  // If API returned no traffic meter data, calculate tiers from the IP-level totals
  if (Object.values(trafficMeters).every(m => !m.found) && (totalInGB > 0 || totalOutGB > 0)) {
    trafficMeters.CTI1000 = { totalGB: totalInGB,                                         found: true, source: 'calculated' };
    trafficMeters.CTO0000 = { totalGB: Math.min(totalOutGB, 2048),                        found: true, source: 'calculated' };
    trafficMeters.CTO1100 = { totalGB: Math.max(0, Math.min(totalOutGB - 2048, 8192)),    found: true, source: 'calculated' };
    trafficMeters.CTO1200 = { totalGB: Math.max(0, Math.min(totalOutGB - 10240, 40960)), found: true, source: 'calculated' };
  }

  res.json({
    summary: {
      vdcName: matched.map(dc => dc.properties?.name || dc.id).join(' + '),
      vdcUUID: matched.map(dc => dc.id).join(', '),
      ipCount: matchedBilling.length, totalIpsInCloud: allIpMeta.size,
      totalInGB, totalOutGB
    },
    ips, dates, trafficMeters, s3Meters
  });
});

// S3 Object Storage traffic query (contract-wide)
app.post('/api/query-s3', async (req, res) => {
  const { token, contractId, period } = req.body;
  if (!token || !contractId || !period)
    return res.status(400).json({ error: 'token, contractId and period are required.' });
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(period))
    return res.status(400).json({ error: 'Invalid billing period. Use YYYY-MM.' });

  let metersData;
  try {
    metersData = await billingGet(
      `/${encodeURIComponent(contractId)}/traffic/${encodeURIComponent(period)}`,
      token
    );
  } catch (err) {
    return res.status(err.status || 502).json({ error: err.message });
  }

  const apiMeters = extractMeters(metersData);

  const buildMeters = ids => {
    const result = {};
    for (const id of ids) {
      const m = apiMeters.get(id);
      result[id] = m
        ? { totalGB: toGB4(m.totalBytes), found: true }
        : { totalGB: null, found: false };
    }
    return result;
  };

  const s3Meters      = buildMeters(Object.keys(S3_METER_INFO));
  const trafficMeters = buildMeters(Object.keys(TRAFFIC_METER_INFO));

  const s3InGB  = ['S3TI2100', 'S3TI2200']
    .reduce((s, id) => s + (s3Meters[id]?.totalGB || 0), 0);
  const s3OutGB = ['S3TO2100', 'S3TO2200', 'S3TO2300']
    .reduce((s, id) => s + (s3Meters[id]?.totalGB || 0), 0);

  res.json({
    s3Meters, trafficMeters,
    totals: {
      inGB:  Math.floor(s3InGB  * 10000) / 10000,
      outGB: Math.floor(s3OutGB * 10000) / 10000
    },
    allMeterIds: Array.from(apiMeters.keys())
  });
});

// ── Projection helpers ───────────────────────────────────────────────────────

function getPreviousPeriods(current, count) {
  let [year, month] = current.split('-').map(Number);
  const out = [];
  for (let i = count; i >= 1; i--) {
    let m = month - i, y = year;
    while (m <= 0) { m += 12; y--; }
    out.push(`${y}-${String(m).padStart(2, '0')}`);
  }
  return out;
}

function getNextPeriod(current) {
  let [year, month] = current.split('-').map(Number);
  if (++month > 12) { month = 1; year++; }
  return `${year}-${String(month).padStart(2, '0')}`;
}

function linReg(pts) {
  const n = pts.length;
  if (n < 2) return null;
  const sx  = pts.reduce((s, p) => s + p.x, 0);
  const sy  = pts.reduce((s, p) => s + p.y, 0);
  const sxy = pts.reduce((s, p) => s + p.x * p.y, 0);
  const sx2 = pts.reduce((s, p) => s + p.x * p.x, 0);
  const d   = n * sx2 - sx * sx;
  if (Math.abs(d) < 1e-10) return { slope: 0, intercept: sy / n };
  const slope = (n * sxy - sx * sy) / d;
  return { slope, intercept: (sy - slope * sx) / n };
}

function calcProjection(history, nextPeriod) {
  const valid = history.filter(h => h.ok && h.inGB != null);
  if (valid.length < 2) return { period: nextPeriod, insufficient: true };
  const nx  = valid.length;
  const ir  = linReg(valid.map((h, i) => ({ x: i, y: h.inGB  })));
  const or_ = linReg(valid.map((h, i) => ({ x: i, y: h.outGB })));
  const ig = [], og = [];
  for (let i = 1; i < valid.length; i++) {
    if (valid[i-1].inGB  > 0) ig.push((valid[i].inGB  - valid[i-1].inGB)  / valid[i-1].inGB);
    if (valid[i-1].outGB > 0) og.push((valid[i].outGB - valid[i-1].outGB) / valid[i-1].outGB);
  }
  const avgIg = ig.length ? ig.reduce((a, b) => a + b, 0) / ig.length : 0;
  const avgOg = og.length ? og.reduce((a, b) => a + b, 0) / og.length : 0;
  return {
    period:      nextPeriod,
    inGB:        Math.round(Math.max(0, ir.slope  * nx + ir.intercept)  * 100) / 100,
    outGB:       Math.round(Math.max(0, or_.slope * nx + or_.intercept) * 100) / 100,
    inTrendPct:  Math.round(avgIg * 10000) / 100,
    outTrendPct: Math.round(avgOg * 10000) / 100,
    inSlope:     Math.round(ir.slope  * 100) / 100,
    outSlope:    Math.round(or_.slope * 100) / 100,
    dataPoints:  valid.length
  };
}

async function fetchMonthlyTotals(mode, token, contractId, period, targetIp, vdcIpSet) {
  if (mode === 'ip') {
    const data  = await billingGet(
      `/${encodeURIComponent(contractId)}/traffic/${encodeURIComponent(period)}?ip=true`, token);
    const list  = data?.trafficObj?.ip;
    if (!Array.isArray(list)) return { inGB: 0, outGB: 0 };
    const block = list.find(e => e.ip === targetIp);
    if (!block) return { inGB: 0, outGB: 0 };
    const dates = block.dates || [];
    return {
      inGB:  toGB2(dates.reduce((s, d) => s + (d.In  ?? d.in  ?? 0), 0)),
      outGB: toGB2(dates.reduce((s, d) => s + (d.Out ?? d.out ?? 0), 0))
    };
  }
  if (mode === 'vdc') {
    const data  = await billingGet(
      `/${encodeURIComponent(contractId)}/traffic/${encodeURIComponent(period)}?ip=true`, token);
    const list  = data?.trafficObj?.ip;
    if (!Array.isArray(list)) return { inGB: 0, outGB: 0 };
    let inB = 0, outB = 0;
    for (const e of list.filter(e => vdcIpSet.has(e.ip)))
      for (const d of e.dates || []) {
        inB  += d.In  ?? d.in  ?? 0;
        outB += d.Out ?? d.out ?? 0;
      }
    return { inGB: toGB2(inB), outGB: toGB2(outB) };
  }
  if (mode === 's3') {
    const data   = await billingGet(
      `/${encodeURIComponent(contractId)}/traffic/${encodeURIComponent(period)}`, token);
    const meters = extractMeters(data);
    const inGB   = ['S3TI2100','S3TI2200']
      .reduce((s, id) => { const m = meters.get(id); return s + (m ? toGB4(m.totalBytes) : 0); }, 0);
    const outGB  = ['S3TO2100','S3TO2200','S3TO2300']
      .reduce((s, id) => { const m = meters.get(id); return s + (m ? toGB4(m.totalBytes) : 0); }, 0);
    return { inGB: Math.round(inGB * 100) / 100, outGB: Math.round(outGB * 100) / 100 };
  }
  throw new Error('Unknown mode');
}

app.post('/api/project', async (req, res) => {
  const { token, contractId, currentPeriod, months, mode, targetIp, vdcQuery } = req.body;
  if (!token || !contractId || !currentPeriod || !months || !mode)
    return res.status(400).json({ error: 'Missing required fields.' });
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(currentPeriod))
    return res.status(400).json({ error: 'Invalid currentPeriod.' });

  const monthsInt  = Math.min(Math.max(parseInt(months, 10), 2), 24);
  const periods    = getPreviousPeriods(currentPeriod, monthsInt);
  const nextPeriod = getNextPeriod(currentPeriod);

  let vdcIpSet = null;
  if (mode === 'vdc') {
    if (!vdcQuery) return res.status(400).json({ error: 'vdcQuery required for VDC mode.' });
    try {
      const dcData  = await cloudGet('/datacenters?depth=1', token);
      const allDcs  = dcData?.items || [];
      const query   = vdcQuery.trim();
      const isUUID  = UUID_RE.test(query);
      const matched = query === '__ALL__' ? allDcs : allDcs.filter(dc =>
        isUUID ? dc.id?.toLowerCase() === query.toLowerCase()
               : dc.properties?.name?.toLowerCase().includes(query.toLowerCase())
      );
      if (!matched.length)
        throw Object.assign(new Error(`No datacenter matched "${query}"`), { status: 404 });
      const ipMaps = await Promise.all(matched.map(dc => getDatacenterIPs(dc.id, token)));
      vdcIpSet = new Set();
      for (const m of ipMaps) for (const ip of m.keys()) vdcIpSet.add(ip);
    } catch (err) {
      return res.status(err.status || 502).json({ error: err.message });
    }
  }
  if (mode === 'ip' && !targetIp)
    return res.status(400).json({ error: 'targetIp required for IP mode.' });

  const settled = await Promise.allSettled(
    periods.map(p => fetchMonthlyTotals(mode, token, contractId, p, targetIp, vdcIpSet))
  );
  const history = periods.map((period, i) => {
    const r = settled[i];
    return r.status === 'fulfilled'
      ? { period, inGB: r.value.inGB, outGB: r.value.outGB, ok: true }
      : { period, inGB: null, outGB: null, ok: false, error: r.reason?.message };
  });

  res.json({ history, projection: calcProjection(history, nextPeriod), nextPeriod });
});

app.post('/api/query-range', async (req, res) => {
  const { token, contractId, endPeriod, months, mode, targetIp, vdcQuery } = req.body;
  if (!token || !contractId || !endPeriod || !months || !mode)
    return res.status(400).json({ error: 'Missing required fields.' });
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(endPeriod))
    return res.status(400).json({ error: 'Invalid endPeriod. Use YYYY-MM.' });

  const monthsInt = Math.min(Math.max(parseInt(months, 10), 1), 24);
  const periods   = getPreviousPeriods(endPeriod, monthsInt - 1);
  periods.push(endPeriod);

  let vdcIpSet = null, vdcName = null, vdcUUID = null;
  if (mode === 'vdc') {
    if (!vdcQuery) return res.status(400).json({ error: 'vdcQuery required for VDC mode.' });
    try {
      const dcData  = await cloudGet('/datacenters?depth=1', token);
      const allDcs  = dcData?.items || [];
      const query   = vdcQuery.trim();
      const isUUID  = UUID_RE.test(query);
      const matched = query === '__ALL__' ? allDcs : allDcs.filter(dc =>
        isUUID ? dc.id?.toLowerCase() === query.toLowerCase()
               : dc.properties?.name?.toLowerCase().includes(query.toLowerCase())
      );
      if (!matched.length)
        throw Object.assign(new Error(`No datacenter matched "${query}"`), { status: 404 });
      const ipMaps = await Promise.all(matched.map(dc => getDatacenterIPs(dc.id, token)));
      vdcIpSet = new Set();
      for (const m of ipMaps) for (const ip of m.keys()) vdcIpSet.add(ip);
      vdcName = matched.map(dc => dc.properties?.name || dc.id).join(' + ');
      vdcUUID = matched.map(dc => dc.id).join(', ');
    } catch (err) {
      return res.status(err.status || 502).json({ error: err.message });
    }
  }
  if (mode === 'ip' && !targetIp)
    return res.status(400).json({ error: 'targetIp required for IP mode.' });

  const settled = await Promise.allSettled(
    periods.map(p => fetchMonthlyTotals(mode, token, contractId, p, targetIp, vdcIpSet))
  );
  const monthlyData = periods.map((period, i) => {
    const r = settled[i];
    return r.status === 'fulfilled'
      ? { period, inGB: r.value.inGB, outGB: r.value.outGB, ok: true }
      : { period, inGB: 0, outGB: 0, ok: false, error: r.reason?.message };
  });

  const totalInGB  = Math.round(monthlyData.reduce((s, m) => s + (m.ok ? m.inGB  : 0), 0) * 100) / 100;
  const totalOutGB = Math.round(monthlyData.reduce((s, m) => s + (m.ok ? m.outGB : 0), 0) * 100) / 100;

  let dailyData = null;
  if (monthsInt <= 3 && mode !== 's3') {
    const dailySettled = await Promise.allSettled(
      periods.map(async p => {
        if (mode === 'ip') {
          const data  = await billingGet(
            `/${encodeURIComponent(contractId)}/traffic/${encodeURIComponent(p)}?ip=true`, token);
          const list  = data?.trafficObj?.ip;
          const block = Array.isArray(list) ? list.find(e => e.ip === targetIp) : null;
          return (block?.dates || []).map(d => ({
            date:  d.Date || d.date || d.day || '—',
            inGB:  toGB4(d.In  ?? d.in  ?? 0),
            outGB: toGB4(d.Out ?? d.out ?? 0)
          }));
        }
        if (mode === 'vdc') {
          const data = await billingGet(
            `/${encodeURIComponent(contractId)}/traffic/${encodeURIComponent(p)}?ip=true`, token);
          const list = data?.trafficObj?.ip;
          if (!Array.isArray(list)) return [];
          const dailyMap = new Map();
          for (const e of list.filter(e => vdcIpSet.has(e.ip)))
            for (const d of e.dates || []) {
              const date = d.Date || d.date || d.day || '—';
              if (!dailyMap.has(date)) dailyMap.set(date, { inB: 0, outB: 0 });
              const b = dailyMap.get(date);
              b.inB  += d.In  ?? d.in  ?? 0;
              b.outB += d.Out ?? d.out ?? 0;
            }
          return Array.from(dailyMap.entries())
            .map(([date, { inB, outB }]) => ({ date, inGB: toGB4(inB), outGB: toGB4(outB) }));
        }
        return [];
      })
    );
    const allDaily = dailySettled
      .flatMap(r => r.status === 'fulfilled' ? r.value : [])
      .sort((a, b) => a.date.localeCompare(b.date));
    if (allDaily.length > 0) dailyData = allDaily;
  }

  res.json({ monthlyData, totals: { inGB: totalInGB, outGB: totalOutGB }, vdcName, vdcUUID, mode, dailyData });
});

// ── Flow Logs ────────────────────────────────────────────────────────────────

const IONOS_S3_REGIONS = {
  'de/fra': { region: 'eu-central-1', endpoint: 'https://s3.eu-central-1.ionoscloud.com', label: 'Frankfurt' },
  'de/txl': { region: 'eu-central-2', endpoint: 'https://s3.eu-central-2.ionoscloud.com', label: 'Berlin' },
  'es/vit': { region: 'eu-south-2',   endpoint: 'https://s3.eu-south-2.ionoscloud.com',   label: 'Logroño' },
  'gb/lhr': { region: 'eu-central-1', endpoint: 'https://s3.eu-central-1.ionoscloud.com', label: 'Frankfurt (nearest)' },
  'us/las': { region: 'us-central-1', endpoint: 'https://s3.us-central-1.ionoscloud.com', label: 'Lenexa' },
  'us/ewr': { region: 'us-central-1', endpoint: 'https://s3.us-central-1.ionoscloud.com', label: 'Lenexa (nearest)' },
  'fr/par': { region: 'eu-central-1', endpoint: 'https://s3.eu-central-1.ionoscloud.com', label: 'Frankfurt (nearest)' },
};
const DEFAULT_S3 = { region: 'eu-central-1', endpoint: 'https://s3.eu-central-1.ionoscloud.com', label: 'Frankfurt' };

function makeS3(accessKey, secretKey, endpoint, region) {
  return new S3Client({
    endpoint, region,
    credentials: { accessKeyId: accessKey, secretAccessKey: secretKey },
    forcePathStyle: true,
  });
}

async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
}

// Returns datacenter match or throws
async function resolveDc(vdcQuery, token) {
  const d = await cloudGet('/datacenters?depth=1', token);
  const isUUID = UUID_RE.test(vdcQuery.trim());
  const matched = (d?.items || []).filter(dc =>
    isUUID ? dc.id?.toLowerCase() === vdcQuery.trim().toLowerCase()
           : dc.properties?.name?.toLowerCase().includes(vdcQuery.trim().toLowerCase())
  );
  if (!matched.length) throw Object.assign(new Error(`No datacenter matched "${vdcQuery}".`), { status: 404 });
  return matched[0];
}

// Fetch all flow-log-capable resources in a datacenter
async function fetchDcResources(dcId, token) {
  const [natRes, nlbRes, albRes, srvRes] = await Promise.allSettled([
    cloudGet(`/datacenters/${dcId}/natgateways?depth=1`, token),
    cloudGet(`/datacenters/${dcId}/networkloadbalancers?depth=1`, token),
    cloudGet(`/datacenters/${dcId}/applicationloadbalancers?depth=1`, token),
    cloudGet(`/datacenters/${dcId}/servers?depth=3`, token),
  ]);
  const resources = [];
  if (natRes.status === 'fulfilled')
    for (const g of natRes.value?.items || [])
      resources.push({ type: 'NAT Gateway', name: g.properties?.name || g.id.slice(0,8), id: g.id,
        flPath: `/datacenters/${dcId}/natgateways/${g.id}/flowlogs` });
  if (nlbRes.status === 'fulfilled')
    for (const n of nlbRes.value?.items || [])
      resources.push({ type: 'NLB', name: n.properties?.name || n.id.slice(0,8), id: n.id,
        flPath: `/datacenters/${dcId}/networkloadbalancers/${n.id}/flowlogs` });
  if (albRes.status === 'fulfilled')
    for (const a of albRes.value?.items || [])
      resources.push({ type: 'ALB', name: a.properties?.name || a.id.slice(0,8), id: a.id,
        flPath: `/datacenters/${dcId}/applicationloadbalancers/${a.id}/flowlogs` });
  if (srvRes.status === 'fulfilled')
    for (const s of srvRes.value?.items || [])
      for (const nic of s.entities?.nics?.items || [])
        resources.push({ type: 'Server NIC',
          name: `${s.properties?.name || s.id.slice(0,8)} / ${nic.properties?.name || nic.id.slice(0,8)}`,
          id: nic.id, serverId: s.id,
          flPath: `/datacenters/${dcId}/servers/${s.id}/nics/${nic.id}/flowlogs` });
  return resources;
}

// GET S3 region list (for frontend dropdown)
// GET /api/env-config — return credentials pre-loaded from .env
// If AUTH_PASSWORD_HASH is set, requires Basic Auth (contract ID as username, password as password)
app.get('/api/env-config', async (req, res) => {
  const hash       = process.env.AUTH_PASSWORD_HASH || '';
  const contractId = process.env.AUTH_CONTRACT_ID   || '';

  if (hash) {
    const authHeader = req.headers['authorization'] || '';
    const b64 = authHeader.startsWith('Basic ') ? authHeader.slice(6) : '';
    const [user, pass] = Buffer.from(b64, 'base64').toString().split(':');
    const valid = b64 && (await bcrypt.compare(pass || '', hash));
    if (!valid) {
      return res.status(401).json({ contractId, protected: true });
    }
  }

  res.json({
    ionosApiToken: process.env.IONOS_API_TOKEN      || '',
    s3AccessKey:   process.env.IONOS_S3_ACCESS_KEY  || '',
    s3SecretKey:   process.env.IONOS_S3_SECRET_KEY  || '',
    contractId:    process.env.IONOS_CONTRACT_ID    || '',
  });
});

app.get('/api/flowlogs/s3-regions', (req, res) => {
  res.json(Object.entries(IONOS_S3_REGIONS).map(([loc, r]) => ({ loc, ...r })));
});

// POST /api/flowlogs/setup — create bucket + activate flow logs on all resources
app.post('/api/flowlogs/setup', async (req, res) => {
  const { token, vdcQuery, s3AccessKey, s3SecretKey, bucketName, retentionDays = 0 } = req.body;
  if (!token || !vdcQuery || !s3AccessKey || !s3SecretKey || !bucketName)
    return res.status(400).json({ error: 'token, vdcQuery, s3AccessKey, s3SecretKey and bucketName are required.' });

  let dc;
  try { dc = await resolveDc(vdcQuery, token); }
  catch (e) { return res.status(e.status || 502).json({ error: e.message }); }

  const location = dc.properties?.location || '';
  const s3Info   = IONOS_S3_REGIONS[location] || DEFAULT_S3;
  const s3       = makeS3(s3AccessKey, s3SecretKey, s3Info.endpoint, s3Info.region);

  // Ensure bucket exists
  let bucketCreated = false;
  try {
    await s3.send(new HeadBucketCommand({ Bucket: bucketName }));
  } catch (e) {
    if (e.$metadata?.httpStatusCode === 404 || e.name === 'NoSuchBucket' || e.$metadata?.httpStatusCode === 403) {
      try {
        // AWS SDK v3 auto-injects LocationConstraint for any region != us-east-1.
        // IONOS S3 uses the endpoint URL to determine location, so we use a
        // us-east-1 client (suppresses LocationConstraint) pointed at the correct endpoint.
        const s3Create = makeS3(s3AccessKey, s3SecretKey, s3Info.endpoint, 'us-east-1');
        await s3Create.send(new CreateBucketCommand({ Bucket: bucketName }));
        bucketCreated = true;
      } catch (ce) {
        return res.status(502).json({ error: `S3 bucket creation failed: ${ce.message}` });
      }
    } else {
      return res.status(502).json({ error: `S3 access check failed: ${e.message}` });
    }
  }

  // Apply lifecycle retention policy if requested
  let lifecycleSet = false, lifecycleDays = null;
  const days = parseInt(retentionDays) || 0;
  if (days > 0) {
    try {
      await s3.send(new PutBucketLifecycleConfigurationCommand({
        Bucket: bucketName,
        LifecycleConfiguration: {
          Rules: [{
            ID: 'flow-log-expiry',
            Status: 'Enabled',
            Filter: { Prefix: '' },
            Expiration: { Days: days }
          }]
        }
      }));
      lifecycleSet = true;
      lifecycleDays = days;
    } catch (le) {
      // Non-fatal — log but continue
      console.error('Lifecycle policy error:', le.message);
    }
  } else if (days === 0 && retentionDays !== 0) {
    // retentionDays explicitly 0 means "keep forever" — remove any existing rule
    try {
      await s3.send(new PutBucketLifecycleConfigurationCommand({
        Bucket: bucketName,
        LifecycleConfiguration: { Rules: [] }
      }));
    } catch (_) {}
  }

  const resources = await fetchDcResources(dc.id, token);
  const activated = [], errors = [];

  const skipped = [];

  await Promise.all(resources.map(async r => {
    // Check if already has a flow log (also catches managed resources that return 403 on GET)
    try {
      const existResp = await fetch(`${CLOUD_API}${r.flPath}?depth=1`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      if (existResp.status === 403) {
        skipped.push({ ...r, reason: 'managed-resource', detail: 'HTTP 403 — managed resource, flow logs not supported' });
        return;
      }
      const existing = existResp.ok ? await existResp.json() : {};
      if ((existing?.items?.length || 0) > 0) {
        const fl = existing.items[0];
        const props = fl.properties || {};
        const needsUpdate = props.direction !== 'BIDIRECTIONAL' || props.action !== 'ALL';
        if (needsUpdate) {
          try {
            const targetBucket = props.bucket || bucketName;
            const safeName = props.name || `fl-${r.name.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-').slice(0, 24)}`;
            const patchResp = await fetch(`${CLOUD_API}${r.flPath}/${fl.id}`, {
              method: 'PATCH',
              headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
              body: JSON.stringify({ properties: { action: 'ALL', direction: 'BIDIRECTIONAL', bucket: targetBucket } })
            });
            if (patchResp.ok) {
              activated.push({ ...r, status: 'updated', flowLogId: fl.id, bucket: targetBucket });
            } else {
              // PATCH failed (likely immutable fields) — delete and recreate
              await fetch(`${CLOUD_API}${r.flPath}/${fl.id}`, {
                method: 'DELETE', headers: { Authorization: `Bearer ${token}` }
              });
              const postResp = await fetch(`${CLOUD_API}${r.flPath}`, {
                method: 'POST',
                headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
                body: JSON.stringify({ properties: { name: safeName, action: 'ALL', direction: 'BIDIRECTIONAL', bucket: targetBucket } })
              });
              const pd = await postResp.json();
              if (postResp.ok) activated.push({ ...r, status: 'updated', flowLogId: pd.id, bucket: targetBucket });
              else errors.push({ ...r, error: `Recreate failed: ${pd.message || postResp.status}` });
            }
          } catch (pe) { errors.push({ ...r, error: `Update error: ${pe.message}` }); }
        } else {
          activated.push({ ...r, status: 'already_active', flowLogId: fl.id, bucket: props.bucket });
        }
        return;
      }
    } catch (_) {}
    // Activate
    try {
      const safeName = `fl-${r.name.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-').slice(0, 24)}`;
      const resp = await fetch(`${CLOUD_API}${r.flPath}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ properties: { name: safeName, action: 'ALL', direction: 'BIDIRECTIONAL', bucket: bucketName } })
      });
      const d = await resp.json();
      if (!resp.ok) {
        if (resp.status === 403)
          skipped.push({ ...r, reason: 'managed-resource', detail: 'HTTP 403 — managed resource, flow logs not supported' });
        else
          errors.push({ ...r, error: d.message || `HTTP ${resp.status}` });
      } else {
        activated.push({ ...r, status: 'activated', flowLogId: d.id, bucket: bucketName });
      }
    } catch (e) {
      errors.push({ ...r, error: e.message });
    }
  }));

  res.json({ dcName: dc.properties?.name, dcLocation: location,
             s3Region: s3Info.region, s3Endpoint: s3Info.endpoint, s3Label: s3Info.label,
             bucketName, bucketCreated, lifecycleSet, lifecycleDays, activated, skipped, errors });
});

// POST /api/flowlogs/status — list current flow log config for all resources in VDC
app.post('/api/flowlogs/status', async (req, res) => {
  const { token, vdcQuery, s3AccessKey, s3SecretKey, bucketName, s3Endpoint, s3Region } = req.body;
  if (!token || !vdcQuery) return res.status(400).json({ error: 'token and vdcQuery required.' });
  let dc;
  try { dc = await resolveDc(vdcQuery, token); }
  catch (e) { return res.status(e.status || 502).json({ error: e.message }); }

  // Fetch bucket lifecycle retention if S3 creds provided
  let retentionDays = null;
  if (s3AccessKey && s3SecretKey && bucketName) {
    try {
      const s3 = makeS3(s3AccessKey, s3SecretKey, s3Endpoint || DEFAULT_S3.endpoint, s3Region || DEFAULT_S3.region);
      const lc = await s3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: bucketName }));
      const expiryRule = (lc.Rules || []).find(r => r.Status === 'Enabled' && r.Expiration?.Days);
      retentionDays = expiryRule ? expiryRule.Expiration.Days : 0;
    } catch (e) {
      // NoSuchLifecycleConfiguration = no rule set (keep forever)
      retentionDays = e.name === 'NoSuchLifecycleConfiguration' || e.$metadata?.httpStatusCode === 404 ? 0 : null;
    }
  }

  const resources = await fetchDcResources(dc.id, token);
  const results = await Promise.all(resources.map(async r => {
    try {
      const resp = await fetch(`${CLOUD_API}${r.flPath}?depth=1`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      if (resp.status === 403)
        return { ...r, hasFlowLog: false, restricted: true, flowLogs: [] };
      const fl = resp.ok ? await resp.json() : {};
      const items = fl?.items || [];
      return { ...r, hasFlowLog: items.length > 0,
        flowLogs: items.map(f => ({ id: f.id, name: f.properties?.name,
          action: f.properties?.action, direction: f.properties?.direction,
          bucket: f.properties?.bucket, state: f.metadata?.state })) };
    } catch (_) {
      return { ...r, hasFlowLog: false, flowLogs: [] };
    }
  }));

  res.json({ dcName: dc.properties?.name, retentionDays, bucketName: bucketName || null, resources: results });
});

// POST /api/flowlogs/read — read and parse flow log records from S3
app.post('/api/flowlogs/read', async (req, res) => {
  const { s3AccessKey, s3SecretKey, s3Endpoint, s3Region, bucketName, maxFiles = 50, sinceSeconds = 0, token, vdcQuery } = req.body;
  if (!s3AccessKey || !s3SecretKey || !bucketName)
    return res.status(400).json({ error: 's3AccessKey, s3SecretKey and bucketName required.' });

  // Optionally enrich IPs with device metadata from the VDC(s)
  let ipMeta = {};
  if (token && vdcQuery) {
    try {
      let dcList;
      if (vdcQuery === '__ALL__') {
        const d = await cloudGet('/datacenters?depth=1', token);
        dcList = d?.items || [];
      } else {
        dcList = [await resolveDc(vdcQuery, token)];
      }
      const maps = await Promise.allSettled(dcList.map(dc => getDatacenterIPs(dc.id, token)));
      for (const r of maps)
        if (r.status === 'fulfilled')
          for (const [ip, meta] of r.value)
            ipMeta[ip] = { type: meta.type, name: meta.resourceName || '', resourceId: meta.resourceId || '' };
    } catch (_) {}
  }

  const endpoint = s3Endpoint || DEFAULT_S3.endpoint;
  const region   = s3Region   || DEFAULT_S3.region;
  const s3       = makeS3(s3AccessKey, s3SecretKey, endpoint, region);
  const cutoff   = sinceSeconds > 0 ? Math.floor(Date.now() / 1000) - parseInt(sinceSeconds) : 0;

  let objects;
  try {
    const list = await s3.send(new ListObjectsV2Command({ Bucket: bucketName, MaxKeys: parseInt(maxFiles) }));
    objects = (list.Contents || []).sort((a, b) => b.LastModified - a.LastModified);
  } catch (e) {
    return res.status(502).json({ error: `S3 list failed: ${e.message}` });
  }
  if (!objects.length) return res.json({ flows: [], filesRead: 0, totalRecords: 0, message: 'No log files found yet — logs may take a few minutes to appear after activation.' });

  const PROTO = { '1': 'ICMP', '6': 'TCP', '17': 'UDP', '47': 'GRE', '50': 'ESP' };
  const flowMap = new Map();
  let totalRecords = 0, filteredRecords = 0;
  let dataStart = Infinity, dataEnd = 0;

  for (const obj of objects.slice(0, parseInt(maxFiles))) {
    try {
      const getResp = await s3.send(new GetObjectCommand({ Bucket: bucketName, Key: obj.Key }));
      const buf = await streamToBuffer(getResp.Body);
      let text;
      try { text = zlib.gunzipSync(buf).toString('utf8'); } catch { text = buf.toString('utf8'); }
      for (const line of text.split('\n')) {
        const p = line.trim().split(/\s+/);
        if (p.length < 14 || p[0] === 'version' || p[3] === '-') continue;
        totalRecords++;
        const [, , ifaceId, src, dst, sport, dport, proto, packets, bytes, start, end, action] = p;
        const startSec = parseInt(start) || 0;
        const endSec   = parseInt(end)   || 0;
        if (cutoff > 0 && startSec < cutoff) continue;
        filteredRecords++;
        if (startSec < dataStart) dataStart = startSec;
        if (endSec   > dataEnd)   dataEnd   = endSec;
        const key = `${src}|${dst}|${proto}|${dport}`;
        const e = flowMap.get(key) || { src, dst, proto: PROTO[proto] || proto, dport, sport, ifaceId, action, packets: 0, bytes: 0, count: 0, firstSeen: startSec, lastSeen: endSec };
        e.packets += parseInt(packets) || 0;
        e.bytes   += parseInt(bytes)   || 0;
        e.count++;
        if (startSec < e.firstSeen) e.firstSeen = startSec;
        if (endSec   > e.lastSeen)  e.lastSeen  = endSec;
        flowMap.set(key, e);
      }
    } catch (_) {}
  }

  const flows = Array.from(flowMap.values()).sort((a, b) => b.bytes - a.bytes).slice(0, 1000);
  res.json({ flows, ipMeta, filesRead: objects.length, totalRecords, filteredRecords,
    dataStart: dataStart === Infinity ? null : dataStart, dataEnd: dataEnd || null });
});


app.listen(PORT, () => {
  console.log(`IONOS Traffic Tool running at http://localhost:${PORT}`);
});
