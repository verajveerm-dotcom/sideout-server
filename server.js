// Sideout online server: parties with 6-character codes, quick match, and a reliable relay between two players.
// Why Socket.IO: automatic reconnection, fallbacks when WebSockets are blocked, simple acks, and it runs on every
// beginner-friendly host. Colyseus is built around running the game simulation on the server, which this game
// doesn't need: both players run the same deterministic simulation and the host is the authority.
const http = require('http');
const { Server } = require('socket.io');

const PORT = Number(process.env.PORT || 3000);
const ORIGINS = (process.env.ALLOWED_ORIGINS || '*').split(',').map(s => s.trim()).filter(Boolean);
const PARTY_TTL_MS = Number(process.env.PARTY_TTL_MIN || 15) * 60e3;      // inactivity before a party expires
const RECONNECT_MS = Number(process.env.RECONNECT_SEC || 30) * 1e3;       // time to come back before a forfeit
const CREATE_PER_MIN = Number(process.env.CREATE_PER_MIN || 10);         // party creations per minute per address
const NET_V = 2;
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';                     // no 0/O, 1/I/L
const BAD = ['fuck','shit','bitch','cunt','nigg','fag','dick','cock','pussy','slut','whore','rape','nazi','retard','twat','wank'];

const parties = new Map();   // code -> party
const queue = [];            // quick match: [{sid, tab, name, at}]
const recentWaits = [];

function newCode() { let c; do { c = Array.from({ length: 6 }, () => CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]).join(''); } while (parties.has(c)); return c; }
function cleanName(n) {
  let s = String(n || '').replace(/[\u0000-\u001f\u007f-\u009f<>]/g, '').replace(/\s+/g, ' ').trim().slice(0, 16);
  const low = s.toLowerCase().replace(/[01345@$]/g, c => ({ 0: 'o', 1: 'i', 3: 'e', 4: 'a', 5: 's', '@': 'a', $: 's' }[c]));
  return BAD.some(w => low.includes(w)) || !s ? 'Player' : s;
}
const normCode = c => String(c || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 6);
const okTab = t => typeof t === 'string' && /^[a-z0-9]{4,16}$/.test(t);

// token bucket per socket: about 30 messages a second with bursts of 60
function limiter(rate, burst) { let tokens = burst, last = Date.now(); return () => { const now = Date.now(); tokens = Math.min(burst, tokens + (now - last) / 1000 * rate); last = now; if (tokens < 1) return false; tokens--; return true; }; }
const createLimits = new Map(); // ip -> [timestamps]
function canCreate(ip) { const now = Date.now(), list = (createLimits.get(ip) || []).filter(t => now - t < 60e3); if (list.length >= CREATE_PER_MIN) return false; list.push(now); createLimits.set(ip, list); return true; }

// a packet from the client's reliable layer: {k:'d',n,d} data, {k:'a',n} ack, {k:'p'|'q',t} ping
function validPkt(p) {
  if (!p || typeof p !== 'object' || typeof p.k !== 'string') return false;
  if (p.k === 'a') return Number.isInteger(p.n) && p.n > 0;
  if (p.k === 'p' || p.k === 'q') return typeof p.t === 'number';
  if (p.k !== 'd' || !Number.isInteger(p.n) || p.n < 1 || p.n > 1e7) return false;
  const d = p.d; if (!d || typeof d !== 'object' || typeof d.t !== 'string') return false;
  const allowed = ['hello','lobby','ready','start','dec','rally','state','rejoin','bye','rematch','bg','go'];
  if (!allowed.includes(d.t)) return false;
  // decisions are small: a number, a bool, a short string, or a small object of those
  if (d.t === 'dec' && !(Number.isInteger(d.n) && d.n > 0 && Number.isInteger(d.e))) return false;
  return true;
}

const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end(`Sideout server ok. ${parties.size} parties, ${queue.length} waiting.`); return; }
  res.writeHead(404); res.end();
});
const io = new Server(server, {
  cors: { origin: ORIGINS.includes('*') ? true : ORIGINS, methods: ['GET', 'POST'] },
  maxHttpBufferSize: 8 * 1024,          // ignore anything big
  pingInterval: 10000, pingTimeout: 8000,
});

function seat(p, role) { return role === 'host' ? p.host : p.guest; }
function other(role) { return role === 'host' ? 'guest' : 'host'; }
function touch(p) { p.touched = Date.now(); }
function sendTo(p, role, ev, data) { const s = seat(p, role); if (s && s.sid) io.to(s.sid).emit(ev, data); }
function closeParty(p, reason) {
  if (p.closed) return; p.closed = true;
  for (const r of ['host', 'guest']) { const s = seat(p, r); if (s) { sendTo(p, r, 'closed', { reason }); const sock = io.sockets.sockets.get(s.sid); if (sock) sock.data.party = null; } clearTimeout(s && s.timer); }
  parties.delete(p.code);
}
function attach(socket, p, role) {
  const s = seat(p, role); s.sid = socket.id; s.online = true; clearTimeout(s.timer); s.timer = null;
  socket.data.party = p.code; socket.data.role = role; touch(p);
  sendTo(p, other(role), 'peer', { status: 'online' });
}

io.on('connection', socket => {
  const ip = (socket.handshake.headers['x-forwarded-for'] || socket.handshake.address || '').split(',')[0].trim();
  const allow = limiter(30, 60);
  const party = () => parties.get(socket.data.party || '');
  const safe = fn => (data, ack) => { try { if (!allow()) return typeof ack === 'function' && ack({ error: 'rate' }); fn(data && typeof data === 'object' ? data : {}, typeof ack === 'function' ? ack : () => {}); } catch (e) { console.warn(e); } };

  socket.on('create', safe((d, ack) => {
    if (d.v !== NET_V) return ack({ error: 'version' });
    if (!okTab(d.tab)) return ack({ error: 'server' });
    if (!canCreate(ip)) return ack({ error: 'rate' });
    leaveCurrent(socket, 'left');
    const code = newCode();
    const p = { code, state: 'lobby', touched: Date.now(), host: { tab: d.tab, name: cleanName(d.name), sid: null, online: false }, guest: null, result: null, reports: {} };
    parties.set(code, p); attach(socket, p, 'host'); ack({ code });
  }));
  socket.on('join', safe((d, ack) => {
    if (d.v !== NET_V) return ack({ error: 'version' });
    const p = parties.get(normCode(d.code));
    if (!p || p.closed) return ack({ error: 'not_found' });
    if (p.host.tab === d.tab) return ack({ error: 'own' });
    const mine = p.guest && p.guest.tab === d.tab;
    if (p.state !== 'lobby' && !mine) return ack({ error: 'started' });
    if (p.guest && !mine) return ack({ error: 'full' });
    leaveCurrent(socket, 'left');
    p.guest = p.guest || { tab: d.tab, name: cleanName(d.name), sid: null, online: false };
    attach(socket, p, 'guest'); ack({ ok: true, hostName: p.host.name });
  }));
  socket.on('rejoin', safe((d, ack) => {
    const p = parties.get(normCode(d.code)), role = d.role === 'host' ? 'host' : 'guest';
    if (!p || p.closed || !seat(p, role) || seat(p, role).tab !== d.tab) return ack({ error: 'not_found' });
    attach(socket, p, role); const o = seat(p, other(role));
    ack({ ok: true, peerOnline: !!(o && o.online), snap: p.snap || null });
  }));
  socket.on('pkt', pkt => {
    if (!allow() || !validPkt(pkt)) return;
    const p = party(); if (!p) return;
    const role = socket.data.role, to = seat(p, other(role)); touch(p);
    if (pkt.k === 'd') {
      const t = pkt.d.t;
      if (t === 'start') { if (role !== 'host') return; p.state = 'playing'; p.reports = {}; }
      if (t === 'rally') { if (role !== 'host') return; p.score = pkt.d.score; }   // the host reports, the server keeps the authoritative tally
    }
    if (to && to.sid) io.to(to.sid).emit('pkt', pkt);
  });
  socket.on('state', d => { const p = party(); if (!p || socket.data.role !== 'host' || !d || typeof d !== 'object') return;
    if (d.state === 'started') p.state = 'playing'; if (d.state === 'lobby') p.state = 'lobby';
    if ('snap' in d) p.snap = d.snap && JSON.stringify(d.snap).length < 6000 ? d.snap : null; touch(p); });
  socket.on('leave', safe((d, ack) => { leaveCurrent(socket, d.reason === 'host_left' ? 'host_left' : 'left'); ack({ ok: true }); }));
  socket.on('quick', safe((d, ack) => {
    if (d.v !== NET_V) return ack({ error: 'version' });
    if (!okTab(d.tab)) return ack({ error: 'server' });
    if (!queue.find(q => q.sid === socket.id)) queue.push({ sid: socket.id, tab: d.tab, name: cleanName(d.name), at: Date.now() });
    matchQueue(); const avg = recentWaits.length ? recentWaits.reduce((a, b) => a + b, 0) / recentWaits.length : 20;
    ack({ ok: true, estimate: Math.round(avg) });
  }));
  socket.on('quickCancel', () => { const i = queue.findIndex(q => q.sid === socket.id); if (i >= 0) queue.splice(i, 1); });
  socket.on('disconnect', () => {
    const i = queue.findIndex(q => q.sid === socket.id); if (i >= 0) queue.splice(i, 1);
    const p = party(); if (!p) return; const role = socket.data.role, s = seat(p, role); if (!s || s.sid !== socket.id) return;
    s.online = false; s.sid = null; sendTo(p, other(role), 'peer', { status: 'offline' });
    // lobby: a host who doesn't come back closes the party; in a match the player gets 30 seconds, then forfeits
    const grace = p.state === 'playing' ? RECONNECT_MS : role === 'host' ? 10e3 : RECONNECT_MS;
    s.timer = setTimeout(() => {
      if (s.online) return;
      if (p.state === 'playing') { p.result = { winner: other(role), forfeit: true }; sendTo(p, other(role), 'peer', { status: 'left' }); }
      if (role === 'host') closeParty(p, 'host_left'); else { p.guest = null; p.state = 'lobby'; sendTo(p, 'host', 'peer', { status: 'left' }); }
    }, grace);
  });
});
function leaveCurrent(socket, reason) {
  const p = parties.get(socket.data.party || ''); if (!p) return; const role = socket.data.role;
  if (role === 'host') closeParty(p, 'host_left');
  else { p.guest = null; p.state = 'lobby'; sendTo(p, 'host', 'peer', { status: 'left' }); }
  socket.data.party = null;
}
function matchQueue() {
  while (queue.length >= 2) {
    const a = queue.shift(), b = queue.shift();
    const sa = io.sockets.sockets.get(a.sid), sb = io.sockets.sockets.get(b.sid);
    if (!sa) { if (sb) queue.unshift(b); continue; } if (!sb) { queue.unshift(a); continue; }
    const code = newCode(), now = Date.now();
    const p = { code, state: 'lobby', touched: now, host: { tab: a.tab, name: a.name, sid: null, online: false }, guest: { tab: b.tab, name: b.name, sid: null, online: false }, reports: {} };
    parties.set(code, p); attach(sa, p, 'host'); attach(sb, p, 'guest');
    sa.emit('matched', { code, role: 'host' }); sb.emit('matched', { code, role: 'guest' });
    recentWaits.push((now - a.at) / 1000, (now - b.at) / 1000); while (recentWaits.length > 20) recentWaits.shift();
  }
}
// sweep: expire parties after 15 minutes without activity, drop stale queue entries
setInterval(() => {
  const now = Date.now();
  for (const p of parties.values()) if (now - p.touched > PARTY_TTL_MS) closeParty(p, 'expired');
  for (let i = queue.length - 1; i >= 0; i--) if (!io.sockets.sockets.get(queue[i].sid) || now - queue[i].at > 10 * 60e3) queue.splice(i, 1);
}, Math.min(30e3, Math.max(250, PARTY_TTL_MS / 3))).unref();

if (require.main === module) server.listen(PORT, () => console.log(`Sideout server listening on ${PORT}`));
module.exports = { server, io, parties };
