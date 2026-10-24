// Run with: npm test   (starts the server on a free port with short timers and checks every feature)
process.env.RECONNECT_SEC = '1.5';
process.env.BG_RECONNECT_SEC = '4';
process.env.PARTY_TTL_MIN = String(6 / 60);   // 6 seconds, so expiry can be tested quickly
process.env.CREATE_PER_MIN = '30';
const { io: connect } = require('socket.io-client');
const http = require('http');
const { server } = require('./server');

let pass = 0, fail = 0;
const ok = (cond, name) => { if (cond) { pass++; console.log('  ✓ ' + name); } else { fail++; console.log('  ✗ ' + name); } };
const sleep = ms => new Promise(r => setTimeout(r, ms));
let url;
const client = () => new Promise(res => { const s = connect(url, { transports: ['websocket'], reconnection: false, forceNew: true }); s.on('connect', () => res(s)); });
const ask = (s, ev, data) => new Promise(res => s.timeout(3000).emit(ev, data, (err, r) => res(err ? { error: 'timeout' } : r)));
const next = (s, ev, ms = 3000) => new Promise(res => { const t = setTimeout(() => res(null), ms); s.once(ev, d => { clearTimeout(t); res(d); }); });
const tab = () => Math.random().toString(36).slice(2, 10);
const V = 2;

(async () => {
  await new Promise(r => server.listen(0, r));
  url = 'http://localhost:' + server.address().port;
  console.log('Sideout server tests on ' + url);

  console.log('Health');
  const body = await new Promise(r => http.get(url + '/health', res => { let b = ''; res.on('data', c => b += c); res.on('end', () => r(b)); }));
  ok(/Sideout server ok/.test(body), 'health endpoint answers');

  console.log('Party codes');
  const h = await client(), g = await client(), x = await client();
  const ht = tab(), gt = tab();
  const c = await ask(h, 'create', { tab: ht, name: 'Hosty', v: V });
  ok(/^[ABCDEFGHJKMNPQRSTUVWXYZ23456789]{6}$/.test(c.code || ''), `6-character readable code (${c.code})`);
  const codes = new Set(); for (let i = 0; i < 4; i++) { const s = await client(); const r = await ask(s, 'create', { tab: tab(), name: 'A', v: V }); codes.add(r.code); s.close(); }
  ok(codes.size === 4 && [...codes].every(k => !/[01OIL]/.test(k)), 'codes are unique and skip 0/O/1/I/L');

  console.log('Joining and errors');
  ok((await ask(g, 'join', { code: 'ZZZZZZ', tab: gt, name: 'G', v: V })).error === 'not_found', '"Party not found" for a wrong code');
  ok((await ask(g, 'join', { code: c.code, tab: gt, name: 'G', v: 1 })).error === 'version', 'old game version is refused');
  ok((await ask(h, 'join', { code: c.code, tab: ht, name: 'H', v: V })).error === 'own', 'cannot join your own party');
  const peerOn = next(h, 'peer');
  const j = await ask(g, 'join', { code: c.code.toLowerCase(), tab: gt, name: 'Guesty', v: V });
  ok(j.ok && j.hostName === 'Hosty', 'join is case-insensitive and returns the host name');
  ok((await peerOn || {}).status === 'online', 'host is told the guest is online');
  ok((await ask(x, 'join', { code: c.code, tab: tab(), name: 'X', v: V })).error === 'full', '"Party is full" for a third player in the lobby');

  console.log('Relay and validation');
  const got = next(g, 'pkt');
  h.emit('pkt', { k: 'd', n: 1, d: { t: 'lobby', lobby: { mode: 'main', target: 25 } } });
  ok(((await got) || {}).d?.t === 'lobby', 'host message reaches the guest');
  const back = next(h, 'pkt');
  g.emit('pkt', { k: 'd', n: 1, d: { t: 'dec', e: 1, n: 1, v: { g: 3 } } });
  ok(((await back) || {}).d?.t === 'dec', 'guest decision reaches the host');
  const none = next(g, 'pkt', 600);
  h.emit('pkt', { k: 'd', n: 2, d: { t: 'evil', x: 1 } }); h.emit('pkt', 'garbage'); h.emit('pkt', { k: 'd', n: -5, d: { t: 'dec' } });
  h.emit('pkt', { k: 'd', n: 3, d: { t: 'dec', n: 'x' } });
  ok((await none) === null, 'malformed and unknown messages are dropped');
  const fakeHost = next(h, 'pkt', 600);
  g.emit('pkt', { k: 'd', n: 2, d: { t: 'start', seed: 1 } });
  ok((await fakeHost) === null, 'only the host can start the match');

  console.log('Match start, "already started", reconnect');
  h.emit('pkt', { k: 'd', n: 4, d: { t: 'start', seed: 7, mode: 'main', target: 25, epoch: 1 } }); await next(g, 'pkt');
  ok((await ask(x, 'join', { code: c.code, tab: tab(), name: 'X', v: V })).error === 'started', '"Party has already started" once the match is on');
  const off = next(h, 'peer'); g.close();
  ok(((await off) || {}).status === 'offline', 'host is told when the guest drops');
  const g2 = await client(); const onAgain = next(h, 'peer');
  const rj = await ask(g2, 'rejoin', { code: c.code, role: 'guest', tab: gt });
  ok(rj.ok && rj.peerOnline, 'guest rejoins within the window');
  ok(((await onAgain) || {}).status === 'online', 'host is told the guest is back');
  const relay2 = next(h, 'pkt'); g2.emit('pkt', { k: 'd', n: 3, d: { t: 'bg', hidden: false } });
  ok(!!(await relay2), 'messages flow again after the rejoin');
  ok((await ask(x, 'rejoin', { code: c.code, role: 'guest', tab: 'someoneelse' })).error === 'not_found', 'a stranger cannot take over a seat');

  console.log('Switched apps');
  const leftOn = ms => new Promise(res => { const f = d => { if (d && d.status === 'left') { h.off('peer', f); res(d); } }; h.on('peer', f); setTimeout(() => { h.off('peer', f); res(null); }, ms); });
  const bgSent = next(h, 'pkt'); g2.emit('pkt', { k: 'd', n: 4, d: { t: 'bg', hidden: true } }); await bgSent;
  const early = leftOn(2500); g2.close();
  ok((await early) === null, 'a player who switched apps keeps the seat past the normal window');
  const g4 = await client();
  ok((await ask(g4, 'rejoin', { code: c.code, role: 'guest', tab: gt })).ok, 'and can still rejoin');
  const bgBack = next(h, 'pkt'); g4.emit('pkt', { k: 'd', n: 5, d: { t: 'bg', hidden: false } }); await bgBack;

  console.log('Forfeit');
  const leftEv = leftOn(4000);
  g4.close();
  ok(((await leftEv) || {}).status === 'left', 'guest who stays away for the reconnect window forfeits');
  const g5 = await client();
  ok((await ask(g5, 'rejoin', { code: c.code, role: 'guest', tab: gt })).error === 'forfeit', 'coming back too late says "forfeit", not "not found"');
  g5.close();

  console.log('Host leaving');
  const h3 = await client(), g3 = await client();
  const c3 = await ask(h3, 'create', { tab: tab(), name: 'H3', v: V }); await ask(g3, 'join', { code: c3.code, tab: tab(), name: 'G3', v: V });
  const closed = next(g3, 'closed'); await ask(h3, 'leave', { reason: 'host_left' });
  ok(((await closed) || {}).reason === 'host_left', 'guest is told when the host closes the party');
  ok((await ask(x, 'join', { code: c3.code, tab: tab(), name: 'X', v: V })).error === 'not_found', 'a closed party cannot be joined');

  console.log('Quick match');
  const q1 = await client(), q2 = await client(), q3 = await client();
  const m1 = next(q1, 'matched'), m2 = next(q2, 'matched');
  const qa = await ask(q1, 'quick', { tab: tab(), name: 'Q1', v: V }); await ask(q2, 'quick', { tab: tab(), name: 'Q2', v: V });
  ok(qa.ok && typeof qa.estimate === 'number', 'queue answers with an estimated wait');
  const [r1, r2] = await Promise.all([m1, m2]);
  ok(r1 && r2 && r1.code === r2.code && r1.role !== r2.role, 'two waiting players land in the same party');
  const pq = next(q2, 'pkt'); q1.emit('pkt', { k: 'd', n: 1, d: { t: 'hello', name: 'Q1' } });
  ok(!!(await pq), 'matched players can talk');
  const m3 = next(q3, 'matched', 800); await ask(q3, 'quick', { tab: tab(), name: 'Q3', v: V }); q3.emit('quickCancel');
  const q4 = await client(); const m4 = next(q4, 'matched', 800); await ask(q4, 'quick', { tab: tab(), name: 'Q4', v: V });
  ok((await m3) === null && (await m4) === null, 'cancel takes you out of the queue');
  q4.emit('quickCancel');

  console.log('Expiry');
  const e1 = await client(); const ce = await ask(e1, 'create', { tab: tab(), name: 'E', v: V }).catch(() => ({}));
  const exp = next(e1, 'closed', 10000);
  ok(!!ce.code && ((await exp) || {}).reason === 'expired', 'idle parties expire');

  console.log('Names and rate limits');
  const n1 = await client(), n2 = await client();
  const cn = await ask(n1, 'create', { tab: tab(), name: 'xXfuckXx', v: V });
  ok((await ask(n2, 'join', { code: cn.code, tab: tab(), name: 'A very long display name indeed', v: V })).hostName === 'Player', 'offensive names are replaced');
  const l1 = await client(), l2 = await client();
  const cl = await ask(l1, 'create', { tab: tab(), name: 'A very long display name indeed', v: V });
  const jl = await ask(l2, 'join', { code: cl.code, tab: tab(), name: 'B', v: V });
  ok(jl.hostName && jl.hostName.length <= 16 && jl.hostName.startsWith('A very long'), `names are capped at 16 characters ("${jl.hostName}")`);
  const flood = await client(), fl2 = await client();
  const cf = await ask(flood, 'create', { tab: tab(), name: 'F', v: V }); await ask(fl2, 'join', { code: cf.code, tab: tab(), name: 'F2', v: V });
  let n = 0; fl2.on('pkt', () => n++);
  for (let i = 1; i <= 300; i++) flood.emit('pkt', { k: 'd', n: i, d: { t: 'bg', hidden: false } });
  await sleep(800);
  ok(n > 40 && n < 300, `message flood is rate-limited (${n} of 300 delivered)`);
  const spam = await client(); let lastErr = null;
  for (let i = 0; i < 40; i++) { const r = await ask(spam, 'create', { tab: tab(), name: 'S', v: V }); if (r.error) lastErr = r.error; }
  ok(lastErr === 'rate', 'party creation is rate-limited per address');

  console.log(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();
