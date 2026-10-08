const express = require('express'), http = require('http'), path = require('path'), fs = require('fs');
const { WebSocketServer } = require('ws');

const app = express(), server = http.createServer(app);
const UP = path.join(__dirname, 'uploads');
fs.mkdirSync(UP, { recursive: true });
const rooms = new Map(); // id -> { peers: Map<peerId, ws>, movie, state, timer }
const clean = s => String(s || '').replace(/[^\w-]/g, '').slice(0, 32);
const file = id => path.join(UP, clean(id) + '.mp4');
const getRoom = id => {
  if (!rooms.has(id)) rooms.set(id, { peers: new Map(), movie: null, state: { playing: false, time: 0, at: Date.now() } });
  return rooms.get(id);
};
const bcast = (room, msg, except) => room.peers.forEach((w, pid) => pid !== except && w.readyState === 1 && w.send(JSON.stringify(msg)));

app.use(express.static(path.join(__dirname, 'public')));
app.get('/r/:id', (_, res) => res.sendFile(path.join(__dirname, 'public/index.html')));

// Anyone with the room link can upload. Raw body streamed straight to disk (handles multi-GB files).
app.post('/upload/:id', (req, res) => {
  const id = clean(req.params.id);
  const room = getRoom(id);
  const out = fs.createWriteStream(file(id));
  req.pipe(out);
  out.on('finish', () => {
    room.movie = { name: decodeURIComponent(req.query.name || 'movie.mp4'), v: Date.now() };
    room.state = { playing: false, time: 0, at: Date.now() };
    bcast(room, { t: 'movie', movie: room.movie });
    res.json({ ok: true });
  });
  out.on('error', () => res.status(500).end());
});

// sendFile supports HTTP Range requests, so seeking and streaming are smooth.
app.get('/media/:id', (req, res) => {
  const f = file(req.params.id);
  fs.existsSync(f) ? res.type('video/mp4').sendFile(f) : res.status(404).end();
});

const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', (ws, req) => {
  const roomId = clean(new URL(req.url, 'http://x').searchParams.get('room'));
  if (!roomId) return ws.close();
  const room = getRoom(roomId);
  clearTimeout(room.timer);
  const id = Math.random().toString(36).slice(2, 10);
  const st = room.state;
  const time = st.time + (st.playing ? (Date.now() - st.at) / 1000 : 0);
  ws.send(JSON.stringify({ t: 'welcome', id, peers: [...room.peers.keys()], movie: room.movie, state: { playing: st.playing, time } }));
  room.peers.set(id, ws);

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'signal') {
      const target = room.peers.get(m.to);
      target && target.readyState === 1 && target.send(JSON.stringify({ t: 'signal', from: id, data: m.data }));
    } else if (m.t === 'sync') {
      const time = Number(m.time) || 0;
      room.state = { playing: m.action === 'play' ? true : m.action === 'pause' ? false : room.state.playing, time, at: Date.now() };
      bcast(room, { t: 'sync', action: m.action, time }, id);
    }
  });

  ws.on('close', () => {
    room.peers.delete(id);
    bcast(room, { t: 'leave', id });
    if (!room.peers.size) // delete the movie 10 min after the room empties
      room.timer = setTimeout(() => { rooms.delete(roomId); fs.rm(file(roomId), { force: true }, () => {}); }, 10 * 60 * 1000);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`moviedate running on http://localhost:${PORT}`));