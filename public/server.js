// TCD Stream Reader — backend
// Si collega alla chat live di TikTok e inoltra i commenti in tempo reale
// al browser (via WebSocket), dove vengono letti ad alta voce.

import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { WebSocketServer } from 'ws';
import { TikTokLiveConnection } from 'tiktok-live-connector';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ================== SESSIONI ==================
// Ogni pagina aperta (telefono, OBS…) ha il suo codice di sessione "sid".
// Così più creator possono usare l'app nello stesso momento senza disturbarsi.
const sessions = new Map(); // sid -> { conn, username, sockets:Set, manualStop, retries, lastEvent }

function getSession(sid) {
  if (!sessions.has(sid)) sessions.set(sid, { conn: null, username: null, sockets: new Set(), manualStop: false, retries: 0, lastEvent: 0 });
  return sessions.get(sid);
}

function sendTo(sid, data) {
  const s = sessions.get(sid);
  if (!s) return;
  const payload = JSON.stringify(data);
  s.sockets.forEach((ws) => { if (ws.readyState === 1) ws.send(payload); });
}

wss.on('connection', (ws, req) => {
  const sid = new URL(req.url, 'http://x').searchParams.get('sid') || 'default';
  const s = getSession(sid);
  s.sockets.add(ws);
  // Appena la pagina si (ri)collega, le dice subito lo stato attuale
  ws.send(JSON.stringify({ type: 'status', status: s.conn ? 'connected' : 'disconnected', username: s.username }));
  ws.on('close', () => s.sockets.delete(ws));
});

// Chiude la connessione TikTok senza mai restare bloccato (max 3 secondi)
async function closeConn(s) {
  const conn = s.conn;
  s.conn = null;
  if (!conn) return;
  try { conn.removeAllListeners(); } catch (e) {}
  await Promise.race([
    Promise.resolve().then(() => conn.disconnect()).catch(() => {}),
    new Promise((r) => setTimeout(r, 3000))
  ]);
}

async function openConn(sid, username) {
  const s = getSession(sid);
  await closeConn(s);

  const connection = new TikTokLiveConnection(username, {
    signApiKey: process.env.EULER_API_KEY || undefined
  });

  // Se TikTok non risponde entro 20 secondi, rinuncia invece di restare appeso
  await Promise.race([
    connection.connect(),
    new Promise((_, rej) => setTimeout(() => rej(new Error('TikTok non risponde (timeout)')), 20000))
  ]);

  s.conn = connection;
  s.username = username;
  s.manualStop = false;
  s.lastEvent = Date.now();
  sendTo(sid, { type: 'status', status: 'connected', username });

  const alive = () => { s.lastEvent = Date.now(); s.retries = 0; };

  connection.on('chat', (data) => {
    alive();
    sendTo(sid, { type: 'chat', user: data.user?.nickname || 'Spettatore', text: data.comment ?? data.content });
  });

  connection.on('gift', (data) => {
    alive();
    // Per i regali "a raffica" (giftType 1) aspetta la fine dello streak
    if (data.giftType === 1 && !data.repeatEnd) return;
    if (data.gift?.type === 1 && !data.repeatEnd) return;
    sendTo(sid, {
      type: 'gift',
      user: data.user?.nickname || 'Spettatore',
      giftName: data.giftDetails?.giftName || data.gift?.name || data.giftName || 'un regalo',
      repeatCount: data.repeatCount || 1
    });
  });

  connection.on('member', (data) => {
    alive();
    sendTo(sid, { type: 'join', user: data.user?.nickname || 'Qualcuno' });
  });

  // La live è finita: si scollega da solo
  connection.on('streamEnd', async () => {
    s.manualStop = true;
    await closeConn(s);
    sendTo(sid, { type: 'status', status: 'disconnected', username: s.username, reason: 'ended' });
  });

  // Connessione caduta senza volerlo: prova a ricollegarsi (max 3 tentativi)
  connection.on('disconnected', async () => {
    if (s.conn !== connection) return;
    s.conn = null;
    if (s.manualStop || s.retries >= 3) {
      sendTo(sid, { type: 'status', status: 'disconnected', username: s.username, reason: 'lost' });
      return;
    }
    s.retries++;
    sendTo(sid, { type: 'status', status: 'reconnecting', username: s.username, attempt: s.retries });
    setTimeout(async () => {
      if (s.manualStop) return;
      try { await openConn(sid, username); }
      catch (e) { sendTo(sid, { type: 'status', status: 'disconnected', username: s.username, reason: 'lost' }); }
    }, 3000 * s.retries);
  });

  connection.on('error', (err) => console.error('Errore TikTok', username, err?.message || err));
}

// Endpoint: avvia il collegamento a una live TikTok
// POST /api/connect  body: { username, sid }
app.post('/api/connect', async (req, res) => {
  const username = String(req.body.username || '').trim().replace(/^@/, '');
  const sid = String(req.body.sid || 'default');
  if (!username) return res.status(400).json({ error: 'Username mancante' });
  getSession(sid).retries = 0;
  try {
    await openConn(sid, username);
    res.json({ ok: true, username });
  } catch (err) {
    res.status(500).json({ error: 'Connessione fallita: la live è attiva? Errore: ' + err.message });
  }
});

// Endpoint: interrompe il collegamento — risponde SUBITO, anche se TikTok è bloccato
app.post('/api/disconnect', (req, res) => {
  const sid = String(req.body.sid || 'default');
  const s = getSession(sid);
  s.manualStop = true;
  closeConn(s);
  sendTo(sid, { type: 'status', status: 'disconnected', username: s.username, reason: 'manual' });
  res.json({ ok: true });
});

// Controllo ogni minuto: sessioni senza pagine aperte da 10 minuti vengono chiuse
setInterval(() => {
  for (const [sid, s] of sessions) {
    if (s.sockets.size === 0 && Date.now() - s.lastEvent > 10 * 60 * 1000) {
      s.manualStop = true;
      closeConn(s);
      sessions.delete(sid);
    }
  }
}, 60 * 1000);

const WAITLIST_FILE = path.join(__dirname, 'waitlist.json');

function readWaitlist() {
  try {
    return JSON.parse(fs.readFileSync(WAITLIST_FILE, 'utf8'));
  } catch (e) {
    return [];
  }
}

function saveWaitlist(list) {
  fs.writeFileSync(WAITLIST_FILE, JSON.stringify(list, null, 2));
}

// Endpoint: iscrizione alla waitlist della landing page
app.post('/api/waitlist', (req, res) => {
  const { email, tiktokUsername } = req.body;
  if (!email || !/^\S+@\S+\.\S+$/.test(email)) {
    return res.status(400).json({ error: 'Email non valida' });
  }
  const list = readWaitlist();
  list.push({ email, tiktokUsername: tiktokUsername || '', date: new Date().toISOString() });
  saveWaitlist(list);
  console.log('Nuova iscrizione waitlist:', email, tiktokUsername || '');
  res.json({ ok: true });
});

// Endpoint: vedere le iscrizioni raccolte (protetto da chiave semplice)
// GET /admin/waitlist?key=LA_TUA_CHIAVE
app.get('/admin/waitlist', (req, res) => {
  const key = req.query.key;
  if (!process.env.ADMIN_KEY || key !== process.env.ADMIN_KEY) {
    return res.status(403).json({ error: 'Non autorizzato' });
  }
  res.json(readWaitlist());
});

app.get('/api/status', (req, res) => {
  const s = sessions.get(String(req.query.sid || 'default'));
  res.json({ connected: !!s?.conn, username: s?.username || null });
});

// Endpoint: sintesi vocale di alta qualità tramite ElevenLabs
// POST /api/tts  body: { text: "testo da leggere", lang: "it" | "en" | "es" | ... }
// Usa il modello Flash v2.5: multilingua (32 lingue), veloce per le live e più economico.
const LANGS_OK = ['it','en','es','fr','pt','de','ja','ko','zh','ar','ru','hi','pl','nl','tr','ro','sv','id','fil','uk','el','cs','fi','hr','ms','sk','da','ta','bg','hu','no','vi'];

app.post('/api/tts', async (req, res) => {
  const { text } = req.body;
  let { lang } = req.body;
  if (!text) return res.status(400).json({ error: 'Testo mancante' });

  const apiKey = (process.env.ELEVENLABS_API_KEY || '').trim();
  if (!apiKey) {
    console.error('ELEVENLABS_API_KEY non impostata nelle variabili del server');
    return res.status(503).json({ error: 'ElevenLabs non configurato, uso voce di riserva' });
  }

  // Voce predefinita multilingua ("Rachel"); si cambia con la variabile ELEVENLABS_VOICE_ID
  const voiceId = (process.env.ELEVENLABS_VOICE_ID || '21m00Tcm4TlvDq8ikWAM').trim();
  const modelId = (process.env.ELEVENLABS_MODEL || 'eleven_flash_v2_5').trim();
  lang = (lang || '').toLowerCase().split('-')[0];

  const body = {
    text: String(text).slice(0, 300),
    model_id: modelId,
    voice_settings: { stability: 0.45, similarity_boost: 0.8, style: 0.2, use_speaker_boost: true }
  };
  // Forza la lingua giusta (supportato dai modelli v2.5)
  if (modelId.includes('v2_5') && LANGS_OK.includes(lang)) body.language_code = lang;

  try {
    const response = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${voiceId}?output_format=mp3_44100_128`, {
      method: 'POST',
      headers: { 'xi-api-key': apiKey, 'Content-Type': 'application/json', 'Accept': 'audio/mpeg' },
      body: JSON.stringify(body)
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error('Errore ElevenLabs:', response.status, errText);
      return res.status(502).json({ error: 'Errore ElevenLabs ' + response.status + ': ' + errText });
    }

    const arrayBuffer = await response.arrayBuffer();
    res.set('Content-Type', 'audio/mpeg');
    res.send(Buffer.from(arrayBuffer));
  } catch (err) {
    res.status(500).json({ error: 'Errore chiamata ElevenLabs: ' + err.message });
  }
});

// Controllo rapido: dice se la voce ElevenLabs è configurata (senza mostrare la chiave)
app.get('/api/tts-status', (req, res) => {
  res.json({ elevenlabs: !!(process.env.ELEVENLABS_API_KEY || '').trim(), model: process.env.ELEVENLABS_MODEL || 'eleven_flash_v2_5' });
});

// Endpoint: sblocco funzioni PRO con codice
// I codici validi si mettono nella variabile PRO_CODES su Bonto, separati da virgola (es. PRO-ANNA-2026,PRO-LUCA-2026)
app.post('/api/unlock', (req, res) => {
  const code = String(req.body.code || '').trim().toUpperCase();
  const codes = (process.env.PRO_CODES || '').split(',').map(c => c.trim().toUpperCase()).filter(Boolean);
  res.json({ pro: !!code && codes.includes(code) });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`TCD Stream Reader in ascolto sulla porta ${PORT}`);
});
