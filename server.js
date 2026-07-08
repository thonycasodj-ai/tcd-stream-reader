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

// Tiene traccia della connessione TikTok attiva (una alla volta, versione base)
let activeConnection = null;
let activeUsername = null;

// Manda un messaggio a tutti i browser collegati (di solito una sola pagina OBS/telefono)
function broadcast(data) {
  const payload = JSON.stringify(data);
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(payload);
  });
}

// Endpoint: avvia il collegamento a una live TikTok
// POST /api/connect  body: { username: "nomeutente_tiktok_senza_@" }
app.post('/api/connect', async (req, res) => {
  const { username } = req.body;
  if (!username) return res.status(400).json({ error: 'Username mancante' });

  // Chiude una connessione precedente se presente
  if (activeConnection) {
    try { await activeConnection.disconnect(); } catch (e) {}
    activeConnection = null;
  }

  const connection = new TikTokLiveConnection(username, {
    signApiKey: process.env.EULER_API_KEY || undefined
  });

  try {
    await connection.connect();
    activeConnection = connection;
    activeUsername = username;

    broadcast({ type: 'status', status: 'connected', username });

    connection.on('chat', (data) => {
      broadcast({
        type: 'chat',
        user: data.user?.nickname || 'Spettatore',
        text: data.content
      });
    });

    connection.on('gift', (data) => {
      const giftType = data.gift?.type;
      // Per i regali "a raffica" (giftType 1) aspetta la fine dello streak
      if (giftType === 1 && !data.repeatEnd) return;

      const giftName = data.gift?.name || 'un regalo';
      broadcast({
        type: 'gift',
        user: data.user?.nickname || 'Spettatore',
        giftName,
        repeatCount: data.repeatCount || 1
      });
    });

    connection.on('member', (data) => {
      broadcast({
        type: 'join',
        user: data.user?.nickname || 'Qualcuno'
      });
    });

    connection.on('disconnected', () => {
      broadcast({ type: 'status', status: 'disconnected', username: activeUsername });
      activeConnection = null;
    });

    res.json({ ok: true, username });
  } catch (err) {
    res.status(500).json({ error: 'Connessione fallita: la live è attiva? Errore: ' + err.message });
  }
});

// Endpoint: interrompe il collegamento
app.post('/api/disconnect', async (req, res) => {
  if (activeConnection) {
    try { await activeConnection.disconnect(); } catch (e) {}
    activeConnection = null;
    broadcast({ type: 'status', status: 'disconnected', username: activeUsername });
  }
  res.json({ ok: true });
});

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
  res.json({ connected: !!activeConnection, username: activeUsername });
});

// Endpoint: sintesi vocale di alta qualità tramite ElevenLabs
// POST /api/tts  body: { text: "testo da leggere" }
app.post('/api/tts', async (req, res) => {
  const { text } = req.body;
  if (!text) return res.status(400).json({ error: 'Testo mancante' });

  const apiKey = process.env.ELEVENLABS_API_KEY;
  if (!apiKey) {
    console.error('ELEVENLABS_API_KEY non impostata su Railway');
    return res.status(503).json({ error: 'ElevenLabs non configurato, uso voce di riserva' });
  }
  console.log('Chiamata ElevenLabs, chiave letta:', apiKey.slice(0, 6) + '...' + apiKey.slice(-4), '| lunghezza:', apiKey.length);

  // Voce predefinita multilingua ("Rachel"). Si può sostituire con un'altra
  // voice_id di ElevenLabs impostando la variabile ELEVENLABS_VOICE_ID.
  const voiceId = process.env.ELEVENLABS_VOICE_ID || '21m00Tcm4TlvDq8ikWAM';

  try {
    const response = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${voiceId}`, {
      method: 'POST',
      headers: {
        'xi-api-key': apiKey,
        'Content-Type': 'application/json',
        'Accept': 'audio/mpeg'
      },
      body: JSON.stringify({
        text,
        model_id: 'eleven_multilingual_v2',
        voice_settings: { stability: 0.4, similarity_boost: 0.75 }
      })
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error('Errore ElevenLabs:', response.status, errText);
      return res.status(502).json({ error: 'Errore ElevenLabs: ' + errText });
    }

    const arrayBuffer = await response.arrayBuffer();
    res.set('Content-Type', 'audio/mpeg');
    res.send(Buffer.from(arrayBuffer));
  } catch (err) {
    res.status(500).json({ error: 'Errore chiamata ElevenLabs: ' + err.message });
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`TCD Stream Reader in ascolto sulla porta ${PORT}`);
});
