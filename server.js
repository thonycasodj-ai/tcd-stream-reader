// TCD Stream Reader — backend
// Si collega alla chat live di TikTok e inoltra i commenti in tempo reale
// al browser (via WebSocket), dove vengono letti ad alta voce.

import express from 'express';
import http from 'http';
import path from 'path';
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

  const connection = new TikTokLiveConnection(username);

  try {
    await connection.connect();
    activeConnection = connection;
    activeUsername = username;

    broadcast({ type: 'status', status: 'connected', username });

    connection.on('chat', (data) => {
      broadcast({
        type: 'chat',
        user: data.user?.nickname || data.user?.uniqueId || 'Spettatore',
        text: data.comment
      });
    });

    connection.on('gift', (data) => {
      const giftType = data.giftDetails?.giftType;
      // Per i regali "a raffica" (giftType 1) aspetta la fine dello streak
      if (giftType === 1 && !data.repeatEnd) return;

      const giftName = data.giftDetails?.giftName || 'un regalo';
      broadcast({
        type: 'gift',
        user: data.user?.nickname || data.user?.uniqueId || 'Spettatore',
        giftName,
        repeatCount: data.repeatCount || 1
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

app.get('/api/status', (req, res) => {
  res.json({ connected: !!activeConnection, username: activeUsername });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`TCD Stream Reader in ascolto sulla porta ${PORT}`);
});
