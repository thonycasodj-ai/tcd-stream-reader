// TCD Stream Reader — backend
// Si collega alla chat live di TikTok e inoltra i commenti in tempo reale
// al browser (via WebSocket), dove vengono letti ad alta voce.

const express = require('express');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');
const { WebcastPushConnection } = require('tiktok-live-connector');

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
    try { activeConnection.disconnect(); } catch (e) {}
    activeConnection = null;
  }

  const connection = new WebcastPushConnection(username);

  try {
    await connection.connect();
    activeConnection = connection;
    activeUsername = username;

    broadcast({ type: 'status', status: 'connected', username });

    connection.on('chat', (data) => {
      broadcast({
        type: 'chat',
        user: data.nickname || data.uniqueId || 'Spettatore',
        text: data.comment
      });
    });

    connection.on('gift', (data) => {
      broadcast({
        type: 'gift',
        user: data.nickname || data.uniqueId || 'Spettatore',
        giftName: data.giftName,
        repeatCount: data.repeatCount
      });
    });

    connection.on('disconnected', () => {
      broadcast({ type: 'status', status: 'disconnected', username });
      activeConnection = null;
    });

    res.json({ ok: true, username });
  } catch (err) {
    res.status(500).json({ error: 'Connessione fallita: la live è attiva? Errore: ' + err.message });
  }
});

// Endpoint: interrompe il collegamento
app.post('/api/disconnect', (req, res) => {
  if (activeConnection) {
    try { activeConnection.disconnect(); } catch (e) {}
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
