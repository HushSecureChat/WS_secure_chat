import { WebSocketServer, WebSocket } from 'ws';
import http from 'http';
import fs from 'fs';

const server = http.createServer((req, res) => {
  if (req.url === '/version.json') {
    try {
      const versionData = fs.readFileSync('./version.json', 'utf8');
      res.writeHead(200, { 
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      });
      res.end(versionData);
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Erreur de lecture du fichier version.json');
    }
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Hush Relay Server is active.');
  }
});

const wss = new WebSocketServer({ server });
const PORT = process.env.PORT || 8080;

const clients = new Map();
const offlineMessages = new Map();

wss.on('connection', (ws) => {
  let currentUserId = null;

  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data);

      // 1. Enregistrement de l'utilisateur
      if (message.type === 'register') {
        currentUserId = message.userId;
        ws.userId = message.userId;
        if (currentUserId) {
          clients.set(currentUserId, ws);
          console.log(`[Connecté] Utilisateur enregistré : ${currentUserId}`);

          if (offlineMessages.has(currentUserId)) {
            const pending = offlineMessages.get(currentUserId);
            console.log(`[File d'attente] Envoi de ${pending.length} message(s) en attente à ${currentUserId}`);
            pending.forEach((msg) => ws.send(JSON.stringify(msg)));
            offlineMessages.delete(currentUserId);
          }
        }
      }

      // 2. Envoi / relais de message chiffré
      else if (message.type === 'message') {
        const { recipientId, encryptedPayload, senderPublicKey, messageId } = message;
        
        const recipientWs = clients.get(recipientId);
        const messagePayload = {
          type: 'message',
          senderId: currentUserId,
          encryptedPayload: encryptedPayload,
          senderPublicKey: senderPublicKey,
          messageId: messageId // Transmis pour l'acquittement
        };

        if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
          recipientWs.send(JSON.stringify(messagePayload));
          console.log(`[Message relayé] De ${currentUserId} vers ${recipientId}`);
        } else {
          if (!offlineMessages.has(recipientId)) {
            offlineMessages.set(recipientId, []);
          }
          offlineMessages.get(recipientId).push(messagePayload);
          console.log(`[Hors ligne] Message stocké pour ${recipientId} (expéditeur: ${currentUserId})`);

          ws.send(JSON.stringify({
            type: 'info',
            message: `Utilisateur hors ligne. Message mis en attente sur le serveur.`
          }));
        }
      }

      // 3. Accusés de réception (lu / distribué)
      else if (message.type === 'ack_delivered' || message.type === 'ack_read') {
  const targetSocket = clients.get(message.targetId);
  if (targetSocket && targetSocket.readyState === WebSocket.OPEN) {
    targetSocket.send(JSON.stringify({
      type: message.type,
      messageId: message.messageId,
      targetId: message.targetId,
      by: ws.userId || currentUserId
    }));
    console.log(`[Relai ACK ${message.type}] De ${ws.userId} vers ${message.targetId}`);
  }
}

      // 4. Demande de statut en ligne
      else if (message.type === 'check_status') {
        const { targetId } = message;
        const isOnline = clients.has(targetId) && clients.get(targetId).readyState === WebSocket.OPEN;

        ws.send(JSON.stringify({
          type: 'status_response',
          targetId: targetId,
          isOnline: isOnline
        }));
      }

    } catch (e) {
      console.error('Erreur lors du traitement du message JSON :', e);
    }
  });

  ws.on('close', () => {
    if (currentUserId) {
      clients.delete(currentUserId);
      console.log(`[Déconnecté] Utilisateur retiré : ${currentUserId}`);
    }
  });
});

server.listen(PORT, () => {
  console.log(`Serveur Hush démarré (WebSocket + API Version) sur le port ${PORT}`);
});