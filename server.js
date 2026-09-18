import { WebSocketServer, WebSocket } from 'ws';

// Utiliser le port attribué par Render ou 8080 par défaut en local
const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// Maps pour stocker les clients connectés et les messages en attente
const clients = new Map();
const offlineMessages = new Map();

console.log(`Serveur de relais WebSocket démarré sur le port ${PORT}`);

wss.on('connection', (ws) => {
  let currentUserId = null;

  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data);

      // 1. Événement d'enregistrement de l'utilisateur avec son ID
      if (message.type === 'register') {
        currentUserId = message.userId;
        if (currentUserId) {
          clients.set(currentUserId, ws);
          console.log(`[Connecté] Utilisateur enregistré : ${currentUserId}`);

          // Dès que l'utilisateur se connecte, on lui envoie ses messages en attente
          if (offlineMessages.has(currentUserId)) {
            const pending = offlineMessages.get(currentUserId);
            console.log(`[File d'attente] Envoi de ${pending.length} message(s) en attente à ${currentUserId}`);
            
            pending.forEach((msg) => {
              ws.send(JSON.stringify(msg));
            });

            // On vide la file d'attente une fois les messages transmis
            offlineMessages.delete(currentUserId);
          }
        }
      }

      // 2. Événement d'envoi de message chiffré vers un autre utilisateur
      else if (message.type === 'message') {
        const { recipientId, encryptedPayload, senderPublicKey } = message;
        
        const recipientWs = clients.get(recipientId);
        const messagePayload = {
          type: 'message',
          senderId: currentUserId,
          encryptedPayload: encryptedPayload,
          senderPublicKey: senderPublicKey
        };

        if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
          // Destinataire en ligne : Envoi immédiat
          recipientWs.send(JSON.stringify(messagePayload));
          console.log(`[Message relayé] De ${currentUserId} vers ${recipientId}`);
        } else {
          // Destinataire hors ligne : Stockage temporaire dans sa file d'attente
          if (!offlineMessages.has(recipientId)) {
            offlineMessages.set(recipientId, []);
          }
          offlineMessages.get(recipientId).push(messagePayload);
          console.log(`[Hors ligne] Message stocké pour ${recipientId} (expéditeur: ${currentUserId})`);

          // Informer l'expéditeur que le message a bien été mis en attente
          ws.send(JSON.stringify({
            type: 'info',
            message: `Utilisateur hors ligne. Message mis en attente sur le serveur.`
          }));
        }
      }
      // Quand l'utilisateur ouvre une conv ou affiche un message
      else if (message.type === 'ack_delivered' || message.type === 'ack_read') {
        const targetSocket = clients.get(message.targetId);
        if (targetSocket && targetSocket.readyState === WebSocket.OPEN) {
          targetSocket.send(JSON.stringify({
            type: message.type,
            messageId: message.messageId, // ou messageIds (array)
            by: ws.userId
          }));
        }
      }
      // 3. Demande de statut en ligne d'un contact
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