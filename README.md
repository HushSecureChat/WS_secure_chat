# Hush — WebSocket Relay Server ⚡

> Le serveur relais aveugle et léger pour l'application de messagerie chiffrée Hush.

[![Node.js](https://img.shields.io/badge/Backend-Node.js-339933?style=flat-square&logo=node.js)](https://nodejs.org)
[![WebSocket](https://img.shields.io/badge/Protocol-WebSockets-010101?style=flat-square&logo=socket.io)](https://developer.mozilla.org/fr/docs/Web/API/WebSockets_API)

## 🔍 Rôle du serveur

Le serveur **Hush WebSocket** agit exclusivement comme une passerelle de routage de paquets aveugle :
* **Zéro Stockage en Clair :** Le serveur ne stocke, ne lit et ne conserve aucun message. Il se contente de relayer les charges utiles (*payloads*) chiffrées de bout en bout d'un client à un autre.
* **Gestion des Statuts :** Il gère la présence en ligne des utilisateurs connectés pour permettre l'affichage des indicateurs de statut en direct sur l'application mobile.
* **Hébergement Souverain :** Conçu pour être déployé facilement sur des services cloud (ex: Render) hors de France pour renforcer la confidentialité des métadonnées de transit.
