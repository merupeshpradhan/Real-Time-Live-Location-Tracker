import http from "node:http";
import path from "node:path";
import express from "express";
import { Server } from "socket.io";
import { kafkaClient } from "./kafka-client.js";

async function main() {
  const PORT = process.env.PORT ?? 8000;
  const app = express();
  const server = http.createServer(app);
  const io = new Server(server); // Simplified attachment

  const kafkaProducer = kafkaClient.producer();
  await kafkaProducer.connect();

  const kafkaConsumer = kafkaClient.consumer({
    groupId: `socket-server-${PORT}`,
  });
  await kafkaConsumer.connect();
  await kafkaConsumer.subscribe({
    topics: ["location-updates"],
    fromBeginning: false,
  });

  kafkaConsumer.run({
    eachMessage: async ({ message, heartbeat }) => {
      const data = JSON.parse(message.value.toString());
      // Broadcast the location to all clients
      io.emit("server:location:updates", data);
      await heartbeat();
    },
  });

  io.on("connection", (socket) => {
    // GET THE USER ID from the frontend query
    const userId = socket.handshake.query.userId || socket.id;
    console.log(`[User Connected]: ${userId}`);

    socket.on("client:location:update", async (locationData) => {
      const { latitude, longitude } = locationData;

      await kafkaProducer.send({
        topic: "location-updates",
        messages: [
          {
            key: userId, // Use userId as key for partition ordering
            value: JSON.stringify({
              id: userId, // Send the actual userId, not socket.id
              latitude,
              longitude,
            }),
          },
        ],
      });
    });

    // HANDLE DISCONNECTS: Important for "Stale User" requirement
    socket.on("disconnect", () => {
      console.log(`[User Disconnected]: ${userId}`);
      io.emit("server:user:left", { id: userId });
    });
  });

  app.use(express.static(path.resolve("./public")));

  // server.listen(PORT, () =>
  //   console.log(`Server running on http://localhost:${PORT}`),
  // );

  // This change allows my phone (192.168.1.7) to talk to my laptop!
  server.listen(PORT, "0.0.0.0", () => {
    console.log(`🚀 Server running!`);
    console.log(`💻 Local: http://localhost:${PORT}`);
    console.log(`📱 Mobile: http://192.168.1.7:${PORT}`);
  });
}

main().catch(console.error);
