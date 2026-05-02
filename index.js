import http from "node:http";
import path from "node:path";
import express from "express";
import { Server } from "socket.io";
import jwt from "jsonwebtoken";
import { kafkaClient } from "./kafka-client.js";

const JWT_SECRET = "assignment-secret-key";

async function main() {
  const PORT = process.env.PORT ?? 8000;
  const app = express();
  app.use(express.json()); // REQUIRED: To parse the login request body

  const server = http.createServer(app);
  const io = new Server(server);

  // --- 1. REQUIRED: OIDC/Auth simulation (Login Route) ---
  app.post("/login", (req, res) => {
    const { username } = req.body;
    if (!username) return res.status(400).json({ error: "Username required" });

    // In a real system, this is where OAuth/OIDC validation happens
    const token = jwt.sign({ userId: username }, JWT_SECRET, {
      expiresIn: "1h",
    });
    res.json({ token });
  });

  const kafkaProducer = kafkaClient.producer();
  await kafkaProducer.connect();

  // --- 2. REQUIRED: Socket connection identifying authenticated user ---
  io.use((socket, next) => {
    const token = socket.handshake.auth.token;
    if (!token) return next(new Error("Unauthenticated: No token provided"));

    try {
      const decoded = jwt.verify(token, JWT_SECRET);
      socket.userId = decoded.userId; // Securely attach verified ID
      next();
    } catch (err) {
      next(new Error("Unauthenticated: Invalid token"));
    }
  });

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
      io.emit("server:location:updates", data);
      await heartbeat();
    },
  });

  io.on("connection", (socket) => {
    const userId = socket.userId; // Uses the SECURE ID from the token
    console.log(`[User Authenticated]: ${userId}`);

    socket.on("client:location:update", async (locationData) => {
      const { latitude, longitude } = locationData;

      // --- 3. REQUIRED: Handle invalid data (Validation) ---
      if (typeof latitude !== "number" || typeof longitude !== "number") {
        return console.warn(`[Blocked]: Invalid data format from ${userId}`);
      }

      await kafkaProducer.send({
        topic: "location-updates",
        messages: [
          {
            key: userId,
            value: JSON.stringify({ id: userId, latitude, longitude }),
          },
        ],
      });
    });

    socket.on("disconnect", () => {
      console.log(`[User Disconnected]: ${userId}`);
      io.emit("server:user:left", { id: userId });
    });
  });

  app.use(express.static(path.resolve("./public")));

  server.listen(PORT, "0.0.0.0", () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
  });
}

main().catch(console.error);
