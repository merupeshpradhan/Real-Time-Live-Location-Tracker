import { kafkaClient } from "./kafka-client.js";
import fs from "node:fs"; // ADDED to simulate storage

async function init() {
  const consumer = kafkaClient.consumer({ groupId: "database-processor" });
  await consumer.connect();
  await consumer.subscribe({ topic: "location-updates", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());

      // LOG TO CONSOLE
      console.log(
        `[DB LOG]: User ${data.id} is at Lat: ${data.latitude}, Lng: ${data.longitude}`,
      );

      // SIMULATE PERSISTENCE (Writing to a file)
      const logEntry = `${new Date().toISOString()} - ${data.id}: ${data.latitude}, ${data.longitude}\n`;
      fs.appendFileSync("location_history.log", logEntry);
    },
  });
}

init();
