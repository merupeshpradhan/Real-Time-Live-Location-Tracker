import { kafkaClient } from "./kafka-client.js";

async function init() {
  const consumer = kafkaClient.consumer({ groupId: "database-processor" });
  await consumer.connect();
  await consumer.subscribe({ topic: "location-updates", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());
      // REQUIREMENT: Simulate storage
      // console.log(`[DB LOG]: Saving location for ${data.id} at ${data.ts}`);
      console.log(`[DB LOG]: User ${data.id} is at Lat: ${data.latitude}, Lng: ${data.longitude}`);
    },
  });
}
init();
