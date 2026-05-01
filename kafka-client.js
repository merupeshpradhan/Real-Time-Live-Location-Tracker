import { Kafka } from "kafkajs";

export const kafkaClient = new Kafka({
  clientId: "my-tracker",
  brokers: ["localhost:9092"],
});
