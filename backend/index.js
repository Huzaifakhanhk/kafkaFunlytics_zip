
// Node.js Kafka Consumer and Humor Logger

const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: "kafka-funlytics",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "fun-group" });

const run = async () => {
    await consumer.connect();
    console.log("[INFO] Connected to Kafka. It's gonna be a fun ride! 🚀");

    await consumer.subscribe({ topic: "humor-events", fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`[${topic}] Ha! ${message.value.toString()}`);
        },
    });
};

run().catch((err) => console.error(`[ERROR] ${err.message}`));
