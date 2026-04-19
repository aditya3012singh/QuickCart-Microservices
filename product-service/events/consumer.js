const amqp = require("amqplib");
const { pool } = require("../db");

let channel;

let successCount = 0;
let failureCount = 0;


async function connectRabbit(retries = 15) {
  while (retries) {
    try {
      const conn = await amqp.connect("amqp://rabbitmq");
      console.log("✅ Connected to RabbitMQ");
      return conn;
    } catch {
      console.log("❌ RabbitMQ not ready, retrying...");
      retries--;
      await new Promise(res => setTimeout(res, 2000));
    }
  }
  process.exit(1);
}

async function initConsumer() {
  const conn = await connectRabbit();
  channel = await conn.createChannel();

  await channel.assertQueue("order_created", { durable: true });
  await channel.assertQueue("stock_reserved", { durable: true });
  await channel.assertQueue("stock_failed", { durable: true });

  channel.prefetch(1);

  console.log("📡 RabbitMQ channel ready");

  channel.consume("order_created", async (msg) => {
    if (!msg) return;

    const start = Date.now();

    try {
      const content = msg.content.toString();
      const { orderId, productId, quantity, correlationId } = JSON.parse(content);

      if (!orderId || !productId || typeof quantity !== "number" || quantity <= 0) {
        console.log(JSON.stringify({
          service: "product-service",
          event: "invalid_payload",
          orderId,
          productId,
          quantity,
          correlationId: correlationId || "unknown",
          timestamp: new Date().toISOString()
        }));
        channel.reject(msg, false);
        return;
      }

      console.log(JSON.stringify({
        service: "product-service",
        event: "event_received",
        orderId,
        productId,
        quantity,
        correlationId,
        timestamp: new Date().toISOString()
      }));

      const result = await pool.query(
          `UPDATE products
          SET stock = stock - $1
          WHERE id = $2 AND stock >= $1
          RETURNING *`,
          [quantity, productId]
      );

      const duration = Date.now() - start;

      if (result.rows.length === 0) {
          failureCount++;
          console.log(JSON.stringify({
            service: "product-service",
            event: "stock_failed",
            orderId,
            productId,
            quantity,
            correlationId,
            duration,
            timestamp: new Date().toISOString()
          }));

          channel.sendToQueue(
              "stock_failed",
              Buffer.from(JSON.stringify({ orderId, correlationId })),
              { persistent: true }
          );

      } else {
          successCount++;
          console.log(JSON.stringify({
            service: "product-service",
            event: "stock_reserved",
            orderId,
            productId,
            quantity,
            correlationId,
            duration,
            timestamp: new Date().toISOString()
          }));

          channel.sendToQueue(
              "stock_reserved",
              Buffer.from(JSON.stringify({ orderId, correlationId })),
              { persistent: true }
          );
      }

      channel.ack(msg);

    } catch (err) {
        console.error(JSON.stringify({
          service: "product-service",
          event: "processing_failed",
          error: err.message,
          correlationId: "unknown",
          timestamp: new Date().toISOString()
        }));
        channel.nack(msg, false, true); // retry
    }
  });
}

module.exports = { initConsumer , connectRabbit, successCount, failureCount};