const amqp = require("amqplib");
const { pool } = require("../db");
let channel;

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

async function initPublisher() {
  const conn = await connectRabbit();
  channel = await conn.createChannel();

  await channel.assertQueue("order_created", { durable: true });

  console.log("📡 RabbitMQ channel ready");
}

function publishEvent(event) {
  if (!channel) {
    throw new Error("Event system not ready");
  }

  channel.sendToQueue(
    "order_created",
    Buffer.from(JSON.stringify(event)),
    { persistent: true }
  );

  console.log(JSON.stringify({
    service: "order-service",
    event: "event_published",
    correlationId: event.correlationId,
    orderId: event.orderId,
    timestamp: new Date().toISOString()
  }));
}

async function consumeOrderEvents() {
  const conn = await connectRabbit();
  const ch = await conn.createChannel();

  await ch.assertQueue("stock_reserved", { durable: true });
  await ch.assertQueue("stock_failed", { durable: true });

  // ✅ Success case
  ch.consume("stock_reserved", async (msg) => {
    if (!msg) return;

    try {
      const { orderId, correlationId } = JSON.parse(msg.content.toString());
      if (!orderId) {
        console.log(JSON.stringify({
          service: "order-service",
          event: "invalid_payload",
          queue: "stock_reserved",
          correlationId: correlationId || "unknown",
          timestamp: new Date().toISOString()
        }));
        ch.reject(msg, false);
        return;
      }

      const result = await pool.query(
        "UPDATE orders SET status = $1 WHERE id = $2 AND status = $3",
        ["CONFIRMED", orderId, "PENDING"]
      );

      if (result.rowCount === 0) {
        console.log(JSON.stringify({
          service: "order-service",
          event: "idempotent_skip",
          orderId,
          correlationId,
          reason: "not_pending"
        }));
      } else {
        console.log(JSON.stringify({
          service: "order-service",
          event: "order_confirmed",
          orderId,
          correlationId,
          timestamp: new Date().toISOString()
        }));
      }

      ch.ack(msg);
    } catch (err) {
      console.error(JSON.stringify({
        service: "order-service",
        event: "processing_failed",
        queue: "stock_reserved",
        error: err.message,
        correlationId: "unknown",
        timestamp: new Date().toISOString()
      }));
      ch.nack(msg, false, true);
    }
  });

  // ❌ Failure case
  ch.consume("stock_failed", async (msg) => {
    if (!msg) return;

    try {
      const { orderId, correlationId } = JSON.parse(msg.content.toString());
      if (!orderId) {
        console.log(JSON.stringify({
          service: "order-service",
          event: "invalid_payload",
          queue: "stock_failed",
          correlationId: correlationId || "unknown",
          timestamp: new Date().toISOString()
        }));
        ch.reject(msg, false);
        return;
      }

      const result = await pool.query(
        "UPDATE orders SET status = $1 WHERE id = $2 AND status = $3",
        ["FAILED", orderId, "PENDING"]
      );

      if (result.rowCount === 0) {
        console.log(JSON.stringify({
          service: "order-service",
          event: "idempotent_skip",
          orderId,
          correlationId,
          reason: "not_pending"
        }));
      } else {
        console.log(JSON.stringify({
          service: "order-service",
          event: "order_failed",
          orderId,
          correlationId,
          timestamp: new Date().toISOString()
        }));
      }

      ch.ack(msg);
    } catch (err) {
      console.error(JSON.stringify({
        service: "order-service",
        event: "processing_failed",
        queue: "stock_failed",
        error: err.message,
        correlationId: "unknown",
        timestamp: new Date().toISOString()
      }));
      ch.nack(msg, false, true);
    }
  });
}

module.exports = { initPublisher, publishEvent, consumeOrderEvents };