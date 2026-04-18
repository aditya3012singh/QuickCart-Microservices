const express = require("express");
const authMiddleware = require("./authMiddleware");
const amqp = require("amqplib");
const app = express();
const { pool, connectWithRetry } = require("./db");
app.use(express.json());

let channel;

(async () => {
  await connectWithRetry();

  await pool.query(`
    CREATE TABLE IF NOT EXISTS orders (
      id SERIAL PRIMARY KEY,
      user_id TEXT,
      product_id INT,
      quantity INT,
      status TEXT
    )
  `);

  const conn = await connectRabbit();
  channel = await conn.createChannel();

  await channel.assertQueue("order_created", { durable: true });

  console.log("📡 RabbitMQ channel ready");
})();

// 🔗 Product Service URL (important for Docker later)
const PRODUCT_SERVICE_URL = "http://product-service:3002";

const SECRET = process.env.JWT_SECRET || "supersecret"; 

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

async function publishEvent(event) {
  if (!channel) {
    return res.status(500).json({
        error: "Event system not ready"
    });
  }

  channel.sendToQueue(
    "order_created",
    Buffer.from(JSON.stringify(event)),
    { persistent: true }
  );

  console.log("📤 Event published:", event);
}

/**
 * Create Order
 * Body: { productId, quantity }
 */
app.post("/", authMiddleware, async (req, res) => {
  try {
    const { productId, quantity } = req.body;

    const result = await pool.query(
      `INSERT INTO orders (user_id, product_id, quantity, status)
       VALUES ($1, $2, $3, $4)
       RETURNING *`,
      [req.user.userId, productId, quantity, "PENDING"]
    );

    const order = result.rows[0];

    if (!channel) {
      return res.status(500).json({ error: "Event system not ready" });
    }

    await publishEvent({
      orderId: order.id,
      productId,
      quantity
    });

    res.json(order);

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Order creation failed" });
  }
});

/**
 * Get all orders
 */
app.get("/", authMiddleware, async (req, res) => {
  const result = await pool.query(
    "SELECT * FROM orders WHERE user_id = $1",
    [req.user.userId]
  );

  res.json(result.rows);
});
/**
 * Health check
 */
app.get("/health", (req, res) => {
  res.send("Order service is running");
});

app.listen(3003, () => {
  console.log("Order service running on port 3003");
});