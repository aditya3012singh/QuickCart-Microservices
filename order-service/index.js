const express = require("express");
const authMiddleware = require("./middleware/authMiddleware");
const app = express();
const { pool, connectWithRetry } = require("./db");
const { initPublisher, publishEvent, consumeOrderEvents } = require("./events/publisher");

app.use(express.json());

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

  await initPublisher();
  consumeOrderEvents();
})();

// 🔗 Product Service URL (important for Docker later)
const PRODUCT_SERVICE_URL = "http://product-service:3002";

const SECRET = process.env.JWT_SECRET || "supersecret";

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
    const correlationId = `order-${order.id}`;

    console.log(JSON.stringify({
      service: "order-service",
      event: "order_created",
      orderId: order.id,
      correlationId,
      status: "pending",
      timestamp: new Date().toISOString()
    }));

    try {
      publishEvent({
        orderId: order.id,
        productId,
        quantity,
        correlationId
      });
    } catch (err) {
      console.error(JSON.stringify({
        service: "order-service",
        event: "publish_failed",
        orderId: order.id,
        correlationId,
        error: err.message,
        timestamp: new Date().toISOString()
      }));
      return res.status(500).json({ error: "Event failed" });
    }

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