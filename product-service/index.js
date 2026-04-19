const express = require("express");
const app = express();
const { pool, connectWithRetry } = require("./db");
const { createProductSchema, updateStockSchema } = require("./schema");
const authMiddleware = require("./middleware/authMiddleware");
const { initConsumer, successCount, failureCount } = require("./events/consumer");

app.use(express.json());

(async () => {
  await connectWithRetry();

  await pool.query(`
    CREATE TABLE IF NOT EXISTS products (
      id SERIAL PRIMARY KEY,
      name TEXT,
      price INT,
      stock INT
    )
  `);

  // 🔥 Start RabbitMQ AFTER DB is ready
  await initConsumer();
})();

const SECRET = process.env.JWT_SECRET || "supersecret";

/**
 * Create Product
 * Fields: name, price, stock
 */

app.post("/", authMiddleware,  async (req, res) => {
  try {
    const data = createProductSchema.parse(req.body);

    const result = await pool.query(
      "INSERT INTO products (name, price, stock) VALUES ($1, $2, $3) RETURNING *",
      [data.name, data.price, data.stock]
    );

    res.status(201).json(result.rows[0]);

  } catch (err) {
    res.status(400).json({
      error: "Validation failed",
      details: err.errors,
    });
  }
});

/**
 * Get all products
 */
app.get("/", async (req, res) => {
  const result = await pool.query("SELECT * FROM products");
  res.json(result.rows);
});

/**
 * Health check
 */
app.get("/health", (req, res) => {
  res.send("Product service is running");
});

/**
 * Metrics endpoint
 */
app.get("/metrics", (req, res) => {
  res.json({
    successCount,
    failureCount
  });
});

/**
 * Get single product
 */
app.get("/:id", async (req, res) => {
  const result = await pool.query(
    "SELECT * FROM products WHERE id = $1",
    [req.params.id]
  );

  if (result.rows.length === 0) {
    return res.status(404).json({ error: "Product not found" });
  }

  res.json(result.rows[0]);
});

/**
 * Update stock (IMPORTANT for orders later)
 */

app.patch("/:id/stock", authMiddleware, async (req, res) => {
  try {
    const { quantity } = updateStockSchema.parse(req.body);

    const result = await pool.query(
      "UPDATE products SET stock = stock - $1 WHERE id = $2 RETURNING *",
      [quantity, req.params.id]
    );

    res.json(result.rows[0]);

  } catch (err) {
    res.status(400).json({
      error: "Validation failed",
      details: err.errors,
    });
  }
});

app.listen(3002, () => {
  console.log("Product service running on port 3002");
});