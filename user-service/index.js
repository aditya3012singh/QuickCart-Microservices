const express = require("express");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const client = require("prom-client");
const authMiddleware = require("./middleware/authMiddleware");
const { pool, connectWithRetry } = require("./db");

const app = express();

client.collectDefaultMetrics();

app.use(express.json());

(async () => {
  await connectWithRetry();

  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      email TEXT UNIQUE NOT NULL,
      password TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `);
})();

// Create user
const SECRET = process.env.JWT_SECRET || "supersecret"; 

app.post("/register", async (req, res) => {
  const { email, password } = req.body;

  try {
    const hashedPassword = await bcrypt.hash(password, 10);

    const result = await pool.query(
      "INSERT INTO users (email, password) VALUES ($1, $2) RETURNING id, email",
      [email, hashedPassword]
    );

    res.status(201).json(result.rows[0]);

  } catch (err) {
    if (err.code === "23505") {
      return res.status(400).json({ error: "Email already exists" });
    }

    res.status(500).json({ error: "Something went wrong" });
  }
});


app.post("/login", async (req, res) => {
  const { email, password } = req.body;

  try {
    const result = await pool.query(
      "SELECT * FROM users WHERE email = $1",
      [email]
    );

    if (result.rows.length === 0) {
      return res.status(400).json({ error: "User not found" });
    }

    const user = result.rows[0];

    const isValid = await bcrypt.compare(password, user.password);

    if (!isValid) {
      return res.status(400).json({ error: "Invalid password" });
    }

    const token = jwt.sign(
      { userId: user.id, email: user.email },
      SECRET,
      { expiresIn: "1h" }
    );

    res.json({ token });

  } catch (err) {
    res.status(500).json({ error: "Login failed" });
  }
});

app.get("/profile", authMiddleware, async (req, res) => {
  const result = await pool.query(
    "SELECT id, email, created_at FROM users WHERE id = $1",
    [req.user.userId]
  );

  res.json(result.rows[0]);
});

// Get user
app.get("/users/:id", async (req, res) => {
  const userId = parseInt(req.params.id, 10);

  if (Number.isNaN(userId)) {
    return res.status(400).json({ error: "Invalid user id" });
  }

  try {
    const result = await pool.query(
      "SELECT id, email, created_at FROM users WHERE id = $1",
      [userId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "User not found" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to load user" });
  }
});

// Health check (VERY IMPORTANT in microservices)
app.get("/health", (req, res) => {
  res.send("User service is running");
});

app.get("/metrics", async (req, res) => {
  res.set("Content-Type", client.register.contentType);
  res.end(await client.register.metrics());
});

app.listen(3001, () => {
  console.log("User service running on port 3001");
});