const { Pool } = require("pg");

const pool = new Pool({
  host: "product-db",
  user: "postgres",
  password: "postgres",
  database: "productdb",
  port: 5432,
});

async function connectWithRetry(retries = 5) {
  while (retries) {
    try {
      await pool.query("SELECT 1");
      console.log("✅ Connected to Postgres");
      break;
    } catch (err) {
      console.log("❌ DB not ready, retrying...");
      retries--;
      await new Promise(res => setTimeout(res, 2000));
    }
  }

  if (!retries) {
    console.error("❌ Could not connect to DB");
    process.exit(1);
  }
}

module.exports = { pool, connectWithRetry };