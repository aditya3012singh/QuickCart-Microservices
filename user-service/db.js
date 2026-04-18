const { Pool } = require("pg");

const pool = new Pool({
  host: "user-db",
  user: "postgres",
  password: "postgres",
  database: "userdb",
  port: 5432,
});

async function connectWithRetry(retries = 5) {
  while (retries) {
    try {
      await pool.query("SELECT 1");
      console.log("✅ Connected to User DB");
      break;
    } catch {
      console.log("❌ User DB not ready, retrying...");
      retries--;
      await new Promise(res => setTimeout(res, 2000));
    }
  }

  if (!retries) process.exit(1);
}

module.exports = { pool, connectWithRetry };