import { beforeEach } from "vitest";
import pg from "pg";

beforeEach(async (context) => {
  process.env.__current_test_name__ = context.task.name;
  console.log(`[setup.ts] process.env.__current_test_name__ set to: ${process.env.__current_test_name__}`);
  console.log(`[setup.ts] beforeEach running for test: ${context.task.name}`);

  const databaseUrl = process.env.DATABASE_URL || "postgres://kruschdb:password@localhost:5435/kruschdb_test";
  console.log(`[setup.ts] Connecting to DB: ${databaseUrl}`);
  const client = new pg.Client({
    connectionString: databaseUrl,
  });

  try {
    await client.connect();
    console.log(`[setup.ts] Connected to DB!`);
    const res = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name != 'effect_sql_migrations';
    `);
    const tables = res.rows.map((row) => row.table_name);
    console.log(`[setup.ts] Tables found: ${tables.join(", ")}`);
    if (tables.length > 0) {
      const escapedTables = tables.map((t) => `"${t}"`).join(", ");
      await client.query(`TRUNCATE TABLE ${escapedTables} RESTART IDENTITY CASCADE;`);
      console.log(`[setup.ts] Tables truncated successfully!`);
    }
  } catch (error) {
    console.error(`[setup.ts] ERROR in beforeEach truncation:`, error);
  } finally {
    await client.end().catch((err) => {
      console.error(`[setup.ts] Error closing connection:`, err);
    });
  }
});


