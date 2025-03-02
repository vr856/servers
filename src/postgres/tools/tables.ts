import { z } from "zod";
import { Pool } from "pg";

// Zod schema for the countTables tool
export const CountTablesSchema = z.object({});

// Function to count tables in the database
export async function countTables(pool: Pool): Promise<number> {
  const client = await pool.connect();
  try {
    const result = await client.query(
      "SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = 'public'"
    );
    return parseInt(result.rows[0].table_count, 10);
  } catch (error) {
    console.error("Error counting tables:", error);
    throw error;
  } finally {
    client.release();
  }
}

// Function to get detailed table information
export async function getTableDetails(pool: Pool): Promise<any[]> {
  const client = await pool.connect();
  try {
    const result = await client.query(
      `SELECT 
        table_name, 
        (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name AND table_schema = 'public') as column_count,
        pg_total_relation_size(quote_ident(table_name)) as size_bytes
      FROM 
        information_schema.tables t
      WHERE 
        table_schema = 'public'
      ORDER BY 
        table_name`
    );
    return result.rows;
  } catch (error) {
    console.error("Error getting table details:", error);
    throw error;
  } finally {
    client.release();
  }
}
