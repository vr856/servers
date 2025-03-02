#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import pg from "pg";
import { CountTablesSchema, countTables, getTableDetails } from "./tools/tables.js";

const server = new Server(
  {
    name: "example-servers/postgres",
    version: "0.1.0",
  },
  {
    capabilities: {
      resources: {},
      tools: {},
    },
  },
);

const args = process.argv.slice(2);
if (args.length === 0) {
  console.error("Please provide a database URL as a command-line argument");
  process.exit(1);
}

const databaseUrl = args[0];

const resourceBaseUrl = new URL(databaseUrl);
resourceBaseUrl.protocol = "postgres:";
resourceBaseUrl.password = "";

const pool = new pg.Pool({
  connectionString: databaseUrl,
  max: 20, // Maximum number of clients in the pool
  idleTimeoutMillis: 30000, // How long a client is allowed to remain idle before being closed
  connectionTimeoutMillis: 10000, // How long to wait for a connection to become available
  allowExitOnIdle: false // Don't allow the pool to exit while we're using it
});

// Add event listeners for pool errors
pool.on('error', (err: Error) => {
  console.error('Unexpected error on idle client', err);
});

const SCHEMA_PATH = "schema";

server.setRequestHandler(ListResourcesRequestSchema, async () => {
  const client = await pool.connect();
  try {
    const result = await client.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
    );
    return {
      resources: result.rows.map((row) => ({
        uri: new URL(`${row.table_name}/${SCHEMA_PATH}`, resourceBaseUrl).href,
        mimeType: "application/json",
        name: `"${row.table_name}" database schema`,
      })),
    };
  } finally {
    client.release();
  }
});

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const resourceUrl = new URL(request.params.uri);

  const pathComponents = resourceUrl.pathname.split("/");
  const schema = pathComponents.pop();
  const tableName = pathComponents.pop();

  if (schema !== SCHEMA_PATH) {
    throw new Error("Invalid resource URI");
  }

  const client = await pool.connect();
  try {
    const result = await client.query(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
      [tableName],
    );

    return {
      contents: [
        {
          uri: request.params.uri,
          mimeType: "application/json",
          text: JSON.stringify(result.rows, null, 2),
        },
      ],
    };
  } finally {
    client.release();
  }
});

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "query",
        description: "Run a read-only SQL query",
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
      {
        name: "execute",
        description: "Run a SQL command that can modify the database",
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
      {
        name: "countTables",
        description: "Count the number of tables in the database and get basic information about them",
        inputSchema: CountTablesSchema,
      }
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  if (request.params.name === "query") {
    const sql = request.params.arguments?.sql as string;

    const client = await pool.connect();
    try {
      await client.query("BEGIN TRANSACTION READ ONLY");
      const result = await client.query(sql);
      return {
        content: [{ type: "text", text: JSON.stringify(result.rows, null, 2) }],
        isError: false,
      };
    } catch (error) {
      throw error;
    } finally {
      client
        .query("ROLLBACK")
        .catch((error) =>
          console.warn("Could not roll back transaction:", error),
        );

      client.release();
    }
  } else if (request.params.name === "execute") {
    const sql = request.params.arguments?.sql as string;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query(sql);
      await client.query("COMMIT");
      
      return {
        content: [{ 
          type: "text", 
          text: JSON.stringify({
            rowCount: result.rowCount,
            command: result.command,
            rows: result.rows
          }, null, 2) 
        }],
        isError: false,
      };
    } catch (error) {
      await client.query("ROLLBACK").catch(e => 
        console.warn("Could not roll back transaction:", e)
      );
      throw error;
    } finally {
      client.release();
    }
  } else if (request.params.name === "countTables") {
    let tableCount = 0;
    let tableDetails = [];
    
    try {
      // Get table count and details in separate try-catch blocks to ensure proper error handling
      try {
        tableCount = await countTables(pool);
      } catch (error: any) {
        console.error("Error in countTables:", error);
        return {
          content: [{ type: "text", text: JSON.stringify({ error: "Failed to count tables: " + error.message }, null, 2) }],
          isError: true,
        };
      }
      
      try {
        tableDetails = await getTableDetails(pool);
      } catch (error: any) {
        console.error("Error in getTableDetails:", error);
        return {
          content: [{ 
            type: "text", 
            text: JSON.stringify({ 
              tableCount,
              error: "Failed to get table details: " + error.message 
            }, null, 2) 
          }],
          isError: true,
        };
      }
      
      return {
        content: [{ 
          type: "text", 
          text: JSON.stringify({
            tableCount,
            tables: tableDetails.map(table => ({
              name: table.table_name,
              columnCount: parseInt(table.column_count, 10),
              sizeBytes: parseInt(table.size_bytes, 10),
              sizeFormatted: formatBytes(parseInt(table.size_bytes, 10))
            }))
          }, null, 2) 
        }],
        isError: false,
      };
    } catch (error: any) {
      console.error("Unexpected error in countTables handler:", error);
      return {
        content: [{ type: "text", text: JSON.stringify({ error: "Unexpected error: " + error.message }, null, 2) }],
        isError: true,
      };
    }
  }
  throw new Error(`Unknown tool: ${request.params.name}`);
});

// Helper function to format bytes into a human-readable format
function formatBytes(bytes: number, decimals = 2): string {
  if (bytes === 0) return '0 Bytes';
  
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
  
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

async function runServer() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

runServer().catch(console.error);
