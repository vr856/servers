// Simple script to run the MCP PostgreSQL server
// Run with: node run-server.js

// Import the server module
import('./dist/index.js').catch(err => {
  console.error('Failed to start server:', err);
  process.exit(1);
});

console.log('Attempting to start MCP PostgreSQL server...'); 