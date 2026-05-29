import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

// Load .env from root directory
const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.resolve(__dirname, "../../.env") });

import express from "express";
import cors from "cors";
import cookieParser from "cookie-parser";
import { createServer } from "http";
import { Server } from "socket.io";

// Routes
import authRoutes, { setSocketIO } from "./src/routes/auth.routes.js";
import dashboardRoutes from "./src/routes/dashboard.routes.js";
import contactsRoutes from "./src/routes/contacts.routes.js";
import tagsRoutes from "./src/routes/tags.routes.js";
import uploadRoutes from "./src/routes/upload.routes.js";
import campaignsRoutes from "./src/routes/campaigns.routes.js";
import conversationsRoutes from "./src/routes/conversations.routes.js";
import botRoutes from "./src/routes/bot.routes.js";
import profileRoutes from "./src/routes/profile.routes.js";
import messagesRoutes from "./src/routes/messages.routes.js";

// Services
import { setupSocketHandlers } from "./src/socket/handlers.js";
import { startScheduler } from "./src/services/blast.service.js";
import { setSocketIO as setMessageStorageSocketIO } from "./src/services/message-storage.service.js";
import { db, whatsappSessions } from "@whatsapp-blast/database";
import { eq } from "drizzle-orm";
import { updateReturningMany } from "./src/utils/db-compat.js";
import { handleDbError } from "./src/utils/db-errors.js";
import { getDbStatusMessage, isDbAvailable } from "./src/utils/db-state.js";

// Startup cleanup: Mark all sessions as disconnected (server restart clears in-memory instances)
async function cleanupOldSessions() {
  try {
    const result = await updateReturningMany(
      whatsappSessions,
      eq(whatsappSessions.status, "connected"),
      { status: "disconnected" }
    );
    if (result.length > 0) {
      console.log(`[Startup] Marked ${result.length} sessions as disconnected (server restart)`);
    }
  } catch (error) {
    if (handleDbError(error, "Startup")) {
      return;
    }
    console.error("[Startup] Error cleaning up sessions:", error);
  }
}

// Auto-migrate: Add missing columns to tables
async function autoMigrate() {
  try {
    // Check if takeover_mode column exists in conversations table
    const { conversations } = await import("@whatsapp-blast/database");

    // Try to query with takeover_mode column
    try {
      await db.select().from(conversations).limit(1);
    } catch (err: any) {
      if (err.message?.includes("takeover_mode") || err.code === "42703") {
        console.log("[Startup] ⚠️ Missing takeover_mode column, adding...");
        // Add missing columns for takeover feature
        await db.execute(`
          ALTER TABLE conversations
          ADD COLUMN IF NOT EXISTS takeover_mode BOOLEAN DEFAULT false,
          ADD COLUMN IF NOT EXISTS takeover_expires_at TIMESTAMP,
          ADD COLUMN IF NOT EXISTS takeover_admin_id TEXT;
        `);
        console.log("[Startup] ✅ Added takeover_mode columns");
      } else {
        throw err;
      }
    }
  } catch (error) {
    if (handleDbError(error, "Startup")) {
      return;
    }
    console.error("[Startup] Error during auto-migrate:", error);
  }
}

const app = express();
const httpServer = createServer(app);
// Allow multiple origins for Socket.io (frontend domains)
const allowedOrigins = [
  process.env.FRONTEND_URL || "http://localhost:3000",
  "https://dev.owlscottage.com",
  "http://localhost:3000",
];

const io = new Server(httpServer, {
  cors: {
    origin: allowedOrigins,
    methods: ["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    credentials: true,
    allowedHeaders: ["Content-Type", "Authorization", "X-Session-ID"],
  },
  // Increase timeouts for large file uploads - very long timeout to prevent disconnects
  pingTimeout: 300000, // 5 minutes - increased to prevent disconnects during large uploads
  pingInterval: 60000, // 60 seconds - less frequent pings to reduce overhead
  maxHttpBufferSize: 2e8, // 200 MB - handles 50MB chunks + base64 overhead (~67MB)
});

// Middleware - CORS first (needed for all routes)
// Use the same allowedOrigins array defined above for Socket.io
app.use(cors({
  origin: allowedOrigins,
  credentials: true,
  allowedHeaders: ["Content-Type", "Authorization", "X-Session-ID", "X-Upload-ID", "X-Chunk-Index", "X-Byte-Offset"],
}));
app.use(cookieParser());

// IMPORTANT: Register upload routes BEFORE express.json() middleware
// This allows the upload route to use its own bodyParser with 500MB limit
app.use("/api/upload", uploadRoutes);

// Static file serving for uploads (no body parsing needed)
app.use("/data", express.static("data"));

// Health check (no body parsing needed)
app.get("/health", (req, res) => {
  res.json({
    status: isDbAvailable() ? "ok" : "degraded",
    database: {
      available: isDbAvailable(),
      message: getDbStatusMessage(),
    },
    timestamp: new Date().toISOString(),
    version: "1.0.0"
  });
});

// Now apply global body parsing middleware for other routes
// Increased limit for base64 uploads (campaign attachments)
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ extended: true, limit: "50mb" }));

// API Routes (these use the global body parser)
app.use("/api/auth", authRoutes);
app.use("/api/dashboard", dashboardRoutes);
app.use("/api/contacts", contactsRoutes);
app.use("/api/tags", tagsRoutes);
app.use("/api/campaigns", campaignsRoutes);
app.use("/api/conversations", conversationsRoutes);
app.use("/api/bot", botRoutes);
app.use("/api/profile", profileRoutes);
app.use("/api/messages", messagesRoutes);

// Socket.io handlers
setupSocketHandlers(io);

// Pass Socket.io to auth routes for WhatsApp QR events
setSocketIO(io);

// Pass Socket.io to message storage service for real-time message updates
setMessageStorageSocketIO(io);

// Export io for use in other modules
export { io };

export default app;


// Start server
// In Pterodactyl the frontend and backend use different ports. Prefer BACKEND_PORT
// for the API server, then fall back to PORT for local/dev compatibility.
const PORT = process.env.BACKEND_PORT || process.env.PORT || 3001;

// Increase timeout for large file uploads (30 minutes)
httpServer.timeout = 30 * 60 * 1000;
httpServer.keepAliveTimeout = 30 * 60 * 1000;
httpServer.headersTimeout = 30 * 60 * 1000;

httpServer.listen(PORT, async () => {
  console.log(`🚀 API Server running on http://localhost:${PORT}`);
  console.log(`📡 Socket.io ready for connections`);

  // Auto-migrate: Add missing columns
  await autoMigrate();

  // Cleanup old sessions (mark as disconnected since server restart cleared in-memory instances)
  await cleanupOldSessions();

  // Start blast scheduler (check for scheduled campaigns every 30 seconds)
  if (isDbAvailable()) {
    startScheduler(30000);
  } else {
    console.warn("[Startup] Scheduler not started because database is unavailable");
  }
});
