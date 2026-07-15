import { Router, json, raw } from "express";
import { db, campaigns, campaignRecipients, contacts, contactTags } from "@whatsapp-blast/database";
import { eq, and, inArray, desc, count } from "drizzle-orm";
import { processCampaign, pauseCampaign, resumeCampaign } from "../services/blast.service.js";
import multer from "multer";
import path from "path";
import fs from "fs/promises";
import { getSessionId, getRealUserId } from "../utils/get-user.js";
import { deleteReturningOne, insertReturningOne, updateReturningOne } from "../utils/db-compat.js";

const router = Router();

// Pre-create upload directory once (synchronous, much faster)
const uploadDir = path.join(process.env.DATA_PATH || "./data", "uploads");
try {
  require("fs").mkdirSync(uploadDir, { recursive: true });
} catch (err) {
  // Directory exists or couldn't create, continue anyway
}

// Configure multer for media uploads
const storage = multer.diskStorage({
  destination: (_req, _file, cb) => {
    // Direct callback - no async, much faster
    cb(null, uploadDir);
  },
  filename: (_req, file, cb) => {
    // Generate unique filename with original extension
    const uniqueSuffix = Date.now() + "-" + Math.round(Math.random() * 1e9);
    const ext = path.extname(file.originalname);
    cb(null, "media-" + uniqueSuffix + ext);
  },
});

const upload = multer({
  storage,
  limits: {
    fileSize: 500 * 1024 * 1024, // 500MB max
    fieldSize: 500 * 1024 * 1024,
  },
  // Remove fileFilter for faster uploads - validation happens on client side
});

// Helper to determine media type from file
function getMediaType(file: Express.Multer.File): string {
  const mimetype = file.mimetype;
  if (mimetype.startsWith("image/")) return "image";
  if (mimetype.startsWith("video/")) return "video";
  return "document";
}

// Helper to determine media type from mimetype
function getMediaTypeFromMime(mimetype: string): string {
  if (mimetype.startsWith("image/")) return "image";
  if (mimetype.startsWith("video/")) return "video";
  return "document";
}

// POST /api/campaigns/upload-base64 - Upload media file via base64 JSON (no multer)
router.post("/upload-base64", async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    const userId = await getRealUserId(sessionId);

    const { base64, fileName, fileType } = req.body;

    if (!base64 || !fileName || !fileType) {
      return res.status(400).json({ error: "Missing required fields: base64, fileName, fileType" });
    }

    // Create uploads directory
    const uploadDir = path.join(process.env.DATA_PATH || "./data", "uploads");
    await fs.mkdir(uploadDir, { recursive: true });

    // Generate unique filename
    const uniqueSuffix = Date.now() + "-" + Math.round(Math.random() * 1e9);
    const ext = path.extname(fileName);
    const filename = "media-" + uniqueSuffix + ext;
    const filePath = path.join(uploadDir, filename);

    // Convert base64 to buffer and save
    const buffer = Buffer.from(base64, "base64");
    await fs.writeFile(filePath, buffer);

    const mediaType = getMediaTypeFromMime(fileType);

    res.json({
      url: `/uploads/${filename}`,
      type: mediaType,
      mimeType: fileType, // Preserve original mimetype for sending
      name: fileName,
      size: buffer.length,
    });
  } catch (error) {
    console.error("Error uploading file (base64):", error);
    res.status(500).json({ error: "Failed to upload file" });
  }
});

// POST /api/campaigns/upload - Upload media file via FormData (with multer)
router.post("/upload", (req, res, next) => {
  // Increase timeout for this specific request (30 minutes for large files)
  req.setTimeout(30 * 60 * 1000);
  res.setTimeout(30 * 60 * 1000);
  next();
}, upload.single("file"), async (req, res) => {
  const startTime = Date.now();
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    // Handle FormData upload (multer)
    if (!req.file) {
      return res.status(400).json({ error: "No file uploaded" });
    }

    console.log(`[Upload] File received: ${req.file.originalname} (${(req.file.size / 1024 / 1024).toFixed(2)} MB)`);

    const mediaType = getMediaType(req.file);
    const relativePath = `/uploads/${req.file.filename}`;

    const elapsed = Date.now() - startTime;
    console.log(`[Upload] Response sent in ${elapsed}ms`);

    // Send response immediately - don't wait for anything else
    res.json({
      url: relativePath,
      type: mediaType,
      mimeType: req.file.mimetype, // Preserve original mimetype for sending
      name: req.file.originalname,
      size: req.file.size,
    });
  } catch (error) {
    console.error("Error uploading file:", error);
    res.status(500).json({ error: "Failed to upload file" });
  }
});

// ============================================================================
// CHUNKED UPLOAD - For large files that exceed Cloudflare/proxy timeouts
// Uploads file in chunks (default 2MB each) to avoid connection timeouts
// ============================================================================

// Store active chunked uploads in memory (reset on server restart)
const activeChunkedUploads = new Map<string, {
  filename: string;
  originalName: string;
  mimeType: string;
  fileSize: number; // Total expected file size in bytes
  totalChunks: number;
  receivedChunks: Set<number>;
  chunkSize: number;
  filePath: string;
  createdAt: number;
}>();

// Clean up incomplete uploads after 6 hours (extended for large files on slow connections)
setInterval(() => {
  const now = Date.now();
  const staleThreshold = 6 * 60 * 60 * 1000; // 6 hours for Cloudflare Tunnel slow uploads
  for (const [uploadId, upload] of activeChunkedUploads.entries()) {
    if (now - upload.createdAt > staleThreshold) {
      // Delete incomplete file
      try {
        fs.unlink(upload.filePath).catch(() => {});
      } catch {}
      activeChunkedUploads.delete(uploadId);
      console.log(`[ChunkedUpload] Cleaned up stale upload: ${uploadId}`);
    }
  }
}, 5 * 60 * 1000); // Check every 5 minutes

// POST /api/campaigns/upload/chunked/start - Initialize chunked upload
router.post("/upload/chunked/start", json(), async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    console.log("[ChunkedUpload] Start request, sessionId:", sessionId?.substring(0, 8) + "...");

    if (!sessionId) {
      console.error("[ChunkedUpload] No session ID found in cookies or headers");
      return res.status(401).json({ error: "Unauthorized" });
    }

    const { filename, fileSize, mimeType, chunkSize = 2 * 1024 * 1024 } = req.body;
    console.log("[ChunkedUpload] Request body:", { filename, fileSize, mimeType, chunkSize });

    if (!filename || !fileSize || !mimeType) {
      console.error("[ChunkedUpload] Missing fields:", { filename, fileSize, mimeType });
      return res.status(400).json({ error: "Missing required fields: filename, fileSize, mimeType" });
    }

    // Ensure upload directory exists
    await fs.mkdir(uploadDir, { recursive: true });
    console.log("[ChunkedUpload] Upload dir ready:", uploadDir);

    // Generate unique upload ID and temp filename
    const uploadId = `${Date.now()}-${Math.round(Math.random() * 1e9)}`;
    const uniqueFilename = `chunked-${uploadId}${path.extname(filename)}`;
    const filePath = path.join(uploadDir, uniqueFilename);
    console.log("[ChunkedUpload] Creating file:", filePath);

    // Create empty file with full size
    const fileHandle = await fs.open(filePath, "w");
    await fileHandle.truncate(fileSize);
    await fileHandle.close();

    const totalChunks = Math.ceil(fileSize / chunkSize);

    activeChunkedUploads.set(uploadId, {
      filename: uniqueFilename,
      originalName: filename,
      mimeType,
      fileSize: Number(fileSize), // Store as number for comparison
      totalChunks,
      receivedChunks: new Set<number>(),
      chunkSize,
      filePath,
      createdAt: Date.now(),
    });

    console.log(`[ChunkedUpload] Started: ${filename} (${(fileSize / 1024 / 1024).toFixed(2)}MB, ${totalChunks} chunks, ID: ${uploadId})`);

    res.json({
      uploadId,
      chunkSize,
      totalChunks,
    });
  } catch (error: any) {
    console.error("[ChunkedUpload] Start error:", error?.message || error);
    console.error("[ChunkedUpload] Error stack:", error?.stack);
    res.status(500).json({ error: "Failed to start chunked upload", details: error?.message });
  }
});

// POST /api/campaigns/upload/chunked/chunk - Upload a single chunk
router.post("/upload/chunked/chunk", raw({ type: "application/octet-stream", limit: "50mb" }), async (req, res) => {
  const startTime = Date.now();
  try {
    const sessionId = getSessionId(req);
    console.log(`[ChunkedUpload-Chunk] Request received - sessionId: ${sessionId?.substring(0, 8)}..., uploadId: ${req.headers["x-upload-id"]}, chunk: ${req.headers["x-chunk-index"]}`);

    if (!sessionId) {
      console.error("[ChunkedUpload-Chunk] No session ID");
      return res.status(401).json({ error: "Unauthorized" });
    }

    const uploadId = req.headers["x-upload-id"] as string;
    const chunkIndex = parseInt(req.headers["x-chunk-index"] as string);
    const byteOffset = req.headers["x-byte-offset"] ? parseInt(req.headers["x-byte-offset"] as string) : null;

    if (!uploadId || isNaN(chunkIndex)) {
      console.error("[ChunkedUpload-Chunk] Missing uploadId or chunkIndex");
      return res.status(400).json({ error: "Missing upload ID or chunk index" });
    }

    const upload = activeChunkedUploads.get(uploadId);
    if (!upload) {
      console.error("[ChunkedUpload-Chunk] Upload not found:", uploadId);
      return res.status(404).json({ error: "Upload not found or expired" });
    }

    console.log(`[ChunkedUpload-Chunk] Writing chunk ${chunkIndex} (${req.body.length} bytes) at offset ${byteOffset ?? 'calculated'}`);

    // Use byte offset from header if provided (for random chunk sizes), otherwise calculate
    const offset = byteOffset ?? (chunkIndex * upload.chunkSize);
    const fileHandle = await fs.open(upload.filePath, "r+");
    await fileHandle.write(req.body, 0, req.body.length, offset);
    await fileHandle.close();

    upload.receivedChunks.add(chunkIndex);

    const progress = (upload.receivedChunks.size / upload.totalChunks) * 100;
    const elapsed = Date.now() - startTime;
    console.log(`[ChunkedUpload-Chunk] ========== Chunk ${chunkIndex + 1} SUCCESS (${elapsed}ms, ${progress.toFixed(1)}% complete) ==========`);

    res.json({
      chunkIndex,
      received: upload.receivedChunks.size,
      total: upload.totalChunks,
      progress: progress.toFixed(1),
    });
  } catch (error: any) {
    const elapsed = Date.now() - startTime;
    console.error(`[ChunkedUpload-Chunk] ========== ERROR after ${elapsed}ms:`, error?.message || error);
    console.error("[ChunkedUpload-Chunk] Error stack:", error?.stack);
    res.status(500).json({ error: "Failed to upload chunk", details: error?.message });
  }
});

// POST /api/campaigns/upload/chunked/complete - Finalize chunked upload
router.post("/upload/chunked/complete", json(), async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const { uploadId } = req.body;

    if (!uploadId) {
      return res.status(400).json({ error: "Missing upload ID" });
    }

    const upload = activeChunkedUploads.get(uploadId);
    if (!upload) {
      return res.status(404).json({ error: "Upload not found" });
    }

    // Get file stats first
    const stats = await fs.stat(upload.filePath);
    
    // Verify all bytes received (use file size, not chunk count - supports adaptive chunk sizing)
    if (stats.size !== upload.fileSize) {
      return res.status(400).json({
        error: "Incomplete upload",
        received: stats.size,
        expected: upload.fileSize,
        chunks: upload.receivedChunks.size,
      });
    }

    // Rename to final filename
    const finalFilename = `media-${Date.now()}${path.extname(upload.originalName)}`;
    const finalPath = path.join(uploadDir, finalFilename);
    await fs.rename(upload.filePath, finalPath);

    // Clean up
    activeChunkedUploads.delete(uploadId);

    const mediaType = getMediaTypeFromMime(upload.mimeType);

    console.log(`[ChunkedUpload] Complete: ${upload.originalName} -> ${finalFilename}`);

    res.json({
      url: `/uploads/${finalFilename}`,
      type: mediaType,
      mimeType: upload.mimeType,
      name: upload.originalName,
      size: stats.size,
    });
  } catch (error) {
    console.error("[ChunkedUpload] Complete error:", error);
    res.status(500).json({ error: "Failed to complete chunked upload" });
  }
});

// POST /api/campaigns/upload/chunked/cancel - Cancel chunked upload
router.post("/upload/chunked/cancel", json(), async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const { uploadId } = req.body;

    if (uploadId) {
      const upload = activeChunkedUploads.get(uploadId);
      if (upload) {
        try {
          await fs.unlink(upload.filePath);
        } catch {}
        activeChunkedUploads.delete(uploadId);
        console.log(`[ChunkedUpload] Cancelled: ${uploadId}`);
      }
    }

    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: "Failed to cancel upload" });
  }
});

// GET /api/campaigns/upload/chunked/status/:uploadId - Check upload status (for resume)
router.get("/upload/chunked/status/:uploadId", async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const { uploadId } = req.params;
    const upload = activeChunkedUploads.get(uploadId);

    if (!upload) {
      return res.status(404).json({ error: "Upload not found or expired" });
    }

    res.json({
      uploadId,
      filename: upload.originalName,
      totalChunks: upload.totalChunks,
      receivedChunks: Array.from(upload.receivedChunks),
      receivedCount: upload.receivedChunks.size,
      progress: ((upload.receivedChunks.size / upload.totalChunks) * 100).toFixed(1),
      chunkSize: upload.chunkSize,
      createdAt: upload.createdAt,
    });
  } catch (error) {
    res.status(500).json({ error: "Failed to get upload status" });
  }
});

// GET /api/campaigns - List all campaigns
router.get("/", async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    const userId = await getRealUserId(sessionId);

    const { status, page = "1", limit = "20" } = req.query;
    const pageNum = parseInt(page as string);
    const limitNum = parseInt(limit as string);
    const offset = (pageNum - 1) * limitNum;

    const userCampaigns = await db.query.campaigns.findMany({
      where: and(
        eq(campaigns.userId, userId),
        status ? eq(campaigns.status, status as string) : undefined
      ),
      orderBy: [desc(campaigns.createdAt)],
      limit: limitNum,
      offset,
    });

    const [countResult] = await db
      .select({ count: count() })
      .from(campaigns)
      .where(eq(campaigns.userId, userId));

    res.json({
      campaigns: userCampaigns,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total: countResult?.count || 0,
      },
    });
  } catch (error) {
    console.error("Error listing campaigns:", error);
    res.status(500).json({ error: "Failed to list campaigns" });
  }
});

// GET /api/campaigns/:id - Get campaign details
router.get("/:id", async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    const userId = await getRealUserId(sessionId);

    const campaign = await db.query.campaigns.findFirst({
      where: and(eq(campaigns.id, req.params.id), eq(campaigns.userId, userId)),
      with: {
        recipients: true,
      },
    });

    if (!campaign) {
      return res.status(404).json({ error: "Campaign not found" });
    }

    res.json(campaign);
  } catch (error) {
    console.error("Error getting campaign:", error);
    res.status(500).json({ error: "Failed to get campaign" });
  }
});

// POST /api/campaigns - Create new campaign
router.post("/", async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    const userId = await getRealUserId(sessionId);
    console.log(`[Campaign] Creating campaign - sessionId: ${sessionId}, userId: ${userId}`);

    const {
      title,
      message,
      mediaUrl,
      mediaType,
      attachments, // New: multiple attachments support
      recipientIds,
      scheduledAt,
      delayMin,
      delayMax,
    } = req.body;

    if (!title || !message) {
      return res.status(400).json({ error: "Title and message are required" });
    }

    // Create campaign
    const campaign = await insertReturningOne(campaigns, {
      userId,
      title,
      message,
      mediaUrl,
      mediaType,
      attachments: attachments || null, // Store attachments array as JSON
      status: scheduledAt ? "scheduled" : "draft",
      recipientCount: recipientIds?.length || 0,
      scheduledAt: scheduledAt ? new Date(scheduledAt) : null,
      delayMin: delayMin !== undefined ? delayMin : 20000,
      delayMax: delayMax !== undefined ? delayMax : 60000,
    });

    // Add recipients if provided
    if (recipientIds && recipientIds.length > 0) {
      const contactsList = await db.query.contacts.findMany({
        where: and(
          eq(contacts.userId, userId),
          inArray(contacts.id, recipientIds)
        ),
      });

      await db.insert(campaignRecipients).values(
        contactsList.map((contact) => ({
          campaignId: campaign.id,
          contactId: contact.id,
          phoneNumber: contact.phoneNumber,
          name: contact.name,
          customData: JSON.stringify(contact.customData),
        }))
      );
    }

    res.status(201).json(campaign);
  } catch (error) {
    console.error("Error creating campaign:", error);
    res.status(500).json({ error: "Failed to create campaign" });
  }
});

// POST /api/campaigns/:id/start - Start/resume campaign
router.post("/:id/start", async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    const userId = await getRealUserId(sessionId);

    const campaign = await db.query.campaigns.findFirst({
      where: and(eq(campaigns.id, req.params.id), eq(campaigns.userId, userId)),
    });

    if (!campaign) {
      return res.status(404).json({ error: "Campaign not found" });
    }

    if (campaign.status === "running") {
      return res.status(400).json({ error: "Campaign is already running" });
    }

    const updated = await updateReturningOne(campaigns, eq(campaigns.id, req.params.id), {
      status: "running",
      startedAt: campaign.startedAt || new Date(),
      updatedAt: new Date(),
    });

    // Trigger blast service in background
    processCampaign(campaign.id).catch(err => {
      console.error(`[Campaign] Error processing campaign ${campaign.id}:`, err);
    });

    res.json(updated);
  } catch (error) {
    console.error("Error starting campaign:", error);
    res.status(500).json({ error: "Failed to start campaign" });
  }
});

// POST /api/campaigns/:id/pause - Pause running campaign
router.post("/:id/pause", async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    const userId = await getRealUserId(sessionId);

    // Tell blast service to pause
    pauseCampaign(req.params.id);

    const updated = await updateReturningOne(
      campaigns,
      and(eq(campaigns.id, req.params.id), eq(campaigns.userId, userId)),
      {
        status: "paused",
        updatedAt: new Date(),
      }
    );

    if (!updated) {
      return res.status(404).json({ error: "Campaign not found" });
    }

    res.json(updated);
  } catch (error) {
    console.error("Error pausing campaign:", error);
    res.status(500).json({ error: "Failed to pause campaign" });
  }
});

// POST /api/campaigns/:id/resume - Resume paused campaign
router.post("/:id/resume", async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    const userId = await getRealUserId(sessionId);

    const campaign = await db.query.campaigns.findFirst({
      where: and(eq(campaigns.id, req.params.id), eq(campaigns.userId, userId)),
    });

    if (!campaign) {
      return res.status(404).json({ error: "Campaign not found" });
    }

    if (campaign.status === "running") {
      return res.status(400).json({ error: "Campaign is already running" });
    }

    const updated = await updateReturningOne(campaigns, eq(campaigns.id, req.params.id), {
      status: "running",
      updatedAt: new Date(),
    });

    // Tell blast service to resume and process campaign
    resumeCampaign(req.params.id);
    processCampaign(campaign.id).catch(err => {
      console.error(`[Campaign] Error processing campaign ${campaign.id}:`, err);
    });

    res.json(updated);
  } catch (error) {
    console.error("Error resuming campaign:", error);
    res.status(500).json({ error: "Failed to resume campaign" });
  }
});

// GET /api/campaigns/:id/progress - Get send progress
router.get("/:id/progress", async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    const userId = await getRealUserId(sessionId);

    const campaign = await db.query.campaigns.findFirst({
      where: and(eq(campaigns.id, req.params.id), eq(campaigns.userId, userId)),
    });

    if (!campaign) {
      return res.status(404).json({ error: "Campaign not found" });
    }

    res.json({
      campaignId: campaign.id,
      status: campaign.status,
      total: campaign.recipientCount,
      sent: campaign.sentCount,
      delivered: campaign.deliveredCount,
      failed: campaign.failedCount,
      progress: (campaign.recipientCount || 0) > 0
        ? Math.round(((campaign.sentCount || 0) / (campaign.recipientCount || 0)) * 100)
        : 0,
    });
  } catch (error) {
    console.error("Error getting progress:", error);
    res.status(500).json({ error: "Failed to get progress" });
  }
});

// DELETE /api/campaigns/:id - Delete campaign
router.delete("/:id", async (req, res) => {
  try {
    const sessionId = getSessionId(req);
    if (!sessionId) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    const userId = await getRealUserId(sessionId);

    const deleted = await deleteReturningOne(
      campaigns,
      and(eq(campaigns.id, req.params.id), eq(campaigns.userId, userId))
    );

    if (!deleted) {
      return res.status(404).json({ error: "Campaign not found" });
    }

    res.json({ success: true, deleted });
  } catch (error) {
    console.error("Error deleting campaign:", error);
    res.status(500).json({ error: "Failed to delete campaign" });
  }
});

export default router;
