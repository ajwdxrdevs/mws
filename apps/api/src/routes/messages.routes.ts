import { Router } from "express";
import { db, conversations, whatsappSessions } from "@whatsapp-blast/database";
import { eq } from "drizzle-orm";
import { getRealUserId, getSessionId } from "../utils/get-user.js";
import { saveMessage } from "../services/message-storage.service.js";

const router = Router();

// Format phone number to WhatsApp JID format
function formatPhoneToJid(phoneNumber: string): string {
  // Keep only digits
  let clean = phoneNumber.replace(/[^\d]/g, "");

  // Handle Malaysian numbers: if starts with 0, replace with 60
  if (clean.startsWith("0") && !clean.startsWith("00")) {
    clean = "60" + clean.substring(1);
  }

  // Remove leading 00 if present
  if (clean.startsWith("00")) {
    clean = clean.substring(2);
  }

  return `${clean}@s.whatsapp.net`;
}

// POST /api/messages - Send individual WhatsApp message
router.post("/", async (req, res) => {
  try {
    const { to, phoneNumber, message, content, conversationId } = req.body;

    // 1. Resolve recipient and message content
    const recipientPhone = to || phoneNumber;
    const text = message || content;

    if (!recipientPhone && conversationId) {
      // Find phone number from conversation if ID was passed
      const conv = await db.query.conversations.findFirst({
        where: eq(conversations.id, conversationId),
      });
      if (conv) {
        req.body.to = conv.phoneNumber;
      }
    }

    const finalPhone = recipientPhone || req.body.to;

    if (!finalPhone) {
      return res.status(400).json({ error: "Missing recipient phone number (to/phoneNumber)" });
    }

    if (!text) {
      return res.status(400).json({ error: "Missing message content (message/content)" });
    }

    // 2. Authorization Check
    let userId: string | null = null;
    let isAuthorized = false;

    // System auth check (e.g. from leave/HR system) via API Key
    const apiKeyHeader = req.headers["x-api-key"] || req.headers["authorization"]?.toString().replace("Bearer ", "");
    const configuredApiKey = process.env.API_KEY || "whatsapp-blast-api-key-default-change-me";
    const isApiKeyConfigured = process.env.API_KEY !== undefined;

    if (isApiKeyConfigured && apiKeyHeader === configuredApiKey) {
      isAuthorized = true;
      console.log("[Messages API] Request authorized via configured API Key");
    } else if (!isApiKeyConfigured) {
      // If no API Key is configured in environment, allow calls (log warning)
      isAuthorized = true;
      console.log("[Messages API] Warning: API request allowed because no API_KEY is configured in .env");
    }

    // Session auth check (frontend Next.js proxy calls)
    const sessionId = getSessionId(req);
    if (sessionId) {
      userId = await getRealUserId(sessionId);
      isAuthorized = true;
      console.log("[Messages API] Request authorized via user session:", userId);
    }

    if (!isAuthorized) {
      return res.status(401).json({ error: "Unauthorized: Invalid API Key or Session" });
    }

    // 3. Retrieve WhatsApp instance
    // If not set by session, default to the first connected WhatsApp session in database
    if (!userId) {
      const activeSession = await db.query.whatsappSessions.findFirst({
        where: eq(whatsappSessions.status, "connected"),
      });
      if (activeSession) {
        userId = activeSession.userId;
      }
    }

    if (!userId) {
      return res.status(503).json({ error: "No connected WhatsApp session found. Please connect WhatsApp account on dashboard first." });
    }

    const { whatsappInstances } = await import("@whatsapp-blast/whatsapp");
    const wa = whatsappInstances.get(userId);

    if (!wa || !wa.isConnected()) {
      return res.status(503).json({ error: `WhatsApp client is not connected for session user: ${userId}` });
    }

    // Check if number is on WhatsApp first to avoid spam trigger
    console.log(`[Messages API] Checking if ${finalPhone} is registered on WhatsApp...`);
    try {
      const numberCheck = await wa.checkNumber(finalPhone);
      if (numberCheck && !numberCheck.isOnWhatsApp) {
        console.log(`[Messages API] Nombor ${finalPhone} tiada dalam WhatsApp. Menolak penghantaran.`);
        return res.status(400).json({ error: "Nombor telefon tidak berdaftar dengan WhatsApp" });
      }
    } catch (checkError: any) {
      // If the check itself fails (e.g. timeout), log but proceed to avoid blocking
      console.log(`[Messages API] WhatsApp registry check failed for ${finalPhone}, proceeding anyway:`, checkError.message || checkError);
    }

    // 4. Send the message via Baileys wrapper
    const jid = formatPhoneToJid(finalPhone);
    console.log(`[Messages API] Sending message to ${jid} using session ${userId}`);
    const sendResult = await wa.sendTextMessage(jid, text);

    // 5. Log and save to database (adds to conversation history)
    const waMessageId = sendResult?.key?.id || undefined;
    const saved = await saveMessage(userId, jid, text, true, Date.now(), undefined, waMessageId);

    res.json({
      success: true,
      messageId: saved?.message?.id || null,
      waMessageId: waMessageId || null,
      recipient: finalPhone,
      status: "sent",
    });

  } catch (error: any) {
    console.error("[Messages API] Error sending message:", error);
    res.status(500).json({ error: error.message || "Failed to send message" });
  }
});

export default router;
