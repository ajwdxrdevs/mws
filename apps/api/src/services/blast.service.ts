import { db, campaigns, campaignRecipients, contacts, whatsappSessions } from "@whatsapp-blast/database";
import { eq, and, inArray, sql } from "drizzle-orm";
import { getWhatsAppInstance, hasActiveInstance } from "@whatsapp-blast/whatsapp";
import fs from "fs/promises";
import path from "path";
import { handleDbError } from "../utils/db-errors.js";

// Track active campaigns for pausing
const activeCampaigns = new Set<string>();

// Format phone number to WhatsApp JID format
// Handles +60, 60, 0xxx formats correctly
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

// Helper to check if campaign is paused
export function isCampaignPaused(campaignId: string): boolean {
  return !activeCampaigns.has(campaignId);
}

// Helper to pause a campaign
export function pauseCampaign(campaignId: string) {
  activeCampaigns.delete(campaignId);
}

// Helper to resume a campaign
export function resumeCampaign(campaignId: string) {
  activeCampaigns.add(campaignId);
}

// Helper to parse Spintax like {Hello|Hi|Salam}
function parseSpintax(text: string): string {
  const spintaxPattern = /\{([^{}]+)\}/g;
  let matches;
  let newText = text;
  while ((matches = spintaxPattern.exec(newText)) !== null) {
    const options = matches[1].split('|');
    const randomOption = options[Math.floor(Math.random() * options.length)];
    newText = newText.replace(matches[0], randomOption);
    spintaxPattern.lastIndex = 0;
  }
  return newText;
}

// Personalize message with contact data
function personalizeMessage(message: string, contact: {
  name: string | null;
  phoneNumber: string;
  customData: string | null;
}): string {
  let personalized = message;

  // Replace {{name}} with contact name or fallback (supports both {{name}} and legacy {{nama}})
  personalized = personalized.replace(/\{\{(name|nama)\}\}/gi, contact.name || "there");

  // Replace {{phone}} with phone number
  personalized = personalized.replace(/\{\{phone\}\}/gi, contact.phoneNumber);

  // Replace {{date}} with today's date
  const today = new Date();
  const formatDate = today.toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric'
  });
  personalized = personalized.replace(/\{\{date\}\}/gi, formatDate);

  // Replace custom data variables like {{company}}, {{position}}, etc.
  if (contact.customData) {
    try {
      const customData = JSON.parse(contact.customData);
      for (const [key, value] of Object.entries(customData)) {
        personalized = personalized.replace(
          new RegExp(`\\{\\{${key}\\}\\}`, "gi"),
          String(value)
        );
      }
    } catch (e) {
      // Invalid JSON, skip
    }
  }

  return parseSpintax(personalized);
}

// Get random delay between min and max
function getRandomDelay(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Retry helper for WhatsApp sends (handles timeout issues)
async function retrySendMessage<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3,
  delayMs: number = 10000, // Increased from 2s to 10s for network stability
  campaignUserId?: string
): Promise<T> {
  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`[Blast] Send attempt ${attempt}/${maxRetries} starting...`);

      // Add explicit timeout wrapper - don't let sends hang indefinitely
      const result = await Promise.race([
        fn(),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("Send timeout after 90 seconds")), 90000)
        )
      ]);

      console.log(`[Blast] Send attempt ${attempt}/${maxRetries} succeeded`);
      return result;
    } catch (error: any) {
      lastError = error;
      const isTimeout = error?.output?.statusCode === 408 || error?.message?.includes('Timed Out') || error?.message?.includes('timeout');

      console.log(`[Blast] Send attempt ${attempt}/${maxRetries} failed:`, error?.message || error);

      // Don't retry if it's not a timeout (e.g., blocked, not on WhatsApp, etc.)
      if (!isTimeout || attempt >= maxRetries) {
        if (attempt >= maxRetries && isTimeout) {
          console.log(`[Blast] Max retries reached for timeout error - connection is stale`);

          // Mark session as disconnected since connection is stale
          if (campaignUserId) {
            const { whatsappSessions } = await import("@whatsapp-blast/database");
            const { eq } = await import("drizzle-orm");
            const { db } = await import("@whatsapp-blast/database");

            db.update(whatsappSessions)
              .set({ status: "disconnected" })
              .where(eq(whatsappSessions.userId, campaignUserId))
              .then(() => console.log(`[Blast] Marked session as disconnected due to timeout`))
              .catch(() => {});
          }
        }
        throw error;
      }

      // Wait before retry
      console.log(`[Blast] Retrying in ${delayMs}ms (${delayMs/1000}s)...`);
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }

  throw lastError;
}

// Simple text sender that tries to send using raw protocol to avoid USync
async function sendSimpleTextMessage(wa: any, jid: string, text: string): Promise<any> {
  try {
    // First try the normal send
    return await wa.sendTextMessage(jid, text);
  } catch (error) {
    console.log(`[Blast] Normal send failed, trying alternative method...`);
    // If that fails, the socket might have a lower-level method we can try
    // For now, just re-throw since we don't have a good alternative
    throw error;
  }
}

// Process a single campaign
export async function processCampaign(campaignId: string): Promise<void> {
  try {
    console.log(`[Blast] Starting campaign ${campaignId}`);

    // Mark campaign as active
    activeCampaigns.add(campaignId);

    // Get campaign details
    const campaign = await db.query.campaigns.findFirst({
      where: eq(campaigns.id, campaignId),
    });

    if (!campaign) {
      console.error(`[Blast] Campaign ${campaignId} not found`);
      return;
    }

    // Get user's WhatsApp session from database (for debugging only - actual connection checked later)
    // NOTE: Database status may be stale after server restart, so we rely on in-memory instances
    const allUserSessions = await db.query.whatsappSessions.findMany({
      where: eq(whatsappSessions.userId, campaign.userId),
    });

    // Debug: Show ALL WhatsApp sessions in database
    const allSessions = await db.query.whatsappSessions.findMany();
    console.log(`[Blast] All WhatsApp sessions in DB:`, allSessions.map(s => ({
      id: s.id,
      userId: s.userId,
      browserSessionId: s.browserSessionId,
      status: s.status,
      phoneNumber: s.phoneNumber,
    })));

    // Debug: Log what we found
    console.log(`[Blast] Looking for WhatsApp session with userId: ${campaign.userId}`);
    console.log(`[Blast] Found ${allUserSessions.length} sessions for this user`);

    // CRITICAL: Check in-memory instances FIRST (actual connection state)
    // Database status may be stale after server restart
    const { whatsappInstances } = await import("@whatsapp-blast/whatsapp");

    // Find a connected instance that MATCHES the campaign's userId
    let connectedInstance: any = null;
    let foundInstanceKey: string | null = null;

    console.log(`[Blast] Searching for healthy connected instance among ${whatsappInstances.size} instances...`);
    console.log(`[Blast] Campaign userId: ${campaign.userId}`);
    console.log(`[Blast] All instance keys:`, Array.from((whatsappInstances as Map<string, any>).keys()));

    // CRITICAL FIX: Only use instance that matches campaign's userId
    // Instance keys are in format "wa:phoneNumber" (e.g., "wa:601111530402")
    const campaignUserId = campaign.userId; // e.g., "wa:601111530402"

    for (const [key, instance] of (whatsappInstances as Map<string, any>).entries()) {
      const isConnected = instance.isConnected();
      console.log(`[Blast] Instance ${key}: connected=${isConnected}, matches=${key === campaignUserId}`);
      // Check BOTH connected AND userId matches
      if (isConnected && key === campaignUserId) {
        console.log(`[Blast] Found matching connected instance: ${key}`);
        connectedInstance = instance;
        foundInstanceKey = key;
        break;
      }
    }

    // Use the connected instance we found directly, or create one if needed
    let wa: any;
    if (connectedInstance) {
      console.log(`[Blast] Using existing connected instance: ${foundInstanceKey}`);
      wa = connectedInstance;

      // Update database session status to match reality
      if (allUserSessions.length > 0) {
        await db.update(whatsappSessions)
          .set({ status: "connected" })
          .where(eq(whatsappSessions.userId, campaign.userId));
        console.log(`[Blast] Updated database session status to "connected"`);
      }
    } else {
      // No connected instance found for this specific user - cannot proceed
      console.error(`[Blast] No connected instance found for user ${campaign.userId}`);
      console.error(`[Blast] Available instances:`, Array.from((whatsappInstances as Map<string, any>).keys()).join(", "));
      await db.update(campaigns)
        .set({
          status: "failed",
          completedAt: new Date(),
          updatedAt: new Date(),
          errorMessage: `WhatsApp account not connected. Please connect your WhatsApp account and try again.`,
        })
        .where(eq(campaigns.id, campaignId));
      return;
    }

    if (!wa.isConnected()) {
      console.error(`[Blast] WhatsApp not connected for user ${campaign.userId}`);
      await db.update(campaigns)
        .set({
          status: "failed",
          completedAt: new Date(),
          updatedAt: new Date(),
        })
        .where(eq(campaigns.id, campaignId));
      return;
    }

    // CRITICAL: Pre-blast connection health check
    // Try to fetch the user's own profile to verify connection is actually working
    console.log(`[Blast] Running pre-blast health check...`);
    let connectionHealthy = false;
    try {
      const userJid = wa.getUser()?.id;
      if (userJid) {
        // Quick query with timeout to verify connection
        const healthCheckPromise = wa.socket?.fetchStatus(userJid);
        if (healthCheckPromise) {
          await Promise.race([
            healthCheckPromise,
            new Promise((_, reject) => setTimeout(() => reject(new Error("Health check timeout")), 10000))
          ]);
          connectionHealthy = true;
          console.log(`[Blast] Health check passed - connection is healthy`);
        } else {
          console.log(`[Blast] Warning: fetchStatus not available, will proceed anyway`);
          connectionHealthy = true; // Assume healthy if method not available
        }
      } else {
        console.log(`[Blast] Warning: No user JID found, will proceed anyway`);
        connectionHealthy = true; // Assume healthy if no user JID
      }
    } catch (healthError) {
      console.error(`[Blast] Health check failed:`, healthError);
      connectionHealthy = false;
    }

    if (!connectionHealthy) {
      console.error(`[Blast] Connection health check failed - cannot proceed`);
      await db.update(campaigns)
        .set({
          status: "failed",
          completedAt: new Date(),
          updatedAt: new Date(),
        })
        .where(eq(campaigns.id, campaignId));

      // Also mark session as disconnected
      if (allUserSessions.length > 0) {
        await db.update(whatsappSessions)
          .set({ status: "disconnected" })
          .where(eq(whatsappSessions.userId, campaign.userId));
      }

      return;
    }

    // Get pending recipients
    const recipients = await db.query.campaignRecipients.findMany({
      where: and(
        eq(campaignRecipients.campaignId, campaignId),
        eq(campaignRecipients.status, "pending")
      ),
    });

    console.log(`[Blast] Campaign ${campaignId} has ${recipients.length} pending recipients`);

    // CRITICAL: Wait for connection to fully stabilize before sending
    // The health check passed but connection might still be warming up
    console.log(`[Blast] Waiting 3 seconds for connection to stabilize before sending...`);
    await new Promise(resolve => setTimeout(resolve, 3000));

    // NOTE: Device cache warmup removed - it was timing out and not helping
    // The socket now has patched getUSyncDevices that handles timeouts gracefully

    console.log(`[Blast] Starting message send loop for ${recipients.length} recipients...`);

    if (recipients.length === 0) {
      // No recipients, mark as completed
      await db.update(campaigns)
        .set({
          status: "completed",
          completedAt: new Date(),
          updatedAt: new Date(),
        })
        .where(eq(campaigns.id, campaignId));
      activeCampaigns.delete(campaignId);
      return;
    }

    let sentCount = 0;
    let failedCount = 0;

    for (const recipient of recipients) {
      // Check if campaign was paused
      if (!activeCampaigns.has(campaignId)) {
        console.log(`[Blast] Campaign ${campaignId} was paused`);
        await db.update(campaigns)
          .set({ status: "paused", updatedAt: new Date() })
          .where(eq(campaigns.id, campaignId));
        return;
      }

      console.log(`[Blast] Preparing to send to ${recipient.phoneNumber} (mediaType: ${campaign.mediaType || "text"})...`);

      try {
        // Personalize message
        const personalizedMessage = personalizeMessage(campaign.message, {
          name: recipient.name,
          phoneNumber: recipient.phoneNumber,
          customData: recipient.customData,
        });

        // Check if number is on WhatsApp first to avoid spam trigger
        console.log(`[Blast] Checking if ${recipient.phoneNumber} is registered on WhatsApp...`);
        try {
          const numberCheck = await wa.checkNumber(recipient.phoneNumber);
          if (numberCheck && !numberCheck.isOnWhatsApp) {
            console.log(`[Blast] Nombor ${recipient.phoneNumber} tiada dalam WhatsApp. Menandakan sebagai gagal.`);
            throw new Error("Nombor telefon tidak berdaftar dengan WhatsApp");
          }
        } catch (checkError: any) {
          // If the check itself fails (e.g., timeout), log and proceed to avoid blocking healthy sends
          console.log(`[Blast] WhatsApp registry check failed for ${recipient.phoneNumber}, proceeding anyway:`, checkError.message || checkError);
          // If it is our thrown error, re-throw it to fail this recipient
          if (checkError.message === "Nombor telefon tidak berdaftar dengan WhatsApp") {
            throw checkError;
          }
        }

        console.log(`[Blast] Message prepared, sending...`);

        // Send message based on media type (with retry for timeouts)
        let sendResult;
        const jid = formatPhoneToJid(recipient.phoneNumber);
        console.log(`[Blast] Using JID: ${jid} (original: ${recipient.phoneNumber})`);

        // Check for new attachments array format (multiple attachments support)
        const attachments = campaign.attachments as { name: string; type: string; mimeType?: string; url: string; size: number }[] | null;
        const hasMultipleAttachments = attachments && attachments.length > 0;

        if (hasMultipleAttachments) {
          console.log(`[Blast] Sending ${attachments.length} attachments...`);

          // Send all attachments
          for (let i = 0; i < attachments.length; i++) {
            const attachment = attachments[i];
            console.log(`[Blast] Sending attachment ${i + 1}/${attachments.length}: ${attachment.name} (${attachment.type})`);

            try {
              const relativePath = attachment.url.startsWith("/") ? attachment.url.substring(1) : attachment.url;
              const mediaPath = path.join(process.env.DATA_PATH || "./data", relativePath);
              const fileBuffer = await fs.readFile(mediaPath);
              const filename = path.basename(mediaPath);
              const fileSizeMB = (fileBuffer.length / (1024 * 1024)).toFixed(2);

              // Determine attachment type and send accordingly
              const isImage = attachment.type.includes("image");
              const isVideo = attachment.type.includes("video");

              if (isImage) {
                console.log(`[Blast] Sending image (${fileSizeMB}MB)...`);
                // For image with caption, only send caption with the first image
                const caption = i === 0 ? personalizedMessage : "";
                await retrySendMessage(() => wa.sendImageMessage(jid, fileBuffer, caption), 3, 10000, campaign.userId);
              } else if (isVideo) {
                console.log(`[Blast] Sending video (${fileSizeMB}MB)...`);
                // For video with caption, only send caption with the first video
                const caption = i === 0 ? personalizedMessage : "";
                if (fileBuffer.length > 200 * 1024 * 1024) {
                  console.log(`[Blast] Video too large, sending as document...`);
                  await retrySendMessage(() => wa.sendDocumentMessage(jid, fileBuffer, filename, "video/mp4", caption), 3, 10000, campaign.userId);
                } else {
                  await retrySendMessage(() => wa.sendVideoMessage(jid, fileBuffer, caption), 3, 10000, campaign.userId);
                }
              } else {
                // Document/file - use mimeType if available, otherwise fallback to a default
                console.log(`[Blast] Sending document (${fileSizeMB}MB)...`);
                // For document with caption, only send caption with the first document
                const caption = i === 0 ? personalizedMessage : "";
                const docMimeType = attachment.mimeType || "application/octet-stream";
                await retrySendMessage(() => wa.sendDocumentMessage(jid, fileBuffer, filename, docMimeType, caption), 3, 10000, campaign.userId);
              }

              console.log(`[Blast] Attachment ${i + 1}/${attachments.length} sent successfully`);

              // Small delay between attachments to avoid rate limiting
              if (i < attachments.length - 1) {
                await new Promise(resolve => setTimeout(resolve, 1000));
              }
            } catch (attachError) {
              console.error(`[Blast] Failed to send attachment ${attachment.name}:`, attachError);
              // Continue with next attachment
            }
          }

          sendResult = { success: true };
          console.log(`[Blast] All attachments sent successfully`);
        } else if (campaign.mediaType === "image" && campaign.mediaUrl) {
          // Check if it's a local file path
          // Strip leading slash to avoid absolute path issue with path.join
          const relativePath = campaign.mediaUrl.startsWith("/") ? campaign.mediaUrl.substring(1) : campaign.mediaUrl;
          const mediaPath = path.join(process.env.DATA_PATH || "./data", relativePath);

          console.log(`[Blast] Reading image from: ${mediaPath}`);

          // Try to read the file
          try {
            const fileBuffer = await fs.readFile(mediaPath);
            console.log(`[Blast] Image file read, size: ${fileBuffer.length} bytes`);
            sendResult = await retrySendMessage(() => wa.sendImageMessage(jid, fileBuffer, personalizedMessage), 3, 10000, campaign.userId);
            console.log(`[Blast] Image sent successfully`);
          } catch (fileError) {
            console.error(`[Blast] Failed to read media file:`, fileError);
            // Fallback to text only
            sendResult = await retrySendMessage(() => wa.sendTextMessage(jid, personalizedMessage), 3, 10000, campaign.userId);
          }
        } else if (campaign.mediaType === "video" && campaign.mediaUrl) {
          // Handle video files
          // Strip leading slash to avoid absolute path issue with path.join
          const relativePath = campaign.mediaUrl.startsWith("/") ? campaign.mediaUrl.substring(1) : campaign.mediaUrl;
          const mediaPath = path.join(process.env.DATA_PATH || "./data", relativePath);

          console.log(`[Blast] Reading video from: ${mediaPath}`);

          try {
            const fileBuffer = await fs.readFile(mediaPath);
            const fileSizeMB = (fileBuffer.length / (1024 * 1024)).toFixed(2);
            console.log(`[Blast] Video file read, size: ${fileSizeMB} MB (${fileBuffer.length} bytes)`);

            // WhatsApp supports video up to ~200MB with playable preview
            // Send as video (not document) so it shows with thumbnail and can be played directly
            if (fileBuffer.length > 200 * 1024 * 1024) {
              console.log(`[Blast] Video too large (${fileSizeMB}MB > 200MB), sending as document...`);
              const filename = path.basename(mediaPath);
              sendResult = await retrySendMessage(() => wa.sendDocumentMessage(jid, fileBuffer, filename, "video/mp4"), 3, 10000, campaign.userId);
            } else {
              console.log(`[Blast] Sending video message (${fileSizeMB}MB)...`);
              sendResult = await retrySendMessage(() => wa.sendVideoMessage(jid, fileBuffer, personalizedMessage), 3, 10000, campaign.userId);
            }
            console.log(`[Blast] Video sent successfully`);
          } catch (fileError) {
            console.error(`[Blast] Failed to read/send video file:`, fileError);
            // Fallback to text only
            console.log(`[Blast] Falling back to text message...`);
            sendResult = await retrySendMessage(() => wa.sendTextMessage(jid, personalizedMessage), 3, 10000, campaign.userId);
          }
        } else if (campaign.mediaType === "document" && campaign.mediaUrl) {
          // Strip leading slash to avoid absolute path issue with path.join
          const relativePath = campaign.mediaUrl.startsWith("/") ? campaign.mediaUrl.substring(1) : campaign.mediaUrl;
          const mediaPath = path.join(process.env.DATA_PATH || "./data", relativePath);

          console.log(`[Blast] Reading document from: ${mediaPath}`);

          try {
            const fileBuffer = await fs.readFile(mediaPath);
            const filename = path.basename(mediaPath);
            console.log(`[Blast] Document file read, size: ${fileBuffer.length} bytes`);
            sendResult = await retrySendMessage(() => wa.sendDocumentMessage(jid, fileBuffer, filename, "application/octet-stream"), 3, 10000, campaign.userId);
            console.log(`[Blast] Document sent successfully`);
          } catch (fileError) {
            console.error(`[Blast] Failed to read document:`, fileError);
            sendResult = await retrySendMessage(() => wa.sendTextMessage(jid, personalizedMessage), 3, 10000, campaign.userId);
          }
        } else {
          // Text only
          console.log(`[Blast] Sending text message...`);
          sendResult = await retrySendMessage(() => wa.sendTextMessage(jid, personalizedMessage));
          console.log(`[Blast] Text sent successfully`);
        }

        // Update recipient status
        await db.update(campaignRecipients)
          .set({
            status: "sent",
            sentAt: new Date(),
          })
          .where(eq(campaignRecipients.id, recipient.id));

        sentCount++;

        // Update campaign progress
        await db.update(campaigns)
          .set({
            sentCount: sentCount,
            updatedAt: new Date(),
          })
          .where(eq(campaigns.id, campaignId));

        console.log(`[Blast] Sent to ${recipient.phoneNumber} (${sentCount}/${recipients.length})`);

        // Delay between sends to avoid rate limiting
        if (sentCount < recipients.length) {
          const delay = getRandomDelay(
            campaign.delayMin !== undefined ? campaign.delayMin : 20000,
            campaign.delayMax !== undefined ? campaign.delayMax : 60000
          );
          console.log(`[Blast] Waiting ${delay}ms before next send...`);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      } catch (sendError) {
        console.error(`[Blast] Failed to send to ${recipient.phoneNumber}:`, sendError);

        // Update recipient status to failed
        await db.update(campaignRecipients)
          .set({
            status: "failed",
            errorMessage: String(sendError),
          })
          .where(eq(campaignRecipients.id, recipient.id));

        failedCount++;
      }
    }

    // Check if all recipients are processed
    const finalRecipients = await db.query.campaignRecipients.findMany({
      where: eq(campaignRecipients.campaignId, campaignId),
    });

    const allProcessed = finalRecipients.every(r => r.status === "sent" || r.status === "failed");
    const hasFailures = failedCount > 0;

    if (allProcessed) {
      // Set status based on whether there were failures
      const finalStatus = hasFailures ? "partial" : "completed";

      await db.update(campaigns)
        .set({
          status: finalStatus,
          completedAt: new Date(),
          failedCount: failedCount,
          sentCount: sentCount,
          updatedAt: new Date(),
        })
        .where(eq(campaigns.id, campaignId));

      activeCampaigns.delete(campaignId);
      console.log(`[Blast] Campaign ${campaignId} ${finalStatus}. Sent: ${sentCount}, Failed: ${failedCount}`);
    }
  } catch (error) {
    if (handleDbError(error, "Blast")) {
      activeCampaigns.delete(campaignId);
      return;
    }
    console.error(`[Blast] Error processing campaign ${campaignId}:`, error);

    // Mark campaign as failed on error
    await db.update(campaigns)
      .set({
        status: "failed",
        updatedAt: new Date(),
      })
      .where(eq(campaigns.id, campaignId));

    activeCampaigns.delete(campaignId);
  }
}

// Check for scheduled campaigns and start them
export async function checkScheduledCampaigns(): Promise<void> {
  try {
    const now = new Date();

    // Find scheduled campaigns that should start
    const scheduledCampaigns = await db.query.campaigns.findMany({
      where: eq(campaigns.status, "scheduled"),
    });

    for (const campaign of scheduledCampaigns) {
      if (campaign.scheduledAt && new Date(campaign.scheduledAt) <= now) {
        console.log(`[Blast] Starting scheduled campaign ${campaign.id}`);
        // Start campaign in background
        processCampaign(campaign.id).catch(err => {
          console.error(`[Blast] Scheduled campaign error:`, err);
        });
      }
    }
  } catch (error) {
    if (handleDbError(error, "Blast")) {
      stopScheduler();
      console.warn("[Blast] Scheduler stopped because database is unavailable");
      return;
    }
    console.error("[Blast] Error checking scheduled campaigns:", error);
  }
}

// Start the scheduler
let schedulerInterval: NodeJS.Timeout | null = null;

export function startScheduler(intervalMs: number = 60000): void {
  if (schedulerInterval) {
    return; // Already running
  }

  console.log(`[Blast] Starting scheduler with ${intervalMs}ms interval`);

  schedulerInterval = setInterval(() => {
    checkScheduledCampaigns();
  }, intervalMs);

  // Check immediately on start
  checkScheduledCampaigns();
}

export function stopScheduler(): void {
  if (schedulerInterval) {
    clearInterval(schedulerInterval);
    schedulerInterval = null;
    console.log("[Blast] Scheduler stopped");
  }
}
