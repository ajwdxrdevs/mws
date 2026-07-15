import makeWASocket, {
  DisconnectReason,
  useMultiFileAuthState,
  WASocket,
  ConnectionState,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
} from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import pino from "pino";
import QRCode from "qrcode";
import path from "path";
import fs from "fs/promises";
import { EventEmitter } from "events";

export interface WhatsAppEvents {
  qr: (qrDataUrl: string) => void;
  connected: (phoneNumber: string, pushName: string) => void;
  disconnected: (reason: string) => void;
  message: (message: any) => void;
  status: (status: string) => void;
}

// Bot message handler callback - for auto-reply functionality
export type BotMessageHandler = (userId: string, message: {
  from: string;
  fromMe: boolean;
  body: string;
  timestamp: number;
  senderName?: string;  // Contact name from pushName
  waMessageId?: string;  // WhatsApp message ID for deduplication
  messageKey?: any;  // Full message key for deleting messages
}) => Promise<string | null>;

// Custom logger that filters out annoying error 515 (stream error) logs
const logger = pino({
  level: "warn",
}, pino.multistream([
  {
    level: "trace",
    stream: {
      write: (chunk: any) => {
        const logData = JSON.parse(chunk);
        // Filter out error 515 stream error logs - they're normal during QR scan
        if (logData?.node?.tag === "stream:error" && logData?.node?.attrs?.code === 515) {
          return; // Silently drop this log
        }
        // Log everything else normally
        console.log(JSON.stringify(logData));
      },
    },
  },
]));

export class WhatsAppService extends EventEmitter {
  private socket: WASocket | null = null;
  private userId: string;
  private sessionPath: string;
  private isConnecting: boolean = false;
  private reconnectAttempts: number = 0;
  private maxReconnectAttempts: number = 100; // Keep trying like real WhatsApp Web
  private connectionActive: boolean = false;
  private socketReady: boolean = false; // Socket is ready to send messages
  private forceFreshConnection: boolean = false;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private isDisconnected: boolean = false; // Track if instance was manually disconnected
  private keepAliveInterval: NodeJS.Timeout | null = null; // Keep-alive ping interval
  private botMessageHandler: BotMessageHandler | null = null; // Bot auto-reply handler
  private pendingMessages: Array<{ to: string; text: string; resolve: (value: any) => void; reject: (error: any) => void }> = []; // Queue for messages when socket not ready
  // Cache for LinkedIn ID to phone number mapping (from incoming messages)
  private linkedInPhoneCache: Map<string, string> = new Map(); // LinkedIn ID -> Phone JID
  // Cache for processed message IDs to prevent duplicates (WhatsApp sends same message as both append and notify)
  private processedMessageIds: Set<string> = new Set();
  // Clean up old message IDs every 5 minutes to prevent memory leak
  private messageIdCleanupInterval: NodeJS.Timeout | null = null;

  constructor(userId: string, dataPath: string = "./data") {
    super();
    this.userId = userId;
    this.sessionPath = path.join(dataPath, userId, "sessions", "baileys");
  }

  // Set the bot message handler for auto-reply
  setBotHandler(handler: BotMessageHandler | null): void {
    this.botMessageHandler = handler;
    console.log(`[${this.userId}] Bot handler ${handler ? 'registered' : 'unregistered'}`);
  }

  async connect(forceFresh: boolean = false): Promise<void> {
    // If forcing fresh connection, reset all state and allow reconnection
    if (forceFresh) {
      console.log(`[${this.userId}] Forcing fresh connection - resetting state...`);
      this.isConnecting = false;
      this.connectionActive = false;
      this.reconnectAttempts = 0;
      this.isDisconnected = false; // Reset disconnect flag
      // Also close existing socket if any
      if (this.socket) {
        try {
          (this.socket as any).ws?.close();
        } catch (e) {
          // Ignore
        }
        this.socket = null;
      }
    }

    // Prevent multiple simultaneous connections (unless forcing fresh)
    if (this.isConnecting || this.connectionActive) {
      console.log(`[${this.userId}] Already connecting or connected, skipping...`);
      return;
    }

    this.forceFreshConnection = forceFresh;
    this.isConnecting = true;
    this.emit("status", "connecting");

    try {
      // If forcing fresh connection, clear old session data first
      if (this.forceFreshConnection) {
        console.log(`[${this.userId}] Forcing fresh connection - clearing old session...`);
        await this.clearSession();
        this.reconnectAttempts = 0;
      }

      // Ensure session directory exists
      await fs.mkdir(this.sessionPath, { recursive: true });

      // Get latest version
      const { version, isLatest } = await fetchLatestBaileysVersion();
      console.log(`Using WA v${version.join(".")}, isLatest: ${isLatest}`);

      // Load auth state
      const { state, saveCreds } = await useMultiFileAuthState(this.sessionPath);

      // Create socket - removed printQRInTerminal (deprecated)
      this.socket = makeWASocket({
        version,
        auth: {
          creds: state.creds,
          keys: makeCacheableSignalKeyStore(state.keys, logger as any),
        },
        logger: logger as any,
        browser: ["burhan2ws", "Desktop", "1.0.0"],
        generateHighQualityLinkPreview: true,
        syncFullHistory: false,
        markOnlineOnConnect: true,
        defaultQueryTimeoutMs: 120000, // 120 seconds - increased for slow connections
        retryRequestDelayMs: 1000,
        // Options to reduce USync overhead
        fireInitQueries: false, // Don't fire initial queries that might timeout
      });

      // WORKAROUND: Patch getUSyncDevices to bypass device sync timeout
      // The issue is that getUSyncDevices times out on slow connections
      // We'll return empty device list immediately to allow send to proceed
      const originalGetUSyncDevices = (this.socket as any).getUSyncDevices;
      if (originalGetUSyncDevices) {
        (this.socket as any).getUSyncDevices = async (jids: string[]) => {
          console.log(`[${this.userId}] getUSyncDevices called for ${jids.length} jids, returning empty device list to bypass timeout`);

          // Return empty device map in correct format to allow send to proceed
          // Format: { [jid: string]: [] } - empty array means no additional devices
          const result: Record<string, any[]> = {};
          for (const jid of jids) {
            result[jid] = [];
          }
          return result;
        };
      }

      // Handle connection updates
      this.socket.ev.on("connection.update", async (update: Partial<ConnectionState>) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
          console.log(`[${this.userId}] 📱 QR Code received!`);
          this.reconnectAttempts = 0;
          try {
            const qrDataUrl = await QRCode.toDataURL(qr, {
              width: 300,
              margin: 2,
              color: {
                dark: "#000000",
                light: "#ffffff",
              },
            });
            this.emit("qr", qrDataUrl);
            this.emit("status", "qr_ready");
          } catch (err) {
            console.error("Error generating QR:", err);
          }
        }

        if (connection === "close") {
          this.isConnecting = false;
          this.connectionActive = false;
          this.socketReady = false; // Reset socket ready flag

          // Stop keep-alive interval when connection closes
          this.stopKeepAlive();

          const error = lastDisconnect?.error as Boom;
          const statusCode = error?.output?.statusCode;
          const errorData = error?.output?.payload;

          // Check if it's a conflict (another session replaced this one)
          const isConflict = statusCode === 440 || statusCode === DisconnectReason.connectionReplaced;
          const isLoggedOut = statusCode === DisconnectReason.loggedOut;
          const isError515 = statusCode === 515;
          const isTimedOut = statusCode === DisconnectReason.timedOut;

          // SILENTLY handle error 515 - stream error that happens during QR scan
          // Just reconnect immediately without showing any error
          if (isError515) {
            console.log(`[${this.userId}] Stream error 515 - silently reconnecting...`);
            this.reconnectAttempts++;
            const delay = 500; // Quick reconnect
            this.reconnectTimeout = setTimeout(() => this.connect(), delay);
            return;
          }

          // CRITICAL FIX: Check for manual disconnect FIRST, before emitting any events
          // Status 401 during manual disconnect is from our own logout() call - silence it
          if (this.isDisconnected) {
            console.log(`[${this.userId}] Instance was manually disconnected. Not reconnecting.`);
            // Reset flag for potential future connections
            this.isDisconnected = false;
            return;
          }

          console.log(`[${this.userId}] Connection closed. Status: ${statusCode}, isConflict: ${isConflict}, isLoggedOut: ${isLoggedOut}`);

          // Don't emit disconnected for temporary disconnects - only for permanent ones
          if (isConflict || isLoggedOut) {
            this.emit("status", "disconnected");
            this.emit("disconnected", statusCode?.toString() || "unknown");
          } else {
            // For temporary disconnects, just log it - don't emit "disconnected" event
            console.log(`[${this.userId}] Temporary disconnect - will reconnect...`);
          }

          // Don't reconnect on conflict or logout - just stop
          if (isConflict) {
            console.log(`[${this.userId}] Session replaced by another connection. Not reconnecting.`);
            return;
          }

          if (isLoggedOut) {
            console.log(`[${this.userId}] Logged out from phone. Clearing session...`);
            await this.clearSession();
            return;
          }

          // Reconnect for all other errors (timeout, network, etc.) - keep trying like WhatsApp Web
          const shouldReconnect = this.reconnectAttempts < this.maxReconnectAttempts;

          if (shouldReconnect) {
            this.reconnectAttempts++;
            // Use shorter delay for timeout errors - they're usually temporary
            const delay = isTimedOut ? 2000 : Math.min(1000 * Math.pow(1.5, this.reconnectAttempts), 30000);
            console.log(`[${this.userId}] Reconnecting in ${delay/1000}s... (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);
            this.reconnectTimeout = setTimeout(() => {
              // Reset connection flags to allow reconnect
              this.isConnecting = false;
              this.connectionActive = false;
              this.connect();
            }, delay);
          } else {
            console.log(`[${this.userId}] Max reconnect attempts reached. Giving up.`);
            this.emit("status", "disconnected");
            this.emit("disconnected", "max_attempts_reached");
          }
        }

        if (connection === "open") {
          this.isConnecting = false;
          this.connectionActive = true;
          this.socketReady = false; // Will be set to true after stabilization
          this.reconnectAttempts = 0;
          this.isDisconnected = false; // Reset disconnect flag on successful connection
          this.reconnectTimeout = null; // Clear reconnect timeout

          // Debug: Log full user object
          console.log(`[${this.userId}] 🔍 User object:`, JSON.stringify(this.socket?.user, null, 2));

          const phoneNumber = this.socket?.user?.id?.split(":")[0] || "";
          const pushName = this.socket?.user?.name || "";

          console.log(`[${this.userId}] ✅ WhatsApp connected:`, { phoneNumber, pushName });

          // Start keep-alive interval to prevent connection timeout
          // WhatsApp Web sends periodic pings to keep connection alive
          this.startKeepAlive();

          // Start message ID cleanup interval to prevent memory leak
          this.startMessageIdCleanup();

          // Wait for connection to stabilize before allowing sends
          setTimeout(() => {
            this.socketReady = true;
            console.log(`[${this.userId}] 🎯 Socket ready - can now send messages`);
            this.emit("status", "connected");
            this.emit("connected", phoneNumber, pushName);
            // Process any pending messages
            this.processPendingMessages();
          }, 3000); // 3 second stabilization delay
        }
      });

      // Save credentials on update
      this.socket.ev.on("creds.update", saveCreds);

      // Handle incoming messages (with bot auto-reply)
      this.socket.ev.on("messages.upsert", async (m) => {
        try {
          const message = m.messages[0];
          const messageId = message.key?.id; // Unique message ID from WhatsApp
          const fromJid = message.key?.remoteJid;
          const isFromMe = message.key?.fromMe || false;
          const messageBody = message.message?.conversation || message.message?.extendedTextMessage?.text || "";

          console.log(`[${this.userId}] 🔔 messages.upsert: type=${m.type}, fromMe=${isFromMe}, remoteJid=${fromJid}, body="${messageBody}"`);

          // Process both "notify" (incoming) and "append" (sent messages/history sync)
          // "notify" = new incoming message
          // "append" = sent message from user OR message history sync
          if (m.type !== "notify" && m.type !== "append") return;

          // Get the OTHER person's JID (the conversation partner)
          // If fromMe=true, the remoteJid is the OTHER person's number
          // If fromMe=false, the remoteJid is the SENDER's number
          let conversationJid = fromJid;

          if (!conversationJid) {
            console.log(`[${this.userId}] ⚠️ No conversationJid, skipping`);
            return;
          }

          // Get JID domain for filtering
          let jidDomain = conversationJid.split('@')[1];

          // For LinkedIn contacts (@lid), try to find the actual phone number
          if (jidDomain === "lid") {
            console.log(`[${this.userId}] 🔍 LinkedIn contact detected: ${conversationJid}, fromMe=${isFromMe}`);
            console.log(`[${this.userId}] PushName (sender name): "${message.pushName}"`);

            // Check ALL possible phone number fields in message.key
            const keyData = message.key || {};
            console.log(`[${this.userId}] 🔍 Checking key fields for phone number:`);
            console.log(`[${this.userId}]   - senderPn: "${keyData.senderPn}"`);
            console.log(`[${this.userId}]   - participant: "${keyData.participant}"`);
            console.log(`[${this.userId}]   - participantPn: "${keyData.participantPn}"`);
            console.log(`[${this.userId}]   - senderLid: "${keyData.senderLid}"`);
            console.log(`[${this.userId}]   - participantLid: "${keyData.participantLid}"`);

            let foundPhone = false;

            // Method 1: senderPn (Sender Phone Number) - available when fromMe=false (incoming)
            if (keyData.senderPn && typeof keyData.senderPn === 'string') {
              // senderPn might be "6062869016@s.whatsapp.net" or just "6062869016"
              if (keyData.senderPn.endsWith("@s.whatsapp.net")) {
                console.log(`[${this.userId}] ✅ Found phone in senderPn: ${keyData.senderPn}`);
                conversationJid = keyData.senderPn;
                // Cache: LinkedIn ID -> Phone JID (for future outgoing messages)
                this.linkedInPhoneCache.set(fromJid!, keyData.senderPn);
                console.log(`[${this.userId}] 💾 Cached: ${fromJid} -> ${keyData.senderPn}`);
                jidDomain = "s.whatsapp.net";
                foundPhone = true;
              } else if (keyData.senderPn.match(/^\d+$/)) {
                const phoneJid = `${keyData.senderPn}@s.whatsapp.net`;
                console.log(`[${this.userId}] ✅ Found phone in senderPn: ${phoneJid}`);
                conversationJid = phoneJid;
                // Cache: LinkedIn ID -> Phone JID
                this.linkedInPhoneCache.set(fromJid!, phoneJid);
                console.log(`[${this.userId}] 💾 Cached: ${fromJid} -> ${phoneJid}`);
                jidDomain = "s.whatsapp.net";
                foundPhone = true;
              }
            }
            // Method 2: participantPn (Participant Phone Number)
            else if (keyData.participantPn && typeof keyData.participantPn === 'string') {
              if (keyData.participantPn.endsWith("@s.whatsapp.net")) {
                console.log(`[${this.userId}] ✅ Found phone in participantPn: ${keyData.participantPn}`);
                conversationJid = keyData.participantPn;
                this.linkedInPhoneCache.set(fromJid!, keyData.participantPn);
                jidDomain = "s.whatsapp.net";
                foundPhone = true;
              } else if (keyData.participantPn.match(/^\d+$/)) {
                const phoneJid = `${keyData.participantPn}@s.whatsapp.net`;
                console.log(`[${this.userId}] ✅ Found phone in participantPn: ${phoneJid}`);
                conversationJid = phoneJid;
                this.linkedInPhoneCache.set(fromJid!, phoneJid);
                jidDomain = "s.whatsapp.net";
                foundPhone = true;
              }
            }
            // Method 3: participant field (might already be a phone JID)
            else if (keyData.participant && keyData.participant.endsWith("@s.whatsapp.net")) {
              console.log(`[${this.userId}] ✅ Found phone in participant: ${keyData.participant}`);
              conversationJid = keyData.participant;
              this.linkedInPhoneCache.set(fromJid!, keyData.participant);
              jidDomain = "s.whatsapp.net";
              foundPhone = true;
            }

            // Method 4: Check cache (for outgoing messages where senderPn is not available)
            if (!foundPhone) {
              const cachedPhone = this.linkedInPhoneCache.get(fromJid!);
              if (cachedPhone) {
                console.log(`[${this.userId}] ✅ Found phone in cache: ${cachedPhone}`);
                conversationJid = cachedPhone;
                jidDomain = "s.whatsapp.net";
                foundPhone = true;
              } else {
                console.log(`[${this.userId}] 🔍 Cache miss for LinkedIn ID: ${fromJid}`);
                console.log(`[${this.userId}] 💡 Cache contains: ${Array.from(this.linkedInPhoneCache.keys()).join(", ")}`);
              }
            }

            // If still LinkedIn JID, we couldn't find the phone number - skip
            if (jidDomain === "lid") {
              console.log(`[${this.userId}] 🚫 Could not find real phone number for LinkedIn contact: ${conversationJid}`);
              console.log(`[${this.userId}] 💡 This person's phone number is not available in WhatsApp`);
              console.log(`[${this.userId}] 💡 They need to send you a message first so we can cache their number`);
              return;
            }
          }

          // CRITICAL: ONLY accept WhatsApp phone numbers (@s.whatsapp.net)
          // BLOCK: groups (@g.us), newsletters (@newsletter), broadcast lists
          if (jidDomain !== "s.whatsapp.net") {
            console.log(`[${this.userId}] 🚫 Blocked non-phone JID: ${conversationJid} (type: ${jidDomain}) - Only real phone numbers allowed`);
            return;
          }

          // Skip empty messages
          if (!messageBody) {
            console.log(`[${this.userId}] ⚠️ Empty message body, skipping. Message type:`, Object.keys(message.message || {}));
            return;
          }

          console.log(`[${this.userId}] ✅ WhatsApp message accepted: fromMe=${isFromMe}, from="${conversationJid}", body="${messageBody}"`);

          // Use the conversation JID directly (already validated as WhatsApp)
          const realJid = conversationJid!; // Non-null assertion - we've validated above
          const displayName = message.pushName || "";

          // Emit message event for backward compatibility
          this.emit("message", message);

          // Process message for bot and storage
          if (this.botMessageHandler) {
            console.log(`[${this.userId}] 💬 Calling bot handler with: fromMe=${isFromMe}, conversationJid=${conversationJid}, body="${messageBody}"`);

            try {
              // Get WhatsApp message ID for deduplication
              const waMsgId = message.key?.id || undefined;

              // Get the original WhatsApp timestamp (in seconds, convert to milliseconds)
              const waTimestamp = message.messageTimestamp ? (message.messageTimestamp as number) * 1000 : Date.now();

              // Call bot handler with correct fromMe flag and waMessageId
              const response = await this.botMessageHandler(this.userId, {
                from: realJid,  // The conversation partner's JID
                fromMe: isFromMe,  // TRUE if user sent it, FALSE if received
                body: messageBody,
                timestamp: waTimestamp,  // Use WhatsApp's original timestamp
                senderName: displayName,
                waMessageId: waMsgId,  // WhatsApp message ID for deduplication
                messageKey: message.key,  // Full message key for deleting messages
              });

              // Only send bot reply for incoming messages (not fromMe)
              if (response && !isFromMe && this.socket && this.connectionActive && fromJid) {
                await this.socket.sendMessage(fromJid, { text: response });
                console.log(`[${this.userId}] ✅ Bot reply sent to ${fromJid}`);
              }
            } catch (botError) {
              console.error(`[${this.userId}] ❌ Bot processing error:`, botError);
            }
          } else {
            console.log(`[${this.userId}] ⚠️ No bot handler registered - message will not be saved to DB!`);
          }
        } catch (error) {
          console.error(`[${this.userId}] ❌ Error processing message:`, error);
        }
      });

    } catch (error) {
      this.isConnecting = false;
      this.connectionActive = false;
      console.error(`[${this.userId}] Connection error:`, error);
      this.emit("status", "error");
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    console.log(`[${this.userId}] Manually disconnecting...`);
    this.isDisconnected = true; // Mark as manually disconnected BEFORE logout
    this.connectionActive = false;
    this.isConnecting = false;

    // Stop keep-alive interval
    this.stopKeepAlive();

    // Stop message ID cleanup interval
    this.stopMessageIdCleanup();

    // Clear any pending reconnect timeout
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

    if (this.socket) {
      try {
        await this.socket.logout();
      } catch (e) {
        // Ignore logout errors
      }
      this.socket = null;
    }
    // Don't emit "disconnected" status for manual disconnects - we want this to be silent
  }

  // Start keep-alive interval to keep connection alive
  private startKeepAlive(): void {
    // Clear any existing interval first
    if (this.keepAliveInterval) {
      clearInterval(this.keepAliveInterval);
    }

    let badStateCount = 0; // Track how many times connection is in bad state

    // Send a keep-alive ping every 25 seconds
    // WhatsApp connection timeout is around 30-60 seconds, so 25s keeps it alive
    this.keepAliveInterval = setInterval(async () => {
      if (this.socket && this.connectionActive) {
        try {
          // Check if WebSocket is still open
          const ws = (this.socket as any).ws;
          if (!ws) {
            badStateCount++;
            // Only log occasionally
            if (badStateCount === 1 || badStateCount % 10 === 0) {
              console.log(`[${this.userId}] ⚠️ WebSocket not available (${badStateCount})`);
            }
            // Stop after 5 consecutive bad states
            if (badStateCount >= 5) {
              console.log(`[${this.userId}] 🛑 Stopping keep-alive - WebSocket unavailable too long`);
              this.stopKeepAlive();
            }
            return;
          }

          if (ws.readyState === 1) {
            // WebSocket is OPEN, send keep-alive
            badStateCount = 0; // Reset counter
            try {
              await this.socket.sendPresenceUpdate("available");
              console.log(`[${this.userId}] 💓 Keep-alive ping sent`);
            } catch (pingError) {
              badStateCount++;
              console.log(`[${this.userId}] Keep-alive ping error (${badStateCount}):`, pingError instanceof Error ? pingError.message : pingError);
              if (badStateCount >= 3) {
                console.log(`[${this.userId}] 🛑 Stopping keep-alive - too many ping errors`);
                this.stopKeepAlive();
              }
            }
          } else if (ws.readyState === 2) {
            // WebSocket is CLOSING - stop keep-alive
            badStateCount++;
            console.log(`[${this.userId}] ⚠️ WebSocket closing (${badStateCount}), stopping keep-alive`);
            if (badStateCount >= 2) {
              this.stopKeepAlive();
            }
          } else if (ws.readyState === 3) {
            // WebSocket is CLOSED - stop keep-alive
            badStateCount++;
            console.log(`[${this.userId}] ⚠️ WebSocket closed (${badStateCount}), stopping keep-alive`);
            if (badStateCount >= 2) {
              this.stopKeepAlive();
            }
          } else {
            // WebSocket is CONNECTING (0) - give it some time but stop if stuck
            badStateCount++;
            if (badStateCount <= 2) {
              console.log(`[${this.userId}] ⚠️ WebSocket connecting, waiting...`);
            }
            if (badStateCount >= 4) {
              console.log(`[${this.userId}] 🛑 WebSocket stuck connecting, stopping keep-alive`);
              this.stopKeepAlive();
            }
          }
        } catch (error) {
          badStateCount++;
          console.log(`[${this.userId}] Keep-alive check failed (${badStateCount}):`, error instanceof Error ? error.message : error);
          if (badStateCount >= 3) {
            this.stopKeepAlive();
          }
        }
      } else {
        // connectionActive is false - shouldn't happen but stop keep-alive
        badStateCount++;
        if (badStateCount >= 2) {
          console.log(`[${this.userId}] 🛑 Connection not active, stopping keep-alive`);
          this.stopKeepAlive();
        }
      }
    }, 25000); // Every 25 seconds

    badStateCount = 0; // Reset counter when interval starts
    console.log(`[${this.userId}] 💓 Keep-alive interval started (25s)`);
  }

  // Stop keep-alive interval
  private stopKeepAlive(): void {
    if (this.keepAliveInterval) {
      clearInterval(this.keepAliveInterval);
      this.keepAliveInterval = null;
      console.log(`[${this.userId}] 🛑 Keep-alive interval stopped`);
    }
  }

  // Start message ID cleanup interval to prevent memory leak
  private startMessageIdCleanup(): void {
    // Clear any existing interval first
    if (this.messageIdCleanupInterval) {
      clearInterval(this.messageIdCleanupInterval);
    }

    // Clean up old message IDs every 5 minutes
    this.messageIdCleanupInterval = setInterval(() => {
      const sizeBefore = this.processedMessageIds.size;
      this.processedMessageIds.clear();
      console.log(`[${this.userId}] 🧹 Cleaned up ${sizeBefore} processed message IDs from cache`);
    }, 5 * 60 * 1000); // 5 minutes

    console.log(`[${this.userId}] 🧹 Message ID cleanup interval started (5 minutes)`);
  }

  // Stop message ID cleanup interval
  private stopMessageIdCleanup(): void {
    if (this.messageIdCleanupInterval) {
      clearInterval(this.messageIdCleanupInterval);
      this.messageIdCleanupInterval = null;
    }
  }

  async clearSession(): Promise<void> {
    try {
      await fs.rm(this.sessionPath, { recursive: true, force: true });
      console.log(`[${this.userId}] Session cleared`);
    } catch (error) {
      console.error(`[${this.userId}] Error clearing session:`, error);
    }
  }

  async sendTextMessage(to: string, text: string): Promise<any> {
    if (!this.socket || !this.connectionActive) {
      throw new Error("WhatsApp not connected");
    }

    // Check if socket is ready to send
    if (!this.socketReady) {
      console.log(`[${this.userId}] ⏸️ Socket not ready, queuing message to ${to}`);
      return new Promise((resolve, reject) => {
        this.pendingMessages.push({ to, text, resolve, reject });
      });
    }

    // Check if socket is actually ready
    if (!this.socket.user) {
      throw new Error("WhatsApp socket not ready - no user object");
    }

    const jid = to.includes("@s.whatsapp.net") ? to : `${to}@s.whatsapp.net`;

    console.log(`[${this.userId}] 📤 Sending message to ${jid}: "${text}"`);

    try {
      // Simulate typing behavior (presence: composing)
      try {
        await this.socket.sendPresenceUpdate("composing", jid);
        // Random typing duration: 15ms per character, between 1.5s and 4s
        const typingDelay = Math.min(Math.max(text.length * 15, 1500), 4000);
        await new Promise(resolve => setTimeout(resolve, typingDelay));
        await this.socket.sendPresenceUpdate("paused", jid);
      } catch (presenceError) {
        console.log(`[${this.userId}] Failed to send presence composing:`, presenceError);
      }

      const result = await this.socket.sendMessage(jid, { text });
      console.log(`[${this.userId}] ✅ Message sent successfully`);
      return result;
    } catch (error) {
      console.error(`[${this.userId}] ❌ Send failed:`, error);
      throw error;
    }
  }

  private processPendingMessages(): void {
    if (this.pendingMessages.length === 0) return;

    console.log(`[${this.userId}] 📋 Processing ${this.pendingMessages.length} pending messages`);

    const messages = [...this.pendingMessages];
    this.pendingMessages = [];

    messages.forEach(async ({ to, text, resolve, reject }) => {
      try {
        const result = await this.sendTextMessage(to, text);
        resolve(result);
      } catch (error) {
        reject(error);
      }
    });
  }

  async sendImageMessage(to: string, imageBuffer: Buffer, caption?: string): Promise<any> {
    if (!this.socket || !this.connectionActive) {
      throw new Error("WhatsApp not connected");
    }

    const jid = to.includes("@s.whatsapp.net") ? to : `${to}@s.whatsapp.net`;

    if (caption) {
      try {
        await this.socket.sendPresenceUpdate("composing", jid);
        const typingDelay = Math.min(Math.max(caption.length * 15, 1500), 4000);
        await new Promise(resolve => setTimeout(resolve, typingDelay));
        await this.socket.sendPresenceUpdate("paused", jid);
      } catch (presenceError) {
        // Ignore
      }
    }

    return this.socket.sendMessage(jid, {
      image: imageBuffer,
      caption: caption || "",
    });
  }

  async sendDocumentMessage(to: string, docBuffer: Buffer, filename: string, mimetype: string, caption?: string): Promise<any> {
    if (!this.socket || !this.connectionActive) {
      throw new Error("WhatsApp not connected");
    }

    const jid = to.includes("@s.whatsapp.net") ? to : `${to}@s.whatsapp.net`;

    if (caption) {
      try {
        await this.socket.sendPresenceUpdate("composing", jid);
        const typingDelay = Math.min(Math.max(caption.length * 15, 1500), 4000);
        await new Promise(resolve => setTimeout(resolve, typingDelay));
        await this.socket.sendPresenceUpdate("paused", jid);
      } catch (presenceError) {
        // Ignore
      }
    }

    const message: any = {
      document: docBuffer,
      fileName: filename,
      mimetype,
    };
    if (caption) {
      message.caption = caption;
    }
    return this.socket.sendMessage(jid, message);
  }

  async sendVideoMessage(to: string, videoBuffer: Buffer, caption?: string): Promise<any> {
    if (!this.socket || !this.connectionActive) {
      throw new Error("WhatsApp not connected");
    }

    const jid = to.includes("@s.whatsapp.net") ? to : `${to}@s.whatsapp.net`;

    if (caption) {
      try {
        await this.socket.sendPresenceUpdate("composing", jid);
        const typingDelay = Math.min(Math.max(caption.length * 15, 1500), 4000);
        await new Promise(resolve => setTimeout(resolve, typingDelay));
        await this.socket.sendPresenceUpdate("paused", jid);
      } catch (presenceError) {
        // Ignore
      }
    }

    return this.socket.sendMessage(jid, {
      video: videoBuffer,
      caption: caption || "",
    });
  }

  // Delete a message (for hiding command messages from chat)
  async deleteMessage(messageKey: any): Promise<boolean> {
    if (!this.socket || !this.connectionActive) {
      console.log(`[${this.userId}] Cannot delete message: WhatsApp not connected`);
      return false;
    }

    try {
      const jid = messageKey?.remoteJid;
      if (!jid) {
        console.log(`[${this.userId}] Cannot delete message: No remoteJid in messageKey`);
        return false;
      }

      // Use Baileys' sendMessage with delete protocol
      // This deletes the message for everyone
      await this.socket.sendMessage(jid, {
        delete: messageKey
      });

      console.log(`[${this.userId}] ✅ Message deleted: ${messageKey.id}`);
      return true;
    } catch (error) {
      console.error(`[${this.userId}] ❌ Error deleting message:`, error);
      return false;
    }
  }

  // Edit a message (for replacing command messages with cleaner text)
  async editMessage(messageKey: any, newText: string): Promise<boolean> {
    if (!this.socket || !this.connectionActive) {
      console.log(`[${this.userId}] Cannot edit message: WhatsApp not connected`);
      return false;
    }

    try {
      const jid = messageKey?.remoteJid;
      if (!jid) {
        console.log(`[${this.userId}] Cannot edit message: No remoteJid in messageKey`);
        return false;
      }

      // Use Baileys' sendMessage with edit protocol
      // This edits the message in place
      await this.socket.sendMessage(jid, {
        text: newText,
        edit: messageKey
      });

      console.log(`[${this.userId}] ✅ Message edited: ${messageKey.id} -> "${newText.substring(0, 50)}..."`);
      return true;
    } catch (error) {
      console.error(`[${this.userId}] ❌ Error editing message:`, error);
      return false;
    }
  }

  isConnected(): boolean {
    // Simplified check: connection is active AND user object exists
    // WebSocket readyState check removed as it can be flaky
    return this.connectionActive && this.socket?.user !== undefined;
  }

  // Check if connection is actually healthy (can communicate with WhatsApp)
  isConnectionHealthy(): boolean {
    // Simply use isConnected() - checking internal ws property is unreliable
    // as Baileys library structure may change
    return this.isConnected();
  }

  getUser() {
    return this.socket?.user;
  }

  // Get profile picture URL for a jid (or self)
  async getProfilePicUrl(jid?: string): Promise<string | null> {
    if (!this.socket || !this.connectionActive) {
      return null;
    }
    try {
      const targetJid = jid || this.socket.user?.id;
      if (!targetJid) return null;
      const url = await this.socket.profilePictureUrl(targetJid, "image");
      return url || null;
    } catch (error) {
      // User might not have a profile picture
      return null;
    }
  }

  // Get "About" status for a jid (or self)
  async getAboutStatus(jid?: string): Promise<string | null> {
    if (!this.socket || !this.connectionActive) {
      return null;
    }
    try {
      const targetJid = jid || this.socket.user?.id;
      if (!targetJid) return null;
      const result = await this.socket.fetchStatus(targetJid);
      // fetchStatus may return an array or object depending on Baileys version
      if (Array.isArray(result) && result.length > 0) {
        // Newer Baileys returns array of {status, setAt}
        const first = result[0] as any;
        return typeof first?.status === "string" ? first.status : null;
      }
      // Older Baileys returns object with {status, setAt}
      const statusObj = result as any;
      return typeof statusObj?.status === "string" ? statusObj.status : null;
    } catch (error) {
      console.error("Error fetching about status:", error);
      return null;
    }
  }

  // Get full profile details
  async getFullProfile(): Promise<{
    phoneNumber: string | null;
    displayName: string | null;
    about: string | null;
    profilePicUrl: string | null;
  }> {
    const user = this.getUser();
    const phoneNumber = user?.id?.split(":")[0] || null;
    let displayName: string | null = user?.name || null;
    
    // Try multiple methods to get the display name
    if (!displayName && this.socket && this.connectionActive && user?.id) {
      // Method 1: Try reading from saved creds.json file
      try {
        const credsPath = path.join(this.sessionPath, "creds.json");
        const credsData = await fs.readFile(credsPath, "utf-8");
        const creds = JSON.parse(credsData);
        if (creds?.me?.name) {
          displayName = creds.me.name;
          console.log(`[${this.userId}] Name from creds.json:`, displayName);
        }
      } catch (e) {
        console.log(`[${this.userId}] Could not read creds.json`);
      }
      
      // Method 2: Try fetching business profile
      if (!displayName) {
        try {
          const businessProfile = await this.socket.getBusinessProfile(user.id) as any;
          if (businessProfile?.wid?.user) {
            displayName = businessProfile.description || businessProfile.wid.user;
          }
          console.log(`[${this.userId}] Business profile:`, businessProfile);
        } catch (e) {
          console.log(`[${this.userId}] No business profile available`);
        }
      }
    }
    
    // Get about and profile pic
    const [about, profilePicUrl] = await Promise.all([
      this.getAboutStatus(),
      this.getProfilePicUrl(),
    ]);

    console.log(`[${this.userId}] Full profile result:`, { phoneNumber, displayName, about: about ? "yes" : "no", profilePicUrl: profilePicUrl ? "yes" : "no" });

    return {
      phoneNumber,
      displayName,
      about,
      profilePicUrl,
    };
  }

  // Health check: Verify connection is actually working by querying WhatsApp
  async healthCheck(): Promise<boolean> {
    if (!this.isConnected()) {
      console.log(`[${this.userId}] Health check failed: Not connected`);
      return false;
    }

    try {
      // Try a simple query to verify connection is working
      // Fetch own profile as a ping - should be fast if connection is healthy
      const userJid = this.socket?.user?.id;
      if (!userJid) {
        console.log(`[${this.userId}] Health check failed: No user ID`);
        return false;
      }

      // Use a short timeout for health check
      const timeoutPromise = new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("Health check timeout")), 5000)
      );

      await Promise.race([
        this.socket!.fetchStatus(userJid),
        timeoutPromise
      ]);

      console.log(`[${this.userId}] Health check passed`);
      return true;
    } catch (error) {
      console.log(`[${this.userId}] Health check failed:`, error instanceof Error ? error.message : error);
      return false;
    }
  }

  // Check if a phone number is registered on WhatsApp
  // Returns { isOnWhatsApp: boolean, jid: string } or null if not connected
  async checkNumber(phoneNumber: string): Promise<{ isOnWhatsApp: boolean; jid: string } | null> {
    if (!this.isConnected()) {
      console.log(`[${this.userId}] Cannot check number: Not connected`);
      return null;
    }

    try {
      // Clean the phone number - keep only digits
      let cleanNumber = phoneNumber.replace(/[^\d]/g, "");

      // Handle Malaysian numbers: if starts with 0, replace with 60
      if (cleanNumber.startsWith("0") && !cleanNumber.startsWith("00")) {
        cleanNumber = "60" + cleanNumber.substring(1);
      }

      // Remove leading 00 if present
      if (cleanNumber.startsWith("00")) {
        cleanNumber = cleanNumber.substring(2);
      }

      console.log(`[${this.userId}] Checking if number is on WhatsApp: ${cleanNumber} (original: ${phoneNumber})`);

      // Convert to JID format
      const jid = `${cleanNumber}@s.whatsapp.net`;

      // Use onWhatsApp method from Baileys
      // Result comes back as an array: [{ jid: '...', exists: true }]
      const result = await Promise.race([
        (this.socket as any).onWhatsApp(jid),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error("Check number timeout")), 15000)
        )
      ]);

      console.log(`[${this.userId}] Number check result:`, result);

      // Baileys returns an array, extract first element and map 'exists' to 'isOnWhatsApp'
      if (Array.isArray(result) && result.length > 0) {
        return {
          isOnWhatsApp: result[0].exists || false,
          jid: result[0].jid
        };
      }

      // Fallback for unexpected format
      if (result && typeof result === 'object') {
        return {
          isOnWhatsApp: (result as any).exists || (result as any).isOnWhatsApp || false,
          jid: (result as any).jid
        };
      }

      return null;
    } catch (error) {
      console.log(`[${this.userId}] Check number failed:`, error instanceof Error ? error.message : error);
      return null;
    }
  }

  // Warm up device cache for recipients to prevent timeout on first send
  // This pre-fetches device info using USyncQuery with generous timeout
  async warmUpDeviceCache(recipientJids: string[]): Promise<void> {
    if (!this.socket || !this.isConnected()) {
      console.log(`[${this.userId}] Cannot warm up cache: not connected`);
      return;
    }

    if (recipientJids.length === 0) {
      console.log(`[${this.userId}] No recipients to warm up cache for`);
      return;
    }

    console.log(`[${this.userId}] Warming up device cache for ${recipientJids.length} recipients...`);

    try {
      const meId = this.socket.user?.id;
      if (!meId) {
        console.log(`[${this.userId}] Cannot warm up cache: no user ID`);
        return;
      }

      // Process in batches of 20 to avoid overwhelming the connection
      const batchSize = 20;
      const batches: string[][] = [];

      for (let i = 0; i < recipientJids.length; i += batchSize) {
        batches.push(recipientJids.slice(i, i + batchSize));
      }

      let successCount = 0;
      let failureCount = 0;

      for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
        const batch = batches[batchIndex];
        console.log(`[${this.userId}] Warming up batch ${batchIndex + 1}/${batches.length} (${batch.length} recipients)...`);

        for (const jid of batch) {
          try {
            // Use getUSyncDevices with generous timeout to pre-fetch device info
            // This is the same call that sendMessage makes internally
            const warmupPromise = (this.socket as any).getUSyncDevices([meId, jid], "query");
            if (warmupPromise) {
              await Promise.race([
                warmupPromise,
                new Promise((_, reject) =>
                  setTimeout(() => reject(new Error("Warmup timeout")), 30000)
                )
              ]);
              successCount++;
              // Small delay between each to avoid rate limiting
              await new Promise(resolve => setTimeout(resolve, 100));
            }
          } catch (e) {
            // Individual failures are OK - cache will be built on send
            failureCount++;
            console.log(`[${this.userId}] Warmup failed for ${jid}:`, e instanceof Error ? e.message : e);
          }
        }

        // Small delay between batches
        if (batchIndex < batches.length - 1) {
          await new Promise(resolve => setTimeout(resolve, 500));
        }
      }

      console.log(`[${this.userId}] Device cache warmup complete: ${successCount} succeeded, ${failureCount} failed`);
    } catch (error) {
      console.log(`[${this.userId}] Device cache warmup error:`, error instanceof Error ? error.message : error);
      // Don't throw - warmup failure shouldn't prevent blast from starting
    }
  }
}

// Store active WhatsApp instances per user
const instances = new Map<string, WhatsAppService>();

export function getWhatsAppInstance(userId: string, dataPath?: string): WhatsAppService {
  if (!instances.has(userId)) {
    const instance = new WhatsAppService(userId, dataPath);
    instances.set(userId, instance);
  }
  return instances.get(userId)!;
}

export async function getFreshWhatsAppInstance(userId: string, dataPath?: string): Promise<WhatsAppService> {
  // Remove old instance if exists - AWAIT for clean disconnect!
  const oldInstance = instances.get(userId);
  if (oldInstance) {
    await oldInstance.disconnect();
    instances.delete(userId);
  }
  // Create and return new instance
  const instance = new WhatsAppService(userId, dataPath);
  instances.set(userId, instance);
  return instance;
}

export async function removeWhatsAppInstance(userId: string): Promise<void> {
  const instance = instances.get(userId);
  if (instance) {
    await instance.disconnect(); // AWAIT disconnect to complete!
    instances.delete(userId);
  }
}

export function hasActiveInstance(userId: string): boolean {
  const instance = instances.get(userId);
  return instance?.isConnected() || false;
}

export { instances as whatsappInstances };
