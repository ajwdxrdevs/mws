import { mysqlTable, text, timestamp, boolean, int, json, primaryKey } from "drizzle-orm/mysql-core";
import { createSchema } from "./factory.js";

const mysqlTimestamp = (name: string) => timestamp(name, { mode: "date" });

const createdSchema = createSchema({
  table: mysqlTable,
  text,
  timestamp: mysqlTimestamp,
  boolean,
  integer: int,
  json,
  primaryKey,
});

export const schema = createdSchema;
export const {
  users,
  sessions,
  accounts,
  verifications,
  whatsappSessions,
  tags,
  contacts,
  contactTags,
  conversations,
  messages,
  quickReplies,
  campaigns,
  campaignRecipients,
  aiModels,
  botConfig,
  knowledgeBase,
  botFiles,
  botCommands,
  contactsRelations,
  tagsRelations,
  contactTagsRelations,
  conversationsRelations,
  messagesRelations,
  campaignsRelations,
  campaignRecipientsRelations,
  botCommandsRelations,
} = createdSchema;
