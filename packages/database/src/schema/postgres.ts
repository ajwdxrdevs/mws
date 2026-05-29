import { pgTable, text, timestamp, boolean, integer, json, jsonb, primaryKey } from "drizzle-orm/pg-core";
import { createSchema } from "./factory.js";

const createdSchema = createSchema({
  table: pgTable,
  text,
  timestamp,
  boolean,
  integer,
  json: jsonb ?? json,
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
