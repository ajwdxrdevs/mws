const path = require("path");
const dotenv = require("dotenv");

dotenv.config({ path: path.resolve(__dirname, "../../.env") });

const dbProvider = (process.env.DB_PROVIDER || "postgresql").toLowerCase() === "mysql" ? "mysql" : "postgresql";
const databaseUrl =
  process.env.DATABASE_URL ||
  (dbProvider === "mysql"
    ? "mysql://root:password@localhost:3306/whatsapp_blast"
    : "postgresql://localhost:5432/whatsapp_blast");

module.exports = {
  dialect: dbProvider,
  schema: dbProvider === "mysql" ? "./dist/schema/mysql.js" : "./dist/schema/postgres.js",
  out: "./drizzle",
  dbCredentials: {
    url: databaseUrl,
  },
  verbose: true,
  strict: false,
};
