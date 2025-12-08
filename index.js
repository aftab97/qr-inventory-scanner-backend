require('dotenv').config();
const express = require("express");
const multer = require("multer");
const { parse } = require("csv-parse/sync");
const XLSX = require("xlsx"); // kept; export endpoints refactored to exceljs below
const cors = require("cors");
const { v4: uuidv4 } = require("uuid");

const {
  DynamoDBClient,
  ScanCommand,
  BatchWriteItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  PutItemCommand,
} = require("@aws-sdk/client-dynamodb");
const { marshall, unmarshall } = require("@aws-sdk/util-dynamodb");

const { S3Client, PutObjectCommand, GetObjectCommand } = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const ExcelJS = require("exceljs");

const app = express();
const port = 8080;
const upload = multer({ storage: multer.memoryStorage() });

// Env/config
const DYNAMO_TABLE = process.env.DYNAMO_TABLE || "Jewelry_Boutique";
const DYNAMO_PK = process.env.DYNAMO_PK || "ID";
const AWS_REGION = process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || "us-east-1";
const DYNAMO_SCHEMA_PREFIX = process.env.DYNAMO_SCHEMA_PREFIX || "__meta__schema";

const S3_BUCKET = process.env.S3_BUCKET || "your-images-bucket";
const S3_PUBLIC_BASE = process.env.S3_PUBLIC_BASE || `https://${S3_BUCKET}.s3.${AWS_REGION}.amazonaws.com`;
const MAX_IMAGE_SIZE_BYTES = Number(process.env.MAX_IMAGE_SIZE_BYTES || 10 * 1024 * 1024);
const ALLOWED_IMAGE_TYPES = (process.env.ALLOWED_IMAGE_TYPES || "image/jpeg,image/png,image/webp").split(",");

// Logging
app.use((req, res, next) => {
  console.log(new Date().toISOString(), req.method, req.originalUrl);
  next();
});

app.use(cors());
app.use("/parse", express.text({ type: ["text/csv", "text/plain"], limit: "50mb" }));
app.use(express.json());

app.get("/", async (req, res) => res.send("works"));

function createDynamoClient() {
  return new DynamoDBClient({
    region: AWS_REGION,
    credentials: { accessKeyId: process.env.accessKeyId, secretAccessKey: process.env.secretAccessKey },
  });
}
function createS3Client() {
  return new S3Client({
    region: AWS_REGION,
    credentials: { accessKeyId: process.env.accessKeyId, secretAccessKey: process.env.secretAccessKey },
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function batchWriteWithRetries(dynamo, requestItems, maxRetries = 5) {
  let unprocessed = requestItems;
  let attempt = 0;
  while (Object.keys(unprocessed).length > 0 && attempt <= maxRetries) {
    try {
      const cmd = new BatchWriteItemCommand({ RequestItems: unprocessed });
      const resp = await dynamo.send(cmd);
      unprocessed = resp.UnprocessedItems || {};
      if (Object.keys(unprocessed).length === 0) return resp;
    } catch (err) { console.error("BatchWriteItemCommand error on attempt", attempt, err); }
    attempt += 1;
    await sleep(Math.pow(2, attempt) * 100 + Math.random() * 100);
  }
  if (Object.keys(unprocessed).length > 0) throw new Error("BatchWrite did not complete after retries; some items remain unprocessed.");
  return { UnprocessedItems: {} };
}

async function scanAll(dynamo, params = {}) {
  const collected = [];
  let ExclusiveStartKey = undefined;
  do {
    const cmdParams = { TableName: DYNAMO_TABLE, ...params, ExclusiveStartKey };
    const cmd = new ScanCommand(cmdParams);
    const resp = await dynamo.send(cmd);
    const items = resp.Items || [];
    for (const it of items) collected.push(unmarshall(it));
    ExclusiveStartKey = resp.LastEvaluatedKey;
  } while (ExclusiveStartKey);
  return collected;
}

async function collectPrimaryKeys(dynamo, category = null) {
  if (!dynamo) throw new Error("Dynamo client required");
  if (category == null) {
    const rows = await scanAll(dynamo, { ProjectionExpression: DYNAMO_PK });
    return rows.map((r) => r[DYNAMO_PK]).filter(Boolean);
  }
  const exprValues = marshall({ ":cat": category });
  const rows = await scanAll(dynamo, {
    FilterExpression: "#c = :cat",
    ExpressionAttributeNames: { "#c": "category" },
    ExpressionAttributeValues: exprValues,
    ProjectionExpression: `${DYNAMO_PK}, category`,
  });
  return rows.map((r) => r[DYNAMO_PK]).filter(Boolean);
}

async function deleteAllItems(dynamo, category = null) {
  const deletedKeys = await collectPrimaryKeys(dynamo, category);
  let deletedCount = 0;
  for (let i = 0; i < deletedKeys.length; i += 25) {
    const chunk = deletedKeys.slice(i, i + 25);
    const requestItems = {};
    requestItems[DYNAMO_TABLE] = chunk.map((keyVal) => ({ DeleteRequest: { Key: marshall({ [DYNAMO_PK]: keyVal }) } }));
    await batchWriteWithRetries(dynamo, requestItems);
    deletedCount += chunk.length;
  }
  return { deletedCount, deletedKeys };
}

async function putItems(dynamo, items) {
  if (!dynamo) throw new Error("Dynamo client required");
  let writtenCount = 0;
  for (let i = 0; i < items.length; i += 25) {
    const chunk = items.slice(i, i + 25);
    const keysInChunk = new Set();
    for (const it of chunk) {
      const keyVal = it[DYNAMO_PK];
      if (keysInChunk.has(keyVal)) throw new Error(`Duplicate key "${keyVal}" detected within a batch chunk.`);
      keysInChunk.add(keyVal);
    }
    const requestItems = {};
    requestItems[DYNAMO_TABLE] = chunk.map((item) => ({ PutRequest: { Item: marshall(item, { removeUndefinedValues: true }) } }));
    await batchWriteWithRetries(dynamo, requestItems);
    writtenCount += chunk.length;
  }
  return { writtenCount };
}

function normalizeHeader(h) { return String(h || "").trim().toLowerCase(); }
function sanitizeHeaderList(headers) {
  return headers.map((h) => String(h || "").trim()).filter((h) => {
    if (h === "") return false;
    const lower = h.toLowerCase();
    if (lower === "__empty" || /^__empty\d*$/i.test(lower)) return false;
    if (/^unnamed/i.test(h)) return false;
    return true;
  });
}

// ... [Your buildItemsForDataset and /parse endpoint code remains unchanged] ...

// List categories
app.get("/categories", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const raw = await scanAll(dynamo, {});
    const schemaItems = raw.filter((it) => {
      const pkVal = it[DYNAMO_PK];
      return (typeof pkVal === "string" && pkVal.startsWith(`${DYNAMO_SCHEMA_PREFIX}#`));
    });
    const categories = schemaItems.map((s) => ({
      name: s.category,
      headerOriginalOrder: s.headerOriginalOrder,
      headerNormalizedOrder: s.headerNormalizedOrder,
      updatedAt: s.updatedAt,
    }));
    return res.json({ categories });
  } catch (err) { res.status(500).json({ error: "Failed to list categories", details: String(err) }); }
});

function shouldExcludeColumn(normName) {
  const n = String(normName || "").trim().toLowerCase();
  return (
    n === "id" ||
    n === "total" || n === "quantité totale" || n === "quantité total" || n === "quantite totale" ||
    n === "quantite total" || n === "fotos" || n === "pictures" || n === "photos"
  );
}

app.get("/columns", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const all = await scanAll(dynamo, {});
    const schemaItems = all.filter((it) => {
      const pkVal = it[DYNAMO_PK];
      return (typeof pkVal === "string" && pkVal.startsWith(`${DYNAMO_SCHEMA_PREFIX}#`));
    });

    const map = new Map();
    for (const sch of schemaItems) {
      const headerOrig = Array.isArray(sch.headerOriginalOrder) ? sch.headerOriginalOrder : [];
      const headerNorm = Array.isArray(sch.headerNormalizedOrder) ? sch.headerNormalizedOrder : headerOrig.map((h) => String(h || "").toLowerCase());
      const candidates = headerOrig.length ? headerOrig : headerNorm;
      for (const h of candidates) {
        if (!h) continue;
        const orig = String(h).trim();
        const norm = orig.toLowerCase();
        if (!norm) continue;
        if (shouldExcludeColumn(norm)) continue;
        if (!map.has(norm)) map.set(norm, orig);
      }
    }

    const columns = Array.from(map.values()).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));
    return res.json({ columns });
  } catch (err) { return res.status(500).json({ error: "Failed to aggregate columns", details: String(err) }); }
});

// Get items for a category (excludes schema)
app.get("/category/:name", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const category = req.params.name;
    if (!category) return res.status(400).json({ error: "Missing category param" });

    const items = [];
    let ExclusiveStartKey = undefined;
    do {
      const params = {
        TableName: DYNAMO_TABLE,
        FilterExpression: "#c = :cat",
        ExpressionAttributeNames: { "#c": "category" },
        ExpressionAttributeValues: marshall({ ":cat": category }),
        ExclusiveStartKey,
      };
      const cmd = new ScanCommand(params);
      const resp = await dynamo.send(cmd);
      const raw = resp.Items || [];
      for (const it of raw) {
        const obj = unmarshall(it);
        const pkVal = obj[DYNAMO_PK];
        if (typeof pkVal === "string" && pkVal === `${DYNAMO_SCHEMA_PREFIX}#${category}`) continue;
        items.push(obj);
      }
      ExclusiveStartKey = resp.LastEvaluatedKey;
    } while (ExclusiveStartKey);

    const all = await scanAll(dynamo, {});
    const schema = all.find((s) => s[DYNAMO_PK] === `${DYNAMO_SCHEMA_PREFIX}#${category}`) || null;

    return res.json({ category, schema, items });
  } catch (err) { res.status(500).json({ error: "Failed to get category items", details: String(err) }); }
});

// Helpers
async function getItemById(dynamo, id) {
  const all = await scanAll(dynamo, {});
  for (const it of all) {
    const pkVal = it[DYNAMO_PK];
    if (typeof pkVal === "string" && pkVal.startsWith(`${DYNAMO_SCHEMA_PREFIX}#`)) continue;
    if (pkVal === id) return it;
  }
  return null;
}
async function getSchemaForCategory(dynamo, category) {
  const all = await scanAll(dynamo, {});
  return all.find((s) => s[DYNAMO_PK] === `${DYNAMO_SCHEMA_PREFIX}#${category}`) || null;
}
function norm(str) { return String(str || "").trim().toLowerCase(); }

// Item exists check
app.get("/item/exists/:id", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const id = req.params.id;
    if (!id) return res.status(400).json({ error: "Missing id in path" });
    const item = await getItemById(dynamo, id);
    return res.json({ exists: !!item });
  } catch (err) { return res.status(500).json({ error: "Failed to check ID existence", details: String(err) }); }
});

// PATCH item attribute — enforce column exists in category schema
app.patch("/item/:id", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const id = req.params.id;
    if (!id) return res.status(400).json({ error: "Missing item id in path" });

    const { attribute, value, category: bodyCategory } = req.body || {};
    if (!attribute) return res.status(400).json({ error: "Missing attribute in body" });

    const item = await getItemById(dynamo, id);
    if (!item) return res.status(404).json({ error: "Item not found" });

    const effectiveCategory = bodyCategory || item.category;
    if (!effectiveCategory) return res.status(400).json({ error: "Item category unknown; cannot validate attribute" });

    const schemaItem = await getSchemaForCategory(dynamo, effectiveCategory);
    if (!schemaItem) return res.status(400).json({ error: `Schema not found for category "${effectiveCategory}"` });

    const headerNorm = Array.isArray(schemaItem.headerNormalizedOrder)
      ? schemaItem.headerNormalizedOrder.map(norm)
      : Array.isArray(schemaItem.headerOriginalOrder)
      ? schemaItem.headerOriginalOrder.map(norm)
      : [];

    const attrNorm = norm(attribute);
    if (!headerNorm.includes(attrNorm)) {
      return res.status(400).json({
        error: "Attribute not allowed for this category",
        details: `Column "${attribute}" does not exist in category "${effectiveCategory}".`,
        code: "COLUMN_NOT_IN_SCHEMA",
      });
    }

    const Key = marshall({ [DYNAMO_PK]: id });
    const ExpressionAttributeNames = { "#attr": attribute, "#cat": "category" };
    const marshalledVals = marshall({ ":val": value, ":cat": effectiveCategory });
    const ExpressionAttributeValues = { ":val": marshalledVals[":val"], ":cat": marshalledVals[":cat"] };

    const params = {
      TableName: DYNAMO_TABLE,
      Key,
      UpdateExpression: "SET #attr = :val",
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      ConditionExpression: "#cat = :cat",
      ReturnValues: "ALL_NEW",
    };

    const cmd = new UpdateItemCommand(params);
    const resp = await dynamo.send(cmd);
    const updated = resp.Attributes ? unmarshall(resp.Attributes) : null;
    return res.json({ success: true, updated });
  } catch (err) {
    if (err && err.name === "ConditionalCheckFailedException") {
      return res.status(409).json({ error: "Item category mismatch or condition failed", code: "CATEGORY_CONDITION_FAILED", details: String(err) });
    }
    return res.status(500).json({ error: "Failed to update item", details: String(err) });
  }
});

// DELETE single item
app.delete("/item/:id", async (req, res) => {
  try {
    const id = req.params.id;
    if (!id) return res.status(400).json({ error: "Missing item id" });
    const dynamo = createDynamoClient();
    const Key = marshall({ [DYNAMO_PK]: id });
    const params = { TableName: DYNAMO_TABLE, Key };
    const cmd = new DeleteItemCommand(params);
    await dynamo.send(cmd);
    return res.json({ deleted: id });
  } catch (err) { return res.status(500).json({ error: "Failed to delete item", details: String(err) }); }
});



// Create item (generic route)
app.post("/item", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const { category, values = {}, id } = req.body || {};
    if (!category) return res.status(400).json({ error: "Missing category in body" });

    const all = await scanAll(dynamo, {});
    const schemaPk = `${DYNAMO_SCHEMA_PREFIX}#${category}`;
    const schemaItem = all.find((s) => s[DYNAMO_PK] === schemaPk);
    if (!schemaItem) return res.status(404).json({ error: `Schema not found for category "${category}"` });

    const headerNorm = Array.isArray(schemaItem.headerNormalizedOrder) ? schemaItem.headerNormalizedOrder.map(norm) : [];
    const newId = id && String(id).trim() ? String(id).trim() : uuidv4();

    const existing = await getItemById(dynamo, newId);
    if (existing) return res.status(409).json({ error: "ID already exists", id: newId, code: "ID_CONFLICT" });

    const item = { [DYNAMO_PK]: newId, category };
    for (const k of Object.keys(values || {})) {
      const nk = norm(k);
      if (nk === norm(DYNAMO_PK) || nk === "category") continue;
      if (headerNorm.includes(nk)) item[nk] = values[k];
    }
    for (const nk of headerNorm) {
      if (nk === norm(DYNAMO_PK) || nk === "category") continue;
      if (item[nk] === undefined) item[nk] = null;
    }

    await new PutItemCommand({ TableName: DYNAMO_TABLE, Item: marshall(item, { removeUndefinedValues: true }) });
    const cmd = new PutItemCommand({ TableName: DYNAMO_TABLE, Item: marshall(item, { removeUndefinedValues: true }) });
    const dynamoRes = await createDynamoClient().send(cmd);

    return res.json({ message: "Item created", item });
  } catch (err) { return res.status(500).json({ error: "Failed to create item", details: String(err) }); }
});

// NEW: category-scoped row creation with uniqueness check
app.post("/category/:name/row", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const category = req.params.name;
    if (!category) return res.status(400).json({ error: "Missing category param" });

    const { insertIndex, id, values = {} } = req.body || {};
    const newId = id && String(id).trim() ? String(id).trim() : uuidv4();

    const existing = await getItemById(dynamo, newId);
    if (existing) return res.status(409).json({ error: "ID already exists", id: newId, code: "ID_CONFLICT" });

    const schemaItem = await getSchemaForCategory(dynamo, category);
    if (!schemaItem) return res.status(404).json({ error: `Schema not found for category "${category}"` });

    const headerNorm = Array.isArray(schemaItem.headerNormalizedOrder) ? schemaItem.headerNormalizedOrder.map(norm) : [];
    const item = { [DYNAMO_PK]: newId, category };
    for (const k of Object.keys(values || {})) {
      const nk = norm(k);
      if (nk === norm(DYNAMO_PK) || nk === "category") continue;
      if (headerNorm.includes(nk)) item[nk] = values[k];
    }
    for (const nk of headerNorm) {
      if (nk === norm(DYNAMO_PK) || nk === "category") continue;
      if (item[nk] === undefined) item[nk] = null;
    }

    const cmd = new PutItemCommand({ TableName: DYNAMO_TABLE, Item: marshall(item, { removeUndefinedValues: true }) });
    await dynamo.send(cmd);

    return res.json({ message: "Row created", item, insertIndex });
  } catch (err) { return res.status(500).json({ error: "Failed to create row", details: String(err) }); }
});

// Helper already added (as per your snippet)
function safeSegment(input) {
  return String(input || "")
    .trim()
    .toLowerCase()
    .replace(/[\/\\]/g, "-")
    .replace(/\s+/g, "-")
    .replace(/[^a-z0-9._-]/g, "-");
}

// Presign PUT: no ACL
app.post("/uploads/presign", async (req, res) => {
  try {
    const s3 = createS3Client();
    const { filename, contentType, category, itemId, column } = req.body || {};
    if (!filename || !contentType) return res.status(400).json({ error: "Missing filename or contentType" });
    if (!ALLOWED_IMAGE_TYPES.includes(contentType)) return res.status(400).json({ error: `Unsupported content type: ${contentType}` });

    const ext = filename.includes(".") ? filename.split(".").pop() : "bin";
    const key = [
      "images",
      safeSegment(category || "unknown"),
      safeSegment(itemId || "unknown"),
      safeSegment(column || "image"),
      `${uuidv4()}.${ext}`
    ].join("/");

    const putCommand = new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: key,
      ContentType: contentType,
      // NO ACL here; we keep bucket private
    });

    const uploadUrl = await getSignedUrl(s3, putCommand, { expiresIn: 300 }); // 5 min
    // finalUrl is not publicly readable; store s3Key and use presigned GET to render
    const finalUrl = `${S3_PUBLIC_BASE}/${encodeURI(key)}`;
    return res.json({ uploadUrl, finalUrl, s3Key: key, expires: 300 });
  } catch (err) {
    console.error("POST /uploads/presign error", err);
    return res.status(500).json({ error: "Failed to presign upload", details: String(err) });
  }
});

// Presign GET for rendering thumbnails (short-lived)
app.get("/uploads/presign-get", async (req, res) => {
  try {
    const s3 = createS3Client();
    const s3Key = req.query.s3Key;
    if (!s3Key) return res.status(400).json({ error: "Missing s3Key" });

    // const getUrl = await getSignedUrl(
    //   s3,
    //   new PutObjectCommand({ Bucket: S3_BUCKET, Key: s3Key }), // wrong command, use GetObjectCommand
    //   { expiresIn: 300 }
    // );
    // Correction:
    const getUrl = await getSignedUrl(s3, new GetObjectCommand({ Bucket: S3_BUCKET, Key: s3Key }), { expiresIn: 300 });

    return res.json({ url: getUrl, expires: 300 });
  } catch (err) {
    console.error("GET /uploads/presign-get error", err);
    return res.status(500).json({ error: "Failed to presign GET", details: String(err) });
  }
});

// Refactored export: embed images via exceljs for category
app.get("/export/category/:name", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const category = req.params.name;
    if (!category) return res.status(400).json({ error: "Missing category param" });

    const all = await scanAll(dynamo, {});
    const schemaItem = all.find((s) => s[DYNAMO_PK] === `${DYNAMO_SCHEMA_PREFIX}#${category}`);

    // Fetch items
    const items = [];
    let ExclusiveStartKey;
    do {
      const params = {
        TableName: DYNAMO_TABLE,
        FilterExpression: "#c = :cat",
        ExpressionAttributeNames: { "#c": "category" },
        ExpressionAttributeValues: marshall({ ":cat": category }),
        ExclusiveStartKey,
      };
      const cmd = new ScanCommand(params);
      const resp = await dynamo.send(cmd);
      const raw = resp.Items || [];
      for (const it of raw) {
        const obj = unmarshall(it);
        if (obj[DYNAMO_PK] === `${DYNAMO_SCHEMA_PREFIX}#${category}`) continue;
        items.push(obj);
      }
      ExclusiveStartKey = resp.LastEvaluatedKey;
    } while (ExclusiveStartKey);

    const headerOrig = schemaItem?.headerOriginalOrder || [];
    const headerNorm = schemaItem?.headerNormalizedOrder || headerOrig.map((h) => String(h).toLowerCase());
    const columns = [DYNAMO_PK, ...headerOrig];

    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet((category || "category").substring(0, 31));

    // Header row
    sheet.addRow(columns);

    // Helper to fetch image buffer from URL; for brevity, we only embed for http(s) URLs
    async function fetchImageBuffer(url) {
      if (!/^https?:\/\//i.test(url)) return null;
      try {
        const r = await fetch(url);
        if (!r.ok) return null;
        const arr = await r.arrayBuffer();
        return Buffer.from(arr);
      } catch { return null; }
    }

    // Add rows and embed images
    for (const it of items) {
      const rowData = {};
      rowData[DYNAMO_PK] = it[DYNAMO_PK];
      for (let i = 0; i < headerNorm.length; i++) {
        const n = headerNorm[i];
        const orig = headerOrig[i] || n;
        const val = it[n];
        rowData[orig] = val === undefined ? "" : val;
      }
      const row = sheet.addRow(columns.map((c) => rowData[c] === undefined ? "" : rowData[c]));

      // Embed images for any column containing JSON type=image
      for (let i = 0; i < headerNorm.length; i++) {
        const n = headerNorm[i];
        const orig = headerOrig[i] || n;
        const cellVal = it[n];
        let imgObj = null;
        if (typeof cellVal === "string") {
          try {
            const parsed = JSON.parse(cellVal);
            if (parsed && parsed.type === "image" && parsed.src) imgObj = parsed;
          } catch {}
        }
        if (!imgObj) continue;

        const buf = await fetchImageBuffer(imgObj.src);
        if (!buf) continue;
        const ext = (imgObj.src || "").toLowerCase().includes(".png") ? "png" : "jpeg";
        const imageId = workbook.addImage({ buffer: buf, extension: ext });
        // Position image inside the cell (row.number, col index + 1 because ID col offset)
        const colIdx = i + 2; // Because first column is the PK
        sheet.addImage(imageId, {
          tl: { col: colIdx - 1, row: row.number - 1 },
          br: { col: colIdx, row: row.number },
          editAs: "oneCell",
        });
        // Optionally keep URL as text in the same cell for fallback
        row.getCell(colIdx).value = imgObj.src;
      }
    }

    const buffer = await workbook.xlsx.writeBuffer();
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="${category}.xlsx"`);
    res.send(Buffer.from(buffer));
  } catch (err) {
    console.error("/export/category error", err);
    res.status(500).json({ error: "Failed to export category as XLSX", details: String(err) });
  }
});

// Export all sheets with images
app.get("/export/all", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const all = await scanAll(dynamo, {});
    const schemaItems = all.filter((it) => {
      const pk = it[DYNAMO_PK];
      return (typeof pk === "string" && pk.startsWith(`${DYNAMO_SCHEMA_PREFIX}#`));
    });
    if (!schemaItems.length) return res.status(400).json({ error: "No categories/schema found to export" });

    const workbook = new ExcelJS.Workbook();

    async function fetchImageBuffer(url) {
      if (!/^https?:\/\//i.test(url)) return null;
      try {
        const r = await fetch(url);
        if (!r.ok) return null;
        const arr = await r.arrayBuffer();
        return Buffer.from(arr);
      } catch { return null; }
    }

    for (const sch of schemaItems) {
      const category = sch.category;
      const items = [];
      let ExclusiveStartKey = undefined;
      do {
        const params = {
          TableName: DYNAMO_TABLE,
          FilterExpression: "#c = :cat",
          ExpressionAttributeNames: { "#c": "category" },
          ExpressionAttributeValues: marshall({ ":cat": category }),
          ExclusiveStartKey,
        };
        const cmd = new ScanCommand(params);
        const resp = await dynamo.send(cmd);
        const raw = resp.Items || [];
        for (const it of raw) {
          const obj = unmarshall(it);
          if (obj[DYNAMO_PK] === `${DYNAMO_SCHEMA_PREFIX}#${category}`) continue;
          items.push(obj);
        }
        ExclusiveStartKey = resp.LastEvaluatedKey;
      } while (ExclusiveStartKey);

      const headerOrig = sch.headerOriginalOrder && sch.headerOriginalOrder.length ? sch.headerOriginalOrder : [];
      const headerNorm = sch.headerNormalizedOrder && sch.headerNormalizedOrder.length ? sch.headerNormalizedOrder : headerOrig.map((h) => String(h).toLowerCase());
      const columns = [DYNAMO_PK, ...headerOrig];
      const sheet = workbook.addWorksheet((category || "category").substring(0, 31));
      sheet.addRow(columns);

      for (const it of items) {
        const rowData = {};
        rowData[DYNAMO_PK] = it[DYNAMO_PK];
        for (let i = 0; i < headerNorm.length; i++) {
          const n = headerNorm[i];
          const orig = headerOrig[i] || n;
          const val = it[n];
          rowData[orig] = val === undefined ? "" : val;
        }
        const row = sheet.addRow(columns.map((c) => rowData[c] === undefined ? "" : rowData[c]));

        for (let i = 0; i < headerNorm.length; i++) {
          const n = headerNorm[i];
          const cellVal = it[n];
          let imgObj = null;
          if (typeof cellVal === "string") {
            try {
              const parsed = JSON.parse(cellVal);
              if (parsed && parsed.type === "image" && parsed.src) imgObj = parsed;
            } catch {}
          }
          if (!imgObj) continue;
          const buf = await fetchImageBuffer(imgObj.src);
          if (!buf) continue;
          const ext = (imgObj.src || "").toLowerCase().includes(".png") ? "png" : "jpeg";
          const imageId = workbook.addImage({ buffer: buf, extension: ext });
          const colIdx = i + 2;
          sheet.addImage(imageId, {
            tl: { col: colIdx - 1, row: row.number - 1 },
            br: { col: colIdx, row: row.number },
            editAs: "oneCell",
          });
          row.getCell(colIdx).value = imgObj.src;
        }
      }
    }

    const buffer = await workbook.xlsx.writeBuffer();
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="all_categories.xlsx"`);
    res.send(Buffer.from(buffer));
  } catch (err) {
    console.error("Export all error", err);
    res.status(500).json({ error: "Failed to export all categories", details: String(err) });
  }
});

// DELETE category
app.delete("/category/:name", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const category = req.params.name;
    if (!category)
      return res.status(400).json({ error: "Missing category param" });
    const result = await deleteAllItems(dynamo, category);
    return res.json({
      message: "Category deleted",
      category,
      deletedCount: result.deletedCount,
      deletedKeys: result.deletedKeys,
    });
  } catch (err) {
    console.error("DELETE /category/:name error", err);
    res
      .status(500)
      .json({ error: "Failed to delete category", details: String(err) });
  }
});

// DELETE column (param)
app.delete("/category/:name/column/:columnName", async (req, res) => {
  console.log("DELETE column (param) called:", req.params);
  const dynamo = createDynamoClient();
  try {
    const category = req.params.name;
    const rawColumnName = req.params.columnName;

    const rows = await scanAll(dynamo, {
      FilterExpression: "#c = :cat",
      ExpressionAttributeNames: { "#c": "category" },
      ExpressionAttributeValues: marshall({ ":cat": category }),
      ProjectionExpression: `${DYNAMO_PK}`,
    });

    let removedCount = 0;
    for (const r of rows) {
      const pkVal = r[DYNAMO_PK];
      if (typeof pkVal === "string" && pkVal.startsWith(`${DYNAMO_SCHEMA_PREFIX}#`)) continue;
      try {
        const Key = marshall({ [DYNAMO_PK]: pkVal });
        const params = {
          TableName: DYNAMO_TABLE,
          Key,
          UpdateExpression: `REMOVE #attr`,
          ExpressionAttributeNames: { "#attr": rawColumnName },
          ReturnValues: "ALL_OLD",
        };
        const cmd = new UpdateItemCommand(params);
        await dynamo.send(cmd);
        removedCount++;
      } catch (err) {
        console.error("Failed to remove attribute for item:", pkVal, rawColumnName, err);
      }
    }

    const all = await scanAll(dynamo, {});
    const schemaPk = `${DYNAMO_SCHEMA_PREFIX}#${category}`;
    const schemaItem = all.find((s) => s[DYNAMO_PK] === schemaPk);
    let updatedSchema = false;
    if (schemaItem) {
      const hdrOrig = Array.isArray(schemaItem.headerOriginalOrder) ? schemaItem.headerOriginalOrder : [];
      const hdrNorm = Array.isArray(schemaItem.headerNormalizedOrder) ? schemaItem.headerNormalizedOrder : [];
      const filteredOrig = [];
      const filteredNorm = [];
      for (let i = 0; i < hdrOrig.length; i++) {
        const o = hdrOrig[i];
        const n = hdrNorm[i] || (typeof o === "string" ? o.toLowerCase() : "");
        if (String(o).trim() === String(rawColumnName).trim()) {
        } else if (String(n).trim() === String(rawColumnName).trim().toLowerCase()) {
        } else {
          filteredOrig.push(o);
          filteredNorm.push(n);
        }
      }
      if (filteredOrig.length !== hdrOrig.length || filteredNorm.length !== hdrNorm.length) {
        const Key = marshall({ [DYNAMO_PK]: schemaPk });
        const exprVals = marshall({ ":h": filteredOrig, ":n": filteredNorm, ":u": new Date().toISOString() });
        const params = {
          TableName: DYNAMO_TABLE,
          Key,
          UpdateExpression: "SET headerOriginalOrder = :h, headerNormalizedOrder = :n, updatedAt = :u",
          ExpressionAttributeValues: exprVals,
          ReturnValues: "ALL_NEW",
        };
        try {
          const cmd = new UpdateItemCommand(params);
          await dynamo.send(cmd);
          updatedSchema = true;
        } catch (err) {
          console.error("Failed to update schema item:", schemaPk, err);
        }
      }
    }

    return res.json({ message: "Column deletion completed", removedCount, updatedSchema });
  } catch (err) {
    console.error("DELETE /category/:name/column/:columnName error", err);
    return res
      .status(500)
      .json({ error: "Failed to delete column", details: String(err) });
  }
});

// DELETE column (body fallback)
app.delete("/category/:name/column", async (req, res) => {
  console.log("DELETE column (body) called:", req.params, req.body);
  const dynamo = createDynamoClient();
  try {
    const category = req.params.name;
    const rawColumnName = (req.body && (req.body.column || req.body.columnName)) || null;
   
    const rows = await scanAll(dynamo, {
      FilterExpression: "#c = :cat",
      ExpressionAttributeNames: { "#c": "category" },
      ExpressionAttributeValues: marshall({ ":cat": category }),
      ProjectionExpression: `${DYNAMO_PK}`,
    });

    let removedCount = 0;
    for (const r of rows) {
      const pkVal = r[DYNAMO_PK];
      if (typeof pkVal === "string" && pkVal.startsWith(`${DYNAMO_SCHEMA_PREFIX}#`)) continue;
      try {
        const Key = marshall({ [DYNAMO_PK]: pkVal });
        const params = {
          TableName: DYNAMO_TABLE,
          Key,
          UpdateExpression: `REMOVE #attr`,
          ExpressionAttributeNames: { "#attr": rawColumnName },
          ReturnValues: "ALL_OLD",
        };
        const cmd = new UpdateItemCommand(params);
        await dynamo.send(cmd);
        removedCount++;
      } catch (err) {
        console.error("Failed to remove attribute for item:", pkVal, rawColumnName, err);
      }
    }

    const all = await scanAll(dynamo, {});
    const schemaPk = `${DYNAMO_SCHEMA_PREFIX}#${category}`;
    const schemaItem = all.find((s) => s[DYNAMO_PK] === schemaPk);
    let updatedSchema = false;
    if (schemaItem) {
      const hdrOrig = Array.isArray(schemaItem.headerOriginalOrder) ? schemaItem.headerOriginalOrder : [];
      const hdrNorm = Array.isArray(schemaItem.headerNormalizedOrder) ? schemaItem.headerNormalizedOrder : [];
      const filteredOrig = [];
      const filteredNorm = [];
      for (let i = 0; i < hdrOrig.length; i++) {
        const o = hdrOrig[i];
        const n = hdrNorm[i] || (typeof o === "string" ? o.toLowerCase() : "");
        if (String(o).trim() === String(rawColumnName).trim()) {
        } else if (String(n).trim() === String(rawColumnName).trim().toLowerCase()) {
        } else {
          filteredOrig.push(o);
          filteredNorm.push(n);
        }
      }
      if (filteredOrig.length !== hdrOrig.length || filteredNorm.length !== hdrNorm.length) {
        const Key = marshall({ [DYNAMO_PK]: schemaPk });
        const exprVals = marshall({ ":h": filteredOrig, ":n": filteredNorm, ":u": new Date().toISOString() });
        const params = {
          TableName: DYNAMO_TABLE,
          Key,
          UpdateExpression: "SET headerOriginalOrder = :h, headerNormalizedOrder = :n, updatedAt = :u",
          ExpressionAttributeValues: exprVals,
          ReturnValues: "ALL_NEW",
        };
        try {
          const cmd = new UpdateItemCommand(params);
          await dynamo.send(cmd);
          updatedSchema = true;
        } catch (err) {
          console.error("Failed to update schema item:", schemaPk, err);
        }
      }
    }

    return res.json({ message: "Column deletion completed", removedCount, updatedSchema });
  } catch (err) {
    console.error("DELETE /category/:name/column (body) error", err);
    return res
      .status(500)
      .json({ error: "Failed to delete column", details: String(err) });
  }
});

function withTimeout(promise, ms, label) {
  const ac = new AbortController();
  const timeout = setTimeout(() => {
    ac.abort();
    console.error(`[${label}] timed out after ${ms}ms, aborting`);
  }, ms);
  return promise
    .finally(() => clearTimeout(timeout))
    .catch((err) => {
      // If aborted, AWS SDK will throw AbortError
      throw err;
    });
}

app.post("/category/:name/column", cors(), async (req, res) => {
  const startedAt = new Date().toISOString();
  console.log(`[insertColumn] start ${startedAt} category=${req.params.name} body=`, req.body);
  try {
    const dynamo = createDynamoClient();
    const category = req.params.name;
    if (!category) return res.status(400).json({ error: "Missing category param" });

    const { columnName, insertIndex, defaultValue = null } = req.body || {};
    const cleanedName = String(columnName || "").trim();
    if (!cleanedName) return res.status(400).json({ error: "Column name is required" });
    const normName = normalizeHeader(cleanedName);

    const isProtected = (n) => {
      n = String(n || "").trim().toLowerCase();
      return false
    };
    if (isProtected(cleanedName) || isProtected(normName)) {
      return res.status(400).json({ error: `Column "${cleanedName}" is protected and cannot be added` });
    }

    console.log(`[insertColumn] load schema for ${category}`);
    const schemaItem = await getSchemaForCategory(dynamo, category);
    if (!schemaItem) return res.status(404).json({ error: `Schema not found for category "${category}"` });

    const origHeaders = Array.isArray(schemaItem.headerOriginalOrder) ? schemaItem.headerOriginalOrder.slice() : [];
    const normHeaders = Array.isArray(schemaItem.headerNormalizedOrder)
      ? schemaItem.headerNormalizedOrder.slice()
      : origHeaders.map((h) => normalizeHeader(h));

    const exists =
      origHeaders.some((h) => String(h).trim().toLowerCase() === normName) ||
      normHeaders.includes(normName);
    if (exists) {
      return res.status(409).json({ error: `Column "${cleanedName}" already exists in schema` });
    }

    const count = origHeaders.length;
    let idx = Number.isFinite(insertIndex) ? Number(insertIndex) : count;
    if (idx < 0) idx = 0;
    if (idx > count) idx = count;

    origHeaders.splice(idx, 0, cleanedName);
    normHeaders.splice(idx, 0, normName);

    const schemaPk = `${DYNAMO_SCHEMA_PREFIX}#${category}`;
    const Key = marshall({ [DYNAMO_PK]: schemaPk });
    const ExpressionAttributeNames = { "#hoo": "headerOriginalOrder", "#hno": "headerNormalizedOrder", "#ua": "updatedAt" };
    const ExpressionAttributeValues = marshall({
      ":hoo": origHeaders,
      ":hno": normHeaders,
      ":ua": new Date().toISOString(),
    });

    const updateSchemaCmd = new UpdateItemCommand({
      TableName: DYNAMO_TABLE,
      Key,
      UpdateExpression: "SET #hoo = :hoo, #hno = :hno, #ua = :ua",
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      ReturnValues: "ALL_NEW",
    });

    console.log(`[insertColumn] updating schema…`);
    // Add a short timeout to surface credentials/network issues quickly during debugging (e.g., 10s)
    const updateResp = await withTimeout(dynamo.send(updateSchemaCmd), 10000, "updateSchema");
    const updatedSchema = updateResp.Attributes
      ? unmarshall(updateResp.Attributes)
      : { [DYNAMO_PK]: schemaPk, category, headerOriginalOrder: origHeaders, headerNormalizedOrder: normHeaders, updatedAt: new Date().toISOString() };

    console.log(`[insertColumn] schema updated, responding to client`);
    // Respond immediately so the request does not stay "Pending"
    res.json({
      success: true,
      message: `Column "${cleanedName}" inserted at index ${idx}`,
      schema: updatedSchema,
    });

    // Initialize items in background (limited concurrency)
    (async () => {
      try {
        console.log(`[insertColumn/bg] scanning item IDs for ${category}`);
        const ids = [];
        let ExclusiveStartKey;
        do {
          const params = {
            TableName: DYNAMO_TABLE,
            FilterExpression: "#c = :cat",
            ExpressionAttributeNames: { "#c": "category" },
            ExpressionAttributeValues: marshall({ ":cat": category }),
            ExclusiveStartKey,
            ProjectionExpression: `${DYNAMO_PK}, category`,
          };
          const scanCmd = new ScanCommand(params);
          const resp = await dynamo.send(scanCmd);
          const raw = resp.Items || [];
          for (const it of raw) {
            const obj = unmarshall(it);
            if (obj[DYNAMO_PK] !== schemaPk) ids.push(obj[DYNAMO_PK]);
          }
          ExclusiveStartKey = resp.LastEvaluatedKey;
        } while (ExclusiveStartKey);

        console.log(`[insertColumn/bg] updating ${ids.length} items to add "${normName}" if missing`);
        const CONCURRENCY = 20;
        for (let i = 0; i < ids.length; i += CONCURRENCY) {
          const chunk = ids.slice(i, i + CONCURRENCY);
          await Promise.allSettled(
            chunk.map(async (id) => {
              try {
                const itemObj = await getItemById(dynamo, id);
                if (itemObj && Object.prototype.hasOwnProperty.call(itemObj, normName)) return;
                const updKey = marshall({ [DYNAMO_PK]: id });
                const names = { "#attr": normName, "#cat": "category" };
                const marshVals = marshall({ ":val": defaultValue, ":cat": category });
                const vals = { ":val": marshVals[":val"], ":cat": marshVals[":cat"] };
                const upd = new UpdateItemCommand({
                  TableName: DYNAMO_TABLE,
                  Key: updKey,
                  UpdateExpression: "SET #attr = :val",
                  ExpressionAttributeNames: names,
                  ExpressionAttributeValues: vals,
                  ConditionExpression: "#cat = :cat",
                });
                await dynamo.send(upd);
              } catch (err) {
                console.warn(`[insertColumn/bg] init failed for ${id}: ${String(err)}`);
              }
            })
          );
        }
        console.log(`[insertColumn/bg] done initializing column "${normName}"`);
      } catch (bgErr) {
        console.error(`[insertColumn/bg] error:`, bgErr);
      }
    })();
  } catch (err) {
    console.error("POST /category/:name/column error", err);
    return res.status(500).json({ error: "Failed to insert column", details: String(err) });
  }
});
// Create item (existing route, unchanged logic; PutItemCommand is now imported)
app.post("/item", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const { category, values = {}, id } = req.body || {};
    if (!category)
      return res.status(400).json({ error: "Missing category in body" });

    const all = await scanAll(dynamo, {});
    const schemaPk = `${DYNAMO_SCHEMA_PREFIX}#${category}`;
    const schemaItem = all.find((s) => s[DYNAMO_PK] === schemaPk);
    if (!schemaItem)
      return res
        .status(404)
        .json({ error: `Schema not found for category "${category}"` });

    const headerNorm = Array.isArray(schemaItem.headerNormalizedOrder)
      ? schemaItem.headerNormalizedOrder.map(norm)
      : [];
    const newId = id && String(id).trim() ? String(id).trim() : uuidv4();

    const item = { [DYNAMO_PK]: newId, category };
    for (const k of Object.keys(values || {})) {
      const nk = norm(k);
      if (nk === norm(DYNAMO_PK) || nk === "category") continue;
      if (headerNorm.includes(nk)) item[nk] = values[k];
    }
    for (const nk of headerNorm) {
      if (nk === norm(DYNAMO_PK) || nk === "category") continue;
      if (item[nk] === undefined) item[nk] = null;
    }

    const cmd = new PutItemCommand({
      TableName: DYNAMO_TABLE,
      Item: marshall(item, { removeUndefinedValues: true }),
    });
    await dynamo.send(cmd);

    return res.json({ message: "Item created", item });
  } catch (err) {
    console.error("POST /item error", err);
    return res
      .status(500)
      .json({ error: "Failed to create item", details: String(err) });
  }
});

// NEW: category-scoped row creation with uniqueness check; used by Insert Row modal
app.post("/category/:name/row", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const category = req.params.name;
    if (!category) return res.status(400).json({ error: "Missing category param" });

    const { insertIndex, id, values = {} } = req.body || {};
    const newId = id && String(id).trim() ? String(id).trim() : uuidv4();

    const existing = await getItemById(dynamo, newId);
    if (existing) {
      return res.status(409).json({ error: "ID already exists", id: newId, code: "ID_CONFLICT" });
    }

    const schemaItem = await getSchemaForCategory(dynamo, category);
    if (!schemaItem) {
      return res.status(404).json({ error: `Schema not found for category "${category}"` });
    }

    const headerNorm = Array.isArray(schemaItem.headerNormalizedOrder)
      ? schemaItem.headerNormalizedOrder.map(norm)
      : [];

    const item = { [DYNAMO_PK]: newId, category };
    for (const k of Object.keys(values || {})) {
      const nk = norm(k);
      if (nk === norm(DYNAMO_PK) || nk === "category") continue;
      if (headerNorm.includes(nk)) item[nk] = values[k];
    }
    for (const nk of headerNorm) {
      if (nk === norm(DYNAMO_PK) || nk === "category") continue;
      if (item[nk] === undefined) item[nk] = null;
    }

    await dynamo.send(new PutItemCommand({
      TableName: DYNAMO_TABLE,
      Item: marshall(item, { removeUndefinedValues: true }),
    }));

    return res.json({ message: "Row created", item, insertIndex });
  } catch (err) {
    console.error("POST /category/:name/row error", err);
    return res.status(500).json({ error: "Failed to create row", details: String(err) });
  }
});

app.listen(port, () => {
  console.log(`CSV/XLSX app listening at http://localhost:${port}`);
  console.log("ENV:", { DYNAMO_TABLE, DYNAMO_PK, AWS_REGION, DYNAMO_SCHEMA_PREFIX, S3_BUCKET });
});

process.on("unhandledRejection", (reason) => { console.error("Unhandled Rejection:", reason); });
process.on("uncaughtException", (err) => { console.error("Uncaught Exception:", err); process.exit(1); });