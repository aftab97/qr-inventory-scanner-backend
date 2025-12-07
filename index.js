require('dotenv').config();
const express = require("express");
const multer = require("multer");
const { parse } = require("csv-parse/sync");
const XLSX = require("xlsx");
const cors = require("cors");
const { v4: uuidv4 } = require("uuid");

const {
  DynamoDBClient,
  ScanCommand,
  BatchWriteItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
} = require("@aws-sdk/client-dynamodb");
const { marshall, unmarshall } = require("@aws-sdk/util-dynamodb");

const app = express();
const port = 8080;
const upload = multer({ storage: multer.memoryStorage() });

// Configuration via environment variables:
const DYNAMO_TABLE = process.env.DYNAMO_TABLE || "Jewelry_Boutique";
const DYNAMO_PK = process.env.DYNAMO_PK || "ID";
const AWS_REGION =
  process.env.AWS_REGION || process.env.AWS_DEFAULT_REGION || "us-east-1";
const DYNAMO_SCHEMA_PREFIX =
  process.env.DYNAMO_SCHEMA_PREFIX || "__meta__schema";

// Basic logging to help debug routing
app.use((req, res, next) => {
  console.log(new Date().toISOString(), req.method, req.originalUrl);
  next();
});

app.use(cors());
app.use(
  "/parse",
  express.text({ type: ["text/csv", "text/plain"], limit: "50mb" })
);
app.use(express.json());

app.get("/", async (req, res) => {
  return res.send("works");
});

function createDynamoClient() {
  return new DynamoDBClient({
    region: "us-east-1",
    credentials: {
      accessKeyId: process.env.accessKeyId,
      secretAccessKey: process.env.secretAccessKey,
    },
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
    } catch (err) {
      console.error("BatchWriteItemCommand error on attempt", attempt, err);
    }
    attempt += 1;
    await sleep(Math.pow(2, attempt) * 100 + Math.random() * 100);
  }
  if (Object.keys(unprocessed).length > 0) {
    throw new Error(
      "BatchWrite did not complete after retries; some items remain unprocessed."
    );
  }
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
    requestItems[DYNAMO_TABLE] = chunk.map((keyVal) => ({
      DeleteRequest: { Key: marshall({ [DYNAMO_PK]: keyVal }) },
    }));
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
      if (keysInChunk.has(keyVal))
        throw new Error(
          `Duplicate key "${keyVal}" detected within a batch chunk.`
        );
      keysInChunk.add(keyVal);
    }
    const requestItems = {};
    requestItems[DYNAMO_TABLE] = chunk.map((item) => ({
      PutRequest: { Item: marshall(item, { removeUndefinedValues: true }) },
    }));
    await batchWriteWithRetries(dynamo, requestItems);
    writtenCount += chunk.length;
  }
  return { writtenCount };
}

function normalizeHeader(h) {
  return String(h || "")
    .trim()
    .toLowerCase();
}
function sanitizeHeaderList(headers) {
  return headers
    .map((h) => String(h || "").trim())
    .filter((h) => {
      if (h === "") return false;
      const lower = h.toLowerCase();
      if (lower === "__empty" || /^__empty\d*$/i.test(lower)) return false;
      if (/^unnamed/i.test(h)) return false;
      return true;
    });
}

function buildItemsForDataset(rows, headerOriginalOrder, category) {
  const sanitizedHeaders = sanitizeHeaderList(headerOriginalOrder);
  const headerNormalizedOrder = sanitizedHeaders.map(normalizeHeader);

  const pkNormalized = String(DYNAMO_PK || "id")
    .trim()
    .toLowerCase();
  const pkIndex = headerNormalizedOrder.findIndex((h) => h === pkNormalized);

  const totalOrig = "Total";
  const totalNorm = "total";
  const hasTotalAlready = headerNormalizedOrder.includes(totalNorm);
  if (!hasTotalAlready) {
    headerNormalizedOrder.push(totalNorm);
    sanitizedHeaders.push(totalOrig);
  }

  const excludedForSum = new Set([
    pkNormalized,
    "nombre",
    "fotos",
    "pierres",
    "piedras",
    "pictures",
    "photos",
  ]);
  const sourceHeaders = sanitizedHeaders.filter(
    (h) => String(h).trim().toLowerCase() !== totalNorm
  );
  const normalized = rows.map((row) => {
    const newRow = {};
    sourceHeaders.forEach((origKey) => {
      const k = normalizeHeader(origKey);
      let v = row[origKey];
      if (v === undefined) v = null;
      if (typeof v === "string") v = v.trim();
      newRow[k] = v;
    });
    return newRow;
  });

  const filtered = normalized.filter((r) => {
    const nomVal =
      r.nom !== undefined && r.nom !== null
        ? String(r.nom).trim().toLowerCase()
        : "";
    const nombreVal =
      r.nombre !== undefined && r.nombre !== null
        ? String(r.nombre).trim().toLowerCase()
        : "";
    if (nomVal === "total" || nombreVal === "total") return false;
    return true;
  });

  const missing = [];
  const items = [];
  const seenIds = new Set();
  const warnings = [];

  for (let idx = 0; idx < filtered.length; idx++) {
    const r = filtered[idx];

    const pierresRaw =
      r.pierres !== undefined &&
      r.pierres !== null &&
      String(r.pierres).trim() !== ""
        ? r.pierres
        : r.piedras;
    const pierresStr = String(pierresRaw ?? "").trim();
    if (pierresStr === "") {
      missing.push({ index: idx, row: r });
      continue;
    }

    let idVal = null;
    if (pkIndex !== -1) {
      const pkKey = headerNormalizedOrder[pkIndex];
      const candidate = r[pkKey];
      if (
        candidate !== undefined &&
        candidate !== null &&
        String(candidate).trim() !== ""
      )
        idVal = String(candidate).trim();
    }
    if (!idVal) idVal = uuidv4();

    if (seenIds.has(idVal))
      throw new Error(
        `Duplicate ID "${idVal}" detected in import for category "${category}" (row index ${idx}).`
      );
    seenIds.add(idVal);

    let sum = 0;
    for (const hdr of Object.keys(r)) {
      const nh = String(hdr).trim().toLowerCase();
      if (excludedForSum.has(nh)) continue;
      if (nh === totalNorm) continue;
      const raw = r[hdr];
      if (raw === null || raw === undefined || raw === "") continue;
      const n = Number(String(raw).replace(",", "."));
      if (Number.isFinite(n)) sum += n;
      else
        warnings.push({
          row: idx,
          column: hdr,
          value: raw,
          message: "Non-numeric value treated as 0 for Total",
        });
    }

    const item = { [DYNAMO_PK]: idVal, category };
    for (const hdr of headerNormalizedOrder) {
      if (hdr === pkNormalized || hdr === "category") continue;
      item[hdr] = r[hdr] === undefined ? null : r[hdr];
    }
    item[totalNorm] = Number(Number.isFinite(sum) ? sum : 0);

    items.push(item);
  }

  const schemaItem = {
    [DYNAMO_PK]: `${DYNAMO_SCHEMA_PREFIX}#${category}`,
    category,
    headerOriginalOrder: sanitizedHeaders,
    headerNormalizedOrder,
    updatedAt: new Date().toISOString(),
  };

  return {
    items,
    missing,
    schemaItem,
    headerOriginalOrder: sanitizedHeaders,
    headerNormalizedOrder,
    warnings,
  };
}

function parseCategoriesField(req) {
  if (req.body && typeof req.body.categories === "string") {
    try {
      const parsed = JSON.parse(req.body.categories);
      if (parsed && typeof parsed === "object") return parsed;
    } catch (e) {}
  }
  return {};
}

// Main parse endpoint (preview/apply)
app.post("/parse", upload.single("file"), async (req, res) => {
  try {
    let buffer = null;
    let text = null;
    let parseType = null;
    const categoriesMapping = parseCategoriesField(req);
    const providedCategory =
      req.query.category || (req.body && req.body.category) || null;
    const replaceMode =
      req.query.replace === "true" ||
      req.query.replace === "1" ||
      req.query.replace === "yes";
    const updateMode =
      req.query.update === "true" ||
      req.query.update === "1" ||
      req.query.update === "yes";
    const dryRun =
      req.query.dry === "true" ||
      req.query.dry === "1" ||
      req.query.dry === "yes";

    if (req.file && req.file.buffer) buffer = req.file.buffer;
    else if (
      req.body &&
      typeof req.body === "string" &&
      req.body.trim().length > 0
    )
      text = req.body;
    else
      return res.status(400).json({ error: "No CSV or spreadsheet provided." });

    const datasets = [];

    if (buffer) {
      const isZip =
        buffer.length >= 4 &&
        buffer[0] === 0x50 &&
        buffer[1] === 0x4b &&
        buffer[2] === 0x03 &&
        buffer[3] === 0x04;
      if (isZip) {
        const workbook = XLSX.read(buffer, { type: "buffer" });
        const sheetNames = workbook.SheetNames;
        if (!sheetNames || sheetNames.length === 0)
          return res.status(400).json({ error: "XLSX contains no sheets" });
        for (const sheetName of sheetNames) {
          const sheet = workbook.Sheets[sheetName];
          const parsedRecords = XLSX.utils.sheet_to_json(sheet, {
            defval: null,
          });
          if (!Array.isArray(parsedRecords) || parsedRecords.length === 0)
            continue;
          const rawHeaders = Object.keys(parsedRecords[0]).map((h) =>
            String(h || "").trim()
          );
          const headerOriginalOrder = sanitizeHeaderList(rawHeaders);
          const cleanedRecords = parsedRecords.map((r) => {
            const cleanRow = {};
            headerOriginalOrder.forEach((h) => {
              cleanRow[h] = r[h] === undefined ? null : r[h];
            });
            return cleanRow;
          });
          const category =
            categoriesMapping[sheetName] || providedCategory || sheetName;
          datasets.push({
            sheetName,
            category,
            parsedRecords: cleanedRecords,
            headerOriginalOrder,
          });
        }
        parseType = "xlsx";
      } else {
        text = buffer.toString("utf8");
      }
    }

    if (!datasets.length && text != null) {
      const records = parse(text, {
        columns: true,
        skip_empty_lines: true,
        trim: false,
      });
      if (!Array.isArray(records) || records.length === 0)
        return res.status(400).json({ error: "No rows found in provided CSV" });
      if (!providedCategory && !(req.body && req.body.categories))
        return res.status(400).json({ error: "CSV upload requires category." });
      const rawHeaders = Object.keys(records[0]).map((h) =>
        String(h || "").trim()
      );
      const headerOriginalOrder = sanitizeHeaderList(rawHeaders);
      const cleanedRecords = records.map((r) => {
        const cleanRow = {};
        headerOriginalOrder.forEach((h) => {
          cleanRow[h] = r[h] === undefined ? null : r[h];
        });
        return cleanRow;
      });
      const category =
        providedCategory ||
        (req.body && req.body.categories
          ? JSON.parse(req.body.categories)["dataset"]
          : null);
      datasets.push({
        sheetName: "dataset",
        category,
        parsedRecords: cleanedRecords,
        headerOriginalOrder,
      });
      parseType = "csv";
    }

    if (datasets.length === 0)
      return res
        .status(400)
        .json({ error: "No datasets found in the uploaded file" });

    const allWork = [];
    for (const ds of datasets) {
      try {
        const built = buildItemsForDataset(
          ds.parsedRecords,
          ds.headerOriginalOrder,
          ds.category
        );
        allWork.push({
          sheetName: ds.sheetName,
          category: ds.category,
          ...built,
        });
      } catch (err) {
        return res
          .status(400)
          .json({
            error: `Failed to build items for sheet ${ds.sheetName}: ${err.message}`,
          });
      }
    }

    const bad = allWork.find((w) => w.missing && w.missing.length > 0);
    if (bad) {
      return res
        .status(400)
        .json({
          error:
            "Some rows are missing Pierres/Piedras in category: " +
            bad.category,
          examples: bad.missing.slice(0, 5),
        });
    }

    if (!replaceMode && !updateMode) {
      return res.json({
        type: parseType,
        datasets: allWork.map((w) => ({
          sheetName: w.sheetName,
          category: w.category,
          headerOriginalOrder: w.headerOriginalOrder,
          headerNormalizedOrder: w.headerNormalizedOrder,
          rows: Math.max(0, w.items.length),
          warnings: w.warnings || [],
          defaultSelectedColumn: "Total",
        })),
        note: "Preview returned. To apply, POST again with ?update=true or ?replace=true and optionally categories mapping.",
      });
    }

    const dynamo = createDynamoClient();

    if (dryRun) {
      const previews = [];
      for (const work of allWork) {
        if (replaceMode) {
          const existingKeys = await collectPrimaryKeys(dynamo, work.category);
          const deleteCount = existingKeys.length;
          previews.push({
            sheetName: work.sheetName,
            category: work.category,
            deleteCount,
            deletePreview: existingKeys.slice(0, 200),
            putCount: work.items.length,
            putPreview: work.items.slice(0, 200),
            schemaItemPreview: work.schemaItem,
            warnings: work.warnings || [],
            defaultSelectedColumn: "Total",
          });
        } else {
          previews.push({
            sheetName: work.sheetName,
            category: work.category,
            putCount: work.items.length,
            putPreview: work.items.slice(0, 200),
            schemaItemPreview: work.schemaItem,
            warnings: work.warnings || [],
            defaultSelectedColumn: "Total",
          });
        }
      }
      return res.json({
        mode: replaceMode ? "replace" : "update",
        dry: true,
        previews,
      });
    }

    const results = [];
    for (const work of allWork) {
      const schemaItem = work.schemaItem;
      if (replaceMode) {
        let deleteResult;
        try {
          deleteResult = await deleteAllItems(dynamo, work.category);
        } catch (err) {
          console.error(
            "Error deleting items for category",
            work.category,
            err
          );
          return res
            .status(500)
            .json({
              error: `Failed to delete items for category ${work.category}`,
              details: String(err),
            });
        }
        const itemsToWrite = [...work.items, schemaItem];
        let putResult;
        try {
          putResult = await putItems(dynamo, itemsToWrite);
        } catch (err) {
          console.error("Error writing items for category", work.category, err);
          return res
            .status(500)
            .json({
              error: `Failed to write items for category ${work.category}`,
              details: String(err),
            });
        }
        results.push({
          sheetName: work.sheetName,
          category: work.category,
          mode: "replace",
          deletedCount: deleteResult.deletedCount,
          writtenCount: putResult.writtenCount,
          warnings: work.warnings || [],
          defaultSelectedColumn: "Total",
        });
      } else {
        const itemsToWrite = [...work.items, schemaItem];
        let putResult;
        try {
          putResult = await putItems(dynamo, itemsToWrite);
        } catch (err) {
          console.error(
            "Error upserting items for category",
            work.category,
            err
          );
          return res
            .status(500)
            .json({
              error: `Failed to upsert items for category ${work.category}`,
              details: String(err),
            });
        }
        results.push({
          sheetName: work.sheetName,
          category: work.category,
          mode: "update",
          writtenCount: putResult.writtenCount,
          warnings: work.warnings || [],
          defaultSelectedColumn: "Total",
        });
      }
    }

    return res.json({ message: "Operation completed", results });
  } catch (err) {
    console.error("Unexpected error:", err);
    res
      .status(500)
      .json({ error: "Internal server error", details: String(err) });
  }
});

// List categories
app.get("/categories", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const raw = await scanAll(dynamo, {});
    const schemaItems = raw.filter((it) => {
      const pkVal = it[DYNAMO_PK];
      return (
        typeof pkVal === "string" &&
        pkVal.startsWith(`${DYNAMO_SCHEMA_PREFIX}#`)
      );
    });
    const categories = schemaItems.map((s) => ({
      name: s.category,
      headerOriginalOrder: s.headerOriginalOrder,
      headerNormalizedOrder: s.headerNormalizedOrder,
      updatedAt: s.updatedAt,
    }));
    return res.json({ categories });
  } catch (err) {
    console.error("GET /categories error", err);
    res
      .status(500)
      .json({ error: "Failed to list categories", details: String(err) });
  }
});

function shouldExcludeColumn(normName) {
  const n = String(normName || "")
    .trim()
    .toLowerCase();
  return (
    n === "id" ||
    n === "nom" ||
    n === "nombre" ||
    n === "pierres" ||
    n === "piedras" ||
    n === "total" ||
    n === "quantité totale" ||
    n === "quantité total" ||
    n === "quantite totale" ||
    n === "quantite total" ||
    n === "fotos" ||
    n === "pictures" ||
    n === "photos"
  );
}

/**
 * GET /columns
 * Aggregates column names across all categories (sheets).
 */
app.get("/columns", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const all = await scanAll(dynamo, {});
    const schemaItems = all.filter((it) => {
      const pkVal = it[DYNAMO_PK];
      return (
        typeof pkVal === "string" &&
        pkVal.startsWith(`${DYNAMO_SCHEMA_PREFIX}#`)
      );
    });

    const map = new Map(); // norm -> sample original
    for (const sch of schemaItems) {
      const headerOrig = Array.isArray(sch.headerOriginalOrder)
        ? sch.headerOriginalOrder
        : [];
      const headerNorm = Array.isArray(sch.headerNormalizedOrder)
        ? sch.headerNormalizedOrder
        : headerOrig.map((h) => String(h || "").toLowerCase());

      const candidates = headerOrig.length ? headerOrig : headerNorm;
      for (const h of candidates) {
        if (!h) continue;
        const orig = String(h).trim();
        const norm = orig.toLowerCase();
        if (!norm) continue;
        if (shouldExcludeColumn(norm)) continue;
        if (!map.has(norm)) {
          map.set(norm, orig);
        }
      }
    }

    const columns = Array.from(map.values()).sort((a, b) =>
      a.localeCompare(b, undefined, { sensitivity: "base" })
    );

    return res.json({ columns });
  } catch (err) {
    console.error("GET /columns error", err);
    return res
      .status(500)
      .json({ error: "Failed to aggregate columns", details: String(err) });
  }
});

// Get items for a category (excludes schema)
app.get("/category/:name", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const category = req.params.name;
    if (!category)
      return res.status(400).json({ error: "Missing category param" });

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
        if (
          typeof pkVal === "string" &&
          pkVal === `${DYNAMO_SCHEMA_PREFIX}#${category}`
        )
          continue;
        items.push(obj);
      }
      ExclusiveStartKey = resp.LastEvaluatedKey;
    } while (ExclusiveStartKey);

    const all = await scanAll(dynamo, {});
    const schema =
      all.find((s) => s[DYNAMO_PK] === `${DYNAMO_SCHEMA_PREFIX}#${category}`) ||
      null;

    return res.json({ category, schema, items });
  } catch (err) {
    console.error("GET /category/:name error", err);
    res
      .status(500)
      .json({ error: "Failed to get category items", details: String(err) });
  }
});

// Helpers for schema-aware PATCH
async function getItemById(dynamo, id) {
  const all = await scanAll(dynamo, {});
  for (const it of all) {
    const pkVal = it[DYNAMO_PK];
    if (
      typeof pkVal === "string" &&
      pkVal.startsWith(`${DYNAMO_SCHEMA_PREFIX}#`)
    )
      continue;
    if (pkVal === id) return it;
  }
  return null;
}
async function getSchemaForCategory(dynamo, category) {
  const all = await scanAll(dynamo, {});
  return (
    all.find((s) => s[DYNAMO_PK] === `${DYNAMO_SCHEMA_PREFIX}#${category}`) ||
    null
  );
}
function norm(str) {
  return String(str || "")
    .trim()
    .toLowerCase();
}

// PATCH item attribute — enforce column exists in category schema
app.patch("/item/:id", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const id = req.params.id;
    if (!id) return res.status(400).json({ error: "Missing item id in path" });

    const { attribute, value, category: bodyCategory } = req.body || {};
    if (!attribute)
      return res.status(400).json({ error: "Missing attribute in body" });

    // 1) Load item
    const item = await getItemById(dynamo, id);
    if (!item) return res.status(404).json({ error: "Item not found" });

    const effectiveCategory = bodyCategory || item.category;
    if (!effectiveCategory) {
      return res
        .status(400)
        .json({ error: "Item category unknown; cannot validate attribute" });
    }

    // 2) Load schema for category
    const schemaItem = await getSchemaForCategory(dynamo, effectiveCategory);
    if (!schemaItem) {
      return res
        .status(400)
        .json({
          error: `Schema not found for category "${effectiveCategory}"`,
        });
    }

    const headerNorm = Array.isArray(schemaItem.headerNormalizedOrder)
      ? schemaItem.headerNormalizedOrder.map(norm)
      : Array.isArray(schemaItem.headerOriginalOrder)
      ? schemaItem.headerOriginalOrder.map(norm)
      : [];

    const attrNorm = norm(attribute);

    // 3) Validate attribute exists in schema
    if (!headerNorm.includes(attrNorm)) {
      return res.status(400).json({
        error: "Attribute not allowed for this category",
        details: `Column "${attribute}" does not exist in category "${effectiveCategory}".`,
        code: "COLUMN_NOT_IN_SCHEMA",
      });
    }

    // 4) Update with category condition
    const Key = marshall({ [DYNAMO_PK]: id });
    const ExpressionAttributeNames = { "#attr": attribute, "#cat": "category" };
    const marshalledVals = marshall({
      ":val": value,
      ":cat": effectiveCategory,
    });
    const ExpressionAttributeValues = {
      ":val": marshalledVals[":val"],
      ":cat": marshalledVals[":cat"],
    };

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
    console.error("PATCH /item/:id error", err);
    if (err && err.name === "ConditionalCheckFailedException") {
      return res.status(409).json({
        error: "Item category mismatch or condition failed",
        code: "CATEGORY_CONDITION_FAILED",
        details: String(err),
      });
    }
    return res
      .status(500)
      .json({ error: "Failed to update item", details: String(err) });
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
  } catch (err) {
    console.error("DELETE /item/:id error", err);
    return res
      .status(500)
      .json({ error: "Failed to delete item", details: String(err) });
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
    if (!category || !rawColumnName)
      return res.status(400).json({ error: "Missing category or column name" });
    if (
      ["nom", "nombre", "pierres", "piedras"].includes(
        String(rawColumnName).trim().toLowerCase()
      )
    )
      return res.status(403).json({ error: "Protected column" });

    const rows = await scanAll(dynamo, {
      FilterExpression: "#c = :cat",
      ExpressionAttributeNames: { "#c": "category" },
      ExpressionAttributeValues: marshall({ ":cat": category }),
      ProjectionExpression: `${DYNAMO_PK}`,
    });

    let removedCount = 0;
    for (const r of rows) {
      const pkVal = r[DYNAMO_PK];
      if (
        typeof pkVal === "string" &&
        pkVal.startsWith(`${DYNAMO_SCHEMA_PREFIX}#`)
      )
        continue;
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
        console.error(
          "Failed to remove attribute for item:",
          pkVal,
          rawColumnName,
          err
        );
      }
    }

    // update schema
    const all = await scanAll(dynamo, {});
    const schemaPk = `${DYNAMO_SCHEMA_PREFIX}#${category}`;
    const schemaItem = all.find((s) => s[DYNAMO_PK] === schemaPk);
    let updatedSchema = false;
    if (schemaItem) {
      const hdrOrig = Array.isArray(schemaItem.headerOriginalOrder)
        ? schemaItem.headerOriginalOrder
        : [];
      const hdrNorm = Array.isArray(schemaItem.headerNormalizedOrder)
        ? schemaItem.headerNormalizedOrder
        : [];
      const filteredOrig = [];
      const filteredNorm = [];
      for (let i = 0; i < hdrOrig.length; i++) {
        const o = hdrOrig[i];
        const n = hdrNorm[i] || (typeof o === "string" ? o.toLowerCase() : "");
        if (String(o).trim() === String(rawColumnName).trim()) {
        } else if (
          String(n).trim() === String(rawColumnName).trim().toLowerCase()
        ) {
        } else {
          filteredOrig.push(o);
          filteredNorm.push(n);
        }
      }
      if (
        filteredOrig.length !== hdrOrig.length ||
        filteredNorm.length !== hdrNorm.length
      ) {
        const Key = marshall({ [DYNAMO_PK]: schemaPk });
        const exprVals = marshall({
          ":h": filteredOrig,
          ":n": filteredNorm,
          ":u": new Date().toISOString(),
        });
        const params = {
          TableName: DYNAMO_TABLE,
          Key,
          UpdateExpression:
            "SET headerOriginalOrder = :h, headerNormalizedOrder = :n, updatedAt = :u",
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

    return res.json({
      message: "Column deletion completed",
      removedCount,
      updatedSchema,
    });
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
    const rawColumnName =
      (req.body && (req.body.column || req.body.columnName)) || null;
    if (!category || !rawColumnName)
      return res
        .status(400)
        .json({ error: "Missing category or column name in body" });
    if (
      ["nom", "nombre", "pierres", "piedras"].includes(
        String(rawColumnName).trim().toLowerCase()
      )
    )
      return res.status(403).json({ error: "Protected column" });

    const rows = await scanAll(dynamo, {
      FilterExpression: "#c = :cat",
      ExpressionAttributeNames: { "#c": "category" },
      ExpressionAttributeValues: marshall({ ":cat": category }),
      ProjectionExpression: `${DYNAMO_PK}`,
    });

    let removedCount = 0;
    for (const r of rows) {
      const pkVal = r[DYNAMO_PK];
      if (
        typeof pkVal === "string" &&
        pkVal.startsWith(`${DYNAMO_SCHEMA_PREFIX}#`)
      )
        continue;
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
        console.error(
          "Failed to remove attribute for item:",
          pkVal,
          rawColumnName,
          err
        );
      }
    }

    // update schema (same logic as above)
    const all = await scanAll(dynamo, {});
    const schemaPk = `${DYNAMO_SCHEMA_PREFIX}#${category}`;
    const schemaItem = all.find((s) => s[DYNAMO_PK] === schemaPk);
    let updatedSchema = false;
    if (schemaItem) {
      const hdrOrig = Array.isArray(schemaItem.headerOriginalOrder)
        ? schemaItem.headerOriginalOrder
        : [];
      const hdrNorm = Array.isArray(schemaItem.headerNormalizedOrder)
        ? schemaItem.headerNormalizedOrder
        : [];
      const filteredOrig = [];
      const filteredNorm = [];
      for (let i = 0; i < hdrOrig.length; i++) {
        const o = hdrOrig[i];
        const n = hdrNorm[i] || (typeof o === "string" ? o.toLowerCase() : "");
        if (String(o).trim() === String(rawColumnName).trim()) {
        } else if (
          String(n).trim() === String(rawColumnName).trim().toLowerCase()
        ) {
        } else {
          filteredOrig.push(o);
          filteredNorm.push(n);
        }
      }
      if (
        filteredOrig.length !== hdrOrig.length ||
        filteredNorm.length !== hdrNorm.length
      ) {
        const Key = marshall({ [DYNAMO_PK]: schemaPk });
        const exprVals = marshall({
          ":h": filteredOrig,
          ":n": filteredNorm,
          ":u": new Date().toISOString(),
        });
        const params = {
          TableName: DYNAMO_TABLE,
          Key,
          UpdateExpression:
            "SET headerOriginalOrder = :h, headerNormalizedOrder = :n, updatedAt = :u",
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

    return res.json({
      message: "Column deletion completed",
      removedCount,
      updatedSchema,
    });
  } catch (err) {
    console.error("DELETE /category/:name/column (body) error", err);
    return res
      .status(500)
      .json({ error: "Failed to delete column", details: String(err) });
  }
});

// Export routes
app.get("/export/category/:name", async (req, res) => {
  try {
    const category = req.params.name;
    const dynamo = createDynamoClient();
    const all = await scanAll(dynamo, {});
    const schemaItem = all.find(
      (s) => s[DYNAMO_PK] === `${DYNAMO_SCHEMA_PREFIX}#${category}`
    );

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

    const workbook = XLSX.utils.book_new();
    const headerOrig =
      schemaItem && schemaItem.headerOriginalOrder
        ? schemaItem.headerOriginalOrder
        : [];
    const headerNorm =
      schemaItem && schemaItem.headerNormalizedOrder
        ? schemaItem.headerNormalizedOrder
        : headerOrig.map((h) => String(h).toLowerCase());
    const columns = [DYNAMO_PK, ...headerOrig];

    const rows = items.map((it) => {
      const row = {};
      row[DYNAMO_PK] = it[DYNAMO_PK];
      for (let i = 0; i < headerNorm.length; i++) {
        const norm = headerNorm[i];
        const orig = headerOrig[i] || norm;
        row[orig] = it[norm] === undefined ? "" : it[norm];
      }
      return row;
    });

    const ws = XLSX.utils.json_to_sheet(rows, { header: columns });
    XLSX.utils.book_append_sheet(
      workbook,
      ws,
      (category || "category").substring(0, 31)
    );
    const wbout = XLSX.write(workbook, { bookType: "xlsx", type: "buffer" });
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${category}.xlsx"`
    );
    res.send(wbout);
  } catch (err) {
    console.error("/export/category error", err);
    res
      .status(500)
      .json({
        error: "Failed to export category as XLSX",
        details: String(err),
      });
  }
});

app.get("/export/all", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const all = await scanAll(dynamo, {});
    const schemaItems = all.filter((it) => {
      const pk = it[DYNAMO_PK];
      return (
        typeof pk === "string" && pk.startsWith(`${DYNAMO_SCHEMA_PREFIX}#`)
      );
    });
    if (!schemaItems.length)
      return res
        .status(400)
        .json({ error: "No categories/schema found to export" });
    const workbook = XLSX.utils.book_new();

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
          if (obj[DYNAMO_PK] === `${DYNAMO_SCHEMA_PREFIX}#${category}`)
            continue;
          items.push(obj);
        }
        ExclusiveStartKey = resp.LastEvaluatedKey;
      } while (ExclusiveStartKey);

      const headerOrig =
        sch.headerOriginalOrder && sch.headerOriginalOrder.length
          ? sch.headerOriginalOrder
          : [];
      const headerNorm =
        sch.headerNormalizedOrder && sch.headerNormalizedOrder.length
          ? sch.headerNormalizedOrder
          : headerOrig.map((h) => String(h).toLowerCase());
      const columns = [DYNAMO_PK, ...headerOrig];

      const rows = items.map((it) => {
        const row = {};
        row[DYNAMO_PK] = it[DYNAMO_PK];
        for (let i = 0; i < headerNorm.length; i++) {
          const norm = headerNorm[i];
          const orig = headerOrig[i] || norm;
          row[orig] = it[norm] === undefined ? "" : it[norm];
        }
        return row;
      });

      const ws = XLSX.utils.json_to_sheet(rows, { header: columns });
      XLSX.utils.book_append_sheet(
        workbook,
        ws,
        (category || "category").substring(0, 31)
      );
    }

    const wbout = XLSX.write(workbook, { bookType: "xlsx", type: "buffer" });
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="all_categories.xlsx"`
    );
    res.send(wbout);
  } catch (err) {
    console.error("Export all error", err);
    res
      .status(500)
      .json({ error: "Failed to export all categories", details: String(err) });
  }
});

// Insert column at a specific index in schema arrays and initialize values in items
// Body: { columnName: string, insertIndex: number, defaultValue?: any }
app.post("/category/:name/column", async (req, res) => {
  const dynamo = createDynamoClient();
  try {
    const category = req.params.name;
    const { columnName, insertIndex, defaultValue = null } = req.body || {};
    if (!category)
      return res.status(400).json({ error: "Missing category param" });
    if (!columnName || String(columnName).trim() === "")
      return res.status(400).json({ error: "Missing columnName in body" });

    // Disallow protected names and reserved attributes
    const protectedSet = new Set([
      "nom",
      "nombre",
      "pierres",
      "piedras",
      "total",
      "id",
      String(DYNAMO_PK).toLowerCase(),
      "category",
    ]);
    const colNorm = norm(columnName);
    if (protectedSet.has(colNorm))
      return res
        .status(403)
        .json({ error: `Protected or reserved column "${columnName}"` });

    // Load schema
    const all = await scanAll(dynamo, {});
    const schemaPk = `${DYNAMO_SCHEMA_PREFIX}#${category}`;
    const schemaItem = all.find((s) => s[DYNAMO_PK] === schemaPk);
    if (!schemaItem)
      return res
        .status(404)
        .json({ error: `Schema not found for category "${category}"` });

    const headerOrig = Array.isArray(schemaItem.headerOriginalOrder)
      ? schemaItem.headerOriginalOrder.slice()
      : [];
    const headerNorm = Array.isArray(schemaItem.headerNormalizedOrder)
      ? schemaItem.headerNormalizedOrder.slice()
      : headerOrig.map((h) => norm(h));

    // If column already exists, abort
    if (headerNorm.includes(colNorm))
      return res
        .status(409)
        .json({ error: `Column "${columnName}" already exists in schema` });

    // Clamp insert index
    const idx = Math.max(
      0,
      Math.min(Number(insertIndex ?? headerOrig.length), headerOrig.length)
    );
    headerOrig.splice(idx, 0, String(columnName));
    headerNorm.splice(idx, 0, colNorm);

    // Update schema item
    const schemaKey = marshall({ [DYNAMO_PK]: schemaPk });
    const exprVals = marshall({
      ":h": headerOrig,
      ":n": headerNorm,
      ":u": new Date().toISOString(),
    });
    const schemaParams = {
      TableName: DYNAMO_TABLE,
      Key: schemaKey,
      UpdateExpression:
        "SET headerOriginalOrder = :h, headerNormalizedOrder = :n, updatedAt = :u",
      ExpressionAttributeValues: exprVals,
      ReturnValues: "ALL_NEW",
    };
    await dynamo.send(new UpdateItemCommand(schemaParams));

    // Initialize attribute on all items of this category
    const items = [];
    let ExclusiveStartKey = undefined;
    do {
      // NOTE: removed "#attr" from ExpressionAttributeNames to avoid ValidationException
      const scanParams = {
        TableName: DYNAMO_TABLE,
        FilterExpression: "#c = :cat",
        ExpressionAttributeNames: { "#c": "category" },
        ExpressionAttributeValues: marshall({ ":cat": category }),
        ExclusiveStartKey,
      };
      const scanCmd = new ScanCommand(scanParams);
      const resp = await dynamo.send(scanCmd);
      const raw = resp.Items || [];
      for (const it of raw) {
        const obj = unmarshall(it);
        const pkVal = obj[DYNAMO_PK];
        if (
          typeof pkVal === "string" &&
          pkVal.startsWith(`${DYNAMO_SCHEMA_PREFIX}#`)
        )
          continue; // skip schema

        try {
          const Key = marshall({ [DYNAMO_PK]: pkVal });
          const up = {
            TableName: DYNAMO_TABLE,
            Key,
            UpdateExpression: "SET #attr = :val",
            ExpressionAttributeNames: { "#attr": String(columnName) },
            ExpressionAttributeValues: marshall({ ":val": defaultValue }),
          };
          await dynamo.send(new UpdateItemCommand(up));
        } catch (err) {
          console.error("Failed to initialize new column on item", pkVal, err);
        }
        items.push(obj);
      }
      ExclusiveStartKey = resp.LastEvaluatedKey;
    } while (ExclusiveStartKey);

    return res.json({
      message: "Column inserted",
      column: columnName,
      insertIndex: idx,
      initializedCount: items.length,
      schema: {
        headerOriginalOrder: headerOrig,
        headerNormalizedOrder: headerNorm,
      },
    });
  } catch (err) {
    console.error("POST /category/:name/column error", err);
    return res
      .status(500)
      .json({ error: "Failed to insert column", details: String(err) });
  }
});

// Create a new item (row) in category
// Body: { category: string, values: Record<string, any>, id?: string }
// Returns created item. The viewer controls visual insertion position client-side.
app.post("/item", async (req, res) => {
  try {
    const dynamo = createDynamoClient();
    const { category, values = {}, id } = req.body || {};
    if (!category)
      return res.status(400).json({ error: "Missing category in body" });

    // Load schema to validate/normalize keys
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
    // Populate allowed attributes; non-schema keys ignored
    for (const k of Object.keys(values || {})) {
      const nk = norm(k);
      if (nk === norm(DYNAMO_PK) || nk === "category") continue;
      if (headerNorm.includes(nk)) item[nk] = values[k];
    }
    // Ensure all schema keys exist (null if not provided)
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

app.listen(port, () => {
  console.log(`CSV/XLSX app listening at http://localhost:${port}`);
  console.log("ENV:", {
    DYNAMO_TABLE,
    DYNAMO_PK,
    AWS_REGION,
    DYNAMO_SCHEMA_PREFIX,
  });
});

process.on("unhandledRejection", (reason) => {
  console.error("Unhandled Rejection:", reason);
});
process.on("uncaughtException", (err) => {
  console.error("Uncaught Exception:", err);
  process.exit(1);
});
