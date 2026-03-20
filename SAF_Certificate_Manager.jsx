import React, { useState, useEffect, useCallback, useRef } from "react";
import { supabase } from "./src/supabase.js";

const ANTHROPIC_MODEL = "claude-sonnet-4-20250514";
const ANTHROPIC_HEADERS = {
  "Content-Type": "application/json",
  "x-api-key": import.meta.env.VITE_ANTHROPIC_KEY,
  "anthropic-version": "2023-06-01",
  "anthropic-dangerous-direct-browser-access": "true",
};

const MATCH_TOLERANCE = 0.002;
const MAX_COMBINATION_SIZE = 6;
const MAX_SEARCH_RESULTS = 8;
const MAX_POOL_SIZE = 28;

const NUMERIC_FIELDS = [
  "quantity",
  "totalVolume",
  "ghgTotal",
  "ghgSaving",
  "ghgEec",
  "ghgEl",
  "ghgEp",
  "ghgEtd",
  "ghgEu",
  "ghgEsca",
  "ghgEccs",
  "ghgEccr",
  "energyContent",
  "lcv",
  "density",
];

const EXTRACT_PROMPT = `You are an expert in SAF (Sustainable Aviation Fuel) certification documents.
Extract ALL of the following fields from this certificate PDF. Return ONLY a valid JSON object with these exact keys:

{
  "docType": "PoS (Proof of Sustainability) or PoC (Proof of Compliance) — PoC is an umbrella certificate referencing underlying PoS batches",
  "uniqueNumber": "",
  "dateIssuance": "",
  "issuer": "the entity that issued/signed this document",
  "issuerAddress": "",
  "issuerCertNumber": "certification number of the issuer",
  "safSupplier": "the actual SAF producer/supplier — may differ from issuer on PoC docs (look for 'underlying supplier', 'SAF producer', or the PoS holder name)",
  "safSupplierAddress": "",
  "recipient": "buyer / consignee",
  "recipientAddress": "",
  "contractNumber": "all contract references found (comma-separated if multiple, e.g. LMT21482_1576, LMT22430_1576)",
  "dispatchAddress": "",
  "receiptAddress": "",
  "supplyPeriod": "for PoC: the full supply period, e.g. '01/01/2025 – 31/12/2025'",
  "dateDispatch": "for PoS: the actual dispatch date; leave empty for PoC",
  "physicalDeliveryAirport": "ICAO or name of the main physical delivery airport",
  "deliveryAirports": "all delivery airports listed (comma-separated ICAO/IATA codes)",
  "productType": "",
  "rawMaterial": "",
  "rawMaterialOrigin": "",
  "quantity": "for PoS: the SAF quantity. For PoC: the bio/SAF component quantity (NOT the total fossil+bio volume)",
  "totalVolume": "for PoC only: total physical JET A-1 volume including fossil component",
  "quantityUnit": "m3, litres, MT, etc.",
  "energyContent": "",
  "euRedCompliant": "",
  "isccCompliant": "",
  "chainOfCustody": "",
  "productionCountry": "",
  "productionStartDate": "",
  "ghgTotal": "weighted average GHG intensity (gCO2eq/MJ) — compute weighted average if multiple batches",
  "ghgSaving": "weighted average GHG saving % vs fossil reference — compute weighted average if multiple batches",
  "ghgEec": "",
  "ghgEl": "",
  "ghgEp": "",
  "ghgEtd": "",
  "ghgEu": "",
  "ghgEsca": "",
  "ghgEccs": "",
  "ghgEccr": "",
  "wasteResidueCompliant": "",
  "sustainabilityCriteria": "",
  "certificationScheme": "",
  "complianceScheme": "",
  "underlyingPoSList": [
    {
      "poSNumber": "e.g. LUD_25_12_053",
      "rawMaterial": "",
      "origin": "country of origin of raw material",
      "ghgTotal": "",
      "ghgSaving": "",
      "quantity": "",
      "quantityUnit": ""
    }
  ],
  "isComplexPoC": "true if PoC covering 3+ airports with a table of monthly volumes",
  "monthlyVolumes": [{ "month": "YYYY-MM", "airport": "ICAO/IATA", "quantity": "", "quantityUnit": "" }],
  "airportVolumes": [{ "airport": "ICAO/IATA", "country": "", "quantity": "", "quantityUnit": "" }],
  "safType": "e.g. HEFA, AtJ, FT",
  "lcv": "",
  "density": ""
}

IMPORTANT RULES:
- underlyingPoSList should be an array of objects (one per underlying PoS batch). If only one batch or a PoS doc, still use the array format with one entry.
- On a PoC, "quantity" must be the bio/SAF volume only, NOT the total blended JET A-1 volume.
- contractNumber: capture ALL contract references (look for EXTRANET refs, LMT numbers, contract IDs).
- isComplexPoC: set to "true" if this is a PoC covering 3 or more airports with a breakdown table of volumes per month. Otherwise "false" or "".
- monthlyVolumes: if the document contains a table with volumes broken down by month AND airport, extract each row as an entry with "month" (YYYY-MM), "airport" (ICAO or IATA code), "quantity", "quantityUnit". Leave as [] if no such table exists.
- airportVolumes: if the document lists volumes per airport (but not per month), extract each row with "airport", "country", "quantity", "quantityUnit". Leave as [] if not present.
- If a field is not present, use empty string "" (or empty array [] for underlyingPoSList/monthlyVolumes/airportVolumes).
- NUMBERS: always use a dot "." as the decimal separator, never a comma. E.g. write 5.599 not 5,599 — even if the source document uses European comma-decimal formatting.
- Return ONLY the JSON, no markdown, no explanation.`;

const INVOICE_HEADER_ALIASES = {
  invoice_no: ["invoice no", "invoice_no", "invoice"],
  customer: ["customer"],
  uplift_date: ["uplift date", "date"],
  flight_no: ["flight no", "flight"],
  delivery_ticket: ["delivery ticket", "delivery tick", "delivery", "ticket"],
  iata: ["iata"],
  icao: ["icao"],
  country: ["country"],
  supplier: ["supplier"],
  vol_m3: ["vol in m3", "vol m3", "volume m3", "m3"],
};

function normalizeText(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function compactText(value) {
  return normalizeText(value).replace(/\s+/g, "");
}

function formatVolume(value) {
  const num = typeof value === "number" ? value : parseFlexibleNumber(value);
  if (!Number.isFinite(num)) return "—";
  return num.toFixed(3).replace(/\.?0+$/, "");
}

function parseFlexibleNumber(value) {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  let str = String(value ?? "").trim();
  if (!str) return null;
  str = str.replace(/\s+/g, "");
  if (str.includes(",") && str.includes(".")) {
    if (str.lastIndexOf(",") > str.lastIndexOf(".")) {
      str = str.replace(/\./g, "").replace(",", ".");
    } else {
      str = str.replace(/,/g, "");
    }
  } else if (str.includes(",")) {
    const parts = str.split(",");
    if (parts.length === 2 && parts[1].length <= 3) {
      str = `${parts[0].replace(/\./g, "")}.${parts[1]}`;
    } else {
      str = str.replace(/,/g, "");
    }
  }
  str = str.replace(/[^0-9.-]/g, "");
  if (!str || str === "-" || str === "." || str === "-.") return null;
  const num = Number(str);
  return Number.isFinite(num) ? num : null;
}

function parseDateValue(value) {
  const str = String(value ?? "").trim();
  if (!str) return null;
  const dmy = str.match(/^(\d{1,2})[/. -](\d{1,2})[/. -](\d{4})$/);
  if (dmy) {
    const date = new Date(Date.UTC(Number(dmy[3]), Number(dmy[2]) - 1, Number(dmy[1])));
    return Number.isNaN(date.getTime()) ? null : date;
  }
  const ymd = str.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (ymd) {
    const date = new Date(Date.UTC(Number(ymd[1]), Number(ymd[2]) - 1, Number(ymd[3])));
    return Number.isNaN(date.getTime()) ? null : date;
  }
  const parsed = new Date(str);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function toISODate(value) {
  const date = value instanceof Date ? value : parseDateValue(value);
  if (!date) return null;
  return date.toISOString().slice(0, 10);
}

function dateInRange(dateStr, range) {
  if (!range?.start || !range?.end) return false;
  const date = parseDateValue(dateStr);
  if (!date) return false;
  return date >= range.start && date <= range.end;
}

function parseDateRange(cert) {
  if (cert?.supplyPeriod) {
    const parts = cert.supplyPeriod.split(/[\u2013\u2014\-–]+/).map((part) => part.trim());
    if (parts.length >= 2) {
      const start = parseDateValue(parts[0]);
      const end = parseDateValue(parts[1]);
      if (start && end) return { start, end };
    }
  }
  if (cert?.dateDispatch) {
    const sameDay = parseDateValue(cert.dateDispatch);
    if (sameDay) return { start: sameDay, end: sameDay };
  }
  return null;
}

function splitRefs(value) {
  return String(value ?? "")
    .split(/[,;/\n]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function tokenizeName(value) {
  return normalizeText(value)
    .split(/\s+/)
    .filter((token) => token.length >= 4);
}

function parseCSVGrid(text) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    const next = text[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        field += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      row.push(field);
      field = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && next === "\n") i += 1;
      row.push(field);
      if (row.some((cell) => cell !== "")) rows.push(row);
      row = [];
      field = "";
      continue;
    }

    field += char;
  }

  row.push(field);
  if (row.some((cell) => cell !== "")) rows.push(row);
  return rows;
}

function resolveHeader(headers, aliases) {
  const normalized = headers.map((header) => compactText(header));
  for (const alias of aliases) {
    const compactAlias = compactText(alias);
    const exactIndex = normalized.findIndex((header) => header === compactAlias);
    if (exactIndex >= 0) return headers[exactIndex];
  }
  for (const alias of aliases) {
    const compactAlias = compactText(alias);
    const fuzzyIndex = normalized.findIndex((header) => header.includes(compactAlias));
    if (fuzzyIndex >= 0) return headers[fuzzyIndex];
  }
  return null;
}

function parseInvoiceCSV(text) {
  const grid = parseCSVGrid(text);
  if (!grid.length) return { headers: [], rows: [], missing: Object.keys(INVOICE_HEADER_ALIASES) };

  const headers = grid[0].map((header) => String(header ?? "").trim());
  const headerMap = Object.fromEntries(
    Object.entries(INVOICE_HEADER_ALIASES).map(([key, aliases]) => [key, resolveHeader(headers, aliases)])
  );
  const missing = Object.entries(headerMap)
    .filter(([, value]) => !value)
    .map(([key]) => key);

  const rows = grid.slice(1).map((cells, index) => {
    const raw = {};
    headers.forEach((header, cellIndex) => {
      raw[header] = String(cells[cellIndex] ?? "").trim();
    });

    return {
      row_number: index + 2,
      invoice_no: headerMap.invoice_no ? raw[headerMap.invoice_no] || "" : "",
      customer: headerMap.customer ? raw[headerMap.customer] || "" : "",
      uplift_date: toISODate(headerMap.uplift_date ? raw[headerMap.uplift_date] : ""),
      flight_no: headerMap.flight_no ? raw[headerMap.flight_no] || "" : "",
      delivery_ticket: headerMap.delivery_ticket ? raw[headerMap.delivery_ticket] || "" : "",
      iata: headerMap.iata ? (raw[headerMap.iata] || "").toUpperCase() : "",
      icao: headerMap.icao ? (raw[headerMap.icao] || "").toUpperCase() : "",
      country: headerMap.country ? raw[headerMap.country] || "" : "",
      supplier: headerMap.supplier ? raw[headerMap.supplier] || "" : "",
      vol_m3: parseFlexibleNumber(headerMap.vol_m3 ? raw[headerMap.vol_m3] : ""),
      raw_payload: raw,
      is_allocated: false,
    };
  });

  return { headers, rows, missing };
}

function normalizeCommaDecimals(parsed) {
  const fix = (value) => {
    if (typeof value !== "string") return value;
    return value.replace(/^(-?\d+),(\d{1,3})(%?)$/, (_, integer, decimal, suffix) => `${integer}.${decimal}${suffix}`);
  };

  const out = { ...parsed };
  for (const field of NUMERIC_FIELDS) {
    if (out[field]) out[field] = fix(out[field]);
  }
  if (Array.isArray(out.underlyingPoSList)) {
    out.underlyingPoSList = out.underlyingPoSList.map((batch) => ({
      ...batch,
      ghgTotal: fix(batch.ghgTotal ?? ""),
      ghgSaving: fix(batch.ghgSaving ?? ""),
      quantity: fix(batch.quantity ?? ""),
    }));
  }
  return out;
}

function fileToBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result.split(",")[1]);
    reader.onerror = () => reject(new Error("Read failed"));
    reader.readAsDataURL(file);
  });
}

function blobToBase64(blob) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result.split(",")[1]);
    reader.onerror = () => reject(new Error("Read failed"));
    reader.readAsDataURL(blob);
  });
}

async function extractCertificateFromBase64(base64) {
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: ANTHROPIC_HEADERS,
    body: JSON.stringify({
      model: ANTHROPIC_MODEL,
      max_tokens: 4096,
      messages: [
        {
          role: "user",
          content: [
            {
              type: "document",
              source: { type: "base64", media_type: "application/pdf", data: base64 },
            },
            { type: "text", text: EXTRACT_PROMPT },
          ],
        },
      ],
    }),
  });

  const data = await res.json();
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${data.error?.message || JSON.stringify(data)}`);
  }
  const text = data.content?.map((block) => block.text || "").join("") || "{}";
  const clean = text.replace(/```json|```/g, "").trim();
  return normalizeCommaDecimals(JSON.parse(clean));
}

function isMissingTableError(error) {
  return error?.code === "42P01" || /does not exist/i.test(error?.message || "");
}

async function fetchAllPages(buildQuery, pageSize = 1000) {
  let from = 0;
  const allRows = [];

  while (true) {
    const { data, error } = await buildQuery(from, from + pageSize - 1);
    if (error) return { data: null, error };
    allRows.push(...(data || []));
    if (!data || data.length < pageSize) break;
    from += pageSize;
  }

  return { data: allRows, error: null };
}

function buildCertCriteria(cert) {
  const airports = new Set();
  splitRefs(cert?.deliveryAirports).forEach((code) => airports.add(code.toUpperCase()));
  if (cert?.physicalDeliveryAirport) airports.add(String(cert.physicalDeliveryAirport).trim().toUpperCase());
  (cert?.airportVolumes || []).forEach((item) => item?.airport && airports.add(String(item.airport).trim().toUpperCase()));
  (cert?.monthlyVolumes || []).forEach((item) => item?.airport && airports.add(String(item.airport).trim().toUpperCase()));

  const contractRefs = splitRefs(cert?.contractNumber).map((item) => item.toUpperCase());
  const recipientTokens = tokenizeName(cert?.recipient);
  const supplierTokens = tokenizeName(cert?.safSupplier || cert?.issuer);

  return {
    targetVolume: parseFlexibleNumber(cert?.quantity),
    airports,
    dateRange: parseDateRange(cert),
    contractRefs,
    recipientTokens,
    supplierTokens,
  };
}

function scoreInvoiceRow(row, criteria) {
  const haystack = [
    row.invoice_no,
    row.delivery_ticket,
    row.customer,
    row.supplier,
    row.iata,
    row.icao,
    ...Object.values(row.raw_payload || {}),
  ]
    .join(" ")
    .toUpperCase();

  const contractMatch =
    criteria.contractRefs.length > 0 &&
    criteria.contractRefs.some((ref) => ref && haystack.includes(ref));
  const airportMatch =
    criteria.airports.size > 0 &&
    (criteria.airports.has((row.iata || "").toUpperCase()) || criteria.airports.has((row.icao || "").toUpperCase()));
  const dateMatch = criteria.dateRange ? dateInRange(row.uplift_date, criteria.dateRange) : false;
  const customerText = normalizeText(row.customer);
  const supplierText = normalizeText(row.supplier);
  const customerMatch =
    criteria.recipientTokens.length > 0 &&
    criteria.recipientTokens.some((token) => customerText.includes(token));
  const supplierMatch =
    criteria.supplierTokens.length > 0 &&
    criteria.supplierTokens.some((token) => supplierText.includes(token));

  let score = 0;
  if (contractMatch) score += 12;
  if (airportMatch) score += 6;
  if (dateMatch) score += 4;
  if (customerMatch) score += 2;
  if (supplierMatch) score += 1;

  return {
    contractMatch,
    airportMatch,
    dateMatch,
    customerMatch,
    supplierMatch,
    score,
  };
}

function serializeRowSnapshot(row) {
  return {
    invoice_row_id: row.id,
    row_number: row.row_number,
    invoice_no: row.invoice_no || "",
    customer: row.customer || "",
    uplift_date: row.uplift_date || null,
    iata: row.iata || "",
    icao: row.icao || "",
    allocated_m3: Number(row.vol_m3),
  };
}

function buildCandidateFromRows(rows, label, signals, priority, criteria) {
  const totalVolume = rows.reduce((sum, row) => sum + Number(row.vol_m3 || 0), 0);
  const variance = totalVolume - criteria.targetVolume;
  const invoiceRows = rows.map(serializeRowSnapshot);
  const score =
    priority * 100 +
    rows.reduce((sum, row) => sum + (signals.get(row.id)?.score || 0), 0) -
    Math.abs(variance) * 1000 -
    rows.length;
  const reasonParts = [];
  if (label) reasonParts.push(label);
  if (invoiceRows.some((row) => row.iata || row.icao)) reasonParts.push("row-level airport evidence");
  return {
    key: invoiceRows.map((row) => row.invoice_row_id).sort().join("|"),
    match_method: label,
    total_volume_m3: Number(totalVolume.toFixed(6)),
    variance_m3: Number(variance.toFixed(6)),
    rows: invoiceRows,
    reason: reasonParts.join(" · "),
    score,
  };
}

function searchCandidateSets(pool, criteria, signals, label, priority) {
  const results = [];
  const sorted = [...pool]
    .filter((row) => Number.isFinite(Number(row.vol_m3)) && Number(row.vol_m3) > 0 && Number(row.vol_m3) <= criteria.targetVolume + MATCH_TOLERANCE)
    .sort((a, b) => {
      const scoreDiff = (signals.get(b.id)?.score || 0) - (signals.get(a.id)?.score || 0);
      if (scoreDiff !== 0) return scoreDiff;
      const closenessDiff =
        Math.abs(criteria.targetVolume - Number(a.vol_m3)) - Math.abs(criteria.targetVolume - Number(b.vol_m3));
      if (closenessDiff !== 0) return closenessDiff;
      return (b.vol_m3 || 0) - (a.vol_m3 || 0);
    })
    .slice(0, MAX_POOL_SIZE);

  function walk(startIndex, chosen, sum) {
    if (results.length >= MAX_SEARCH_RESULTS) return;
    if (chosen.length > 0 && Math.abs(sum - criteria.targetVolume) <= MATCH_TOLERANCE) {
      results.push(buildCandidateFromRows(chosen, label, signals, priority, criteria));
      return;
    }
    if (chosen.length >= MAX_COMBINATION_SIZE || sum > criteria.targetVolume + MATCH_TOLERANCE) return;

    for (let index = startIndex; index < sorted.length; index += 1) {
      const row = sorted[index];
      const nextSum = sum + Number(row.vol_m3 || 0);
      if (nextSum > criteria.targetVolume + MATCH_TOLERANCE) continue;
      walk(index + 1, [...chosen, row], nextSum);
      if (results.length >= MAX_SEARCH_RESULTS) return;
    }
  }

  walk(0, [], 0);
  return results;
}

function buildDeterministicMatch(certData, invoiceRows) {
  const criteria = buildCertCriteria(certData);
  if (!Number.isFinite(criteria.targetVolume) || criteria.targetVolume <= 0) {
    return {
      status: "unmatched",
      match_method: "unusable-volume",
      cert_volume_m3: null,
      allocated_volume_m3: 0,
      variance_m3: null,
      review_note: "Certificate quantity could not be parsed into m3.",
      candidate_sets: [],
      linked_rows: [],
    };
  }

  const availableRows = invoiceRows.filter(
    (row) => !row.is_allocated && Number.isFinite(Number(row.vol_m3)) && Number(row.vol_m3) > 0
  );

  const signals = new Map(availableRows.map((row) => [row.id, scoreInvoiceRow(row, criteria)]));

  const pools = [];
  const pushPool = (rows, label, priority) => {
    if (!rows.length) return;
    pools.push({ rows, label, priority });
  };

  const exactSingles = availableRows.filter(
    (row) => Math.abs(Number(row.vol_m3) - criteria.targetVolume) <= MATCH_TOLERANCE
  );
  pushPool(exactSingles, "exact-single-row", 9);

  if (criteria.contractRefs.length) {
    pushPool(availableRows.filter((row) => signals.get(row.id)?.contractMatch), "contract-ref", 8);
  }
  if (criteria.airports.size && criteria.dateRange) {
    pushPool(
      availableRows.filter((row) => signals.get(row.id)?.airportMatch && signals.get(row.id)?.dateMatch),
      "airport+date",
      7
    );
  }
  if (criteria.airports.size) {
    pushPool(availableRows.filter((row) => signals.get(row.id)?.airportMatch), "airport", 6);
  }
  if (criteria.dateRange) {
    pushPool(availableRows.filter((row) => signals.get(row.id)?.dateMatch), "date", 5);
  }
  if (criteria.recipientTokens.length) {
    pushPool(availableRows.filter((row) => signals.get(row.id)?.customerMatch), "customer", 4);
  }

  pushPool(
    [...availableRows].sort((a, b) => {
      const scoreDiff = (signals.get(b.id)?.score || 0) - (signals.get(a.id)?.score || 0);
      if (scoreDiff !== 0) return scoreDiff;
      return Math.abs(criteria.targetVolume - Number(a.vol_m3)) - Math.abs(criteria.targetVolume - Number(b.vol_m3));
    }),
    "volume-only",
    1
  );

  const candidateMap = new Map();
  for (const pool of pools) {
    const found = searchCandidateSets(pool.rows, criteria, signals, pool.label, pool.priority);
    for (const candidate of found) {
      const existing = candidateMap.get(candidate.key);
      if (!existing || candidate.score > existing.score) candidateMap.set(candidate.key, candidate);
    }
  }

  const candidates = [...candidateMap.values()].sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    if (Math.abs(a.variance_m3) !== Math.abs(b.variance_m3)) {
      return Math.abs(a.variance_m3) - Math.abs(b.variance_m3);
    }
    if (a.rows.length !== b.rows.length) return a.rows.length - b.rows.length;
    return (a.rows[0]?.row_number || 0) - (b.rows[0]?.row_number || 0);
  });

  if (!candidates.length) {
    return {
      status: "unmatched",
      match_method: "none",
      cert_volume_m3: criteria.targetVolume,
      allocated_volume_m3: 0,
      variance_m3: criteria.targetVolume,
      review_note: `No free invoice row group matches ${formatVolume(criteria.targetVolume)} m3 within ${MATCH_TOLERANCE} m3.`,
      candidate_sets: [],
      linked_rows: [],
    };
  }

  if (candidates.length === 1) {
    const winner = candidates[0];
    return {
      status: "auto_linked",
      match_method: winner.match_method,
      cert_volume_m3: criteria.targetVolume,
      allocated_volume_m3: winner.total_volume_m3,
      variance_m3: winner.variance_m3,
      review_note: `Unique deterministic match found using ${winner.match_method}.`,
      candidate_sets: [winner],
      linked_rows: winner.rows,
    };
  }

  const topCandidates = candidates.slice(0, MAX_SEARCH_RESULTS);
  return {
    status: "needs_review",
    match_method: topCandidates[0].match_method,
    cert_volume_m3: criteria.targetVolume,
    allocated_volume_m3: topCandidates[0].total_volume_m3,
    variance_m3: topCandidates[0].variance_m3,
    review_note: `${candidates.length} valid invoice groups match ${formatVolume(criteria.targetVolume)} m3. Manual approval required.`,
    candidate_sets: topCandidates,
    linked_rows: [],
  };
}

function certTitle(cert) {
  return cert?.data?.uniqueNumber || cert?.filename || "Certificate";
}

function Badge({ status }) {
  const label = String(status || "unknown");
  const map = {
    auto_linked: ["#00ff9d", "#001a0d"],
    approved: ["#00ff9d", "#001a0d"],
    needs_review: ["#ffbb00", "#1a1200"],
    unmatched: ["#ff6666", "#1a0000"],
    rejected: ["#ff4444", "#1a0000"],
    allocated: ["#00ff9d", "#001a0d"],
    free: ["#4a9fd4", "#061423"],
  };
  const key = label.toLowerCase().replace(/\s+/g, "_");
  const [fg, bg] = map[key] || ["#c8dff0", "#0a1628"];
  return (
    <span
      style={{
        background: bg,
        color: fg,
        padding: "2px 10px",
        borderRadius: 4,
        fontFamily: "'Space Mono', monospace",
        fontSize: 10,
        fontWeight: 700,
        letterSpacing: 1,
        textTransform: "uppercase",
      }}
    >
      {label.replace(/_/g, " ")}
    </span>
  );
}

function FieldRow({ label, value, highlight }) {
  if (value === undefined || value === null || value === "") return null;
  return (
    <div
      style={{
        display: "flex",
        gap: 12,
        padding: "5px 0",
        borderBottom: "1px solid #0d2040",
      }}
    >
      <div
        style={{
          color: "#4a7fa0",
          fontSize: 11,
          width: 180,
          flexShrink: 0,
          fontFamily: "'Space Mono', monospace",
        }}
      >
        {label}
      </div>
      <div style={{ color: highlight ? "#00ff9d" : "#c8dff0", fontSize: 12, wordBreak: "break-word" }}>{value}</div>
    </div>
  );
}

function MatchRowsTable({ rows, title = "LINKED INVOICE ROWS", emptyText = "No linked invoice rows yet." }) {
  return (
    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
      <div
        style={{
          color: "#00bfff",
          fontFamily: "'Space Mono', monospace",
          fontSize: 10,
          marginBottom: 10,
          letterSpacing: 1,
        }}
      >
        {title}
      </div>
      {!rows?.length ? (
        <div style={{ color: "#4a7fa0", fontSize: 11 }}>{emptyText}</div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "'Space Mono', monospace", fontSize: 10 }}>
            <thead>
              <tr style={{ borderBottom: "1px solid #0d3060" }}>
                {["CSV ROW", "INVOICE", "CUSTOMER", "UPLIFT DATE", "IATA", "ICAO", "VOL M3"].map((header) => (
                  <th key={header} style={{ padding: "6px 10px", textAlign: "left", color: "#00bfff", whiteSpace: "nowrap" }}>
                    {header}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={`${row.invoice_row_id || row.row_number}-${index}`} style={{ borderBottom: "1px solid #0d2040" }}>
                  <td style={{ padding: "6px 10px", color: "#4a9fd4" }}>{row.row_number || "—"}</td>
                  <td style={{ padding: "6px 10px", color: "#e0f0ff" }}>{row.invoice_no || "—"}</td>
                  <td style={{ padding: "6px 10px", color: "#c8dff0" }}>{row.customer || "—"}</td>
                  <td style={{ padding: "6px 10px", color: "#888" }}>{row.uplift_date || "—"}</td>
                  <td style={{ padding: "6px 10px", color: "#c8dff0" }}>{row.iata || "—"}</td>
                  <td style={{ padding: "6px 10px", color: "#c8dff0" }}>{row.icao || "—"}</td>
                  <td style={{ padding: "6px 10px", color: "#00ff9d", fontWeight: 700 }}>{formatVolume(row.allocated_m3)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function CertCard({ cert, index, selected, onSelect, onAnalyze, onReExtract, onOpenPdf }) {
  const status = cert.match?.status;
  return (
    <div
      onClick={() => onSelect(index)}
      style={{
        background: selected ? "#0a1628" : "#060e1a",
        border: selected ? "1px solid #00bfff" : "1px solid #0d2040",
        borderRadius: 8,
        padding: "14px 18px",
        cursor: "pointer",
        marginBottom: 8,
        transition: "all 0.15s",
        boxShadow: selected ? "0 0 16px #00bfff33" : "none",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 8 }}>
        <div>
          <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 11, marginBottom: 4 }}>
            {cert.data?.docType || "CERTIFICATE"}
          </div>
          {cert.pdfPath ? (
            <button
              className="btn"
              onClick={(event) => {
                event.stopPropagation();
                onOpenPdf(cert);
              }}
              style={{
                background: "none",
                color: "#e0f0ff",
                fontWeight: 600,
                fontSize: 13,
                padding: 0,
                textAlign: "left",
                textDecoration: "underline",
                textUnderlineOffset: 3,
              }}
            >
              {certTitle(cert)}
            </button>
          ) : (
            <div style={{ color: "#e0f0ff", fontWeight: 600, fontSize: 13 }}>{certTitle(cert)}</div>
          )}
          <div style={{ color: "#4a7fa0", fontSize: 11, marginTop: 3 }}>
            {formatVolume(cert.data?.quantity)} {cert.data?.quantityUnit || "m3"}
          </div>
          <div style={{ color: "#4a7fa0", fontSize: 10, marginTop: 3 }}>
            {cert.match ? `${formatVolume(cert.match.allocated_volume_m3)} linked` : "Not matched yet"}
          </div>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 6, alignItems: "flex-end" }}>
          {status ? <Badge status={status} /> : null}
          <button
            className="btn"
            onClick={(event) => {
              event.stopPropagation();
              onAnalyze(index);
            }}
            style={{
              background: "linear-gradient(135deg,#0050aa,#00bfff)",
              color: "#fff",
              padding: "3px 10px",
              borderRadius: 4,
              fontFamily: "'Space Mono', monospace",
              fontSize: 10,
              letterSpacing: 1,
            }}
          >
            MATCH
          </button>
          {cert.pdfPath ? (
            <button
              className="btn"
              onClick={(event) => {
                event.stopPropagation();
                onReExtract(index);
              }}
              style={{
                background: "#0a1628",
                color: "#4a9fd4",
                padding: "2px 8px",
                borderRadius: 4,
                fontFamily: "'Space Mono', monospace",
                fontSize: 9,
                letterSpacing: 1,
                border: "1px solid #0d3060",
              }}
            >
              RE-EXTRACT
            </button>
          ) : null}
        </div>
      </div>
    </div>
  );
}

export default function SAFManager({ onLogout, userEmail }) {
  const [certs, setCerts] = useState([]);
  const [selected, setSelected] = useState(null);
  const [invoiceRows, setInvoiceRows] = useState([]);
  const [invoiceImport, setInvoiceImport] = useState(null);
  const [loading, setLoading] = useState("");
  const [tab, setTab] = useState("certs");
  const [log, setLog] = useState([]);
  const [expandedCandidates, setExpandedCandidates] = useState({});
  const [pdfPreview, setPdfPreview] = useState(null);
  const pdfInputRef = useRef();
  const csvInputRef = useRef();

  const addLog = useCallback((msg, type = "info") => {
    setLog((prev) => [...prev, { msg, type, ts: new Date().toLocaleTimeString() }]);
  }, []);

  const deleteByIds = useCallback(
    async (table, ids) => {
      if (!ids.length) return;
      for (let index = 0; index < ids.length; index += 200) {
        const chunk = ids.slice(index, index + 200);
        const { error } = await supabase.from(table).delete().in("id", chunk);
        if (error) throw error;
      }
    },
    []
  );

  const loadFromDB = useCallback(async () => {
    const { data: certRows, error: certErr } = await supabase
      .from("certificates")
      .select("*")
      .order("created_at", { ascending: false });
    if (certErr) {
      addLog(`Cert DB error: ${certErr.message}`, "error");
      return;
    }

    const [importRes, matchRes, linkRes] = await Promise.all([
      supabase.from("invoice_imports").select("*").order("created_at", { ascending: false }).limit(1),
      supabase.from("certificate_matches").select("*"),
      supabase.from("certificate_invoice_links").select("*"),
    ]);

    if (importRes.error && !isMissingTableError(importRes.error)) addLog(`Invoice import error: ${importRes.error.message}`, "error");
    if (matchRes.error && !isMissingTableError(matchRes.error)) addLog(`Match DB error: ${matchRes.error.message}`, "error");
    if (linkRes.error && !isMissingTableError(linkRes.error)) addLog(`Link DB error: ${linkRes.error.message}`, "error");

    const latestImport = importRes.data?.[0] || null;
    setInvoiceImport(latestImport);

    let invoiceRowsData = [];
    if (latestImport) {
      const { data, error } = await fetchAllPages((from, to) =>
        supabase
          .from("invoice_rows")
          .select("*")
          .eq("import_id", latestImport.id)
          .order("row_number", { ascending: true })
          .range(from, to)
      );
      if (error) addLog(`Invoice rows error: ${error.message}`, "error");
      else invoiceRowsData = data || [];
    }
    setInvoiceRows(invoiceRowsData);

    const linksByCertId = new Map();
    for (const link of linkRes.data || []) {
      const arr = linksByCertId.get(link.certificate_id) || [];
      arr.push(link);
      linksByCertId.set(link.certificate_id, arr);
    }

    const matchByCertId = new Map(
      (matchRes.data || []).map((match) => [
        match.certificate_id,
        {
          ...match,
          links: (linksByCertId.get(match.certificate_id) || []).sort((a, b) => (a.row_number || 0) - (b.row_number || 0)),
          candidate_sets: match.candidate_sets || [],
        },
      ])
    );

    const hydrated = (certRows || []).map((row) => ({
      id: row.id,
      filename: row.filename,
      data: row.data,
      analysis: row.analysis,
      pdfPath: row.pdf_path,
      match: matchByCertId.get(row.id) || null,
    }));
    setCerts(hydrated);
    addLog(`Loaded ${hydrated.length} certificate(s) and ${invoiceRowsData.length} invoice row(s)`, "success");
  }, [addLog]);

  useEffect(() => {
    loadFromDB();
  }, [loadFromDB]);

  useEffect(() => {
    return () => {
      if (pdfPreview?.url) URL.revokeObjectURL(pdfPreview.url);
    };
  }, [pdfPreview]);

  useEffect(() => {
    if (selected !== null && selected >= certs.length) setSelected(certs.length ? 0 : null);
    if (selected === null && certs.length) setSelected(0);
  }, [certs, selected]);

  const clearCertificateMatch = useCallback(async (certificateId) => {
    const { data: links, error: linksErr } = await supabase
      .from("certificate_invoice_links")
      .select("invoice_row_id")
      .eq("certificate_id", certificateId);
    if (linksErr && !isMissingTableError(linksErr)) throw linksErr;

    const linkedIds = (links || []).map((item) => item.invoice_row_id).filter(Boolean);
    if (linkedIds.length) {
      const { error: unlockErr } = await supabase.from("invoice_rows").update({ is_allocated: false }).in("id", linkedIds);
      if (unlockErr) throw unlockErr;
    }

    const { error: deleteErr } = await supabase.from("certificate_matches").delete().eq("certificate_id", certificateId);
    if (deleteErr && !isMissingTableError(deleteErr)) throw deleteErr;
  }, []);

  const persistMatch = useCallback(
    async (certificate, result, reviewer) => {
      await clearCertificateMatch(certificate.id);

      const { data: insertedMatch, error: matchErr } = await supabase
        .from("certificate_matches")
        .insert({
          certificate_id: certificate.id,
          status: result.status,
          match_method: result.match_method,
          cert_volume_m3: result.cert_volume_m3,
          allocated_volume_m3: result.allocated_volume_m3,
          variance_m3: result.variance_m3,
          review_note: result.review_note,
          reviewed_by: reviewer || null,
          reviewed_at: reviewer && (result.status === "approved" || result.status === "rejected") ? new Date().toISOString() : null,
          candidate_sets: result.candidate_sets || [],
          updated_at: new Date().toISOString(),
        })
        .select("*")
        .single();

      if (matchErr) throw matchErr;

      const linkedRows = result.linked_rows || [];
      if (linkedRows.length) {
        const { error: linksErr } = await supabase.from("certificate_invoice_links").insert(
          linkedRows.map((row) => ({
            certificate_match_id: insertedMatch.id,
            certificate_id: certificate.id,
            invoice_row_id: row.invoice_row_id,
            row_number: row.row_number,
            invoice_no: row.invoice_no,
            customer: row.customer,
            uplift_date: row.uplift_date,
            iata: row.iata,
            icao: row.icao,
            allocated_m3: row.allocated_m3,
          }))
        );
        if (linksErr) throw linksErr;

        const { error: allocErr } = await supabase
          .from("invoice_rows")
          .update({ is_allocated: true })
          .in(
            "id",
            linkedRows.map((row) => row.invoice_row_id)
          );
        if (allocErr) throw allocErr;
      }
    },
    [clearCertificateMatch]
  );

  const handlePDFUpload = useCallback(
    async (files) => {
      const list = Array.from(files || []);
      if (!list.length) return;
      setLoading("Extracting certificate data...");

      for (const file of list) {
        if (!file.type.includes("pdf")) continue;
        addLog(`Processing ${file.name}`, "info");
        try {
          const base64 = await fileToBase64(file);
          const parsed = await extractCertificateFromBase64(base64);
          const uniqueNumber = parsed.uniqueNumber || null;
          const storagePath = uniqueNumber ? `${uniqueNumber}.pdf` : `no-id/${Date.now()}-${file.name}`;

          const { error: storageErr } = await supabase.storage
            .from("certificates-pdf")
            .upload(storagePath, file, { contentType: "application/pdf", upsert: true });
          if (storageErr) addLog(`PDF storage warning for ${file.name}: ${storageErr.message}`, "error");

          let saveRes;
          if (uniqueNumber) {
            saveRes = await supabase
              .from("certificates")
              .insert({ filename: file.name, data: parsed, unique_number: uniqueNumber, pdf_path: storageErr ? null : storagePath })
              .select("id")
              .single();
            if (saveRes.error?.code === "23505") {
              saveRes = await supabase
                .from("certificates")
                .update({ filename: file.name, data: parsed, pdf_path: storageErr ? null : storagePath })
                .eq("unique_number", uniqueNumber)
                .select("id")
                .single();
            }
          } else {
            saveRes = await supabase
              .from("certificates")
              .insert({ filename: file.name, data: parsed, pdf_path: storageErr ? null : storagePath })
              .select("id")
              .single();
          }
          if (saveRes.error) throw saveRes.error;
          addLog(`Saved ${certTitle({ filename: file.name, data: parsed })}`, "success");
        } catch (error) {
          addLog(`Certificate import failed for ${file.name}: ${error.message}`, "error");
        }
      }

      setLoading("");
      await loadFromDB();
      setTab("certs");
    },
    [addLog, loadFromDB]
  );

  const handleCSVUpload = useCallback(
    async (file) => {
      if (!file) return;
      setLoading("Importing annual invoice CSV...");
      addLog(`Importing invoice CSV ${file.name}`, "info");

      try {
        const text = await file.text();
        const parsed = parseInvoiceCSV(text);
        if (!parsed.rows.length) throw new Error("CSV is empty.");
        if (parsed.missing.includes("vol_m3")) throw new Error("Missing required column: Vol in M3.");

        const storagePath = `invoices/${file.name}`;
        const { error: storageErr } = await supabase.storage
          .from("invoices-csv")
          .upload(storagePath, file, { contentType: "text/csv", upsert: true });
        if (storageErr) throw storageErr;

        const [existingImportsRes, existingMatchesRes] = await Promise.all([
          supabase.from("invoice_imports").select("id"),
          supabase.from("certificate_matches").select("id"),
        ]);
        if (existingMatchesRes.error && !isMissingTableError(existingMatchesRes.error)) throw existingMatchesRes.error;
        if (existingImportsRes.error && !isMissingTableError(existingImportsRes.error)) throw existingImportsRes.error;

        await deleteByIds("certificate_matches", (existingMatchesRes.data || []).map((row) => row.id));
        await deleteByIds("invoice_imports", (existingImportsRes.data || []).map((row) => row.id));

        const { data: importRow, error: importErr } = await supabase
          .from("invoice_imports")
          .insert({
            filename: file.name,
            storage_path: storagePath,
            year: 2025,
            status: "active",
            row_count: parsed.rows.length,
          })
          .select("*")
          .single();
        if (importErr) throw importErr;

        const cleanRows = parsed.rows.filter((row) => row.invoice_no || row.customer || Number.isFinite(row.vol_m3));
        for (let index = 0; index < cleanRows.length; index += 500) {
          const chunk = cleanRows.slice(index, index + 500).map((row) => ({
            ...row,
            import_id: importRow.id,
          }));
          const { error: rowsErr } = await supabase.from("invoice_rows").insert(chunk);
          if (rowsErr) throw rowsErr;
        }

        addLog(
          `Imported ${cleanRows.length} invoice rows${parsed.missing.length ? `; missing optional columns: ${parsed.missing.join(", ")}` : ""}`,
          "success"
        );
        setLoading("");
        await loadFromDB();
        setTab("db");
      } catch (error) {
        setLoading("");
        addLog(`Invoice import failed: ${error.message}`, "error");
      }
    },
    [addLog, deleteByIds, loadFromDB]
  );

  const openCertificatePdf = useCallback(
    async (cert) => {
      if (!cert?.pdfPath) {
        addLog(`No stored PDF found for ${certTitle(cert)}`, "error");
        return;
      }

      setLoading(`Opening ${certTitle(cert)}...`);
      try {
        const { data, error } = await supabase.storage.from("certificates-pdf").download(cert.pdfPath);
        if (error) throw error;
        const blobUrl = URL.createObjectURL(data);
        setPdfPreview((prev) => {
          if (prev?.url) URL.revokeObjectURL(prev.url);
          return { title: certTitle(cert), url: blobUrl };
        });
        addLog(`Opened PDF preview for ${certTitle(cert)}`, "success");
      } catch (error) {
        addLog(`Open PDF failed for ${certTitle(cert)}: ${error.message}`, "error");
      }
      setLoading("");
    },
    [addLog]
  );

  const reExtractCert = useCallback(
    async (index) => {
      const cert = certs[index];
      if (!cert?.pdfPath) {
        addLog("No stored PDF for this certificate", "error");
        return;
      }
      setLoading(`Re-extracting ${certTitle(cert)}...`);
      addLog(`Re-extracting ${cert.pdfPath}`, "info");

      try {
        const { data: blob, error: dlErr } = await supabase.storage.from("certificates-pdf").download(cert.pdfPath);
        if (dlErr) throw dlErr;
        const base64 = await blobToBase64(blob);
        const parsed = await extractCertificateFromBase64(base64);

        const { error: updateErr } = await supabase.from("certificates").update({ data: parsed }).eq("id", cert.id);
        if (updateErr) throw updateErr;

        addLog(`Re-extracted ${certTitle({ ...cert, data: parsed })}`, "success");
        setLoading("");
        await loadFromDB();
      } catch (error) {
        setLoading("");
        addLog(`Re-extract failed: ${error.message}`, "error");
      }
    },
    [addLog, certs, loadFromDB]
  );

  const analyzeSingle = useCallback(
    async (index) => {
      const cert = certs[index];
      if (!cert?.id) return;
      if (!invoiceRows.length) {
        addLog("Load the annual invoice CSV before matching certificates.", "error");
        return;
      }

      setLoading(`Matching ${certTitle(cert)}...`);
      addLog(`Matching ${certTitle(cert)}`, "info");

      try {
        const workingRows = invoiceRows.map((row) => ({ ...row }));
        for (const link of cert.match?.links || []) {
          const target = workingRows.find((row) => row.id === link.invoice_row_id);
          if (target) target.is_allocated = false;
        }
        const result = buildDeterministicMatch(cert.data, workingRows);
        await persistMatch(cert, result, null);
        addLog(`${certTitle(cert)} → ${result.status}`, result.status === "unmatched" ? "error" : "success");
        setLoading("");
        await loadFromDB();
        setTab("analysis");
      } catch (error) {
        setLoading("");
        addLog(`Matching failed for ${certTitle(cert)}: ${error.message}`, "error");
      }
    },
    [addLog, certs, invoiceRows, loadFromDB, persistMatch]
  );

  const analyzeAll = useCallback(async () => {
    if (!certs.length) return;
    if (!invoiceRows.length) {
      addLog("Load the annual invoice CSV before matching certificates.", "error");
      return;
    }

    setLoading("Resetting allocations...");
    addLog("Resetting all matches before full run", "info");

    try {
      const existingMatchesRes = await supabase.from("certificate_matches").select("id");
      if (existingMatchesRes.error && !isMissingTableError(existingMatchesRes.error)) throw existingMatchesRes.error;
      await deleteByIds("certificate_matches", (existingMatchesRes.data || []).map((row) => row.id));
      const { error: resetRowsErr } = await supabase.from("invoice_rows").update({ is_allocated: false }).neq("id", "00000000-0000-0000-0000-000000000000");
      if (resetRowsErr) throw resetRowsErr;

      const workingRows = invoiceRows.map((row) => ({ ...row, is_allocated: false }));
      for (let index = 0; index < certs.length; index += 1) {
        const cert = certs[index];
        setLoading(`Matching ${index + 1}/${certs.length}: ${certTitle(cert)}`);
        const result = buildDeterministicMatch(cert.data, workingRows);
        await persistMatch(cert, result, null);
        if (result.linked_rows?.length) {
          for (const linked of result.linked_rows) {
            const row = workingRows.find((item) => item.id === linked.invoice_row_id);
            if (row) row.is_allocated = true;
          }
        }
        addLog(`${certTitle(cert)} → ${result.status}`, result.status === "needs_review" ? "info" : "success");
      }

      setLoading("");
      await loadFromDB();
      setTab("analysis");
    } catch (error) {
      setLoading("");
      addLog(`Full matching run failed: ${error.message}`, "error");
    }
  }, [addLog, certs, deleteByIds, invoiceRows, loadFromDB, persistMatch]);

  const approveCandidate = useCallback(
    async (cert, candidateIndex) => {
      const candidate = cert.match?.candidate_sets?.[candidateIndex];
      if (!candidate) return;

      setLoading(`Approving match for ${certTitle(cert)}...`);
      try {
        await persistMatch(
          cert,
          {
            status: "approved",
            match_method: candidate.match_method,
            cert_volume_m3: cert.match.cert_volume_m3,
            allocated_volume_m3: candidate.total_volume_m3,
            variance_m3: candidate.variance_m3,
            review_note: `Approved candidate ${candidateIndex + 1}.`,
            candidate_sets: cert.match.candidate_sets,
            linked_rows: candidate.rows,
          },
          userEmail
        );
        addLog(`Approved match for ${certTitle(cert)}`, "success");
        setLoading("");
        await loadFromDB();
      } catch (error) {
        setLoading("");
        addLog(`Approval failed: ${error.message}`, "error");
      }
    },
    [addLog, loadFromDB, persistMatch, userEmail]
  );

  const rejectMatch = useCallback(
    async (cert) => {
      setLoading(`Rejecting match for ${certTitle(cert)}...`);
      try {
        await persistMatch(
          cert,
          {
            status: "rejected",
            match_method: cert.match?.match_method || "manual",
            cert_volume_m3: cert.match?.cert_volume_m3 ?? parseFlexibleNumber(cert.data?.quantity),
            allocated_volume_m3: 0,
            variance_m3: cert.match?.cert_volume_m3 ?? parseFlexibleNumber(cert.data?.quantity),
            review_note: "Rejected during manual review.",
            candidate_sets: cert.match?.candidate_sets || [],
            linked_rows: [],
          },
          userEmail
        );
        addLog(`Rejected match for ${certTitle(cert)}`, "success");
        setLoading("");
        await loadFromDB();
      } catch (error) {
        setLoading("");
        addLog(`Rejection failed: ${error.message}`, "error");
      }
    },
    [addLog, loadFromDB, persistMatch, userEmail]
  );

  const selectedCert = selected !== null ? certs[selected] : null;
  const stats = {
    totalCerts: certs.length,
    matched: certs.filter((cert) => ["auto_linked", "approved"].includes(cert.match?.status)).length,
    review: certs.filter((cert) => cert.match?.status === "needs_review").length,
    unmatched: certs.filter((cert) => cert.match?.status === "unmatched").length,
    allocatedRows: invoiceRows.filter((row) => row.is_allocated).length,
  };

  return (
    <div
      style={{
        fontFamily: "'DM Sans', 'Segoe UI', sans-serif",
        background: "#020b16",
        minHeight: "100vh",
        color: "#c8dff0",
        display: "flex",
        flexDirection: "column",
      }}
    >
      {pdfPreview ? (
        <div
          style={{
            position: "fixed",
            inset: 0,
            background: "rgba(2,11,22,0.82)",
            zIndex: 1000,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: 24,
          }}
        >
          <div
            style={{
              width: "min(1200px, 96vw)",
              height: "min(92vh, 900px)",
              background: "#04101d",
              border: "1px solid #0d3060",
              borderRadius: 12,
              boxShadow: "0 20px 60px rgba(0,0,0,0.45)",
              display: "flex",
              flexDirection: "column",
              overflow: "hidden",
            }}
          >
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                gap: 12,
                padding: "14px 18px",
                borderBottom: "1px solid #0d2040",
                background: "#030d1a",
              }}
            >
              <div>
                <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 10, letterSpacing: 1 }}>
                  CERTIFICATE PDF
                </div>
                <div style={{ color: "#e0f0ff", fontWeight: 600, fontSize: 14 }}>{pdfPreview.title}</div>
              </div>
              <div style={{ display: "flex", gap: 10 }}>
                <a
                  href={pdfPreview.url}
                  target="_blank"
                  rel="noreferrer"
                  style={{
                    background: "#0a1628",
                    color: "#4a9fd4",
                    padding: "7px 12px",
                    borderRadius: 6,
                    fontFamily: "'Space Mono', monospace",
                    fontSize: 10,
                    border: "1px solid #0d3060",
                    textDecoration: "none",
                  }}
                >
                  OPEN IN NEW TAB
                </a>
                <button
                  className="btn"
                  onClick={() =>
                    setPdfPreview((prev) => {
                      if (prev?.url) URL.revokeObjectURL(prev.url);
                      return null;
                    })
                  }
                  style={{
                    background: "#1a0000",
                    color: "#ff6666",
                    padding: "7px 12px",
                    borderRadius: 6,
                    fontFamily: "'Space Mono', monospace",
                    fontSize: 10,
                    border: "1px solid #ff444433",
                  }}
                >
                  CLOSE
                </button>
              </div>
            </div>
            <iframe
              title={pdfPreview.title}
              src={pdfPreview.url}
              style={{ flex: 1, width: "100%", border: "none", background: "#fff" }}
            />
          </div>
        </div>
      ) : null}

      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');
        * { box-sizing: border-box; }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: #060e1a; }
        ::-webkit-scrollbar-thumb { background: #0d3060; border-radius: 2px; }
        input[type=file] { display: none; }
        .btn { cursor: pointer; border: none; transition: all 0.15s; }
        .btn:hover { filter: brightness(1.1); }
        .tab-btn { background: none; border: none; cursor: pointer; padding: 8px 16px; font-family: 'Space Mono', monospace; font-size: 11px; letter-spacing: 1px; text-transform: uppercase; transition: all 0.15s; }
      `}</style>

      <div
        style={{
          borderBottom: "1px solid #0d2040",
          padding: "16px 28px",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          background: "#030d1a",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
          <div
            style={{
              width: 36,
              height: 36,
              borderRadius: 8,
              background: "linear-gradient(135deg,#0050aa,#00bfff)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              fontSize: 18,
              fontWeight: 900,
              color: "#fff",
            }}
          >
            ✈
          </div>
          <div>
            <div style={{ fontFamily: "'Space Mono', monospace", color: "#00bfff", fontSize: 13, letterSpacing: 2, fontWeight: 700 }}>
              TITAN AVIATION FUELS
            </div>
            <div style={{ color: "#4a7fa0", fontSize: 10, letterSpacing: 1 }}>SAF CERTIFICATE MATCHING SYSTEM</div>
          </div>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          {invoiceImport ? (
            <div style={{ color: "#4a7fa0", fontSize: 10, fontFamily: "'Space Mono', monospace" }}>
              2025 CSV: {invoiceImport.filename} · {invoiceImport.row_count} rows
            </div>
          ) : null}
          {userEmail ? (
            <span style={{ fontFamily: "'Space Mono', monospace", fontSize: 10, color: "#4a7fa0", letterSpacing: 1 }}>
              {userEmail}
            </span>
          ) : null}
          {onLogout ? (
            <button
              className="btn"
              onClick={onLogout}
              style={{
                background: "transparent",
                color: "#4a7fa0",
                padding: "5px 12px",
                borderRadius: 5,
                fontFamily: "'Space Mono', monospace",
                fontSize: 10,
                letterSpacing: 1,
                border: "1px solid #0d3060",
              }}
            >
              SIGN OUT
            </button>
          ) : null}
          {loading ? (
            <div
              style={{
                color: "#00bfff",
                fontFamily: "'Space Mono', monospace",
                fontSize: 11,
                padding: "6px 14px",
                background: "#00bfff11",
                borderRadius: 6,
                border: "1px solid #00bfff33",
              }}
            >
              {loading}
            </div>
          ) : null}
        </div>
      </div>

      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          padding: "12px 28px",
          borderBottom: "1px solid #0d2040",
          background: "#030d1a",
        }}
      >
        <button
          className="btn"
          onClick={() => pdfInputRef.current?.click()}
          style={{
            background: "#0050aa",
            color: "#fff",
            padding: "7px 16px",
            borderRadius: 6,
            fontFamily: "'Space Mono', monospace",
            fontSize: 11,
            letterSpacing: 1,
          }}
        >
          IMPORT PDF(S)
        </button>
        <input ref={pdfInputRef} type="file" accept=".pdf" multiple onChange={(event) => handlePDFUpload(event.target.files)} />

        <button
          className="btn"
          onClick={() => csvInputRef.current?.click()}
          style={{
            background: "#0d2040",
            color: "#c8dff0",
            padding: "7px 16px",
            borderRadius: 6,
            fontFamily: "'Space Mono', monospace",
            fontSize: 11,
            letterSpacing: 1,
            border: "1px solid #1a4080",
          }}
        >
          LOAD 2025 INVOICE CSV
        </button>
        <input ref={csvInputRef} type="file" accept=".csv,text/csv" onChange={(event) => event.target.files[0] && handleCSVUpload(event.target.files[0])} />

        <button
          className="btn"
          onClick={loadFromDB}
          style={{
            background: "#0a1628",
            color: "#4a9fd4",
            padding: "7px 14px",
            borderRadius: 6,
            fontFamily: "'Space Mono', monospace",
            fontSize: 11,
            letterSpacing: 1,
            border: "1px solid #0d3060",
          }}
        >
          RELOAD DB
        </button>

        <div style={{ flex: 1 }} />

        {certs.length ? (
          <>
            <button
              className="btn"
              onClick={analyzeAll}
              style={{
                background: "linear-gradient(135deg,#0050aa,#00bfff)",
                color: "#fff",
                padding: "7px 18px",
                borderRadius: 6,
                fontFamily: "'Space Mono', monospace",
                fontSize: 11,
                letterSpacing: 1,
              }}
            >
              MATCH ALL
            </button>
          </>
        ) : null}
      </div>

      <div style={{ display: "flex", gap: 1, borderBottom: "1px solid #0d2040", background: "#030d1a" }}>
        {[
          { label: "CERTS", val: stats.totalCerts, color: "#00bfff" },
          { label: "MATCHED", val: stats.matched, color: "#00ff9d" },
          { label: "REVIEW", val: stats.review, color: "#ffbb00" },
          { label: "UNMATCHED", val: stats.unmatched, color: "#ff6666" },
          { label: "ALLOCATED ROWS", val: stats.allocatedRows, color: "#4a9fd4" },
        ].map((item) => (
          <div key={item.label} style={{ flex: 1, padding: "8px 20px", borderRight: "1px solid #0d2040" }}>
            <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 18, color: item.color, fontWeight: 700 }}>{item.val}</div>
            <div style={{ color: "#4a7fa0", fontSize: 9, letterSpacing: 1 }}>{item.label}</div>
          </div>
        ))}
      </div>

      <div style={{ display: "flex", borderBottom: "1px solid #0d2040", background: "#030d1a" }}>
        {[
          ["certs", "CERTIFICATES"],
          ["analysis", "MATCHING"],
          ["db", "INVOICE ROWS"],
          ["log", "LOG"],
        ].map(([key, label]) => (
          <button
            key={key}
            className="tab-btn"
            onClick={() => setTab(key)}
            style={{
              color: tab === key ? "#00bfff" : "#4a7fa0",
              borderBottom: tab === key ? "2px solid #00bfff" : "2px solid transparent",
              marginBottom: -1,
            }}
          >
            {label}
          </button>
        ))}
      </div>

      <div style={{ display: "flex", flex: 1, overflow: "hidden", height: "calc(100vh - 220px)" }}>
        {tab === "certs" ? (
          <>
            <div style={{ width: 320, borderRight: "1px solid #0d2040", overflowY: "auto", padding: 12 }}>
              {!certs.length ? (
                <div style={{ padding: "40px 20px", textAlign: "center", color: "#4a7fa0" }}>
                  <div style={{ fontSize: 36, marginBottom: 12 }}>✈</div>
                  <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 11 }}>Import SAF certificate PDFs to begin</div>
                </div>
              ) : (
                certs.map((cert, index) => (
                  <CertCard
                    key={cert.id || index}
                    cert={cert}
                    index={index}
                    selected={selected === index}
                    onSelect={setSelected}
                    onAnalyze={analyzeSingle}
                    onReExtract={reExtractCert}
                    onOpenPdf={openCertificatePdf}
                  />
                ))
              )}
            </div>

            <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
              {selectedCert ? (
                <div>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
                    <div>
                      <div style={{ fontFamily: "'Space Mono', monospace", color: "#00bfff", fontSize: 12 }}>{selectedCert.data?.docType}</div>
                      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                        <div style={{ fontSize: 18, fontWeight: 600, color: "#e0f0ff" }}>{certTitle(selectedCert)}</div>
                        {selectedCert.pdfPath ? (
                          <button
                            className="btn"
                            onClick={() => openCertificatePdf(selectedCert)}
                            style={{
                              background: "#0a1628",
                              color: "#4a9fd4",
                              padding: "4px 10px",
                              borderRadius: 5,
                              fontFamily: "'Space Mono', monospace",
                              fontSize: 10,
                              border: "1px solid #0d3060",
                            }}
                          >
                            OPEN PDF
                          </button>
                        ) : null}
                      </div>
                      <div style={{ color: "#4a7fa0", fontSize: 11 }}>
                        Cert volume: {formatVolume(selectedCert.data?.quantity)} {selectedCert.data?.quantityUnit || "m3"}
                      </div>
                    </div>
                    {selectedCert.match ? <Badge status={selectedCert.match.status} /> : null}
                  </div>

                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}>
                    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
                      <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                        PARTIES
                      </div>
                      <FieldRow label="ISSUER" value={selectedCert.data?.issuer} />
                      <FieldRow label="SAF SUPPLIER" value={selectedCert.data?.safSupplier} highlight />
                      <FieldRow label="RECIPIENT" value={selectedCert.data?.recipient} />
                      <FieldRow label="CONTRACT NR" value={selectedCert.data?.contractNumber} />
                    </div>

                    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
                      <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                        DELIVERY
                      </div>
                      <FieldRow label="DATE DISPATCH" value={selectedCert.data?.dateDispatch} />
                      <FieldRow label="SUPPLY PERIOD" value={selectedCert.data?.supplyPeriod} />
                      <FieldRow label="AIRPORTS" value={selectedCert.data?.deliveryAirports} />
                      <FieldRow label="ISSUED" value={selectedCert.data?.dateIssuance} />
                    </div>

                    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
                      <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                        PRODUCT
                      </div>
                      <FieldRow label="PRODUCT TYPE" value={selectedCert.data?.productType} />
                      <FieldRow label="RAW MATERIAL" value={selectedCert.data?.rawMaterial} />
                      <FieldRow label="ORIGIN" value={selectedCert.data?.rawMaterialOrigin} />
                      <FieldRow label="SAF QUANTITY" value={`${formatVolume(selectedCert.data?.quantity)} ${selectedCert.data?.quantityUnit || "m3"}`} highlight />
                    </div>

                    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
                      <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                        MATCH SUMMARY
                      </div>
                      <FieldRow label="STATUS" value={selectedCert.match?.status?.replace(/_/g, " ")} highlight />
                      <FieldRow label="METHOD" value={selectedCert.match?.match_method} />
                      <FieldRow label="LINKED VOLUME" value={selectedCert.match ? `${formatVolume(selectedCert.match.allocated_volume_m3)} m3` : "—"} />
                      <FieldRow label="VARIANCE" value={selectedCert.match ? `${formatVolume(selectedCert.match.variance_m3)} m3` : "—"} />
                      <FieldRow label="NOTE" value={selectedCert.match?.review_note} />
                    </div>

                    <div style={{ gridColumn: "1 / -1" }}>
                      <MatchRowsTable rows={selectedCert.match?.links || []} />
                    </div>
                  </div>
                </div>
              ) : (
                <div style={{ color: "#4a7fa0", padding: 40, textAlign: "center", fontFamily: "'Space Mono', monospace", fontSize: 11 }}>
                  Select a certificate to view details
                </div>
              )}
            </div>
          </>
        ) : null}

        {tab === "analysis" ? (
          <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
            {!certs.length ? (
              <div style={{ textAlign: "center", color: "#4a7fa0", padding: 60 }}>
                <div style={{ fontSize: 36, marginBottom: 12 }}>🤖</div>
                <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 11 }}>Import certificates and your annual CSV to start matching.</div>
              </div>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
                {certs.map((cert) => (
                  <div
                    key={cert.id}
                    style={{
                      background: "#060e1a",
                      border: "1px solid #0d2040",
                      borderRadius: 10,
                      padding: 20,
                      borderLeft: `4px solid ${
                        cert.match?.status === "auto_linked" || cert.match?.status === "approved"
                          ? "#00ff9d"
                          : cert.match?.status === "needs_review"
                            ? "#ffbb00"
                            : cert.match?.status === "rejected"
                              ? "#ff4444"
                              : "#4a7fa0"
                      }`,
                    }}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 16, marginBottom: 14 }}>
                      <div>
                        <div style={{ fontFamily: "'Space Mono', monospace", color: "#00bfff", fontSize: 11 }}>
                          {cert.data?.docType} · {formatVolume(cert.data?.quantity)} {cert.data?.quantityUnit || "m3"}
                        </div>
                        <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 2 }}>
                          <div style={{ fontSize: 15, fontWeight: 600, color: "#e0f0ff" }}>{certTitle(cert)}</div>
                          {cert.pdfPath ? (
                            <button
                              className="btn"
                              onClick={() => openCertificatePdf(cert)}
                              style={{
                                background: "#0a1628",
                                color: "#4a9fd4",
                                padding: "4px 10px",
                                borderRadius: 5,
                                fontFamily: "'Space Mono', monospace",
                                fontSize: 10,
                                border: "1px solid #0d3060",
                              }}
                            >
                              OPEN PDF
                            </button>
                          ) : null}
                        </div>
                        <div style={{ color: "#4a7fa0", fontSize: 11 }}>{cert.match?.review_note || "Run matching for this certificate."}</div>
                      </div>
                      <div style={{ display: "flex", flexDirection: "column", gap: 6, alignItems: "flex-end" }}>
                        {cert.match ? <Badge status={cert.match.status} /> : null}
                        {cert.match?.match_method ? <Badge status={cert.match.match_method} /> : null}
                      </div>
                    </div>

                    {cert.match?.links?.length ? (
                      <div style={{ marginBottom: 14 }}>
                        <MatchRowsTable rows={cert.match.links} title="LOCKED INVOICE ROWS" />
                      </div>
                    ) : null}

                    {cert.match?.status === "needs_review" ? (
                      <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
                        <div
                          style={{
                            background: "#111a28",
                            border: "1px solid #1a304f",
                            borderRadius: 8,
                            padding: "10px 12px",
                            color: "#c8dff0",
                            fontSize: 12,
                            lineHeight: 1.5,
                          }}
                        >
                          Each candidate below is an alternative invoice group. Approving one group links all rows in that group to this certificate. The other candidate groups are not linked.
                        </div>
                        {(cert.match.candidate_sets || []).map((candidate, candidateIndex) => {
                          const isExpanded = expandedCandidates[`${cert.id}-${candidateIndex}`];
                          return (
                            <div key={`${cert.id}-${candidateIndex}`} style={{ background: "#091220", borderRadius: 8, border: "1px solid #1a304f", padding: 14 }}>
                              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12 }}>
                                <div>
                                  <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 11 }}>
                                    Candidate {candidateIndex + 1} · {candidate.match_method}
                                  </div>
                                  <div style={{ color: "#c8dff0", fontSize: 12 }}>
                                    {formatVolume(candidate.total_volume_m3)} m3 linked · variance {formatVolume(candidate.variance_m3)} m3
                                  </div>
                                </div>
                                <div style={{ display: "flex", gap: 8 }}>
                                  <button
                                    className="btn"
                                    onClick={() => approveCandidate(cert, candidateIndex)}
                                    style={{
                                      background: "#003322",
                                      color: "#00ff9d",
                                      padding: "6px 12px",
                                      borderRadius: 5,
                                      fontFamily: "'Space Mono', monospace",
                                      fontSize: 10,
                                      border: "1px solid #00ff9d44",
                                    }}
                                  >
                                    APPROVE THIS GROUP
                                  </button>
                                  <button
                                    className="btn"
                                    onClick={() =>
                                      setExpandedCandidates((prev) => ({
                                        ...prev,
                                        [`${cert.id}-${candidateIndex}`]: !prev[`${cert.id}-${candidateIndex}`],
                                      }))
                                    }
                                    style={{
                                      background: "#0a1628",
                                      color: "#4a9fd4",
                                      padding: "6px 12px",
                                      borderRadius: 5,
                                      fontFamily: "'Space Mono', monospace",
                                      fontSize: 10,
                                      border: "1px solid #0d3060",
                                    }}
                                  >
                                    {isExpanded ? "HIDE ROWS" : "SHOW ROWS"}
                                  </button>
                                </div>
                              </div>
                              {isExpanded ? (
                                <div style={{ marginTop: 12 }}>
                                  <MatchRowsTable rows={candidate.rows} title="CANDIDATE ROWS" emptyText="No rows in this candidate." />
                                </div>
                              ) : null}
                            </div>
                          );
                        })}
                        <div>
                          <button
                            className="btn"
                            onClick={() => rejectMatch(cert)}
                            style={{
                              background: "#1a0000",
                              color: "#ff6666",
                              padding: "6px 12px",
                              borderRadius: 5,
                              fontFamily: "'Space Mono', monospace",
                              fontSize: 10,
                              border: "1px solid #ff444433",
                            }}
                          >
                            REJECT
                          </button>
                        </div>
                      </div>
                    ) : null}

                    {!cert.match ? (
                      <button
                        className="btn"
                        onClick={() => analyzeSingle(certs.findIndex((item) => item.id === cert.id))}
                        style={{
                          background: "linear-gradient(135deg,#0050aa,#00bfff)",
                          color: "#fff",
                          padding: "7px 14px",
                          borderRadius: 6,
                          fontFamily: "'Space Mono', monospace",
                          fontSize: 11,
                          letterSpacing: 1,
                        }}
                      >
                        MATCH NOW
                      </button>
                    ) : null}
                  </div>
                ))}
              </div>
            )}
          </div>
        ) : null}

        {tab === "db" ? (
          <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
            {!invoiceRows.length ? (
              <div style={{ textAlign: "center", color: "#4a7fa0", padding: 60 }}>
                <div style={{ fontSize: 36, marginBottom: 12 }}>📊</div>
                <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 11 }}>Load the 2025 invoice CSV to create row-level matches.</div>
              </div>
            ) : (
              <div style={{ overflowX: "auto" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "'Space Mono', monospace", fontSize: 10 }}>
                  <thead>
                    <tr style={{ borderBottom: "2px solid #0d3060" }}>
                      {["STATUS", "CSV ROW", "INVOICE", "CUSTOMER", "UPLIFT DATE", "IATA", "ICAO", "COUNTRY", "SUPPLIER", "VOL M3"].map(
                        (header) => (
                          <th key={header} style={{ padding: "8px 12px", textAlign: "left", color: "#00bfff", whiteSpace: "nowrap" }}>
                            {header}
                          </th>
                        )
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    {invoiceRows.map((row, index) => (
                      <tr key={row.id || index} style={{ borderBottom: "1px solid #0d2040", background: index % 2 === 0 ? "#060e1a" : "#030d1a" }}>
                        <td style={{ padding: "7px 12px" }}>
                          <Badge status={row.is_allocated ? "allocated" : "free"} />
                        </td>
                        <td style={{ padding: "7px 12px", color: "#4a9fd4" }}>{row.row_number}</td>
                        <td style={{ padding: "7px 12px", color: "#e0f0ff" }}>{row.invoice_no || "—"}</td>
                        <td style={{ padding: "7px 12px", color: "#c8dff0" }}>{row.customer || "—"}</td>
                        <td style={{ padding: "7px 12px", color: "#888" }}>{row.uplift_date || "—"}</td>
                        <td style={{ padding: "7px 12px", color: "#c8dff0" }}>{row.iata || "—"}</td>
                        <td style={{ padding: "7px 12px", color: "#c8dff0" }}>{row.icao || "—"}</td>
                        <td style={{ padding: "7px 12px", color: "#c8dff0" }}>{row.country || "—"}</td>
                        <td style={{ padding: "7px 12px", color: "#c8dff0" }}>{row.supplier || "—"}</td>
                        <td style={{ padding: "7px 12px", color: "#00ff9d", fontWeight: 700 }}>{formatVolume(row.vol_m3)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        ) : null}

        {tab === "log" ? (
          <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
            <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 11 }}>
              {!log.length ? (
                <div style={{ color: "#4a7fa0" }}>No activity yet</div>
              ) : (
                [...log].reverse().map((entry, index) => (
                  <div key={`${entry.ts}-${index}`} style={{ display: "flex", gap: 12, padding: "5px 0", borderBottom: "1px solid #0d2040" }}>
                    <span style={{ color: "#2a5070", width: 80, flexShrink: 0 }}>{entry.ts}</span>
                    <span
                      style={{
                        color: entry.type === "success" ? "#00ff9d" : entry.type === "error" ? "#ff4444" : "#4a7fa0",
                      }}
                    >
                      {entry.msg}
                    </span>
                  </div>
                ))
              )}
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
