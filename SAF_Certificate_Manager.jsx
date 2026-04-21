import React, { useState, useEffect, useCallback, useRef } from "react";
import { supabase } from "./src/supabase.js";
import { DEFAULT_CERTIFICATE_CLASSIFICATION, deriveCertificateClassification, mergeAirportIdentityEntries } from "./src/certificateClassification.js";
import { buildCertificateAllocationUnitDrafts, buildNormalizedCertificateView } from "./src/certificateNormalization.js";
import {
  collectApprovedClientCertificateGroups,
  formatClientCertificateDate,
  formatClientCertificateMonth,
  formatClientCertificateVolume,
} from "./src/clientCertificates.js";
import { buildCoverageData } from "./src/coverageAggregation.js";

const EXTRACTION_FUNCTION = "extract-certificate";

const MATCH_TOLERANCE = 0.002;
const SMALL_CERTIFICATE_CUTOFF_M3 = 5;
const MAX_SEARCH_RESULTS = 8;
const MAX_ROW_DP_POOL_SIZE = 80;
const ROW_DP_SCALE = 10000;
const MAX_ROW_DP_BUCKET_SIZE = 4;
const MAX_AGGREGATE_COMBINATION_SIZE = 24;
const MAX_AGGREGATE_POOL_SIZE = 60;
const MAX_GROUP_CANDIDATES_PER_CERT = 16;
const MAX_GROUP_ASSIGNMENT_SOLUTIONS = 48;

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
  saf_vol_m3: ["saf vol 2% per m3", "saf vol per m3", "saf volume m3", "saf vol m3"],
};

const MAX_IMPORT_VALIDATION_ERRORS = 12;

const CANONICAL_AIRPORTS = [
  {
    iata: "ALC",
    icao: "LEAL",
    label: "Alicante",
    aliases: ["Alicante", "Alicante Airport", "Aeropuerto de Alicante", "Aeropuerto de Alicante Elche"],
  },
  {
    iata: "AGP",
    icao: "LEMG",
    label: "Malaga",
    aliases: ["Aeropuerto de Malaga", "Aeropuerto de Malaga Costa del Sol", "Malaga Airport", "Malaga", "Málaga", "Aeropuerto de Málaga"],
  },
  {
    iata: "BCN",
    icao: "LEBL",
    label: "Barcelona",
    aliases: ["Barcelona Airport", "Barcelona", "Aeropuerto de Barcelona", "Barcelona El Prat"],
  },
  {
    iata: "BIO",
    icao: "LEBB",
    label: "Bilbao",
    aliases: ["Aeropuerto de Bilbao", "Bilbao Airport", "Bilbao"],
  },
  {
    iata: "CDG",
    icao: "LFPG",
    label: "Paris Charles de Gaulle",
    aliases: [
      "Charles de Gaulle",
      "Charles de Gaulle Roissy",
      "Paris Charles de Gaulle",
      "Roissy Charles de Gaulle",
      "CDG LFPG Charles de Gaulle Roissy France",
    ],
  },
  {
    iata: "EIN",
    icao: "EHEH",
    label: "Eindhoven",
    aliases: ["Eindhoven", "Eindhoven Airport"],
  },
  {
    iata: "FAO",
    icao: "LPFR",
    label: "Faro",
    aliases: ["Faro", "Faro Airport"],
  },
  {
    iata: "GRO",
    icao: "LEGE",
    label: "Girona",
    aliases: ["Aeropuerto de Girona", "Girona Airport", "Girona", "Girona Costa Brava", "Aeropuerto de Girona Costa Brava", "Gerona", "Gerona Airport", "Aeropuerto de Gerona"],
  },
  {
    iata: "GRX",
    icao: "LEGR",
    label: "Granada",
    aliases: ["Aeropuerto de Granada", "Granada Airport", "Granada"],
  },
  {
    iata: "KUN",
    icao: "EYKA",
    label: "Kaunas",
    aliases: ["Kaunas", "Kaunas Intl", "Kaunas International", "Kaunas Int l"],
  },
  {
    iata: "LIS",
    icao: "LPPT",
    label: "Lisbon",
    aliases: ["Lisbon", "Lisbon Airport", "Lisboa", "Aeroporto de Lisboa"],
  },
  {
    iata: "MAD",
    icao: "LEMD",
    label: "Madrid",
    aliases: ["Madrid", "Madrid Airport", "Adolfo Suarez Madrid Barajas", "Barajas", "Madrid-Barajas", "Madrid Barajas"],
  },
  {
    iata: "MJV",
    icao: "LELC",
    label: "Murcia",
    aliases: ["Murcia", "Murcia Airport", "Murcia San Javier"],
  },
  {
    iata: "OVD",
    icao: "LEAS",
    label: "Asturias",
    aliases: ["Aeropuerto de Asturias", "Asturias Airport", "Asturias"],
  },
  {
    iata: "OPO",
    icao: "LPPR",
    label: "Porto",
    aliases: ["Porto", "Porto Airport", "Francisco Sa Carneiro", "Aeroporto do Porto"],
  },
  {
    iata: "PMI",
    icao: "LEPA",
    label: "Palma de Mallorca",
    aliases: ["Aeropuerto de Palma de Mallorca", "Palma de Mallorca Airport", "Palma de Mallorca", "Palma Airport"],
  },
  {
    iata: "PRG",
    icao: "LKPR",
    label: "Prague",
    aliases: ["Prague", "Prague Airport", "Vaclav Havel Prague"],
  },
  {
    iata: "SCQ",
    icao: "LEST",
    label: "Santiago de Compostela",
    aliases: ["Santiago de Compostela", "Santiago Airport", "Aeropuerto de Santiago de Compostela"],
  },
  {
    iata: "SDR",
    icao: "LEXJ",
    label: "Santander",
    aliases: ["Santander", "Santander Airport", "Aeropuerto de Santander", "Santader", "Aeropuerto de Santader"],
  },
  {
    iata: "SVQ",
    icao: "LEZL",
    label: "Seville",
    aliases: ["Seville", "Seville Airport", "Sevilla", "Aeropuerto de Sevilla", "San Pablo", "Aeropuerto de San Pablo", "Aeropuerto de San Pablo Sevilla"],
  },
  {
    iata: "VGO",
    icao: "LEVX",
    label: "Vigo",
    aliases: ["Aeropuerto de Vigo", "Vigo Airport", "Vigo"],
  },
  {
    iata: "VLC",
    icao: "LEVC",
    label: "Valencia",
    aliases: ["Valencia", "Valencia Airport", "Aeropuerto de Valencia"],
  },
  {
    iata: "XRY",
    icao: "LEJR",
    label: "Jerez",
    aliases: ["Aeropuerto de Jerez", "Jerez Airport", "Jerez"],
  },
  {
    iata: "ZAZ",
    icao: "LEZG",
    label: "Zaragoza",
    aliases: ["Aeropuerto de Zaragoza", "Zaragoza Airport", "Zaragoza"],
  },
];

const AIRPORT_ALIAS_LOOKUP = new Map();
const AIRPORT_CODE_LOOKUP = new Map();

for (const airport of CANONICAL_AIRPORTS) {
  AIRPORT_CODE_LOOKUP.set(airport.iata, airport);
  AIRPORT_CODE_LOOKUP.set(airport.icao, airport);
  AIRPORT_ALIAS_LOOKUP.set(normalizeText(airport.label), airport);
  for (const alias of airport.aliases) {
    AIRPORT_ALIAS_LOOKUP.set(normalizeText(alias), airport);
  }
}

function normalizeText(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function compactText(value) {
  return normalizeText(value).replace(/\s+/g, "");
}

const MONTH_NAME_TO_INDEX = new Map([
  ["january", 0],
  ["february", 1],
  ["march", 2],
  ["april", 3],
  ["may", 4],
  ["june", 5],
  ["july", 6],
  ["august", 7],
  ["september", 8],
  ["october", 9],
  ["november", 10],
  ["december", 11],
]);

function startOfMonthUTC(year, monthIndex) {
  return new Date(Date.UTC(year, monthIndex, 1));
}

function endOfMonthUTC(year, monthIndex) {
  return new Date(Date.UTC(year, monthIndex + 1, 0));
}

function formatYearMonth(date) {
  return date.toISOString().slice(0, 7);
}

function buildMonthCoverageFromDate(date, source, matchingEvidence) {
  if (!date) return null;
  const start = startOfMonthUTC(date.getUTCFullYear(), date.getUTCMonth());
  const end = endOfMonthUTC(date.getUTCFullYear(), date.getUTCMonth());
  return {
    matchingMode: "monthly-pos",
    coverageGranularity: "month",
    coverageMonth: formatYearMonth(start),
    coverageStart: toISODate(start),
    coverageEnd: toISODate(end),
    coverageSource: source,
    matchingEvidence,
  };
}

function buildDayCoverageFromDate(date, source, matchingEvidence) {
  if (!date) return null;
  const iso = toISODate(date);
  return {
    matchingMode: "uplift-pos",
    coverageGranularity: "day",
    coverageMonth: formatYearMonth(date),
    coverageStart: iso,
    coverageEnd: iso,
    coverageSource: source,
    matchingEvidence,
  };
}

function normalizeMatchingMode(value, certKind) {
  const text = normalizeText(value);
  if (!text) return certKind === "poc" ? "poc" : "";
  if (text.includes("monthly")) return "monthly-pos";
  if (text.includes("uplift") || text.includes("shipment")) return "uplift-pos";
  if (text.includes("poc")) return "poc";
  if (certKind === "poc") return "poc";
  return "";
}

function parseCoverageMonth(value) {
  const normalized = normalizeMonthValue(value);
  if (!normalized) return null;
  const [year, month] = normalized.split("-").map(Number);
  if (!Number.isFinite(year) || !Number.isFinite(month)) return null;
  return buildMonthCoverageFromDate(startOfMonthUTC(year, month - 1), "extracted-coverage", "extracted-month");
}

function getCertificateAdditionalInfoTexts(data) {
  const preferredKeys = ["additionalInformation", "additionalInfo", "remarks", "notes", "note", "comment", "comments"];
  const values = [];

  for (const key of preferredKeys) {
    if (typeof data?.[key] === "string" && data[key].trim()) values.push(data[key].trim());
  }
  for (const [key, value] of Object.entries(data || {})) {
    if (preferredKeys.includes(key) || typeof value !== "string") continue;
    if (!/saf delivery|delivery [a-z]+ \d{4}/i.test(value)) continue;
    values.push(value.trim());
  }

  return dedupeBy(values, (value) => value);
}

const QUARTER_MONTH_OFFSETS = { 1: [0, 2], 2: [3, 5], 3: [6, 8], 4: [9, 11] };

function inferQuarterCoverage(text) {
  const str = String(text ?? "").trim();
  if (!str) return null;
  const digitFirst = str.match(/\b([1-4])Q\s*['''\u2019]?(\d{2,4})\b/i);
  const qFirst = str.match(/\bQ([1-4])\s*[-/]?\s*['''\u2019]?(\d{2,4})\b/i);
  const match = digitFirst || qFirst;
  if (!match) return null;
  const quarter = Number(match[1]);
  const rawYear = match[2];
  const year = rawYear.length === 2 ? 2000 + Number(rawYear) : Number(rawYear);
  if (!Number.isFinite(year) || year < 2000 || year > 2100) return null;
  const [startMonth, endMonth] = QUARTER_MONTH_OFFSETS[quarter];
  const start = startOfMonthUTC(year, startMonth);
  const end = endOfMonthUTC(year, endMonth);
  return {
    matchingMode: "monthly-pos",
    coverageGranularity: "quarter",
    coverageMonth: formatYearMonth(start),
    coverageStart: toISODate(start),
    coverageEnd: toISODate(end),
    coverageSource: "additional-information",
    matchingEvidence: "additional-information-quarter",
    quarterLabel: `Q${quarter} ${year}`,
  };
}

function inferMonthlyCoverage(data) {
  const textSources = [
    ...getCertificateAdditionalInfoTexts(data),
    typeof data?.filename === "string" ? data.filename : "",
  ].filter(Boolean);

  for (const text of textSources) {
    const quarterResult = inferQuarterCoverage(text);
    if (quarterResult) return quarterResult;

    const match = text.match(/\bSAF\s+Delivery\s+([A-Za-z]+)\s+(\d{4})\b/i) || text.match(/\bDelivery\s+([A-Za-z]+)\s+(\d{4})\b/i);
    if (!match) continue;

    const monthIndex = MONTH_NAME_TO_INDEX.get(normalizeText(match[1]));
    const year = Number(match[2]);
    if (!Number.isInteger(monthIndex) || !Number.isFinite(year)) continue;

    const start = startOfMonthUTC(year, monthIndex);
    const end = endOfMonthUTC(year, monthIndex);
    return {
      matchingMode: "monthly-pos",
      coverageGranularity: "month",
      coverageMonth: formatYearMonth(start),
      coverageStart: toISODate(start),
      coverageEnd: toISODate(end),
      coverageSource: "additional-information",
      matchingEvidence: "additional-information-month",
    };
  }

  return null;
}

function extractAirportHints(data) {
  const hints = [];
  const knownCodePattern = /[A-Z]{3,4}/g;

  const pushCodesFrom = (value) => {
    const tokens = String(value ?? "").toUpperCase().match(knownCodePattern) || [];
    for (const token of tokens) {
      if (AIRPORT_CODE_LOOKUP.has(token)) hints.push(token);
    }
  };

  pushCodesFrom(data?.uniqueNumber);
  for (const text of getCertificateAdditionalInfoTexts(data)) {
    pushCodesFrom(text);
  }

  return dedupeBy(hints, (value) => value);
}

function hasStrongUpliftEvidence(data) {
  const explicitMode = normalizeMatchingMode(data?.matchingMode, getCertKind(data));
  if (explicitMode === "uplift-pos") return true;

  const texts = [
    ...getCertificateAdditionalInfoTexts(data),
    String(data?.contractNumber || ""),
  ]
    .join(" ")
    .toLowerCase();

  return /delivery ticket|shipment|single uplift|single delivery|truck|bill of lading/.test(texts);
}

function inferMatchingMetadata(data) {
  const certKind = getCertKind(data);
  if (certKind === "poc") {
    return {
      matchingMode: "poc",
      coverageGranularity: data?.coverageGranularity || "period",
      coverageMonth: data?.coverageMonth || "",
      coverageStart: data?.coverageStart || "",
      coverageEnd: data?.coverageEnd || "",
      coverageSource: data?.coverageSource || "",
      matchingEvidence: data?.matchingEvidence || "",
    };
  }

  if (certKind !== "pos") {
    return {
      matchingMode: data?.matchingMode || "",
      coverageGranularity: data?.coverageGranularity || "",
      coverageMonth: data?.coverageMonth || "",
      coverageStart: data?.coverageStart || "",
      coverageEnd: data?.coverageEnd || "",
      coverageSource: data?.coverageSource || "",
      matchingEvidence: data?.matchingEvidence || "",
    };
  }

  const explicitMode = normalizeMatchingMode(data?.matchingMode, certKind);
  const explicitMonthCoverage = parseCoverageMonth(data?.coverageMonth);
  if (explicitMode === "monthly-pos" && explicitMonthCoverage) {
    return {
      ...explicitMonthCoverage,
      matchingMode: "monthly-pos",
      coverageSource: data?.coverageSource || explicitMonthCoverage.coverageSource,
      matchingEvidence: data?.matchingEvidence || "extracted-month",
    };
  }

  const explicitRangeStart = parseDateValue(data?.coverageStart);
  const explicitRangeEnd = parseDateValue(data?.coverageEnd);
  if (explicitMode && explicitRangeStart && explicitRangeEnd) {
    return {
      matchingMode: explicitMode,
      coverageGranularity:
        data?.coverageGranularity || (explicitMode === "monthly-pos" ? "month" : explicitRangeStart.getTime() === explicitRangeEnd.getTime() ? "day" : "period"),
      coverageMonth: data?.coverageMonth || formatYearMonth(explicitRangeStart),
      coverageStart: toISODate(explicitRangeStart),
      coverageEnd: toISODate(explicitRangeEnd),
      coverageSource: data?.coverageSource || "extracted-range",
      matchingEvidence: data?.matchingEvidence || "extracted-range",
    };
  }

  const monthlyCoverage = inferMonthlyCoverage(data);
  if (monthlyCoverage) return monthlyCoverage;

  const canonicalCodes = dedupeBy(
    (data?.canonicalAirports || [])
      .map((entry) => canonicalAirportCode(entry))
      .filter(Boolean),
    (value) => value
  );
  const dispatchDate = parseDateValue(data?.dateDispatch);

  if (!hasStrongUpliftEvidence(data) && canonicalCodes.length === 1 && dispatchDate) {
    return buildMonthCoverageFromDate(dispatchDate, "single-airport-pos-default", "single-airport-pos-default");
  }

  if (dispatchDate) {
    return buildDayCoverageFromDate(dispatchDate, "dispatch-date", "dispatch-date");
  }

  return {
    matchingMode: "uplift-pos",
    coverageGranularity: data?.coverageGranularity || "",
    coverageMonth: data?.coverageMonth || "",
    coverageStart: data?.coverageStart || "",
    coverageEnd: data?.coverageEnd || "",
    coverageSource: data?.coverageSource || "",
    matchingEvidence: data?.matchingEvidence || "",
  };
}

function normalizeMonthValue(value) {
  const text = String(value ?? "").trim();
  if (!text) return "";
  if (/^\d{4}-\d{2}$/.test(text)) return text;
  const date = parseDateValue(text);
  return date ? date.toISOString().slice(0, 7) : "";
}

function normalizeDateForDB(value) {
  if (!String(value ?? "").trim()) return null;
  return toISODate(value) || null;
}

function normalizeVolumeNumber(value) {
  const parsed = typeof value === "number" ? value : parseFlexibleNumber(value);
  if (!Number.isFinite(parsed)) return null;
  return Number(parsed.toFixed(6));
}

function summarizeInvoiceRowState(row, allocatedOverride = null) {
  const baseVolume = normalizeVolumeNumber(row?.saf_vol_m3);
  const allocatedTotal =
    allocatedOverride !== null && allocatedOverride !== undefined
      ? normalizeVolumeNumber(allocatedOverride) || 0
      : normalizeVolumeNumber(row?.allocated_m3_total) || 0;

  if (!Number.isFinite(baseVolume) || baseVolume <= 0) {
    return {
      baseVolume: null,
      allocatedTotal,
      remainingVolume: null,
      isAllocable: false,
      isAllocated: false,
      status: "invalid",
      validationNote: "SAF volume is missing or invalid.",
    };
  }

  const remainingVolume = Number(Math.max(0, baseVolume - allocatedTotal).toFixed(6));
  if (row?.is_duplicate) {
    return {
      baseVolume,
      allocatedTotal,
      remainingVolume,
      isAllocable: false,
      isAllocated: false,
      status: "duplicate",
      validationNote: "Duplicate invoice row. Excluded from allocable SAF matching.",
    };
  }

  return {
    baseVolume,
    allocatedTotal,
    remainingVolume,
    isAllocable: remainingVolume > MATCH_TOLERANCE,
    isAllocated: remainingVolume <= MATCH_TOLERANCE,
    status: remainingVolume <= MATCH_TOLERANCE ? "allocated" : allocatedTotal > 0 ? "partial" : "free",
    validationNote: "",
  };
}

function getRowAvailableVolume(row) {
  const explicitAllocated = normalizeVolumeNumber(row?.allocated_m3);
  if (Number.isFinite(explicitAllocated) && explicitAllocated > 0) return explicitAllocated;

  const summary = summarizeInvoiceRowState(row);
  return summary.isAllocable ? summary.remainingVolume : null;
}

function isRowAvailable(row) {
  const summary = summarizeInvoiceRowState(row);
  return summary.isAllocable;
}

function applyRowAllocation(row, amount) {
  const current = summarizeInvoiceRowState(row);
  if (!Number.isFinite(current.baseVolume)) {
    throw new Error(`Invoice row ${row?.row_number || row?.id || "unknown"} has an invalid SAF volume.`);
  }
  if (row?.is_duplicate) {
    throw new Error(`Invoice row ${row?.row_number || row?.id || "unknown"} is flagged as duplicate and cannot be auto-allocated.`);
  }

  const nextAllocated = Number((current.allocatedTotal + amount).toFixed(6));
  const next = summarizeInvoiceRowState(row, nextAllocated);
  row.allocated_m3_total = nextAllocated;
  row.remaining_m3 = next.remainingVolume;
  row.is_allocated = next.isAllocated;
  row.allocation_status = next.status;
  row.validation_note = next.validationNote;
}

function releaseRowAllocation(row, amount) {
  const current = summarizeInvoiceRowState(row);
  const nextAllocated = Number(Math.max(0, current.allocatedTotal - amount).toFixed(6));
  const next = summarizeInvoiceRowState(row, nextAllocated);
  row.allocated_m3_total = nextAllocated;
  row.remaining_m3 = next.remainingVolume;
  row.is_allocated = next.isAllocated;
  row.allocation_status = next.status;
  row.validation_note = next.validationNote;
}

function hydrateInvoiceRowsWithAllocations(rows, links, certs) {
  const totalsByRowId = new Map();
  const certsByRowId = new Map();
  for (const link of links || []) {
    const existing = totalsByRowId.get(link.invoice_row_id) || 0;
    totalsByRowId.set(link.invoice_row_id, Number((existing + Number(link.allocated_m3 || 0)).toFixed(6)));
    const existingCerts = certsByRowId.get(link.invoice_row_id) || [];
    if (!existingCerts.includes(link.certificate_id)) existingCerts.push(link.certificate_id);
    certsByRowId.set(link.invoice_row_id, existingCerts);
  }

  const certById = new Map((certs || []).map((c) => [c.id, c]));

  return (rows || []).map((row) => {
    const allocated = totalsByRowId.get(row.id) || 0;
    const summary = summarizeInvoiceRowState(row, allocated);
    const linkedCertIds = certsByRowId.get(row.id) || [];
    const linkedCertNumbers = linkedCertIds.map((id) => certById.get(id)?.unique_number || id).filter(Boolean);
    return {
      ...row,
      allocated_m3_total: summary.allocatedTotal,
      remaining_m3: summary.remainingVolume,
      is_allocated: summary.isAllocated,
      allocation_status: summary.status,
      // Preserve DB-stored validation_note (e.g. "no cert found", "supplier over-declaration")
      // unless the summary has a more critical system-level note (invalid saf, duplicate).
      validation_note: summary.validationNote || row.validation_note || "",
      linked_cert_ids: linkedCertIds,
      linked_cert_numbers: linkedCertNumbers,
    };
  });
}

function volumesMatch(left, right, tolerance = MATCH_TOLERANCE) {
  if (!Number.isFinite(left) || !Number.isFinite(right)) return false;
  return Math.abs(left - right) <= tolerance;
}

function dedupeBy(items, getKey) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const key = getKey(item);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

function isIATACode(value) {
  return /^[A-Z]{3}$/.test(String(value ?? "").trim().toUpperCase());
}

function isICAOCode(value) {
  return /^[A-Z]{4}$/.test(String(value ?? "").trim().toUpperCase());
}

function canonicalAirportCode(entry) {
  if (!entry) return "";
  return String(entry.iata || entry.icao || "").trim().toUpperCase();
}

function buildCanonicalAirport(raw, airport) {
  return {
    raw: String(raw ?? "").trim(),
    iata: airport?.iata || "",
    icao: airport?.icao || "",
    label: airport?.label || String(raw ?? "").trim(),
  };
}

function deriveAirportFromCodes(codeTokens, raw) {
  if (!codeTokens.length) return null;

  let airport = null;
  let iata = "";
  let icao = "";
  for (const token of codeTokens) {
    const known = AIRPORT_CODE_LOOKUP.get(token);
    if (known) {
      airport = known;
      iata = known.iata;
      icao = known.icao;
      continue;
    }
    if (!iata && isIATACode(token)) iata = token;
    if (!icao && isICAOCode(token)) icao = token;
  }

  if (airport) return buildCanonicalAirport(raw, airport);
  if (!iata && !icao) return null;
  return {
    raw: String(raw ?? "").trim(),
    iata,
    icao,
    label: String(raw ?? "").trim() || iata || icao,
  };
}

function canonicalizeAirportValue(raw) {
  const text = String(raw ?? "").trim();
  if (!text) return null;

  const exactAlias = AIRPORT_ALIAS_LOOKUP.get(normalizeText(text));
  if (exactAlias) return buildCanonicalAirport(text, exactAlias);

  const trimmedHead = text.split(/\s+-\s+/)[0].trim();
  const headAlias = AIRPORT_ALIAS_LOOKUP.get(normalizeText(trimmedHead));
  if (headAlias) return buildCanonicalAirport(text, headAlias);

  const codeTokens = [...new Set((text.toUpperCase().match(/\b[A-Z]{3,4}\b/g) || []).filter(Boolean))];
  const fromCodes = deriveAirportFromCodes(codeTokens, text);
  if (fromCodes) return fromCodes;

  return {
    raw: text,
    iata: "",
    icao: "",
    label: text,
  };
}

function normalizeCertificateAirports(data, context = {}) {
  if (!data || typeof data !== "object") return { data, changed: false };

  const next = { ...data };
  const initialSourceData = context?.filename ? { ...next, filename: context.filename } : next;
  next.additionalInformation = String(
    next.additionalInformation ??
      next.additionalInfo ??
      next.remarks ??
      next.notes ??
      next.note ??
      next.comment ??
      next.comments ??
      ""
  ).trim();
  const canonicalAirports = [];
  const addCanonical = (entry) => {
    if (!entry?.raw && !entry?.iata && !entry?.icao) return;
    canonicalAirports.push({
      raw: entry.raw || "",
      iata: entry.iata || "",
      icao: entry.icao || "",
      label: entry.label || entry.raw || entry.iata || entry.icao || "",
    });
  };

  const deliveryCanonicals = splitAirportRefs(data.deliveryAirports)
    .map((value) => canonicalizeAirportValue(value))
    .filter(Boolean);
  deliveryCanonicals.forEach(addCanonical);

  extractAirportHints(initialSourceData)
    .map((value) => canonicalizeAirportValue(value))
    .filter(Boolean)
    .forEach(addCanonical);

  const physicalCanonical = canonicalizeAirportValue(data.physicalDeliveryAirport);
  if (physicalCanonical) addCanonical(physicalCanonical);

  const airportVolumes = (data.airportVolumes || []).map((item) => {
    const airportCanonical = canonicalizeAirportValue(item?.airport);
    if (airportCanonical) addCanonical(airportCanonical);
    return {
      ...item,
      airportCanonical: airportCanonical || null,
    };
  });

  const monthlyVolumes = (data.monthlyVolumes || []).map((item) => {
    const airportCanonical = canonicalizeAirportValue(item?.airport);
    if (airportCanonical) addCanonical(airportCanonical);
    return {
      ...item,
      airportCanonical: airportCanonical || null,
    };
  });

  next.canonicalAirports = mergeAirportIdentityEntries(canonicalAirports);
  next.physicalDeliveryAirportCanonical = physicalCanonical || null;
  next.airportVolumes = airportVolumes;
  next.monthlyVolumes = monthlyVolumes;
  const classification = deriveCertificateClassification(next, context);
  next.document_family = classification.document_family;
  next.matching_mode = classification.matching_mode;
  next.classification_confidence = classification.classification_confidence;
  next.review_required = classification.review_required;
  next.classification_reason = classification.classification_reason;
  next.supported_boolean = classification.supported_boolean;
  next.processing_mode = classification.processing_mode;
  next.support_reason = classification.support_reason;
  next.matchingMode = classification.matching_mode;
  next.coverageGranularity = classification.coverageGranularity;
  next.coverageMonth = classification.coverageMonth;
  next.coverageStart = classification.coverageStart;
  next.coverageEnd = classification.coverageEnd;
  next.coverageSource = classification.coverageSource;
  next.matchingEvidence = classification.matchingEvidence;

  return {
    data: next,
    classification,
    changed: JSON.stringify(next) !== JSON.stringify(data),
  };
}

function pickCertificateClassification(source = {}) {
  const numericConfidence =
    typeof source.classification_confidence === "number"
      ? source.classification_confidence
      : typeof source.classification_confidence === "string"
        ? parseFlexibleNumber(source.classification_confidence)
        : typeof source.data?.classification_confidence === "number"
          ? source.data.classification_confidence
          : typeof source.data?.classification_confidence === "string"
            ? parseFlexibleNumber(source.data.classification_confidence)
            : null;

  return {
    document_family: source.document_family ?? source.data?.document_family ?? DEFAULT_CERTIFICATE_CLASSIFICATION.document_family,
    matching_mode: source.matching_mode ?? source.data?.matching_mode ?? source.data?.matchingMode ?? DEFAULT_CERTIFICATE_CLASSIFICATION.matching_mode,
    classification_confidence: numericConfidence ?? DEFAULT_CERTIFICATE_CLASSIFICATION.classification_confidence,
    review_required:
      typeof source.review_required === "boolean"
        ? source.review_required
        : typeof source.data?.review_required === "boolean"
          ? source.data.review_required
          : DEFAULT_CERTIFICATE_CLASSIFICATION.review_required,
    classification_reason:
      source.classification_reason ??
      source.data?.classification_reason ??
      DEFAULT_CERTIFICATE_CLASSIFICATION.classification_reason,
    supported_boolean:
      typeof source.supported_boolean === "boolean"
        ? source.supported_boolean
        : typeof source.data?.supported_boolean === "boolean"
          ? source.data.supported_boolean
          : DEFAULT_CERTIFICATE_CLASSIFICATION.supported_boolean,
    processing_mode:
      source.processing_mode ?? source.data?.processing_mode ?? DEFAULT_CERTIFICATE_CLASSIFICATION.processing_mode,
    support_reason:
      source.support_reason ?? source.data?.support_reason ?? DEFAULT_CERTIFICATE_CLASSIFICATION.support_reason,
  };
}

function classificationFieldsChanged(source, classification) {
  const existing = pickCertificateClassification(source);
  return (
    existing.document_family !== classification.document_family ||
    existing.matching_mode !== classification.matching_mode ||
    Number(existing.classification_confidence ?? 0).toFixed(2) !== Number(classification.classification_confidence ?? 0).toFixed(2) ||
    Boolean(existing.review_required) !== Boolean(classification.review_required) ||
    existing.classification_reason !== classification.classification_reason
  );
}

function formatVolume(value) {
  const num = typeof value === "number" ? value : parseFlexibleNumber(value);
  if (!Number.isFinite(num)) return "—";
  return num.toFixed(3).replace(/\.?0+$/, "");
}

function formatConfidence(value) {
  const num = typeof value === "number" ? value : parseFlexibleNumber(value);
  if (!Number.isFinite(num)) return "—";
  return `${Math.round(num * 100)}%`;
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
  const docType = String(cert?.docType || "").toLowerCase();
  const isPoC = docType.includes("poc") || docType.includes("proof of compliance");
  const isPoS = docType.includes("pos") || docType.includes("proof of sustainability");
  const coverageStart = parseDateValue(cert?.coverageStart);
  const coverageEnd = parseDateValue(cert?.coverageEnd);
  if (coverageStart && coverageEnd) {
    return { start: coverageStart, end: coverageEnd };
  }

  if (isPoC && cert?.supplyPeriod) {
    const matches =
      String(cert.supplyPeriod)
        .match(/\d{4}-\d{2}-\d{2}|\d{1,2}[/. -]\d{1,2}[/. -]\d{4}/g)
        ?.map((value) => value.trim())
        .filter(Boolean) || [];
    if (matches.length >= 2) {
      const start = parseDateValue(matches[0]);
      const end = parseDateValue(matches[1]);
      if (start && end) return { start, end };
    }
  }

  if (isPoS && cert?.dateDispatch) {
    const sameDay = parseDateValue(cert.dateDispatch);
    if (sameDay) return { start: sameDay, end: sameDay };
  }

  if (!isPoC && !isPoS && cert?.dateDispatch) {
    const sameDay = parseDateValue(cert.dateDispatch);
    if (sameDay) return { start: sameDay, end: sameDay };
  }

  return null;
}

function formatAllocationUnitLabel(unit) {
  if (!unit) return "Allocation unit";
  const airport = unit.airport_name || unit.airport || [unit.airport_iata, unit.airport_icao].filter(Boolean).join("/") || "All airports";
  const period =
    unit.period_start && unit.period_end
      ? unit.period_start === unit.period_end
        ? unit.period_start
        : `${unit.period_start} to ${unit.period_end}`
      : unit.dispatch_date || "";
  return [unit.unit_type || "unit", airport, period].filter(Boolean).join(" · ");
}

function normalizeAllocationUnitNumber(value) {
  return normalizeVolumeNumber(value);
}

function getAllocationUnitAvailableVolume(unit) {
  const remaining = normalizeAllocationUnitNumber(unit?.remaining_volume_m3);
  if (Number.isFinite(remaining)) return remaining;

  const safVolume = normalizeAllocationUnitNumber(unit?.saf_volume_m3);
  const consumed = normalizeAllocationUnitNumber(unit?.consumed_volume_m3);
  if (Number.isFinite(safVolume) && Number.isFinite(consumed)) {
    return Number(Math.max(0, safVolume - consumed).toFixed(6));
  }

  return safVolume;
}

function normalizePersistedAllocationUnit(unit = {}, index = 0) {
  return {
    ...unit,
    unit_index: Number.isInteger(unit?.unit_index) ? unit.unit_index : index,
    review_required: Boolean(unit?.review_required),
    consumed_volume_m3: normalizeAllocationUnitNumber(unit?.consumed_volume_m3) || 0,
    remaining_volume_m3:
      normalizeAllocationUnitNumber(unit?.remaining_volume_m3) ??
      normalizeAllocationUnitNumber(unit?.saf_volume_m3) ??
      0,
  };
}

function getCertificateAllocationUnits(cert) {
  const persisted = (cert?.allocation_units || []).map((unit, index) => normalizePersistedAllocationUnit(unit, index));
  if (persisted.length) return persisted.sort((a, b) => (a.unit_index || 0) - (b.unit_index || 0));

  const drafts = buildCertificateAllocationUnitDrafts(cert || {}).map((unit, index) =>
    normalizePersistedAllocationUnit(
      {
        ...unit,
        id: unit.id || null,
      },
      index
    )
  );
  return drafts.sort((a, b) => (a.unit_index || 0) - (b.unit_index || 0));
}

function hydrateAllocationUnitsWithConsumption(units, links, certificateId) {
  const totalsByUnitId = new Map();
  const certificateLinks = (links || []).filter((link) => link.certificate_id === certificateId);

  for (const link of certificateLinks) {
    if (!link.allocation_unit_id) continue;
    const existing = totalsByUnitId.get(link.allocation_unit_id) || 0;
    totalsByUnitId.set(link.allocation_unit_id, Number((existing + Number(link.allocated_m3 || 0)).toFixed(6)));
  }

  const normalizedUnits = (units || []).map((unit, index) => normalizePersistedAllocationUnit(unit, index));
  if (normalizedUnits.length === 1) {
    const singleUnitLinkTotal = Number(
      certificateLinks.reduce((sum, link) => sum + Number(link.allocated_m3 || 0), 0).toFixed(6)
    );
    if (!totalsByUnitId.size && singleUnitLinkTotal > 0 && normalizedUnits[0]?.id) {
      totalsByUnitId.set(normalizedUnits[0].id, singleUnitLinkTotal);
    }
  }

  return normalizedUnits.map((unit) => {
    const safVolume = normalizeAllocationUnitNumber(unit.saf_volume_m3) || 0;
    const consumed = totalsByUnitId.get(unit.id) || 0;
    return {
      ...unit,
      consumed_volume_m3: consumed,
      remaining_volume_m3: Number(Math.max(0, safVolume - consumed).toFixed(6)),
    };
  });
}

function createAllocationUnitMatchData(unit, certificateData, targetVolume) {
  const airportLabel = unit.airport_name || [unit.airport_iata, unit.airport_icao].filter(Boolean).join("/") || "";
  const canonicalAirports = airportLabel || unit.airport_iata || unit.airport_icao
    ? [
        {
          raw: airportLabel || unit.airport_iata || unit.airport_icao,
          label: airportLabel || unit.airport_iata || unit.airport_icao,
          iata: unit.airport_iata || "",
          icao: unit.airport_icao || "",
        },
      ]
    : [];

  const mode = unit.matching_mode_override || certificateData?.matching_mode || certificateData?.matchingMode || "needs_review";
  const normalizedMonth =
    normalizeMonthValue(unit.period_start) ||
    normalizeMonthValue(unit.dispatch_date) ||
    normalizeMonthValue(certificateData?.coverageMonth);

  const base = {
    ...certificateData,
    quantity: targetVolume,
    totalVolume: unit.jet_volume_m3 ?? "",
    quantityUnit: certificateData?.quantityUnit || "m3",
    dateDispatch: unit.dispatch_date || "",
    coverageStart: unit.period_start || unit.dispatch_date || "",
    coverageEnd: unit.period_end || unit.dispatch_date || "",
    coverageMonth: normalizedMonth || "",
    deliveryAirports: airportLabel || "",
    physicalDeliveryAirport: airportLabel || "",
    canonicalAirports,
    physicalDeliveryAirportCanonical: canonicalAirports[0] || null,
    airportVolumes: [],
    monthlyVolumes: [],
    matching_mode: mode,
    matchingMode: mode,
    review_required: Boolean(unit.review_required),
  };

  if (mode === "poc-monthly") {
    base.docType = "PoC";
    base.monthlyVolumes = [
      {
        month: normalizedMonth || "",
        airport: airportLabel || unit.airport_iata || unit.airport_icao || "",
        quantity: targetVolume,
        quantityUnit: certificateData?.quantityUnit || "m3",
        airportCanonical: canonicalAirports[0] || null,
      },
    ];
  } else if (mode === "poc-airport") {
    base.docType = "PoC";
    base.airportVolumes = [
      {
        airport: airportLabel || unit.airport_iata || unit.airport_icao || "",
        quantity: targetVolume,
        quantityUnit: certificateData?.quantityUnit || "m3",
        airportCanonical: canonicalAirports[0] || null,
      },
    ];
  } else if (mode === "monthly-pos") {
    base.docType = "PoS";
  } else if (mode === "pos-uplift") {
    base.docType = "PoS";
  }

  return normalizeCertificateAirports(base).data;
}

function buildAllocationUnitReviewResult(unit, diagnostics, targetVolume, note) {
  diagnostics.candidate_count = 0;
  return {
    status: "needs_review",
    match_method: "allocation-unit-review",
    cert_volume_m3: targetVolume,
    allocated_volume_m3: 0,
    variance_m3: targetVolume,
    review_note: note || "Allocation unit requires manual review before matching.",
    candidate_sets: [],
    linked_rows: [],
    diagnostics,
  };
}

function summarizeAllocationUnitResult(unit, result, targetVolume) {
  return {
    allocation_unit_id: unit.id || null,
    unit_index: unit.unit_index,
    unit_type: unit.unit_type || "",
    label: formatAllocationUnitLabel(unit),
    target_volume_m3: targetVolume,
    allocated_volume_m3: result.allocated_volume_m3 ?? 0,
    variance_m3: result.variance_m3 ?? null,
    status: result.status,
    match_method: result.match_method,
    review_note: result.review_note || "",
    candidate_count: result.candidate_sets?.length || 0,
    warning: unit.normalization_warning || "",
  };
}

function buildCertificateMatchFromAllocationUnits(certificate, invoiceRows) {
  return buildDeterministicMatch(certificate.data, invoiceRows);
}

function formatDateRangeLabel(range) {
  if (!range?.start || !range?.end) return "";
  const start = range.start.toISOString().slice(0, 10);
  const end = range.end.toISOString().slice(0, 10);
  return start === end ? start : `${start} to ${end}`;
}

function splitRefs(value) {
  return String(value ?? "")
    .split(/[,;/\n]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function splitAirportRefs(value) {
  return String(value ?? "")
    .split(/[,;\n]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function tokenizeName(value) {
  return normalizeText(value)
    .split(/\s+/)
    .filter((token) => token.length >= 4);
}

function parseCSVGrid(text) {
  // Strip UTF-8 BOM if present
  const clean = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;

  // Auto-detect delimiter from the first line (header row)
  const firstLineEnd = clean.indexOf("\n");
  const firstLine = firstLineEnd >= 0 ? clean.slice(0, firstLineEnd).replace(/\r$/, "") : clean;
  const semicolonCount = (firstLine.match(/;/g) || []).length;
  const commaCount = (firstLine.match(/,/g) || []).length;
  const delimiter = semicolonCount > commaCount ? ";" : ",";

  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < clean.length; i += 1) {
    const char = clean[i];
    const next = clean[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        field += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === delimiter && !inQuotes) {
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

  // Fix mixed-delimiter rows: if the primary delimiter is comma but some data rows
  // used semicolons (or vice versa), those rows appear as a single field. Re-split them.
  if (rows.length > 1) {
    const expectedCols = rows[0].length;
    const altDelimiter = delimiter === "," ? ";" : ",";
    for (let r = 1; r < rows.length; r += 1) {
      if (rows[r].length === 1 && rows[r][0].includes(altDelimiter)) {
        const reParsed = rows[r][0].split(altDelimiter);
        if (reParsed.length === expectedCols) {
          rows[r] = reParsed;
        }
      }
    }
  }

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

function hasInvoiceOperationalContent(row) {
  return Boolean(
    row.invoice_no ||
      row.customer ||
      row.uplift_date ||
      row.flight_no ||
      row.delivery_ticket ||
      row.iata ||
      row.icao ||
      row.country ||
      row.supplier ||
      Number.isFinite(row.vol_m3)
  );
}

function formatInvoiceNumericKey(value) {
  const numeric = normalizeVolumeNumber(value);
  return Number.isFinite(numeric) ? numeric.toFixed(6) : "";
}

function buildInvoiceDuplicateGroupKey(row) {
  return [
    row.invoice_no || "",
    row.uplift_date || "",
    row.delivery_ticket || "",
    row.customer || "",
    row.iata || "",
    row.icao || "",
    row.flight_no || "",
    row.supplier || "",
    formatInvoiceNumericKey(row.saf_vol_m3),
  ].join("|");
}

function summarizeInvoiceValidationRows(rows) {
  return rows.slice(0, MAX_IMPORT_VALIDATION_ERRORS).map((row) => ({
    row_number: row.row_number,
    saf_value: row.saf_value,
    reason: row.reason,
  }));
}

function formatInvoiceValidationError(summary) {
  const count = summary.invalid_row_count || 0;
  const details = (summary.invalid_rows || [])
    .map((row) => `row ${row.row_number} (${row.saf_value || "blank"}: ${row.reason})`)
    .join("; ");
  const remainder = count > (summary.invalid_rows || []).length ? `; +${count - (summary.invalid_rows || []).length} more` : "";
  return `${count} row(s) have invalid SAF Vol 2% per M3 values and were imported as invalid/non-allocable.${details ? ` ${details}${remainder}` : ""}`;
}

function reconcileInvoiceRowValidationState(rows) {
  const normalizedRows = (rows || []).map((row) => {
    const safVolume = normalizeVolumeNumber(row?.saf_vol_m3);
    return {
      ...row,
      saf_vol_m3: safVolume,
      is_duplicate: false,
      duplicate_group_key: "",
    };
  });

  const allocableRows = normalizedRows.filter((row) => {
    const safVolume = normalizeVolumeNumber(row?.saf_vol_m3);
    return hasInvoiceOperationalContent(row) && Number.isFinite(safVolume) && safVolume > 0;
  });

  const duplicatesByKey = new Map();
  for (const row of allocableRows) {
    const duplicateKey = buildInvoiceDuplicateGroupKey(row);
    const group = duplicatesByKey.get(duplicateKey) || [];
    group.push(row);
    duplicatesByKey.set(duplicateKey, group);
  }

  const duplicateGroups = [...duplicatesByKey.entries()]
    .filter(([, group]) => group.length > 1)
    .map(([key, group]) => ({
      key,
      row_numbers: group.map((row) => row.row_number),
    }));

  const reconciledRows = normalizedRows.map((row) => {
    const duplicateKey = buildInvoiceDuplicateGroupKey(row);
    const isDuplicate = (duplicatesByKey.get(duplicateKey) || []).length > 1;
    const nextRow = {
      ...row,
      is_duplicate: isDuplicate,
      duplicate_group_key: isDuplicate ? duplicateKey : "",
    };
    const summary = summarizeInvoiceRowState(nextRow, normalizeVolumeNumber(row?.allocated_m3_total) || 0);
    return {
      ...nextRow,
      remaining_m3: summary.remainingVolume,
      is_allocated: summary.isAllocated,
      allocation_status: summary.status,
      // Preserve DB-stored validation_note (e.g. scope-discrepancy annotations)
      // unless summary yields a more critical system-level note (invalid saf, duplicate).
      validation_note: summary.validationNote || row.validation_note || "",
    };
  });

  return {
    rows: reconciledRows,
    summary: {
      row_count: reconciledRows.length,
      candidate_row_count: allocableRows.length,
      invalid_row_count: reconciledRows.filter((row) => row.allocation_status === "invalid").length,
      duplicate_row_count: reconciledRows.filter((row) => row.is_duplicate).length,
      duplicate_group_count: duplicateGroups.length,
      duplicate_groups: duplicateGroups.slice(0, MAX_IMPORT_VALIDATION_ERRORS),
    },
  };
}

function parseInvoiceCSV(text) {
  const grid = parseCSVGrid(text);
  if (!grid.length) {
    return {
      headers: [],
      rows: [],
      importRows: [],
      candidateRows: [],
      invalidRows: [],
      duplicateRows: [],
      duplicateGroups: [],
      missing: Object.keys(INVOICE_HEADER_ALIASES),
      summary: {
        total_row_count: 0,
        candidate_row_count: 0,
        invalid_row_count: 0,
        duplicate_row_count: 0,
        duplicate_group_count: 0,
        invalid_rows: [],
      },
    };
  }

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

    const rawSafVol = headerMap.saf_vol_m3 ? raw[headerMap.saf_vol_m3] || "" : "";
    const parsedSafVol = parseFlexibleNumber(rawSafVol);

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
      saf_vol_m3: parsedSafVol,
      allocated_m3_total: 0,
      remaining_m3: parsedSafVol,
      raw_payload: raw,
      is_allocated: false,
      allocation_status: "free",
      is_duplicate: false,
      duplicate_group_key: "",
      raw_saf_vol_m3: rawSafVol,
    };
  });

  const invalidRows = [];
  const operationalRows = [];

  for (const row of rows) {
    const hasContent = hasInvoiceOperationalContent(row);
    const safRaw = String(row.raw_saf_vol_m3 || "").trim();
    const safParsed = normalizeVolumeNumber(row.saf_vol_m3);

    if (!hasContent && !safRaw) continue;

    if (!safRaw) {
      const invalidRow = {
        row_number: row.row_number,
        saf_value: "",
        reason: "missing SAF volume",
      };
      invalidRows.push(invalidRow);
      operationalRows.push({
        ...row,
        saf_vol_m3: null,
        remaining_m3: null,
        validation_note: "SAF volume is missing or invalid.",
      });
      continue;
    }

    if (!Number.isFinite(safParsed)) {
      const invalidRow = {
        row_number: row.row_number,
        saf_value: safRaw,
        reason: "not a valid number",
      };
      invalidRows.push(invalidRow);
      operationalRows.push({
        ...row,
        saf_vol_m3: null,
        remaining_m3: null,
        validation_note: "SAF volume is missing or invalid.",
      });
      continue;
    }

    if (safParsed <= 0) {
      const invalidRow = {
        row_number: row.row_number,
        saf_value: safRaw,
        reason: "must be greater than 0",
      };
      invalidRows.push(invalidRow);
      operationalRows.push({
        ...row,
        saf_vol_m3: safParsed,
        remaining_m3: null,
        validation_note: "SAF volume is missing or invalid.",
      });
      continue;
    }

    operationalRows.push({
      ...row,
      saf_vol_m3: safParsed,
      remaining_m3: safParsed,
    });
  }

  const candidateRows = operationalRows.filter((row) => {
    const safVolume = normalizeVolumeNumber(row.saf_vol_m3);
    return hasInvoiceOperationalContent(row) && Number.isFinite(safVolume) && safVolume > 0;
  });

  const duplicatesByKey = new Map();
  for (const row of candidateRows) {
    const duplicateKey = buildInvoiceDuplicateGroupKey(row);
    const group = duplicatesByKey.get(duplicateKey) || [];
    group.push(row);
    duplicatesByKey.set(duplicateKey, group);
  }

  const duplicateGroups = [...duplicatesByKey.entries()]
    .filter(([, group]) => group.length > 1)
    .map(([key, group]) => ({
      key,
      row_numbers: group.map((row) => row.row_number),
    }));

  const duplicateRows = [];
  const preparedRows = operationalRows.map((row) => {
    const duplicateKey = buildInvoiceDuplicateGroupKey(row);
    const isDuplicate = (duplicatesByKey.get(duplicateKey) || []).length > 1;
    if (isDuplicate) {
      duplicateRows.push({
        row_number: row.row_number,
        duplicate_group_key: duplicateKey,
      });
    }
    const preparedRow = {
      ...row,
      is_duplicate: isDuplicate,
      duplicate_group_key: isDuplicate ? duplicateKey : "",
    };
    const summary = summarizeInvoiceRowState(preparedRow, 0);
    return {
      ...preparedRow,
      remaining_m3: summary.remainingVolume,
      is_allocated: summary.isAllocated,
      allocation_status: summary.status,
      validation_note: summary.validationNote,
    };
  });

  return {
    headers,
    rows,
    importRows: preparedRows,
    candidateRows: preparedRows.filter((row) => {
      const summary = summarizeInvoiceRowState(row, normalizeVolumeNumber(row?.allocated_m3_total) || 0);
      return summary.isAllocable;
    }),
    invalidRows,
    duplicateRows,
    duplicateGroups,
    missing,
    summary: {
      total_row_count: preparedRows.length,
      candidate_row_count: candidateRows.length,
      invalid_row_count: invalidRows.length,
      duplicate_row_count: duplicateRows.length,
      duplicate_group_count: duplicateGroups.length,
      invalid_rows: summarizeInvoiceValidationRows(invalidRows),
    },
  };
}

function deriveInvoiceYear(parsedCSV) {
  const yearCounts = new Map();
  for (const row of parsedCSV.importRows || []) {
    const date = parseDateValue(row.uplift_date);
    if (!date) continue;
    const year = date.getUTCFullYear();
    yearCounts.set(year, (yearCounts.get(year) || 0) + 1);
  }
  if (!yearCounts.size) return new Date().getUTCFullYear();
  let bestYear = 0;
  let bestCount = 0;
  for (const [year, count] of yearCounts) {
    if (count > bestCount) {
      bestYear = year;
      bestCount = count;
    }
  }
  return bestYear;
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
  // Cross-check: underlyingPoSList.quantity vs top-level out.quantity.
  // Catches LLM decimal-comma misreads ("1,185" → "1185") that the regex-based fix cannot detect
  // (no separator left to anchor on), and MJ/L energy content mix-ups (ratio ≈ 33).
  const topQty = Number(out.quantity);
  if (Array.isArray(out.underlyingPoSList) && Number.isFinite(topQty) && topQty > 0) {
    out.underlyingPoSList = out.underlyingPoSList.map((batch) => {
      const bq = Number(batch.quantity);
      if (!Number.isFinite(bq) || bq <= 0) return batch;
      const ratio = bq / topQty;
      if (ratio > 800 && ratio < 1200) {
        return {
          ...batch,
          quantity: String(Number((bq / 1000).toFixed(6))),
          _normalization_warning: `Divided underlying quantity by 1000 (comma-decimal anomaly: ${bq} → ${bq / 1000})`,
        };
      }
      if (ratio > 10) {
        return {
          ...batch,
          quantity: String(topQty),
          _normalization_warning: `Replaced underlying quantity ${bq} with top-level ${topQty} (${ratio.toFixed(1)}× divergence — likely energy content mix-up)`,
        };
      }
      return batch;
    });
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

async function extractCertificateFromBase64(base64, filename) {
  const { data, error } = await supabase.functions.invoke(EXTRACTION_FUNCTION, {
    body: { base64, filename },
  });

  if (error) {
    const detail = data?.error || data?.details?.error?.message || "";
    throw new Error(detail || error.message || "Edge function invocation failed.");
  }
  if (!data?.parsed) {
    throw new Error(data?.error || "Extraction function returned no parsed certificate.");
  }

  return {
    parsed: normalizeCommaDecimals(data.parsed),
    model: data.model || "claude-sonnet-4-20250514",
    usage: data.usage || null,
    responseId: data.response_id || null,
  };
}

function isMissingTableError(error) {
  const message = error?.message || "";
  return error?.code === "42P01" || /does not exist/i.test(message) || /could not find the table .* in the schema cache/i.test(message);
}

function isMissingColumnError(error) {
  const message = error?.message || "";
  return error?.code === "42703" || /column .* does not exist/i.test(message) || /could not find the '.*' column of '.*' in the schema cache/i.test(message);
}

function isSchemaCompatibilityError(error) {
  return isMissingTableError(error) || isMissingColumnError(error);
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

function getCertKind(cert) {
  const docType = String(cert?.docType || "").toLowerCase();
  if (docType.includes("poc") || docType.includes("proof of compliance")) return "poc";
  if (docType.includes("pos") || docType.includes("proof of sustainability")) return "pos";
  return "unknown";
}

function getCertificateAirportEntries(cert) {
  const normalized = normalizeCertificateAirports(cert).data || {};
  const entries = [];
  const push = (entry) => {
    if (!entry) return;
    entries.push({
      raw: entry.raw || "",
      iata: entry.iata || "",
      icao: entry.icao || "",
      label: entry.label || entry.raw || entry.iata || entry.icao || "",
    });
  };

  (normalized.canonicalAirports || []).forEach(push);
  push(normalized.physicalDeliveryAirportCanonical);
  (normalized.airportVolumes || []).forEach((item) => push(item?.airportCanonical));
  (normalized.monthlyVolumes || []).forEach((item) => push(item?.airportCanonical));

  return dedupeBy(entries, (item) => `${item.raw}|${item.iata}|${item.icao}|${item.label}`);
}

function buildAirportTargetMap(items, includeMonth) {
  const targets = new Map();
  const unresolvedAirports = [];

  for (const item of items || []) {
    const airportCanonical = item?.airportCanonical || canonicalizeAirportValue(item?.airport);
    const code = canonicalAirportCode(airportCanonical);
    const quantity = normalizeVolumeNumber(item?.quantity);
    const month = includeMonth ? normalizeMonthValue(item?.month) : "";

    if (!code) {
      if (airportCanonical?.raw) unresolvedAirports.push(airportCanonical.raw);
      continue;
    }
    if (!Number.isFinite(quantity) || quantity <= 0) continue;
    if (includeMonth && !month) continue;

    const key = includeMonth ? `${code}|${month}` : code;
    const existing = targets.get(key) || {
      key,
      airport: code,
      month,
      quantity: 0,
    };
    existing.quantity = Number((existing.quantity + quantity).toFixed(6));
    targets.set(key, existing);
  }

  return {
    targets,
    unresolvedAirports: dedupeBy(unresolvedAirports, (value) => value),
  };
}

function buildCertCriteria(cert) {
  const normalizedCert = normalizeCertificateAirports(cert).data || {};
  const airportEntries = getCertificateAirportEntries(normalizedCert);
  const airports = new Set();
  const aggregateAirports = new Set();
  const airportLabels = [];
  const unresolvedAirports = [];

  for (const entry of airportEntries) {
    if (entry.iata) airports.add(entry.iata.toUpperCase());
    if (entry.icao) airports.add(entry.icao.toUpperCase());
    const canonical = canonicalAirportCode(entry);
    if (canonical) aggregateAirports.add(canonical);
    else if (entry.raw) unresolvedAirports.push(entry.raw);
    if (entry.label || entry.raw || canonical) airportLabels.push(entry.label || entry.raw || canonical);
  }

  const monthlyTargetState = buildAirportTargetMap(normalizedCert.monthlyVolumes || [], true);
  const airportTargetState = buildAirportTargetMap(normalizedCert.airportVolumes || [], false);
  monthlyTargetState.unresolvedAirports.forEach((value) => unresolvedAirports.push(value));
  airportTargetState.unresolvedAirports.forEach((value) => unresolvedAirports.push(value));

  return {
    normalizedCert,
    kind: getCertKind(normalizedCert),
    matchingMode: normalizedCert?.matching_mode || normalizedCert?.matchingMode || "manual_only",
    targetVolume: normalizeVolumeNumber(normalizedCert?.quantity),
    airports,
    aggregateAirports,
    airportLabels: dedupeBy(airportLabels.filter(Boolean), (value) => value),
    unresolvedAirports: dedupeBy(unresolvedAirports.filter(Boolean), (value) => value),
    dateRange: parseDateRange(normalizedCert),
    contractRefs: splitRefs(normalizedCert?.contractNumber).map((item) => item.toUpperCase()),
    recipientTokens: tokenizeName(normalizedCert?.recipient),
    supplierTokens: tokenizeName(normalizedCert?.safSupplier || normalizedCert?.issuer),
    monthlyTargets: monthlyTargetState.targets,
    airportTargets: airportTargetState.targets,
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

  let score = 0;
  if (contractMatch) score += 12;
  if (airportMatch) score += 6;
  if (dateMatch) score += 4;

  return {
    contractMatch,
    airportMatch,
    dateMatch,
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
    allocated_m3: getRowAvailableVolume(row) || 0,
  };
}

function serializeAggregateGroup(group) {
  return {
    group_type: group.group_type,
    group_key: group.group_key,
    group_volume_m3: group.total_volume_m3,
    airport: group.airport || "",
    month: group.month || "",
  };
}

function sortCandidates(a, b) {
  if (b.score !== a.score) return b.score - a.score;
  if (Math.abs(a.variance_m3) !== Math.abs(b.variance_m3)) {
    return Math.abs(a.variance_m3) - Math.abs(b.variance_m3);
  }
  if (a.rows.length !== b.rows.length) return a.rows.length - b.rows.length;
  return (a.rows[0]?.row_number || 0) - (b.rows[0]?.row_number || 0);
}

function buildCandidateFromRows(rows, label, signals, priority, criteria) {
  const totalVolume = Number(rows.reduce((sum, row) => sum + Number(getRowAvailableVolume(row) || 0), 0).toFixed(6));
  const variance = Number((totalVolume - criteria.targetVolume).toFixed(6));
  const invoiceRows = rows.map(serializeRowSnapshot);
  const score =
    priority * 100 +
    rows.reduce((sum, row) => sum + (signals.get(row.id)?.score || 0), 0) -
    Math.abs(variance) * 1000 -
    rows.length;

  return {
    key: invoiceRows.map((row) => row.invoice_row_id).sort().join("|"),
    match_method: label,
    total_volume_m3: totalVolume,
    variance_m3: variance,
    rows: invoiceRows,
    groups: [],
    reason: label,
    score,
  };
}

function buildCandidateFromGroups(groups, label, priority, criteria, groupSignals) {
  const sortedGroups = [...groups].sort((a, b) => a.group_key.localeCompare(b.group_key));
  const totalVolume = Number(sortedGroups.reduce((sum, group) => sum + Number(group.total_volume_m3 || 0), 0).toFixed(6));
  const variance = Number((totalVolume - criteria.targetVolume).toFixed(6));

  const rowMap = new Map();
  for (const group of sortedGroups) {
    for (const row of group.rows || []) {
      rowMap.set(row.id, serializeRowSnapshot(row));
    }
  }
  const invoiceRows = [...rowMap.values()].sort((a, b) => (a.row_number || 0) - (b.row_number || 0));
  const score =
    priority * 100 +
    sortedGroups.reduce((sum, group) => sum + (groupSignals.get(group.group_key)?.score || 0), 0) -
    Math.abs(variance) * 1000 -
    invoiceRows.length -
    sortedGroups.length;

  return {
    key: sortedGroups.map((group) => group.group_key).join("|"),
    match_method: label,
    total_volume_m3: totalVolume,
    variance_m3: variance,
    rows: invoiceRows,
    groups: sortedGroups.map(serializeAggregateGroup),
    reason: `${label} · ${sortedGroups[0]?.group_type || "aggregate"}`,
    score,
  };
}

function comparePartialRowCombos(a, b) {
  if (b.partialScore !== a.partialScore) return b.partialScore - a.partialScore;
  if (a.rows.length !== b.rows.length) return a.rows.length - b.rows.length;
  return Math.abs(a.exactSum) - Math.abs(b.exactSum);
}

function searchRowCandidateSets(pool, criteria, signals, label, priority) {
  const sorted = [...pool]
    .filter((row) => {
      const availableVolume = getRowAvailableVolume(row);
      return Number.isFinite(Number(availableVolume)) && Number(availableVolume) > 0 && Number(availableVolume) <= criteria.targetVolume + MATCH_TOLERANCE;
    })
    .sort((a, b) => {
      const scoreDiff = (signals.get(b.id)?.score || 0) - (signals.get(a.id)?.score || 0);
      if (scoreDiff !== 0) return scoreDiff;
      const closenessDiff =
        Math.abs(criteria.targetVolume - Number(getRowAvailableVolume(a) || 0)) - Math.abs(criteria.targetVolume - Number(getRowAvailableVolume(b) || 0));
      if (closenessDiff !== 0) return closenessDiff;
      return (getRowAvailableVolume(b) || 0) - (getRowAvailableVolume(a) || 0);
    })
    .slice(0, MAX_ROW_DP_POOL_SIZE);

  const candidateMap = new Map();
  const maxUnits = Math.ceil((criteria.targetVolume + MATCH_TOLERANCE) * ROW_DP_SCALE);
  let states = new Map([[0, [{ rows: [], exactSum: 0, partialScore: 0, key: "" }]]]);

  const insertCombo = (bucketMap, units, combo) => {
    const current = bucketMap.get(units) || [];
    const deduped = current.filter((item) => item.key !== combo.key);
    deduped.push(combo);
    deduped.sort(comparePartialRowCombos);
    bucketMap.set(units, deduped.slice(0, MAX_ROW_DP_BUCKET_SIZE));
  };

  for (const row of sorted) {
    const rowVolume = normalizeVolumeNumber(getRowAvailableVolume(row));
    const rowUnits = Math.round((rowVolume || 0) * ROW_DP_SCALE);
    if (!Number.isFinite(rowVolume) || rowVolume <= 0 || rowUnits <= 0) continue;

    const nextStates = new Map(states);
    for (const [sumUnits, combos] of states.entries()) {
      const nextUnits = sumUnits + rowUnits;
      if (nextUnits > maxUnits) continue;

      for (const combo of combos) {
        const nextRows = [...combo.rows, row];
        const nextExactSum = Number((combo.exactSum + rowVolume).toFixed(6));
        const nextCombo = {
          rows: nextRows,
          exactSum: nextExactSum,
          partialScore: combo.partialScore + (signals.get(row.id)?.score || 0) - 1,
          key: combo.key ? `${combo.key}|${row.id}` : row.id,
        };
        insertCombo(nextStates, nextUnits, nextCombo);

        if (volumesMatch(nextExactSum, criteria.targetVolume)) {
          const candidate = buildCandidateFromRows(nextRows, label, signals, priority, criteria);
          const existing = candidateMap.get(candidate.key);
          if (!existing || candidate.score > existing.score) candidateMap.set(candidate.key, candidate);
        }
      }
    }

    states = nextStates;
  }

  return [...candidateMap.values()].sort(sortCandidates);
}

function aggregateInvoiceRows(rows, groupType, signals) {
  const groups = new Map();

  for (const row of rows) {
    const airport = (row.iata || row.icao || "").toUpperCase();
    if (!airport) continue;
    const month = row.uplift_date ? String(row.uplift_date).slice(0, 7) : "";
    if (groupType === "airport-month" && !month) continue;

    const groupKey = groupType === "airport-month" ? `${airport}|${month}` : airport;
    const existing = groups.get(groupKey) || {
      group_key: groupKey,
      group_type: groupType,
      airport,
      month,
      total_volume_m3: 0,
      rows: [],
      score: 0,
    };

    existing.total_volume_m3 = Number((existing.total_volume_m3 + Number(getRowAvailableVolume(row) || 0)).toFixed(6));
    existing.rows.push(row);
    existing.score += signals.get(row.id)?.score || 0;
    groups.set(groupKey, existing);
  }

  return [...groups.values()];
}

function buildGroupSignalMap(groups) {
  return new Map(groups.map((group) => [group.group_key, { score: group.score || 0 }]));
}

function searchAggregateCandidates(groups, criteria, label, priority, groupSignals) {
  const candidateMap = new Map();
  const sorted = [...groups]
    .filter((group) => Number.isFinite(group.total_volume_m3) && group.total_volume_m3 > 0)
    .sort((a, b) => {
      const scoreDiff = (groupSignals.get(b.group_key)?.score || 0) - (groupSignals.get(a.group_key)?.score || 0);
      if (scoreDiff !== 0) return scoreDiff;
      const closenessDiff = Math.abs(criteria.targetVolume - a.total_volume_m3) - Math.abs(criteria.targetVolume - b.total_volume_m3);
      if (closenessDiff !== 0) return closenessDiff;
      return b.total_volume_m3 - a.total_volume_m3;
    })
    .slice(0, MAX_AGGREGATE_POOL_SIZE);

  const totalAll = Number(sorted.reduce((sum, group) => sum + group.total_volume_m3, 0).toFixed(6));
  if (sorted.length && volumesMatch(totalAll, criteria.targetVolume)) {
    const allCandidate = buildCandidateFromGroups(sorted, label, priority, criteria, groupSignals);
    candidateMap.set(allCandidate.key, allCandidate);
  }

  const suffixSums = new Array(sorted.length + 1).fill(0);
  for (let index = sorted.length - 1; index >= 0; index -= 1) {
    suffixSums[index] = Number((suffixSums[index + 1] + sorted[index].total_volume_m3).toFixed(6));
  }

  function walk(startIndex, chosen, sum) {
    if (candidateMap.size >= MAX_SEARCH_RESULTS) return;
    if (chosen.length > 0 && volumesMatch(sum, criteria.targetVolume)) {
      const candidate = buildCandidateFromGroups(chosen, label, priority, criteria, groupSignals);
      const existing = candidateMap.get(candidate.key);
      if (!existing || candidate.score > existing.score) candidateMap.set(candidate.key, candidate);
      return;
    }
    if (chosen.length >= MAX_AGGREGATE_COMBINATION_SIZE || sum > criteria.targetVolume + MATCH_TOLERANCE) return;
    if (sum + suffixSums[startIndex] < criteria.targetVolume - MATCH_TOLERANCE) return;

    for (let index = startIndex; index < sorted.length; index += 1) {
      const group = sorted[index];
      const nextSum = Number((sum + group.total_volume_m3).toFixed(6));
      if (nextSum > criteria.targetVolume + MATCH_TOLERANCE) continue;
      walk(index + 1, [...chosen, group], nextSum);
      if (candidateMap.size >= MAX_SEARCH_RESULTS) return;
    }
  }

  walk(0, [], 0);
  return [...candidateMap.values()].sort(sortCandidates);
}

function buildTargetedAggregateCandidate(targets, aggregateGroups, label, priority, criteria, groupSignals) {
  if (!targets.size) return null;
  const chosen = [];

  for (const target of targets.values()) {
    const group = aggregateGroups.get(target.key);
    if (!group) return null;
    if (!volumesMatch(group.total_volume_m3, target.quantity)) return null;
    chosen.push({
      ...group,
      target_volume_m3: target.quantity,
    });
  }

  const candidate = buildCandidateFromGroups(chosen, label, priority, criteria, groupSignals);
  if (!volumesMatch(candidate.total_volume_m3, criteria.targetVolume)) return null;
  candidate.groups = candidate.groups.map((group) => ({
    ...group,
    target_volume_m3: targets.get(group.group_key)?.quantity || null,
  }));
  return candidate;
}

function createDiagnostics(criteria, matchingPath) {
  return {
    available_row_count: 0,
    date_pass_count: 0,
    airport_pass_count: 0,
    aggregate_unit_count: 0,
    candidate_count: 0,
    unresolved_airports: criteria.unresolvedAirports || [],
    matching_path: matchingPath,
  };
}

function noteWithAirportContext(note, criteria) {
  if (!criteria.unresolvedAirports.length) return note;
  return `${note} Unresolved airport aliases: ${criteria.unresolvedAirports.join(", ")}.`;
}

function buildUnmatchedResult(criteria, diagnostics, matchMethod, note) {
  return {
    status: "unmatched",
    match_method: matchMethod,
    cert_volume_m3: criteria.targetVolume,
    allocated_volume_m3: 0,
    variance_m3: criteria.targetVolume,
    review_note: noteWithAirportContext(note, criteria),
    candidate_sets: [],
    linked_rows: [],
    diagnostics,
  };
}

function buildReviewResult(criteria, diagnostics, candidates, note) {
  const topCandidates = candidates.slice(0, MAX_SEARCH_RESULTS);
  diagnostics.candidate_count = topCandidates.length;
  return {
    status: "needs_review",
    match_method: topCandidates[0].match_method,
    cert_volume_m3: criteria.targetVolume,
    allocated_volume_m3: topCandidates[0].total_volume_m3,
    variance_m3: topCandidates[0].variance_m3,
    review_note: noteWithAirportContext(note, criteria),
    candidate_sets: topCandidates,
    linked_rows: [],
    diagnostics,
  };
}

function buildAutoLinkedResult(criteria, diagnostics, candidate, note) {
  diagnostics.candidate_count = 1;
  return {
    status: "auto_linked",
    match_method: candidate.match_method,
    cert_volume_m3: criteria.targetVolume,
    allocated_volume_m3: candidate.total_volume_m3,
    variance_m3: candidate.variance_m3,
    review_note: noteWithAirportContext(note, criteria),
    candidate_sets: [candidate],
    linked_rows: candidate.rows,
    diagnostics,
  };
}

function buildManualOnlyResult(criteria, diagnostics, certData, note) {
  diagnostics.candidate_count = 0;
  return {
    status: "manual_only",
    match_method: "manual_only",
    cert_volume_m3: criteria.targetVolume,
    allocated_volume_m3: 0,
    variance_m3: criteria.targetVolume,
    review_note:
      note ||
      certData?.support_reason ||
      certData?.classification_reason ||
      "Certificate is outside the supported simple scope and will not be auto-linked.",
    candidate_sets: [],
    linked_rows: [],
    diagnostics,
  };
}

function normalizeBusinessMatchingMode(value) {
  const text = normalizeText(value);
  if (!text) return "";
  if (text === "simple monthly airport" || text === "simple_monthly_airport" || text === "simple-monthly-airport") {
    return "simple_monthly_airport";
  }
  if (text === "manual only" || text === "manual_only" || text === "manual-only") return "manual_only";
  return "";
}

function buildPosRowCandidates(criteria, rows, signals) {
  const pools = [];
  const pushPool = (poolRows, label, priority) => {
    if (!poolRows.length) return;
    pools.push({ rows: poolRows, label, priority });
  };

  const exactSingles = rows.filter((row) => volumesMatch(Number(getRowAvailableVolume(row) || 0), criteria.targetVolume));
  pushPool(exactSingles, "exact-single-row", 9);

  if (criteria.contractRefs.length) {
    pushPool(rows.filter((row) => signals.get(row.id)?.contractMatch), "contract-ref", 8);
  }
  if (criteria.airports.size && criteria.dateRange) {
    pushPool(
      rows.filter((row) => signals.get(row.id)?.airportMatch && signals.get(row.id)?.dateMatch),
      "airport+date",
      7
    );
  }
  if (criteria.airports.size) {
    pushPool(rows.filter((row) => signals.get(row.id)?.airportMatch), "airport", 6);
  }
  if (criteria.dateRange) {
    pushPool(rows.filter((row) => signals.get(row.id)?.dateMatch), "date", 5);
  }
  pushPool(
    [...rows].sort((a, b) => {
      const scoreDiff = (signals.get(b.id)?.score || 0) - (signals.get(a.id)?.score || 0);
      if (scoreDiff !== 0) return scoreDiff;
      return Math.abs(criteria.targetVolume - Number(getRowAvailableVolume(a) || 0)) - Math.abs(criteria.targetVolume - Number(getRowAvailableVolume(b) || 0));
    }),
    "volume-only",
    1
  );

  const candidateMap = new Map();
  for (const pool of pools) {
    const found = searchRowCandidateSets(pool.rows, criteria, signals, pool.label, pool.priority);
    for (const candidate of found) {
      const existing = candidateMap.get(candidate.key);
      if (!existing || candidate.score > existing.score) candidateMap.set(candidate.key, candidate);
    }
  }

  return [...candidateMap.values()].sort(sortCandidates);
}

function getMonthlyPosGroupKey(criteria) {
  if (criteria?.matchingMode !== "simple_monthly_airport") return "";
  const coverageMonth = normalizeMonthValue(criteria?.normalizedCert?.coverageMonth);
  const airports = [...(criteria?.aggregateAirports || [])].filter(Boolean);
  if (!coverageMonth || airports.length !== 1) return "";
  return `${airports[0]}|${coverageMonth}`;
}

function formatMonthlyPosGroupLabel(groupKey) {
  if (!groupKey) return "the airport-month pool";
  const [airport, month] = String(groupKey).split("|");
  return airport && month ? `${airport} ${month}` : "the airport-month pool";
}

function sortRowsForFifoAllocation(rows) {
  return [...rows].sort((a, b) => {
    const dateA = a.uplift_date || "";
    const dateB = b.uplift_date || "";
    if (dateA !== dateB) return dateA.localeCompare(dateB);
    const invoiceA = a.invoice_no || "";
    const invoiceB = b.invoice_no || "";
    if (invoiceA !== invoiceB) return invoiceA.localeCompare(invoiceB);
    const rowNumberDiff = (a.row_number || 0) - (b.row_number || 0);
    if (rowNumberDiff !== 0) return rowNumberDiff;
    return String(a.id || "").localeCompare(String(b.id || ""));
  });
}

function sortMonthlyCertificatesForAllocation(certs) {
  return [...certs].sort((a, b) => {
    const titleDiff = certTitle(a).localeCompare(certTitle(b));
    if (titleDiff !== 0) return titleDiff;
    return Number(normalizeVolumeNumber(a.data?.quantity) || 0) - Number(normalizeVolumeNumber(b.data?.quantity) || 0);
  });
}

function buildFifoMonthlyCandidate(criteria, rows) {
  const sortedRows = sortRowsForFifoAllocation(rows);
  const linkedRows = [];
  let remainingTarget = criteria.targetVolume;

  for (const row of sortedRows) {
    const availableVolume = getRowAvailableVolume(row) || 0;
    if (availableVolume <= MATCH_TOLERANCE) continue;
    const take = Number(Math.min(remainingTarget, availableVolume).toFixed(6));
    if (take <= MATCH_TOLERANCE) continue;

    linkedRows.push({
      invoice_row_id: row.id,
      row_number: row.row_number,
      invoice_no: row.invoice_no || "",
      customer: row.customer || "",
      uplift_date: row.uplift_date || null,
      iata: row.iata || "",
      icao: row.icao || "",
      allocated_m3: take,
    });
    remainingTarget = Number(Math.max(0, remainingTarget - take).toFixed(6));
    if (remainingTarget <= MATCH_TOLERANCE) break;
  }

  if (remainingTarget > MATCH_TOLERANCE) return null;

  return {
    key: linkedRows.map((row) => `${row.invoice_row_id}:${row.allocated_m3}`).join("|"),
    match_method: "fifo-monthly-partial",
    total_volume_m3: criteria.targetVolume,
    variance_m3: 0,
    rows: linkedRows,
    groups: [],
    reason: "fifo-monthly-partial",
    score: 1000 - linkedRows.length,
  };
}

function buildMonthlyPosFifoResult(certData, invoiceRows) {
  const criteria = buildCertCriteria(certData);
  const diagnostics = createDiagnostics(criteria, "simple_monthly_airport");
  diagnostics.allocation_policy = "fifo-airport-month-partial";

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
      diagnostics,
    };
  }

  const availableRows = invoiceRows.filter(isRowAvailable);
  diagnostics.available_row_count = availableRows.length;

  const dateValidatedRows = criteria.dateRange
    ? availableRows.filter((row) => dateInRange(row.uplift_date, criteria.dateRange))
    : availableRows;
  diagnostics.date_pass_count = dateValidatedRows.length;

  if (criteria.dateRange && !dateValidatedRows.length) {
    return buildUnmatchedResult(
      criteria,
      diagnostics,
      "date-validated",
      `No free invoice rows fall inside the required coverage window (${formatDateRangeLabel(criteria.dateRange)}).`
    );
  }

  const dateValidatedSignals = new Map(dateValidatedRows.map((row) => [row.id, scoreInvoiceRow(row, criteria)]));
  const airportValidatedRows = criteria.airports.size
    ? dateValidatedRows.filter((row) => dateValidatedSignals.get(row.id)?.airportMatch)
    : dateValidatedRows;
  diagnostics.airport_pass_count = airportValidatedRows.length;

  if (criteria.airports.size && !airportValidatedRows.length) {
    return buildUnmatchedResult(
      criteria,
      diagnostics,
      "airport-validated",
      `No free invoice rows match the required airport(s) ${criteria.airportLabels.join(", ")}${criteria.dateRange ? ` within ${formatDateRangeLabel(criteria.dateRange)}` : ""}.`
    );
  }

  const totalAvailableVolume = Number(airportValidatedRows.reduce((sum, row) => sum + Number(getRowAvailableVolume(row) || 0), 0).toFixed(6));
  if (totalAvailableVolume + MATCH_TOLERANCE < criteria.targetVolume) {
    return buildUnmatchedResult(
      criteria,
      diagnostics,
      "insufficient-volume",
      `Only ${formatVolume(totalAvailableVolume)} m3 remains available inside ${formatMonthlyPosGroupLabel(getMonthlyPosGroupKey(criteria))}, below the certificate target of ${formatVolume(criteria.targetVolume)} m3.`
    );
  }

  const candidate = buildFifoMonthlyCandidate(criteria, airportValidatedRows);
  if (!candidate) {
    return buildUnmatchedResult(
      criteria,
      diagnostics,
      "fifo-failed",
      `FIFO monthly allocation could not satisfy ${formatVolume(criteria.targetVolume)} m3 inside ${formatMonthlyPosGroupLabel(getMonthlyPosGroupKey(criteria))}.`
    );
  }

  diagnostics.candidate_count = 1;
  return buildAutoLinkedResult(
    criteria,
    diagnostics,
    candidate,
    `Matched by deterministic FIFO allocation inside ${formatMonthlyPosGroupLabel(getMonthlyPosGroupKey(criteria))}. Rows are filtered by airport and month, then consumed by uplift date with partial final-row allocation when needed.`
  );
}

function buildMonthlyPosGroupFifoResults(groupCerts, workingRows) {
  const results = new Map();
  const orderedCerts = sortMonthlyCertificatesForAllocation(groupCerts);

  for (const cert of orderedCerts) {
    const result = buildMonthlyPosFifoResult(cert.data, workingRows);
    results.set(cert.id, result);
    if (result.linked_rows?.length) {
      for (const linked of result.linked_rows) {
        const target = workingRows.find((row) => row.id === linked.invoice_row_id);
        if (target) applyRowAllocation(target, linked.allocated_m3 || 0);
      }
    }
  }

  return results;
}

function evaluateMonthlyPosCandidates(certData, invoiceRows) {
  const criteria = buildCertCriteria(certData);
  const diagnostics = createDiagnostics(criteria, "pos-monthly");

  if (!Number.isFinite(criteria.targetVolume) || criteria.targetVolume <= 0) {
    return {
      criteria,
      diagnostics,
      terminalResult: {
        status: "unmatched",
        match_method: "unusable-volume",
        cert_volume_m3: null,
        allocated_volume_m3: 0,
        variance_m3: null,
        review_note: "Certificate quantity could not be parsed into m3.",
        candidate_sets: [],
        linked_rows: [],
        diagnostics,
      },
    };
  }

  const availableRows = invoiceRows.filter(isRowAvailable);
  diagnostics.available_row_count = availableRows.length;

  const dateValidatedRows = criteria.dateRange
    ? availableRows.filter((row) => dateInRange(row.uplift_date, criteria.dateRange))
    : availableRows;
  diagnostics.date_pass_count = dateValidatedRows.length;

  if (criteria.dateRange && !dateValidatedRows.length) {
    return {
      criteria,
      diagnostics,
      terminalResult: buildUnmatchedResult(
        criteria,
        diagnostics,
        "date-validated",
        `No free invoice rows fall inside the required coverage window (${formatDateRangeLabel(criteria.dateRange)}).`
      ),
    };
  }

  const dateValidatedSignals = new Map(dateValidatedRows.map((row) => [row.id, scoreInvoiceRow(row, criteria)]));
  const airportValidatedRows = criteria.airports.size
    ? dateValidatedRows.filter((row) => dateValidatedSignals.get(row.id)?.airportMatch)
    : dateValidatedRows;
  diagnostics.airport_pass_count = airportValidatedRows.length;

  if (criteria.airports.size && !airportValidatedRows.length) {
    return {
      criteria,
      diagnostics,
      terminalResult: buildUnmatchedResult(
        criteria,
        diagnostics,
        "airport-validated",
        `No free invoice rows match the required airport(s) ${criteria.airportLabels.join(", ")}${criteria.dateRange ? ` within ${formatDateRangeLabel(criteria.dateRange)}` : ""}.`
      ),
    };
  }

  const signals = new Map(airportValidatedRows.map((row) => [row.id, scoreInvoiceRow(row, criteria)]));
  const candidates = buildPosRowCandidates(criteria, airportValidatedRows, signals);
  diagnostics.candidate_count = candidates.length;

  if (!candidates.length) {
    return {
      criteria,
      diagnostics,
      terminalResult: buildUnmatchedResult(
        criteria,
        diagnostics,
        "none",
        `No free invoice row group matches ${formatVolume(criteria.targetVolume)} m3 within ${MATCH_TOLERANCE} m3${criteria.dateRange ? ` for ${formatDateRangeLabel(criteria.dateRange)}` : ""}.`
      ),
    };
  }

  return {
    criteria,
    diagnostics,
    candidates,
  };
}

function searchBestGroupAssignments(entries) {
  const orderedEntries = [...entries]
    .map((entry) => ({
      ...entry,
      candidates: (entry.candidates || []).slice(0, MAX_GROUP_CANDIDATES_PER_CERT),
    }))
    .sort((a, b) => {
      if (a.candidates.length !== b.candidates.length) return a.candidates.length - b.candidates.length;
      return b.criteria.targetVolume - a.criteria.targetVolume;
    });

  let bestMatchedCount = -1;
  const bestSolutions = [];
  const solutionKeys = new Set();

  const addSolution = (assignments, matchedCount, totalScore) => {
    const signature = [...assignments.entries()]
      .map(([certId, candidate]) => `${certId}:${candidate.key}`)
      .sort()
      .join("||");
    if (solutionKeys.has(signature)) return;
    solutionKeys.add(signature);
    bestSolutions.push({
      matchedCount,
      totalScore,
      assignments: new Map(assignments),
    });
    if (bestSolutions.length > MAX_GROUP_ASSIGNMENT_SOLUTIONS) {
      const dropped = bestSolutions.pop();
      if (dropped) {
        const droppedSignature = [...dropped.assignments.entries()]
          .map(([certId, candidate]) => `${certId}:${candidate.key}`)
          .sort()
          .join("||");
        solutionKeys.delete(droppedSignature);
      }
    }
  };

  const compareSolutions = (a, b) => {
    return b.totalScore - a.totalScore;
  };

  const usedRowIds = new Set();
  const assignments = new Map();

  const walk = (index, matchedCount, totalScore) => {
    const remaining = orderedEntries.length - index;
    if (matchedCount + remaining < bestMatchedCount) return;

    if (index >= orderedEntries.length) {
      if (matchedCount > bestMatchedCount) {
        bestMatchedCount = matchedCount;
        bestSolutions.length = 0;
        solutionKeys.clear();
      }
      if (matchedCount === bestMatchedCount) {
        addSolution(assignments, matchedCount, totalScore);
        bestSolutions.sort(compareSolutions);
      }
      return;
    }

    const entry = orderedEntries[index];
    const orderedCandidates = [...entry.candidates].sort(sortCandidates);

    for (const candidate of orderedCandidates) {
      const rowIds = candidate.rows.map((row) => row.invoice_row_id);
      if (rowIds.some((rowId) => usedRowIds.has(rowId))) continue;

      assignments.set(entry.cert.id, candidate);
      rowIds.forEach((rowId) => usedRowIds.add(rowId));
      walk(index + 1, matchedCount + 1, totalScore + candidate.score);
      rowIds.forEach((rowId) => usedRowIds.delete(rowId));
      assignments.delete(entry.cert.id);
    }

    walk(index + 1, matchedCount, totalScore);
  };

  walk(0, 0, 0);
  return bestSolutions.sort(compareSolutions);
}

function buildMonthlyPosGroupResults(groupEntries) {
  const results = new Map();
  const readyEntries = [];

  for (const entry of groupEntries) {
    if (entry.terminalResult) {
      results.set(entry.cert.id, entry.terminalResult);
      continue;
    }
    readyEntries.push(entry);
  }

  if (!readyEntries.length) return results;

  const groupKey = getMonthlyPosGroupKey(readyEntries[0].criteria);
  const groupLabel = formatMonthlyPosGroupLabel(groupKey);
  const solutions = searchBestGroupAssignments(readyEntries);

  if (!solutions.length || solutions[0].matchedCount <= 0) {
    for (const entry of readyEntries) {
      const diagnostics = {
        ...entry.diagnostics,
        candidate_count: 0,
        group_key: groupKey,
        group_certificate_count: readyEntries.length,
        group_solution_count: 0,
      };
      results.set(
        entry.cert.id,
        buildUnmatchedResult(
          entry.criteria,
          diagnostics,
          "group-conflict",
          `No globally consistent monthly PoS allocation was found inside ${groupLabel}.`
        )
      );
    }
    return results;
  }

  for (const entry of readyEntries) {
    const solutionCandidates = [];
    for (const solution of solutions) {
      const candidate = solution.assignments.get(entry.cert.id);
      if (candidate) solutionCandidates.push(candidate);
    }

    const distinctCandidates = dedupeBy(solutionCandidates, (candidate) => candidate.key).sort(sortCandidates);
    const diagnostics = {
      ...entry.diagnostics,
      candidate_count: distinctCandidates.length,
      group_key: groupKey,
      group_certificate_count: readyEntries.length,
      group_solution_count: solutions.length,
    };

    if (!distinctCandidates.length) {
      results.set(
        entry.cert.id,
        buildUnmatchedResult(
          entry.criteria,
          diagnostics,
          "group-conflict",
          `No globally consistent monthly PoS allocation was found for this certificate inside ${groupLabel}.`
        )
      );
      continue;
    }

    if (
      distinctCandidates.length === 1 &&
      solutionCandidates.length === solutions.length &&
      !entry.criteria.unresolvedAirports.length
    ) {
      results.set(
        entry.cert.id,
        buildAutoLinkedResult(
          entry.criteria,
          diagnostics,
          distinctCandidates[0],
          `Unique exact monthly PoS match found after ${groupLabel} reconciliation.`
        )
      );
      continue;
    }

    results.set(
      entry.cert.id,
      buildReviewResult(
        entry.criteria,
        diagnostics,
        distinctCandidates,
        `${distinctCandidates.length} globally consistent monthly PoS candidate allocations remain after ${groupLabel} reconciliation. Manual approval required.`
      )
    );
  }

  return results;
}

function buildDeterministicMatch(certData, invoiceRows) {
  const criteria = buildCertCriteria(certData);
  const businessMatchingMode = normalizeBusinessMatchingMode(criteria.matchingMode);

  if (businessMatchingMode !== "simple_monthly_airport") {
    return buildManualOnlyResult(
      criteria,
      createDiagnostics(criteria, "manual_only"),
      certData,
      certData?.support_reason ||
        certData?.classification_reason ||
        "Certificate is outside the supported simple scope and will not be auto-linked."
    );
  }

  return buildMonthlyPosFifoResult(certData, invoiceRows);
}

async function backfillCanonicalCertificateRows(certRows) {
  const normalizedRows = [];
  let updatedCount = 0;

  for (const row of certRows || []) {
    // Skip backfill for certs that already have canonical airports and classification columns populated,
    // UNLESS they are manual_only (re-check in case alias updates fix their classification).
    const alreadyClassified =
      Array.isArray(row.data?.canonicalAirports) &&
      row.data.canonicalAirports.length > 0 &&
      row.document_family &&
      row.matching_mode &&
      row.document_family !== "manual_only";

    if (alreadyClassified) {
      normalizedRows.push(row);
      continue;
    }

    const normalized = normalizeCertificateAirports(row.data || {}, { filename: row.filename });
    normalizedRows.push({
      ...row,
      data: normalized.data,
      document_family: normalized.classification.document_family,
      matching_mode: normalized.classification.matching_mode,
      classification_confidence: normalized.classification.classification_confidence,
      review_required: normalized.classification.review_required,
      classification_reason: normalized.classification.classification_reason,
    });
    if (!normalized.changed && !classificationFieldsChanged(row, normalized.classification)) continue;

    let updateRes = await supabase
      .from("certificates")
      .update({
        data: normalized.data,
        document_family: normalized.classification.document_family,
        matching_mode: normalized.classification.matching_mode,
        classification_confidence: normalized.classification.classification_confidence,
        review_required: normalized.classification.review_required,
        classification_reason: normalized.classification.classification_reason,
      })
      .eq("id", row.id);
    if (updateRes.error && isMissingColumnError(updateRes.error)) {
      updateRes = await supabase.from("certificates").update({ data: normalized.data }).eq("id", row.id);
    }
    const { error } = updateRes;
    if (error) throw error;
    updatedCount += 1;
  }

  return { rows: normalizedRows, updatedCount };
}

function formatDiagnosticsSummary(diagnostics) {
  if (!diagnostics) return "";
  const parts = [
    diagnostics.matching_path || "—",
    `free ${diagnostics.available_row_count ?? 0}`,
    `date ${diagnostics.date_pass_count ?? 0}`,
    `airport ${diagnostics.airport_pass_count ?? 0}`,
    `units ${diagnostics.aggregate_unit_count ?? 0}`,
    `candidates ${diagnostics.candidate_count ?? 0}`,
  ];
  if (diagnostics.allocation_unit_count !== undefined && diagnostics.allocation_unit_count !== null) {
    parts.push(`alloc ${diagnostics.allocation_unit_count}`);
  }
  return parts.join(" · ");
}

function formatUnresolvedAirports(diagnostics) {
  const values = diagnostics?.unresolved_airports || [];
  return values.length ? values.join(", ") : "";
}

function certTitle(cert) {
  return cert?.data?.uniqueNumber || cert?.filename || "Certificate";
}

function isSupportedSimpleCert(cert) {
  return (cert?.document_family || cert?.data?.document_family) === "supported_simple";
}

function isSupportedPocCert(cert) {
  return (cert?.document_family || cert?.data?.document_family) === "supported_poc";
}

function Badge({ status }) {
  const label = String(status || "unknown");
  const map = {
    supported_simple: ["#00ff9d", "#001a0d"],
    manual_only: ["#ffbb00", "#1a1200"],
    simple_monthly_airport: ["#4a9fd4", "#061423"],
    auto_linked: ["#00ff9d", "#001a0d"],
    partial_linked: ["#ff9933", "#1a0d00"],
    approved: ["#00ff9d", "#001a0d"],
    needs_review: ["#ffbb00", "#1a1200"],
    unmatched: ["#ff6666", "#1a0000"],
    rejected: ["#ff4444", "#1a0000"],
    allocated: ["#00ff9d", "#001a0d"],
    partial: ["#ffbb00", "#1a1200"],
    free: ["#4a9fd4", "#061423"],
    duplicate: ["#ffbb00", "#1a1200"],
    invalid: ["#ff6666", "#1a0000"],
    widened: ["#ff9933", "#1a0d00"],
    no_uplift: ["#6b7a8f", "#0a1220"],
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

function formatListValue(value) {
  if (!Array.isArray(value)) return value;
  const filtered = value.map((item) => String(item ?? "").trim()).filter(Boolean);
  return filtered.length ? filtered.join(", ") : "—";
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
                {["CSV ROW", "INVOICE", "CUSTOMER", "UPLIFT DATE", "IATA", "ICAO", "SAF M3"].map((header) => (
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

function ManualMatchPanel({ cert, invoiceRows, loading, onSave }) {
  const certVol = Number(cert?.match?.cert_volume_m3) || Number(cert?.data?.quantity) || 0;
  const existingAllocated = Number(cert?.match?.allocated_volume_m3) || 0;

  const defaultAirport = (() => {
    const candidates = [
      ...(cert?.data?.canonicalAirports || []),
      ...(cert?.data?.airports || []),
      ...(cert?.match?.diagnostics?.resolved_airports || []),
    ];
    for (const a of candidates) {
      const code = (a?.iata || a?.icao || (typeof a === "string" ? a : "") || "").toUpperCase();
      if (code && /^[A-Z]{3,4}$/.test(code)) return code;
    }
    return "";
  })();
  const defaultMonth = (() => {
    const candidates = [cert?.match?.diagnostics?.coverage_month, cert?.data?.coverageMonth, cert?.data?.supplyPeriodStart];
    for (const c of candidates) {
      if (typeof c === "string" && /^\d{4}-\d{2}/.test(c)) return c.slice(0, 7);
    }
    return "";
  })();

  const [airportFilter, setAirportFilter] = useState(defaultAirport);
  const [monthFilter, setMonthFilter] = useState(defaultMonth);
  const [supplierFilter, setSupplierFilter] = useState("");
  const [searchText, setSearchText] = useState("");
  const [selectedIds, setSelectedIds] = useState(() => new Set());
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    setAirportFilter(defaultAirport);
    setMonthFilter(defaultMonth);
    setSupplierFilter("");
    setSearchText("");
    setSelectedIds(new Set());
  }, [cert?.id]); // eslint-disable-line react-hooks/exhaustive-deps

  const existingLinkRowIds = new Set((cert?.match?.links || []).map((l) => l.invoice_row_id).filter(Boolean));

  const getRowMonth = (row) => {
    if (!row?.uplift_date) return null;
    const d = new Date(row.uplift_date);
    if (isNaN(d)) return null;
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
  };
  const monthNames = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];

  const searchLower = searchText.toLowerCase();
  const availableRows = (invoiceRows || []).filter((row) => {
    if (existingLinkRowIds.has(row.id)) return false;
    if (!(Number(row.remaining_m3) > MATCH_TOLERANCE)) return false;
    if (airportFilter && row.iata !== airportFilter) return false;
    if (monthFilter && getRowMonth(row) !== monthFilter) return false;
    if (supplierFilter && row.supplier !== supplierFilter) return false;
    if (searchLower && !(row.customer || "").toLowerCase().includes(searchLower) && !(row.invoice_no || "").toLowerCase().includes(searchLower)) return false;
    return true;
  });

  const uniqueAirports = [...new Set((invoiceRows || []).map((r) => r.iata).filter((a) => a && /^[A-Z]{3,4}$/.test(a)))].sort();
  const uniqueMonths = [...new Set((invoiceRows || []).map((r) => getRowMonth(r)).filter(Boolean))].sort();
  const uniqueSuppliers = [...new Set((invoiceRows || []).map((r) => r.supplier).filter(Boolean))].sort();

  const selectedRows = availableRows.filter((r) => selectedIds.has(r.id));
  const selectedVolume = selectedRows.reduce((sum, r) => sum + (Number(r.remaining_m3) || 0), 0);
  const projected = existingAllocated + selectedVolume;

  let volumeColor = "#c8dff0";
  if (selectedVolume > 0) {
    if (projected > certVol + MATCH_TOLERANCE) volumeColor = "#ffbb00";
    else if (projected >= certVol - MATCH_TOLERANCE) volumeColor = "#00ff9d";
    else volumeColor = "#ff9933";
  }

  const toggleRow = (id) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const handleSave = async () => {
    if (!selectedIds.size || saving) return;
    try {
      setSaving(true);
      await onSave([...selectedIds]);
      setSelectedIds(new Set());
    } finally {
      setSaving(false);
    }
  };

  const hasFilters = airportFilter || monthFilter || supplierFilter || searchText;
  const inputStyle = {
    background: "#0a1628",
    color: "#c8dff0",
    border: "1px solid #0d3060",
    borderRadius: 6,
    padding: "6px 10px",
    fontSize: 11,
    fontFamily: "'Space Mono', monospace",
  };

  return (
    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040", marginTop: 12 }}>
      <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
        MANUAL MATCH — BROWSE & LINK INVOICE ROWS
      </div>

      <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginBottom: 12 }}>
        <select value={airportFilter} onChange={(e) => setAirportFilter(e.target.value)} style={inputStyle}>
          <option value="">All airports</option>
          {uniqueAirports.map((a) => (
            <option key={a} value={a}>
              {a}
            </option>
          ))}
        </select>
        <select value={monthFilter} onChange={(e) => setMonthFilter(e.target.value)} style={inputStyle}>
          <option value="">All months</option>
          {uniqueMonths.map((m) => {
            const [y, mo] = m.split("-");
            return (
              <option key={m} value={m}>
                {monthNames[Number(mo) - 1]} {y}
              </option>
            );
          })}
        </select>
        <select value={supplierFilter} onChange={(e) => setSupplierFilter(e.target.value)} style={inputStyle}>
          <option value="">All suppliers</option>
          {uniqueSuppliers.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <input
          type="text"
          placeholder="Search customer or invoice..."
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          style={{ ...inputStyle, width: 220 }}
        />
        {hasFilters ? (
          <button
            onClick={() => {
              setAirportFilter("");
              setMonthFilter("");
              setSupplierFilter("");
              setSearchText("");
            }}
            style={{
              background: "#0a1628",
              color: "#4a9fd4",
              border: "1px solid #0d3060",
              borderRadius: 6,
              padding: "6px 12px",
              fontSize: 10,
              fontFamily: "'Space Mono', monospace",
              letterSpacing: 1,
              cursor: "pointer",
            }}
          >
            CLEAR FILTERS
          </button>
        ) : null}
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 10, fontFamily: "'Space Mono', monospace", fontSize: 11, flexWrap: "wrap" }}>
        <span style={{ color: "#4a7fa0" }}>
          <span style={{ color: "#00bfff" }}>{selectedIds.size}</span> selected · <span style={{ color: "#00bfff" }}>{availableRows.length}</span> available
        </span>
        <span style={{ color: "#4a7fa0" }}>
          Selected volume: <span style={{ color: volumeColor, fontWeight: 700 }}>{formatVolume(selectedVolume)} m³</span>
        </span>
        <span style={{ color: "#4a7fa0" }}>
          After save: <span style={{ color: volumeColor, fontWeight: 700 }}>{formatVolume(projected)} / {formatVolume(certVol)} m³</span>
        </span>
        <button
          onClick={handleSave}
          disabled={!selectedIds.size || saving || !!loading}
          style={{
            marginLeft: "auto",
            background: selectedIds.size && !saving && !loading ? "#003322" : "#0a1628",
            color: selectedIds.size && !saving && !loading ? "#00ff9d" : "#4a7fa0",
            padding: "8px 20px",
            borderRadius: 6,
            fontFamily: "'Space Mono', monospace",
            fontSize: 11,
            fontWeight: 700,
            letterSpacing: 1,
            border: `1px solid ${selectedIds.size && !saving && !loading ? "#00ff9d44" : "#0d3060"}`,
            cursor: selectedIds.size && !saving && !loading ? "pointer" : "not-allowed",
          }}
        >
          {saving ? "SAVING..." : "SAVE MANUAL MATCH"}
        </button>
      </div>

      {!availableRows.length ? (
        <div style={{ color: "#4a7fa0", fontSize: 11, padding: 20, textAlign: "center" }}>
          No invoice rows match current filters (only rows with remaining volume are shown).
        </div>
      ) : (
        <div style={{ overflowX: "auto", maxHeight: 420, overflowY: "auto", border: "1px solid #0d2040", borderRadius: 6 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "'Space Mono', monospace", fontSize: 10 }}>
            <thead>
              <tr style={{ borderBottom: "1px solid #0d3060", background: "#0a1628", position: "sticky", top: 0 }}>
                {["PICK", "CSV ROW", "INVOICE", "CUSTOMER", "UPLIFT DATE", "IATA", "SUPPLIER", "REMAINING M3"].map((h) => (
                  <th key={h} style={{ padding: "6px 10px", textAlign: "left", color: "#00bfff", whiteSpace: "nowrap" }}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {availableRows.map((row, i) => {
                const isChecked = selectedIds.has(row.id);
                return (
                  <tr
                    key={row.id}
                    onClick={() => toggleRow(row.id)}
                    style={{
                      borderBottom: "1px solid #0d2040",
                      background: isChecked ? "#0a2040" : i % 2 === 0 ? "#060e1a" : "#030d1a",
                      cursor: "pointer",
                    }}
                  >
                    <td style={{ padding: "6px 10px" }}>
                      <input type="checkbox" checked={isChecked} onChange={() => toggleRow(row.id)} onClick={(e) => e.stopPropagation()} />
                    </td>
                    <td style={{ padding: "6px 10px", color: "#4a9fd4" }}>{row.row_number}</td>
                    <td style={{ padding: "6px 10px", color: "#e0f0ff" }}>{row.invoice_no || "—"}</td>
                    <td style={{ padding: "6px 10px", color: "#c8dff0" }}>{row.customer || "—"}</td>
                    <td style={{ padding: "6px 10px", color: "#888" }}>{row.uplift_date || "—"}</td>
                    <td style={{ padding: "6px 10px", color: "#c8dff0" }}>{row.iata || "—"}</td>
                    <td style={{ padding: "6px 10px", color: "#c8dff0" }}>{row.supplier || "—"}</td>
                    <td style={{ padding: "6px 10px", color: "#00ff9d", fontWeight: 700 }}>{formatVolume(row.remaining_m3)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function monthFromPeriodStart(value) {
  if (typeof value !== "string") return null;
  const m = value.match(/^(\d{4})-(\d{2})/);
  return m ? `${m[1]}-${m[2]}` : null;
}

function deriveUnitStatus(certVol, allocVol, parentStatus, excludedNoUplift) {
  if (excludedNoUplift) return "no_uplift";
  if (certVol <= 0) return parentStatus || "no_match";
  if (allocVol <= MATCH_TOLERANCE) return "unmatched";
  if (certVol - allocVol <= MATCH_TOLERANCE) {
    return parentStatus === "approved" ? "approved" : "auto_linked";
  }
  return "partial_linked";
}

function buildDashboardData(certs, invoiceRows) {
  const grid = {};
  const airports = new Set();
  const months = new Set();
  let totalCertVolume = 0;
  let totalAllocated = 0;
  const alerts = [];

  // Rank used to compute the per-cell worstStatus. "no_uplift" has negative rank so it never
  // overrides any real status sharing the same cell.
  const statusRank = { unmatched: 3, partial_linked: 2, manual_only: 1, auto_linked: 0, approved: 0, no_uplift: -1 };

  const addToGrid = (airport, month, certVolDelta, allocVolDelta, cellStatus, cert, isNoUplift) => {
    if (!airport || airport === "???" || !month || month === "???") return;
    airports.add(airport);
    months.add(month);
    const key = `${airport}|${month}`;
    if (!grid[key]) grid[key] = { certVolume: 0, allocatedVolume: 0, certs: [], worstStatus: null, hasNoUplift: false };
    grid[key].certVolume += certVolDelta;
    grid[key].allocatedVolume += allocVolDelta;
    if (!grid[key].certs.includes(cert)) grid[key].certs.push(cert);
    if (isNoUplift) grid[key].hasNoUplift = true;
    if (grid[key].worstStatus === null || (statusRank[cellStatus] ?? 0) > (statusRank[grid[key].worstStatus] ?? 0)) {
      grid[key].worstStatus = cellStatus;
    }
  };

  for (const cert of certs) {
    const match = cert.match;
    if (!match) continue;

    const certVol = Number(match.cert_volume_m3) || 0;
    const allocVol = Number(match.allocated_volume_m3) || 0;
    const status = match.status;

    if (["auto_linked", "approved", "partial_linked", "unmatched"].includes(status)) {
      // Subtract unit volumes for airport/months that have no 2025 Titan uplift (excluded_no_uplift)
      // to avoid penalising coverage for airports where no allocation is possible.
      const units = cert.allocation_units || [];
      const excludedVol = units
        .filter((u) => u.excluded_no_uplift)
        .reduce((s, u) => s + (Number(u.saf_volume_m3) || 0), 0);
      const includedCertVol = Math.max(0, certVol - excludedVol);
      totalCertVolume += includedCertVol;
      totalAllocated += allocVol;
    }

    // Populate grid: one cell per allocation unit (airport × month). Falls back to the cert's
    // canonicalAirports[0] + coverageMonth only when no allocation units are attached.
    const units = Array.isArray(cert.allocation_units) ? cert.allocation_units : [];
    if (units.length > 0) {
      for (const unit of units) {
        const airport = (unit.airport_iata || "").toUpperCase();
        const month = monthFromPeriodStart(unit.period_start);
        if (!airport || !month) continue;
        const uCertVol = Number(unit.saf_volume_m3) || 0;
        const uAllocVol = Number(unit.consumed_volume_m3) || 0;
        const excluded = !!unit.excluded_no_uplift;
        const unitStatus = deriveUnitStatus(uCertVol, uAllocVol, status, excluded);
        addToGrid(airport, month, uCertVol, uAllocVol, unitStatus, cert, excluded);
      }
    } else {
      const canonicalAirports = cert.data?.canonicalAirports || [];
      const airportCode = canonicalAirports[0]?.iata || "???";
      const month = cert.data?.coverageMonth || match.diagnostics?.coverage_month || "???";
      addToGrid(airportCode, month, certVol, allocVol, status, cert, false);
    }

    // Alerts (cert-level, unchanged)
    const canonicalAirports = cert.data?.canonicalAirports || [];
    const primaryAirport = canonicalAirports[0]?.iata || "???";
    const primaryMonth = cert.data?.coverageMonth || match.diagnostics?.coverage_month || "???";
    if (certVol > 50) {
      alerts.push({ type: "volume", cert, message: `${primaryAirport} ${primaryMonth}: volume ${certVol.toFixed(1)} m³ exceeds plausibility threshold` });
    }
    if (status === "partial_linked" && certVol > 0 && allocVol / certVol < 0.10 && !match.match_method?.includes("widened")) {
      alerts.push({ type: "gap", cert, message: `${primaryAirport} ${primaryMonth}: only ${(allocVol / certVol * 100).toFixed(1)}% allocated — probable extraction error` });
    }
    if (status === "unmatched") {
      const totalRows = Number(match.diagnostics?.total_row_count) || 0;
      alerts.push({
        type: "unmatched", cert,
        message: totalRows > 0
          ? `${primaryAirport} ${primaryMonth}: ${totalRows} invoice rows found but all SAF volume consumed by other certs`
          : `${primaryAirport} ${primaryMonth}: no invoice rows found for this airport/month`
      });
    }
    if (status === "manual_only" && cert.document_family === "manual_only") {
      alerts.push({ type: "manual", cert, message: `${cert.unique_number?.slice(-20) || "?"}: ${cert.classification_reason || "requires manual review"}` });
    }
  }

  return {
    grid,
    airports: [...airports].sort(),
    months: [...months].sort(),
    totalCertVolume,
    totalAllocated,
    gap: totalCertVolume - totalAllocated,
    coveragePercent: totalCertVolume > 0 ? (totalAllocated / totalCertVolume) * 100 : 0,
    certsOk: certs.filter((c) => c.match?.status === "approved").length,
    certsAttention: certs.filter((c) => ["partial_linked", "unmatched", "manual_only"].includes(c.match?.status)).length,
    alerts,
  };
}

function DashboardTab({ certs, invoiceRows, onSelectCert, onSwitchTab }) {
  const data = buildDashboardData(certs, invoiceRows);
  const monthNames = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
  const fmtMonth = (m) => {
    if (!/^\d{4}-\d{2}$/.test(m)) return m;
    const [y, mo] = m.split("-");
    return `${monthNames[Number(mo) - 1]} ${y.slice(2)}`;
  };

  const cellColor = (cell) => {
    if (!cell) return { bg: "#0a1628", fg: "#334", label: "—" };
    if (cell.worstStatus === "approved" || cell.worstStatus === "auto_linked") return { bg: "#001a0d", fg: "#00ff9d", label: "OK" };
    if (cell.worstStatus === "partial_linked") return { bg: "#1a0d00", fg: "#ff9933", label: "PARTIAL" };
    if (cell.worstStatus === "unmatched") return { bg: "#1a0000", fg: "#ff6666", label: "UNMATCHED" };
    if (cell.worstStatus === "manual_only") return { bg: "#1a1200", fg: "#ffbb00", label: "MANUAL" };
    if (cell.worstStatus === "no_uplift") return { bg: "#0a1020", fg: "#556680", label: "NO UPLIFT" };
    return { bg: "#0a1628", fg: "#4a7fa0", label: "?" };
  };

  const kpiStyle = { flex: 1, padding: "12px 20px", borderRight: "1px solid #0d2040", textAlign: "center" };
  const kpiVal = { fontFamily: "'Space Mono', monospace", fontSize: 20, fontWeight: 700 };
  const kpiLabel = { color: "#4a7fa0", fontSize: 9, letterSpacing: 1, marginTop: 2 };

  const covColor = data.coveragePercent >= 95 ? "#00ff9d" : data.coveragePercent >= 50 ? "#ff9933" : "#ff6666";

  return (
    <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
      {/* KPIs */}
      <div style={{ display: "flex", gap: 1, background: "#030d1a", borderRadius: 8, border: "1px solid #0d2040", marginBottom: 24, overflow: "hidden" }}>
        <div style={kpiStyle}><div style={{ ...kpiVal, color: "#00bfff" }}>{data.totalCertVolume.toFixed(3)}</div><div style={kpiLabel}>CERTIFIED M³</div></div>
        <div style={kpiStyle}><div style={{ ...kpiVal, color: "#00ff9d" }}>{data.totalAllocated.toFixed(3)}</div><div style={kpiLabel}>ALLOCATED M³</div></div>
        <div style={kpiStyle}><div style={{ ...kpiVal, color: data.gap > 0.01 ? "#ff6666" : "#00ff9d" }}>{data.gap.toFixed(3)}</div><div style={kpiLabel}>GAP M³</div></div>
        <div style={kpiStyle}><div style={{ ...kpiVal, color: covColor }}>{data.coveragePercent.toFixed(1)}%</div><div style={kpiLabel}>COVERAGE</div></div>
        <div style={kpiStyle}><div style={{ ...kpiVal, color: "#00ff9d" }}>{data.certsOk}</div><div style={kpiLabel}>CERTS OK</div></div>
        <div style={{ ...kpiStyle, borderRight: "none" }}><div style={{ ...kpiVal, color: data.certsAttention > 0 ? "#ff9933" : "#00ff9d" }}>{data.certsAttention}</div><div style={kpiLabel}>NEED ATTENTION</div></div>
      </div>

      {/* Airport × Month Grid */}
      <div style={{ marginBottom: 24 }}>
        <div style={{ color: "#00bfff", fontSize: 11, letterSpacing: 2, marginBottom: 12, fontFamily: "'Space Mono', monospace" }}>COVERAGE BY AIRPORT & MONTH</div>
        <div style={{ overflowX: "auto" }}>
          <table style={{ borderCollapse: "collapse", fontFamily: "'Space Mono', monospace", fontSize: 10, width: "100%" }}>
            <thead>
              <tr>
                <th style={{ padding: "8px 12px", textAlign: "left", color: "#00bfff", borderBottom: "2px solid #0d3060" }}>AIRPORT</th>
                {data.months.map((m) => <th key={m} style={{ padding: "8px 10px", textAlign: "center", color: "#00bfff", borderBottom: "2px solid #0d3060", whiteSpace: "nowrap" }}>{fmtMonth(m)}</th>)}
              </tr>
            </thead>
            <tbody>
              {data.airports.map((airport) => (
                <tr key={airport} style={{ borderBottom: "1px solid #0d2040" }}>
                  <td style={{ padding: "8px 12px", color: "#e0f0ff", fontWeight: 600 }}>{airport}</td>
                  {data.months.map((m) => {
                    const cell = data.grid[`${airport}|${m}`];
                    const { bg, fg, label } = cellColor(cell);
                    return (
                      <td key={m} style={{ padding: "6px 8px", textAlign: "center" }}
                        title={cell ? `Cert: ${cell.certVolume.toFixed(3)} m³\nAllocated: ${cell.allocatedVolume.toFixed(3)} m³\nGap: ${(cell.certVolume - cell.allocatedVolume).toFixed(3)} m³${cell.hasNoUplift ? "\n(no Titan uplift at this airport/month)" : ""}` : "No certificate"}>
                        <span style={{ display: "inline-block", padding: "3px 8px", borderRadius: 4, background: bg, color: fg, fontSize: 9, fontWeight: 700, minWidth: 58 }}>
                          {cell ? `${cell.allocatedVolume.toFixed(3)}` : "—"}
                        </span>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Alerts */}
      {data.alerts.length > 0 && (
        <div>
          <div style={{ color: "#ff9933", fontSize: 11, letterSpacing: 2, marginBottom: 12, fontFamily: "'Space Mono', monospace" }}>ATTENTION REQUIRED ({data.alerts.length})</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {data.alerts.map((alert, i) => {
              const iconMap = { volume: "⚠", gap: "📉", unmatched: "❌", manual: "🔧" };
              const colorMap = { volume: "#ff9933", gap: "#ff9933", unmatched: "#ff6666", manual: "#ffbb00" };
              return (
                <div key={i}
                  onClick={() => {
                    if (alert.cert) {
                      const idx = certs.indexOf(alert.cert);
                      if (idx >= 0) { onSelectCert(idx); onSwitchTab("certs"); }
                    }
                  }}
                  style={{
                    padding: "8px 14px", background: "#060e1a", border: `1px solid ${colorMap[alert.type] || "#0d3060"}33`,
                    borderLeft: `3px solid ${colorMap[alert.type] || "#0d3060"}`, borderRadius: 6, cursor: "pointer",
                    color: "#c8dff0", fontSize: 11, fontFamily: "'Space Mono', monospace",
                    transition: "background 0.15s",
                  }}>
                  <span style={{ marginRight: 8 }}>{iconMap[alert.type] || "•"}</span>
                  <span style={{ color: colorMap[alert.type] || "#c8dff0" }}>{alert.message}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

function exportInvoiceRowsCSV(rows) {
  const headers = ["Status", "CSV Row", "Invoice", "Customer", "Uplift Date", "IATA", "ICAO", "Country", "Supplier", "Uplift M3", "SAF M3", "Remaining SAF M3", "Linked Certificate"];
  const csvRows = [headers.join(",")];
  for (const row of rows) {
    const vals = [
      row.allocation_status || "free",
      row.row_number || "",
      `"${(row.invoice_no || "").replace(/"/g, '""')}"`,
      `"${(row.customer || "").replace(/"/g, '""')}"`,
      row.uplift_date || "",
      row.iata || "",
      row.icao || "",
      row.country || "",
      `"${(row.supplier || "").replace(/"/g, '""')}"`,
      row.vol_m3 || "",
      row.saf_vol_m3 || "",
      row.remaining_m3 ?? "",
      `"${(row.linked_cert_numbers || []).join("; ")}"`,
    ];
    csvRows.push(vals.join(","));
  }
  const blob = new Blob([csvRows.join("\n")], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `invoice_rows_export_${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

function buildCertDetailData(certs, invoiceRows) {
  const getRowMonth = (row) => {
    if (!row.uplift_date) return null;
    const d = new Date(row.uplift_date);
    if (isNaN(d)) return null;
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
  };
  const invoicePools = new Map();
  for (const row of invoiceRows || []) {
    const key = `${(row.iata || "").toUpperCase()}|${getRowMonth(row)}`;
    const pool = invoicePools.get(key) || { totalSaf: 0, rowCount: 0 };
    pool.totalSaf += Number(row.saf_vol_m3) || 0;
    pool.rowCount++;
    invoicePools.set(key, pool);
  }
  const result = [];
  for (const cert of certs) {
    const isPoc = (cert.document_family || cert.data?.document_family) === "supported_poc";
    const units = Array.isArray(cert.allocation_units) ? cert.allocation_units : [];
    if (isPoc && units.length > 0) {
      for (const unit of units) {
        const airport = (unit.airport_iata || "???").toUpperCase();
        const month = monthFromPeriodStart(unit.period_start) || "???";
        const pool = invoicePools.get(`${airport}|${month}`);
        const certVol = Number(unit.saf_volume_m3) || 0;
        const allocVol = Number(unit.consumed_volume_m3) || 0;
        result.push({
          airport, month,
          supplier: cert.data?.safSupplier || "—",
          uniqueNumber: cert.unique_number || "—",
          unitIndex: unit.unit_index ?? null,
          docType: cert.data?.docType || "—",
          rawMaterial: cert.data?.rawMaterial || "—",
          certVolume: certVol,
          invoiceSaf: pool?.totalSaf || 0,
          invoiceRowCount: pool?.rowCount || 0,
          allocated: allocVol,
          gap: certVol - allocVol,
          status: deriveUnitStatus(certVol, allocVol, cert.match?.status, !!unit.excluded_no_uplift),
          matchMethod: cert.match?.match_method || "poc-unit",
          coverage: certVol > 0 ? (allocVol / certVol) * 100 : 0,
          cert,
          isDuplicate: false,
        });
      }
    } else {
      const airport = cert.data?.canonicalAirports?.[0]?.iata || "???";
      const month = cert.data?.coverageMonth || "???";
      const pool = invoicePools.get(`${airport}|${month}`);
      const certVol = Number(cert.match?.cert_volume_m3) || 0;
      const allocVol = Number(cert.match?.allocated_volume_m3) || 0;
      result.push({
        airport, month,
        supplier: cert.data?.safSupplier || "—",
        uniqueNumber: cert.unique_number || "—",
        unitIndex: null,
        docType: cert.data?.docType || "—",
        rawMaterial: cert.data?.rawMaterial || "—",
        certVolume: certVol,
        invoiceSaf: pool?.totalSaf || 0,
        invoiceRowCount: pool?.rowCount || 0,
        allocated: allocVol,
        gap: certVol - allocVol,
        status: cert.match?.status || "no_match",
        matchMethod: cert.match?.match_method || "—",
        coverage: certVol > 0 ? (allocVol / certVol) * 100 : 0,
        cert,
        isDuplicate: false,
      });
    }
  }
  result.sort((a, b) => a.airport.localeCompare(b.airport) || a.month.localeCompare(b.month) || b.gap - a.gap);

  // Mark exact duplicates: same airport + month + volume
  const seen = new Map();
  for (const row of result) {
    const dupKey = `${row.airport}|${row.month}|${row.certVolume.toFixed(3)}`;
    if (seen.has(dupKey)) {
      row.isDuplicate = true;
      seen.get(dupKey).isDuplicate = true;
    } else {
      seen.set(dupKey, row);
    }
  }

  return result;
}

function exportCertDetailCSV(rows) {
  const headers = ["Airport", "Month", "Supplier", "Certificate", "Doc Type", "Raw Material", "Cert Volume M3", "Invoice SAF M3", "Invoice Rows", "Allocated M3", "Gap M3", "Coverage %", "Status", "Match Method"];
  const csvRows = [headers.join(",")];
  for (const r of rows) {
    csvRows.push([r.airport, r.month, `"${(r.supplier || "").replace(/"/g, '""')}"`, `"${r.uniqueNumber}"`, r.docType, `"${r.rawMaterial}"`, r.certVolume.toFixed(3), r.invoiceSaf.toFixed(3), r.invoiceRowCount, r.allocated.toFixed(3), r.gap.toFixed(3), r.coverage.toFixed(1), r.status, r.matchMethod].join(","));
  }
  const blob = new Blob([csvRows.join("\n")], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `saf_cert_detail_${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

function CertDetailTable({ certs, invoiceRows, onSelectCert, onSwitchTab }) {
  const [airportFilter, setAirportFilter] = React.useState("");
  const [monthFilter, setMonthFilter] = React.useState("");
  const [statusFilter, setStatusFilter] = React.useState("");
  const allRows = buildCertDetailData(certs, invoiceRows);
  const filtered = allRows.filter((r) => {
    if (airportFilter && r.airport !== airportFilter) return false;
    if (monthFilter && r.month !== monthFilter) return false;
    if (statusFilter && r.status !== statusFilter) return false;
    return true;
  });
  const uniqueAirports = [...new Set(allRows.map((r) => r.airport).filter((a) => a !== "???"))].sort();
  const uniqueMonths = [...new Set(allRows.map((r) => r.month).filter((m) => m !== "???"))].sort();
  const uniqueStatuses = [...new Set(allRows.map((r) => r.status))].sort();
  const totals = filtered.reduce((acc, r) => ({ certVol: acc.certVol + r.certVolume, invoiceSaf: acc.invoiceSaf + r.invoiceSaf, alloc: acc.alloc + r.allocated, gap: acc.gap + r.gap }), { certVol: 0, invoiceSaf: 0, alloc: 0, gap: 0 });
  const monthNames = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
  const fmtMonth = (m) => { if (!/^\d{4}-\d{2}$/.test(m)) return m; const [y, mo] = m.split("-"); return `${monthNames[Number(mo) - 1]} ${y.slice(2)}`; };
  const selectStyle = { background: "#0a1628", color: "#c8dff0", border: "1px solid #0d3060", borderRadius: 6, padding: "6px 10px", fontSize: 11, fontFamily: "'Space Mono', monospace" };
  const statusColor = (s) => ({ approved: "#00ff9d", auto_linked: "#00ff9d", partial_linked: "#ff9933", unmatched: "#ff6666", manual_only: "#ffbb00", no_uplift: "#6b7a8f" }[s] || "#4a7fa0");

  return (
    <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 16, flexWrap: "wrap", fontFamily: "'Space Mono', monospace", fontSize: 11 }}>
        <select value={airportFilter} onChange={(e) => setAirportFilter(e.target.value)} style={selectStyle}>
          <option value="">All airports</option>
          {uniqueAirports.map((a) => <option key={a} value={a}>{a}</option>)}
        </select>
        <select value={monthFilter} onChange={(e) => setMonthFilter(e.target.value)} style={selectStyle}>
          <option value="">All months</option>
          {uniqueMonths.map((m) => <option key={m} value={m}>{fmtMonth(m)}</option>)}
        </select>
        <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)} style={selectStyle}>
          <option value="">All statuses</option>
          {uniqueStatuses.map((s) => <option key={s} value={s}>{s}</option>)}
        </select>
        <button className="btn" onClick={() => exportCertDetailCSV(filtered)} style={{ background: "#0a1628", color: "#00bfff", padding: "5px 12px", borderRadius: 5, fontFamily: "'Space Mono', monospace", fontSize: 10, letterSpacing: 1, border: "1px solid #0d3060", cursor: "pointer" }}>EXPORT CSV</button>
        <span style={{ color: "#4a7fa0", marginLeft: "auto" }}>Showing <span style={{ color: "#00bfff" }}>{filtered.length}</span> of <span style={{ color: "#00bfff" }}>{allRows.length}</span> rows</span>
      </div>
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "'Space Mono', monospace", fontSize: 10 }}>
          <thead>
            <tr style={{ borderBottom: "2px solid #0d3060" }}>
              {["AIRPORT", "MONTH", "SUPPLIER", "CERTIFICATE", "TYPE", "MATERIAL", "CERT M³", "INV SAF M³", "ROWS", "ALLOC M³", "GAP M³", "COV %", "STATUS", "METHOD"].map((h) => (
                <th key={h} style={{ padding: "8px 8px", textAlign: "left", color: "#00bfff", whiteSpace: "nowrap" }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map((r, i) => (
              <tr key={`${r.uniqueNumber}-${r.unitIndex ?? ""}-${r.airport}-${r.month}-${i}`} onClick={() => { const idx = certs.indexOf(r.cert); if (idx >= 0) { onSelectCert(idx); onSwitchTab("certs"); } }}
                style={{ borderBottom: "1px solid #0d2040", background: r.isDuplicate ? "#1a0a0a" : (i % 2 === 0 ? "#060e1a" : "#030d1a"), cursor: "pointer", borderLeft: `3px solid ${r.isDuplicate ? "#ff4444" : statusColor(r.status)}` }}>
                <td style={{ padding: "6px 8px", color: "#e0f0ff", fontWeight: 600 }}>{r.airport}{r.isDuplicate ? <span style={{ color: "#ff4444", fontSize: 9, marginLeft: 4, fontWeight: 700 }} title="Exact duplicate: same airport + month + volume as another certificate">DUP</span> : null}</td>
                <td style={{ padding: "6px 8px", color: "#c8dff0" }}>{fmtMonth(r.month)}</td>
                <td style={{ padding: "6px 8px", color: "#888", maxWidth: 120, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={r.supplier}>{r.supplier}</td>
                <td style={{ padding: "6px 8px", color: "#00bfff", maxWidth: 140, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={r.uniqueNumber}>{r.uniqueNumber.slice(-18)}</td>
                <td style={{ padding: "6px 8px", color: "#888" }}>{r.docType}</td>
                <td style={{ padding: "6px 8px", color: "#888", maxWidth: 80, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={r.rawMaterial}>{r.rawMaterial}</td>
                <td style={{ padding: "6px 8px", color: "#e0f0ff", fontWeight: 700 }}>{r.certVolume.toFixed(3)}</td>
                <td style={{ padding: "6px 8px", color: "#4a9fd4" }}>{r.invoiceSaf.toFixed(3)}</td>
                <td style={{ padding: "6px 8px", color: "#888" }}>{r.invoiceRowCount}</td>
                <td style={{ padding: "6px 8px", color: "#00ff9d", fontWeight: 700 }}>{r.allocated.toFixed(3)}</td>
                <td style={{ padding: "6px 8px", color: r.gap > 0.001 ? "#ff6666" : "#00ff9d", fontWeight: 700 }}>{r.gap.toFixed(3)}</td>
                <td style={{ padding: "6px 8px", color: r.coverage >= 95 ? "#00ff9d" : r.coverage >= 50 ? "#ff9933" : "#ff6666" }}>{r.coverage.toFixed(0)}%</td>
                <td style={{ padding: "6px 8px" }}><Badge status={r.status} /></td>
                <td style={{ padding: "6px 8px", color: r.matchMethod.includes("widened") ? "#ff9933" : "#888", fontSize: 9 }}>{r.matchMethod.includes("widened") ? "WIDENED" : r.matchMethod === "fifo-monthly-partial" ? "MONTH" : r.matchMethod === "fifo-quarterly-partial" ? "QUARTER" : r.matchMethod.split("-")[0] || "—"}</td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr style={{ borderTop: "2px solid #0d3060", background: "#0a1628" }}>
              <td colSpan={6} style={{ padding: "8px 8px", color: "#00bfff", fontWeight: 700 }}>TOTALS ({filtered.length} certs)</td>
              <td style={{ padding: "8px 8px", color: "#e0f0ff", fontWeight: 700 }}>{totals.certVol.toFixed(3)}</td>
              <td style={{ padding: "8px 8px", color: "#4a9fd4" }}>{totals.invoiceSaf.toFixed(3)}</td>
              <td style={{ padding: "8px 8px" }}></td>
              <td style={{ padding: "8px 8px", color: "#00ff9d", fontWeight: 700 }}>{totals.alloc.toFixed(3)}</td>
              <td style={{ padding: "8px 8px", color: totals.gap > 0.001 ? "#ff6666" : "#00ff9d", fontWeight: 700 }}>{totals.gap.toFixed(3)}</td>
              <td style={{ padding: "8px 8px", color: totals.certVol > 0 ? (totals.alloc / totals.certVol * 100 >= 95 ? "#00ff9d" : "#ff9933") : "#888" }}>{totals.certVol > 0 ? (totals.alloc / totals.certVol * 100).toFixed(0) : 0}%</td>
              <td colSpan={2}></td>
            </tr>
          </tfoot>
        </table>
      </div>
    </div>
  );
}

function CertCard({ cert, index, selected, onSelect, onAnalyze, onReExtract, onOpenPdf, hasClientCert }) {
  const status = cert.match?.status;
  const isCompleted = status === "approved" && hasClientCert;
  const borderLeftColor =
    isCompleted ? "#00ff9d" :
    status === "approved" ? "#00cc7a" :
    status === "auto_linked" ? "#00bfff" :
    status === "partial_linked" ? "#ff9933" :
    status === "unmatched" ? "#ff6666" :
    status === "manual_only" ? "#ffbb00" : "#334";
  return (
    <div
      onClick={() => onSelect(index)}
      style={{
        background: selected ? "#0a1628" : "#060e1a",
        borderTop: selected ? "1px solid #00bfff" : "1px solid #0d2040",
        borderRight: selected ? "1px solid #00bfff" : "1px solid #0d2040",
        borderBottom: selected ? "1px solid #00bfff" : "1px solid #0d2040",
        borderLeft: `4px solid ${borderLeftColor}`,
        borderRadius: 8,
        padding: "14px 18px",
        cursor: "pointer",
        marginBottom: 8,
        transition: "all 0.15s",
        boxShadow: selected ? "0 0 16px #00bfff33" : "none",
        opacity: isCompleted ? 0.5 : 1,
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 8 }}>
        <div style={{ minWidth: 0 }}>
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
                wordBreak: "break-all",
              }}
            >
              {certTitle(cert)}
            </button>
          ) : (
            <div style={{ color: "#e0f0ff", fontWeight: 600, fontSize: 13, wordBreak: "break-all" }}>{certTitle(cert)}</div>
          )}
          <div style={{ color: "#4a7fa0", fontSize: 11, marginTop: 3 }}>
            {formatVolume(cert.data?.quantity)} {cert.data?.quantityUnit || "m3"}
            {Number(cert.data?.quantity) > 50 ? <span style={{ color: "#ff9933", marginLeft: 6, fontSize: 9, fontWeight: 700 }} title="Volume exceeds 50 m³ — verify PDF extraction. This is likely a number format error.">⚠ VOLUME?</span> : null}
            {cert.match?.status === "partial_linked" && Number(cert.match?.cert_volume_m3) > 0 && Number(cert.match?.allocated_volume_m3) / Number(cert.match?.cert_volume_m3) < 0.10 && !cert.match?.match_method?.includes("widened") ? <span style={{ color: "#ff9933", marginLeft: 6, fontSize: 9, fontWeight: 700 }} title="Only a fraction of the volume was allocated — probable extraction error">⚠ DATA?</span> : null}
          </div>
          <div style={{ color: "#4a7fa0", fontSize: 10, marginTop: 3, wordBreak: "break-word" }}>
            {cert.support_reason || cert.data?.support_reason || "Not processed yet"}
          </div>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 6, alignItems: "flex-end", flexShrink: 0 }}>
          <Badge status={cert.document_family || cert.data?.document_family || "unknown"} />
          {status ? <Badge status={status} /> : null}
          {cert.match?.match_method?.includes("widened") ? <Badge status="widened" /> : null}
          {isCompleted ? (
            <span style={{ color: "#00ff9d", fontSize: 10, fontFamily: "'Space Mono', monospace", fontWeight: 700 }}>✓ DONE</span>
          ) : (
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
              RUN
            </button>
          )}
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

function formatShortMonth(monthKey) {
  if (!/^\d{4}-\d{2}$/.test(monthKey)) return monthKey;
  const [y, m] = monthKey.split("-");
  const names = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
  return `${names[Number(m) - 1]} ${y.slice(2)}`;
}

function CoverageTable({ coverageData, airportFilter, setAirportFilter, clientSearch, setClientSearch }) {
  const { months, airports, airportMap } = coverageData;
  const searchLower = (clientSearch || "").toLowerCase();

  const filteredAirports = airportFilter ? airports.filter((a) => a === airportFilter) : airports;

  let grandTotal = 0;
  const grandMonthTotals = {};
  for (const m of months) grandMonthTotals[m] = 0;

  const airportSections = filteredAirports.map((airport) => {
    const clientMap = airportMap.get(airport);
    if (!clientMap) return null;

    let clients = [...clientMap.entries()].map(([name, monthMap]) => ({ name, monthMap }));
    if (searchLower) clients = clients.filter((c) => c.name.toLowerCase().includes(searchLower));
    if (!clients.length) return null;

    clients.sort((a, b) => a.name.localeCompare(b.name));

    const subtotalByMonth = {};
    for (const m of months) subtotalByMonth[m] = 0;
    let subtotal = 0;

    const rows = clients.map((client) => {
      let rowTotal = 0;
      const cells = months.map((m) => {
        const cell = client.monthMap.get(m);
        if (!cell) return { volume: 0, status: "none" };
        rowTotal += cell.volume;
        subtotalByMonth[m] += cell.volume;
        grandMonthTotals[m] += cell.volume;
        return { volume: cell.volume, status: cell.hasClientCert ? "generated" : "approved" };
      });
      subtotal += rowTotal;
      grandTotal += rowTotal;
      return { name: client.name, cells, total: rowTotal };
    });

    return { airport, rows, subtotalByMonth, subtotal };
  }).filter(Boolean);

  const totalClients = airportSections.reduce((sum, s) => sum + s.rows.length, 0);

  if (!months.length) {
    return (
      <div style={{ flex: 1, padding: 60, textAlign: "center", color: "#4a7fa0" }}>
        <div style={{ fontSize: 36, marginBottom: 12 }}>📊</div>
        <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 11 }}>
          No approved allocations found. Run matching on certificates to populate this view.
        </div>
      </div>
    );
  }

  const thStyle = {
    padding: "8px 10px", textAlign: "right", color: "#4a7fa0", fontSize: 9,
    letterSpacing: 1, fontWeight: 600, position: "sticky", top: 0, background: "#060e1a", zIndex: 2,
  };
  const tdStyle = { padding: "6px 10px", textAlign: "right", fontSize: 11, borderBottom: "1px solid #0a1a2e" };
  const stickyCol = { position: "sticky", left: 0, background: "#060e1a", zIndex: 1 };

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div style={{ display: "flex", gap: 12, padding: "12px 20px", borderBottom: "1px solid #0d2040", alignItems: "center", flexShrink: 0 }}>
        <select
          value={airportFilter}
          onChange={(e) => setAirportFilter(e.target.value)}
          style={{
            background: "#0a1628", color: "#c8dff0", border: "1px solid #0d3060", borderRadius: 5,
            padding: "6px 10px", fontFamily: "'Space Mono', monospace", fontSize: 11,
          }}
        >
          <option value="">All airports ({airports.length})</option>
          {airports.map((a) => <option key={a} value={a}>{a}</option>)}
        </select>
        <input
          type="text"
          placeholder="Search client..."
          value={clientSearch}
          onChange={(e) => setClientSearch(e.target.value)}
          style={{
            background: "#0a1628", color: "#c8dff0", border: "1px solid #0d3060", borderRadius: 5,
            padding: "6px 10px", fontFamily: "'Space Mono', monospace", fontSize: 11, width: 200,
          }}
        />
        <div style={{ color: "#4a7fa0", fontSize: 10, fontFamily: "'Space Mono', monospace" }}>
          {totalClients} clients · {filteredAirports.length} airports · {months.length} months
        </div>
        <div style={{ marginLeft: "auto", display: "flex", gap: 16, fontSize: 10, fontFamily: "'Space Mono', monospace" }}>
          <span style={{ color: "#4a7fa0" }}>
            <span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: "#00ff9d", marginRight: 4 }} />
            Client cert generated
          </span>
          <span style={{ color: "#4a7fa0" }}>
            <span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: "#ffa500", marginRight: 4 }} />
            Approved, not generated
          </span>
        </div>
      </div>
      <div style={{ flex: 1, overflow: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "'Space Mono', monospace" }}>
          <thead>
            <tr>
              <th style={{ ...thStyle, textAlign: "left", minWidth: 200, ...stickyCol, zIndex: 3 }}>CLIENT / AIRPORT</th>
              {months.map((m) => <th key={m} style={thStyle}>{formatShortMonth(m)}</th>)}
              <th style={{ ...thStyle, color: "#00bfff" }}>TOTAL</th>
            </tr>
          </thead>
          <tbody>
            {airportSections.map((section) => (
              <React.Fragment key={section.airport}>
                <tr>
                  <td
                    colSpan={months.length + 2}
                    style={{
                      padding: "10px 10px 6px", fontWeight: 700, fontSize: 12, color: "#00bfff",
                      borderBottom: "1px solid #0d2040", letterSpacing: 1, ...stickyCol,
                    }}
                  >
                    ✈ {section.airport}
                  </td>
                </tr>
                {section.rows.map((row) => (
                  <tr key={row.name} style={{ cursor: "default" }} onMouseEnter={(e) => { e.currentTarget.style.background = "#0a1628"; }} onMouseLeave={(e) => { e.currentTarget.style.background = ""; }}>
                    <td style={{ ...tdStyle, textAlign: "left", color: "#c8dff0", paddingLeft: 20, ...stickyCol }}>
                      {row.name}
                    </td>
                    {row.cells.map((cell, ci) => (
                      <td key={months[ci]} style={tdStyle}>
                        {cell.status === "none" ? (
                          <span style={{ color: "#1a2a3e" }}>—</span>
                        ) : (
                          <span style={{ color: cell.status === "generated" ? "#00ff9d" : "#ffa500" }}>
                            <span style={{
                              display: "inline-block", width: 6, height: 6, borderRadius: "50%",
                              background: cell.status === "generated" ? "#00ff9d" : "#ffa500",
                              marginRight: 4, verticalAlign: "middle",
                            }} />
                            {cell.volume.toFixed(3)}
                          </span>
                        )}
                      </td>
                    ))}
                    <td style={{ ...tdStyle, color: "#e0f0ff", fontWeight: 600 }}>{row.total.toFixed(3)}</td>
                  </tr>
                ))}
                <tr style={{ borderBottom: "2px solid #0d2040" }}>
                  <td style={{ ...tdStyle, textAlign: "left", color: "#4a7fa0", fontWeight: 600, paddingLeft: 20, fontSize: 10, ...stickyCol }}>
                    SUBTOTAL {section.airport}
                  </td>
                  {months.map((m) => (
                    <td key={m} style={{ ...tdStyle, color: "#4a7fa0", fontWeight: 600, fontSize: 10 }}>
                      {section.subtotalByMonth[m] ? section.subtotalByMonth[m].toFixed(3) : "—"}
                    </td>
                  ))}
                  <td style={{ ...tdStyle, color: "#4a9fd4", fontWeight: 700, fontSize: 10 }}>
                    {section.subtotal.toFixed(3)}
                  </td>
                </tr>
              </React.Fragment>
            ))}
            <tr style={{ background: "#030d1a" }}>
              <td style={{ padding: "10px 10px", textAlign: "left", fontWeight: 700, color: "#00bfff", fontSize: 11, ...stickyCol, background: "#030d1a" }}>
                GRAND TOTAL
              </td>
              {months.map((m) => (
                <td key={m} style={{ padding: "10px 10px", textAlign: "right", fontWeight: 700, color: "#00bfff", fontSize: 11 }}>
                  {grandMonthTotals[m] ? grandMonthTotals[m].toFixed(3) : "—"}
                </td>
              ))}
              <td style={{ padding: "10px 10px", textAlign: "right", fontWeight: 700, color: "#00bfff", fontSize: 12 }}>
                {grandTotal.toFixed(3)}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function SAFManager({ onLogout, userEmail }) {
  const [certs, setCerts] = useState([]);
  const [selected, setSelected] = useState(null);
  const [certStatusFilter, setCertStatusFilter] = useState("");
  const [invoiceRows, setInvoiceRows] = useState([]);
  const [invoiceImport, setInvoiceImport] = useState(null);
  const [clientCertificateRecords, setClientCertificateRecords] = useState([]);
  const [companiesByName, setCompaniesByName] = useState(new Map());
  const [loading, setLoading] = useState("");
  const [tab, setTab] = useState("certs");
  const [log, setLog] = useState([]);
  const [expandedCandidates, setExpandedCandidates] = useState({});
  const [pdfPreview, setPdfPreview] = useState(null);
  const [coverageAirportFilter, setCoverageAirportFilter] = useState("");
  const [coverageClientSearch, setCoverageClientSearch] = useState("");
  const [invoiceStatusFilter, setInvoiceStatusFilter] = useState("");
  const [invoiceAirportFilter, setInvoiceAirportFilter] = useState("");
  const [invoiceCustomerSearch, setInvoiceCustomerSearch] = useState("");
  const [invoiceMonthFilter, setInvoiceMonthFilter] = useState("");
  const pdfInputRef = useRef();
  const csvInputRef = useRef();
  const initialLoadStartedRef = useRef(false);
  const loadRequestRef = useRef(0);
  const loadedInvoiceImportId = invoiceImport?.id || null;
  const allocableInvoiceRowCount = invoiceRows.filter((row) => isRowAvailable(row)).length;

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

  const syncAllocationUnitConsumption = useCallback(async (certificateId, providedUnits = null, providedLinks = null) => {
    if (!certificateId) return [];

    let units = providedUnits;
    if (!units) {
      const { data, error } = await supabase
        .from("certificate_allocation_units")
        .select("*")
        .eq("certificate_id", certificateId)
        .order("unit_index", { ascending: true });
      if (error) {
        if (isMissingTableError(error)) return [];
        throw error;
      }
      units = data || [];
    }

    if (!units.length) return [];

    let links = providedLinks;
    if (!links) {
      const { data, error } = await supabase.from("certificate_invoice_links").select("*").eq("certificate_id", certificateId);
      if (error) {
        if (isMissingTableError(error)) links = [];
        else throw error;
      } else {
        links = data || [];
      }
    }

    const hydrated = hydrateAllocationUnitsWithConsumption(units, links || [], certificateId);
    for (const unit of hydrated) {
      const { error } = await supabase
        .from("certificate_allocation_units")
        .update({
          consumed_volume_m3: unit.consumed_volume_m3,
          remaining_volume_m3: unit.remaining_volume_m3,
          updated_at: new Date().toISOString(),
        })
        .eq("id", unit.id);
      if (error && !isMissingTableError(error)) throw error;
    }

    return hydrated;
  }, []);

  const syncCertificateAllocationUnits = useCallback(
    async (certificateLike, options = {}) => {
      if (!certificateLike?.id) return [];

      const { data: existingUnits, error: existingErr } = await supabase
        .from("certificate_allocation_units")
        .select("*")
        .eq("certificate_id", certificateLike.id)
        .order("unit_index", { ascending: true });
      if (existingErr) {
        if (isMissingTableError(existingErr)) return [];
        throw existingErr;
      }

      if (options.preserveExisting && (existingUnits || []).length) {
        return existingUnits || [];
      }

      const drafts = buildCertificateAllocationUnitDrafts(certificateLike);
      const existingByIndex = new Map((existingUnits || []).map((unit) => [unit.unit_index, unit]));
      const payloads = drafts.map((draft) => {
        const existing = existingByIndex.get(draft.unit_index);
        return {
          certificate_id: certificateLike.id,
          unit_index: draft.unit_index,
          unit_type: draft.unit_type,
          airport_iata: draft.airport_iata,
          airport_icao: draft.airport_icao,
          airport_name: draft.airport_name,
          period_start: normalizeDateForDB(draft.period_start),
          period_end: normalizeDateForDB(draft.period_end),
          dispatch_date: normalizeDateForDB(draft.dispatch_date),
          saf_volume_m3: draft.saf_volume_m3,
          jet_volume_m3: draft.jet_volume_m3,
          source_reference: draft.source_reference,
          matching_mode_override: draft.matching_mode_override,
          review_required: draft.review_required,
          normalization_warning: draft.normalization_warning,
          consumed_volume_m3: existing?.consumed_volume_m3 || 0,
          remaining_volume_m3:
            draft.saf_volume_m3 !== null && draft.saf_volume_m3 !== undefined
              ? Number(Math.max(0, Number(draft.saf_volume_m3 || 0) - Number(existing?.consumed_volume_m3 || 0)).toFixed(6))
              : null,
          updated_at: new Date().toISOString(),
        };
      });

      if (payloads.length) {
        const { error } = await supabase
          .from("certificate_allocation_units")
          .upsert(payloads, { onConflict: "certificate_id,unit_index" });
        if (error) throw error;
      }

      const staleIds = (existingUnits || [])
        .filter((unit) => !drafts.some((draft) => draft.unit_index === unit.unit_index))
        .map((unit) => unit.id);
      if (staleIds.length) {
        await deleteByIds("certificate_allocation_units", staleIds);
      }

      const { data: refreshedUnits, error: refreshedErr } = await supabase
        .from("certificate_allocation_units")
        .select("*")
        .eq("certificate_id", certificateLike.id)
        .order("unit_index", { ascending: true });
      if (refreshedErr) {
        if (isMissingTableError(refreshedErr)) return [];
        throw refreshedErr;
      }

      return syncAllocationUnitConsumption(certificateLike.id, refreshedUnits || []);
    },
    [deleteByIds, syncAllocationUnitConsumption]
  );

  const backfillMissingAllocationUnits = useCallback(
    async (certRows, existingUnitsByCertId) => {
      const merged = new Map(existingUnitsByCertId || []);
      let createdCount = 0;

      for (const row of certRows || []) {
        if ((merged.get(row.id) || []).length) continue;
        const synced = await syncCertificateAllocationUnits(
          {
            id: row.id,
            filename: row.filename,
            data: row.data,
            ...pickCertificateClassification(row),
          },
          { preserveExisting: false }
        );
        if (synced.length) {
          merged.set(row.id, synced);
          createdCount += 1;
        }
      }

      return { unitsByCertId: merged, createdCount };
    },
    [syncCertificateAllocationUnits]
  );

  const backfillMissingSafVolumes = useCallback(async (invoiceRowsData) => {
    if (!invoiceRowsData?.length) return { rows: invoiceRowsData || [], updatedCount: 0 };

    const safAliases = INVOICE_HEADER_ALIASES.saf_vol_m3;
    const rowsNeedingBackfill = invoiceRowsData.filter((row) => {
      if (normalizeVolumeNumber(row.saf_vol_m3) > 0) return false;
      if (!row.raw_payload || typeof row.raw_payload !== "object") return false;
      const keys = Object.keys(row.raw_payload);
      return safAliases.some((alias) => resolveHeader(keys, [alias]) && parseFlexibleNumber(row.raw_payload[resolveHeader(keys, [alias])]) > 0);
    });

    if (!rowsNeedingBackfill.length) return { rows: invoiceRowsData, updatedCount: 0 };

    const resolvedRows = rowsNeedingBackfill.map((row) => {
      const keys = Object.keys(row.raw_payload);
      for (const alias of safAliases) {
        const header = resolveHeader(keys, [alias]);
        if (header) {
          const val = parseFlexibleNumber(row.raw_payload[header]);
          if (val > 0) return { id: row.id, saf_vol_m3: val };
        }
      }
      return null;
    }).filter(Boolean);

    for (let i = 0; i < resolvedRows.length; i += 100) {
      const chunk = resolvedRows.slice(i, i + 100);
      await Promise.all(
        chunk.map(async ({ id, saf_vol_m3 }) => {
          const { error } = await supabase.from("invoice_rows").update({ saf_vol_m3 }).eq("id", id);
          if (error) throw error;
        })
      );
    }

    const updatedRows = invoiceRowsData.map((row) => {
      const resolved = resolvedRows.find((r) => r.id === row.id);
      return resolved ? { ...row, saf_vol_m3: resolved.saf_vol_m3 } : row;
    });

    return { rows: updatedRows, updatedCount: resolvedRows.length };
  }, []);

  const backfillInvoiceRowValidationState = useCallback(
    async (invoiceImportRow, invoiceRowsData) => {
      if (!invoiceImportRow?.id || !invoiceRowsData?.length) {
        return {
          rows: invoiceRowsData || [],
          updatedCount: 0,
          importUpdated: false,
        };
      }

      const reconciled = reconcileInvoiceRowValidationState(invoiceRowsData);
      let updatedCount = 0;

      const changedRows = reconciled.rows.filter((row) => {
        const original = invoiceRowsData.find((candidate) => candidate.id === row.id);
        if (!original) return false;
        return (
          Boolean(original.is_duplicate) !== Boolean(row.is_duplicate) ||
          String(original.duplicate_group_key || "") !== String(row.duplicate_group_key || "") ||
          String(original.validation_note || "") !== String(row.validation_note || "")
        );
      });

      for (let index = 0; index < changedRows.length; index += 100) {
        const chunk = changedRows.slice(index, index + 100);
        await Promise.all(
          chunk.map(async (row) => {
            let updateRes = await supabase
              .from("invoice_rows")
              .update({
                is_duplicate: Boolean(row.is_duplicate),
                duplicate_group_key: row.duplicate_group_key || null,
                validation_note: row.validation_note || null,
              })
              .eq("id", row.id);
            if (updateRes.error && isMissingColumnError(updateRes.error)) {
              updateRes = await supabase
                .from("invoice_rows")
                .update({
                  is_duplicate: Boolean(row.is_duplicate),
                  duplicate_group_key: row.duplicate_group_key || null,
                })
                .eq("id", row.id);
            }
            if (updateRes.error) throw updateRes.error;
          })
        );
        updatedCount += chunk.length;
      }

      const nextSummary = {
        ...(invoiceImportRow.validation_summary || {}),
        candidate_row_count: reconciled.summary.candidate_row_count,
        invalid_row_count: reconciled.summary.invalid_row_count,
        duplicate_row_count: reconciled.summary.duplicate_row_count,
        duplicate_group_count: reconciled.summary.duplicate_group_count,
        duplicate_groups: reconciled.summary.duplicate_groups,
        backfilled_at: new Date().toISOString(),
      };

      const importNeedsUpdate =
        Number(invoiceImportRow.row_count || 0) !== Number(reconciled.summary.row_count || 0) ||
        Number(invoiceImportRow.candidate_row_count || 0) !== Number(reconciled.summary.candidate_row_count || 0) ||
        Number(invoiceImportRow.invalid_row_count || 0) !== Number(reconciled.summary.invalid_row_count || 0) ||
        Number(invoiceImportRow.duplicate_row_count || 0) !== Number(reconciled.summary.duplicate_row_count || 0) ||
        JSON.stringify(invoiceImportRow.validation_summary || {}) !== JSON.stringify(nextSummary);

      if (importNeedsUpdate) {
        const { error } = await supabase
          .from("invoice_imports")
          .update({
            row_count: reconciled.summary.row_count,
            candidate_row_count: reconciled.summary.candidate_row_count,
            invalid_row_count: reconciled.summary.invalid_row_count,
            duplicate_row_count: reconciled.summary.duplicate_row_count,
            validation_summary: nextSummary,
          })
          .eq("id", invoiceImportRow.id);
        if (error && !isMissingTableError(error)) throw error;
      }

      return {
        rows: reconciled.rows,
        updatedCount,
        importUpdated: importNeedsUpdate,
      };
    },
    []
  );

  const loadFromDB = useCallback(async () => {
    const requestId = loadRequestRef.current + 1;
    loadRequestRef.current = requestId;
    const isCurrentRequest = () => loadRequestRef.current === requestId;

    try {
      const { data: certRows, error: certErr } = await supabase
        .from("certificates")
        .select("*")
        .order("created_at", { ascending: false });
      if (certErr) {
        if (isCurrentRequest()) addLog(`Cert DB error: ${certErr.message}`, "error");
        return;
      }

      let normalizedCertRows = certRows || [];
      try {
        const backfillRes = await backfillCanonicalCertificateRows(certRows || []);
        normalizedCertRows = backfillRes.rows;
        if (backfillRes.updatedCount && isCurrentRequest()) {
          addLog(`Backfilled canonical airports and classification for ${backfillRes.updatedCount} certificate(s)`, "info");
        }
      } catch (error) {
        if (isCurrentRequest()) addLog(`Canonical airport backfill warning: ${error.message}`, "error");
      }

      const [importRes, fallbackImportRes, legacyImportRes, matchRes, linkRes, unitRes, clientCertRes, companiesRes] = await Promise.all([
        supabase.from("invoice_imports").select("*").eq("status", "active").order("activated_at", { ascending: false }).order("created_at", { ascending: false }).limit(1),
        supabase
          .from("invoice_imports")
          .select("*")
          .in("status", ["staging", "superseded"])
          .order("activated_at", { ascending: false })
          .order("created_at", { ascending: false })
          .limit(1),
        supabase.from("invoices").select("*").not("csv_path", "is", null).order("created_at", { ascending: false }).limit(1),
        supabase.from("certificate_matches").select("*"),
        fetchAllPages((from, to) => supabase.from("certificate_invoice_links").select("*").range(from, to)),
        fetchAllPages((from, to) => supabase.from("certificate_allocation_units").select("*").order("unit_index", { ascending: true }).range(from, to)),
        supabase.from("client_certificates").select("*").order("created_at", { ascending: false }),
        supabase.from("companies").select("name, street, street_no, zip, city, country"),
      ]);

      if (importRes.error && !isMissingTableError(importRes.error) && isCurrentRequest()) addLog(`Invoice import error: ${importRes.error.message}`, "error");
      if (fallbackImportRes.error && !isMissingTableError(fallbackImportRes.error) && isCurrentRequest()) {
        addLog(`Fallback invoice import error: ${fallbackImportRes.error.message}`, "error");
      }
      if (legacyImportRes.error && !isMissingTableError(legacyImportRes.error) && isCurrentRequest()) {
        addLog(`Legacy invoice import error: ${legacyImportRes.error.message}`, "error");
      }
      if (matchRes.error && !isMissingTableError(matchRes.error) && isCurrentRequest()) addLog(`Match DB error: ${matchRes.error.message}`, "error");
      if (linkRes.error && !isMissingTableError(linkRes.error) && isCurrentRequest()) addLog(`Link DB error: ${linkRes.error.message}`, "error");
      if (unitRes.error && !isMissingTableError(unitRes.error) && isCurrentRequest()) addLog(`Allocation unit DB error: ${unitRes.error.message}`, "error");
      if (clientCertRes.error && !isMissingTableError(clientCertRes.error) && isCurrentRequest()) {
        addLog(`Client certificates DB error: ${clientCertRes.error.message}`, "error");
      }
      if (companiesRes.error && !isMissingTableError(companiesRes.error) && isCurrentRequest()) {
        addLog(`Companies DB error: ${companiesRes.error.message}`, "error");
      }

      const companiesByName = new Map((companiesRes.data || []).map((c) => [c.name.toLowerCase(), c]));

      const latestImport = importRes.data?.[0] || fallbackImportRes.data?.[0] || null;

      let nextInvoiceImport = latestImport;
      let invoiceRowsData = [];
      if (latestImport) {
        if (latestImport.status !== "active" && isCurrentRequest()) {
          addLog(
            `Showing ${latestImport.status} invoice import ${latestImport.filename} because no active import is available.`,
            "info"
          );
        }
        const { data, error } = await fetchAllPages((from, to) =>
          supabase
            .from("invoice_rows")
            .select("*")
            .eq("import_id", latestImport.id)
            .order("row_number", { ascending: true })
            .range(from, to)
        );
        if (error) {
          if (isCurrentRequest()) addLog(`Invoice rows error: ${error.message}`, "error");
        } else {
          invoiceRowsData = data || [];
        }

        if (latestImport.storage_path) {
          try {
            const { data: csvBlob, error: downloadErr } = await supabase.storage.from("invoices-csv").download(latestImport.storage_path);
            if (downloadErr) {
              // Storage file may be missing/renamed — not critical if invoice rows already exist in DB
              if (!invoiceRowsData.length) throw downloadErr;
            } else {
            const text = await csvBlob.text();
            const parsed = parseInvoiceCSV(text);
            const expectedRows = parsed.importRows || [];
            const existingRowNumbers = new Set(invoiceRowsData.map((row) => Number(row.row_number || 0)));
            const missingRows = expectedRows.filter((row) => !existingRowNumbers.has(Number(row.row_number || 0)));

            if (missingRows.length) {
              for (let index = 0; index < missingRows.length; index += 500) {
                const chunk = missingRows.slice(index, index + 500).map((row) => ({
                  import_id: latestImport.id,
                  row_number: row.row_number,
                  invoice_no: row.invoice_no,
                  customer: row.customer,
                  uplift_date: row.uplift_date,
                  flight_no: row.flight_no,
                  delivery_ticket: row.delivery_ticket,
                  iata: row.iata,
                  icao: row.icao,
                  country: row.country,
                  supplier: row.supplier,
                  vol_m3: row.vol_m3,
                  saf_vol_m3: row.saf_vol_m3,
                  raw_payload: row.raw_payload,
                  is_allocated: false,
                  is_duplicate: Boolean(row.is_duplicate),
                  duplicate_group_key: row.duplicate_group_key || null,
                  validation_note: row.validation_note || null,
                }));
                let insertRes = await supabase.from("invoice_rows").insert(chunk);
                if (insertRes.error && isMissingColumnError(insertRes.error)) {
                  insertRes = await supabase.from("invoice_rows").insert(
                    chunk.map(({ validation_note: _ignoredValidationNote, ...row }) => row)
                  );
                }
                if (insertRes.error) throw insertRes.error;
              }

              const refetched = await fetchAllPages((from, to) =>
                supabase
                  .from("invoice_rows")
                  .select("*")
                  .eq("import_id", latestImport.id)
                  .order("row_number", { ascending: true })
                  .range(from, to)
              );
              if (refetched.error) throw refetched.error;
              invoiceRowsData = refetched.data || [];
              if (isCurrentRequest()) addLog(`Backfilled ${missingRows.length} missing invoice row(s) from ${latestImport.filename}`, "info");
            }
            }
          } catch (error) {
            if (isCurrentRequest()) {
              const msg = error instanceof Error ? error.message : (error?.message || error?.error || error?.statusText || (typeof error === "string" ? error : JSON.stringify(error)));
              addLog(`Invoice row backfill warning: ${msg}`, "error");
            }
          }
        }

        try {
          const safBackfillRes = await backfillMissingSafVolumes(invoiceRowsData);
          if (safBackfillRes.updatedCount) {
            invoiceRowsData = safBackfillRes.rows;
            if (isCurrentRequest()) addLog(`Backfilled saf_vol_m3 for ${safBackfillRes.updatedCount} invoice row(s) from raw_payload`, "info");
          }
        } catch (error) {
          if (isCurrentRequest()) addLog(`SAF volume backfill warning: ${error.message}`, "error");
        }

        try {
          const invoiceBackfillRes = await backfillInvoiceRowValidationState(latestImport, invoiceRowsData);
          invoiceRowsData = invoiceBackfillRes.rows;
          if ((invoiceBackfillRes.updatedCount || invoiceBackfillRes.importUpdated) && isCurrentRequest()) {
            addLog(
              `Backfilled invoice validation state for ${invoiceBackfillRes.updatedCount} row(s) on ${latestImport.filename}`,
              "info"
            );
          }
        } catch (error) {
          if (isCurrentRequest()) addLog(`Invoice validation backfill warning: ${error.message}`, "error");
        }
      } else {
        const legacyImport = legacyImportRes.data?.[0] || null;
        if (legacyImport?.csv_path) {
          try {
            const { data: csvBlob, error: downloadErr } = await supabase.storage.from("invoices-csv").download(legacyImport.csv_path);
            if (downloadErr) throw downloadErr;
            const text = await csvBlob.text();
            const parsed = parseInvoiceCSV(text);
            invoiceRowsData = parsed.importRows || [];
            nextInvoiceImport = {
              id: null,
              filename: legacyImport.filename,
              row_count: parsed.summary.total_row_count || 0,
              status: "legacy",
              created_at: legacyImport.created_at,
            };
            if (isCurrentRequest()) {
              addLog(
                `Loaded legacy invoice CSV ${legacyImport.filename} for UI display. Re-import it to enable database-backed allocation.`,
                "info"
              );
            }
          } catch (error) {
            if (isCurrentRequest()) addLog(`Legacy invoice CSV load failed: ${error.message}`, "error");
          }
        }
      }

      const linksByCertId = new Map();
      for (const link of linkRes.data || []) {
        const arr = linksByCertId.get(link.certificate_id) || [];
        arr.push(link);
        linksByCertId.set(link.certificate_id, arr);
      }

      let unitsByCertId = new Map();
      for (const unit of unitRes.data || []) {
        const arr = unitsByCertId.get(unit.certificate_id) || [];
        arr.push(unit);
        unitsByCertId.set(unit.certificate_id, arr);
      }

      try {
        const unitBackfillRes = await backfillMissingAllocationUnits(normalizedCertRows, unitsByCertId);
        unitsByCertId = unitBackfillRes.unitsByCertId;
        if (unitBackfillRes.createdCount && isCurrentRequest()) {
          addLog(`Generated allocation units for ${unitBackfillRes.createdCount} certificate(s)`, "info");
        }
      } catch (error) {
        if (isCurrentRequest()) addLog(`Allocation unit backfill warning: ${error.message}`, "error");
      }

      for (const row of normalizedCertRows) {
        const hydratedUnits = hydrateAllocationUnitsWithConsumption(unitsByCertId.get(row.id) || [], linkRes.data || [], row.id);
        if (hydratedUnits.length) unitsByCertId.set(row.id, hydratedUnits);
      }

      const hydratedInvoiceRows = hydrateInvoiceRowsWithAllocations(invoiceRowsData, linkRes.data || [], normalizedCertRows);

      const matchByCertId = new Map(
        (matchRes.data || []).map((match) => [
          match.certificate_id,
          {
            ...match,
            links: (linksByCertId.get(match.certificate_id) || []).sort((a, b) => (a.row_number || 0) - (b.row_number || 0)),
            candidate_sets: match.candidate_sets || [],
            diagnostics: match.diagnostics || {},
          },
        ])
      );

      const hydrated = normalizedCertRows.map((row) => ({
        id: row.id,
        filename: row.filename,
        data: row.data,
        analysis: row.analysis,
        pdfPath: row.pdf_path,
        ...pickCertificateClassification(row),
        allocation_units: (unitsByCertId.get(row.id) || []).sort((a, b) => (a.unit_index || 0) - (b.unit_index || 0)),
        match: matchByCertId.get(row.id) || null,
      }));

      if (!isCurrentRequest()) return;

      // Verify client certificate PDFs still exist in storage; clear stale references
      const clientCertRecords = clientCertRes.data || [];
      const withPaths = clientCertRecords.filter((r) => r.generated_file_path);
      if (withPaths.length > 0) {
        const checks = await Promise.all(
          withPaths.map((r) =>
            supabase.storage
              .from("client-certificates-pdf")
              .createSignedUrl(r.generated_file_path, 1)
              .then(({ error }) => (error ? r : null))
          )
        );
        const stale = checks.filter(Boolean);
        if (stale.length > 0) {
          const staleIds = new Set(stale.map((r) => r.id));
          for (const r of stale) {
            addLog(`Client certificate "${r.internal_reference || r.group_key}": PDF missing from storage, clearing stale reference`, "warn");
            supabase.from("client_certificates").update({ generated_file_path: null }).eq("id", r.id).then(({ error }) => {
              if (error) addLog(`Failed to clear stale path for ${r.group_key}: ${error.message}`, "error");
            });
          }
          for (const r of clientCertRecords) {
            if (staleIds.has(r.id)) r.generated_file_path = null;
          }
        }
      }

      setInvoiceImport(nextInvoiceImport);
      setInvoiceRows(hydratedInvoiceRows);
      setCerts(hydrated);
      setClientCertificateRecords(clientCertRecords);
      setCompaniesByName(companiesByName);
      addLog(`Loaded ${hydrated.length} certificate(s) and ${hydratedInvoiceRows.length} invoice row(s)`, "success");
      return { certs: hydrated, invoiceRows: hydratedInvoiceRows, clientCertificateRecords: clientCertRecords, companiesByName };
    } catch (error) {
      if (isCurrentRequest()) addLog(`DB reload failed: ${error.message}`, "error");
      return null;
    }
  }, [addLog, backfillInvoiceRowValidationState, backfillMissingAllocationUnits, backfillMissingSafVolumes]);

  useEffect(() => {
    if (initialLoadStartedRef.current) return;
    initialLoadStartedRef.current = true;
    setLoading("Loading certificates and invoices...");
    loadFromDB().finally(() => setLoading(""));
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

  const runDatabaseSimpleAllocation = useCallback(
    async (certificate) => {
      const { data, error } = await supabase.rpc("allocate_simple_certificate", {
        p_certificate_id: certificate.id,
        p_import_id: loadedInvoiceImportId,
        p_actor: userEmail || null,
      });
      if (error) throw error;

      const result = data && typeof data === "object" ? data : {};
      await syncAllocationUnitConsumption(certificate.id);

      return {
        status: result.status || "unmatched",
        match_method: result.match_method || "simple_monthly_airport",
        cert_volume_m3: normalizeVolumeNumber(result.cert_volume_m3),
        allocated_volume_m3: normalizeVolumeNumber(result.allocated_volume_m3) || 0,
        variance_m3: normalizeVolumeNumber(result.variance_m3),
        review_note: result.review_note || "",
        candidate_sets: Array.isArray(result.candidate_sets) ? result.candidate_sets : [],
        linked_rows: Array.isArray(result.linked_rows) ? result.linked_rows : [],
        diagnostics: result.diagnostics && typeof result.diagnostics === "object" ? result.diagnostics : {},
      };
    },
    [loadedInvoiceImportId, syncAllocationUnitConsumption, userEmail]
  );

  const runDatabasePocAllocation = useCallback(
    async (certificate) => {
      const { data, error } = await supabase.rpc("allocate_poc_certificate", {
        p_certificate_id: certificate.id,
        p_import_id: loadedInvoiceImportId,
        p_actor: userEmail || null,
      });
      if (error) throw error;

      const result = data && typeof data === "object" ? data : {};
      await syncAllocationUnitConsumption(certificate.id);

      return {
        status: result.status || "unmatched",
        match_method: result.match_method || "fifo-poc-monthly-airport",
        cert_volume_m3: normalizeVolumeNumber(result.cert_volume_m3),
        allocated_volume_m3: normalizeVolumeNumber(result.allocated_volume_m3) || 0,
        variance_m3: normalizeVolumeNumber(result.variance_m3),
        review_note: "",
        candidate_sets: [],
        linked_rows: Array.isArray(result.linked_rows) ? result.linked_rows : [],
        unit_breakdown: Array.isArray(result.unit_breakdown) ? result.unit_breakdown : [],
        diagnostics: result.diagnostics && typeof result.diagnostics === "object" ? result.diagnostics : {},
      };
    },
    [loadedInvoiceImportId, syncAllocationUnitConsumption, userEmail]
  );

  const clearCertificateMatch = useCallback(async (certificateId) => {
    const { error: deleteErr } = await supabase.from("certificate_matches").delete().eq("certificate_id", certificateId);
    if (deleteErr && !isMissingTableError(deleteErr)) throw deleteErr;
    await syncAllocationUnitConsumption(certificateId, null, []);
  }, [syncAllocationUnitConsumption]);

  const insertCertificateInvoiceLinks = useCallback(async (insertedMatchId, certificateId, linkedRows) => {
    if (!linkedRows.length) return;

    const fullPayload = linkedRows.map((row) => ({
      certificate_match_id: insertedMatchId,
      certificate_id: certificateId,
      invoice_row_id: row.invoice_row_id,
      row_number: row.row_number,
      invoice_no: row.invoice_no,
      customer: row.customer,
      uplift_date: row.uplift_date,
      iata: row.iata,
      icao: row.icao,
      allocated_m3: row.allocated_m3,
      allocation_unit_id: row.allocation_unit_id || null,
      allocation_unit_index: row.allocation_unit_index ?? null,
      allocation_unit_type: row.allocation_unit_type || null,
    }));

    let linkRes = await supabase.from("certificate_invoice_links").insert(fullPayload);
    if (linkRes.error && isMissingColumnError(linkRes.error)) {
      linkRes = await supabase.from("certificate_invoice_links").insert(
        linkedRows.map((row) => ({
          certificate_match_id: insertedMatchId,
          certificate_id: certificateId,
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
    }
    if (linkRes.error) throw linkRes.error;
  }, []);

  const persistMatch = useCallback(
    async (certificate, result, reviewer, options = {}) => {
      const { skipClear = false, skipAllocationSync = false } = options;
      if (!skipClear) {
        await clearCertificateMatch(certificate.id);
      }

      const matchPayload = {
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
        diagnostics: result.diagnostics || {},
        updated_at: new Date().toISOString(),
      };

      let insertRes = await supabase.from("certificate_matches").insert(matchPayload).select("*").single();
      if (insertRes.error && /diagnostics/i.test(insertRes.error.message || "")) {
        const { diagnostics: _ignoredDiagnostics, ...fallbackPayload } = matchPayload;
        insertRes = await supabase.from("certificate_matches").insert(fallbackPayload).select("*").single();
      }

      const { data: insertedMatch, error: matchErr } = insertRes;
      if (matchErr) throw matchErr;

      const linkedRows = result.linked_rows || [];
      if (linkedRows.length) {
        await insertCertificateInvoiceLinks(insertedMatch.id, certificate.id, linkedRows);
      }

      if (!skipAllocationSync) {
        await syncAllocationUnitConsumption(certificate.id);
      }
    },
    [clearCertificateMatch, insertCertificateInvoiceLinks, syncAllocationUnitConsumption]
  );

  const saveManualMatch = useCallback(
    async (certificate, selectedRowIds) => {
      if (!certificate?.id || !selectedRowIds?.length) return;
      if (loading) {
        addLog("Another operation is in progress. Please wait.", "error");
        return;
      }
      setLoading("Saving manual match...");
      try {
        const idSet = new Set(selectedRowIds);
        const newLinkedRows = invoiceRows
          .filter((r) => idSet.has(r.id) && Number(r.remaining_m3) > MATCH_TOLERANCE)
          .map((r) => ({
            invoice_row_id: r.id,
            row_number: r.row_number,
            invoice_no: r.invoice_no,
            customer: r.customer,
            uplift_date: r.uplift_date,
            iata: r.iata,
            icao: r.icao,
            allocated_m3: Number(r.remaining_m3),
            allocation_unit_id: null,
            allocation_unit_index: null,
            allocation_unit_type: null,
          }));

        if (!newLinkedRows.length) {
          addLog("No selectable invoice rows with remaining volume.", "error");
          return;
        }

        const manuallyAdded = newLinkedRows.reduce((s, r) => s + Number(r.allocated_m3 || 0), 0);
        const certVol = Number(certificate.match?.cert_volume_m3) || Number(certificate.data?.quantity) || 0;
        const existingMatch = certificate.match;
        const existingAllocated = Number(existingMatch?.allocated_volume_m3) || 0;
        const existingLinks = Array.isArray(existingMatch?.links) ? existingMatch.links.length : 0;
        const newAllocated = Number((existingAllocated + manuallyAdded).toFixed(6));
        const nextStatus = newAllocated >= certVol - MATCH_TOLERANCE ? "approved" : "partial_linked";
        const variance = Number((certVol - newAllocated).toFixed(6));
        const note = existingLinks > 0
          ? `Manually linked ${newLinkedRows.length} row(s) on top of ${existingLinks} auto-linked row(s).`
          : `Manually linked ${newLinkedRows.length} invoice row(s).`;

        const hasExistingMatchRow = !!existingMatch?.id;
        const hasExistingLinks = existingLinks > 0;

        if (!hasExistingMatchRow || !hasExistingLinks) {
          await persistMatch(
            certificate,
            {
              status: nextStatus,
              match_method: "manual",
              cert_volume_m3: certVol,
              allocated_volume_m3: newAllocated,
              variance_m3: variance,
              review_note: note,
              diagnostics: { ...(existingMatch?.diagnostics || {}), manual_link_count: newLinkedRows.length },
              candidate_sets: existingMatch?.candidate_sets || [],
              linked_rows: newLinkedRows,
            },
            userEmail
          );
        } else {
          await insertCertificateInvoiceLinks(existingMatch.id, certificate.id, newLinkedRows);
          const { error: updErr } = await supabase
            .from("certificate_matches")
            .update({
              status: nextStatus,
              match_method: "manual",
              allocated_volume_m3: newAllocated,
              variance_m3: variance,
              review_note: note,
              reviewed_by: userEmail || null,
              reviewed_at: nextStatus === "approved" ? new Date().toISOString() : null,
              updated_at: new Date().toISOString(),
            })
            .eq("id", existingMatch.id);
          if (updErr) throw updErr;
          await syncAllocationUnitConsumption(certificate.id);
        }

        addLog(`Manual match saved: ${newLinkedRows.length} row(s), ${manuallyAdded.toFixed(3)} m³ linked (status: ${nextStatus}).`, "ok");
        await loadFromDB();
      } catch (err) {
        console.error(err);
        addLog(`Manual match failed: ${err?.message || err}`, "error");
      } finally {
        setLoading("");
      }
    },
    [invoiceRows, loading, addLog, persistMatch, insertCertificateInvoiceLinks, syncAllocationUnitConsumption, userEmail, loadFromDB]
  );

  const handlePDFUpload = useCallback(
    async (files) => {
      const list = Array.from(files || []);
      if (!list.length) return;
      if (loading) {
        addLog("Another operation is in progress. Please wait.", "error");
        return;
      }
      setLoading("Extracting certificate data...");

      for (const file of list) {
        if (!file.type.includes("pdf")) continue;
        addLog(`Processing ${file.name}`, "info");
        try {
          const base64 = await fileToBase64(file);
          const extraction = await extractCertificateFromBase64(base64, file.name);
          const normalized = normalizeCertificateAirports(extraction.parsed, { filename: file.name });
          const parsed = normalized.data;
          const classification = normalized.classification;
          const uniqueNumber = parsed.uniqueNumber || null;
          const storagePath = uniqueNumber ? `${uniqueNumber}.pdf` : `no-id/${Date.now()}-${file.name}`;

          const { error: storageErr } = await supabase.storage
            .from("certificates-pdf")
            .upload(storagePath, file, { contentType: "application/pdf", upsert: true });
          if (storageErr) addLog(`PDF storage warning for ${file.name}: ${storageErr.message}`, "error");

          // Duplicate detection: same airport + month + volume = likely duplicate
          const newAirport = parsed.canonicalAirports?.[0]?.iata || "";
          const newMonth = parsed.coverageMonth || "";
          const newVolume = Number(parsed.quantity) || 0;
          if (newAirport && newMonth && newVolume > 0) {
            const existingDup = certs.find((c) => {
              if (uniqueNumber && c.unique_number === uniqueNumber) return false; // same cert being re-uploaded, not a dup
              const cAirport = c.data?.canonicalAirports?.[0]?.iata || "";
              const cMonth = c.data?.coverageMonth || "";
              const cVolume = Number(c.data?.quantity) || 0;
              return cAirport === newAirport && cMonth === newMonth && Math.abs(cVolume - newVolume) < 0.001;
            });
            if (existingDup) {
              addLog(`⚠ DUPLICATE DETECTED: ${file.name} matches existing cert ${existingDup.unique_number || existingDup.filename} (${newAirport} ${newMonth} ${newVolume} m³). Skipping.`, "error");
              continue;
            }
          }

          const baseCertificatePayload = {
            filename: file.name,
            data: parsed,
            pdf_path: storageErr ? null : storagePath,
          };
          const extendedCertificatePayload = {
            ...baseCertificatePayload,
            document_family: classification.document_family,
            matching_mode: classification.matching_mode,
            classification_confidence: classification.classification_confidence,
            review_required: classification.review_required,
            classification_reason: classification.classification_reason,
          };

          let saveRes;
          if (uniqueNumber) {
            saveRes = await supabase
              .from("certificates")
              .insert({ ...extendedCertificatePayload, unique_number: uniqueNumber })
              .select("id")
              .single();
            if (saveRes.error && isMissingColumnError(saveRes.error)) {
              saveRes = await supabase
                .from("certificates")
                .insert({ ...baseCertificatePayload, unique_number: uniqueNumber })
                .select("id")
                .single();
            }
            if (saveRes.error?.code === "23505") {
              // Duplicate cert — update data but preserve existing pdf_path to avoid storage mismatch
              const { pdf_path: _drop, ...extendedWithoutPath } = extendedCertificatePayload;
              const { pdf_path: _drop2, ...baseWithoutPath } = baseCertificatePayload;
              saveRes = await supabase
                .from("certificates")
                .update(extendedWithoutPath)
                .eq("unique_number", uniqueNumber)
                .select("id")
                .single();
              if (saveRes.error && isMissingColumnError(saveRes.error)) {
                saveRes = await supabase
                  .from("certificates")
                  .update(baseWithoutPath)
                  .eq("unique_number", uniqueNumber)
                  .select("id")
                  .single();
              }
            }
          } else {
            saveRes = await supabase
              .from("certificates")
              .insert(extendedCertificatePayload)
              .select("id")
              .single();
            if (saveRes.error && isMissingColumnError(saveRes.error)) {
              saveRes = await supabase.from("certificates").insert(baseCertificatePayload).select("id").single();
            }
          }
          if (saveRes.error) {
            // DB insert/update failed — clean up the orphaned PDF from storage
            if (!storageErr) {
              await supabase.storage.from("certificates-pdf").remove([storagePath]).catch((cleanupErr) => {
                addLog(`Warning: failed to clean up orphaned PDF ${storagePath}: ${cleanupErr.message}`, "error");
              });
            }
            throw saveRes.error;
          }
          try {
            await syncCertificateAllocationUnits({
              id: saveRes.data?.id,
              filename: file.name,
              data: parsed,
              ...classification,
            });
          } catch (unitErr) {
            addLog(`Allocation unit sync warning for ${file.name}: ${unitErr.message}`, "error");
          }
          addLog(`Saved ${certTitle({ filename: file.name, data: parsed })}`, "success");
          if (extraction.usage) {
            addLog(
              `Extracted with ${extraction.model} (${extraction.usage.input_tokens || 0} in / ${extraction.usage.output_tokens || 0} out tokens)`,
              "info"
            );
          }
        } catch (error) {
          addLog(`Certificate import failed for ${file.name}: ${error.message}`, "error");
        }
      }

      setLoading("");
      await loadFromDB();
      setTab("certs");
    },
    [addLog, loading, loadFromDB, syncCertificateAllocationUnits]
  );

  const handleCSVUpload = useCallback(
    async (file) => {
      if (!file) return;
      if (loading) {
        addLog("Another operation is in progress. Please wait.", "error");
        return;
      }
      setLoading("Importing annual invoice CSV...");
      addLog(`Importing invoice CSV ${file.name}`, "info");

      let stagedImportId = null;
      let importActivated = false;

      try {
        const text = await file.text();
        const parsed = parseInvoiceCSV(text);
        const requiredHeaders = ["saf_vol_m3"];
        const missingRequiredHeaders = parsed.missing.filter((key) => requiredHeaders.includes(key));
        const validationSummary = {
          filename: file.name,
          total_row_count: parsed.summary.total_row_count || 0,
          candidate_row_count: parsed.summary.candidate_row_count || 0,
          invalid_row_count: parsed.summary.invalid_row_count || 0,
          duplicate_row_count: parsed.summary.duplicate_row_count || 0,
          duplicate_group_count: parsed.summary.duplicate_group_count || 0,
          invalid_rows: parsed.summary.invalid_rows || [],
          duplicate_groups: parsed.duplicateGroups.slice(0, MAX_IMPORT_VALIDATION_ERRORS),
          missing_headers: missingRequiredHeaders,
        };

        const invoiceYear = deriveInvoiceYear(parsed);
        const storagePath = `invoices/${Date.now()}-${file.name}`;
        const { error: storageErr } = await supabase.storage
          .from("invoices-csv")
          .upload(storagePath, file, { contentType: "text/csv", upsert: false });
        if (storageErr) throw storageErr;

        const { data: stagedImport, error: importErr } = await supabase
          .from("invoice_imports")
          .insert({
            filename: file.name,
            storage_path: storagePath,
            year: invoiceYear,
            status: "staging",
            row_count: 0,
            candidate_row_count: 0,
            invalid_row_count: 0,
            duplicate_row_count: 0,
            validation_summary: {
              filename: file.name,
              phase: "staging_created",
            },
          })
          .select("*")
          .single();
        if (importErr) throw importErr;
        stagedImportId = stagedImport.id;

        const markImportFailed = async (message, summary = validationSummary) => {
          if (!stagedImportId || importActivated) return;
          const failureSummary = {
            ...summary,
            error_message: message,
          };
          const { error } = await supabase
            .from("invoice_imports")
            .update({
              status: "failed",
              row_count: summary.candidate_row_count || 0,
              candidate_row_count: summary.candidate_row_count || 0,
              invalid_row_count: summary.invalid_row_count || 0,
              duplicate_row_count: summary.duplicate_row_count || 0,
              validation_summary: failureSummary,
              failed_at: new Date().toISOString(),
            })
            .eq("id", stagedImportId);
          if (error && !isMissingTableError(error)) {
            addLog(`Failed to mark staged import as failed: ${error.message}`, "error");
          }
        };

        if (!parsed.rows.length) throw new Error("CSV is empty.");
        if (missingRequiredHeaders.length) {
          const message = "Missing required column: SAF Vol 2% per M3.";
          await markImportFailed(message);
          throw new Error(message);
        }
        if (!parsed.importRows.length) {
          const message = "No operational invoice rows were found in the CSV.";
          await markImportFailed(message);
          throw new Error(message);
        }

        const { error: stagingUpdateErr } = await supabase
          .from("invoice_imports")
          .update({
            row_count: parsed.importRows.length,
            candidate_row_count: parsed.candidateRows.length,
            invalid_row_count: parsed.invalidRows.length,
            duplicate_row_count: parsed.duplicateRows.length,
            validation_summary: validationSummary,
          })
          .eq("id", stagedImportId);
        if (stagingUpdateErr) throw stagingUpdateErr;

        const totalChunks = Math.ceil(parsed.importRows.length / 500);
        for (let index = 0; index < parsed.importRows.length; index += 500) {
          const chunkNum = Math.floor(index / 500) + 1;
          setLoading(`Inserting invoice rows (batch ${chunkNum}/${totalChunks})...`);
          const chunk = parsed.importRows.slice(index, index + 500).map((row) => ({
            import_id: stagedImportId,
            row_number: row.row_number,
            invoice_no: row.invoice_no,
            customer: row.customer,
            uplift_date: row.uplift_date,
            flight_no: row.flight_no,
            delivery_ticket: row.delivery_ticket,
            iata: row.iata,
            icao: row.icao,
            country: row.country,
            supplier: row.supplier,
            vol_m3: row.vol_m3,
            saf_vol_m3: row.saf_vol_m3,
            raw_payload: row.raw_payload,
            is_allocated: false,
            is_duplicate: Boolean(row.is_duplicate),
            duplicate_group_key: row.duplicate_group_key || null,
            validation_note: row.validation_note || null,
          }));
          let rowsRes = await supabase.from("invoice_rows").insert(chunk);
          if (rowsRes.error && isMissingColumnError(rowsRes.error)) {
            rowsRes = await supabase.from("invoice_rows").insert(
              chunk.map(({ validation_note: _ignoredValidationNote, ...row }) => row)
            );
          }
          const { error: rowsErr } = rowsRes;
          if (rowsErr) throw new Error(`Row insertion failed at batch ${chunkNum}/${totalChunks} (rows ${index + 1}-${index + chunk.length}): ${rowsErr.message}`);
        }

        const { data: activationResult, error: activationErr } = await supabase.rpc("activate_invoice_import", {
          p_import_id: stagedImportId,
        });
        if (activationErr) throw activationErr;
        importActivated = true;

        addLog(
          `Imported ${parsed.importRows.length} invoice rows and activated ${file.name}`,
          "success"
        );
        if (parsed.invalidRows.length) {
          addLog(formatInvoiceValidationError(validationSummary), "info");
        }
        if (parsed.duplicateGroups.length) {
          addLog(
            `Flagged ${parsed.duplicateRows.length} duplicate row(s) across ${parsed.duplicateGroups.length} duplicate group(s) in the staged import`,
            "info"
          );
        }
        if (activationResult?.previous_active_filename) {
          addLog(`Superseded previous active import ${activationResult.previous_active_filename}`, "info");
        }
        await loadFromDB();
        setTab("db");
      } catch (error) {
        if (stagedImportId && !importActivated) {
          const fallbackSummary = {
            filename: file.name,
          };
          try {
            const { error: failErr } = await supabase
              .from("invoice_imports")
              .update({
                status: "failed",
                failed_at: new Date().toISOString(),
                validation_summary: {
                  ...fallbackSummary,
                  error_message: error.message,
                },
              })
              .eq("id", stagedImportId)
              .eq("status", "staging");
            if (failErr && !isMissingTableError(failErr)) {
              addLog(`Failed to update staged import status: ${failErr.message}`, "error");
            }
          } catch (cleanupErr) {
            addLog(`Failed to mark import as failed: ${cleanupErr.message}`, "error");
          }
        }
        addLog(`Invoice import failed: ${error.message}`, "error");
      } finally {
        setLoading("");
      }
    },
    [addLog, loading, loadFromDB]
  );

  const openStoredPdf = useCallback(
    async ({ bucket, path, title }) => {
      if (!bucket || !path) {
        addLog(`No stored PDF found for ${title || "document"}`, "error");
        return;
      }

      setLoading(`Opening ${title || "PDF"}...`);
      try {
        const { data, error } = await supabase.storage.from(bucket).download(path);
        if (error) throw error;
        const blobUrl = URL.createObjectURL(data);
        setPdfPreview((prev) => {
          if (prev?.url?.startsWith("blob:")) URL.revokeObjectURL(prev.url);
          return { title: title || "PDF", url: blobUrl };
        });
        addLog(`Opened PDF preview for ${title || "document"}`, "success");
      } catch (error) {
        addLog(`Open PDF failed for ${title || "document"}: ${error.message}`, "error");
      }
      setLoading("");
    },
    [addLog]
  );

  const openCertificatePdf = useCallback(
    async (cert) => openStoredPdf({ bucket: "certificates-pdf", path: cert?.pdfPath, title: certTitle(cert) }),
    [openStoredPdf]
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
        const extraction = await extractCertificateFromBase64(base64, cert.filename || `${certTitle(cert)}.pdf`);
        const normalized = normalizeCertificateAirports(extraction.parsed, { filename: cert.filename });
        const parsed = normalized.data;
        const classification = normalized.classification;
        const baseUpdatePayload = {
          data: parsed,
        };
        const extendedUpdatePayload = {
          ...baseUpdatePayload,
          document_family: classification.document_family,
          matching_mode: classification.matching_mode,
          classification_confidence: classification.classification_confidence,
          review_required: classification.review_required,
          classification_reason: classification.classification_reason,
        };

        let updateRes = await supabase
          .from("certificates")
          .update(extendedUpdatePayload)
          .eq("id", cert.id);
        if (updateRes.error && isMissingColumnError(updateRes.error)) {
          updateRes = await supabase.from("certificates").update(baseUpdatePayload).eq("id", cert.id);
        }
        const { error: updateErr } = updateRes;
        if (updateErr) throw updateErr;
        await syncCertificateAllocationUnits({
          id: cert.id,
          filename: cert.filename,
          data: parsed,
          ...classification,
        });

        addLog(`Re-extracted ${certTitle({ ...cert, data: parsed })}`, "success");
        if (extraction.usage) {
          addLog(
            `Re-extracted with ${extraction.model} (${extraction.usage.input_tokens || 0} in / ${extraction.usage.output_tokens || 0} out tokens)`,
            "info"
          );
        }
        setLoading("");
        await loadFromDB();
      } catch (error) {
        setLoading("");
        addLog(`Re-extract failed: ${error.message}`, "error");
      }
    },
    [addLog, certs, loadFromDB, syncCertificateAllocationUnits]
  );

  const analyzeSingle = useCallback(
    async (index) => {
      const cert = certs[index];
      if (!cert?.id) return;
      if (isSupportedSimpleCert(cert) || isSupportedPocCert(cert)) {
        if (!invoiceRows.length) {
          addLog("No invoice rows are loaded. Import the invoice CSV before matching certificates.", "error");
          return;
        }
        if (!allocableInvoiceRowCount) {
          addLog("No allocable invoice rows are currently loaded for deterministic matching.", "error");
          return;
        }
        if (!loadedInvoiceImportId) {
          addLog("Invoice rows are visible, but they come from a legacy CSV view and are not database-backed. Re-import the CSV to enable matching.", "error");
          return;
        }
      }

      setLoading(`Matching ${certTitle(cert)}...`);
      addLog(`Matching ${certTitle(cert)}`, "info");

      try {
        const result = isSupportedPocCert(cert)
          ? await runDatabasePocAllocation(cert)
          : await runDatabaseSimpleAllocation(cert);
        addLog(
          `${certTitle(cert)} → ${result.status} · ${formatDiagnosticsSummary(result.diagnostics)}`,
          result.status === "unmatched" ? "error" : result.status === "manual_only" ? "info" : "success"
        );
        setLoading("");
        try {
          await Promise.all([
            supabase.rpc("sync_invoice_allocation_flags"),
            supabase.rpc("sync_allocation_units_consumed"),
            supabase.rpc("sync_no_uplift_exclusions"),
          ]);
        } catch (syncErr) { /* best-effort */ }
        await loadFromDB();
        setTab("certs");
      } catch (error) {
        setLoading("");
        addLog(`Matching failed for ${certTitle(cert)}: ${error.message}`, "error");
      }
    },
    [addLog, allocableInvoiceRowCount, certs, invoiceRows.length, loadFromDB, loadedInvoiceImportId, runDatabasePocAllocation, runDatabaseSimpleAllocation]
  );

  const analyzeAll = useCallback(async () => {
    if (!certs.length) return;
    if (certs.some((cert) => isSupportedSimpleCert(cert) || isSupportedPocCert(cert))) {
      if (!invoiceRows.length) {
        addLog("No invoice rows are loaded. Import the invoice CSV before matching certificates.", "error");
        return;
      }
      if (!allocableInvoiceRowCount) {
        addLog("No allocable invoice rows are currently loaded for deterministic matching.", "error");
        return;
      }
      if (!loadedInvoiceImportId) {
        addLog("Invoice rows are visible, but they come from a legacy CSV view and are not database-backed. Re-import the CSV to enable matching.", "error");
        return;
      }
    }

    setLoading("Running deterministic allocation...");
    addLog("Running database-side FIFO allocation for all certificates", "info");

    const failures = [];
    try {
      for (let index = 0; index < certs.length; index += 1) {
        const cert = certs[index];
        if (cert.match?.status === "approved") continue;
        setLoading(`Matching ${index + 1}/${certs.length}: ${certTitle(cert)}`);
        try {
          const result = isSupportedPocCert(cert)
            ? await runDatabasePocAllocation(cert)
            : await runDatabaseSimpleAllocation(cert);
          addLog(
            `${certTitle(cert)} → ${result.status} · ${formatDiagnosticsSummary(result.diagnostics)}`,
            result.status === "unmatched" ? "error" : result.status === "manual_only" || result.status === "partial_linked" ? "info" : "success"
          );
        } catch (certErr) {
          failures.push(certTitle(cert));
          addLog(`Matching failed for ${certTitle(cert)}: ${certErr.message}`, "error");
        }
      }

      if (failures.length) {
        addLog(`Matching completed with ${failures.length} failure(s) out of ${certs.length} certificate(s)`, "error");
      }
      // Reconcile derived flags from the link ledger (best-effort — don't block matching)
      try {
        const [{ data: flagData }, { data: unitData }, { data: noUpliftData }] = await Promise.all([
          supabase.rpc("sync_invoice_allocation_flags"),
          supabase.rpc("sync_allocation_units_consumed"),
          supabase.rpc("sync_no_uplift_exclusions"),
        ]);
        if ((flagData ?? 0) > 0 || (unitData ?? 0) > 0 || (noUpliftData ?? 0) > 0) {
          addLog(`Reconciled ${flagData ?? 0} invoice flag(s), ${unitData ?? 0} allocation unit(s), ${noUpliftData ?? 0} no-uplift exclusion(s)`, "info");
        }
      } catch (syncErr) {
        addLog(`Post-match sync warning: ${syncErr.message}`, "error");
      }
      await loadFromDB();
      setTab("certs");
    } catch (error) {
      addLog(`Full matching run failed: ${error.message}`, "error");
    } finally {
      setLoading("");
    }
  }, [addLog, allocableInvoiceRowCount, certs, invoiceRows.length, loadFromDB, loadedInvoiceImportId, runDatabasePocAllocation, runDatabaseSimpleAllocation]);

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
            diagnostics: cert.match?.diagnostics || {},
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
            diagnostics: cert.match?.diagnostics || {},
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

  const approveAutoLinked = useCallback(
    async (cert) => {
      setLoading(`Approving ${certTitle(cert)}...`);
      try {
        const { error } = await supabase
          .from("certificate_matches")
          .update({
            status: "approved",
            reviewed_by: userEmail || null,
            reviewed_at: new Date().toISOString(),
            review_note: "Approved auto-linked match.",
            updated_at: new Date().toISOString(),
          })
          .eq("certificate_id", cert.id)
          .eq("status", "auto_linked");
        if (error) throw error;
        addLog(`Approved ${certTitle(cert)}`, "success");
        await loadFromDB();
      } catch (err) {
        addLog(`Approval failed: ${err.message}`, "error");
      } finally {
        setLoading("");
      }
    },
    [addLog, loadFromDB, userEmail]
  );

  const approveManual = useCallback(
    async (cert) => {
      setLoading(`Approving manual cert ${certTitle(cert)}...`);
      try {
        const { error } = await supabase
          .from("certificate_matches")
          .update({
            status: "approved",
            reviewed_by: userEmail || null,
            reviewed_at: new Date().toISOString(),
            review_note: "Manually reviewed and approved — no automated allocation performed.",
            updated_at: new Date().toISOString(),
          })
          .eq("certificate_id", cert.id)
          .eq("status", "manual_only");
        if (error) throw error;
        addLog(`Manually approved ${certTitle(cert)}`, "success");
        await loadFromDB();
      } catch (err) {
        addLog(`Manual approval failed: ${err.message}`, "error");
      } finally {
        setLoading("");
      }
    },
    [addLog, loadFromDB, userEmail]
  );

  const bulkApproveAndGenerate = useCallback(
    async () => {
      const autoLinked = certs.filter((c) => c.match?.status === "auto_linked");
      if (!autoLinked.length) {
        addLog("No auto-linked certificates to approve.", "info");
        return;
      }

      if (loading) {
        addLog("Another operation is in progress. Please wait.", "error");
        return;
      }

      setLoading(`Approving ${autoLinked.length} certificates...`);
      try {
        const ids = autoLinked.map((c) => c.id);
        const { error } = await supabase
          .from("certificate_matches")
          .update({
            status: "approved",
            reviewed_by: userEmail || null,
            reviewed_at: new Date().toISOString(),
            review_note: "Bulk approved auto-linked match.",
            updated_at: new Date().toISOString(),
          })
          .in("certificate_id", ids)
          .eq("status", "auto_linked");
        if (error) throw error;
        addLog(`Approved ${autoLinked.length} auto-linked certificates`, "success");

        const freshData = await loadFromDB();
        if (!freshData) return;

        const freshClientCertRecords = freshData.clientCertificateRecords || [];
        const freshClientCertRecordsByKey = new Map(freshClientCertRecords.map((r) => [r.group_key, r]));
        const existingInternalRefs = new Set(freshClientCertRecords.filter((r) => r.generated_file_path).map((r) => r.internal_reference));
        const groups = collectApprovedClientCertificateGroups(freshData.certs, freshData.invoiceRows, new Date(), freshData.companiesByName || companiesByName).map((group) => {
          const persisted = freshClientCertRecordsByKey.get(group.group_key) || null;
          const alreadyGenerated = persisted?.generated_file_path || existingInternalRefs.has(group.internal_reference);
          return alreadyGenerated
            ? { ...group, generated_file_path: persisted?.generated_file_path || "exists" }
            : { ...group, generated_file_path: "" };
        });

        const toGenerate = groups.filter((g) => g.can_generate && !g.generated_file_path);
        if (!toGenerate.length) {
          addLog("All client certificates already generated or no valid groups to generate.", "info");
          return;
        }

        const { generateClientCertificatePdf } = await import("./src/clientCertificatePdf.js");
        let generated = 0;
        let failures = 0;
        for (let i = 0; i < toGenerate.length; i++) {
          const group = toGenerate[i];
          setLoading(`Generating client cert ${i + 1}/${toGenerate.length}: ${group.internal_reference}`);
          try {
            const pdfBytes = await generateClientCertificatePdf(group);
            const filePath = `generated/${Date.now()}-${group.internal_reference}.pdf`;
            const pdfBlob = new Blob([pdfBytes], { type: "application/pdf" });
            const { error: uploadErr } = await supabase.storage
              .from("client-certificates-pdf")
              .upload(filePath, pdfBlob, { contentType: "application/pdf", upsert: false });
            if (uploadErr) throw uploadErr;

            const payload = {
              group_key: group.group_key,
              client_name: group.client_name,
              airport_code: group.airport_code,
              month: group.month,
              total_saf_volume_m3: group.total_saf_volume_m3,
              source_certificate_refs: group.source_certificate_refs,
              source_certificate_ids: group.source_certificate_ids,
              source_invoice_row_ids: group.source_invoice_row_ids,
              source_link_ids: group.source_link_ids,
              approved_link_count: group.approved_link_count,
              matched_row_count: group.matched_row_count,
              issue_date: group.issue_date,
              internal_reference: group.internal_reference,
              generated_file_path: filePath,
              updated_at: new Date().toISOString(),
            };
            const { error: saveErr } = await supabase.from("client_certificates").upsert(payload, { onConflict: "group_key" });
            if (saveErr) throw saveErr;
            generated++;
          } catch (genErr) {
            failures++;
            addLog(`Failed to generate ${group.internal_reference}: ${genErr.message}`, "error");
          }
        }

        addLog(
          `Client certificates: ${generated} generated` + (failures ? `, ${failures} failed` : ""),
          failures ? "error" : "success"
        );
        await loadFromDB();
      } catch (err) {
        addLog(`Bulk approval failed: ${err.message}`, "error");
      } finally {
        setLoading("");
      }
    },
    [certs, addLog, loading, loadFromDB, userEmail]
  );

  const openClientCertificatePdf = useCallback(
    async (group) =>
      openStoredPdf({
        bucket: "client-certificates-pdf",
        path: group?.generated_file_path,
        title: group?.internal_reference || "Client certificate",
      }),
    [openStoredPdf]
  );

  const generateClientCertificate = useCallback(
    async (group) => {
      if (!group) return;
      if (group.generated_file_path) {
        addLog(`Client certificate ${group.internal_reference} already generated.`, "info");
        return;
      }
      // Check DB for existing record with same internal_reference (handles case-sensitivity duplicates)
      const { data: existingRef } = await supabase
        .from("client_certificates")
        .select("id, internal_reference, generated_file_path")
        .eq("internal_reference", group.internal_reference)
        .maybeSingle();
      if (existingRef?.generated_file_path) {
        addLog(`Client certificate ${group.internal_reference} already exists in database.`, "info");
        return;
      }
      const validationErrors = group.validation_errors || [];
      if (validationErrors.length) {
        addLog(`Client certificate generation blocked: ${validationErrors.join(" | ")}`, "error");
        return;
      }

      setLoading(`Generating ${group.internal_reference}...`);
      try {
        const { generateClientCertificatePdf } = await import("./src/clientCertificatePdf.js");
        const pdfBytes = await generateClientCertificatePdf(group);
        const filePath = `generated/${Date.now()}-${group.internal_reference}.pdf`;
        const pdfBlob = new Blob([pdfBytes], { type: "application/pdf" });
        const { error: uploadErr } = await supabase.storage
          .from("client-certificates-pdf")
          .upload(filePath, pdfBlob, { contentType: "application/pdf", upsert: false });
        if (uploadErr) throw uploadErr;

        const payload = {
          group_key: group.group_key,
          client_name: group.client_name,
          airport_code: group.airport_code,
          month: group.month,
          total_saf_volume_m3: group.total_saf_volume_m3,
          source_certificate_refs: group.source_certificate_refs,
          source_certificate_ids: group.source_certificate_ids,
          source_invoice_row_ids: group.source_invoice_row_ids,
          source_link_ids: group.source_link_ids,
          approved_link_count: group.approved_link_count,
          matched_row_count: group.matched_row_count,
          issue_date: group.issue_date,
          internal_reference: group.internal_reference,
          generated_file_path: filePath,
          updated_at: new Date().toISOString(),
        };

        const { error: saveErr } = await supabase.from("client_certificates").upsert(payload, { onConflict: "group_key" });
        if (saveErr) throw saveErr;

        addLog(`Generated client certificate ${group.internal_reference}`, "success");
        setLoading("");
        await loadFromDB();
        setTab("clientCerts");
      } catch (error) {
        setLoading("");
        addLog(`Client certificate generation failed for ${group?.internal_reference || "group"}: ${error.message}`, "error");
      }
    },
    [addLog, loadFromDB]
  );

  const selectedCert = selected !== null ? certs[selected] : null;
  const normalizedSelectedCert = selectedCert ? buildNormalizedCertificateView(selectedCert) : null;
  const selectedCertVolume = normalizeVolumeNumber(selectedCert?.match?.cert_volume_m3 ?? selectedCert?.data?.quantity);
  const selectedAllocatedVolume = normalizeVolumeNumber(selectedCert?.match?.allocated_volume_m3) || 0;
  const selectedRemainingVolume =
    selectedCertVolume !== null ? Number(Math.max(0, selectedCertVolume - selectedAllocatedVolume).toFixed(6)) : null;
  const selectedMatchedRowCount = selectedCert?.match?.links?.length || 0;
  const selectedInterpretedMonth =
    selectedCert?.data?.coverageMonth ||
    normalizeMonthValue(normalizedSelectedCert?.interpreted_period_start) ||
    normalizeMonthValue(normalizedSelectedCert?.interpreted_dispatch_date) ||
    "";
  const selectedIsQuarterly = selectedCert?.data?.coverageGranularity === "quarter";
  const selectedPeriodLabel = selectedIsQuarterly
    ? `${selectedCert?.data?.coverageStart} to ${selectedCert?.data?.coverageEnd}`
    : selectedInterpretedMonth;
  const selectedSupportReason =
    selectedCert?.support_reason ||
    selectedCert?.data?.support_reason ||
    selectedCert?.classification_reason ||
    selectedCert?.data?.classification_reason ||
    "—";
  const stats = {
    totalCerts: certs.length,
    supported: certs.filter((cert) => {
      const fam = cert.document_family || cert.data?.document_family;
      return fam === "supported_simple" || fam === "supported_poc";
    }).length,
    manualOnly: certs.filter((cert) => {
      const fam = cert.document_family || cert.data?.document_family;
      return fam === "manual_only";
    }).length,
    matched: certs.filter((cert) => ["auto_linked", "approved"].includes(cert.match?.status)).length,
    partialLinked: certs.filter((cert) => cert.match?.status === "partial_linked").length,
    unmatched: certs.filter((cert) => cert.match?.status === "unmatched").length,
    allocatedRows: invoiceRows.filter((row) => row.allocation_status === "allocated" || row.allocation_status === "partial").length,
  };
  const clientCertificateRecordsByKey = new Map((clientCertificateRecords || []).map((row) => [row.group_key, row]));
  const liveGroups = collectApprovedClientCertificateGroups(certs, invoiceRows, new Date(), companiesByName).map((group) => {
    const persisted = clientCertificateRecordsByKey.get(group.group_key) || null;
    return persisted
      ? {
          ...group,
          issue_date: persisted.issue_date || group.issue_date,
          issue_date_display: formatClientCertificateDate(persisted.issue_date || group.issue_date),
          internal_reference: persisted.internal_reference || group.internal_reference,
          generated_file_path: persisted.generated_file_path || "",
          created_at: persisted.created_at || null,
          persisted_id: persisted.id,
        }
      : {
          ...group,
          generated_file_path: "",
          created_at: null,
          persisted_id: null,
        };
  });

  const liveGroupKeys = new Set(liveGroups.map((g) => g.group_key));
  const persistedOnlyGroups = (clientCertificateRecords || [])
    .filter((r) => r.generated_file_path && !liveGroupKeys.has(r.group_key))
    .map((r) => ({
      group_key: r.group_key,
      client_name: r.client_name || "",
      airport_code: r.airport_code || "",
      month: r.month || "",
      month_label: formatClientCertificateMonth(r.month),
      issue_date: r.issue_date || "",
      issue_date_display: formatClientCertificateDate(r.issue_date),
      internal_reference: r.internal_reference || "",
      total_saf_volume_m3: Number(r.total_saf_volume_m3 || 0),
      source_certificate_refs: r.source_certificate_refs || [],
      source_certificate_ids: r.source_certificate_ids || [],
      source_invoice_row_ids: r.source_invoice_row_ids || [],
      source_link_ids: r.source_link_ids || [],
      approved_link_count: r.approved_link_count || 0,
      matched_row_count: r.matched_row_count || 0,
      validation_errors: [],
      can_generate: false,
      generated_file_path: r.generated_file_path || "",
      created_at: r.created_at || null,
      persisted_id: r.id,
    }));

  const clientCertificateGroups = [...liveGroups, ...persistedOnlyGroups].sort((a, b) =>
    (a.client_name || "").localeCompare(b.client_name || "") ||
    (a.airport_code || "").localeCompare(b.airport_code || "") ||
    (a.month || "").localeCompare(b.month || "")
  );

  const bulkGenerateClientCerts = async () => {
    const toGenerate = clientCertificateGroups.filter((g) => g.can_generate && !g.generated_file_path);
    if (!toGenerate.length) {
      addLog("No client certificates to generate.", "info");
      return;
    }
    const { generateClientCertificatePdf } = await import("./src/clientCertificatePdf.js");
    let generated = 0;
    let failures = 0;
    for (let i = 0; i < toGenerate.length; i++) {
      const group = toGenerate[i];
      setLoading(`Generating client cert ${i + 1}/${toGenerate.length}: ${group.internal_reference}`);
      try {
        const pdfBytes = await generateClientCertificatePdf(group);
        const filePath = `generated/${Date.now()}-${group.internal_reference}.pdf`;
        const pdfBlob = new Blob([pdfBytes], { type: "application/pdf" });
        const { error: uploadErr } = await supabase.storage
          .from("client-certificates-pdf")
          .upload(filePath, pdfBlob, { contentType: "application/pdf", upsert: false });
        if (uploadErr) throw uploadErr;
        const payload = {
          group_key: group.group_key,
          client_name: group.client_name,
          airport_code: group.airport_code,
          month: group.month,
          total_saf_volume_m3: group.total_saf_volume_m3,
          source_certificate_refs: group.source_certificate_refs,
          source_certificate_ids: group.source_certificate_ids,
          source_invoice_row_ids: group.source_invoice_row_ids,
          source_link_ids: group.source_link_ids,
          approved_link_count: group.approved_link_count,
          matched_row_count: group.matched_row_count,
          issue_date: group.issue_date,
          internal_reference: group.internal_reference,
          generated_file_path: filePath,
          updated_at: new Date().toISOString(),
        };
        const { error: saveErr } = await supabase.from("client_certificates").upsert(payload, { onConflict: "group_key" });
        if (saveErr) throw saveErr;
        generated++;
      } catch (genErr) {
        failures++;
        addLog(`Failed to generate ${group.internal_reference}: ${genErr.message}`, "error");
      }
    }
    addLog(
      `Client certificates: ${generated} generated` + (failures ? `, ${failures} failed` : ""),
      failures ? "error" : "success"
    );
    setLoading("");
    await loadFromDB();
  };

  const coverageData = buildCoverageData(certs, clientCertificateRecords);

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
                      if (prev?.url?.startsWith("blob:")) URL.revokeObjectURL(prev.url);
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
              2025 CSV: {invoiceImport.filename} · {invoiceImport.candidate_row_count || invoiceImport.row_count} rows
              {invoiceImport.invalid_row_count ? ` (${invoiceImport.invalid_row_count} invalid)` : ""}
              {invoiceImport.status ? ` · ${String(invoiceImport.status).toUpperCase()}` : ""}
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

        <button
          className="btn"
          onClick={analyzeAll}
          disabled={!certs.length || !!loading}
          style={{
            background: !certs.length || loading ? "#0a1628" : "linear-gradient(135deg,#0050aa,#00bfff)",
            color: !certs.length || loading ? "#4a7fa0" : "#fff",
            padding: "7px 18px",
            borderRadius: 6,
            fontFamily: "'Space Mono', monospace",
            fontSize: 11,
            letterSpacing: 1,
            border: !certs.length || loading ? "1px solid #0d3060" : "none",
          }}
        >
          MATCH ALL
        </button>
        {certs.some((c) => c.match?.status === "auto_linked") ? (
          <button
            className="btn"
            onClick={bulkApproveAndGenerate}
            disabled={!!loading}
            style={{
              background: loading ? "#0a1628" : "linear-gradient(135deg,#006030,#00ff9d)",
              color: loading ? "#4a7fa0" : "#001a0d",
              padding: "7px 18px",
              borderRadius: 6,
              fontFamily: "'Space Mono', monospace",
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: 1,
              border: loading ? "1px solid #0d3060" : "none",
            }}
          >
            APPROVE ALL ({certs.filter((c) => c.match?.status === "auto_linked").length})
          </button>
        ) : null}
      </div>

      <div style={{ display: "flex", gap: 1, borderBottom: "1px solid #0d2040", background: "#030d1a" }}>
        {[
          { label: "CERTS", val: stats.totalCerts, color: "#00bfff" },
          { label: "SUPPORTED", val: stats.supported, color: "#00ff9d" },
          { label: "MATCHED", val: stats.matched, color: "#00ff9d" },
          { label: "MANUAL ONLY", val: stats.manualOnly, color: "#ffbb00" },
          { label: "PARTIAL", val: stats.partialLinked, color: "#ff9933" },
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
          ["dashboard", "DASHBOARD"],
          ["certDetail", "SAF DATA"],
          ["certs", "CERTIFICATES"],
          ["clientCerts", "CLIENT CERTIFICATES"],
          ["coverage", "COVERAGE"],
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
        {tab === "dashboard" ? (
          <DashboardTab certs={certs} invoiceRows={invoiceRows} onSelectCert={setSelected} onSwitchTab={setTab} />
        ) : null}

        {tab === "certDetail" ? (
          <CertDetailTable certs={certs} invoiceRows={invoiceRows} onSelectCert={setSelected} onSwitchTab={setTab} />
        ) : null}

        {tab === "certs" ? (
          <>
            <div style={{ width: 380, borderRight: "1px solid #0d2040", overflowY: "auto", padding: 12 }}>
              {!certs.length ? (
                <div style={{ padding: "40px 20px", textAlign: "center", color: "#4a7fa0" }}>
                  <div style={{ fontSize: 36, marginBottom: 12 }}>✈</div>
                  <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 11 }}>Import SAF certificate PDFs to begin</div>
                </div>
              ) : (() => {
                const certIdsWithClientCert = new Set();
                clientCertificateGroups.forEach((g) => {
                  if (g.generated_file_path && g.source_certificate_ids) {
                    g.source_certificate_ids.forEach((id) => certIdsWithClientCert.add(id));
                  }
                });

                const approvedCount = certs.filter((c) => c.match?.status === "approved").length;
                const autoLinkedCount = certs.filter((c) => c.match?.status === "auto_linked").length;
                const partialLinkedCount = certs.filter((c) => c.match?.status === "partial_linked").length;
                const unmatchedCount = certs.filter((c) => c.match?.status === "unmatched").length;
                const manualCount = certs.filter((c) => c.match?.status === "manual_only").length;
                const processedCount = approvedCount + autoLinkedCount;
                const processedPct = certs.length ? Math.round((processedCount / certs.length) * 100) : 0;
                const approvedPct = certs.length ? (approvedCount / certs.length) * 100 : 0;
                const autoLinkedPct = certs.length ? (autoLinkedCount / certs.length) * 100 : 0;
                const partialLinkedPct = certs.length ? (partialLinkedCount / certs.length) * 100 : 0;

                const statusOrder = { unmatched: 0, partial_linked: 1, auto_linked: 2, manual_only: 3, approved: 4 };
                const sortedCerts = [...certs].sort((a, b) => {
                  const sa = statusOrder[a.match?.status] ?? -1;
                  const sb = statusOrder[b.match?.status] ?? -1;
                  return sa - sb;
                });

                const filteredCerts = certStatusFilter
                  ? sortedCerts.filter((c) => {
                      if (certStatusFilter === "completed") return c.match?.status === "approved";
                      return c.match?.status === certStatusFilter;
                    })
                  : sortedCerts;

                const filterBtns = [
                  { key: "", label: "ALL", count: certs.length, color: "#4a7fa0" },
                  { key: "unmatched", label: "ATTENTION", count: unmatchedCount, color: "#ff6666" },
                  { key: "partial_linked", label: "PARTIAL", count: partialLinkedCount, color: "#ff9933" },
                  { key: "auto_linked", label: "TO APPROVE", count: autoLinkedCount, color: "#00bfff" },
                  { key: "manual_only", label: "MANUAL", count: manualCount, color: "#ffbb00" },
                  { key: "completed", label: "DONE", count: approvedCount, color: "#00ff9d" },
                ];

                return (
                  <>
                    <div style={{ marginBottom: 10, fontFamily: "'Space Mono', monospace", fontSize: 10 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4, color: "#4a7fa0" }}>
                        <span>{processedCount}/{certs.length} processed</span>
                        <span style={{ color: "#00bfff" }}>{processedPct}%</span>
                      </div>
                      <div style={{ height: 6, background: "#0a1628", borderRadius: 3, overflow: "hidden", display: "flex" }}>
                        <div style={{ width: `${approvedPct}%`, background: "#00ff9d", transition: "width 0.3s" }} />
                        <div style={{ width: `${autoLinkedPct}%`, background: "#00bfff", transition: "width 0.3s" }} />
                        <div style={{ width: `${partialLinkedPct}%`, background: "#ff9933", transition: "width 0.3s" }} />
                      </div>
                      <div style={{ display: "flex", gap: 10, marginTop: 3, fontSize: 9, color: "#4a7fa0" }}>
                        <span><span style={{ color: "#00ff9d" }}>■</span> Approved</span>
                        <span><span style={{ color: "#00bfff" }}>■</span> Matched</span>
                        <span><span style={{ color: "#ff9933" }}>■</span> Partial</span>
                      </div>
                    </div>

                    <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginBottom: 10 }}>
                      {filterBtns.map((fb) => (
                        <button
                          key={fb.key}
                          onClick={() => { setCertStatusFilter(fb.key); setSelected(null); }}
                          style={{
                            background: certStatusFilter === fb.key ? fb.color + "22" : "#060e1a",
                            border: `1px solid ${certStatusFilter === fb.key ? fb.color : "#0d2040"}`,
                            color: certStatusFilter === fb.key ? fb.color : "#4a7fa0",
                            borderRadius: 4,
                            padding: "3px 7px",
                            fontFamily: "'Space Mono', monospace",
                            fontSize: 9,
                            cursor: "pointer",
                            letterSpacing: 0.5,
                            transition: "all 0.15s",
                          }}
                        >
                          {fb.label} ({fb.count})
                        </button>
                      ))}
                    </div>

                    {filteredCerts.map((cert) => {
                      const origIndex = certs.indexOf(cert);
                      return (
                        <CertCard
                          key={cert.id || origIndex}
                          cert={cert}
                          index={origIndex}
                          selected={selected === origIndex}
                          onSelect={setSelected}
                          onAnalyze={analyzeSingle}
                          onReExtract={reExtractCert}
                          onOpenPdf={openCertificatePdf}
                          hasClientCert={certIdsWithClientCert.has(cert.id)}
                        />
                      );
                    })}
                  </>
                );
              })()}
            </div>

            <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
              {selectedCert ? (
                <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: 16 }}>
                    <div>
                      <div style={{ fontFamily: "'Space Mono', monospace", color: "#00bfff", fontSize: 12 }}>{selectedCert.data?.docType || "CERTIFICATE"}</div>
                      <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 4, flexWrap: "wrap" }}>
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
                      <div style={{ color: "#4a7fa0", fontSize: 11, marginTop: 4 }}>
                        Uploaded certificate with simplified deterministic matching only.
                      </div>
                    </div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 6, alignItems: "flex-end" }}>
                      <Badge status={selectedCert.document_family || selectedCert.data?.document_family || "unknown"} />
                      {selectedCert.match ? <Badge status={selectedCert.match.status} /> : null}
                    </div>
                  </div>

                  {/* Alert banners for suspect data */}
                  {Number(selectedCert.match?.cert_volume_m3 ?? selectedCert.data?.quantity) > 50 && (
                    <div style={{ padding: "10px 14px", background: "#1a0d00", border: "1px solid #ff993344", borderLeft: "3px solid #ff9933", borderRadius: 6, marginBottom: 12, fontFamily: "'Space Mono', monospace", fontSize: 11, color: "#ff9933" }}>
                      ⚠ Volume exceeds 50 m³ ({Number(selectedCert.match?.cert_volume_m3 ?? selectedCert.data?.quantity).toFixed(1)} m³). This likely indicates a number format error during PDF extraction. Consider re-extracting this certificate.
                    </div>
                  )}
                  {selectedCert.match?.status === "partial_linked" && Number(selectedCert.match?.cert_volume_m3) > 0 && Number(selectedCert.match?.allocated_volume_m3) / Number(selectedCert.match?.cert_volume_m3) < 0.10 && !selectedCert.match?.match_method?.includes("widened") && (
                    <div style={{ padding: "10px 14px", background: "#1a0d00", border: "1px solid #ff993344", borderLeft: "3px solid #ff9933", borderRadius: 6, marginBottom: 12, fontFamily: "'Space Mono', monospace", fontSize: 11, color: "#ff9933" }}>
                      📉 Only {(Number(selectedCert.match?.allocated_volume_m3) / Number(selectedCert.match?.cert_volume_m3) * 100).toFixed(1)}% of the volume was allocated. This strongly suggests an extraction error rather than missing invoice data.
                    </div>
                  )}
                  {selectedCert.match?.diagnostics?.widened && (
                    <div style={{ padding: "10px 14px", background: "#061423", border: "1px solid #00bfff33", borderLeft: "3px solid #00bfff", borderRadius: 6, marginBottom: 12, fontFamily: "'Space Mono', monospace", fontSize: 11, color: "#c8dff0" }}>
                      📅 Allocation widened from <span style={{ color: "#00bfff" }}>{selectedCert.match.diagnostics.original_period_start} – {selectedCert.match.diagnostics.original_period_end}</span> to <span style={{ color: "#00bfff" }}>{selectedCert.match.diagnostics.widened_period_start} – {selectedCert.match.diagnostics.widened_period_end}</span> (quarter).
                      {selectedCert.match.diagnostics.widen_candidate_count > 0 ? ` ${selectedCert.match.diagnostics.widen_candidate_count} additional rows from neighboring months.` : ""}
                    </div>
                  )}

                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}>
                    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
                      <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                        CERTIFICATE
                      </div>
                      <FieldRow label="ISSUER" value={selectedCert.data?.issuer} />
                      <FieldRow label="SAF SUPPLIER" value={selectedCert.data?.safSupplier} highlight />
                      <FieldRow label="RECIPIENT" value={selectedCert.data?.recipient} />
                      <FieldRow label="CONTRACT NR" value={selectedCert.data?.contractNumber} />
                      <FieldRow label="DATE DISPATCH" value={selectedCert.data?.dateDispatch} />
                      <FieldRow label="ISSUED" value={selectedCert.data?.dateIssuance} />
                      <FieldRow label="PRODUCT TYPE" value={selectedCert.data?.productType} />
                      <FieldRow label="RAW MATERIAL" value={selectedCert.data?.rawMaterial} />
                      <FieldRow label="ORIGIN" value={selectedCert.data?.rawMaterialOrigin} />
                      <FieldRow label="SAF QUANTITY" value={`${formatVolume(selectedCert.data?.quantity)} ${selectedCert.data?.quantityUnit || "m3"}`} highlight />
                    </div>

                    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
                      <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                        INTERPRETED MATCHING SCOPE
                      </div>
                      <FieldRow label="AIRPORT" value={formatListValue(normalizedSelectedCert?.interpreted_airports)} highlight />
                      <FieldRow label={selectedIsQuarterly ? "QUARTER" : "MONTH"} value={selectedPeriodLabel} highlight />
                      <FieldRow
                        label="SAF VOLUME M3"
                        value={
                          normalizedSelectedCert?.interpreted_saf_volume !== null && normalizedSelectedCert?.interpreted_saf_volume !== undefined
                            ? formatVolume(normalizedSelectedCert.interpreted_saf_volume)
                            : formatVolume(selectedCert.data?.quantity)
                        }
                        highlight
                      />
                      <FieldRow label="MATCHING MODE" value={selectedCert.matching_mode || selectedCert.data?.matching_mode || selectedCert.data?.matchingMode} />
                      <FieldRow label="MATCHING EVIDENCE" value={selectedCert.data?.matchingEvidence} />
                      <FieldRow
                        label="CANONICAL AIRPORTS"
                        value={(selectedCert.data?.canonicalAirports || [])
                          .map((item) => {
                            const codes = [item.iata, item.icao].filter(Boolean).join("/");
                            return codes ? `${item.raw || item.label} → ${codes}` : item.raw || item.label;
                          })
                          .join(", ")}
                      />
                      <FieldRow
                        label="WARNINGS"
                        value={
                          normalizedSelectedCert?.warnings?.length
                            ? normalizedSelectedCert.warnings.join(" | ")
                            : "No normalization warnings."
                        }
                      />
                    </div>

                    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
                      <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                        CLASSIFICATION
                      </div>
                      <FieldRow label="SUPPORT STATUS" value={selectedCert.document_family || selectedCert.data?.document_family} highlight />
                      <FieldRow label="SUPPORT REASON" value={selectedSupportReason} />
                      <FieldRow label="MATCHING MODE" value={selectedCert.matching_mode || selectedCert.data?.matching_mode || selectedCert.data?.matchingMode} />
                      <FieldRow label="CONFIDENCE" value={formatConfidence(selectedCert.classification_confidence ?? selectedCert.data?.classification_confidence)} />
                      <FieldRow
                        label="REVIEW REQUIRED"
                        value={
                          typeof (selectedCert.review_required ?? selectedCert.data?.review_required) === "boolean"
                            ? (selectedCert.review_required ?? selectedCert.data?.review_required)
                              ? "Yes"
                              : "No"
                            : "—"
                        }
                      />
                    </div>

                    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
                      <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                        ALLOCATION RESULT
                      </div>
                      <FieldRow label="AUTO-LINK RESULT" value={selectedCert.match?.status?.replace(/_/g, " ") || "Not matched yet"} highlight />
                      <FieldRow label="METHOD" value={selectedCert.match?.match_method} />
                      <FieldRow label="ALLOCATED SAF VOLUME" value={`${formatVolume(selectedAllocatedVolume)} m3`} highlight />
                      <FieldRow label="REMAINING SAF VOLUME" value={selectedRemainingVolume !== null ? `${formatVolume(selectedRemainingVolume)} m3` : "—"} />
                      <FieldRow label="MATCHED ROW COUNT" value={String(selectedMatchedRowCount)} />
                      <FieldRow label="FILTER COUNTS" value={formatDiagnosticsSummary(selectedCert.match?.diagnostics)} />
                      <FieldRow label="UNRESOLVED AIRPORTS" value={formatUnresolvedAirports(selectedCert.match?.diagnostics)} />
                      <FieldRow label="NOTE" value={selectedCert.match?.review_note || "Run matching to generate an allocation result."} />
                      {(selectedCert.match?.status === "unmatched" || selectedCert.match?.status === "partial_linked") && selectedCert.match?.diagnostics ? (() => {
                        const diag = selectedCert.match.diagnostics;
                        const isPartial = selectedCert.match.status === "partial_linked";
                        const totalRows = diag.total_row_count || 0;
                        const availableRows = diag.candidate_count || 0;
                        const accentColor = isPartial ? "#ff9933" : "#ff6666";
                        // For unmatched: distinguish "no rows" from "rows fully consumed"
                        const periodLabel = (diag.coverage_granularity === "quarter" || selectedIsQuarterly) ? "quarter" : "month";
                        const unmatchedReason = !isPartial
                          ? totalRows === 0
                            ? `No invoice rows found for this airport + ${periodLabel} combination.`
                            : `${totalRows} invoice row${totalRows > 1 ? "s" : ""} found for this airport + ${periodLabel}, but all volume is already consumed by other certificates.`
                          : null;
                        return (
                          <div style={{ marginTop: 8, padding: "8px 10px", background: isPartial ? "#1a0d0022" : "#1a000022", border: `1px solid ${accentColor}44`, borderRadius: 6 }}>
                            <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 10, color: accentColor, marginBottom: 4, letterSpacing: 1 }}>
                              {isPartial ? "SUPPLY / DEMAND GAP" : "UNMATCHED REASON"}
                            </div>
                            {unmatchedReason ? (
                              <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 11, color: accentColor, marginBottom: 4 }}>
                                {unmatchedReason}
                              </div>
                            ) : null}
                            <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 11, color: "#c8dff0" }}>
                              Available: {Number(diag.available_volume || 0).toFixed(3)} m³ across {availableRows} invoice row{availableRows !== 1 ? "s" : ""}
                            </div>
                            <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 11, color: "#c8dff0" }}>
                              Needed: {Number(diag.certificate_volume || 0).toFixed(3)} m³
                            </div>
                            <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 11, color: accentColor, fontWeight: 700 }}>
                              Gap: {Number(diag.variance || 0).toFixed(3)} m³
                            </div>
                          </div>
                        );
                      })() : null}
                    </div>

                    {isSupportedPocCert(selectedCert) &&
                      Array.isArray(selectedCert.match?.diagnostics?.unit_breakdown) &&
                      selectedCert.match.diagnostics.unit_breakdown.length > 0 && (
                        <div style={{ gridColumn: "1 / -1", background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
                          <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                            PER-AIRPORT ALLOCATION BREAKDOWN ({selectedCert.match.diagnostics.unit_breakdown.length} airports)
                          </div>
                          <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "'Space Mono', monospace", fontSize: 10 }}>
                            <thead>
                              <tr style={{ color: "#4a7fa0" }}>
                                <th style={{ textAlign: "left", padding: "4px 8px" }}>AIRPORT</th>
                                <th style={{ textAlign: "left", padding: "4px 8px" }}>PERIOD</th>
                                <th style={{ textAlign: "right", padding: "4px 8px" }}>TARGET m³</th>
                                <th style={{ textAlign: "right", padding: "4px 8px" }}>ALLOCATED m³</th>
                                <th style={{ textAlign: "right", padding: "4px 8px" }}>ROWS</th>
                                <th style={{ textAlign: "left", padding: "4px 8px" }}>STATUS</th>
                              </tr>
                            </thead>
                            <tbody>
                              {selectedCert.match.diagnostics.unit_breakdown.map((unit, i) => {
                                const statusColor = unit.status === "auto_linked" ? "#00ff9d" : unit.status === "partial_linked" ? "#ff9933" : unit.status === "skipped" ? "#4a7fa0" : "#ff6666";
                                return (
                                  <tr key={i} style={{ borderTop: "1px solid #0d2040" }}>
                                    <td style={{ padding: "4px 8px", color: "#e0f0ff" }}>{unit.airport_iata || unit.airport_icao || "—"}</td>
                                    <td style={{ padding: "4px 8px", color: "#c8dff0" }}>{unit.period_start ? String(unit.period_start).slice(0, 7) : "—"}</td>
                                    <td style={{ padding: "4px 8px", textAlign: "right", color: "#c8dff0" }}>{Number(unit.target_volume_m3 || 0).toFixed(3)}</td>
                                    <td style={{ padding: "4px 8px", textAlign: "right", color: statusColor, fontWeight: 700 }}>{Number(unit.allocated_volume_m3 || 0).toFixed(3)}</td>
                                    <td style={{ padding: "4px 8px", textAlign: "right", color: "#c8dff0" }}>{unit.linked_count ?? "—"}</td>
                                    <td style={{ padding: "4px 8px", color: statusColor, letterSpacing: 0.5 }}>{(unit.status || "—").replace(/_/g, " ")}</td>
                                  </tr>
                                );
                              })}
                            </tbody>
                          </table>
                        </div>
                    )}

                    <div style={{ gridColumn: "1 / -1" }}>
                      <MatchRowsTable rows={selectedCert.match?.links || []} title="LINKED INVOICE ROWS" />
                    </div>

                    {(selectedCert.match?.status === "unmatched" ||
                      selectedCert.match?.status === "partial_linked" ||
                      selectedCert.match?.status === "manual_only" ||
                      !selectedCert.match) && (
                      <div style={{ gridColumn: "1 / -1" }}>
                        <ManualMatchPanel
                          cert={selectedCert}
                          invoiceRows={invoiceRows}
                          loading={loading}
                          onSave={(ids) => saveManualMatch(selectedCert, ids)}
                        />
                      </div>
                    )}

                    {(selectedCert.match?.status === "auto_linked" || selectedCert.match?.status === "partial_linked") ? (
                      <div
                        style={{
                          gridColumn: "1 / -1",
                          display: "flex",
                          gap: 12,
                          alignItems: "center",
                          padding: "12px 0",
                        }}
                      >
                        <button
                          className="btn"
                          onClick={() => approveAutoLinked(selectedCert)}
                          disabled={!!loading}
                          style={{
                            background: "#003322",
                            color: "#00ff9d",
                            padding: "8px 20px",
                            borderRadius: 6,
                            fontFamily: "'Space Mono', monospace",
                            fontSize: 11,
                            fontWeight: 700,
                            letterSpacing: 1,
                            border: "1px solid #00ff9d44",
                          }}
                        >
                          APPROVE{selectedCert.match?.status === "partial_linked" ? " PARTIAL" : ""}
                        </button>
                        <button
                          className="btn"
                          onClick={() => rejectMatch(selectedCert)}
                          disabled={!!loading}
                          style={{
                            background: "#1a0000",
                            color: "#ff6666",
                            padding: "8px 20px",
                            borderRadius: 6,
                            fontFamily: "'Space Mono', monospace",
                            fontSize: 11,
                            fontWeight: 700,
                            letterSpacing: 1,
                            border: "1px solid #ff666644",
                          }}
                        >
                          REJECT
                        </button>
                        <span style={{ color: "#4a7fa0", fontSize: 10, fontFamily: "'Space Mono', monospace" }}>
                          {selectedCert.match?.status === "partial_linked"
                            ? `Partial match: ${Number(selectedCert.match?.allocated_volume_m3 || 0).toFixed(3)} m³ allocated / ${Number(selectedCert.match?.cert_volume_m3 || 0).toFixed(3)} m³ needed (gap: ${Number(selectedCert.match?.variance_m3 || 0).toFixed(3)} m³). Approve to accept partial allocation.`
                            : "This certificate was auto-linked by the FIFO algorithm. Approve to enable client certificate generation."}
                        </span>
                      </div>
                    ) : null}

                    {selectedCert.match?.status === "manual_only" ? (
                      <div
                        style={{
                          gridColumn: "1 / -1",
                          display: "flex",
                          gap: 12,
                          alignItems: "center",
                          padding: 12,
                          background: "#1a1200",
                          borderRadius: 8,
                          border: "1px solid #ffbb0033",
                        }}
                      >
                        <button
                          className="btn"
                          onClick={() => approveManual(selectedCert)}
                          disabled={!!loading}
                          style={{
                            background: "linear-gradient(135deg,#006633,#00ff9d)",
                            color: "#000",
                            padding: "8px 20px",
                            borderRadius: 6,
                            fontFamily: "'Space Mono', monospace",
                            fontSize: 11,
                            fontWeight: 700,
                            letterSpacing: 1,
                          }}
                        >
                          APPROVE (MANUAL REVIEW)
                        </button>
                        <button
                          className="btn"
                          onClick={() => rejectMatch(selectedCert)}
                          disabled={!!loading}
                          style={{
                            background: "#1a0000",
                            color: "#ff6666",
                            padding: "8px 20px",
                            borderRadius: 6,
                            fontFamily: "'Space Mono', monospace",
                            fontSize: 11,
                            fontWeight: 700,
                            letterSpacing: 1,
                            border: "1px solid #ff666644",
                          }}
                        >
                          REJECT
                        </button>
                        <span style={{ color: "#ffbb00", fontSize: 10, fontFamily: "'Space Mono', monospace" }}>
                          This certificate requires manual review. No automated allocation was performed.
                        </span>
                      </div>
                    ) : null}

                    {(selectedCert.match?.candidate_sets || []).length ? (
                      <div
                        style={{
                          gridColumn: "1 / -1",
                          background: "#060e1a",
                          borderRadius: 8,
                          padding: 16,
                          border: "1px solid #0d2040",
                          display: "flex",
                          flexDirection: "column",
                          gap: 12,
                        }}
                      >
                        <div>
                          <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 10, letterSpacing: 1 }}>
                            LEGACY REVIEW CONTROLS
                          </div>
                          <div style={{ color: "#4a7fa0", fontSize: 11, marginTop: 4 }}>
                            These controls are only shown for existing records that still contain legacy candidate groups.
                          </div>
                        </div>
                        {selectedCert.match.candidate_sets.map((candidate, candidateIndex) => {
                          const isExpanded = expandedCandidates[`${selectedCert.id}-${candidateIndex}`];
                          return (
                            <div key={`${selectedCert.id}-${candidateIndex}`} style={{ background: "#091220", borderRadius: 8, border: "1px solid #1a304f", padding: 14 }}>
                              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12 }}>
                                <div>
                                  <div style={{ color: "#00bfff", fontFamily: "'Space Mono', monospace", fontSize: 11 }}>
                                    Candidate {candidateIndex + 1} · {candidate.match_method}
                                  </div>
                                  <div style={{ color: "#c8dff0", fontSize: 12 }}>
                                    {formatVolume(candidate.total_volume_m3)} m3 linked · variance {formatVolume(candidate.variance_m3)} m3
                                  </div>
                                </div>
                                <div style={{ display: "flex", gap: 8, flexWrap: "wrap", justifyContent: "flex-end" }}>
                                  <button
                                    className="btn"
                                    onClick={() => approveCandidate(selectedCert, candidateIndex)}
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
                                        [`${selectedCert.id}-${candidateIndex}`]: !prev[`${selectedCert.id}-${candidateIndex}`],
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
                            onClick={() => rejectMatch(selectedCert)}
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

        {tab === "db" ? (
          <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
            <style>{`.invoice-row:hover { background: #0a2040 !important; }`}</style>
            {!invoiceRows.length ? (
              <div style={{ textAlign: "center", color: "#4a7fa0", padding: 60 }}>
                <div style={{ fontSize: 36, marginBottom: 12 }}>📊</div>
                <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 11 }}>Load the 2025 invoice CSV to create row-level matches.</div>
              </div>
            ) : (() => {
              const searchLower = (invoiceCustomerSearch || "").toLowerCase();
              const monthNames = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
              const getRowMonth = (row) => {
                if (!row.uplift_date) return null;
                const d = new Date(row.uplift_date);
                if (isNaN(d)) return null;
                return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
              };
              const filteredRows = invoiceRows.filter((row) => {
                if (invoiceStatusFilter) {
                  const status = row.allocation_status || (row.is_allocated ? "allocated" : "free");
                  if (invoiceStatusFilter !== status) return false;
                }
                if (invoiceAirportFilter && row.iata !== invoiceAirportFilter) return false;
                if (invoiceMonthFilter && getRowMonth(row) !== invoiceMonthFilter) return false;
                if (searchLower && !(row.customer || "").toLowerCase().includes(searchLower) && !(row.invoice_no || "").toLowerCase().includes(searchLower)) return false;
                return true;
              });
              const uniqueAirports = [...new Set(invoiceRows.map((r) => r.iata).filter((a) => a && /^[A-Z]{3,4}$/.test(a)))].sort();
              const uniqueMonths = [...new Set(invoiceRows.map((r) => getRowMonth(r)).filter(Boolean))].sort();
              return (
                <>
                  <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 16, flexWrap: "wrap", fontFamily: "'Space Mono', monospace", fontSize: 11 }}>
                    <select value={invoiceStatusFilter} onChange={(e) => setInvoiceStatusFilter(e.target.value)}
                      style={{ background: "#0a1628", color: "#c8dff0", border: "1px solid #0d3060", borderRadius: 6, padding: "6px 10px", fontSize: 11, fontFamily: "'Space Mono', monospace" }}>
                      <option value="">All statuses</option>
                      <option value="free">Free</option>
                      <option value="allocated">Allocated</option>
                      <option value="partial">Partial</option>
                    </select>
                    <select value={invoiceAirportFilter} onChange={(e) => setInvoiceAirportFilter(e.target.value)}
                      style={{ background: "#0a1628", color: "#c8dff0", border: "1px solid #0d3060", borderRadius: 6, padding: "6px 10px", fontSize: 11, fontFamily: "'Space Mono', monospace" }}>
                      <option value="">All airports</option>
                      {uniqueAirports.map((a) => <option key={a} value={a}>{a}</option>)}
                    </select>
                    <select value={invoiceMonthFilter} onChange={(e) => setInvoiceMonthFilter(e.target.value)}
                      style={{ background: "#0a1628", color: "#c8dff0", border: "1px solid #0d3060", borderRadius: 6, padding: "6px 10px", fontSize: 11, fontFamily: "'Space Mono', monospace" }}>
                      <option value="">All months</option>
                      {uniqueMonths.map((m) => {
                        const [y, mo] = m.split("-");
                        return <option key={m} value={m}>{monthNames[Number(mo) - 1]} {y}</option>;
                      })}
                    </select>
                    <input type="text" placeholder="Search customer or invoice..." value={invoiceCustomerSearch} onChange={(e) => setInvoiceCustomerSearch(e.target.value)}
                      style={{ background: "#0a1628", color: "#c8dff0", border: "1px solid #0d3060", borderRadius: 6, padding: "6px 10px", fontSize: 11, fontFamily: "'Space Mono', monospace", width: 220 }} />
                    <button
                      className="btn"
                      onClick={() => exportInvoiceRowsCSV(filteredRows)}
                      style={{
                        background: "#0a1628", color: "#00bfff", padding: "5px 12px", borderRadius: 5,
                        fontFamily: "'Space Mono', monospace", fontSize: 10, letterSpacing: 1,
                        border: "1px solid #0d3060", cursor: "pointer",
                      }}>
                      EXPORT CSV
                    </button>
                    <span style={{ color: "#4a7fa0", marginLeft: "auto" }}>
                      Showing <span style={{ color: "#00bfff" }}>{filteredRows.length}</span> of <span style={{ color: "#00bfff" }}>{invoiceRows.length}</span> rows
                    </span>
                  </div>
                  <div style={{ overflowX: "auto" }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "'Space Mono', monospace", fontSize: 10 }}>
                      <thead>
                        <tr style={{ borderBottom: "2px solid #0d3060" }}>
                          {["STATUS", "LINKED CERT", "CSV ROW", "INVOICE", "CUSTOMER", "UPLIFT DATE", "IATA", "ICAO", "COUNTRY", "SUPPLIER", "UPLIFT M3", "SAF M3", "REMAINING SAF M3", "NOTE"].map(
                            (header) => (
                              <th key={header} style={{ padding: "8px 12px", textAlign: "left", color: "#00bfff", whiteSpace: "nowrap" }}>
                                {header}
                              </th>
                            )
                          )}
                        </tr>
                      </thead>
                      <tbody>
                        {filteredRows.map((row, index) => (
                          <tr key={row.id || index} className="invoice-row" style={{ borderBottom: "1px solid #0d2040", background: index % 2 === 0 ? "#060e1a" : "#030d1a", transition: "background 0.15s" }}>
                            <td style={{ padding: "7px 12px" }}>
                              <Badge status={row.allocation_status || (row.is_allocated ? "allocated" : "free")} />
                            </td>
                            <td style={{ padding: "7px 12px" }}>
                              {(row.linked_cert_numbers || []).length > 0 ? (
                                <button
                                  className="btn"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    const certId = row.linked_cert_ids?.[0];
                                    if (certId) {
                                      const idx = certs.findIndex((c) => c.id === certId);
                                      if (idx >= 0) { setSelected(idx); setTab("certs"); }
                                    }
                                  }}
                                  style={{ background: "none", color: "#00bfff", fontSize: 9, padding: 0, textDecoration: "underline", textUnderlineOffset: 2, cursor: "pointer", fontFamily: "'Space Mono', monospace", border: "none", maxWidth: 120, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", display: "block" }}
                                  title={(row.linked_cert_numbers || []).join("\n")}
                                >
                                  {row.linked_cert_numbers[0]?.slice(-15) || "—"}
                                </button>
                              ) : <span style={{ color: "#334", fontSize: 9 }}>—</span>}
                            </td>
                            <td style={{ padding: "7px 12px", color: "#4a9fd4" }}>{row.row_number}</td>
                            <td style={{ padding: "7px 12px", color: "#e0f0ff" }}>{row.invoice_no || "—"}</td>
                            <td style={{ padding: "7px 12px", color: "#c8dff0" }}>{row.customer || "—"}</td>
                            <td style={{ padding: "7px 12px", color: "#888" }}>{row.uplift_date || "—"}</td>
                            <td style={{ padding: "7px 12px", color: "#c8dff0" }}>{row.iata || "—"}</td>
                            <td style={{ padding: "7px 12px", color: "#c8dff0" }}>{row.icao || "—"}</td>
                            <td style={{ padding: "7px 12px", color: "#c8dff0" }}>{row.country || "—"}</td>
                            <td style={{ padding: "7px 12px", color: "#c8dff0" }}>{row.supplier || "—"}</td>
                            <td style={{ padding: "7px 12px", color: "#c8dff0" }}>{formatVolume(row.vol_m3)}</td>
                            <td style={{ padding: "7px 12px", color: "#00ff9d", fontWeight: 700 }}>{formatVolume(row.saf_vol_m3)}</td>
                            <td style={{ padding: "7px 12px", color: "#c8dff0" }}>{formatVolume(row.remaining_m3)}</td>
                            <td style={{ padding: "7px 12px", maxWidth: 260, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
                              color: (row.validation_note || "").startsWith("Supplier over-declaration") ? "#ff9933"
                                   : (row.validation_note || "").startsWith("No SAF certificate") ? "#ff6666"
                                   : (row.validation_note || "").startsWith("Partial coverage") ? "#ffbb00"
                                   : "#4a7fa0",
                              fontSize: 9 }} title={row.validation_note || ""}>
                              {row.validation_note ? row.validation_note : ""}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              );
            })()}
          </div>
        ) : null}

        {tab === "coverage" ? (
          <CoverageTable
            coverageData={coverageData}
            airportFilter={coverageAirportFilter}
            setAirportFilter={setCoverageAirportFilter}
            clientSearch={coverageClientSearch}
            setClientSearch={setCoverageClientSearch}
          />
        ) : null}

        {tab === "clientCerts" ? (
          <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
            {!clientCertificateGroups.length ? (
              <div style={{ textAlign: "center", color: "#4a7fa0", padding: 60 }}>
                <div style={{ fontSize: 36, marginBottom: 12 }}>🧾</div>
                <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 11 }}>
                  No approved allocation groups are available for client certificate generation.
                </div>
                <div style={{ marginTop: 10, fontSize: 11 }}>
                  Client certificates are generated only from approved SAF allocation links grouped by client, airport, and month.
                </div>
              </div>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
                {(() => {
                  const pendingCount = clientCertificateGroups.filter((g) => g.can_generate && !g.generated_file_path).length;
                  return pendingCount > 0 ? (
                    <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 4 }}>
                      <button
                        className="btn"
                        onClick={bulkGenerateClientCerts}
                        disabled={!!loading}
                        style={{
                          background: "linear-gradient(135deg,#0050aa,#00bfff)",
                          color: "#fff",
                          padding: "8px 18px",
                          borderRadius: 6,
                          fontFamily: "'Space Mono', monospace",
                          fontSize: 11,
                          letterSpacing: 1,
                          border: "none",
                        }}
                      >
                        GENERATE ALL ({pendingCount})
                      </button>
                    </div>
                  ) : null;
                })()}
                {clientCertificateGroups.map((group) => (
                  <div
                    key={group.group_key}
                    style={{
                      background: "#060e1a",
                      border: "1px solid #0d2040",
                      borderRadius: 10,
                      padding: 20,
                    }}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "flex-start" }}>
                      <div>
                        <div style={{ fontFamily: "'Space Mono', monospace", color: "#00bfff", fontSize: 11 }}>
                          {group.internal_reference}
                        </div>
                        <div style={{ fontSize: 15, fontWeight: 600, color: "#e0f0ff", marginTop: 2 }}>
                          {group.client_name || "Unknown client"} · {group.airport_code || "—"} · {formatClientCertificateMonth(group.month)}
                        </div>
                        <div style={{ color: "#4a7fa0", fontSize: 11, marginTop: 4 }}>
                          SAF Volume: {formatClientCertificateVolume(group.total_saf_volume_m3)} m3 · Approved links: {group.approved_link_count} · Matched rows: {group.matched_row_count}
                        </div>
                        <div style={{ color: "#4a7fa0", fontSize: 11, marginTop: 4 }}>
                          Source Certificates: {(group.source_certificate_refs || []).join(", ") || "—"}
                        </div>
                        <div style={{ color: "#4a7fa0", fontSize: 11, marginTop: 4 }}>
                          Issue Date: {formatClientCertificateDate(group.issue_date)}
                        </div>
                        {group.validation_errors?.length ? (
                          <div style={{ color: "#ff6666", fontSize: 11, marginTop: 8 }}>
                            {group.validation_errors.join(" | ")}
                          </div>
                        ) : null}
                      </div>
                      <div style={{ display: "flex", flexDirection: "column", gap: 8, alignItems: "flex-end" }}>
                        <Badge status={group.can_generate ? "approved" : "manual_only"} />
                        <button
                          className="btn"
                          onClick={() => generateClientCertificate(group)}
                          disabled={!group.can_generate || !!group.generated_file_path}
                          style={{
                            background: group.generated_file_path
                              ? "#001a0d"
                              : group.can_generate
                                ? "linear-gradient(135deg,#0050aa,#00bfff)"
                                : "#0a1628",
                            color: group.generated_file_path ? "#00ff9d" : group.can_generate ? "#fff" : "#4a7fa0",
                            padding: "7px 14px",
                            borderRadius: 6,
                            fontFamily: "'Space Mono', monospace",
                            fontSize: 11,
                            letterSpacing: 1,
                            border: group.generated_file_path
                              ? "1px solid #00ff9d44"
                              : group.can_generate
                                ? "none"
                                : "1px solid #0d3060",
                            opacity: group.can_generate || group.generated_file_path ? 1 : 0.7,
                          }}
                        >
                          {group.generated_file_path ? "GENERATED" : "GENERATE PDF"}
                        </button>
                        {group.generated_file_path ? (
                          <button
                            className="btn"
                            onClick={() => openClientCertificatePdf(group)}
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
                            OPEN GENERATED PDF
                          </button>
                        ) : null}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        ) : null}

        {tab === "log" ? (
          <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
            <div style={{ fontFamily: "'Space Mono', monospace", fontSize: 11 }}>
              <div style={{ color: "#4a7fa0", marginBottom: 14 }}>
                Debug and troubleshooting log for imports, classification, matching, and allocation warnings.
              </div>
              {!log.length ? (
                <div style={{ color: "#4a7fa0" }}>No troubleshooting events yet.</div>
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
