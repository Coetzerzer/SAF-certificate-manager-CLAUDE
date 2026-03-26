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
    aliases: ["Aeropuerto de Girona", "Girona Airport", "Girona"],
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
    aliases: ["Madrid", "Madrid Airport", "Adolfo Suarez Madrid Barajas", "Barajas"],
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
    aliases: ["Santander", "Santander Airport", "Aeropuerto de Santander"],
  },
  {
    iata: "SVQ",
    icao: "LEZL",
    label: "Seville",
    aliases: ["Seville", "Seville Airport", "Sevilla", "Aeropuerto de Sevilla"],
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

function inferMonthlyCoverage(data) {
  const textSources = [
    ...getCertificateAdditionalInfoTexts(data),
    typeof data?.filename === "string" ? data.filename : "",
  ].filter(Boolean);

  for (const text of textSources) {
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

function hydrateInvoiceRowsWithAllocations(rows, links) {
  const totalsByRowId = new Map();
  for (const link of links || []) {
    const existing = totalsByRowId.get(link.invoice_row_id) || 0;
    totalsByRowId.set(link.invoice_row_id, Number((existing + Number(link.allocated_m3 || 0)).toFixed(6)));
  }

  return (rows || []).map((row) => {
    const allocated = totalsByRowId.get(row.id) || 0;
    const summary = summarizeInvoiceRowState(row, allocated);
    return {
      ...row,
      allocated_m3_total: summary.allocatedTotal,
      remaining_m3: summary.remainingVolume,
      is_allocated: summary.isAllocated,
      allocation_status: summary.status,
      validation_note: summary.validationNote,
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
      validation_note: summary.validationNote,
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

async function extractCertificateFromBase64(base64, filename) {
  const { data, error } = await supabase.functions.invoke(EXTRACTION_FUNCTION, {
    body: { base64, filename },
  });

  if (error) {
    throw new Error(error.message || "Edge function invocation failed.");
  }
  if (!data?.parsed) {
    throw new Error(data?.error || "Extraction function returned no parsed certificate.");
  }

  return {
    parsed: normalizeCommaDecimals(data.parsed),
    model: data.model || "gpt-5-mini",
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

function Badge({ status }) {
  const label = String(status || "unknown");
  const map = {
    supported_simple: ["#00ff9d", "#001a0d"],
    manual_only: ["#ffbb00", "#1a1200"],
    simple_monthly_airport: ["#4a9fd4", "#061423"],
    auto_linked: ["#00ff9d", "#001a0d"],
    approved: ["#00ff9d", "#001a0d"],
    needs_review: ["#ffbb00", "#1a1200"],
    unmatched: ["#ff6666", "#1a0000"],
    rejected: ["#ff4444", "#1a0000"],
    allocated: ["#00ff9d", "#001a0d"],
    partial: ["#ffbb00", "#1a1200"],
    free: ["#4a9fd4", "#061423"],
    duplicate: ["#ffbb00", "#1a1200"],
    invalid: ["#ff6666", "#1a0000"],
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
            {cert.support_reason || cert.data?.support_reason || "Not processed yet"}
          </div>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 6, alignItems: "flex-end" }}>
          <Badge status={cert.document_family || cert.data?.document_family || "unknown"} />
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
            RUN
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
  const [clientCertificateRecords, setClientCertificateRecords] = useState([]);
  const [loading, setLoading] = useState("");
  const [tab, setTab] = useState("certs");
  const [log, setLog] = useState([]);
  const [expandedCandidates, setExpandedCandidates] = useState({});
  const [pdfPreview, setPdfPreview] = useState(null);
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

      const [importRes, fallbackImportRes, legacyImportRes, matchRes, linkRes, unitRes, clientCertRes] = await Promise.all([
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
        supabase.from("certificate_invoice_links").select("*"),
        supabase.from("certificate_allocation_units").select("*").order("unit_index", { ascending: true }),
        supabase.from("client_certificates").select("*").order("created_at", { ascending: false }),
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
            if (downloadErr) throw downloadErr;
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
          } catch (error) {
            if (isCurrentRequest()) addLog(`Invoice row backfill warning: ${error.message}`, "error");
          }
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

      const hydratedInvoiceRows = hydrateInvoiceRowsWithAllocations(invoiceRowsData, linkRes.data || []);

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

      setInvoiceImport(nextInvoiceImport);
      setInvoiceRows(hydratedInvoiceRows);
      setCerts(hydrated);
      setClientCertificateRecords(clientCertRes.data || []);
      addLog(`Loaded ${hydrated.length} certificate(s) and ${hydratedInvoiceRows.length} invoice row(s)`, "success");
    } catch (error) {
      if (isCurrentRequest()) addLog(`DB reload failed: ${error.message}`, "error");
    }
  }, [addLog, backfillInvoiceRowValidationState, backfillMissingAllocationUnits]);

  useEffect(() => {
    if (initialLoadStartedRef.current) return;
    initialLoadStartedRef.current = true;
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
              saveRes = await supabase
                .from("certificates")
                .update(extendedCertificatePayload)
                .eq("unique_number", uniqueNumber)
                .select("id")
                .single();
              if (saveRes.error && isMissingColumnError(saveRes.error)) {
                saveRes = await supabase
                  .from("certificates")
                  .update(baseCertificatePayload)
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
          if (saveRes.error) throw saveRes.error;
          await syncCertificateAllocationUnits({
            id: saveRes.data?.id,
            filename: file.name,
            data: parsed,
            ...classification,
          });
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
    [addLog, loadFromDB, syncCertificateAllocationUnits]
  );

  const handleCSVUpload = useCallback(
    async (file) => {
      if (!file) return;
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
            year: 2025,
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

        for (let index = 0; index < parsed.importRows.length; index += 500) {
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
          if (rowsErr) throw rowsErr;
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
        setLoading("");
        await loadFromDB();
        setTab("db");
      } catch (error) {
        if (stagedImportId && !importActivated) {
          const fallbackSummary = {
            filename: file.name,
          };
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
        }
        setLoading("");
        addLog(`Invoice import failed: ${error.message}`, "error");
      }
    },
    [addLog, loadFromDB]
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
          if (prev?.url) URL.revokeObjectURL(prev.url);
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
      if (isSupportedSimpleCert(cert)) {
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
        const result = await runDatabaseSimpleAllocation(cert);
        addLog(
          `${certTitle(cert)} → ${result.status} · ${formatDiagnosticsSummary(result.diagnostics)}`,
          result.status === "unmatched" ? "error" : result.status === "manual_only" ? "info" : "success"
        );
        setLoading("");
        await loadFromDB();
        setTab("certs");
      } catch (error) {
        setLoading("");
        addLog(`Matching failed for ${certTitle(cert)}: ${error.message}`, "error");
      }
    },
    [addLog, allocableInvoiceRowCount, certs, invoiceRows.length, loadFromDB, loadedInvoiceImportId, runDatabaseSimpleAllocation]
  );

  const analyzeAll = useCallback(async () => {
    if (!certs.length) return;
    if (certs.some((cert) => isSupportedSimpleCert(cert))) {
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

    try {
      for (let index = 0; index < certs.length; index += 1) {
        const cert = certs[index];
        setLoading(`Matching ${index + 1}/${certs.length}: ${certTitle(cert)}`);
        const result = await runDatabaseSimpleAllocation(cert);
        addLog(
          `${certTitle(cert)} → ${result.status} · ${formatDiagnosticsSummary(result.diagnostics)}`,
          result.status === "unmatched" ? "error" : result.status === "manual_only" ? "info" : "success"
        );
      }

      setLoading("");
      await loadFromDB();
      setTab("certs");
    } catch (error) {
      setLoading("");
      addLog(`Full matching run failed: ${error.message}`, "error");
    }
  }, [addLog, allocableInvoiceRowCount, certs, invoiceRows.length, loadFromDB, loadedInvoiceImportId, runDatabaseSimpleAllocation]);

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
  const selectedSupportReason =
    selectedCert?.support_reason ||
    selectedCert?.data?.support_reason ||
    selectedCert?.classification_reason ||
    selectedCert?.data?.classification_reason ||
    "—";
  const stats = {
    totalCerts: certs.length,
    supported: certs.filter((cert) => cert.document_family === "supported_simple" || cert.data?.document_family === "supported_simple").length,
    manualOnly: certs.filter((cert) => cert.document_family === "manual_only" || cert.data?.document_family === "manual_only").length,
    matched: certs.filter((cert) => ["auto_linked", "approved"].includes(cert.match?.status)).length,
    unmatched: certs.filter((cert) => cert.match?.status === "unmatched").length,
    allocatedRows: invoiceRows.filter((row) => row.allocation_status === "allocated" || row.allocation_status === "partial").length,
  };
  const clientCertificateRecordsByKey = new Map((clientCertificateRecords || []).map((row) => [row.group_key, row]));
  const clientCertificateGroups = collectApprovedClientCertificateGroups(certs, invoiceRows).map((group) => {
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
          { label: "SUPPORTED", val: stats.supported, color: "#00ff9d" },
          { label: "MANUAL ONLY", val: stats.manualOnly, color: "#ffbb00" },
          { label: "MATCHED", val: stats.matched, color: "#00ff9d" },
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
          ["clientCerts", "CLIENT CERTIFICATES"],
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
                      <FieldRow label="MONTH" value={selectedInterpretedMonth} highlight />
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
                    </div>

                    <div style={{ gridColumn: "1 / -1" }}>
                      <MatchRowsTable rows={selectedCert.match?.links || []} title="LINKED INVOICE ROWS" />
                    </div>

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
                      {["STATUS", "CSV ROW", "INVOICE", "CUSTOMER", "UPLIFT DATE", "IATA", "ICAO", "COUNTRY", "SUPPLIER", "UPLIFT M3", "SAF M3", "REMAINING SAF M3"].map(
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
                          <Badge status={row.allocation_status || (row.is_allocated ? "allocated" : "free")} />
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
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
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
                          disabled={!group.can_generate}
                          style={{
                            background: group.can_generate ? "linear-gradient(135deg,#0050aa,#00bfff)" : "#0a1628",
                            color: group.can_generate ? "#fff" : "#4a7fa0",
                            padding: "7px 14px",
                            borderRadius: 6,
                            fontFamily: "'Space Mono', monospace",
                            fontSize: 11,
                            letterSpacing: 1,
                            border: group.can_generate ? "none" : "1px solid #0d3060",
                            opacity: group.can_generate ? 1 : 0.7,
                          }}
                        >
                          GENERATE PDF
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
