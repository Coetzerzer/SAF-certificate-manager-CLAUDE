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

const DISPATCH_MONTH_WHITELIST = [
  {
    key: "tita_fao_poc_filename",
    docTypePattern: /\bpoc\b/i,
    filenamePattern: /^TITA_FAO\d{6}_\d+\.pdf$/i,
    airportCodes: new Set(["FAO", "LPFR"]),
  },
];

export const DEFAULT_CERTIFICATE_CLASSIFICATION = Object.freeze({
  document_family: "manual_only",
  matching_mode: "manual_only",
  classification_confidence: 0,
  review_required: false,
  classification_reason: "Certificate has not been classified yet.",
  supported_boolean: false,
  processing_mode: "manual_only",
  support_reason: "Certificate has not been classified yet.",
});

/**
 * @typedef {Object} CertificateClassification
 * @property {string} document_family
 * @property {string} matching_mode
 * @property {number} classification_confidence
 * @property {boolean} review_required
 * @property {string} classification_reason
 * @property {string} coverageGranularity
 * @property {string} coverageMonth
 * @property {string} coverageStart
 * @property {string} coverageEnd
 * @property {string} coverageSource
 * @property {string} matchingEvidence
 * @property {boolean} supported_boolean
 * @property {string} processing_mode
 * @property {string} support_reason
 */

function normalizeText(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function dedupeBy(items, getKey) {
  const seen = new Set();
  const out = [];
  for (const item of items || []) {
    const key = getKey(item);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

function startOfMonthUTC(year, monthIndex) {
  return new Date(Date.UTC(year, monthIndex, 1));
}

function endOfMonthUTC(year, monthIndex) {
  return new Date(Date.UTC(year, monthIndex + 1, 0));
}

function formatYearMonth(date) {
  return date.toISOString().slice(0, 7);
}

function parseDateValue(value) {
  const str = String(value ?? "").trim();
  if (!str) return null;

  const dmy = str.match(/^(\d{1,2})[/. -](\d{1,2})[/. -](\d{2,4})$/);
  if (dmy) {
    const year = Number(dmy[3].length === 2 ? `20${dmy[3]}` : dmy[3]);
    const date = new Date(Date.UTC(year, Number(dmy[2]) - 1, Number(dmy[1])));
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
  if (!date) return "";
  return date.toISOString().slice(0, 10);
}

function normalizeMonthValue(value) {
  const text = String(value ?? "").trim();
  if (!text) return "";
  if (/^\d{4}-\d{2}$/.test(text)) return text;
  const parsed = parseDateValue(text);
  return parsed ? parsed.toISOString().slice(0, 7) : "";
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
  return Number.isFinite(num) ? Number(num.toFixed(6)) : null;
}

function getCertKind(data) {
  const docType = String(data?.docType || "").toLowerCase();
  if (/\bpoc\b/.test(docType) || docType.includes("proof of compliance")) return "poc";
  if (/\bpos\b/.test(docType) || docType.includes("proof of sustainability")) return "pos";
  return "unknown";
}

function isNonM3Unit(unitStr) {
  if (!unitStr) return false;
  const u = String(unitStr).toLowerCase().replace(/[^a-z0-9/°³]/g, "");
  if (/^m3|^m³|^m3\/15|^m³\/15/.test(u)) return false;
  if (/litr|^l$|^lt$|gallon|^gal$|barrel|^bbl$|^mt$|tonne|^kg$|^ton$/.test(u)) return true;
  return false;
}

function normalizeAirportCode(value) {
  return String(value ?? "").trim().toUpperCase();
}

function isValidAirportCode(code, type) {
  if (!code) return false;
  if (type === "iata") return /^[A-Z]{3}$/.test(code);
  if (type === "icao") return /^[A-Z]{4}$/.test(code);
  return false;
}

function normalizeAirportEntry(entry) {
  if (!entry) return null;
  const rawIata = normalizeAirportCode(entry.iata);
  const rawIcao = normalizeAirportCode(entry.icao);
  const iata = isValidAirportCode(rawIata, "iata") ? rawIata : "";
  const icao = isValidAirportCode(rawIcao, "icao") ? rawIcao : "";
  const raw = String(entry.raw || "").trim();
  const label = String(entry.label || entry.raw || rawIata || rawIcao || "").trim();
  if (!iata && !icao && !raw && !label) return null;
  return { raw, label, iata, icao };
}

function airportEntriesMatch(left, right) {
  if (!left || !right) return false;
  if (left.iata && right.iata && left.iata === right.iata) return true;
  if (left.icao && right.icao && left.icao === right.icao) return true;
  const leftLabel = normalizeText(left.label || left.raw);
  const rightLabel = normalizeText(right.label || right.raw);
  if (leftLabel && rightLabel && leftLabel === rightLabel) return true;
  return false;
}

export function getAirportIdentityKey(entry) {
  const normalized = normalizeAirportEntry(entry);
  if (!normalized) return "";
  if (normalized.iata && normalized.icao) return `${normalized.iata}|${normalized.icao}`;
  if (normalized.iata) return `IATA:${normalized.iata}`;
  if (normalized.icao) return `ICAO:${normalized.icao}`;
  return `LABEL:${normalizeText(normalized.label || normalized.raw)}`;
}

export function mergeAirportIdentityEntries(entries) {
  const merged = [];

  for (const sourceEntry of entries || []) {
    const entry = normalizeAirportEntry(sourceEntry);
    if (!entry) continue;

    const matchingIndexes = [];
    for (let index = 0; index < merged.length; index += 1) {
      if (airportEntriesMatch(merged[index], entry)) matchingIndexes.push(index);
    }

    if (!matchingIndexes.length) {
      merged.push({ ...entry });
      continue;
    }

    const primary = merged[matchingIndexes[0]];
    primary.raw = primary.raw || entry.raw;
    primary.label = primary.label || entry.label;
    primary.iata = primary.iata || entry.iata;
    primary.icao = primary.icao || entry.icao;

    for (let idx = matchingIndexes.length - 1; idx >= 1; idx -= 1) {
      const duplicate = merged[matchingIndexes[idx]];
      primary.raw = primary.raw || duplicate.raw;
      primary.label = primary.label || duplicate.label;
      primary.iata = primary.iata || duplicate.iata;
      primary.icao = primary.icao || duplicate.icao;
      merged.splice(matchingIndexes[idx], 1);
    }
  }

  return dedupeBy(merged, (entry) => getAirportIdentityKey(entry));
}

function collectCanonicalAirportEntries(data) {
  const entries = [];
  const pushEntry = (entry) => {
    const normalized = normalizeAirportEntry(entry);
    if (normalized) entries.push(normalized);
  };

  for (const entry of data?.canonicalAirports || []) {
    pushEntry(entry);
  }
  pushEntry(data?.physicalDeliveryAirportCanonical);
  for (const item of data?.airportVolumes || []) {
    pushEntry(item?.airportCanonical);
  }
  for (const item of data?.monthlyVolumes || []) {
    pushEntry(item?.airportCanonical);
  }

  return mergeAirportIdentityEntries(entries);
}

function getCanonicalAirportCodes(data) {
  return collectCanonicalAirportEntries(data).map((entry) => entry.iata || entry.icao || entry.label || entry.raw);
}

function dispatchMonthWhitelisted(data, context = {}) {
  const filename = String(context?.filename || data?.filename || "").trim();
  const docType = String(data?.docType || "").trim();
  const airportCodes = new Set(
    collectCanonicalAirportEntries(data)
      .flatMap((entry) => [entry.iata, entry.icao])
      .filter(Boolean)
  );

  return DISPATCH_MONTH_WHITELIST.some((rule) => {
    if (rule.docTypePattern && !rule.docTypePattern.test(docType)) return false;
    if (rule.filenamePattern && !rule.filenamePattern.test(filename)) return false;
    if (rule.airportCodes && rule.airportCodes.size) {
      const hasWhitelistedAirport = [...rule.airportCodes].some((code) => airportCodes.has(code));
      if (!hasWhitelistedAirport) return false;
    }
    return true;
  });
}

function countUnderlyingPoS(data) {
  return (data?.underlyingPoSList || []).filter((entry) =>
    [entry?.poSNumber, entry?.quantity, entry?.rawMaterial, entry?.origin].some((value) => String(value ?? "").trim())
  ).length;
}

function getCertificateAdditionalInfoTexts(data) {
  const preferredKeys = ["additionalInformation", "additionalInfo", "remarks", "notes", "note", "comment", "comments"];
  return preferredKeys
    .map((key) => (typeof data?.[key] === "string" ? data[key].trim() : ""))
    .filter(Boolean);
}

function buildMonthCoverage(month, source, evidence) {
  if (!month) return null;
  const [year, monthNumber] = month.split("-").map(Number);
  if (!Number.isFinite(year) || !Number.isFinite(monthNumber)) return null;
  const start = startOfMonthUTC(year, monthNumber - 1);
  const end = endOfMonthUTC(year, monthNumber - 1);
  return {
    coverageGranularity: "month",
    coverageMonth: month,
    coverageStart: toISODate(start),
    coverageEnd: toISODate(end),
    coverageSource: source,
    matchingEvidence: evidence,
  };
}

function inferMonthFromText(text) {
  const match =
    String(text ?? "").match(/\b(?:saf\s+)?delivery\s+([a-z]+)\s+(\d{4})\b/i) ||
    String(text ?? "").match(/\b([a-z]+)\s+(\d{4})\b/i);
  if (!match) return "";
  const monthIndex = MONTH_NAME_TO_INDEX.get(normalizeText(match[1]));
  const year = Number(match[2]);
  if (!Number.isInteger(monthIndex) || !Number.isFinite(year)) return "";
  return formatYearMonth(startOfMonthUTC(year, monthIndex));
}

const QUARTER_MONTH_OFFSETS = { 1: [0, 2], 2: [3, 5], 3: [6, 8], 4: [9, 11] };

function inferQuarterFromText(text) {
  const str = String(text ?? "").trim();
  if (!str) return null;

  const digitFirst = str.match(/\b([1-4])Q\s*[''\u2019]?(\d{2,4})\b/i);
  const qFirst = str.match(/\bQ([1-4])\s*[-/]?\s*[''\u2019]?(\d{2,4})\b/i);
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
    quarter,
    year,
    coverageStart: toISODate(start),
    coverageEnd: toISODate(end),
    coverageMonth: formatYearMonth(start),
    quarterLabel: `Q${quarter} ${year}`,
    raw: match[0],
  };
}

function parseRangeFromValue(value) {
  const matches =
    String(value ?? "")
      .match(/\d{4}-\d{2}-\d{2}|\d{1,2}[/. -]\d{1,2}[/. -]\d{2,4}/g)
      ?.map((entry) => entry.trim())
      .filter(Boolean) || [];
  if (matches.length < 2) return null;
  const start = parseDateValue(matches[0]);
  const end = parseDateValue(matches[1]);
  if (!start || !end) return null;
  return { start, end };
}

function deriveSimpleMonthCoverage(data, context = {}) {
  const monthCandidates = [];
  const addCandidate = (month, source, evidence) => {
    if (!month) return;
    monthCandidates.push({ month, source, evidence });
  };

  addCandidate(normalizeMonthValue(data?.coverageMonth), "extracted-coverage", "coverage-month");

  const explicitStart = parseDateValue(data?.coverageStart);
  const explicitEnd = parseDateValue(data?.coverageEnd);
  if (explicitStart && explicitEnd) {
    if (formatYearMonth(explicitStart) === formatYearMonth(explicitEnd)) {
      addCandidate(formatYearMonth(explicitStart), "extracted-range", "coverage-range");
    } else {
      return {
        coverageGranularity: "period",
        coverageMonth: "",
        coverageStart: toISODate(explicitStart),
        coverageEnd: toISODate(explicitEnd),
        coverageSource: "extracted-range",
        matchingEvidence: "multi-month-range",
        isAmbiguous: true,
        ambiguityReason: "certificate covers more than one month",
      };
    }
  }

  // Only fall back to supplyPeriod if no coverage candidates found yet.
  // This prevents an annual supply period (e.g. Jan-Dec) from overriding
  // a specific coverageMonth already extracted (e.g. from SAF Delivery site).
  if (monthCandidates.length === 0) {
    const supplyPeriod = parseRangeFromValue(data?.supplyPeriod);
    if (supplyPeriod) {
      if (formatYearMonth(supplyPeriod.start) === formatYearMonth(supplyPeriod.end)) {
        addCandidate(formatYearMonth(supplyPeriod.start), "supply-period", "single-month-supply-period");
      } else {
        return {
          coverageGranularity: "period",
          coverageMonth: "",
          coverageStart: toISODate(supplyPeriod.start),
          coverageEnd: toISODate(supplyPeriod.end),
          coverageSource: "supply-period",
          matchingEvidence: "multi-month-supply-period",
          isAmbiguous: true,
          ambiguityReason: "supply period covers more than one month",
        };
      }
    }
  }

  for (const text of getCertificateAdditionalInfoTexts(data)) {
    const quarterResult = inferQuarterFromText(text);
    if (quarterResult) {
      return {
        coverageGranularity: "quarter",
        coverageMonth: quarterResult.coverageMonth,
        coverageStart: quarterResult.coverageStart,
        coverageEnd: quarterResult.coverageEnd,
        coverageSource: "additional-information",
        matchingEvidence: "additional-information-quarter",
        isAmbiguous: false,
        ambiguityReason: "",
        isQuarterly: true,
        quarterLabel: quarterResult.quarterLabel,
      };
    }
    addCandidate(inferMonthFromText(text), "additional-information", "additional-information-month");
  }

  const dispatchDate = parseDateValue(data?.dateDispatch);
  if (dispatchDate && dispatchMonthWhitelisted(data, context)) {
    addCandidate(formatYearMonth(dispatchDate), "dispatch-date", "dispatch-month");
  }

  const distinctMonths = dedupeBy(monthCandidates.filter((item) => item.month), (item) => item.month);
  if (!distinctMonths.length) {
    return {
      coverageGranularity: "",
      coverageMonth: "",
      coverageStart: "",
      coverageEnd: "",
      coverageSource: "",
      matchingEvidence: "",
      isAmbiguous: true,
      ambiguityReason: "missing clear monthly period",
    };
  }

  if (distinctMonths.length > 1) {
    return {
      coverageGranularity: "",
      coverageMonth: "",
      coverageStart: "",
      coverageEnd: "",
      coverageSource: distinctMonths.map((item) => `${item.source}:${item.month}`).join(", "),
      matchingEvidence: "conflicting-month-signals",
      isAmbiguous: true,
      ambiguityReason: "multiple conflicting month signals were found",
    };
  }

  const selected = distinctMonths[0];
  return {
    ...buildMonthCoverage(selected.month, selected.source, selected.evidence),
    isAmbiguous: false,
    ambiguityReason: "",
  };
}

function buildClassification({
  documentFamily,
  matchingMode,
  supportedBoolean,
  processingMode,
  supportReason,
  coverage,
}) {
  return {
    document_family: documentFamily,
    matching_mode: matchingMode,
    classification_confidence: 0.99,
    review_required: false,
    classification_reason: supportReason,
    coverageGranularity: coverage.coverageGranularity || "",
    coverageMonth: coverage.coverageMonth || "",
    coverageStart: coverage.coverageStart || "",
    coverageEnd: coverage.coverageEnd || "",
    coverageSource: coverage.coverageSource || "",
    matchingEvidence: coverage.matchingEvidence || "",
    supported_boolean: supportedBoolean,
    processing_mode: processingMode,
    support_reason: supportReason,
  };
}

function buildSupportedClassification(coverage, reasonParts) {
  return buildClassification({
    documentFamily: "supported_simple",
    matchingMode: "simple_monthly_airport",
    supportedBoolean: true,
    processingMode: "supported_simple",
    supportReason: reasonParts.join("; "),
    coverage,
  });
}

function buildManualOnlyClassification(coverage, reasonParts) {
  return buildClassification({
    documentFamily: "manual_only",
    matchingMode: "manual_only",
    supportedBoolean: false,
    processingMode: "manual_only",
    supportReason: reasonParts.filter(Boolean).join("; "),
    coverage,
  });
}

export function deriveCertificateClassification(data, context = {}) {
  const certKind = getCertKind(data);
  const airportCodes = getCanonicalAirportCodes(data);
  const airportCount = airportCodes.length;
  const coverage = deriveSimpleMonthCoverage(data, context);
  const quantity = parseFlexibleNumber(data?.quantity);
  const monthlyVolumeAirports = new Set(
    (data?.monthlyVolumes || []).map((item) => getAirportIdentityKey(item?.airportCanonical)).filter(Boolean)
  );
  const airportVolumeAirports = new Set(
    (data?.airportVolumes || []).map((item) => getAirportIdentityKey(item?.airportCanonical)).filter(Boolean)
  );
  const hasComplexMonthlyTable = monthlyVolumeAirports.size > 1;
  const hasComplexAirportTable = airportVolumeAirports.size > 1;
  const underlyingPoSCount = countUnderlyingPoS(data);
  const additionalTexts = getCertificateAdditionalInfoTexts(data).join(" ");
  const annualTextHint = /\b(annual|calendar year|full year|yearly)\b/i.test(additionalTexts);

  const rejectionReasons = [];

  if (certKind === "unknown") rejectionReasons.push("document type is unclear");
  if (airportCount !== 1) rejectionReasons.push(airportCount ? "certificate covers multiple airports" : "certificate airport is unclear");
  if (coverage.isAmbiguous) rejectionReasons.push(coverage.ambiguityReason);
  if (!Number.isFinite(quantity) || quantity <= 0) rejectionReasons.push("certificate SAF volume is unclear");
  else if (quantity > 50) rejectionReasons.push(`SAF volume ${quantity} m³ exceeds plausibility threshold — likely a number format error`);
  if (isNonM3Unit(data?.quantityUnit)) rejectionReasons.push(`quantity unit "${data.quantityUnit}" is not m³ — manual conversion required`);
  if (hasComplexMonthlyTable || hasComplexAirportTable) rejectionReasons.push("complex tables are present");
  // Multiple underlying PoS is normal for PoC certificates — only reject for PoS documents
  if (underlyingPoSCount > 1 && certKind !== "poc") rejectionReasons.push("multiple underlying certificates are present");
  if (annualTextHint) rejectionReasons.push("annual coverage was detected");

  if (rejectionReasons.length) {
    return buildManualOnlyClassification(coverage, [
      certKind === "unknown" ? "manual only" : `${certKind.toUpperCase()} certificate`,
      ...rejectionReasons,
    ]);
  }

  return buildSupportedClassification(coverage, [
    `${certKind.toUpperCase()} certificate`,
    `1 airport (${airportCodes[0]})`,
    coverage.isQuarterly ? `1 quarter (${coverage.quarterLabel})` : `1 month (${coverage.coverageMonth})`,
    `1 SAF volume (${quantity} m3)`,
    "no complex tables",
    underlyingPoSCount === 1 ? "1 underlying certificate" : "no multiple underlying certificates",
  ]);
}
