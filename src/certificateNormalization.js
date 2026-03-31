import { DEFAULT_CERTIFICATE_CLASSIFICATION, deriveCertificateClassification, mergeAirportIdentityEntries } from "./certificateClassification.js";

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
  if (!date) return "";
  return date.toISOString().slice(0, 10);
}

function startOfMonthUTC(year, monthIndex) {
  return new Date(Date.UTC(year, monthIndex, 1));
}

function endOfMonthUTC(year, monthIndex) {
  return new Date(Date.UTC(year, monthIndex + 1, 0));
}

function normalizeMonthValue(value) {
  const text = String(value ?? "").trim();
  if (!text) return "";
  if (/^\d{4}-\d{2}$/.test(text)) return text;
  const parsed = parseDateValue(text);
  return parsed ? parsed.toISOString().slice(0, 7) : "";
}

function makeMonthBounds(value) {
  const month = normalizeMonthValue(value);
  if (!month) return { month: "", start: "", end: "" };
  const [year, monthNumber] = month.split("-").map(Number);
  if (!Number.isFinite(year) || !Number.isFinite(monthNumber)) return { month: "", start: "", end: "" };
  const start = startOfMonthUTC(year, monthNumber - 1);
  const end = endOfMonthUTC(year, monthNumber - 1);
  return {
    month,
    start: toISODate(start),
    end: toISODate(end),
  };
}

function formatAirportLabel(entry) {
  if (!entry) return "";
  const code = [entry.iata, entry.icao].filter(Boolean).join("/");
  const label = entry.label || entry.raw || code;
  return code && label && label !== code ? `${label} (${code})` : label || code;
}

function buildAirportDescriptor(entry, fallbackLabel = "") {
  if (!entry) {
    return {
      iata: "",
      icao: "",
      name: fallbackLabel || "",
      label: fallbackLabel || "",
    };
  }

  return {
    iata: entry.iata || "",
    icao: entry.icao || "",
    name: entry.label || entry.raw || fallbackLabel || entry.iata || entry.icao || "",
    label: formatAirportLabel(entry) || fallbackLabel || "",
  };
}

function collectAirports(data) {
  const airports = [];
  const push = (entry) => {
    if (!entry) return;
    airports.push({
      raw: entry.raw || "",
      iata: entry.iata || "",
      icao: entry.icao || "",
      label: entry.label || entry.raw || entry.iata || entry.icao || "",
    });
  };

  for (const entry of data?.canonicalAirports || []) push(entry);
  push(data?.physicalDeliveryAirportCanonical);
  for (const item of data?.airportVolumes || []) push(item?.airportCanonical);
  for (const item of data?.monthlyVolumes || []) push(item?.airportCanonical);

  return mergeAirportIdentityEntries(airports);
}

function pickClassification(certificate, data) {
  const classification =
    certificate?.document_family ||
    certificate?.matching_mode ||
    data?.document_family ||
    data?.matching_mode
      ? {
          document_family: certificate?.document_family ?? data?.document_family ?? DEFAULT_CERTIFICATE_CLASSIFICATION.document_family,
          matching_mode: certificate?.matching_mode ?? data?.matching_mode ?? data?.matchingMode ?? DEFAULT_CERTIFICATE_CLASSIFICATION.matching_mode,
          classification_confidence:
            parseFlexibleNumber(certificate?.classification_confidence ?? data?.classification_confidence) ??
            DEFAULT_CERTIFICATE_CLASSIFICATION.classification_confidence,
          review_required:
            typeof (certificate?.review_required ?? data?.review_required) === "boolean"
              ? (certificate?.review_required ?? data?.review_required)
              : DEFAULT_CERTIFICATE_CLASSIFICATION.review_required,
          classification_reason:
            certificate?.classification_reason ?? data?.classification_reason ?? DEFAULT_CERTIFICATE_CLASSIFICATION.classification_reason,
          supported_boolean:
            typeof (certificate?.supported_boolean ?? data?.supported_boolean) === "boolean"
              ? (certificate?.supported_boolean ?? data?.supported_boolean)
              : DEFAULT_CERTIFICATE_CLASSIFICATION.supported_boolean,
          processing_mode:
            certificate?.processing_mode ?? data?.processing_mode ?? DEFAULT_CERTIFICATE_CLASSIFICATION.processing_mode,
          support_reason:
            certificate?.support_reason ?? data?.support_reason ?? DEFAULT_CERTIFICATE_CLASSIFICATION.support_reason,
        }
      : deriveCertificateClassification(data, { filename: certificate?.filename });

  return classification;
}

function createBaseUnit({
  key,
  unitType,
  airport,
  airportIata,
  airportIcao,
  airportName,
  periodStart,
  periodEnd,
  dispatchDate,
  safVolume,
  jetVolume,
  quantityUnit,
  source,
  sourceReference,
  matchingModeOverride,
  reviewRequired,
  normalizationWarning,
  notes,
}) {
  return {
    key,
    unit_type: unitType || "",
    airport: airport || "",
    airport_iata: airportIata || "",
    airport_icao: airportIcao || "",
    airport_name: airportName || airport || "",
    period_start: toISODate(periodStart) || null,
    period_end: toISODate(periodEnd) || null,
    dispatch_date: toISODate(dispatchDate) || null,
    saf_volume: parseFlexibleNumber(safVolume),
    jet_volume: parseFlexibleNumber(jetVolume),
    quantity_unit: quantityUnit || "",
    source: source || "",
    source_reference: sourceReference || source || "",
    matching_mode_override: matchingModeOverride || "",
    review_required: Boolean(reviewRequired),
    normalization_warning: normalizationWarning || "",
    notes: notes || "",
  };
}

function buildSimpleMonthlyUnits(data, airports, warnings) {
  if (!airports || !airports.length) {
    warnings.push("No airports available for simple monthly normalization.");
    return [];
  }
  const airportMeta = buildAirportDescriptor(airports[0], "—");
  const monthBounds = makeMonthBounds(data?.coverageMonth || data?.coverageStart || data?.dateDispatch);
  if (!monthBounds.month) warnings.push("Simple monthly normalization could not confirm the covered month.");
  if (airports.length > 1) warnings.push("Simple monthly path expects one airport, but multiple airports were extracted.");

  return [
    createBaseUnit({
      key: "simple-monthly-airport",
      unitType: "monthly-airport",
      airport: airportMeta.label,
      airportIata: airportMeta.iata,
      airportIcao: airportMeta.icao,
      airportName: airportMeta.name,
      periodStart: data?.coverageStart || monthBounds.start,
      periodEnd: data?.coverageEnd || monthBounds.end,
      dispatchDate: data?.dateDispatch || "",
      safVolume: data?.quantity,
      jetVolume: data?.totalVolume,
      quantityUnit: data?.quantityUnit || "",
      source: "certificate aggregate",
      sourceReference: data?.uniqueNumber || data?.filename || "certificate",
      matchingModeOverride: "simple_monthly_airport",
      notes: monthBounds.month ? `Simple supported airport-month volume for ${monthBounds.month}.` : "Simple supported airport-month volume.",
    }),
  ];
}

function buildManualOnlyUnits(data, airports, warnings) {
  warnings.push("This certificate is outside the supported simple scope and will not be auto-matched.");
  return [
    createBaseUnit({
      key: "manual-only",
      unitType: "manual-only",
      airport: airports.map(formatAirportLabel).filter(Boolean).join(", ") || "—",
      airportName: airports.map((entry) => entry.label || entry.raw || entry.iata || entry.icao).filter(Boolean).join(", "),
      periodStart: data?.coverageStart || "",
      periodEnd: data?.coverageEnd || "",
      dispatchDate: data?.dateDispatch || "",
      safVolume: data?.quantity,
      jetVolume: data?.totalVolume,
      quantityUnit: data?.quantityUnit || "",
      source: "certificate aggregate",
      sourceReference: data?.uniqueNumber || data?.filename || "certificate",
      matchingModeOverride: "manual_only",
      normalizationWarning: warnings[warnings.length - 1] || "Manual-only certificate.",
      notes: "Manual-only aggregate representation.",
    }),
  ];
}

function buildUnitsForMode(data, classification, airports, warnings) {
  switch (classification.matching_mode) {
    case "simple_monthly_airport":
      return buildSimpleMonthlyUnits(data, airports, warnings);
    case "manual_only":
    default:
      return buildManualOnlyUnits(data, airports, warnings);
  }
}

function summarizeNormalizedValues(data, classification, airports, units) {
  return {
    document_family: classification.document_family,
    matching_mode: classification.matching_mode,
    interpreted_airports: airports.map(formatAirportLabel).filter(Boolean),
    interpreted_period_start: data?.coverageStart || units[0]?.period_start || "",
    interpreted_period_end: data?.coverageEnd || units[0]?.period_end || "",
    interpreted_dispatch_date: data?.dateDispatch || units[0]?.dispatch_date || "",
    interpreted_saf_volume: parseFlexibleNumber(data?.quantity),
    interpreted_jet_volume: parseFlexibleNumber(data?.totalVolume),
    quantity_unit: data?.quantityUnit || "",
    manual_review_required: Boolean(classification.review_required),
    normalized_unit_count: units.length,
  };
}

function buildTransformations(data, classification, airports, units, warnings) {
  const steps = [
    "Canonical airport aliases were consolidated into interpreted airport labels/codes.",
    "Coverage fields were normalized into an explicit business period start/end.",
    `Business family '${classification.document_family}' mapped to matching mode '${classification.matching_mode}'.`,
  ];

  if (classification.matching_mode === "simple_monthly_airport") {
    steps.push("A single airport-month allocation unit was generated for deterministic FIFO matching.");
  } else {
    steps.push("A manual-only aggregate allocation unit was generated because the certificate falls outside the supported scope.");
  }

  if ((data?.underlyingPoSList || []).length) {
    steps.push(`Underlying PoS references were retained for audit (${data.underlyingPoSList.length} row(s)).`);
  }

  if (!units.length) {
    warnings.push("No normalized allocation units were generated.");
  }

  return steps;
}

export function buildNormalizedCertificateView(certificate) {
  const data = certificate?.data || {};
  const classification = pickClassification(certificate, data);
  const airports = collectAirports(data);
  const warnings = [];
  const units = buildUnitsForMode(data, classification, airports, warnings);
  const transformations = buildTransformations(data, classification, airports, units, warnings);
  const normalizedValues = summarizeNormalizedValues(data, classification, airports, units);

  const rawValues = {
    certificate_number: data?.uniqueNumber || certificate?.filename || "",
    doc_type: data?.docType || "",
    delivery_airports: data?.deliveryAirports || "",
    physical_delivery_airport: data?.physicalDeliveryAirport || "",
    supply_period: data?.supplyPeriod || "",
    date_dispatch: data?.dateDispatch || "",
    quantity: data?.quantity || "",
    total_volume: data?.totalVolume || "",
    quantity_unit: data?.quantityUnit || "",
    monthly_volumes_count: Array.isArray(data?.monthlyVolumes) ? data.monthlyVolumes.length : 0,
    airport_volumes_count: Array.isArray(data?.airportVolumes) ? data.airportVolumes.length : 0,
    underlying_pos_count: Array.isArray(data?.underlyingPoSList) ? data.underlyingPoSList.length : 0,
  };

  return {
    certificate_number: data?.uniqueNumber || certificate?.filename || "Certificate",
    certificate_id: certificate?.id || "",
    document_family: classification.document_family,
    matching_mode: classification.matching_mode,
    classification_confidence: classification.classification_confidence,
    review_required: classification.review_required,
    classification_reason: classification.classification_reason,
    supported_boolean: Boolean(classification.supported_boolean),
    processing_mode: classification.processing_mode || "",
    support_reason: classification.support_reason || classification.classification_reason,
    interpreted_airports: airports.map(formatAirportLabel).filter(Boolean),
    interpreted_period_start: normalizedValues.interpreted_period_start,
    interpreted_period_end: normalizedValues.interpreted_period_end,
    interpreted_dispatch_date: normalizedValues.interpreted_dispatch_date,
    interpreted_saf_volume: normalizedValues.interpreted_saf_volume,
    interpreted_jet_volume: normalizedValues.interpreted_jet_volume,
    quantity_unit: data?.quantityUnit || "",
    feedstock: data?.rawMaterial || "",
    origin: data?.rawMaterialOrigin || "",
    ghg_total: data?.ghgTotal || "",
    ghg_saving: data?.ghgSaving || "",
    units,
    warnings: dedupeBy(warnings.filter(Boolean), (value) => normalizeText(value)),
    transformations,
    raw_values: rawValues,
    normalized_values: normalizedValues,
    underlying_pos: (data?.underlyingPoSList || []).filter((entry) =>
      [entry?.poSNumber, entry?.quantity, entry?.rawMaterial, entry?.origin].some((value) => String(value ?? "").trim())
    ),
  };
}

export function buildCertificateAllocationUnitDrafts(certificate) {
  const normalized = buildNormalizedCertificateView(certificate);
  return (normalized.units || []).map((unit, index) => ({
    certificate_id: certificate?.id || "",
    unit_index: index,
    unit_type: unit.unit_type || "",
    airport_iata: unit.airport_iata || "",
    airport_icao: unit.airport_icao || "",
    airport_name: unit.airport_name || unit.airport || "",
    period_start: unit.period_start || null,
    period_end: unit.period_end || null,
    dispatch_date: unit.dispatch_date || null,
    saf_volume_m3: parseFlexibleNumber(unit.saf_volume),
    jet_volume_m3: parseFlexibleNumber(unit.jet_volume),
    source_reference: unit.source_reference || unit.source || "",
    matching_mode_override: unit.matching_mode_override || normalized.matching_mode || "",
    review_required: Boolean(unit.review_required || normalized.review_required),
    normalization_warning: unit.normalization_warning || normalized.warnings?.join(" | ") || "",
    consumed_volume_m3: 0,
    remaining_volume_m3: parseFlexibleNumber(unit.saf_volume),
  }));
}
