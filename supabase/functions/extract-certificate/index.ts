const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const ANTHROPIC_MODEL = Deno.env.get("ANTHROPIC_MODEL") || "claude-sonnet-4-20250514";

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
  "additionalInformation": "VERBATIM copy of the 'Additional Information (voluntary)' field — even if it looks unimportant. This often contains scope hints like '4Q25 Delivery', 'MAD delivery 2025 (IBZ & MAH)', 'Annual', etc. NEVER summarize or omit",
  "supplyPeriod": "for PoC: the full supply period, e.g. '01/01/2025 – 31/12/2025'",
  "dateDispatch": "for PoS: the actual dispatch date; leave empty for PoC",
  "physicalDeliveryAirport": "ICAO or name of the main physical delivery airport",
  "deliveryAirports": "all delivery airports listed (comma-separated ICAO/IATA codes), INCLUDING any extra airports mentioned in 'Additional Information' parentheses like '(IBZ & MAH)' or '(LIN, MXP)'",
  "matchingMode": "monthly-pos | quarterly-pos | yearly-pos | period-pos | uplift-pos | poc",
  "coverageGranularity": "month | quarter | year | period | day",
  "coverageMonth": "YYYY-MM when the document covers one month",
  "coverageStart": "YYYY-MM-DD",
  "coverageEnd": "YYYY-MM-DD",
  "matchingEvidence": "short explanation of why matchingMode/coverage were chosen",
  "productType": "",
  "rawMaterial": "",
  "rawMaterialOrigin": "",
  "quantity": "the physical VOLUME or MASS of SAF (in m3, litres, MT, kg). NEVER put energy content (MJ) here. For PoS: the SAF volume. For PoC: the bio/SAF component volume (NOT the total fossil+bio volume)",
  "totalVolume": "for PoC only: total physical JET A-1 volume including fossil component",
  "quantityUnit": "m3, litres, MT, etc.",
  "energyContent": "the energy value in MJ (or GJ/kWh). This is separate from the physical volume — do NOT confuse with quantity",
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
- DATE RULE: never confuse "dateIssuance" with "dateDispatch".
- For PoS, "dateDispatch" must be the "Date of dispatch of the sustainable material" (or equivalent transport/dispatch wording). Do NOT copy the certificate issue/signature/issuance date into "dateDispatch".
- For PoC, "supplyPeriod" is the reconciliation date field. Leave "dateDispatch" empty unless the document truly provides a dispatch date for the sustainable material.
- "dateIssuance" is only the document issue/signature date and must never be used as uplift/dispatch date.
- ALWAYS extract "additionalInformation" VERBATIM from the document — including text in the "Additional Information (voluntary)" field even if short or seemingly trivial. Examples to preserve exactly: "4Q25 Delivery", "3Q25 Delivery", "MAD delivery 2025 (IBZ & MAH)", "Annual 2025". This field often contains the TRUE scope (quarter / year / multi-airport) that overrides the dispatch date.
- ALWAYS extract "contractNumber" VERBATIM. Examples: "ALC. SAF Delivery April 2025" (monthly), "VLC. HEFA Delivery April-December 2025" (period), "MJV. HEFA Delivery 2025" (annual). The wording in this field is critical for scope detection.
- matchingMode rules — INSPECT 'additionalInformation' AND 'contractNumber' BEFORE deciding:
  - Use "poc" for PoC documents.
  - Use "quarterly-pos" if 'additionalInformation' or 'contractNumber' contains a quarter marker like "1Q25", "2Q25", "3Q25", "4Q25", "Q1 2025", "Q4 2025", "1st Quarter 2025", etc. (one airport, one quarter).
  - Use "yearly-pos" if 'contractNumber' or 'additionalInformation' says "Delivery YYYY", "Annual YYYY", or any year-only scope without a month.
  - Use "period-pos" if the document explicitly mentions a multi-month range like "April-December 2025", "Apr-Dec 2025", or "from MM/YYYY to MM/YYYY".
  - Use "monthly-pos" for one-airport PoS documents that represent a monthly certified quantity (e.g. "SAF Delivery February 2025").
  - Use "uplift-pos" only when the document clearly refers to a single shipment / uplift / delivery event rather than a monthly airport total.
  - For one-airport PoS documents with no clear single-uplift evidence and no quarter/year/period markers, prefer "monthly-pos".
- coverage rules:
  - For "monthly-pos", set "coverageGranularity" = "month", infer "coverageMonth" as YYYY-MM, and set "coverageStart"/"coverageEnd" to the first and last day of that month.
  - For "quarterly-pos", set "coverageGranularity" = "quarter" and set "coverageStart" / "coverageEnd" to the FIRST and LAST day of the quarter (e.g. for 4Q25: coverageStart="2025-10-01", coverageEnd="2025-12-31"). Set "coverageMonth" to the LAST month of the quarter (YYYY-MM).
  - For "yearly-pos", set "coverageGranularity" = "year", "coverageStart" = "YYYY-01-01", "coverageEnd" = "YYYY-12-31", and "coverageMonth" = "YYYY-12" (last month).
  - For "period-pos", set "coverageGranularity" = "period" and the actual start/end dates from the period mentioned. Set "coverageMonth" to the LAST month of the period.
  - If the document says e.g. "SAF Delivery February 2025", use that month for coverage even if "dateDispatch" is the month-end date.
  - For "uplift-pos", set "coverageGranularity" = "day" and set "coverageStart" = "coverageEnd" = the actual uplift/dispatch date.
  - For "poc", set "coverageGranularity" = "period" and use the documented supply period where possible.
- MULTI-AIRPORT in 'additionalInformation': if the field contains parentheses with extra airport codes like "(IBZ & MAH)", "(LIN, MXP)", "(BCN, AGP)", these airports MUST be added to "deliveryAirports" (comma-separated) IN ADDITION to the primary delivery airport. The certificate covers ALL listed airports.
  - Example: contractNumber "MAD. SAF Delivery December 2025" + additionalInformation "MAD delivery 2025 (IBZ & MAH)" → deliveryAirports = "IBZ, MAH" (the parenthetical airports are the actual scope; MAD is just the logistics hub) AND coverageGranularity = "year".
- "matchingEvidence" should be a short phrase like "additional-information-quarter", "additional-information-year", "contract-number-period", "additional-information-multi-airport", "single-airport-pos-default", "explicit-uplift-wording", or "poc-supply-period".
- contractNumber: capture ALL contract references (look for EXTRANET refs, LMT numbers, contract IDs).
- isComplexPoC: set to "true" if this is a PoC covering 3 or more airports with a breakdown table of volumes per month. Otherwise "false" or "".
- monthlyVolumes: if the document contains a table with volumes broken down by month AND airport, extract each row as an entry with "month" (YYYY-MM), "airport" (ICAO or IATA code), "quantity", "quantityUnit". Leave as [] if no such table exists.
- airportVolumes: if the document lists volumes per airport (but not per month), extract each row with "airport", "country", "quantity", "quantityUnit". Leave as [] if not present.
- If a field is not present, use empty string "" (or empty array [] for underlyingPoSList/monthlyVolumes/airportVolumes).
- NUMBERS: always use a dot "." as the decimal separator, never a comma. E.g. write 5.599 not 5,599 — even if the source document uses European comma-decimal formatting.
- QUANTITY vs ENERGY — THIS IS CRITICAL: The PDF typically has a "Quantity" row (with m3/15°C, litres, MT, kg) and a separate "Energy content" row (with MJ). You MUST map the VOLUME from the "Quantity" row into the "quantity" JSON field, and the MJ value from the "Energy content" row into the "energyContent" JSON field. Do NOT swap them. Self-check: if your "quantity" value is in MJ or your "energyContent" is in m3, you have them backwards — fix it. For SAF, energy content in MJ is typically much larger than volume in m3 (roughly 33,000 MJ per m3).
- CRITICAL NUMBER RULE: In European documents, a comma is ALWAYS a decimal separator, NEVER a thousands separator. European documents use spaces or dots for thousands grouping. So "2,080" means 2.080 (about two), "43,115" means 43.115 (about forty-three), "2 328,388" means 2328.388. Never interpret a comma as a thousands separator in these documents. When in doubt, check if the numeric magnitude makes sense in context (SAF volumes are typically 0.1–500 m3, not thousands).
- TOTALENERGIES PoC FORMAT: Some PoC documents (especially from TotalEnergies) contain TWO separate tables:
  (a) "JET A-1 Sales" — airports where fuel was sold to the customer. The SAF certificate covers ALL these airports.
  (b) "SAF Delivery site" — where SAF was physically blended and in which month(s).
  When both tables exist:
  - "deliveryAirports": list ALL airports from BOTH tables (all are covered by the SAF).
  - "physicalDeliveryAirport": use the airport from "SAF Delivery site".
  - "quantity": use BioQuantity (total SAF volume from the SAF Delivery site total).
  - "totalVolume": use Volumes M3 (total JET A-1 from JET A-1 Sales total).
  - CRITICAL for coverageMonth: use the month(s) with non-zero volume in the "SAF Delivery site" table, NOT the full supply period. If SAF was delivered in multiple months, set coverageMonth to the month with the LARGEST volume. Put the full supply period in "supplyPeriod" only.
  - "coverageStart"/"coverageEnd": set to the first/last day of the SAF delivery month (from coverageMonth), NOT the full supply period.
  - "monthlyVolumes": For the "JET A-1 Sales" table, extract EVERY non-zero cell as a separate entry. Each row in the table has 12 month columns (Jan–Dec). For each airport AND each month that has a non-zero value, create one entry: { "month": "YYYY-MM", "airport": "IATA code", "quantity": "value from that cell", "quantityUnit": "m3" }. Example: if LIN has Jan=24.720 and Feb=39.107, create TWO entries: [{ "month":"2025-01", "airport":"LIN", "quantity":"24.720", "quantityUnit":"m3" }, { "month":"2025-02", "airport":"LIN", "quantity":"39.107", "quantityUnit":"m3" }]. IMPORTANT: Include ALL airport rows, even those displayed BELOW the TOTAL row in the table (some PoC documents place airports after the total line). Skip rows labeled "TOTAL". The volumes in this table are total JET A-1 volumes (not SAF) — extract them as-is, the system will normalize to SAF proportionally.
  - "isComplexPoC": "true" if JET A-1 Sales has 3+ airports.
- Return ONLY the JSON, no markdown, no explanation.`;

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function extractOutputText(payload: any) {
  for (const block of payload?.content || []) {
    if (block?.type === "text" && typeof block?.text === "string" && block.text.trim()) {
      return block.text.trim();
    }
  }
  return "";
}

// Static legacy IATA mapping (mirror of public.iata_legacy_mapping).
// Update both this table and the SQL table when adding new entries.
const LEGACY_IATA_MAPPING: Record<string, { iata: string; icao?: string; label?: string }> = {
  MJV: { iata: "RMU", icao: "LEMI", label: "Murcia (ex-San Javier)" },
};

function quarterBounds(year: number, quarter: number): { start: string; end: string; lastMonth: string } {
  const startMonth = (quarter - 1) * 3 + 1;
  const endMonth = startMonth + 2;
  const start = `${year}-${String(startMonth).padStart(2, "0")}-01`;
  // Last day: build first of next month then subtract a day, or pin Q1=Mar31, Q2=Jun30, Q3=Sep30, Q4=Dec31
  const lastDays: Record<number, string> = { 3: "31", 6: "30", 9: "30", 12: "31" };
  const end = `${year}-${String(endMonth).padStart(2, "0")}-${lastDays[endMonth]}`;
  const lastMonth = `${year}-${String(endMonth).padStart(2, "0")}`;
  return { start, end, lastMonth };
}

const MONTH_NAMES: Record<string, number> = {
  jan: 1, january: 1, janvier: 1, enero: 1, gennaio: 1,
  feb: 2, february: 2, fevrier: 2, février: 2, febrero: 2, febbraio: 2,
  mar: 3, march: 3, mars: 3, marzo: 3,
  apr: 4, april: 4, avril: 4, abril: 4, aprile: 4,
  may: 5, mai: 5, mayo: 5, maggio: 5,
  jun: 6, june: 6, juin: 6, junio: 6, giugno: 6,
  jul: 7, july: 7, juillet: 7, julio: 7, luglio: 7,
  aug: 8, august: 8, août: 8, aout: 8, agosto: 8,
  sep: 9, sept: 9, september: 9, septembre: 9, septiembre: 9, settembre: 9,
  oct: 10, october: 10, octobre: 10, octubre: 10, ottobre: 10,
  nov: 11, november: 11, novembre: 11, noviembre: 11,
  dec: 12, december: 12, décembre: 12, decembre: 12, diciembre: 12, dicembre: 12,
};

function monthEndDay(year: number, month: number): string {
  const d = new Date(Date.UTC(year, month, 0));
  return String(d.getUTCDate()).padStart(2, "0");
}

/**
 * Detect quarter / year / period scope hints in 'additionalInformation' and
 * 'contractNumber'. Override coverageStart / coverageEnd / coverageGranularity
 * if a stronger scope is found.
 *
 * Returns an updated parsed object.
 */
function detectScopeOverrides(parsed: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = { ...parsed };
  const addl = String(parsed.additionalInformation ?? "").trim();
  const contract = String(parsed.contractNumber ?? "").trim();
  const blob = `${addl} | ${contract}`;
  const dispatchStr = String(parsed.dateDispatch ?? "");
  // Try to infer year from dispatch date (DD/MM/YYYY) or coverageMonth (YYYY-MM)
  let year: number | null = null;
  const dispatchMatch = dispatchStr.match(/(\d{2})[\/\-](\d{2})[\/\-](\d{4})/);
  if (dispatchMatch) year = parseInt(dispatchMatch[3], 10);
  if (!year) {
    const cm = String(parsed.coverageMonth ?? "").match(/^(\d{4})-\d{2}$/);
    if (cm) year = parseInt(cm[1], 10);
  }
  if (!year) {
    const yearInBlob = blob.match(/\b(20\d{2})\b/);
    if (yearInBlob) year = parseInt(yearInBlob[1], 10);
  }
  if (!year) return out;

  // Pattern 1: "1Q25", "2Q25", "3Q25", "4Q25" or "Q1 2025" / "Q4 2025" / "1st quarter 2025"
  const quarterMatch = blob.match(/\b([1-4])Q(\d{2})\b/i)
    || blob.match(/\bQ([1-4])\s*(20\d{2})\b/i)
    || blob.match(/\b([1-4])(?:st|nd|rd|th)?\s*Quarter\s*(20\d{2})\b/i);
  if (quarterMatch) {
    const q = parseInt(quarterMatch[1], 10);
    const qYear = quarterMatch[2].length === 2 ? 2000 + parseInt(quarterMatch[2], 10) : parseInt(quarterMatch[2], 10);
    const { start, end, lastMonth } = quarterBounds(qYear, q);
    out.coverageStart = start;
    out.coverageEnd = end;
    out.coverageMonth = lastMonth;
    out.coverageGranularity = "quarter";
    out.matchingMode = "quarterly-pos";
    out.matchingEvidence = "additional-information-quarter";
    out._scopeOverrideSource = `quarter ${q}Q${qYear}`;
  }

  // Pattern 2: "Month-Month YYYY" (period range, e.g. "April-December 2025")
  if (!quarterMatch) {
    // Match "April-December 2025", "April – December 2025", "April to December 2025"
    const periodMatch = blob.match(/\b([A-Za-z]+)\s*(?:[-–—]\s*|\s+to\s+)\s*([A-Za-z]+)\s+(20\d{2})\b/i);
    if (periodMatch) {
      const m1 = MONTH_NAMES[periodMatch[1].toLowerCase()];
      const m2 = MONTH_NAMES[periodMatch[2].toLowerCase()];
      const periodYear = parseInt(periodMatch[3], 10);
      if (m1 && m2 && m2 >= m1) {
        out.coverageStart = `${periodYear}-${String(m1).padStart(2, "0")}-01`;
        out.coverageEnd = `${periodYear}-${String(m2).padStart(2, "0")}-${monthEndDay(periodYear, m2)}`;
        out.coverageMonth = `${periodYear}-${String(m2).padStart(2, "0")}`;
        out.coverageGranularity = "period";
        out.matchingMode = "period-pos";
        out.matchingEvidence = "contract-number-period";
        out._scopeOverrideSource = `period ${periodMatch[1]}-${periodMatch[2]} ${periodYear}`;
      }
    }
  }

  // Pattern 3: Annual — contract number says "Delivery YYYY" without a month (after period detection)
  if (out.coverageGranularity !== "quarter" && out.coverageGranularity !== "period") {
    // Detect "Delivery 2025" or "Annual 2025" without month name. Reject if a single month name appears.
    const hasMonthName = Object.keys(MONTH_NAMES).some((m) => new RegExp(`\\b${m}\\b`, "i").test(blob));
    const annualMatch = blob.match(/\b(?:Delivery|Annual|Annuel|Year|Anno)\s*(20\d{2})\b/i);
    if (annualMatch && !hasMonthName) {
      const yr = parseInt(annualMatch[1], 10);
      out.coverageStart = `${yr}-01-01`;
      out.coverageEnd = `${yr}-12-31`;
      out.coverageMonth = `${yr}-12`;
      out.coverageGranularity = "year";
      out.matchingMode = "yearly-pos";
      out.matchingEvidence = "contract-number-year";
      out._scopeOverrideSource = `annual ${yr}`;
    }
  }

  // Pattern 4: Multi-airport in parentheses, e.g. "(IBZ & MAH)" or "(LIN, MXP)"
  // The parenthetical airports are the TRUE delivery scope when the issuance airport is just a logistics hub.
  const multiMatch = addl.match(/\(([A-Z]{3}(?:\s*[&,\/]\s*[A-Z]{3})+)\)/);
  if (multiMatch) {
    const airports = multiMatch[1].split(/[&,\/]/).map((a) => a.trim().toUpperCase()).filter((a) => /^[A-Z]{3}$/.test(a));
    if (airports.length >= 2) {
      // If 'Annual' was also detected above, treat the parenthetical airports as the FULL scope.
      // Replace deliveryAirports + canonicalAirports.
      out.deliveryAirports = airports.join(", ");
      out.canonicalAirports = airports.map((a) => ({ raw: a, iata: a }));
      out._scopeOverrideSource = `${out._scopeOverrideSource ?? ""} multi-airport(${airports.join(",")})`.trim();
    }
  }

  return out;
}

/**
 * Apply legacy IATA → current IATA remapping on canonicalAirports and physicalDeliveryAirportCanonical.
 * Idempotent.
 */
function applyLegacyIataRemap(parsed: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = { ...parsed };
  const remapOne = (entry: any) => {
    if (!entry || typeof entry !== "object") return entry;
    const orig = String(entry.iata ?? "").toUpperCase();
    const mapping = LEGACY_IATA_MAPPING[orig];
    if (!mapping) return entry;
    return {
      ...entry,
      raw: entry.raw ?? orig,
      iata: mapping.iata,
      icao: mapping.icao ?? entry.icao ?? "",
      label: mapping.label ?? entry.label ?? "",
      _remappedFrom: orig,
    };
  };

  if (Array.isArray(out.canonicalAirports)) {
    out.canonicalAirports = out.canonicalAirports.map(remapOne);
  }
  if (out.physicalDeliveryAirportCanonical && typeof out.physicalDeliveryAirportCanonical === "object") {
    out.physicalDeliveryAirportCanonical = remapOne(out.physicalDeliveryAirportCanonical);
  }
  // Also rewrite deliveryAirports text if it contains a legacy code.
  if (typeof out.deliveryAirports === "string" && out.deliveryAirports) {
    const remapped = out.deliveryAirports
      .split(/[,;\/]/)
      .map((p: string) => {
        const trimmed = p.trim().toUpperCase();
        return LEGACY_IATA_MAPPING[trimmed]?.iata ?? p.trim();
      })
      .join(", ");
    out.deliveryAirports = remapped;
  }
  return out;
}

function normalizeCommaDecimals(parsed: Record<string, unknown>) {
  const numericFields = [
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

  const fix = (value: unknown) => {
    if (typeof value !== "string") return value;
    return value.replace(/^(-?\d+),(\d{1,3})(%?)$/, (_match, integer, decimal, suffix) => `${integer}.${decimal}${suffix}`);
  };

  const out: Record<string, unknown> = { ...parsed };
  for (const field of numericFields) {
    if (field in out) out[field] = fix(out[field]);
  }

  if (Array.isArray(out.underlyingPoSList)) {
    out.underlyingPoSList = out.underlyingPoSList.map((batch: any) => ({
      ...batch,
      ghgTotal: fix(batch?.ghgTotal ?? ""),
      ghgSaving: fix(batch?.ghgSaving ?? ""),
      quantity: fix(batch?.quantity ?? ""),
    }));
  }

  return out;
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  if (req.method !== "POST") {
    return jsonResponse({ error: "Method not allowed" }, 405);
  }

  const apiKey = Deno.env.get("ANTHROPIC_API_KEY");
  if (!apiKey) {
    return jsonResponse({ error: "ANTHROPIC_API_KEY secret is not configured." }, 500);
  }

  try {
    const { base64, filename } = await req.json();

    if (!base64 || typeof base64 !== "string") {
      return jsonResponse({ error: "Missing base64 PDF payload." }, 400);
    }

    const anthropicResponse = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: ANTHROPIC_MODEL,
        max_tokens: 16384,
        temperature: 0,
        messages: [
          {
            role: "user",
            content: [
              {
                type: "document",
                source: {
                  type: "base64",
                  media_type: "application/pdf",
                  data: base64,
                },
              },
              {
                type: "text",
                text: EXTRACT_PROMPT,
              },
            ],
          },
        ],
      }),
    });

    const payload = await anthropicResponse.json();

    if (!anthropicResponse.ok) {
      return jsonResponse(
        {
          error: payload?.error?.message || "Anthropic extraction failed.",
          details: payload,
        },
        anthropicResponse.status
      );
    }

    const outputText = extractOutputText(payload);
    if (!outputText) {
      return jsonResponse(
        {
          error: "Anthropic returned no extractable text output.",
          details: payload,
        },
        502
      );
    }

    const clean = outputText.replace(/```json|```/g, "").trim();
    let parsed = normalizeCommaDecimals(JSON.parse(clean));

    // Detect quarter / year / period scope hints in additionalInformation + contractNumber.
    // Defensive: a bad regex / input should NOT crash the whole extraction.
    try {
      parsed = detectScopeOverrides(parsed);
    } catch (e) {
      parsed._scopeOverrideError = e instanceof Error ? e.message : String(e);
    }

    // Remap legacy IATA codes (e.g. MJV → RMU). Same defensive wrap.
    try {
      parsed = applyLegacyIataRemap(parsed);
    } catch (e) {
      parsed._legacyRemapError = e instanceof Error ? e.message : String(e);
    }

    // Post-extraction: detect swapped quantity / energyContent.
    // SAF energy density is ~33 MJ/litre = ~33,000 MJ/m3.
    // If energyContent looks like a small volume and quantity looks like energy, swap them.
    const qtyNum = parseFloat(String(parsed.quantity));
    const ecNum = parseFloat(String(parsed.energyContent));
    const qtyUnit = String(parsed.quantityUnit || "").toLowerCase();
    if (
      Number.isFinite(qtyNum) &&
      Number.isFinite(ecNum) &&
      ecNum > 0 &&
      qtyNum > 0 &&
      (qtyUnit.includes("m3") || qtyUnit.includes("litre") || qtyUnit.includes("mt") || qtyUnit.includes("kg")) &&
      ecNum < qtyNum &&
      ecNum < 5 &&
      qtyNum > 5
    ) {
      // Very likely swapped: energyContent is tiny (looks like volume) and quantity is large (looks like MJ)
      const tmp = parsed.quantity;
      parsed.quantity = parsed.energyContent;
      parsed.energyContent = tmp;
      parsed._swapCorrected = "quantity and energyContent were swapped by the model and auto-corrected";
    }

    // Validate critical fields exist
    const missingFields: string[] = [];
    if (!parsed.docType) missingFields.push("docType");
    if (!parsed.uniqueNumber) missingFields.push("uniqueNumber");
    if (parsed.quantity === undefined || parsed.quantity === null || parsed.quantity === "") missingFields.push("quantity");
    if (missingFields.length) {
      parsed._extractionWarnings = `Missing critical fields: ${missingFields.join(", ")}`;
    }

    return jsonResponse({
      parsed,
      model: payload.model || ANTHROPIC_MODEL,
      usage: payload.usage || null,
      response_id: payload.id || null,
    });
  } catch (error) {
    return jsonResponse({ error: error instanceof Error ? error.message : "Unexpected extraction error." }, 500);
  }
});
