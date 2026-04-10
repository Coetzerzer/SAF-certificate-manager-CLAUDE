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
  "additionalInformation": "free-text from 'Additional Information' or similar notes field",
  "supplyPeriod": "for PoC: the full supply period, e.g. '01/01/2025 – 31/12/2025'",
  "dateDispatch": "for PoS: the actual dispatch date; leave empty for PoC",
  "physicalDeliveryAirport": "ICAO or name of the main physical delivery airport",
  "deliveryAirports": "all delivery airports listed (comma-separated ICAO/IATA codes)",
  "matchingMode": "monthly-pos | uplift-pos | poc",
  "coverageGranularity": "month | day | period",
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
- Extract "additionalInformation" from the document when present, especially "Additional Information (voluntary)" style fields.
- matchingMode rules:
  - Use "poc" for PoC documents.
  - Use "monthly-pos" for one-airport PoS documents that represent a monthly certified quantity, especially when the document or notes say things like "SAF Delivery February 2025".
  - Use "uplift-pos" only when the document clearly refers to a single shipment / uplift / delivery event rather than a monthly airport total.
- For one-airport PoS documents with no clear single-uplift evidence, prefer "monthly-pos".
- coverage rules:
  - For "monthly-pos", set "coverageGranularity" = "month", infer "coverageMonth" as YYYY-MM, and set "coverageStart"/"coverageEnd" to the first and last day of that month.
  - If the document says e.g. "SAF Delivery February 2025", use that month for coverage even if "dateDispatch" is the month-end date.
  - For "uplift-pos", set "coverageGranularity" = "day" and set "coverageStart" = "coverageEnd" = the actual uplift/dispatch date.
  - For "poc", set "coverageGranularity" = "period" and use the documented supply period where possible.
- "matchingEvidence" should be a short phrase like "additional-information-month", "single-airport-pos-default", "explicit-uplift-wording", or "poc-supply-period".
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
  - "monthlyVolumes": populate from the "JET A-1 Sales" table rows (this shows the fuel coverage breakdown by airport and month).
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
    const parsed = normalizeCommaDecimals(JSON.parse(clean));

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
