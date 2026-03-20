const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const OPENAI_MODEL = Deno.env.get("OPENAI_MODEL") || "gpt-5-mini";

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
- DATE RULE: never confuse "dateIssuance" with "dateDispatch".
- For PoS, "dateDispatch" must be the "Date of dispatch of the sustainable material" (or equivalent transport/dispatch wording). Do NOT copy the certificate issue/signature/issuance date into "dateDispatch".
- For PoC, "supplyPeriod" is the reconciliation date field. Leave "dateDispatch" empty unless the document truly provides a dispatch date for the sustainable material.
- "dateIssuance" is only the document issue/signature date and must never be used as uplift/dispatch date.
- contractNumber: capture ALL contract references (look for EXTRANET refs, LMT numbers, contract IDs).
- isComplexPoC: set to "true" if this is a PoC covering 3 or more airports with a breakdown table of volumes per month. Otherwise "false" or "".
- monthlyVolumes: if the document contains a table with volumes broken down by month AND airport, extract each row as an entry with "month" (YYYY-MM), "airport" (ICAO or IATA code), "quantity", "quantityUnit". Leave as [] if no such table exists.
- airportVolumes: if the document lists volumes per airport (but not per month), extract each row with "airport", "country", "quantity", "quantityUnit". Leave as [] if not present.
- If a field is not present, use empty string "" (or empty array [] for underlyingPoSList/monthlyVolumes/airportVolumes).
- NUMBERS: always use a dot "." as the decimal separator, never a comma. E.g. write 5.599 not 5,599 — even if the source document uses European comma-decimal formatting.
- Return ONLY the JSON, no markdown, no explanation.`;

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function extractOutputText(payload: any) {
  if (typeof payload?.output_text === "string" && payload.output_text.trim()) {
    return payload.output_text.trim();
  }

  const blocks = [];
  for (const item of payload?.output || []) {
    for (const content of item?.content || []) {
      if (typeof content?.text === "string" && content.text.trim()) {
        blocks.push(content.text);
      }
    }
  }

  return blocks.join("\n").trim();
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

  const apiKey = Deno.env.get("OPENAI_API_KEY");
  if (!apiKey) {
    return jsonResponse({ error: "OPENAI_API_KEY secret is not configured." }, 500);
  }

  try {
    const { base64, filename } = await req.json();

    if (!base64 || typeof base64 !== "string") {
      return jsonResponse({ error: "Missing base64 PDF payload." }, 400);
    }

    const openAiResponse = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: OPENAI_MODEL,
        reasoning: { effort: "medium" },
        max_output_tokens: 4096,
        input: [
          {
            role: "user",
            content: [
              {
                type: "input_file",
                filename: filename || "certificate.pdf",
                file_data: `data:application/pdf;base64,${base64}`,
              },
              {
                type: "input_text",
                text: EXTRACT_PROMPT,
              },
            ],
          },
        ],
      }),
    });

    const payload = await openAiResponse.json();

    if (!openAiResponse.ok) {
      return jsonResponse(
        {
          error: payload?.error?.message || "OpenAI extraction failed.",
          details: payload,
        },
        openAiResponse.status
      );
    }

    const outputText = extractOutputText(payload);
    if (!outputText) {
      return jsonResponse(
        {
          error: "OpenAI returned no extractable text output.",
          details: payload,
        },
        502
      );
    }

    const clean = outputText.replace(/```json|```/g, "").trim();
    const parsed = normalizeCommaDecimals(JSON.parse(clean));

    return jsonResponse({
      parsed,
      model: payload.model || OPENAI_MODEL,
      usage: payload.usage || null,
      response_id: payload.id || null,
    });
  } catch (error) {
    return jsonResponse({ error: error instanceof Error ? error.message : "Unexpected extraction error." }, 500);
  }
});
