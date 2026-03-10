import React, { useState, useEffect, useCallback, useRef } from "react";
import { supabase } from "./src/supabase.js";

// ─── Helpers ──────────────────────────────────────────────────────────────────

const ANTHROPIC_MODEL = "claude-sonnet-4-20250514";
const ANTHROPIC_HEADERS = {
  "Content-Type": "application/json",
  "x-api-key": import.meta.env.VITE_ANTHROPIC_KEY,
  "anthropic-version": "2023-06-01",
  "anthropic-dangerous-direct-browser-access": "true",
};

async function callClaude(messages, systemPrompt = "", maxTokens = 2048) {
  const body = {
    model: ANTHROPIC_MODEL,
    max_tokens: maxTokens,
    messages,
  };
  if (systemPrompt) body.system = systemPrompt;
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: ANTHROPIC_HEADERS,
    body: JSON.stringify(body),
  });
  const data = await res.json();
  return data.content?.map((b) => b.text || "").join("") || "";
}

function fileToBase64(file) {
  return new Promise((res, rej) => {
    const r = new FileReader();
    r.onload = () => res(r.result.split(",")[1]);
    r.onerror = () => rej(new Error("Read failed"));
    r.readAsDataURL(file);
  });
}

function blobToBase64(blob) {
  return new Promise((res, rej) => {
    const r = new FileReader();
    r.onload = () => res(r.result.split(",")[1]);
    r.onerror = () => rej(new Error("Read failed"));
    r.readAsDataURL(blob);
  });
}

function csvToRows(text) {
  const lines = text.trim().split("\n");
  if (!lines.length) return [];
  const headers = lines[0].split(",").map((h) => h.trim().replace(/^"|"$/g, ""));
  return lines.slice(1).map((line) => {
    const vals = line.split(",").map((v) => v.trim().replace(/^"|"$/g, ""));
    const obj = {};
    headers.forEach((h, i) => (obj[h] = vals[i] ?? ""));
    return obj;
  });
}

// Normalize European comma-decimals (e.g. "4,2" → "4.2", "95,5%" → "95.5%")
// Only converts values that look like numbers with a comma decimal (not thousands separators).
const NUMERIC_FIELDS = ["quantity","totalVolume","ghgTotal","ghgSaving","ghgEec","ghgEl","ghgEp","ghgEtd","ghgEu","ghgEsca","ghgEccs","ghgEccr","energyContent","lcv","density"];
function normalizeCommaDecimals(parsed) {
  const fix = (v) => {
    if (typeof v !== "string") return v;
    // Match: optional digits, a comma, exactly 1-3 digits, optional % — but NOT a thousands separator (which would have 3 digits after comma followed by more digits)
    return v.replace(/^(-?\d+),(\d{1,3})(%?)$/, (_, int, dec, pct) => `${int}.${dec}${pct}`);
  };
  const out = { ...parsed };
  for (const f of NUMERIC_FIELDS) { if (out[f]) out[f] = fix(out[f]); }
  if (Array.isArray(out.underlyingPoSList)) {
    out.underlyingPoSList = out.underlyingPoSList.map(b => ({
      ...b,
      ghgTotal: fix(b.ghgTotal ?? ""),
      ghgSaving: fix(b.ghgSaving ?? ""),
      quantity: fix(b.quantity ?? ""),
    }));
  }
  return out;
}

// SAF fields to extract from PDF
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

const COMPARE_PROMPT = (cert, invoices) => `You are a SAF compliance analyst at Titan Aviation Fuels SARL.

INVOICE SCHEMA: Each invoice row has these columns: IATA (airport IATA code), ICAO (airport ICAO code), COUNTRY, CUSTOMER (buyer name). There may also be volume, date, or reference columns.

Here is an extracted SAF certificate:
${JSON.stringify(cert, null, 2)}

Here are our internal invoice records:
${JSON.stringify(invoices.slice(0, 300), null, 2)}

MATCHING LOGIC:
1. docType matters: PoC covers a supply PERIOD at multiple airports; PoS is a single shipment.
2. Primary match: contractNumber — search invoice rows for any of the contract references in cert.contractNumber.
3. Secondary match: airport — cert.deliveryAirports contains ICAO/IATA codes; match against IATA and ICAO columns in invoices.
4. For PoC: the supply period (cert.supplyPeriod) may span a full year — date tolerance should be the entire period.
5. Quantity comparison: SUM all matched invoice volumes across the entire supply period before comparing against cert.quantity. cert.quantity is the total SAF bio-component for the whole period — never compare against a single invoice row. Express the aggregated sum in the same unit as cert.quantityUnit.
6. GHG saving threshold: EU RED II requires ≥ 65% saving for new installations. Flag if cert.ghgSaving < 65%.
7. supplierVerified: cross-check cert.safSupplier (actual SAF producer) against known suppliers; cert.issuer may be the distributor, not the producer.

Analyze and compare. Return ONLY a valid JSON:
{
  "matchFound": true/false,
  "matchedInvoice": "contract ref, IATA code, or null",
  "matchConfidence": "High/Medium/Low/None",
  "matchReasons": ["reason 1", ...],
  "discrepancies": ["discrepancy 1", ...],
  "warnings": ["warning 1", ...],
  "complianceStatus": "Compliant/Non-Compliant/Needs Review",
  "complianceNotes": "brief summary covering docType, supplier chain, GHG, airports",
  "ghgSavingOk": true/false,
  "quantityMatch": true/false,
  "supplierVerified": true/false,
  "airportsVerified": true/false,
  "contractRefFound": true/false,
  "recommendedAction": "Accept / Reject / Review Manually",
  "matchedInvoiceRows": ["row 3: LMT21482_1576 · BGY · 2025-03-15 · 145 m³", "..."]
}`;

const ATTRIBUTION_PROMPT = (cert, filteredInvoices) => `You are a SAF compliance analyst at Titan Aviation Fuels SARL.

This is a complex PoC certificate covering multiple airports. Your task is to attribute the certified SAF volumes to specific customers based on our internal invoice records.

CERTIFICATE DATA:
${JSON.stringify({ uniqueNumber: cert.uniqueNumber, contractNumber: cert.contractNumber, supplyPeriod: cert.supplyPeriod, deliveryAirports: cert.deliveryAirports, monthlyVolumes: cert.monthlyVolumes, airportVolumes: cert.airportVolumes, quantity: cert.quantity, quantityUnit: cert.quantityUnit }, null, 2)}

INVOICE RECORDS (pre-filtered for relevant airports and period):
${JSON.stringify(filteredInvoices, null, 2)}

ATTRIBUTION LOGIC:
1. Match invoice rows to certificate slots (airport + month combinations from monthlyVolumes).
2. Group invoice rows by CUSTOMER + IATA/ICAO airport code + month.
3. For each certificate slot (airport + month), find matching invoice rows and sum their volumes. List the specific invoice rows used (cite _rowIdx and key fields: contract ref, IATA, date, volume) in the invoiceRefs array.
4. A slot is "matched" if invoice volume is within 10% of cert volume for that slot.
5. If monthlyVolumes is empty but airportVolumes exists, attribute by airport only (no month breakdown).
6. certVolume and invoicedVolume should be numeric values with units from the certificate/invoices.

Return ONLY a valid JSON object:
{
  "clientAttribution": [
    {
      "customer": "customer name from invoice",
      "airport": "IATA or ICAO code",
      "month": "YYYY-MM or null if no monthly breakdown",
      "certVolume": "volume from cert for this slot",
      "invoicedVolume": "sum of invoice volumes for this customer/airport/month",
      "matched": true/false,
      "confidence": "High/Medium/Low",
      "invoiceRefs": ["row 3: LMT21482_1576 · BGY · 2025-03-15 · 145 m³", "..."]
    }
  ],
  "unattributedSlots": [
    {
      "airport": "IATA or ICAO code",
      "month": "YYYY-MM or null",
      "certVolume": "cert volume for this slot",
      "reason": "why no match was found"
    }
  ],
  "totalAttributedVolume": "sum of all attributed volumes with unit",
  "totalUnattributedVolume": "sum of unattributed volumes with unit",
  "attributionStatus": "Complete/Partial/None",
  "attributionNotes": "brief summary of attribution quality and any issues"
}`;

function filterInvoicesForCert(cert, invoices) {
  // Build set of airport codes from the certificate
  const airportCodes = new Set();
  if (cert.deliveryAirports) {
    cert.deliveryAirports.split(/[,\s]+/).forEach(c => { if (c.trim()) airportCodes.add(c.trim().toUpperCase()); });
  }
  (cert.airportVolumes || []).forEach(av => { if (av.airport) airportCodes.add(av.airport.toUpperCase()); });
  (cert.monthlyVolumes || []).forEach(mv => { if (mv.airport) airportCodes.add(mv.airport.toUpperCase()); });

  // Parse supplyPeriod e.g. "01/01/2025 – 31/12/2025"
  let periodStart = null, periodEnd = null;
  if (cert.supplyPeriod) {
    const parts = cert.supplyPeriod.split(/[\u2013\u2014\-–]+/).map(s => s.trim());
    if (parts.length >= 2) {
      const parseDate = (s) => {
        const [d, m, y] = s.split("/");
        return y && m && d ? new Date(`${y}-${m}-${d}`) : null;
      };
      periodStart = parseDate(parts[0]);
      periodEnd = parseDate(parts[1]);
    }
  }

  return invoices.filter(row => {
    // Airport match
    const rowIata = (row.IATA || row.iata || "").toUpperCase();
    const rowIcao = (row.ICAO || row.icao || "").toUpperCase();
    const airportMatch = airportCodes.size === 0 || airportCodes.has(rowIata) || airportCodes.has(rowIcao);
    if (!airportMatch) return false;

    // Date match — if no period defined or date unparseable, include the row
    if (!periodStart || !periodEnd) return true;
    const rawDate = row.Date || row.date || row.DATE || row["Invoice Date"] || "";
    if (!rawDate) return true;
    let rowDate = null;
    // Try DD/MM/YYYY first, then ISO
    const dmy = rawDate.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
    if (dmy) rowDate = new Date(`${dmy[3]}-${dmy[2]}-${dmy[1]}`);
    else { rowDate = new Date(rawDate); }
    if (isNaN(rowDate?.getTime())) return true; // unparseable → include
    return rowDate >= periodStart && rowDate <= periodEnd;
  }).map((row, idx) => ({ ...row, _rowIdx: idx + 1 }));
}

function certIsComplex(certData) {
  if (!certData) return false;
  // Explicit field set by extraction
  if (certData.isComplexPoC === "true" || certData.isComplexPoC === true) return true;
  // Heuristic: PoC + 3+ airports + has monthly or airport volume breakdown
  const isPoC = (certData.docType || "").toLowerCase().includes("poc") ||
                (certData.docType || "").toLowerCase().includes("proof of compliance");
  if (!isPoC) return false;
  const airports = (certData.deliveryAirports || "").split(/[,\s]+/).filter(Boolean);
  const hasMultipleAirports = airports.length >= 3;
  return hasMultipleAirports; // volume breakdown may not be extracted, airport count is sufficient
}

// ─── Sub-components ───────────────────────────────────────────────────────────

function Badge({ status }) {
  const map = {
    Compliant: ["#00ff9d", "#001a0d"],
    "Non-Compliant": ["#ff4444", "#1a0000"],
    "Needs Review": ["#ffbb00", "#1a1200"],
    Accept: ["#00ff9d", "#001a0d"],
    Reject: ["#ff4444", "#1a0000"],
    "Review Manually": ["#ffbb00", "#1a1200"],
    High: ["#00ff9d", "#001a0d"],
    Medium: ["#ffbb00", "#1a1200"],
    Low: ["#ff9900", "#1a0800"],
    None: ["#888", "#111"],
    COMPLEX: ["#aa00ff", "#110022"],
    Complete: ["#00ff9d", "#001a0d"],
    Partial: ["#ffbb00", "#1a1200"],
  };
  const [bg, fg] = map[status] || ["#555", "#fff"];
  return (
    <span style={{
      background: bg, color: fg, padding: "2px 10px", borderRadius: 4,
      fontFamily: "'Space Mono', monospace", fontSize: 11, fontWeight: 700,
      letterSpacing: 1, textTransform: "uppercase"
    }}>{status}</span>
  );
}

function CertCard({ cert, index, onSelect, selected, onAnalyze, onReExtract }) {
  return (
    <div onClick={() => onSelect(index)} style={{
      background: selected ? "#0a1628" : "#060e1a",
      border: selected ? "1px solid #00bfff" : "1px solid #0d2040",
      borderRadius: 8, padding: "14px 18px", cursor: "pointer",
      marginBottom: 8, transition: "all 0.15s",
      boxShadow: selected ? "0 0 16px #00bfff33" : "none"
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div>
          <div style={{ color: "#00bfff", fontFamily: "'Space Mono',monospace", fontSize: 11, marginBottom: 4, display: "flex", alignItems: "center", gap: 6 }}>
            {cert.data?.docType || "CERTIFICATE"}
            {certIsComplex(cert.data) && <Badge status="COMPLEX" />}
          </div>
          <div style={{ color: "#e0f0ff", fontWeight: 600, fontSize: 13 }}>
            {cert.data?.uniqueNumber || cert.filename}
          </div>
          <div style={{ color: "#4a7fa0", fontSize: 11, marginTop: 3 }}>
            {cert.data?.issuer || cert.data?.supplier} → {cert.data?.recipient}
          </div>
        </div>
        <div style={{ textAlign: "right", display: "flex", flexDirection: "column", gap: 6, alignItems: "flex-end" }}>
          {cert.analysis
            ? (
              <div style={{ display: "flex", flexDirection: "column", gap: 4, alignItems: "flex-end" }}>
                <Badge status={cert.analysis.complianceStatus} />
                {cert.pdfPath && (
                  <button className="btn" onClick={e => { e.stopPropagation(); onReExtract(index); }} style={{
                    background: "#0a1628", color: "#4a9fd4", padding: "2px 8px", borderRadius: 4,
                    fontFamily: "'Space Mono',monospace", fontSize: 9, letterSpacing: 1,
                    border: "1px solid #0d3060"
                  }}>↺ RE-EXTRACT</button>
                )}
              </div>
            )
            : (
              <div style={{ display: "flex", flexDirection: "column", gap: 4, alignItems: "flex-end" }}>
                <button className="btn" onClick={e => { e.stopPropagation(); onAnalyze(index); }} style={{
                  background: "linear-gradient(135deg,#0050aa,#00bfff)", color: "#fff",
                  padding: "3px 10px", borderRadius: 4,
                  fontFamily: "'Space Mono',monospace", fontSize: 10, letterSpacing: 1
                }}>
                  🤖 ANALYZE
                </button>
                {cert.pdfPath && (
                  <button className="btn" onClick={e => { e.stopPropagation(); onReExtract(index); }} style={{
                    background: "#0a1628", color: "#4a9fd4", padding: "2px 8px", borderRadius: 4,
                    fontFamily: "'Space Mono',monospace", fontSize: 9, letterSpacing: 1,
                    border: "1px solid #0d3060"
                  }}>↺ RE-EXTRACT</button>
                )}
              </div>
            )
          }
          <div style={{ color: "#4a7fa0", fontSize: 10 }}>
            {cert.data?.dateDispatch || cert.data?.supplyPeriod}
          </div>
        </div>
      </div>
    </div>
  );
}

function FieldRow({ label, value, highlight }) {
  if (!value) return null;
  return (
    <div style={{
      display: "flex", gap: 12, padding: "5px 0",
      borderBottom: "1px solid #0d2040"
    }}>
      <div style={{ color: "#4a7fa0", fontSize: 11, width: 180, flexShrink: 0, fontFamily: "'Space Mono',monospace" }}>
        {label}
      </div>
      <div style={{ color: highlight ? "#00ff9d" : "#c8dff0", fontSize: 12, wordBreak: "break-word" }}>
        {value}
      </div>
    </div>
  );
}

function GHGBar({ label, value, max = 10 }) {
  const v = parseFloat(value) || 0;
  const pct = Math.min((Math.abs(v) / max) * 100, 100);
  return (
    <div style={{ marginBottom: 6 }}>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: "#4a7fa0", marginBottom: 2 }}>
        <span style={{ fontFamily: "'Space Mono',monospace" }}>{label}</span>
        <span style={{ color: "#00bfff" }}>{value || "0"} gCO₂eq/MJ</span>
      </div>
      <div style={{ background: "#0d2040", borderRadius: 2, height: 4 }}>
        <div style={{
          width: `${pct}%`, height: 4, borderRadius: 2,
          background: v < 0 ? "#00ff9d" : "#00bfff", transition: "width 0.5s"
        }} />
      </div>
    </div>
  );
}

// ─── Main Application ─────────────────────────────────────────────────────────

export default function SAFManager({ onLogout, userEmail }) {
  const [certs, setCerts] = useState([]);
  const [selected, setSelected] = useState(null);
  const [invoices, setInvoices] = useState([]);
  const [loading, setLoading] = useState("");
  const [tab, setTab] = useState("certs"); // certs | analysis | db
  const [log, setLog] = useState([]);
  const [expandedAttributionRow, setExpandedAttributionRow] = useState(null);
  const pdfInputRef = useRef();
  const csvInputRef = useRef();

  const addLog = (msg, type = "info") => {
    setLog((p) => [...p, { msg, type, ts: new Date().toLocaleTimeString() }]);
  };

  // Load persisted data from Supabase on mount
  const loadFromDB = useCallback(async () => {
    // Load certificates
    const { data: certRows, error: certErr } = await supabase
      .from("certificates").select("*").order("created_at", { ascending: false });
    if (certErr) { addLog(`✗ Cert DB error: ${certErr.message}`, "error"); }
    else {
      const count = certRows?.length || 0;
      setCerts((certRows || []).map(r => ({ id: r.id, filename: r.filename, data: r.data, analysis: r.analysis, pdfPath: r.pdf_path })));
      addLog(`↺ DB: ${count} certificate(s) loaded`, count > 0 ? "success" : "info");
    }

    // Load latest invoice CSV from storage bucket
    const { data: invRows, error: invErr } = await supabase
      .from("invoices").select("id, filename, csv_path").not("csv_path", "is", null).order("created_at", { ascending: false }).limit(1);
    if (invErr) { addLog(`✗ Invoice DB error: ${invErr.message}`, "error"); return; }
    const latest = invRows?.[0];
    if (!latest?.csv_path) return;
    const { data: blob, error: dlErr } = await supabase.storage
      .from("invoices-csv").download(latest.csv_path);
    if (dlErr) { addLog(`✗ Invoice CSV download: ${dlErr.message}`, "error"); return; }
    const text = await blob.text();
    const rows = csvToRows(text);
    setInvoices(rows);
    addLog(`✓ Loaded ${rows.length} invoice records from ${latest.filename}`, "success");
  }, []);

  useEffect(() => { loadFromDB(); }, []);

  const handlePDFUpload = useCallback(async (files) => {
    setLoading("Extracting certificate data...");
    const arr = Array.from(files);
    const newCerts = [];

    for (const file of arr) {
      if (!file.type.includes("pdf")) continue;
      addLog(`Processing: ${file.name}`, "info");
      try {
        const b64 = await fileToBase64(file);

        // Call Claude with the PDF as a document
        const res = await fetch("https://api.anthropic.com/v1/messages", {
          method: "POST",
          headers: ANTHROPIC_HEADERS,
          body: JSON.stringify({
            model: ANTHROPIC_MODEL,
            max_tokens: 4096,
            messages: [{
              role: "user",
              content: [
                {
                  type: "document",
                  source: { type: "base64", media_type: "application/pdf", data: b64 }
                },
                { type: "text", text: EXTRACT_PROMPT }
              ]
            }]
          })
        });
        const data = await res.json();
        if (!res.ok) {
          throw new Error(`API error ${res.status}: ${data.error?.message || JSON.stringify(data)}`);
        }
        const text = data.content?.map(b => b.text || "").join("") || "{}";

        let parsed = {};
        try {
          const clean = text.replace(/```json|```/g, "").trim();
          parsed = normalizeCommaDecimals(JSON.parse(clean));
        } catch (e) {
          addLog(`⚠ Parse error for ${file.name}: ${e.message} — raw: ${text.slice(0, 100)}`, "error");
        }

        const uniqueNumber = parsed.uniqueNumber || null;

        // Upload original PDF to storage bucket for future re-extraction
        const storagePath = uniqueNumber
          ? `${uniqueNumber}.pdf`
          : `no-id/${Date.now()}-${file.name}`;
        const { error: storageErr } = await supabase.storage
          .from("certificates-pdf")
          .upload(storagePath, file, { contentType: "application/pdf", upsert: true });
        if (storageErr) addLog(`⚠ PDF storage failed: ${storageErr.message} — re-extraction will not be available for this cert`, "error");

        const pdfPath = storageErr ? null : storagePath;
        let saved, dbErr;

        if (uniqueNumber) {
          // Try insert first; on unique violation (23505) update the existing row instead
          ({ data: saved, error: dbErr } = await supabase
            .from("certificates")
            .insert({ filename: file.name, data: parsed, unique_number: uniqueNumber, pdf_path: pdfPath })
            .select("id, analysis, pdf_path").single());
          if (dbErr?.code === "23505") {
            ({ data: saved, error: dbErr } = await supabase
              .from("certificates")
              .update({ filename: file.name, data: parsed, pdf_path: pdfPath })
              .eq("unique_number", uniqueNumber)
              .select("id, analysis, pdf_path").single());
          }
        } else {
          ({ data: saved, error: dbErr } = await supabase
            .from("certificates")
            .insert({ filename: file.name, data: parsed, pdf_path: pdfPath })
            .select("id, analysis, pdf_path").single());
        }

        if (dbErr) addLog(`⚠ DB save error for ${file.name}: ${dbErr.message}`, "error");
        else addLog(`✓ Saved: ${parsed.uniqueNumber || file.name}${pdfPath ? " + PDF stored" : ""}`, "success");
        newCerts.push({ id: saved?.id, filename: file.name, data: parsed, analysis: saved?.analysis ?? null, pdfPath: saved?.pdf_path ?? pdfPath });
      } catch (e) {
        addLog(`✗ Error: ${file.name} — ${e.message}`, "error");
      }
    }

    setCerts(p => {
      const map = new Map(p.map(c => [c.id, c]));
      for (const c of newCerts) if (c.id) map.set(c.id, c);
      return [...map.values()];
    });
    setLoading("");
    if (newCerts.length) setTab("certs");
  }, []);

  const handleCSVUpload = useCallback(async (file) => {
    try {
      // Upload CSV to storage bucket (upsert so re-uploading a new file replaces it)
      const storagePath = `invoices/${file.name}`;
      const { error: storageErr } = await supabase.storage
        .from("invoices-csv")
        .upload(storagePath, file, { contentType: "text/csv", upsert: true });
      if (storageErr) {
        addLog(`⚠ Storage upload: ${storageErr.message}`, "error");
        return;
      }

      // Save metadata to invoices table
      const { error: dbErr } = await supabase
        .from("invoices")
        .insert({ filename: file.name, csv_path: storagePath });
      if (dbErr) addLog(`⚠ DB save warning: ${dbErr.message}`, "error");

      // Parse and load into memory
      const text = await file.text();
      const rows = csvToRows(text);
      setInvoices(rows);
      addLog(`✓ Loaded ${rows.length} invoice records from ${file.name}`, "success");
    } catch (e) {
      addLog(`✗ CSV error: ${e.message}`, "error");
    }
  }, []);

  const analyzeComplexPoC = useCallback(async (index) => {
    const cert = certs[index];
    if (!cert) return;
    const label = cert.data?.uniqueNumber || cert.filename;
    addLog(`Complex PoC analysis: ${label}`, "info");

    // Step 1: Compliance check
    setLoading("[1/2] Compliance check...");
    let complianceAnalysis = { complianceStatus: "Needs Review", complianceNotes: "Compliance check failed" };
    try {
      const filteredForCompliance = filterInvoicesForCert(cert.data, invoices);
      const prompt = COMPARE_PROMPT(cert.data, filteredForCompliance);
      const res = await callClaude([{ role: "user", content: prompt }],
        "You are a SAF compliance expert. Return only valid JSON.", 4096);
      const clean = res.replace(/```json|```/g, "").trim();
      complianceAnalysis = JSON.parse(clean);
      addLog(`✓ Compliance: ${complianceAnalysis.complianceStatus}`, "success");
    } catch (e) {
      addLog(`⚠ Compliance step error: ${e.message}`, "error");
    }

    // Step 2: Client attribution
    setLoading("[2/2] Attribution...");
    let attributionResult = null;
    try {
      const filteredForAttribution = filterInvoicesForCert(cert.data, invoices);
      const prompt = ATTRIBUTION_PROMPT(cert.data, filteredForAttribution);
      const res = await callClaude([{ role: "user", content: prompt }],
        "You are a SAF compliance analyst. Return only valid JSON.", 4096);
      const clean = res.replace(/```json|```/g, "").trim();
      attributionResult = JSON.parse(clean);
      addLog(`✓ Attribution: ${attributionResult.attributionStatus}`, "success");
    } catch (e) {
      addLog(`⚠ Attribution step error: ${e.message}`, "error");
    }

    const analysis = { ...complianceAnalysis, isComplexPoC: true, attribution: attributionResult };
    if (cert.id) {
      const { error: updateErr } = await supabase.from("certificates").update({ analysis }).eq("id", cert.id);
      if (updateErr) addLog(`⚠ Failed to persist analysis: ${updateErr.message}`, "error");
    }
    setCerts(p => { const a = [...p]; a[index] = { ...cert, analysis }; return a; });
    setLoading("");
    setTab("analysis");
  }, [certs, invoices]);

  const analyzeAll = useCallback(async () => {
    if (!certs.length) return;
    setLoading("Analyzing with AI...");
    const updated = [...certs];

    for (let i = 0; i < updated.length; i++) {
      const cert = updated[i];
      addLog(`Analyzing: ${cert.data?.uniqueNumber || cert.filename}`, "info");

      if (certIsComplex(cert.data)) {
        await analyzeComplexPoC(i);
        updated[i] = certs[i]; // will be updated by analyzeComplexPoC via setCerts
        continue;
      }

      try {
        const filteredInvoices = filterInvoicesForCert(cert.data, invoices);
        const prompt = COMPARE_PROMPT(cert.data, filteredInvoices.length ? filteredInvoices : invoices.slice(0, 300));
        const res = await callClaude([{ role: "user", content: prompt }],
          "You are a SAF compliance expert. Return only valid JSON.");
        const clean = res.replace(/```json|```/g, "").trim();
        const analysis = JSON.parse(clean);
        updated[i] = { ...cert, analysis };
        if (cert.id) {
          const { error: updateErr } = await supabase.from("certificates").update({ analysis }).eq("id", cert.id);
          if (updateErr) addLog(`⚠ Failed to persist analysis: ${updateErr.message}`, "error");
        }
        addLog(`✓ Analysis complete: ${analysis.complianceStatus}`, "success");
      } catch (e) {
        addLog(`✗ Analysis error: ${e.message}`, "error");
        const fallback = { complianceStatus: "Needs Review", complianceNotes: "Error during analysis" };
        updated[i] = { ...cert, analysis: fallback };
        if (cert.id) {
          const { error: updateErr } = await supabase.from("certificates").update({ analysis: fallback }).eq("id", cert.id);
          if (updateErr) addLog(`⚠ Failed to persist analysis: ${updateErr.message}`, "error");
        }
      }
    }

    setCerts(updated);
    setLoading("");
    setTab("analysis");
  }, [certs, invoices, analyzeComplexPoC]);

  const analyzeSingle = useCallback(async (index) => {
    const cert = certs[index];
    if (!cert) return;

    if (certIsComplex(cert.data)) return analyzeComplexPoC(index);

    setLoading(`Analyzing ${cert.data?.uniqueNumber || cert.filename}...`);
    addLog(`Analyzing: ${cert.data?.uniqueNumber || cert.filename}`, "info");
    try {
      const filteredInvoices = filterInvoicesForCert(cert.data, invoices);
      const prompt = COMPARE_PROMPT(cert.data, filteredInvoices.length ? filteredInvoices : invoices.slice(0, 300));
      const res = await callClaude([{ role: "user", content: prompt }],
        "You are a SAF compliance expert. Return only valid JSON.");
      const clean = res.replace(/```json|```/g, "").trim();
      const analysis = JSON.parse(clean);
      const updated = { ...cert, analysis };
      if (cert.id) {
        const { error: updateErr } = await supabase.from("certificates").update({ analysis }).eq("id", cert.id);
        if (updateErr) addLog(`⚠ Failed to persist analysis: ${updateErr.message}`, "error");
      }
      setCerts(p => { const a = [...p]; a[index] = updated; return a; });
      addLog(`✓ Analysis complete: ${analysis.complianceStatus}`, "success");
      setTab("analysis");
    } catch (e) {
      addLog(`✗ Analysis error: ${e.message}`, "error");
    }
    setLoading("");
  }, [certs, invoices, analyzeComplexPoC]);

  const reExtractCert = useCallback(async (index) => {
    const cert = certs[index];
    if (!cert?.pdfPath) { addLog("✗ No stored PDF for this certificate", "error"); return; }
    setLoading(`Re-extracting ${cert.data?.uniqueNumber || cert.filename}...`);
    addLog(`Re-extracting from bucket: ${cert.pdfPath}`, "info");
    try {
      const { data: blob, error: dlErr } = await supabase.storage
        .from("certificates-pdf").download(cert.pdfPath);
      if (dlErr) throw new Error(`Storage download: ${dlErr.message}`);
      const b64 = await blobToBase64(blob);
      // Retry with exponential backoff on 429
      let attempt = 0;
      let res;
      while (attempt < 4) {
        res = await fetch("https://api.anthropic.com/v1/messages", {
          method: "POST",
          headers: ANTHROPIC_HEADERS,
          body: JSON.stringify({
            model: ANTHROPIC_MODEL, max_tokens: 4096,
            messages: [{ role: "user", content: [
              { type: "document", source: { type: "base64", media_type: "application/pdf", data: b64 } },
              { type: "text", text: EXTRACT_PROMPT }
            ]}]
          })
        });
        if (res.status !== 429) break;
        const delay = Math.pow(2, attempt) * 5000;
        addLog(`⏳ Rate limited, retrying in ${delay / 1000}s…`, "info");
        await new Promise(r => setTimeout(r, delay));
        attempt++;
      }
      const data = await res.json();
      if (!res.ok) throw new Error(`API error ${res.status}: ${data.error?.message}`);
      const text = data.content?.map(b => b.text || "").join("") || "{}";
      const parsed = normalizeCommaDecimals(JSON.parse(text.replace(/```json|```/g, "").trim()));
      if (cert.id) {
        const { error: updateErr } = await supabase.from("certificates").update({ data: parsed }).eq("id", cert.id);
        if (updateErr) addLog(`⚠ Failed to persist re-extracted data: ${updateErr.message}`, "error");
      }
      setCerts(p => { const a = [...p]; a[index] = { ...cert, data: parsed }; return a; });
      addLog(`✓ Re-extracted: ${parsed.uniqueNumber || cert.filename}`, "success");
    } catch (e) {
      addLog(`✗ Re-extract error: ${e.message}`, "error");
    }
    setLoading("");
  }, [certs]);

  const syncBucket = useCallback(async () => {
    setLoading("Listing bucket files...");
    addLog("↺ Syncing bucket → DB…", "info");

    // List all files in the bucket (paginate in chunks of 1000)
    const allFiles = [];
    const PAGE = 1000;
    // Recursively list a folder and collect PDF entries
    const listFolder = async (prefix) => {
      let offset = 0;
      while (true) {
        const { data: page, error } = await supabase.storage
          .from("certificates-pdf").list(prefix || null, { limit: PAGE, offset, sortBy: { column: "name", order: "asc" } });
        if (error) { addLog(`✗ Bucket list error (${prefix || "root"}): ${error.message}`, "error"); return false; }
        if (!page?.length) break;
        for (const f of page) {
          const fullPath = prefix ? `${prefix}/${f.name}` : f.name;
          if (f.id === null) {
            // It's a virtual folder — recurse into it
            const ok = await listFolder(fullPath);
            if (!ok) return false;
          } else if (f.name.toLowerCase().endsWith(".pdf")) {
            allFiles.push({ ...f, name: fullPath });
          }
        }
        if (page.length < PAGE) break;
        offset += PAGE;
      }
      return true;
    };
    const ok = await listFolder(null);
    if (!ok) { setLoading(""); return; }
    addLog(`↳ ${allFiles.length} PDF file(s) found in bucket`, "info");

    // Load all known pdf_paths from DB
    const { data: dbRows } = await supabase.from("certificates").select("pdf_path");
    const knownPaths = new Set((dbRows || []).map(r => r.pdf_path).filter(Boolean));

    const orphans = allFiles.filter(f => !knownPaths.has(f.name));
    addLog(`Found ${allFiles.length} bucket file(s), ${orphans.length} not yet in DB`, "info");

    if (!orphans.length) { addLog("✓ All bucket files already in DB", "success"); setLoading(""); return; }

    let done = 0;
    for (const file of orphans) {
      const pdfPath = file.name;
      setLoading(`Syncing ${done + 1}/${orphans.length}: ${pdfPath}`);
      try {
        const { data: blob, error: dlErr } = await supabase.storage.from("certificates-pdf").download(pdfPath);
        if (dlErr) throw new Error(`Download: ${dlErr.message}`);
        const b64 = await blobToBase64(blob);

        // Call Claude with retry on 429
        let attempt = 0, res;
        while (attempt < 5) {
          res = await fetch("https://api.anthropic.com/v1/messages", {
            method: "POST",
            headers: ANTHROPIC_HEADERS,
            body: JSON.stringify({
              model: ANTHROPIC_MODEL, max_tokens: 4096,
              messages: [{ role: "user", content: [
                { type: "document", source: { type: "base64", media_type: "application/pdf", data: b64 } },
                { type: "text", text: EXTRACT_PROMPT }
              ]}]
            })
          });
          if (res.status !== 429) break;
          const delay = Math.pow(2, attempt) * 5000;
          addLog(`⏳ Rate limited, retrying in ${delay / 1000}s…`, "info");
          await new Promise(r => setTimeout(r, delay));
          attempt++;
        }

        const data = await res.json();
        if (!res.ok) throw new Error(`API error ${res.status}: ${data.error?.message}`);
        const text = data.content?.map(b => b.text || "").join("") || "{}";
        let parsed = {};
        try { parsed = normalizeCommaDecimals(JSON.parse(text.replace(/```json|```/g, "").trim())); }
        catch (e) { addLog(`⚠ Parse error for ${pdfPath}: ${e.message}`, "error"); }

        const uniqueNumber = parsed.uniqueNumber || null;
        const filename = pdfPath.split("/").pop();

        let saved, dbErr;
        if (uniqueNumber) {
          ({ data: saved, error: dbErr } = await supabase.from("certificates")
            .insert({ filename, data: parsed, unique_number: uniqueNumber, pdf_path: pdfPath })
            .select("id").single());
          if (dbErr?.code === "23505") {
            // Already exists by unique_number — just update pdf_path + data
            ({ data: saved, error: dbErr } = await supabase.from("certificates")
              .update({ data: parsed, pdf_path: pdfPath })
              .eq("unique_number", uniqueNumber)
              .select("id").single());
          }
        } else {
          ({ data: saved, error: dbErr } = await supabase.from("certificates")
            .insert({ filename, data: parsed, pdf_path: pdfPath })
            .select("id").single());
        }

        if (dbErr) addLog(`⚠ DB error for ${pdfPath}: ${dbErr.message}`, "error");
        else addLog(`✓ [${done + 1}/${orphans.length}] ${parsed.uniqueNumber || filename}`, "success");
      } catch (e) {
        addLog(`✗ [${done + 1}/${orphans.length}] ${pdfPath}: ${e.message}`, "error");
      }
      done++;
      // Pause between calls to stay under 30k tokens/min rate limit
      if (done < orphans.length) await new Promise(r => setTimeout(r, 4000));
    }

    addLog(`✓ Sync complete — ${done} file(s) processed`, "success");
    setLoading("Reloading DB…");
    await loadFromDB();
    setLoading("");
  }, [loadFromDB]);

  const exportXLSX = useCallback(async () => {
    // Build CSV-like export data
    const headers = [
      "Doc Type", "Unique Number", "Date Issuance", "Supplier", "Recipient",
      "Contract Number", "Date Dispatch", "Product Type", "Raw Material",
      "Country Origin", "Quantity", "Unit", "Energy (MJ)", "GHG Total", "GHG Saving %",
      "Compliance Status", "Recommended Action", "Match Confidence", "Matched Invoice",
      "Discrepancies", "Warnings"
    ];

    const rows = certs.map(c => [
      c.data?.docType, c.data?.uniqueNumber, c.data?.dateIssuance,
      c.data?.issuer || c.data?.supplier, c.data?.recipient, c.data?.contractNumber,
      c.data?.dateDispatch, c.data?.productType, c.data?.rawMaterial,
      c.data?.rawMaterialOrigin, c.data?.quantity, c.data?.quantityUnit,
      c.data?.energyContent, c.data?.ghgTotal, c.data?.ghgSaving,
      c.analysis?.complianceStatus, c.analysis?.recommendedAction,
      c.analysis?.matchConfidence, c.analysis?.matchedInvoice,
      (c.analysis?.discrepancies || []).join("; "),
      (c.analysis?.warnings || []).join("; ")
    ]);

    const csv = [headers, ...rows].map(r => r.map(v => `"${v ?? ""}"`).join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `SAF_Certificates_Export_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    addLog("✓ Export complete", "success");
  }, [certs]);

  const selectedCert = selected !== null ? certs[selected] : null;

  // Stats
  const stats = {
    total: certs.length,
    compliant: certs.filter(c => c.analysis?.complianceStatus === "Compliant").length,
    issues: certs.filter(c => c.analysis?.complianceStatus === "Non-Compliant").length,
    review: certs.filter(c => c.analysis?.complianceStatus === "Needs Review").length,
  };

  return (
    <div style={{
      fontFamily: "'DM Sans', 'Segoe UI', sans-serif",
      background: "#020b16",
      minHeight: "100vh",
      color: "#c8dff0",
      display: "flex",
      flexDirection: "column"
    }}>
      {/* Import Space Mono */}
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');
        * { box-sizing: border-box; }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: #060e1a; }
        ::-webkit-scrollbar-thumb { background: #0d3060; border-radius: 2px; }
        input[type=file] { display: none; }
        .btn { cursor: pointer; border: none; transition: all 0.15s; }
        .btn:hover { filter: brightness(1.15); }
        .btn:active { transform: scale(0.97); }
        .tab-btn { background: none; border: none; cursor: pointer; padding: 8px 16px; 
          font-family: 'Space Mono', monospace; font-size: 11px; letter-spacing: 1px;
          text-transform: uppercase; transition: all 0.15s; }
      `}</style>

      {/* Header */}
      <div style={{
        borderBottom: "1px solid #0d2040",
        padding: "16px 28px",
        display: "flex", alignItems: "center", justifyContent: "space-between",
        background: "#030d1a"
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
          <div style={{
            width: 36, height: 36, borderRadius: 8,
            background: "linear-gradient(135deg,#0050aa,#00bfff)",
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 18, fontWeight: 900, color: "#fff", letterSpacing: -1
          }}>✈</div>
          <div>
            <div style={{ fontFamily: "'Space Mono',monospace", color: "#00bfff", fontSize: 13, letterSpacing: 2, fontWeight: 700 }}>
              TITAN AVIATION FUELS
            </div>
            <div style={{ color: "#4a7fa0", fontSize: 10, letterSpacing: 1 }}>
              SAF CERTIFICATE MANAGEMENT SYSTEM
            </div>
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          {userEmail && (
            <span style={{ fontFamily: "'Space Mono',monospace", fontSize: 10, color: "#4a7fa0", letterSpacing: 1 }}>
              {userEmail}
            </span>
          )}
          {onLogout && (
            <button className="btn" onClick={onLogout} style={{
              background: "transparent", color: "#4a7fa0", padding: "5px 12px",
              borderRadius: 5, fontFamily: "'Space Mono',monospace", fontSize: 10,
              letterSpacing: 1, border: "1px solid #0d3060", cursor: "pointer"
            }}>
              SIGN OUT
            </button>
          )}
          {loading && (
            <div style={{
              color: loading.startsWith("[2/2]") ? "#aa00ff" : "#00bfff",
              fontFamily: "'Space Mono',monospace", fontSize: 11,
              padding: "6px 14px", background: loading.startsWith("[2/2]") ? "#aa00ff11" : "#00bfff11",
              borderRadius: 6,
              border: `1px solid ${loading.startsWith("[2/2]") ? "#aa00ff33" : "#00bfff33"}`,
              animation: "pulse 1.5s infinite",
              display: "flex", alignItems: "center", gap: 8
            }}>
              {loading.startsWith("[1/2]") && <span style={{ letterSpacing: 2 }}>■□</span>}
              {loading.startsWith("[2/2]") && <span style={{ letterSpacing: 2, color: "#aa00ff" }}>■■</span>}
              ⟳ {loading}
            </div>
          )}
        </div>
      </div>

      {/* Toolbar */}
      <div style={{
        display: "flex", alignItems: "center", gap: 10, padding: "12px 28px",
        borderBottom: "1px solid #0d2040", background: "#030d1a"
      }}>
        {/* Upload PDF */}
        <button className="btn" onClick={() => pdfInputRef.current?.click()} style={{
          background: "#0050aa", color: "#fff", padding: "7px 16px", borderRadius: 6,
          fontFamily: "'Space Mono',monospace", fontSize: 11, letterSpacing: 1
        }}>
          ＋ IMPORT PDF(s)
        </button>
        <input ref={pdfInputRef} type="file" accept=".pdf" multiple
          onChange={e => handlePDFUpload(e.target.files)} />

        {/* Upload CSV */}
        <button className="btn" onClick={() => csvInputRef.current?.click()} style={{
          background: "#0d2040", color: "#c8dff0", padding: "7px 16px", borderRadius: 6,
          fontFamily: "'Space Mono',monospace", fontSize: 11, letterSpacing: 1,
          border: "1px solid #1a4080"
        }}>
          📊 LOAD INVOICE CSV
        </button>
        <input ref={csvInputRef} type="file" accept=".csv"
          onChange={e => e.target.files[0] && handleCSVUpload(e.target.files[0])} />

        <button className="btn" onClick={loadFromDB} style={{
          background: "#0a1628", color: "#4a9fd4", padding: "7px 14px", borderRadius: 6,
          fontFamily: "'Space Mono',monospace", fontSize: 11, letterSpacing: 1,
          border: "1px solid #0d3060"
        }}>
          🔄 RELOAD DB
        </button>

        <button className="btn" onClick={syncBucket} style={{
          background: "#0a1628", color: "#f0c040", padding: "7px 14px", borderRadius: 6,
          fontFamily: "'Space Mono',monospace", fontSize: 11, letterSpacing: 1,
          border: "1px solid #403010"
        }}>
          ☁ SYNC BUCKET
        </button>

        <div style={{ flex: 1 }} />

        {certs.length > 0 && (
          <>
            {certs.some(c => c.pdfPath) && (
              <button className="btn" onClick={async () => {
                for (let i = 0; i < certs.length; i++) {
                  if (!certs[i]?.pdfPath) continue;
                  await reExtractCert(i);
                  if (i < certs.length - 1) await new Promise(r => setTimeout(r, 3000));
                }
              }} style={{
                background: "#001428", color: "#4a9fd4", padding: "7px 16px", borderRadius: 6,
                fontFamily: "'Space Mono',monospace", fontSize: 11, letterSpacing: 1,
                border: "1px solid #0d3060"
              }}>
                ↺ RE-EXTRACT ALL
              </button>
            )}
            <button className="btn" onClick={analyzeAll} style={{
              background: "linear-gradient(135deg,#0050aa,#00bfff)", color: "#fff",
              padding: "7px 18px", borderRadius: 6,
              fontFamily: "'Space Mono',monospace", fontSize: 11, letterSpacing: 1
            }}>
              🤖 AI ANALYZE ALL
            </button>
            <button className="btn" onClick={exportXLSX} style={{
              background: "#003322", color: "#00ff9d", padding: "7px 16px", borderRadius: 6,
              fontFamily: "'Space Mono',monospace", fontSize: 11, letterSpacing: 1,
              border: "1px solid #00ff9d44"
            }}>
              ↓ EXPORT CSV
            </button>
          </>
        )}
      </div>

      {/* Stats Bar */}
      {certs.length > 0 && (
        <div style={{
          display: "flex", gap: 1, borderBottom: "1px solid #0d2040",
          background: "#030d1a"
        }}>
          {[
            { label: "TOTAL", val: stats.total, color: "#00bfff" },
            { label: "COMPLIANT", val: stats.compliant, color: "#00ff9d" },
            { label: "ISSUES", val: stats.issues, color: "#ff4444" },
            { label: "REVIEW", val: stats.review, color: "#ffbb00" },
            { label: "INVOICES", val: invoices.length, color: "#888" },
          ].map(s => (
            <div key={s.label} style={{
              flex: 1, padding: "8px 20px", borderRight: "1px solid #0d2040"
            }}>
              <div style={{ fontFamily: "'Space Mono',monospace", fontSize: 18, color: s.color, fontWeight: 700 }}>
                {s.val}
              </div>
              <div style={{ color: "#4a7fa0", fontSize: 9, letterSpacing: 1 }}>{s.label}</div>
            </div>
          ))}
        </div>
      )}

      {/* Tabs */}
      <div style={{ display: "flex", borderBottom: "1px solid #0d2040", background: "#030d1a" }}>
        {[["certs", "📄 CERTIFICATES"], ["analysis", "🔍 ANALYSIS"], ["db", "📊 INVOICES"], ["log", "📋 LOG"]].map(([k, label]) => (
          <button key={k} className="tab-btn" onClick={() => setTab(k)} style={{
            color: tab === k ? "#00bfff" : "#4a7fa0",
            borderBottom: tab === k ? "2px solid #00bfff" : "2px solid transparent",
            marginBottom: -1
          }}>{label}</button>
        ))}
      </div>

      {/* Main Content */}
      <div style={{ display: "flex", flex: 1, overflow: "hidden", height: "calc(100vh - 220px)" }}>

        {/* ── CERTIFICATES TAB ── */}
        {tab === "certs" && (
          <>
            <div style={{
              width: 300, borderRight: "1px solid #0d2040",
              overflowY: "auto", padding: 12
            }}>
              {certs.length === 0 ? (
                <div style={{
                  padding: "40px 20px", textAlign: "center", color: "#4a7fa0"
                }}>
                  <div style={{ fontSize: 36, marginBottom: 12 }}>✈</div>
                  <div style={{ fontFamily: "'Space Mono',monospace", fontSize: 11 }}>
                    Import SAF certificate PDFs to begin
                  </div>
                </div>
              ) : certs.map((c, i) => (
                <CertCard key={i} cert={c} index={i}
                  onSelect={setSelected} selected={selected === i}
                  onAnalyze={analyzeSingle} onReExtract={reExtractCert} />
              ))}
            </div>

            <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
              {selectedCert ? (
                <div>
                  <div style={{
                    display: "flex", justifyContent: "space-between",
                    alignItems: "center", marginBottom: 20
                  }}>
                    <div>
                      <div style={{ fontFamily: "'Space Mono',monospace", color: "#00bfff", fontSize: 12 }}>
                        {selectedCert.data?.docType}
                      </div>
                      <div style={{ fontSize: 18, fontWeight: 600, color: "#e0f0ff" }}>
                        {selectedCert.data?.uniqueNumber || selectedCert.filename}
                      </div>
                    </div>
                    {selectedCert.analysis && (
                      <Badge status={selectedCert.analysis.complianceStatus} />
                    )}
                  </div>

                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}>
                    {/* Party info */}
                    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
                      <div style={{ color: "#00bfff", fontFamily: "'Space Mono',monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                        PARTIES
                      </div>
                      <FieldRow label="ISSUER" value={selectedCert.data?.issuer || selectedCert.data?.supplier} />
                      <FieldRow label="ISSUER ADDR" value={selectedCert.data?.issuerAddress || selectedCert.data?.supplierAddress} />
                      <FieldRow label="CERT NUMBER" value={selectedCert.data?.issuerCertNumber || selectedCert.data?.supplierCertNumber} />
                      {selectedCert.data?.safSupplier && <FieldRow label="SAF SUPPLIER" value={selectedCert.data?.safSupplier} highlight />}
                      <FieldRow label="RECIPIENT" value={selectedCert.data?.recipient} />
                      <FieldRow label="RECIPIENT ADDR" value={selectedCert.data?.recipientAddress} />
                      <FieldRow label="CONTRACT NR" value={selectedCert.data?.contractNumber} />
                    </div>

                    {/* Logistics */}
                    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
                      <div style={{ color: "#00bfff", fontFamily: "'Space Mono',monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                        LOGISTICS
                      </div>
                      <FieldRow label="DISPATCH ADDR" value={selectedCert.data?.dispatchAddress} />
                      <FieldRow label="RECEIPT ADDR" value={selectedCert.data?.receiptAddress} />
                      <FieldRow label="DATE DISPATCH" value={selectedCert.data?.dateDispatch} />
                      {selectedCert.data?.supplyPeriod && <FieldRow label="SUPPLY PERIOD" value={selectedCert.data?.supplyPeriod} />}
                      {selectedCert.data?.deliveryAirports && <FieldRow label="AIRPORTS" value={selectedCert.data?.deliveryAirports} />}
                      <FieldRow label="ISSUED" value={selectedCert.data?.dateIssuance} />
                    </div>

                    {/* Product */}
                    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
                      <div style={{ color: "#00bfff", fontFamily: "'Space Mono',monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                        PRODUCT & FEEDSTOCK
                      </div>
                      <FieldRow label="PRODUCT TYPE" value={selectedCert.data?.productType} />
                      <FieldRow label="SAF TYPE" value={selectedCert.data?.safType} />
                      <FieldRow label="RAW MATERIAL" value={selectedCert.data?.rawMaterial} />
                      <FieldRow label="ORIGIN" value={selectedCert.data?.rawMaterialOrigin} />
                      <FieldRow label="BIO QUANTITY" value={`${selectedCert.data?.quantity} ${selectedCert.data?.quantityUnit || "m³"}`} />
                      {selectedCert.data?.totalVolume && <FieldRow label="TOTAL VOLUME" value={`${selectedCert.data?.totalVolume} ${selectedCert.data?.quantityUnit || "m³"}`} />}
                      <FieldRow label="ENERGY (MJ)" value={selectedCert.data?.energyContent} />
                      <FieldRow label="LCV (MJ/kg)" value={selectedCert.data?.lcv} />
                      <FieldRow label="CHAIN" value={selectedCert.data?.chainOfCustody} />
                      <FieldRow label="EU RED" value={selectedCert.data?.euRedCompliant} highlight />
                      <FieldRow label="ISCC" value={selectedCert.data?.isccCompliant} />
                      <FieldRow label="SCHEME" value={selectedCert.data?.complianceScheme} />
                    </div>

                    {/* underlyingPoSList */}
                    {selectedCert.data?.underlyingPoSList?.length > 0 && (
                      <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040", gridColumn: "1 / -1" }}>
                        <div style={{ color: "#00bfff", fontFamily: "'Space Mono',monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                          UNDERLYING PoS BATCHES
                        </div>
                        <div style={{ overflowX: "auto" }}>
                          <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "'Space Mono',monospace", fontSize: 10 }}>
                            <thead>
                              <tr style={{ borderBottom: "1px solid #0d3060" }}>
                                {["PoS NUMBER", "RAW MATERIAL", "ORIGIN", "GHG TOTAL", "GHG SAVING", "QUANTITY", "UNIT"].map(h => (
                                  <th key={h} style={{ padding: "6px 10px", textAlign: "left", color: "#00bfff", letterSpacing: 1, whiteSpace: "nowrap", fontSize: 9 }}>{h}</th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {selectedCert.data.underlyingPoSList.map((pos, pi) => (
                                <tr key={pi} style={{ borderBottom: "1px solid #0d2040", background: pi % 2 === 0 ? "#060e1a" : "#030d1a" }}>
                                  <td style={{ padding: "5px 10px", color: "#00bfff" }}>{pos.poSNumber || "—"}</td>
                                  <td style={{ padding: "5px 10px", color: "#c8dff0" }}>{pos.rawMaterial || "—"}</td>
                                  <td style={{ padding: "5px 10px", color: "#c8dff0" }}>{pos.origin || "—"}</td>
                                  <td style={{ padding: "5px 10px", color: "#00ff9d" }}>{pos.ghgTotal || "—"}</td>
                                  <td style={{ padding: "5px 10px", color: "#00ff9d" }}>{pos.ghgSaving || "—"}</td>
                                  <td style={{ padding: "5px 10px", color: "#e0f0ff", fontWeight: 600 }}>{pos.quantity || "—"}</td>
                                  <td style={{ padding: "5px 10px", color: "#888" }}>{pos.quantityUnit || "—"}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}

                    {/* GHG */}
                    <div style={{ background: "#060e1a", borderRadius: 8, padding: 16, border: "1px solid #0d2040" }}>
                      <div style={{ color: "#00bfff", fontFamily: "'Space Mono',monospace", fontSize: 10, marginBottom: 10, letterSpacing: 1 }}>
                        GHG EMISSIONS
                      </div>
                      <div style={{
                        display: "flex", justifyContent: "space-between",
                        marginBottom: 14, padding: "8px 12px",
                        background: "#001a0d", borderRadius: 6, border: "1px solid #00ff9d33"
                      }}>
                        <span style={{ color: "#4a7fa0", fontSize: 11 }}>TOTAL GHG</span>
                        <span style={{ color: "#00ff9d", fontFamily: "'Space Mono',monospace", fontWeight: 700 }}>
                          {selectedCert.data?.ghgTotal} gCO₂eq/MJ
                        </span>
                      </div>
                      <div style={{
                        display: "flex", justifyContent: "space-between",
                        marginBottom: 14, padding: "8px 12px",
                        background: "#001a0d", borderRadius: 6, border: "1px solid #00ff9d33"
                      }}>
                        <span style={{ color: "#4a7fa0", fontSize: 11 }}>GHG SAVING</span>
                        <span style={{ color: "#00ff9d", fontFamily: "'Space Mono',monospace", fontWeight: 700 }}>
                          {selectedCert.data?.ghgSaving}
                        </span>
                      </div>
                      <GHGBar label="Eec (extraction)" value={selectedCert.data?.ghgEec} />
                      <GHGBar label="El (land use)" value={selectedCert.data?.ghgEl} />
                      <GHGBar label="Ep (processing)" value={selectedCert.data?.ghgEp} />
                      <GHGBar label="Etd (transport)" value={selectedCert.data?.ghgEtd} />
                      <GHGBar label="Eu (fuel use)" value={selectedCert.data?.ghgEu} />
                    </div>
                  </div>
                </div>
              ) : (
                <div style={{ color: "#4a7fa0", padding: 40, textAlign: "center", fontFamily: "'Space Mono',monospace", fontSize: 11 }}>
                  Select a certificate to view details
                </div>
              )}
            </div>
          </>
        )}

        {/* ── ANALYSIS TAB ── */}
        {tab === "analysis" && (
          <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
            {certs.filter(c => c.analysis).length === 0 ? (
              <div style={{ textAlign: "center", color: "#4a7fa0", padding: 60 }}>
                <div style={{ fontSize: 36, marginBottom: 12 }}>🤖</div>
                <div style={{ fontFamily: "'Space Mono',monospace", fontSize: 11 }}>
                  Click "AI ANALYZE ALL" to run compliance analysis
                </div>
              </div>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
                {certs.map((cert, i) => cert.analysis && (
                  <div key={i} style={{
                    background: "#060e1a", border: "1px solid #0d2040",
                    borderRadius: 10, padding: 20,
                    borderLeft: `4px solid ${
                      cert.analysis.complianceStatus === "Compliant" ? "#00ff9d" :
                      cert.analysis.complianceStatus === "Non-Compliant" ? "#ff4444" : "#ffbb00"
                    }`
                  }}>
                    <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 14 }}>
                      <div>
                        <div style={{ fontFamily: "'Space Mono',monospace", color: "#00bfff", fontSize: 11 }}>
                          {cert.data?.docType} · {cert.data?.dateDispatch}
                        </div>
                        <div style={{ fontSize: 15, fontWeight: 600, color: "#e0f0ff", marginTop: 2 }}>
                          {cert.data?.uniqueNumber || cert.filename}
                        </div>
                        <div style={{ color: "#4a7fa0", fontSize: 11 }}>
                          {cert.data?.issuer || cert.data?.supplier} → {cert.data?.recipient}
                        </div>
                      </div>
                      <div style={{ display: "flex", flexDirection: "column", gap: 6, alignItems: "flex-end" }}>
                        <Badge status={cert.analysis.complianceStatus} />
                        <Badge status={cert.analysis.recommendedAction} />
                        <div style={{ fontSize: 10, color: "#4a7fa0" }}>
                          Match: <Badge status={cert.analysis.matchConfidence} />
                        </div>
                      </div>
                    </div>

                    <div style={{ color: "#c8dff0", fontSize: 12, marginBottom: 12, lineHeight: 1.5 }}>
                      {cert.analysis.complianceNotes}
                    </div>

                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12 }}>
                      {cert.analysis.matchedInvoice && (
                        <div style={{ background: "#001a0d", borderRadius: 6, padding: 10, border: "1px solid #00ff9d22" }}>
                          <div style={{ color: "#4a7fa0", fontSize: 9, marginBottom: 4 }}>MATCHED INVOICE</div>
                          <div style={{ color: "#00ff9d", fontFamily: "'Space Mono',monospace", fontSize: 11 }}>
                            {cert.analysis.matchedInvoice}
                          </div>
                          {cert.analysis.matchedInvoiceRows?.map((ref, k) => (
                            <div key={k} style={{ color: "#88ffcc", fontSize: 9, marginTop: 2 }}>• {ref}</div>
                          ))}
                        </div>
                      )}

                      {cert.analysis.discrepancies?.length > 0 && (
                        <div style={{ background: "#1a0000", borderRadius: 6, padding: 10, border: "1px solid #ff444422" }}>
                          <div style={{ color: "#ff4444", fontSize: 9, marginBottom: 4 }}>DISCREPANCIES</div>
                          {cert.analysis.discrepancies.map((d, j) => (
                            <div key={j} style={{ color: "#ffaaaa", fontSize: 10, marginBottom: 2 }}>• {d}</div>
                          ))}
                        </div>
                      )}

                      {cert.analysis.warnings?.length > 0 && (
                        <div style={{ background: "#1a1200", borderRadius: 6, padding: 10, border: "1px solid #ffbb0022" }}>
                          <div style={{ color: "#ffbb00", fontSize: 9, marginBottom: 4 }}>WARNINGS</div>
                          {cert.analysis.warnings.map((w, j) => (
                            <div key={j} style={{ color: "#ffdd88", fontSize: 10, marginBottom: 2 }}>• {w}</div>
                          ))}
                        </div>
                      )}

                      {cert.analysis.matchReasons?.length > 0 && (
                        <div style={{ background: "#001020", borderRadius: 6, padding: 10, border: "1px solid #00bfff22" }}>
                          <div style={{ color: "#00bfff", fontSize: 9, marginBottom: 4 }}>MATCH REASONS</div>
                          {cert.analysis.matchReasons.map((r, j) => (
                            <div key={j} style={{ color: "#88ccff", fontSize: 10, marginBottom: 2 }}>• {r}</div>
                          ))}
                        </div>
                      )}
                    </div>

                    {/* Quick checks */}
                    <div style={{ display: "flex", gap: 10, marginTop: 12, flexWrap: "wrap" }}>
                      {[
                        ["GHG Saving OK", cert.analysis.ghgSavingOk],
                        ["Quantity Match", cert.analysis.quantityMatch],
                        ["Supplier Verified", cert.analysis.supplierVerified],
                        ["Airports Verified", cert.analysis.airportsVerified],
                        ["Contract Ref Found", cert.analysis.contractRefFound],
                      ].filter(([, v]) => v !== undefined).map(([label, ok]) => (
                        <div key={label} style={{
                          padding: "4px 10px", borderRadius: 4, fontSize: 10,
                          background: ok ? "#001a0d" : "#1a0000",
                          color: ok ? "#00ff9d" : "#ff6666",
                          border: `1px solid ${ok ? "#00ff9d33" : "#ff444433"}`,
                          fontFamily: "'Space Mono',monospace"
                        }}>
                          {ok ? "✓" : "✗"} {label}
                        </div>
                      ))}
                    </div>

                    {/* Complex PoC Attribution Table */}
                    {cert.analysis.isComplexPoC && cert.analysis.attribution && (
                      <div style={{ marginTop: 20 }}>
                        <div style={{
                          background: "#110022", border: "1px solid #aa00ff44",
                          borderRadius: 8, padding: 16
                        }}>
                          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
                            <div style={{ color: "#aa00ff", fontFamily: "'Space Mono',monospace", fontSize: 11, letterSpacing: 1 }}>
                              CLIENT ATTRIBUTION · <Badge status={cert.analysis.attribution.attributionStatus || "None"} />
                            </div>
                            <div style={{ fontSize: 10, color: "#888", fontFamily: "'Space Mono',monospace" }}>
                              {cert.analysis.attribution.attributionNotes}
                            </div>
                          </div>

                          {/* Summary bar */}
                          <div style={{ display: "flex", gap: 16, marginBottom: 14 }}>
                            <div style={{ background: "#001a0d", borderRadius: 6, padding: "8px 14px", border: "1px solid #00ff9d22", flex: 1 }}>
                              <div style={{ color: "#4a7fa0", fontSize: 9, marginBottom: 2 }}>ATTRIBUTED VOLUME</div>
                              <div style={{ color: "#00ff9d", fontFamily: "'Space Mono',monospace", fontSize: 12, fontWeight: 700 }}>
                                {cert.analysis.attribution.totalAttributedVolume || "—"}
                              </div>
                            </div>
                            <div style={{ background: "#1a0a00", borderRadius: 6, padding: "8px 14px", border: "1px solid #ff990022", flex: 1 }}>
                              <div style={{ color: "#4a7fa0", fontSize: 9, marginBottom: 2 }}>UNATTRIBUTED VOLUME</div>
                              <div style={{ color: "#ff9900", fontFamily: "'Space Mono',monospace", fontSize: 12, fontWeight: 700 }}>
                                {cert.analysis.attribution.totalUnattributedVolume || "—"}
                              </div>
                            </div>
                          </div>

                          {/* Attribution table */}
                          {cert.analysis.attribution.clientAttribution?.length > 0 && (
                            <div style={{ overflowX: "auto", marginBottom: 14 }}>
                              <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "'Space Mono',monospace", fontSize: 10 }}>
                                <thead>
                                  <tr style={{ borderBottom: "1px solid #aa00ff33" }}>
                                    {["CUSTOMER", "AIRPORT", "MONTH", "CERT VOL", "INVOICED VOL", "MATCH", "CONFIDENCE", "INVOICES"].map(h => (
                                      <th key={h} style={{ padding: "6px 10px", textAlign: "left", color: "#aa00ff", letterSpacing: 1, whiteSpace: "nowrap", fontSize: 9 }}>{h}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {cert.analysis.attribution.clientAttribution.map((row, ri) => (
                                    <React.Fragment key={`attr-${ri}`}>
                                      <tr style={{ borderBottom: "1px solid #1a0033", background: ri % 2 === 0 ? "#0d0022" : "#110022" }}>
                                        <td style={{ padding: "5px 10px", color: "#e0f0ff" }}>{row.customer}</td>
                                        <td style={{ padding: "5px 10px", color: "#00bfff" }}>{row.airport}</td>
                                        <td style={{ padding: "5px 10px", color: "#888" }}>{row.month || "—"}</td>
                                        <td style={{ padding: "5px 10px", color: "#c8dff0" }}>{row.certVolume}</td>
                                        <td style={{ padding: "5px 10px", color: "#c8dff0" }}>{row.invoicedVolume}</td>
                                        <td style={{ padding: "5px 10px" }}>
                                          <span style={{ color: row.matched ? "#00ff9d" : "#ff4444", fontWeight: 700 }}>
                                            {row.matched ? "✓" : "✗"}
                                          </span>
                                        </td>
                                        <td style={{ padding: "5px 10px" }}><Badge status={row.confidence} /></td>
                                        <td style={{ padding: "5px 10px" }}>
                                          {row.invoiceRefs?.length > 0 && (
                                            <button onClick={() => setExpandedAttributionRow(expandedAttributionRow === ri ? null : ri)}
                                              style={{ color: "#aa00ff", background: "none", border: "1px solid #aa00ff44",
                                                       borderRadius: 4, padding: "2px 6px", fontSize: 9, cursor: "pointer" }}>
                                              ▶ {row.invoiceRefs.length}
                                            </button>
                                          )}
                                        </td>
                                      </tr>
                                      {expandedAttributionRow === ri && (
                                        <tr>
                                          <td colSpan={8} style={{ background: "#0a0018", padding: "8px 16px", borderBottom: "1px solid #aa00ff44" }}>
                                            {row.invoiceRefs.map((ref, k) => (
                                              <div key={k} style={{ color: "#cc88ff", fontSize: 10, fontFamily: "'Space Mono',monospace", marginBottom: 2 }}>• {ref}</div>
                                            ))}
                                          </td>
                                        </tr>
                                      )}
                                    </React.Fragment>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          )}

                          {/* Unattributed slots */}
                          {cert.analysis.attribution.unattributedSlots?.length > 0 && (
                            <div style={{ background: "#1a1000", border: "1px solid #ffbb0022", borderRadius: 6, padding: 10 }}>
                              <div style={{ color: "#ffbb00", fontSize: 9, marginBottom: 6, fontFamily: "'Space Mono',monospace", letterSpacing: 1 }}>
                                UNATTRIBUTED SLOTS
                              </div>
                              {cert.analysis.attribution.unattributedSlots.map((slot, si) => (
                                <div key={si} style={{ color: "#ffdd88", fontSize: 10, marginBottom: 3 }}>
                                  • {slot.airport}{slot.month ? ` · ${slot.month}` : ""} — {slot.certVolume} — {slot.reason}
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* ── INVOICES DB TAB ── */}
        {tab === "db" && (
          <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
            {invoices.length === 0 ? (
              <div style={{ textAlign: "center", color: "#4a7fa0", padding: 60 }}>
                <div style={{ fontSize: 36, marginBottom: 12 }}>📊</div>
                <div style={{ fontFamily: "'Space Mono',monospace", fontSize: 11 }}>
                  Load a CSV file with your invoice records
                </div>
                <div style={{ fontSize: 10, marginTop: 8, color: "#2a5070" }}>
                  Expected columns: Invoice Nr, Supplier, Date, Quantity, SAF Volume, Airport...
                </div>
              </div>
            ) : (
              <div style={{ overflowX: "auto" }}>
                <table style={{
                  width: "100%", borderCollapse: "collapse",
                  fontFamily: "'Space Mono',monospace", fontSize: 10
                }}>
                  <thead>
                    <tr style={{ borderBottom: "2px solid #0d3060" }}>
                      {Object.keys(invoices[0]).map(h => (
                        <th key={h} style={{
                          padding: "8px 12px", textAlign: "left",
                          color: "#00bfff", letterSpacing: 1, whiteSpace: "nowrap"
                        }}>{h.toUpperCase()}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {invoices.map((row, i) => (
                      <tr key={i} style={{
                        borderBottom: "1px solid #0d2040",
                        background: i % 2 === 0 ? "#060e1a" : "#030d1a"
                      }}>
                        {Object.values(row).map((v, j) => (
                          <td key={j} style={{
                            padding: "7px 12px", color: "#c8dff0",
                            fontSize: 10, whiteSpace: "nowrap"
                          }}>{v}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}

        {/* ── LOG TAB ── */}
        {tab === "log" && (
          <div style={{ flex: 1, overflowY: "auto", padding: 24 }}>
            <div style={{ fontFamily: "'Space Mono',monospace", fontSize: 11 }}>
              {log.length === 0 ? (
                <div style={{ color: "#4a7fa0" }}>No activity yet</div>
              ) : [...log].reverse().map((entry, i) => (
                <div key={i} style={{
                  display: "flex", gap: 12, padding: "5px 0",
                  borderBottom: "1px solid #0d2040"
                }}>
                  <span style={{ color: "#2a5070", width: 80, flexShrink: 0 }}>{entry.ts}</span>
                  <span style={{
                    color: entry.type === "success" ? "#00ff9d" :
                      entry.type === "error" ? "#ff4444" : "#4a7fa0"
                  }}>{entry.msg}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
