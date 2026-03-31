function toMonthKey(value) {
  const text = String(value ?? "").trim();
  if (/^\d{4}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text.slice(0, 7);
  return "";
}

function toNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const parsed = Number.parseFloat(String(value ?? "").replace(",", "."));
  return Number.isFinite(parsed) ? parsed : 0;
}

function buildGroupKey(client, airport, month) {
  return [client || "", airport || "", month || ""].join("|").toUpperCase();
}

function addCell(airportMap, monthsSet, airport, customer, month, volume, hasClientCert) {
  if (!customer || !airport || !month) return;

  monthsSet.add(month);

  if (!airportMap.has(airport)) airportMap.set(airport, new Map());
  const clientMap = airportMap.get(airport);

  if (!clientMap.has(customer)) clientMap.set(customer, new Map());
  const monthMap = clientMap.get(customer);

  const existing = monthMap.get(month) || { volume: 0, hasClientCert: false };
  existing.volume = Number((existing.volume + volume).toFixed(6));
  if (hasClientCert) existing.hasClientCert = true;

  monthMap.set(month, existing);
}

export function buildCoverageData(certs, clientCertificateRecords) {
  const clientCertByKey = new Map(
    (clientCertificateRecords || []).map((r) => [r.group_key?.toUpperCase() || "", r])
  );

  const monthsSet = new Set();
  const airportMap = new Map();
  const seenGroupKeys = new Set();

  // 1. From approved certificate links (live allocation data)
  for (const cert of certs || []) {
    if (cert?.match?.status !== "approved") continue;

    for (const link of cert.match?.links || []) {
      const customer = String(link.customer || "").trim();
      const airport = String(link.iata || link.icao || "").trim().toUpperCase();
      const month = toMonthKey(link.uplift_date);
      const volume = toNumber(link.allocated_m3);
      const groupKey = buildGroupKey(customer, airport, month);
      const hasClientCert = clientCertByKey.has(groupKey) && !!clientCertByKey.get(groupKey).generated_file_path;

      addCell(airportMap, monthsSet, airport, customer, month, volume, hasClientCert);
      seenGroupKeys.add(groupKey);
    }
  }

  // 2. From persisted client_certificates records (survives match resets)
  for (const record of clientCertificateRecords || []) {
    if (!record.generated_file_path) continue;
    const groupKey = (record.group_key || "").toUpperCase();
    if (seenGroupKeys.has(groupKey)) continue;

    const customer = String(record.client_name || "").trim();
    const airport = String(record.airport_code || "").trim().toUpperCase();
    const month = toMonthKey(record.month);
    const volume = toNumber(record.total_saf_volume_m3);

    addCell(airportMap, monthsSet, airport, customer, month, volume, true);
  }

  const months = [...monthsSet].sort();
  const airports = [...airportMap.keys()].sort();

  return { months, airports, airportMap };
}
