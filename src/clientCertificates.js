function toNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return Number(value.toFixed(6));
  const parsed = Number.parseFloat(String(value ?? "").replace(",", "."));
  return Number.isFinite(parsed) ? Number(parsed.toFixed(6)) : null;
}

function toMonthKey(value) {
  const text = String(value ?? "").trim();
  if (/^\d{4}-\d{2}$/.test(text)) return text;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text.slice(0, 7);
  return "";
}

function toDateOnly(value) {
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value.toISOString().slice(0, 10);
  const text = String(value ?? "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const date = new Date(text);
  return Number.isNaN(date.getTime()) ? "" : date.toISOString().slice(0, 10);
}

function formatMonthLabel(monthKey) {
  if (!/^\d{4}-\d{2}$/.test(String(monthKey ?? ""))) return "";
  const [year, month] = monthKey.split("-").map(Number);
  if (month < 1 || month > 12) return "";
  const date = new Date(Date.UTC(year, month - 1, 1));
  return date.toLocaleString("en-US", {
    month: "long",
    year: "numeric",
    timeZone: "UTC",
  });
}

function formatDateDisplay(dateValue) {
  const dateOnly = toDateOnly(dateValue);
  if (!dateOnly) return "";
  const [year, month, day] = dateOnly.split("-");
  return `${day}/${month}/${year}`;
}

function slugify(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .toUpperCase();
}

function buildInternalReference({ clientName, airportCode, month, issueDate }) {
  const monthPart = String(month ?? "").replace("-", "");
  const datePart = String(issueDate ?? "").replace(/-/g, "");
  const clientPart = slugify(clientName).slice(0, 14) || "CLIENT";
  const airportPart = slugify(airportCode).slice(0, 6) || "AIRPORT";
  return `TITA-${monthPart}-${airportPart}-${clientPart}-${datePart}`;
}

function buildGroupKey(clientName, airportCode, month) {
  return [clientName || "", airportCode || "", month || ""].join("|").toUpperCase();
}

export function formatClientCertificateMonth(monthKey) {
  return formatMonthLabel(monthKey);
}

export function formatClientCertificateDate(dateValue) {
  return formatDateDisplay(dateValue);
}

export function formatClientCertificateVolume(value) {
  const numeric = toNumber(value);
  return numeric === null ? "" : numeric.toFixed(3);
}

function buildClientAddress(company) {
  if (!company) return "";
  const parts = [];
  const street = [company.street, company.street_no].filter(Boolean).join(" ").trim();
  if (street) parts.push(street);
  const cityLine = [company.zip, company.city].filter(Boolean).join(" ").trim();
  if (cityLine) parts.push(cityLine);
  if (company.country) parts.push(company.country);
  return parts.join(", ");
}

export function collectApprovedClientCertificateGroups(certs, invoiceRows, issueDate = new Date(), companiesByName = null) {
  const issueDateIso = toDateOnly(issueDate) || new Date().toISOString().slice(0, 10);
  const invoiceRowById = new Map((invoiceRows || []).map((row) => [row.id, row]));
  const groups = new Map();

  for (const cert of certs || []) {
    if (cert?.match?.status !== "approved") continue;

    const certificateRef = cert?.data?.uniqueNumber || cert?.filename || cert?.id || "certificate";
    const certificateId = cert?.id || null;

    for (const link of cert.match?.links || []) {
      const invoiceRow = invoiceRowById.get(link.invoice_row_id) || null;
      const clientName = String(link.customer || invoiceRow?.customer || "").trim();
      const airportCode = String(invoiceRow?.iata || link.iata || invoiceRow?.icao || link.icao || "").trim().toUpperCase();
      const month = toMonthKey(invoiceRow?.uplift_date || link.uplift_date || "");
      const groupKey = buildGroupKey(clientName, airportCode, month);
      const allocatedSaf = toNumber(link.allocated_m3) || 0;

      const existing =
        groups.get(groupKey) ||
        {
          group_key: groupKey,
          client_name: clientName,
          airport_code: airportCode,
          month,
          month_label: formatMonthLabel(month),
          issue_date: issueDateIso,
          issue_date_display: formatDateDisplay(issueDateIso),
          internal_reference: buildInternalReference({
            clientName,
            airportCode,
            month,
            issueDate: issueDateIso,
          }),
          client_address: companiesByName ? buildClientAddress(companiesByName.get(clientName.toLowerCase())) : "",
          total_saf_volume_m3: 0,
          source_certificate_refs: [],
          source_certificate_ids: [],
          source_invoice_row_ids: [],
          source_link_ids: [],
          approved_link_count: 0,
          matched_row_count: 0,
          validation_errors: [],
          _certificateRefs: new Set(),
          _certificateIds: new Set(),
          _invoiceRowIds: new Set(),
          _linkIds: new Set(),
        };

      if (!clientName) existing.validation_errors.push("Missing client name on approved allocation.");
      if (!airportCode) existing.validation_errors.push("Missing airport code on approved allocation.");
      if (!month) existing.validation_errors.push("Missing uplift month on approved allocation.");
      if (!invoiceRow) existing.validation_errors.push("Approved allocation references an invoice row that is not loaded.");

      existing.total_saf_volume_m3 = Number((existing.total_saf_volume_m3 + allocatedSaf).toFixed(6));
      existing.approved_link_count += 1;

      if (certificateRef && !existing._certificateRefs.has(certificateRef)) {
        existing._certificateRefs.add(certificateRef);
        existing.source_certificate_refs.push(certificateRef);
      }
      if (certificateId && !existing._certificateIds.has(certificateId)) {
        existing._certificateIds.add(certificateId);
        existing.source_certificate_ids.push(certificateId);
      }
      if (link?.id && !existing._linkIds.has(link.id)) {
        existing._linkIds.add(link.id);
        existing.source_link_ids.push(link.id);
      }
      if (link?.invoice_row_id && !existing._invoiceRowIds.has(link.invoice_row_id)) {
        existing._invoiceRowIds.add(link.invoice_row_id);
        existing.source_invoice_row_ids.push(link.invoice_row_id);
        existing.matched_row_count += 1;
      }

      groups.set(groupKey, existing);
    }
  }

  return [...groups.values()]
    .map((group) => {
      const validationErrors = [...new Set(group.validation_errors.filter(Boolean))];
      if (!group.source_certificate_refs.length) validationErrors.push("Missing source supplier certificate references.");
      if (!(group.total_saf_volume_m3 > 0)) validationErrors.push("Approved SAF volume must be greater than 0.");

      return {
        group_key: group.group_key,
        client_name: group.client_name,
        client_address: group.client_address,
        airport_code: group.airport_code,
        month: group.month,
        month_label: group.month_label,
        issue_date: group.issue_date,
        issue_date_display: group.issue_date_display,
        internal_reference: group.internal_reference,
        total_saf_volume_m3: Number(group.total_saf_volume_m3.toFixed(6)),
        source_certificate_refs: group.source_certificate_refs,
        source_certificate_ids: group.source_certificate_ids,
        source_invoice_row_ids: group.source_invoice_row_ids,
        source_link_ids: group.source_link_ids,
        approved_link_count: group.approved_link_count,
        matched_row_count: group.matched_row_count,
        validation_errors: validationErrors,
        can_generate: validationErrors.length === 0,
      };
    })
    .sort((left, right) => {
      return (
        String(left.client_name || "").localeCompare(String(right.client_name || "")) ||
        String(left.airport_code || "").localeCompare(String(right.airport_code || "")) ||
        String(left.month || "").localeCompare(String(right.month || ""))
      );
    });
}
