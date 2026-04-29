function toNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return Number(value.toFixed(6));
  const parsed = Number.parseFloat(String(value ?? "").replace(",", "."));
  return Number.isFinite(parsed) ? Number(parsed.toFixed(6)) : null;
}

function weightedAverage(entries) {
  let totalWeight = 0;
  let weightedSum = 0;
  for (const { value, weight } of entries) {
    const v = toNumber(value);
    const w = toNumber(weight);
    if (v === null || w === null || w <= 0) continue;
    weightedSum += v * w;
    totalWeight += w;
  }
  if (totalWeight <= 0) return null;
  return Number((weightedSum / totalWeight).toFixed(6));
}

function aggregateStringField(values) {
  const unique = [...new Set(values.map((v) => String(v ?? "").trim()).filter(Boolean))];
  if (unique.length === 0) return "";
  if (unique.length > 3) return "Mixed";
  return unique.join(", ");
}

function aggregateYesNo(values) {
  const normalized = values
    .map((v) => String(v ?? "").trim().toLowerCase())
    .filter(Boolean);
  if (normalized.length === 0) return "";
  if (normalized.every((v) => v.startsWith("y"))) return "Yes";
  if (normalized.every((v) => v.startsWith("n"))) return "No";
  return "Mixed";
}

function earliestDate(values) {
  const dates = values.map((v) => toDateOnly(v)).filter(Boolean).sort();
  return dates[0] || "";
}

function latestDate(values) {
  const dates = values.map((v) => toDateOnly(v)).filter(Boolean).sort();
  return dates[dates.length - 1] || "";
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
  const clientPart = slugify(clientName).slice(0, 32) || "CLIENT";
  const airportPart = slugify(airportCode).slice(0, 8) || "AIRPORT";
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
          _isccContributions: [],
          _dispatchDates: [],
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

      const data = cert?.data || {};
      const sourceVolume = toNumber(data.quantity);
      const sourceEnergy = toNumber(data.energyContent);
      let mjPerM3 =
        sourceVolume && sourceVolume > 0 && sourceEnergy !== null
          ? sourceEnergy / sourceVolume
          : null;
      // Sanity-check: SAF energy density is ~33,000-35,000 MJ/m3. If the ratio
      // is clearly outside a plausible range (e.g. source stored energyContent
      // as a per-kg rate or in GJ), drop it rather than show a bogus value.
      if (mjPerM3 !== null && (mjPerM3 < 10000 || mjPerM3 > 100000)) {
        mjPerM3 = null;
      }

      existing._isccContributions.push({
        weight: allocatedSaf,
        data,
        mjPerM3,
      });

      const dispatchDate = toDateOnly(invoiceRow?.uplift_date || link.uplift_date || "");
      if (dispatchDate) existing._dispatchDates.push(dispatchDate);

      groups.set(groupKey, existing);
    }
  }

  return [...groups.values()]
    .map((group) => {
      const validationErrors = [...new Set(group.validation_errors.filter(Boolean))];
      if (!group.source_certificate_refs.length) validationErrors.push("Missing source supplier certificate references.");
      if (!(group.total_saf_volume_m3 > 0)) validationErrors.push("Approved SAF volume must be greater than 0.");

      const contribs = group._isccContributions;
      const weightedFor = (field) =>
        weightedAverage(contribs.map((c) => ({ value: c.data?.[field], weight: c.weight })));
      const stringsFor = (field) => contribs.map((c) => c.data?.[field]);

      const ghgTotal = weightedFor("ghgTotal");
      const ghgEec = weightedFor("ghgEec");
      const ghgEl = weightedFor("ghgEl");
      const ghgEp = weightedFor("ghgEp");
      const ghgEtd = weightedFor("ghgEtd");
      const ghgEu = weightedFor("ghgEu");
      const ghgEccs = weightedFor("ghgEccs");

      const totalEnergyMj = contribs.reduce((acc, c) => {
        if (c.mjPerM3 === null || !(c.weight > 0)) return acc;
        return acc + c.weight * c.mjPerM3;
      }, 0);
      const energyMjPerM3 = weightedAverage(
        contribs.map((c) => ({ value: c.mjPerM3, weight: c.weight }))
      );

      const safTypeAgg = aggregateStringField(stringsFor("safType"));
      const productTypeAgg = aggregateStringField(stringsFor("productType"));
      const iscc = {
        // Prefer safType (HEFA, AtJ, FT) since productType on PoC docs is often
        // "JETA1" (the final blended jet fuel) rather than the SAF product.
        product_type: safTypeAgg || productTypeAgg,
        product_type_raw: productTypeAgg,
        saf_type: safTypeAgg,
        raw_material: aggregateStringField(stringsFor("rawMaterial")),
        raw_material_origin: aggregateStringField(stringsFor("rawMaterialOrigin")),
        production_country: aggregateStringField(stringsFor("productionCountry")),
        production_start_date: earliestDate(stringsFor("productionStartDate")),
        eu_red_compliant: aggregateYesNo(stringsFor("euRedCompliant")),
        iscc_compliant: aggregateYesNo(stringsFor("isccCompliant")),
        chain_of_custody: aggregateStringField(stringsFor("chainOfCustody")),
        energy_mj_per_m3: energyMjPerM3,
        energy_total_mj: totalEnergyMj > 0 ? Number(totalEnergyMj.toFixed(2)) : null,
        ghg_total: ghgTotal,
        ghg_eec: ghgEec,
        ghg_el: ghgEl,
        ghg_ep: ghgEp,
        ghg_etd: ghgEtd,
        ghg_eu: ghgEu,
        ghg_eccs: ghgEccs,
        ghg_saving_percent:
          ghgTotal === null ? null : Number((((94 - ghgTotal) / 94) * 100).toFixed(2)),
        fossil_comparator: 94,
        dispatch_date_min: earliestDate(group._dispatchDates),
        dispatch_date_max: latestDate(group._dispatchDates),
        supplier_cert_numbers: [...group.source_certificate_refs],
      };

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
        iscc,
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
