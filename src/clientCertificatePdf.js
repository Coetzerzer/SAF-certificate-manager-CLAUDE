import { PDFDocument, StandardFonts, rgb } from "pdf-lib";
import {
  formatClientCertificateDate,
  formatClientCertificateVolume,
} from "./clientCertificates.js";

const templateUrl = new URL("./assets/iscc-pos-template.pdf", import.meta.url).href;

function sanitizeForPdf(text) {
  return String(text ?? "").normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

function formatNumberOrEmpty(value, decimals = 2) {
  if (value === null || value === undefined || value === "") return "";
  const num = Number(value);
  if (!Number.isFinite(num)) return "";
  return num.toFixed(decimals);
}

function formatDateIso(value) {
  const text = String(value ?? "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const [y, m, d] = text.split("-");
  return `${d}/${m}/${y}`;
}

function formatDispatchRange(min, max) {
  const low = formatDateIso(min);
  const high = formatDateIso(max);
  if (!low && !high) return "";
  if (!high || low === high) return low;
  return `${low} - ${high}`;
}

const TEXT_BLACK = rgb(0, 0, 0);
const WHITE = rgb(1, 1, 1);

export async function generateClientCertificatePdf(group) {
  const bytes = await fetch(templateUrl).then((res) => res.arrayBuffer());
  const pdf = await PDFDocument.load(bytes);
  const pages = pdf.getPages();
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const bold = await pdf.embedFont(StandardFonts.HelveticaBold);

  const draw = (pageIndex, text, x, y, opts = {}) => {
    const value = sanitizeForPdf(text);
    if (!value) return;
    const page = pages[pageIndex];
    if (!page) return;
    page.drawText(value, {
      x,
      y,
      font: opts.bold ? bold : font,
      size: opts.size ?? 9,
      color: opts.color ?? TEXT_BLACK,
      maxWidth: opts.maxWidth,
      lineHeight: opts.lineHeight ?? 11,
    });
  };

  // Apply a compliance tick (EU RED / ISCC). When defaultTicked is true, the template
  // already draws a ⊠; we cover it with a white rectangle for "No"/"Mixed".
  // When defaultTicked is false, we draw an X for "Yes" (leave blank for No/Mixed).
  const applyComplianceTick = (pageIndex, value, y, defaultTicked) => {
    const yesX = 212;
    if (value === "Yes" && !defaultTicked) {
      draw(pageIndex, "X", yesX, y, { bold: true, size: 10 });
    } else if ((value === "No" || value === "Mixed") && defaultTicked) {
      pages[pageIndex].drawRectangle({
        x: yesX - 3, y: y - 2, width: 14, height: 13, color: WHITE,
      });
    }
  };

  const iscc = group.iscc || {};

  // --- Page 0 ---
  // Top block: unique number & issue date (black to match other filled values).
  draw(0, group.internal_reference, 230, 605, { bold: true, size: 9, maxWidth: 210 });
  draw(0, formatClientCertificateDate(group.issue_date), 230, 580);

  // Supplier block — Name and Address are large merged boxes.
  draw(0, "Titan Aviation Fuels SARL", 140, 540, { bold: true, maxWidth: 150 });
  draw(0, "8 rue du Bois-du-Lan, 1217 Meyrin, Switzerland", 145, 495, {
    size: 8,
    maxWidth: 150,
    lineHeight: 10,
  });
  // Supplier certificate number: sits in the merged input below the label (row 18).
  // Shrink font only when the ref is long enough to need it.
  const certStr = (iscc.supplier_cert_numbers || []).join(", ");
  const certSize = certStr.length > 35 ? 7 : 9;
  draw(0, certStr, 100, 443, {
    size: certSize,
    maxWidth: 195,
    lineHeight: certSize + 1,
  });

  // Recipient block
  draw(0, group.client_name, 345, 540, { bold: true, maxWidth: 170 });
  if (group.client_address) {
    draw(0, group.client_address, 350, 495, { size: 8, maxWidth: 165, lineHeight: 10 });
  }

  // Addresses of dispatch/receipt.
  // We write a custom receipt address, so cover the template's default ⊠ on
  // "Same as address of recipient" to avoid a contradictory state. Visual
  // calibration (via debug PDF): ⊠ glyph sits at PDF (~213, ~376).
  pages[0].drawRectangle({ x: 206, y: 370, width: 16, height: 14, color: WHITE });
  draw(0, `Airport ${group.airport_code || ""}`, 230, 383);
  draw(0, formatDispatchRange(iscc.dispatch_date_min, iscc.dispatch_date_max), 230, 348);

  // General information
  draw(0, iscc.product_type, 230, 311, { maxWidth: 260 });
  draw(0, iscc.raw_material, 230, 295, { maxWidth: 260 });
  draw(0, group.month_label || "", 230, 279, { maxWidth: 260 });
  draw(0, iscc.raw_material_origin, 230, 261, { maxWidth: 260 });

  // Quantity + Energy. Template already ticks ⊠ m³ by default, so no overlay needed.
  draw(0, formatClientCertificateVolume(group.total_saf_volume_m3), 250, 246, {
    bold: true,
    size: 10,
  });
  draw(
    0,
    iscc.energy_total_mj ? Math.round(iscc.energy_total_mj).toString() : "",
    230,
    230,
    { bold: true },
  );

  // Compliance ticks. Template defaults: EU RED ⊠ Yes, ISCC ⬜ Yes.
  applyComplianceTick(0, iscc.eu_red_compliant, 216, true);
  applyComplianceTick(0, iscc.iscc_compliant, 203, false);

  draw(0, iscc.chain_of_custody, 280, 185, { maxWidth: 200 });
  draw(0, iscc.production_country, 230, 170, { maxWidth: 260 });
  draw(0, formatDateIso(iscc.production_start_date), 230, 151);

  // --- Page 1 ---
  // Scope of certification — template default is already ⊠ Yes, no overlay needed.

  // GHG emission values. Fill missing components with "0.00" so the formula
  // E = Ei + Ep + Etd + Eu − Eccs visibly reconciles with the total.
  const hasAnyGhg =
    iscc.ghg_total !== null && iscc.ghg_total !== undefined;
  const ghgCell = (v) => (hasAnyGhg ? formatNumberOrEmpty(v, 2) || "0.00" : "");
  draw(1, ghgCell(iscc.ghg_eec), 122, 437);
  draw(1, ghgCell(iscc.ghg_ep), 158, 437);
  draw(1, ghgCell(iscc.ghg_etd), 200, 437);
  draw(1, ghgCell(iscc.ghg_eu), 240, 437);
  draw(1, ghgCell(iscc.ghg_eccs), 275, 437);
  // Total E (right side near "=" sign). Cover the template's "0" placeholder first.
  if (hasAnyGhg) {
    pages[1].drawRectangle({
      x: 428, y: 434, width: 30, height: 13, color: WHITE,
    });
    draw(1, formatNumberOrEmpty(iscc.ghg_total, 2), 430, 437, { bold: true });
  }

  // Ei breakdown (row y ~ 403). Leave blank when source data is unavailable.
  draw(1, formatNumberOrEmpty(iscc.ghg_eec, 2), 92, 403);
  draw(1, formatNumberOrEmpty(iscc.ghg_el, 2), 145, 403);

  // GHG saving % — overwrite the "100,0%" placeholder.
  const savingText =
    iscc.ghg_saving_percent === null || iscc.ghg_saving_percent === undefined
      ? ""
      : `${Number(iscc.ghg_saving_percent).toFixed(1)}%`;
  if (savingText) {
    pages[1].drawRectangle({
      x: 108, y: 353, width: 32, height: 11, color: WHITE,
    });
    draw(1, savingText, 112, 355, { bold: true });
  }

  return pdf.save();
}
