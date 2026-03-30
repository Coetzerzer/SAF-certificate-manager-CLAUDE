import { PDFDocument, StandardFonts, rgb } from "pdf-lib";
import {
  formatClientCertificateDate,
  formatClientCertificateMonth,
  formatClientCertificateVolume,
} from "./clientCertificates.js";

const PAGE = {
  width: 842,
  height: 595,
  margin: 42,
};

function drawLabelValue(page, font, boldFont, x, y, label, value, options = {}) {
  const labelWidth = options.labelWidth ?? 180;
  const valueWidth = options.valueWidth ?? 220;
  page.drawText(label, {
    x,
    y,
    font,
    size: 9,
    color: rgb(0.31, 0.50, 0.63),
  });
  page.drawText(String(value ?? ""), {
    x: x + labelWidth,
    y,
    font: options.bold ? boldFont : font,
    size: options.size ?? 10,
    color: options.color ?? rgb(0.09, 0.15, 0.22),
    maxWidth: valueWidth,
    lineHeight: 12,
  });
}

export async function generateClientCertificatePdf(group) {
  const pdf = await PDFDocument.create();
  const page = pdf.addPage([PAGE.width, PAGE.height]);
  const font = await pdf.embedFont(StandardFonts.Helvetica);
  const boldFont = await pdf.embedFont(StandardFonts.HelveticaBold);

  page.drawRectangle({
    x: 0,
    y: PAGE.height - 64,
    width: PAGE.width,
    height: 64,
    color: rgb(0.02, 0.10, 0.20),
  });

  page.drawText("TITAN AVIATION FUELS SARL", {
    x: PAGE.margin,
    y: PAGE.height - 34,
    font: boldFont,
    size: 18,
    color: rgb(0.00, 0.75, 1.00),
  });

  page.drawText("Proof of Sustainability (PoS) Customer Allocation Certificate", {
    x: PAGE.margin,
    y: PAGE.height - 54,
    font,
    size: 10,
    color: rgb(0.83, 0.90, 0.95),
  });

  page.drawRectangle({
    x: PAGE.margin,
    y: PAGE.height - 142,
    width: PAGE.width - PAGE.margin * 2,
    height: 54,
    borderWidth: 1,
    borderColor: rgb(0.05, 0.19, 0.38),
  });

  drawLabelValue(page, font, boldFont, PAGE.margin + 14, PAGE.height - 108, "Certificate Number", group.internal_reference, {
    bold: true,
    labelWidth: 120,
    valueWidth: 220,
  });
  drawLabelValue(page, font, boldFont, PAGE.margin + 420, PAGE.height - 108, "Issue Date", formatClientCertificateDate(group.issue_date), {
    bold: true,
    labelWidth: 70,
    valueWidth: 100,
  });

  const leftX = PAGE.margin;
  const topY = PAGE.height - 174;
  const columnWidth = (PAGE.width - PAGE.margin * 2 - 16) / 2;

  page.drawRectangle({
    x: leftX,
    y: topY - 108,
    width: columnWidth,
    height: 108,
    borderWidth: 1,
    borderColor: rgb(0.05, 0.19, 0.38),
  });
  page.drawRectangle({
    x: leftX + columnWidth + 16,
    y: topY - 108,
    width: columnWidth,
    height: 108,
    borderWidth: 1,
    borderColor: rgb(0.05, 0.19, 0.38),
  });

  page.drawText("Supplier", {
    x: leftX + 14,
    y: topY - 18,
    font: boldFont,
    size: 10,
    color: rgb(0.00, 0.75, 1.00),
  });
  page.drawText("Recipient", {
    x: leftX + columnWidth + 30,
    y: topY - 18,
    font: boldFont,
    size: 10,
    color: rgb(0.00, 0.75, 1.00),
  });

  drawLabelValue(page, font, boldFont, leftX + 14, topY - 40, "Name", "Titan Aviation Fuels SARL", {
    bold: true,
    labelWidth: 60,
    valueWidth: columnWidth - 90,
  });
  drawLabelValue(page, font, boldFont, leftX + 14, topY - 60, "Address", "8 rue du Bois-du-Lan, 1217 Meyrin, Switzerland", {
    labelWidth: 60,
    valueWidth: columnWidth - 90,
    size: 9,
  });
  drawLabelValue(page, font, boldFont, leftX + 14, topY - 80, "System", "ISCC EU", {
    labelWidth: 60,
    valueWidth: columnWidth - 90,
  });

  drawLabelValue(page, font, boldFont, leftX + columnWidth + 30, topY - 40, "Name", group.client_name, {
    bold: true,
    labelWidth: 60,
    valueWidth: columnWidth - 90,
  });
  drawLabelValue(page, font, boldFont, leftX + columnWidth + 30, topY - 60, "Airport", group.airport_code, {
    labelWidth: 60,
    valueWidth: columnWidth - 90,
  });
  drawLabelValue(page, font, boldFont, leftX + columnWidth + 30, topY - 80, "Period", formatClientCertificateMonth(group.month), {
    labelWidth: 60,
    valueWidth: columnWidth - 90,
  });

  const summaryY = topY - 140;
  page.drawRectangle({
    x: PAGE.margin,
    y: summaryY - 118,
    width: PAGE.width - PAGE.margin * 2,
    height: 118,
    borderWidth: 1,
    borderColor: rgb(0.05, 0.19, 0.38),
  });

  page.drawText("Approved SAF Allocation Summary", {
    x: PAGE.margin + 14,
    y: summaryY - 18,
    font: boldFont,
    size: 11,
    color: rgb(0.00, 0.75, 1.00),
  });

  drawLabelValue(page, font, boldFont, PAGE.margin + 14, summaryY - 44, "Customer", group.client_name, {
    labelWidth: 120,
    valueWidth: 260,
  });
  drawLabelValue(page, font, boldFont, PAGE.margin + 14, summaryY - 64, "Airport", group.airport_code, {
    labelWidth: 120,
    valueWidth: 120,
  });
  drawLabelValue(page, font, boldFont, PAGE.margin + 14, summaryY - 84, "Period", formatClientCertificateMonth(group.month), {
    labelWidth: 120,
    valueWidth: 160,
  });
  drawLabelValue(page, font, boldFont, PAGE.margin + 14, summaryY - 104, "SAF Volume", `${formatClientCertificateVolume(group.total_saf_volume_m3)} m3`, {
    bold: true,
    labelWidth: 120,
    valueWidth: 140,
    color: rgb(0.00, 0.58, 0.31),
  });

  const sourcesY = summaryY - 150;
  page.drawRectangle({
    x: PAGE.margin,
    y: sourcesY - 116,
    width: PAGE.width - PAGE.margin * 2,
    height: 116,
    borderWidth: 1,
    borderColor: rgb(0.05, 0.19, 0.38),
  });

  page.drawText("Traceability", {
    x: PAGE.margin + 14,
    y: sourcesY - 18,
    font: boldFont,
    size: 11,
    color: rgb(0.00, 0.75, 1.00),
  });

  drawLabelValue(page, font, boldFont, PAGE.margin + 14, sourcesY - 44, "Approved linked rows", String(group.matched_row_count), {
    labelWidth: 140,
    valueWidth: 60,
  });
  drawLabelValue(page, font, boldFont, PAGE.margin + 14, sourcesY - 64, "Approved link records", String(group.approved_link_count), {
    labelWidth: 140,
    valueWidth: 60,
  });
  drawLabelValue(
    page,
    font,
    boldFont,
    PAGE.margin + 14,
    sourcesY - 96,
    "Source Certificates",
    (group.source_certificate_refs || []).join(", "),
    {
      labelWidth: 140,
      valueWidth: PAGE.width - PAGE.margin * 2 - 170,
      size: 9,
    }
  );

  page.drawText(
    "This customer certificate reflects only approved SAF allocations. SAF volume is calculated from approved allocated links and is not derived from the supplier certificate total.",
    {
      x: PAGE.margin,
      y: 42,
      font,
      size: 9,
      color: rgb(0.31, 0.50, 0.63),
      maxWidth: PAGE.width - PAGE.margin * 2,
      lineHeight: 12,
    }
  );

  return pdf.save();
}
