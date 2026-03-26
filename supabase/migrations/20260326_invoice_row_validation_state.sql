alter table public.invoice_rows
  add column if not exists validation_note text;
