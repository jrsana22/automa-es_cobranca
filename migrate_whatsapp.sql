-- Migration: adiciona whatsapp_destinatario em automacoes
ALTER TABLE automacoes ADD COLUMN whatsapp_destinatario VARCHAR(20);
