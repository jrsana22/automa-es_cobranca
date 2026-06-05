-- Migration: adiciona whatsapp_destinatario em automacoes
ALTER TABLE automacoes ADD COLUMN whatsapp_destinatario VARCHAR(20);

-- Preenche os clientes existentes
UPDATE automacoes SET whatsapp_destinatario = '5511950535686' WHERE id = 1;  -- Jardim Independência
UPDATE automacoes SET whatsapp_destinatario = '5521964482777' WHERE id = 3;  -- Austin 3 - Eduardo Santos
UPDATE automacoes SET whatsapp_destinatario = '5511934476544' WHERE id = 4;  -- Austin 2 - Jefferson Santos
UPDATE automacoes SET whatsapp_destinatario = '5511982381665' WHERE id = 6;  -- Regional Capitão - Carol e Gil
UPDATE automacoes SET whatsapp_destinatario = '5521988798730' WHERE id = 7;  -- Regional Madureira Shopping 02 - Euci Cabral
UPDATE automacoes SET whatsapp_destinatario = '5511917786665' WHERE id = 8;  -- Regional Santo André Centro - Rafael Neves
UPDATE automacoes SET whatsapp_destinatario = '5521974502059' WHERE id = 9;  -- Regional Nilo Peçanha
UPDATE automacoes SET whatsapp_destinatario = '5511982687617' WHERE id = 10; -- Regional Las Vegas
UPDATE automacoes SET whatsapp_destinatario = '5541996226415' WHERE id = 11; -- São José dos Pinhais 2
UPDATE automacoes SET whatsapp_destinatario = '5521969336728' WHERE id = 12; -- Regional Olaria
UPDATE automacoes SET whatsapp_destinatario = '5571987804910' WHERE id = 13; -- Regional Shopping São Cristóvão - Rafa Alves
UPDATE automacoes SET whatsapp_destinatario = '5527997146154' WHERE id = 14; -- Regional Cariacica
UPDATE automacoes SET whatsapp_destinatario = '5527997146154' WHERE id = 15; -- Regional Naque
