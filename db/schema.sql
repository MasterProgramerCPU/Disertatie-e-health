CREATE SCHEMA IF NOT EXISTS covid;

CREATE TABLE IF NOT EXISTS covid.locatii (
  cod_iso TEXT PRIMARY KEY,
  denumire TEXT NOT NULL,
  continent TEXT,
  populatie BIGINT,
  densitate_populatie DOUBLE PRECISION,
  varsta_mediana DOUBLE PRECISION,
  procent_65_plus DOUBLE PRECISION,
  procent_70_plus DOUBLE PRECISION,
  pib_pe_cap DOUBLE PRECISION,
  saracie_extrema DOUBLE PRECISION,
  rata_decese_cardiovasculare DOUBLE PRECISION,
  prevalenta_diabet DOUBLE PRECISION,
  fumatoare_proc DOUBLE PRECISION,
  fumatori_proc DOUBLE PRECISION,
  facilitati_spalare_maini DOUBLE PRECISION,
  paturi_spital_la_mie DOUBLE PRECISION,
  speranta_viata DOUBLE PRECISION,
  indice_dezvoltare_umana DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS covid.cazuri_zilnice (
  cod_iso TEXT NOT NULL REFERENCES covid.locatii(cod_iso),
  data DATE NOT NULL,
  total_cazuri BIGINT,
  cazuri_noi DOUBLE PRECISION,
  cazuri_noi_netezite DOUBLE PRECISION,
  total_decese BIGINT,
  decese_noi DOUBLE PRECISION,
  decese_noi_netezite DOUBLE PRECISION,
  total_cazuri_la_milion DOUBLE PRECISION,
  cazuri_noi_la_milion DOUBLE PRECISION,
  cazuri_noi_netezite_la_milion DOUBLE PRECISION,
  total_decese_la_milion DOUBLE PRECISION,
  decese_noi_la_milion DOUBLE PRECISION,
  decese_noi_netezite_la_milion DOUBLE PRECISION,
  rata_reproductie DOUBLE PRECISION,
  indice_strictete DOUBLE PRECISION,
  PRIMARY KEY (cod_iso, data)
);

CREATE TABLE IF NOT EXISTS covid.spitalizari_zilnice (
  cod_iso TEXT NOT NULL REFERENCES covid.locatii(cod_iso),
  data DATE NOT NULL,
  pacienti_uti DOUBLE PRECISION,
  pacienti_uti_la_milion DOUBLE PRECISION,
  pacienti_spital DOUBLE PRECISION,
  pacienti_spital_la_milion DOUBLE PRECISION,
  internari_uti_saptamanale DOUBLE PRECISION,
  internari_uti_saptamanale_la_milion DOUBLE PRECISION,
  internari_spital_saptamanale DOUBLE PRECISION,
  internari_spital_saptamanale_la_milion DOUBLE PRECISION,
  PRIMARY KEY (cod_iso, data)
);

CREATE TABLE IF NOT EXISTS covid.testari_zilnice (
  cod_iso TEXT NOT NULL REFERENCES covid.locatii(cod_iso),
  data DATE NOT NULL,
  total_testari BIGINT,
  testari_noi DOUBLE PRECISION,
  total_testari_la_mie DOUBLE PRECISION,
  testari_noi_la_mie DOUBLE PRECISION,
  testari_noi_netezite DOUBLE PRECISION,
  testari_noi_netezite_la_mie DOUBLE PRECISION,
  rata_pozitivitate DOUBLE PRECISION,
  teste_pe_caz DOUBLE PRECISION,
  unitati_testare TEXT,
  PRIMARY KEY (cod_iso, data)
);

CREATE TABLE IF NOT EXISTS covid.vaccinari_zilnice (
  cod_iso TEXT NOT NULL REFERENCES covid.locatii(cod_iso),
  data DATE NOT NULL,
  total_vaccinari BIGINT,
  persoane_vaccinate BIGINT,
  persoane_vaccinate_complet BIGINT,
  total_boostere BIGINT,
  vaccinari_noi DOUBLE PRECISION,
  vaccinari_noi_netezite DOUBLE PRECISION,
  total_vaccinari_la_suta DOUBLE PRECISION,
  persoane_vaccinate_la_suta DOUBLE PRECISION,
  persoane_vaccinate_complet_la_suta DOUBLE PRECISION,
  total_boostere_la_suta DOUBLE PRECISION,
  vaccinari_noi_netezite_la_milion DOUBLE PRECISION,
  persoane_noi_vaccinate_netezite DOUBLE PRECISION,
  persoane_noi_vaccinate_netezite_la_suta DOUBLE PRECISION,
  PRIMARY KEY (cod_iso, data)
);

CREATE TABLE IF NOT EXISTS covid.mortalitate_exces_zilnica (
  cod_iso TEXT NOT NULL REFERENCES covid.locatii(cod_iso),
  data DATE NOT NULL,
  mortalitate_exces_cumulata_absoluta DOUBLE PRECISION,
  mortalitate_exces_cumulata DOUBLE PRECISION,
  mortalitate_exces DOUBLE PRECISION,
  mortalitate_exces_cumulata_la_milion DOUBLE PRECISION,
  PRIMARY KEY (cod_iso, data)
);
