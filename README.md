# Disertatie e-health

Repository pentru modulele utilizate in lucrarea de disertatie pe tema e-health: seturi de date, schema BD si unelte auxiliare.


## Descriere proiect
Proiectul centralizeaza date COVID-19 (sursa OWID) si defineste o schema relationala normalizata pentru analiza la nivel de tara/continent, precum si unelte ajutatoare.

## Structura repository
- `db/schema.sql` – schema BD in limba romana (PostgreSQL), fara indexuri/comentarii.
- `db/etl/` – scripturi ETL si `requirements.txt` pentru Python.
- `owid-covid-data.csv` – setul de date OWID (Our World in Data) folosit la incarcare.
- `db_barrel/` – utilitar optional pentru vizualizarea topologiei si schemelor PostgreSQL (nu este necesar pentru incarcare/analiza datelor).
- `.env` – variabilele de conexiune PostgreSQL folosite de ETL (PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE, PGSSLMODE, PGSSLROOTCERT).

## Seturi de date
- OWID COVID-19 (`owid-covid-data.csv`)
  - Perioada: 2020-01-01 … 2024-08-14
  - 67 coloane, granularitate zilnica pe locatie (`iso_code`, `date`)
  - Include: cazuri/decese, testare, vaccinare, spitalizari, demografie si indicatori economici

## Baze de date
Au fost definite doua baze (nume orientative):
- `ehealth_live` – pentru date operationale folosite de aplicatii
- `ehealth_train` – pentru date de antrenare/analiza

Motor recomandat: PostgreSQL.

## Schema bazelor de date

Schema: `covid`

Tabele:

- `locatii` — dimensiune
  - Cheie: `cod_iso` (PK)
  - Campuri: `denumire`, `continent`, `populatie`, `densitate_populatie`, `varsta_mediana`, `procent_65_plus`, `procent_70_plus`, `pib_pe_cap`, `saracie_extrema`, `rata_decese_cardiovasculare`, `prevalenta_diabet`, `fumatoare_proc`, `fumatori_proc`, `facilitati_spalare_maini`, `paturi_spital_la_mie`, `speranta_viata`, `indice_dezvoltare_umana`

- `cazuri_zilnice` — fapt
  - Cheie: (`cod_iso`, `data`)
  - Campuri: `total_cazuri`, `cazuri_noi`, `cazuri_noi_netezite`, `total_decese`, `decese_noi`, `decese_noi_netezite`, `total_cazuri_la_milion`, `cazuri_noi_la_milion`, `cazuri_noi_netezite_la_milion`, `total_decese_la_milion`, `decese_noi_la_milion`, `decese_noi_netezite_la_milion`, `rata_reproductie`, `indice_strictete`

- `testari_zilnice` — fapt
  - Cheie: (`cod_iso`, `data`)
  - Campuri: `total_testari`, `testari_noi`, `total_testari_la_mie`, `testari_noi_la_mie`, `testari_noi_netezite`, `testari_noi_netezite_la_mie`, `rata_pozitivitate`, `teste_pe_caz`, `unitati_testare`

- `vaccinari_zilnice` — fapt
  - Cheie: (`cod_iso`, `data`)
  - Campuri: `total_vaccinari`, `persoane_vaccinate`, `persoane_vaccinate_complet`, `total_boostere`, `vaccinari_noi`, `vaccinari_noi_netezite`, `total_vaccinari_la_suta`, `persoane_vaccinate_la_suta`, `persoane_vaccinate_complet_la_suta`, `total_boostere_la_suta`, `vaccinari_noi_netezite_la_milion`, `persoane_noi_vaccinate_netezite`, `persoane_noi_vaccinate_netezite_la_suta`

- `spitalizari_zilnice` — fapt
  - Cheie: (`cod_iso`, `data`)
  - Campuri: `pacienti_uti`, `pacienti_uti_la_milion`, `pacienti_spital`, `pacienti_spital_la_milion`, `internari_uti_saptamanale`, `internari_uti_saptamanale_la_milion`, `internari_spital_saptamanale`, `internari_spital_saptamanale_la_milion`

- `mortalitate_exces_zilnica` — fapt
  - Cheie: (`cod_iso`, `data`)
  - Campuri: `mortalitate_exces_cumulata_absoluta`, `mortalitate_exces_cumulata`, `mortalitate_exces`, `mortalitate_exces_cumulata_la_milion`



## Script ETL

- `db/etl/load_owid.py` — script Python care incarca CSV-ul in PostgreSQL (schema `covid`), citind configurarea din `.env`; efectueaza staging, deduplicare pe (`iso_code`, `date`) si upsert in tabelele `locatii`, `cazuri_zilnice`, `testari_zilnice`, `vaccinari_zilnice`, `spitalizari_zilnice`, `mortalitate_exces_zilnica`.

## Rulare ETL

1. Instaleaza dependintele Python:
   ```bash
   python3 -m venv .venv
   . .venv/bin/activate
   pip install -r db/etl/requirements.txt
   ```
2. Verifica `.env` (valori implicite setate pentru conexiune la BD; modifica la nevoie `PGDATABASE`, `PGHOST`, etc.).
3. Ruleaza incarcarea:
   ```bash
   python db/etl/load_owid.py --csv-path owid-covid-data.csv
   ```
