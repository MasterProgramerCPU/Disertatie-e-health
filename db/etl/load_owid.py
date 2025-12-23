#!/usr/bin/env python3
import argparse
import csv
import os
from contextlib import closing

import psycopg2
from dotenv import load_dotenv


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def parse_args():
    p = argparse.ArgumentParser(description="Incarca owid-covid-data.csv in schema COVID (tabele in romana)")
    p.add_argument("--csv-path", default="owid-covid-data.csv")
    p.add_argument("--schema", default="covid")
    p.add_argument("--env", default=".env", help="Calea catre fisierul .env cu variabile PG*")
    p.add_argument("--dsn", default=None, help="Ex: postgresql://user:pass@host:5432/db")
    p.add_argument("--host", default=None)
    p.add_argument("--port", default=None)
    p.add_argument("--dbname", default=None)
    p.add_argument("--user", default=None)
    p.add_argument("--password", default=None)
    p.add_argument("--sslmode", default=None)
    p.add_argument("--sslrootcert", default=None)
    p.add_argument("--keep-staging", action="store_true")
    return p.parse_args()


def get_conn(args):
    if args.dsn:
        return psycopg2.connect(args.dsn)
    # Prefer env (possibly loaded from .env), fall back to CLI flags
    host = args.host or os.getenv("PGHOST")
    port = args.port or os.getenv("PGPORT", "5432")
    dbname = args.dbname or os.getenv("PGDATABASE")
    user = args.user or os.getenv("PGUSER")
    password = args.password or os.getenv("PGPASSWORD")
    sslmode = args.sslmode or os.getenv("PGSSLMODE")
    sslrootcert = args.sslrootcert or os.getenv("PGSSLROOTCERT")

    dsn_parts = []
    if host:
        dsn_parts.append(f"host={host}")
    if port:
        dsn_parts.append(f"port={port}")
    if dbname:
        dsn_parts.append(f"dbname={dbname}")
    if user:
        dsn_parts.append(f"user={user}")
    if password:
        dsn_parts.append(f"password={password}")
    if sslmode:
        dsn_parts.append(f"sslmode={sslmode}")
    if sslrootcert:
        dsn_parts.append(f"sslrootcert={sslrootcert}")
    return psycopg2.connect(" ".join(dsn_parts))


def read_header(csv_path):
    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        header = next(r)
    return [h.strip() for h in header]


def create_schema_and_staging(cur, schema, header_cols):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {qident(schema)};")
    cols = ",\n  ".join(f"{qident(c)} TEXT" for c in header_cols)
    cur.execute(f"DROP TABLE IF EXISTS {qident(schema)}.stg_owid;")
    cur.execute(f"CREATE TABLE {qident(schema)}.stg_owid (\n  {cols}\n);")


def copy_to_staging(cur, schema, csv_path, header_cols):
    columns = ", ".join(qident(c) for c in header_cols)
    sql = f"COPY {qident(schema)}.stg_owid ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)"
    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(sql, f)


def create_temp_dedup(cur, schema, header_cols):
    score_cols = [c for c in header_cols if c not in ("iso_code", "date", "location")]
    score_expr = " + ".join([f"(CASE WHEN NULLIF({qident(c)}, '') IS NOT NULL THEN 1 ELSE 0 END)" for c in score_cols])
    cur.execute("DROP TABLE IF EXISTS stg_owid_dedup;")
    cur.execute(
        f"""
        CREATE TEMP TABLE stg_owid_dedup AS
        WITH scored AS (
          SELECT s.*, ({score_expr}) AS score
          FROM {qident(schema)}.stg_owid s
        ), ranked AS (
          SELECT scored.*, ROW_NUMBER() OVER (PARTITION BY iso_code, date ORDER BY score DESC) AS rn
          FROM scored
        )
        SELECT * FROM ranked WHERE rn = 1;
        """
    )


def upsert_locatii(cur, schema):
    cur.execute(
        f"""
        WITH latest AS (
          SELECT d.* FROM stg_owid_dedup d
          JOIN (
            SELECT iso_code, MAX(date) AS date
            FROM stg_owid_dedup
            GROUP BY iso_code
          ) m USING (iso_code, date)
        )
        INSERT INTO {qident(schema)}.locatii (
          cod_iso, denumire, continent, populatie, densitate_populatie, varsta_mediana,
          procent_65_plus, procent_70_plus, pib_pe_cap, saracie_extrema,
          rata_decese_cardiovasculare, prevalenta_diabet, fumatoare_proc, fumatori_proc,
          facilitati_spalare_maini, paturi_spital_la_mie, speranta_viata, indice_dezvoltare_umana
        )
        SELECT
          iso_code,
          location,
          continent,
          ROUND(NULLIF(population, '')::double precision)::bigint,
          NULLIF(population_density, '')::double precision,
          NULLIF(median_age, '')::double precision,
          NULLIF(aged_65_older, '')::double precision,
          NULLIF(aged_70_older, '')::double precision,
          NULLIF(gdp_per_capita, '')::double precision,
          NULLIF(extreme_poverty, '')::double precision,
          NULLIF(cardiovasc_death_rate, '')::double precision,
          NULLIF(diabetes_prevalence, '')::double precision,
          NULLIF(female_smokers, '')::double precision,
          NULLIF(male_smokers, '')::double precision,
          NULLIF(handwashing_facilities, '')::double precision,
          NULLIF(hospital_beds_per_thousand, '')::double precision,
          NULLIF(life_expectancy, '')::double precision,
          NULLIF(human_development_index, '')::double precision
        FROM latest
        ON CONFLICT (cod_iso) DO UPDATE SET
          denumire = COALESCE(EXCLUDED.denumire, {qident(schema)}.locatii.denumire),
          continent = COALESCE(EXCLUDED.continent, {qident(schema)}.locatii.continent),
          populatie = COALESCE(EXCLUDED.populatie, {qident(schema)}.locatii.populatie),
          densitate_populatie = COALESCE(EXCLUDED.densitate_populatie, {qident(schema)}.locatii.densitate_populatie),
          varsta_mediana = COALESCE(EXCLUDED.varsta_mediana, {qident(schema)}.locatii.varsta_mediana),
          procent_65_plus = COALESCE(EXCLUDED.procent_65_plus, {qident(schema)}.locatii.procent_65_plus),
          procent_70_plus = COALESCE(EXCLUDED.procent_70_plus, {qident(schema)}.locatii.procent_70_plus),
          pib_pe_cap = COALESCE(EXCLUDED.pib_pe_cap, {qident(schema)}.locatii.pib_pe_cap),
          saracie_extrema = COALESCE(EXCLUDED.saracie_extrema, {qident(schema)}.locatii.saracie_extrema),
          rata_decese_cardiovasculare = COALESCE(EXCLUDED.rata_decese_cardiovasculare, {qident(schema)}.locatii.rata_decese_cardiovasculare),
          prevalenta_diabet = COALESCE(EXCLUDED.prevalenta_diabet, {qident(schema)}.locatii.prevalenta_diabet),
          fumatoare_proc = COALESCE(EXCLUDED.fumatoare_proc, {qident(schema)}.locatii.fumatoare_proc),
          fumatori_proc = COALESCE(EXCLUDED.fumatori_proc, {qident(schema)}.locatii.fumatori_proc),
          facilitati_spalare_maini = COALESCE(EXCLUDED.facilitati_spalare_maini, {qident(schema)}.locatii.facilitati_spalare_maini),
          paturi_spital_la_mie = COALESCE(EXCLUDED.paturi_spital_la_mie, {qident(schema)}.locatii.paturi_spital_la_mie),
          speranta_viata = COALESCE(EXCLUDED.speranta_viata, {qident(schema)}.locatii.speranta_viata),
          indice_dezvoltare_umana = COALESCE(EXCLUDED.indice_dezvoltare_umana, {qident(schema)}.locatii.indice_dezvoltare_umana)
        ;
        """
    )


def upsert_cazuri(cur, schema):
    cur.execute(
        f"""
        INSERT INTO {qident(schema)}.cazuri_zilnice (
          cod_iso, data,
          total_cazuri, cazuri_noi, cazuri_noi_netezite,
          total_decese, decese_noi, decese_noi_netezite,
          total_cazuri_la_milion, cazuri_noi_la_milion, cazuri_noi_netezite_la_milion,
          total_decese_la_milion, decese_noi_la_milion, decese_noi_netezite_la_milion,
          rata_reproductie, indice_strictete
        )
        SELECT
          iso_code,
          NULLIF(date, '')::date,
          NULLIF(total_cases, '')::bigint,
          NULLIF(new_cases, '')::double precision,
          NULLIF(new_cases_smoothed, '')::double precision,
          NULLIF(total_deaths, '')::bigint,
          NULLIF(new_deaths, '')::double precision,
          NULLIF(new_deaths_smoothed, '')::double precision,
          NULLIF(total_cases_per_million, '')::double precision,
          NULLIF(new_cases_per_million, '')::double precision,
          NULLIF(new_cases_smoothed_per_million, '')::double precision,
          NULLIF(total_deaths_per_million, '')::double precision,
          NULLIF(new_deaths_per_million, '')::double precision,
          NULLIF(new_deaths_smoothed_per_million, '')::double precision,
          NULLIF(reproduction_rate, '')::double precision,
          NULLIF(stringency_index, '')::double precision
        FROM stg_owid_dedup
        ON CONFLICT (cod_iso, data) DO UPDATE SET
          total_cazuri = COALESCE(EXCLUDED.total_cazuri, {qident(schema)}.cazuri_zilnice.total_cazuri),
          cazuri_noi = COALESCE(EXCLUDED.cazuri_noi, {qident(schema)}.cazuri_zilnice.cazuri_noi),
          cazuri_noi_netezite = COALESCE(EXCLUDED.cazuri_noi_netezite, {qident(schema)}.cazuri_zilnice.cazuri_noi_netezite),
          total_decese = COALESCE(EXCLUDED.total_decese, {qident(schema)}.cazuri_zilnice.total_decese),
          decese_noi = COALESCE(EXCLUDED.decese_noi, {qident(schema)}.cazuri_zilnice.decese_noi),
          decese_noi_netezite = COALESCE(EXCLUDED.decese_noi_netezite, {qident(schema)}.cazuri_zilnice.decese_noi_netezite),
          total_cazuri_la_milion = COALESCE(EXCLUDED.total_cazuri_la_milion, {qident(schema)}.cazuri_zilnice.total_cazuri_la_milion),
          cazuri_noi_la_milion = COALESCE(EXCLUDED.cazuri_noi_la_milion, {qident(schema)}.cazuri_zilnice.cazuri_noi_la_milion),
          cazuri_noi_netezite_la_milion = COALESCE(EXCLUDED.cazuri_noi_netezite_la_milion, {qident(schema)}.cazuri_zilnice.cazuri_noi_netezite_la_milion),
          total_decese_la_milion = COALESCE(EXCLUDED.total_decese_la_milion, {qident(schema)}.cazuri_zilnice.total_decese_la_milion),
          decese_noi_la_milion = COALESCE(EXCLUDED.decese_noi_la_milion, {qident(schema)}.cazuri_zilnice.decese_noi_la_milion),
          decese_noi_netezite_la_milion = COALESCE(EXCLUDED.decese_noi_netezite_la_milion, {qident(schema)}.cazuri_zilnice.decese_noi_netezite_la_milion),
          rata_reproductie = COALESCE(EXCLUDED.rata_reproductie, {qident(schema)}.cazuri_zilnice.rata_reproductie),
          indice_strictete = COALESCE(EXCLUDED.indice_strictete, {qident(schema)}.cazuri_zilnice.indice_strictete)
        ;
        """
    )


def upsert_testari(cur, schema):
    cur.execute(
        f"""
        INSERT INTO {qident(schema)}.testari_zilnice (
          cod_iso, data,
          total_testari, testari_noi, total_testari_la_mie, testari_noi_la_mie,
          testari_noi_netezite, testari_noi_netezite_la_mie, rata_pozitivitate, teste_pe_caz, unitati_testare
        )
        SELECT
          iso_code,
          NULLIF(date, '')::date,
          NULLIF(total_tests, '')::bigint,
          NULLIF(new_tests, '')::double precision,
          NULLIF(total_tests_per_thousand, '')::double precision,
          NULLIF(new_tests_per_thousand, '')::double precision,
          NULLIF(new_tests_smoothed, '')::double precision,
          NULLIF(new_tests_smoothed_per_thousand, '')::double precision,
          NULLIF(positive_rate, '')::double precision,
          NULLIF(tests_per_case, '')::double precision,
          NULLIF(tests_units, '')
        FROM stg_owid_dedup
        ON CONFLICT (cod_iso, data) DO UPDATE SET
          total_testari = COALESCE(EXCLUDED.total_testari, {qident(schema)}.testari_zilnice.total_testari),
          testari_noi = COALESCE(EXCLUDED.testari_noi, {qident(schema)}.testari_zilnice.testari_noi),
          total_testari_la_mie = COALESCE(EXCLUDED.total_testari_la_mie, {qident(schema)}.testari_zilnice.total_testari_la_mie),
          testari_noi_la_mie = COALESCE(EXCLUDED.testari_noi_la_mie, {qident(schema)}.testari_zilnice.testari_noi_la_mie),
          testari_noi_netezite = COALESCE(EXCLUDED.testari_noi_netezite, {qident(schema)}.testari_zilnice.testari_noi_netezite),
          testari_noi_netezite_la_mie = COALESCE(EXCLUDED.testari_noi_netezite_la_mie, {qident(schema)}.testari_zilnice.testari_noi_netezite_la_mie),
          rata_pozitivitate = COALESCE(EXCLUDED.rata_pozitivitate, {qident(schema)}.testari_zilnice.rata_pozitivitate),
          teste_pe_caz = COALESCE(EXCLUDED.teste_pe_caz, {qident(schema)}.testari_zilnice.teste_pe_caz),
          unitati_testare = COALESCE(EXCLUDED.unitati_testare, {qident(schema)}.testari_zilnice.unitati_testare)
        ;
        """
    )


def upsert_vaccinari(cur, schema):
    cur.execute(
        f"""
        INSERT INTO {qident(schema)}.vaccinari_zilnice (
          cod_iso, data,
          total_vaccinari, persoane_vaccinate, persoane_vaccinate_complet, total_boostere,
          vaccinari_noi, vaccinari_noi_netezite,
          total_vaccinari_la_suta, persoane_vaccinate_la_suta, persoane_vaccinate_complet_la_suta, total_boostere_la_suta,
          vaccinari_noi_netezite_la_milion, persoane_noi_vaccinate_netezite, persoane_noi_vaccinate_netezite_la_suta
        )
        SELECT
          iso_code,
          NULLIF(date, '')::date,
          NULLIF(total_vaccinations, '')::bigint,
          NULLIF(people_vaccinated, '')::bigint,
          NULLIF(people_fully_vaccinated, '')::bigint,
          NULLIF(total_boosters, '')::bigint,
          NULLIF(new_vaccinations, '')::double precision,
          NULLIF(new_vaccinations_smoothed, '')::double precision,
          NULLIF(total_vaccinations_per_hundred, '')::double precision,
          NULLIF(people_vaccinated_per_hundred, '')::double precision,
          NULLIF(people_fully_vaccinated_per_hundred, '')::double precision,
          NULLIF(total_boosters_per_hundred, '')::double precision,
          NULLIF(new_vaccinations_smoothed_per_million, '')::double precision,
          NULLIF(new_people_vaccinated_smoothed, '')::double precision,
          NULLIF(new_people_vaccinated_smoothed_per_hundred, '')::double precision
        FROM stg_owid_dedup
        ON CONFLICT (cod_iso, data) DO UPDATE SET
          total_vaccinari = COALESCE(EXCLUDED.total_vaccinari, {qident(schema)}.vaccinari_zilnice.total_vaccinari),
          persoane_vaccinate = COALESCE(EXCLUDED.persoane_vaccinate, {qident(schema)}.vaccinari_zilnice.persoane_vaccinate),
          persoane_vaccinate_complet = COALESCE(EXCLUDED.persoane_vaccinate_complet, {qident(schema)}.vaccinari_zilnice.persoane_vaccinate_complet),
          total_boostere = COALESCE(EXCLUDED.total_boostere, {qident(schema)}.vaccinari_zilnice.total_boostere),
          vaccinari_noi = COALESCE(EXCLUDED.vaccinari_noi, {qident(schema)}.vaccinari_zilnice.vaccinari_noi),
          vaccinari_noi_netezite = COALESCE(EXCLUDED.vaccinari_noi_netezite, {qident(schema)}.vaccinari_zilnice.vaccinari_noi_netezite),
          total_vaccinari_la_suta = COALESCE(EXCLUDED.total_vaccinari_la_suta, {qident(schema)}.vaccinari_zilnice.total_vaccinari_la_suta),
          persoane_vaccinate_la_suta = COALESCE(EXCLUDED.persoane_vaccinate_la_suta, {qident(schema)}.vaccinari_zilnice.persoane_vaccinate_la_suta),
          persoane_vaccinate_complet_la_suta = COALESCE(EXCLUDED.persoane_vaccinate_complet_la_suta, {qident(schema)}.vaccinari_zilnice.persoane_vaccinate_complet_la_suta),
          total_boostere_la_suta = COALESCE(EXCLUDED.total_boostere_la_suta, {qident(schema)}.vaccinari_zilnice.total_boostere_la_suta),
          vaccinari_noi_netezite_la_milion = COALESCE(EXCLUDED.vaccinari_noi_netezite_la_milion, {qident(schema)}.vaccinari_zilnice.vaccinari_noi_netezite_la_milion),
          persoane_noi_vaccinate_netezite = COALESCE(EXCLUDED.persoane_noi_vaccinate_netezite, {qident(schema)}.vaccinari_zilnice.persoane_noi_vaccinate_netezite),
          persoane_noi_vaccinate_netezite_la_suta = COALESCE(EXCLUDED.persoane_noi_vaccinate_netezite_la_suta, {qident(schema)}.vaccinari_zilnice.persoane_noi_vaccinate_netezite_la_suta)
        ;
        """
    )


def upsert_spitalizari(cur, schema):
    cur.execute(
        f"""
        INSERT INTO {qident(schema)}.spitalizari_zilnice (
          cod_iso, data,
          pacienti_uti, pacienti_uti_la_milion, pacienti_spital, pacienti_spital_la_milion,
          internari_uti_saptamanale, internari_uti_saptamanale_la_milion,
          internari_spital_saptamanale, internari_spital_saptamanale_la_milion
        )
        SELECT
          iso_code,
          NULLIF(date, '')::date,
          NULLIF(icu_patients, '')::double precision,
          NULLIF(icu_patients_per_million, '')::double precision,
          NULLIF(hosp_patients, '')::double precision,
          NULLIF(hosp_patients_per_million, '')::double precision,
          NULLIF(weekly_icu_admissions, '')::double precision,
          NULLIF(weekly_icu_admissions_per_million, '')::double precision,
          NULLIF(weekly_hosp_admissions, '')::double precision,
          NULLIF(weekly_hosp_admissions_per_million, '')::double precision
        FROM stg_owid_dedup
        ON CONFLICT (cod_iso, data) DO UPDATE SET
          pacienti_uti = COALESCE(EXCLUDED.pacienti_uti, {qident(schema)}.spitalizari_zilnice.pacienti_uti),
          pacienti_uti_la_milion = COALESCE(EXCLUDED.pacienti_uti_la_milion, {qident(schema)}.spitalizari_zilnice.pacienti_uti_la_milion),
          pacienti_spital = COALESCE(EXCLUDED.pacienti_spital, {qident(schema)}.spitalizari_zilnice.pacienti_spital),
          pacienti_spital_la_milion = COALESCE(EXCLUDED.pacienti_spital_la_milion, {qident(schema)}.spitalizari_zilnice.pacienti_spital_la_milion),
          internari_uti_saptamanale = COALESCE(EXCLUDED.internari_uti_saptamanale, {qident(schema)}.spitalizari_zilnice.internari_uti_saptamanale),
          internari_uti_saptamanale_la_milion = COALESCE(EXCLUDED.internari_uti_saptamanale_la_milion, {qident(schema)}.spitalizari_zilnice.internari_uti_saptamanale_la_milion),
          internari_spital_saptamanale = COALESCE(EXCLUDED.internari_spital_saptamanale, {qident(schema)}.spitalizari_zilnice.internari_spital_saptamanale),
          internari_spital_saptamanale_la_milion = COALESCE(EXCLUDED.internari_spital_saptamanale_la_milion, {qident(schema)}.spitalizari_zilnice.internari_spital_saptamanale_la_milion)
        ;
        """
    )


def upsert_mortalitate(cur, schema):
    cur.execute(
        f"""
        INSERT INTO {qident(schema)}.mortalitate_exces_zilnica (
          cod_iso, data,
          mortalitate_exces_cumulata_absoluta, mortalitate_exces_cumulata,
          mortalitate_exces, mortalitate_exces_cumulata_la_milion
        )
        SELECT
          iso_code,
          NULLIF(date, '')::date,
          NULLIF(excess_mortality_cumulative_absolute, '')::double precision,
          NULLIF(excess_mortality_cumulative, '')::double precision,
          NULLIF(excess_mortality, '')::double precision,
          NULLIF(excess_mortality_cumulative_per_million, '')::double precision
        FROM stg_owid_dedup
        ON CONFLICT (cod_iso, data) DO UPDATE SET
          mortalitate_exces_cumulata_absoluta = COALESCE(EXCLUDED.mortalitate_exces_cumulata_absoluta, {qident(schema)}.mortalitate_exces_zilnica.mortalitate_exces_cumulata_absoluta),
          mortalitate_exces_cumulata = COALESCE(EXCLUDED.mortalitate_exces_cumulata, {qident(schema)}.mortalitate_exces_zilnica.mortalitate_exces_cumulata),
          mortalitate_exces = COALESCE(EXCLUDED.mortalitate_exces, {qident(schema)}.mortalitate_exces_zilnica.mortalitate_exces),
          mortalitate_exces_cumulata_la_milion = COALESCE(EXCLUDED.mortalitate_exces_cumulata_la_milion, {qident(schema)}.mortalitate_exces_zilnica.mortalitate_exces_cumulata_la_milion)
        ;
        """
    )


def run():
    args = parse_args()
    if args.env:
        load_dotenv(args.env)
    header = read_header(args.csv_path)
    with closing(get_conn(args)) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            create_schema_and_staging(cur, args.schema, header)
            copy_to_staging(cur, args.schema, args.csv_path, header)
            create_temp_dedup(cur, args.schema, header)
            upsert_locatii(cur, args.schema)
            upsert_cazuri(cur, args.schema)
            upsert_testari(cur, args.schema)
            upsert_vaccinari(cur, args.schema)
            upsert_spitalizari(cur, args.schema)
            upsert_mortalitate(cur, args.schema)
            if not args.keep_staging:
                cur.execute(f"DROP TABLE IF EXISTS {qident(args.schema)}.stg_owid;")
        conn.commit()


if __name__ == "__main__":
    run()
 
