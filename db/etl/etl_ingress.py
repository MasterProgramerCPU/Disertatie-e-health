#!/usr/bin/env python3
"""
ETL pentru incarcarea dataset-ului OWID COVID-19 in PostgreSQL folosind doar configuratie din .env.

Ce face scriptul:
- Citeste configuratia din .env: conexiunea PG* (PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE, PGSSLMODE, PGSSLROOTCERT),
  schema tinta (COVID_SCHEMA) si calea catre CSV (CSV_PATH).
- Creeaza schema si o masa de staging identica cu header-ul CSV (toate coloanele TEXT pentru incarcare robusta).
- Incarca rapid CSV-ul in staging prin COPY FROM STDIN.
- Deduplica la nivel (iso_code, date) alegand randul cu cele mai multe valori nenule.
- Populeaza tabelele normalizate din schema (locatii, cazuri_zilnice, testari_zilnice, vaccinari_zilnice, spitalizari_zilnice, mortalitate_exces_zilnica)
  prin INSERT .. ON CONFLICT .. DO UPDATE cu COALESCE pentru a pastra valorile existente cand cele noi sunt NULL.
- Curata staging-ul la final (implicit).

Scriptul raporteaza progresul in consola pentru fiecare etapa majora.
"""

import csv
import os
import json
import datetime
from pathlib import Path
from contextlib import closing
import sys
import time

import psycopg2
from dotenv import load_dotenv
# psutil sampling disabled (DB is remote; local CPU/mem not useful)


def log(msg: str) -> None:
    """Afiseaza un mesaj de progres, imediat (cu flush)."""
    print(f"[ETL] {msg}", flush=True)


def qident(name: str) -> str:
    """Quote pentru identificatori SQL (tabele/coloane)."""
    return '"' + name.replace('"', '""') + '"'


def load_config_from_env():
    """Incarca .env si citeste configuratia necesara.

    Variabile acceptate:
      - CSV_PATH: calea catre fisierul CSV (implicit: owid-covid-data.csv)
      - COVID_SCHEMA: schema tinta (implicit: covid)
      - KEEP_STAGING: daca e '1' sau 'true', nu sterge staging la final
      - Conexiune PG: PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE, Optional PGSSLMODE, PGSSLROOTCERT
    """

    # Incearca mai intai .env din current working dir; apoi din radacina repo-ului (doua nivele mai sus de acest script)
    loaded = load_dotenv()
    if not loaded:
        repo_root = Path(__file__).resolve().parents[2]
        env_path = repo_root / ".env"
        if env_path.exists():
            load_dotenv(env_path)

    cfg = {
        "csv_path": os.getenv("CSV_PATH", "owid-covid-data.csv"),
        "schema": os.getenv("COVID_SCHEMA", "covid"),
        "keep_staging": os.getenv("KEEP_STAGING", "0").lower() in ("1", "true", "yes"),
        "report_path": os.getenv("ETL_REPORT_PATH", "etl_report.html"),
        # batch size pentru UPSERT-uri; compatibil cu env UPSERT_BATCH
        "upsert_batch": int(os.getenv("UPSERT_BATCH", "20000")),
        # PG connection params are read later by get_conn()
    }
    return cfg


def get_conn():
    """Construieste conexiunea la Postgres din variabilele PG* din .env."""
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    dbname = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    sslmode = os.getenv("PGSSLMODE")
    sslrootcert = os.getenv("PGSSLROOTCERT")

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


def read_header(csv_path: str):
    """Citeste prima linie (header-ul) a CSV-ului ca lista de coloane."""
    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.reader(f)
        header = next(r)
    return [h.strip() for h in header]


def create_schema_and_staging(cur, schema: str, header_cols):
    """Creeaza schema si masa de staging (toate coloanele TEXT)."""
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {qident(schema)};")
    cols = ",\n  ".join(f"{qident(c)} TEXT" for c in header_cols)
    cur.execute(f"DROP TABLE IF EXISTS {qident(schema)}.stg_owid;")
    cur.execute(f"CREATE TABLE {qident(schema)}.stg_owid (\n  {cols}\n);")


def copy_to_staging(cur, schema: str, csv_path: str, header_cols):
    """Incarca rapid CSV-ul in staging cu COPY FROM STDIN, afisand progres pe linii.

    Returneaza: (linii_procesate, linii_totale, durata_secunde).
    """
    total_lines = count_total_data_lines(csv_path)
    columns = ", ".join(qident(c) for c in header_cols)
    sql = f"COPY {qident(schema)}.stg_owid ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)"
    # Progres textual prin ProgressFile
    pf = ProgressFile(csv_path, total_lines)
    start = time.perf_counter()
    try:
        cur.copy_expert(sql, pf)
    finally:
        pf.finish()
    duration = time.perf_counter() - start
    processed = max(0, pf.lines_seen - 1)
    return processed, total_lines, duration


def create_temp_dedup(cur, schema: str, header_cols):
    """Genereaza o masa temporara cu un singur rand per (iso_code, date).

    Scorul este numarul de coloane nenule (excluzand cheile si denumirea),
    iar pentru fiecare cheie se pastreaza randul cu scorul maxim.
    """
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
          SELECT scored.*, ROW_NUMBER() OVER (PARTITION BY {qident('iso_code')}, {qident('date')} ORDER BY score DESC) AS rn
          FROM scored
        )
        SELECT * FROM ranked WHERE rn = 1;
        """
    )


def upsert_locatii(cur, schema: str):
    """Upsert in locatii (insert + update) fara bare de progres."""
    cur.execute(
        f"""
        WITH latest_raw AS (
          SELECT d.* FROM stg_owid_dedup d
          JOIN (
            SELECT iso_code, MAX(date) AS date
            FROM stg_owid_dedup
            GROUP BY iso_code
          ) m USING (iso_code, date)
        ), src AS (
          SELECT
            iso_code AS cod_iso,
            location AS denumire,
            continent,
            ROUND(NULLIF(population, '')::double precision)::bigint AS populatie,
            NULLIF(population_density, '')::double precision AS densitate_populatie,
            NULLIF(median_age, '')::double precision AS varsta_mediana,
            NULLIF(aged_65_older, '')::double precision AS procent_65_plus,
            NULLIF(aged_70_older, '')::double precision AS procent_70_plus,
            NULLIF(gdp_per_capita, '')::double precision AS pib_pe_cap,
            NULLIF(extreme_poverty, '')::double precision AS saracie_extrema,
            NULLIF(cardiovasc_death_rate, '')::double precision AS rata_decese_cardiovasculare,
            NULLIF(diabetes_prevalence, '')::double precision AS prevalenta_diabet,
            NULLIF(female_smokers, '')::double precision AS fumatoare_proc,
            NULLIF(male_smokers, '')::double precision AS fumatori_proc,
            NULLIF(handwashing_facilities, '')::double precision AS facilitati_spalare_maini,
            NULLIF(hospital_beds_per_thousand, '')::double precision AS paturi_spital_la_mie,
            NULLIF(life_expectancy, '')::double precision AS speranta_viata,
            NULLIF(human_development_index, '')::double precision AS indice_dezvoltare_umana
          FROM latest_raw
        )
        INSERT INTO {qident(schema)}.locatii (
          cod_iso, denumire, continent, populatie, densitate_populatie, varsta_mediana,
          procent_65_plus, procent_70_plus, pib_pe_cap, saracie_extrema,
          rata_decese_cardiovasculare, prevalenta_diabet, fumatoare_proc, fumatori_proc,
          facilitati_spalare_maini, paturi_spital_la_mie, speranta_viata, indice_dezvoltare_umana
        )
        SELECT s.* FROM src s
        LEFT JOIN {qident(schema)}.locatii t ON t.cod_iso = s.cod_iso
        WHERE t.cod_iso IS NULL;
        """
    )
    ins = cur.rowcount or 0

    # Actualizari (cheile existente)
    cur.execute(
        f"""
        WITH latest_raw AS (
          SELECT d.* FROM stg_owid_dedup d
          JOIN (
            SELECT iso_code, MAX(date) AS date
            FROM stg_owid_dedup
            GROUP BY iso_code
          ) m USING (iso_code, date)
        ), src AS (
          SELECT
            iso_code AS cod_iso,
            location AS denumire,
            continent,
            ROUND(NULLIF(population, '')::double precision)::bigint AS populatie,
            NULLIF(population_density, '')::double precision AS densitate_populatie,
            NULLIF(median_age, '')::double precision AS varsta_mediana,
            NULLIF(aged_65_older, '')::double precision AS procent_65_plus,
            NULLIF(aged_70_older, '')::double precision AS procent_70_plus,
            NULLIF(gdp_per_capita, '')::double precision AS pib_pe_cap,
            NULLIF(extreme_poverty, '')::double precision AS saracie_extrema,
            NULLIF(cardiovasc_death_rate, '')::double precision AS rata_decese_cardiovasculare,
            NULLIF(diabetes_prevalence, '')::double precision AS prevalenta_diabet,
            NULLIF(female_smokers, '')::double precision AS fumatoare_proc,
            NULLIF(male_smokers, '')::double precision AS fumatori_proc,
            NULLIF(handwashing_facilities, '')::double precision AS facilitati_spalare_maini,
            NULLIF(hospital_beds_per_thousand, '')::double precision AS paturi_spital_la_mie,
            NULLIF(life_expectancy, '')::double precision AS speranta_viata,
            NULLIF(human_development_index, '')::double precision AS indice_dezvoltare_umana
          FROM latest_raw
        )
        UPDATE {qident(schema)}.locatii t
        SET denumire = COALESCE(s.denumire, t.denumire),
            continent = COALESCE(s.continent, t.continent),
            populatie = COALESCE(s.populatie, t.populatie),
            densitate_populatie = COALESCE(s.densitate_populatie, t.densitate_populatie),
            varsta_mediana = COALESCE(s.varsta_mediana, t.varsta_mediana),
            procent_65_plus = COALESCE(s.procent_65_plus, t.procent_65_plus),
            procent_70_plus = COALESCE(s.procent_70_plus, t.procent_70_plus),
            pib_pe_cap = COALESCE(s.pib_pe_cap, t.pib_pe_cap),
            saracie_extrema = COALESCE(s.saracie_extrema, t.saracie_extrema),
            rata_decese_cardiovasculare = COALESCE(s.rata_decese_cardiovasculare, t.rata_decese_cardiovasculare),
            prevalenta_diabet = COALESCE(s.prevalenta_diabet, t.prevalenta_diabet),
            fumatoare_proc = COALESCE(s.fumatoare_proc, t.fumatoare_proc),
            fumatori_proc = COALESCE(s.fumatori_proc, t.fumatori_proc),
            facilitati_spalare_maini = COALESCE(s.facilitati_spalare_maini, t.facilitati_spalare_maini),
            paturi_spital_la_mie = COALESCE(s.paturi_spital_la_mie, t.paturi_spital_la_mie),
            speranta_viata = COALESCE(s.speranta_viata, t.speranta_viata),
            indice_dezvoltare_umana = COALESCE(s.indice_dezvoltare_umana, t.indice_dezvoltare_umana)
        FROM src s
        WHERE t.cod_iso = s.cod_iso;
        """
    )
    upd = cur.rowcount or 0
    return ins, upd


 


 


 


 


 


def table_count(cur, schema: str, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {qident(schema)}.{qident(table)};")
    (n,) = cur.fetchone()
    return int(n or 0)


def count_total_data_lines(csv_path: str) -> int:
    """Numara liniile de date (fara header) din CSV, in mod eficient."""
    total = 0
    with open(csv_path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            total += chunk.count(b'\n')
    # scadem header-ul (prima linie)
    return max(0, total - 1)


class ProgressFile:
    """Wrapper peste fisier care contorizeaza liniile citite si afiseaza un progress bar.

    Contorizeaza \n din chunk-uri. Estimarea poate varia usor daca CSV contine \n in campuri quoted,
    dar pentru OWID acest caz este rar si progresul este suficient de fidel.
    """

    def __init__(self, path: str, total_lines: int):
        self.f = open(path, 'r', encoding='utf-8')
        self.total_lines = total_lines
        self.lines_seen = 0  # include si header-ul
        self.start = time.time()
        self.last_print = 0.0
        self._active = True

    def read(self, size: int = -1):
        chunk = self.f.read(size)
        if chunk:
            self.lines_seen += chunk.count('\n')
            self._maybe_print()
        return chunk

    def readline(self, size: int = -1):
        line = self.f.readline(size)
        if line:
            self.lines_seen += 1
            self._maybe_print()
        return line

    def _maybe_print(self):
        now = time.time()
        if now - self.last_print < 0.1:
            return
        self.last_print = now
        processed = max(0, self.lines_seen - 1)  # nu numaram header-ul
        remaining = max(0, self.total_lines - processed)
        percent = 100.0 * processed / self.total_lines if self.total_lines else 100.0
        bar_len = 40
        filled = int(percent / 100.0 * bar_len)
        bar = '#' * filled + '-' * (bar_len - filled)
        elapsed = now - self.start
        sys.stdout.write(f"\r[ETL] COPY {percent:6.2f}% |{bar}| proc: {processed:,} ramase: {remaining:,} t={elapsed:5.1f}s")
        sys.stdout.flush()

    def finish(self):
        if not self._active:
            return
        # afisam 100% indiferent daca COPY a raportat exact numarul de \n
        processed = max(0, self.lines_seen - 1)
        remaining = max(0, self.total_lines - processed)
        percent = 100.0 * processed / self.total_lines if self.total_lines else 100.0
        bar_len = 40
        filled = int(min(1.0, percent / 100.0) * bar_len)
        bar = '#' * filled + '-' * (bar_len - filled)
        elapsed = time.time() - self.start
        sys.stdout.write(f"\r[ETL] COPY {percent:6.2f}% |{bar}| proc: {processed:,} ramase: {remaining:,} t={elapsed:5.1f}s\n")
        sys.stdout.flush()
        self._active = False
        try:
            self.f.close()
        except Exception:
            pass


 


class GlobalProgress:
    """Bară de progres globală pentru faza de upsert (toate tabelele)."""
    def __init__(self, label: str, total_units: int):
        self.label = label
        self.total = max(1, int(total_units))
        self.done = 0
        self.start = time.time()
        self.last = 0.0

    def add(self, units: int):
        self.done = min(self.total, self.done + int(units))
        now = time.time()
        if now - self.last < 0.1:
            return
        self.last = now
        pct = min(100.0, 100.0 * self.done / self.total)
        bar_len = 40
        filled = int(pct / 100.0 * bar_len)
        bar = '#' * filled + '-' * (bar_len - filled)
        elapsed = now - self.start
        remaining = max(0, self.total - self.done)
        sys.stdout.write(f"\r[ETL] {self.label} {pct:6.2f}% |{bar}| proc: {self.done:,} ramase: {remaining:,} t={elapsed:5.1f}s")
        sys.stdout.flush()

    def finish(self):
        self.done = self.total
        self.add(0)
        sys.stdout.write("\n")
        sys.stdout.flush()


 


def run():
    """Punctul de intrare: incarca .env, ruleaza toate etapele si raporteaza progresul."""
    cfg = load_config_from_env()

    csv_path = cfg["csv_path"]
    schema = cfg["schema"]
    keep_staging = cfg["keep_staging"]
    etl_wall_start = datetime.datetime.now()
    etl_perf_start = time.perf_counter()
    upsert_batch = int(cfg.get("upsert_batch", 20000))

    # Verificari simple si informari
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"Nu gasesc fisierul CSV: {csv_path}")

    log(f"CSV_PATH = {csv_path}")
    log(f"COVID_SCHEMA = {schema}")
    log("Deschid conexiunea la baza de date folosind variabilele PG* din .env …")

    header = read_header(csv_path)
    log(f"Header CSV detectat: {len(header)} coloane")

    with closing(get_conn()) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            metrics = {"stages": {}, "upserts": {}, "copy": {}}

            t0 = time.perf_counter()
            log("Creez schema si masa de staging …")
            create_schema_and_staging(cur, schema, header)
            metrics["stages"]["create_staging_sec"] = time.perf_counter() - t0
            # Commit intermediar pentru a evita expirarea tranzactiei pe operatiuni lungi (Yugabyte)
            c0 = time.perf_counter(); conn.commit(); metrics["stages"]["commit_after_create_sec"] = time.perf_counter() - c0

            log("Incarc CSV-ul in staging prin COPY …")
            t0 = time.perf_counter()
            processed, total_lines, copy_dur = copy_to_staging(cur, schema, csv_path, header)
            metrics["copy"] = {
                "processed": int(processed),
                "total": int(total_lines),
                "duration_sec": float(copy_dur),
                "size_bytes": int(Path(csv_path).stat().st_size),
            }
            metrics["stages"]["copy_sec"] = time.perf_counter() - t0
            c0 = time.perf_counter(); conn.commit(); metrics["stages"]["commit_after_copy_sec"] = time.perf_counter() - c0

            # Raportam cate randuri au intrat in staging
            cur.execute(f"SELECT COUNT(*) FROM {qident(schema)}.stg_owid;")
            (staging_count,) = cur.fetchone()
            log(f"Incarcate in staging: {staging_count} randuri")
            metrics["stages"]["staging_rows"] = int(staging_count or 0)

            log("Deduplic (iso_code, date) in masa temporara …")
            t0 = time.perf_counter()
            create_temp_dedup(cur, schema, header)
            metrics["stages"]["dedup_sec"] = time.perf_counter() - t0
            c0 = time.perf_counter(); conn.commit(); metrics["stages"]["commit_after_dedup_sec"] = time.perf_counter() - c0

            # Randuri unice dupa deduplicare
            cur.execute("SELECT COUNT(*) FROM stg_owid_dedup;")
            (dedup_count,) = cur.fetchone()
            log(f"Randuri dupa deduplicare: {dedup_count}")
            metrics["stages"]["dedup_rows"] = int(dedup_count or 0)
            # Fallback in cazuri speciale (de ex. incompatibilitati pe window functions)
            if (dedup_count or 0) == 0 and (staging_count or 0) > 0:
                log("Dedup a returnat 0 randuri desi staging are date. Reincerc cu DISTINCT ON …")
                cur.execute("DROP TABLE IF EXISTS stg_owid_dedup;")
                cur.execute(
                    f"""
                    CREATE TEMP TABLE stg_owid_dedup AS
                    SELECT DISTINCT ON ({qident('iso_code')}, {qident('date')}) s.*
                    FROM {qident(schema)}.stg_owid s
                    ORDER BY {qident('iso_code')}, {qident('date')};
                    """
                )
                c0 = time.perf_counter(); conn.commit(); metrics["stages"]["commit_after_dedup_fallback_sec"] = time.perf_counter() - c0
                cur.execute("SELECT COUNT(*) FROM stg_owid_dedup;")
                (dedup_count2,) = cur.fetchone()
                log(f"Randuri dupa dedup DISTINCT ON: {dedup_count2}")
                metrics["stages"]["dedup_rows"] = int(dedup_count2 or 0)

            # Upsert locatii (numar mic, fara progress detaliat)
            t0 = time.perf_counter()
            log("Upsert in locatii …")
            ins, upd = upsert_locatii(cur, schema)
            metrics["upserts"]["locatii"] = {"inserted": int(ins), "updated": int(upd), "sec": time.perf_counter() - t0}
            c0 = time.perf_counter(); conn.commit(); metrics["upserts"]["locatii"]["commit_sec"] = time.perf_counter() - c0

            # Upserts pentru tabelele zilnice in batch-uri, cu o singura bara globala
            total_rows = int(metrics["stages"].get("dedup_rows", 0))
            if total_rows == 0:
                # fallback la staging_count daca dedup_rows lipseste
                total_rows = int(metrics["stages"].get("staging_rows", 0))
            gprog = GlobalProgress("UPSERT toate tabelele", total_rows)

            # Pregatim index pentru batch-uri
            cur.execute("DROP TABLE IF EXISTS stg_keys;")
            cur.execute(
                """
                CREATE TEMP TABLE stg_keys AS
                SELECT iso_code, date, ROW_NUMBER() OVER (ORDER BY iso_code, date) AS rn
                FROM stg_owid_dedup;
                """
            )
            conn.commit()

            inserted_counts = {k: 0 for k in ["cazuri_zilnice","testari_zilnice","vaccinari_zilnice","spitalizari_zilnice","mortalitate_exces_zilnica"]}
            updated_counts = {k: 0 for k in inserted_counts}
            time_spent = {k: 0.0 for k in inserted_counts}

            processed = 0
            base = 1
            while base <= total_rows:
                upper = min(base + upsert_batch - 1, total_rows)
                # batch
                cur.execute("DROP TABLE IF EXISTS tmp_batch;")
                cur.execute(
                    """
                    CREATE TEMP TABLE tmp_batch AS
                    SELECT d.*
                    FROM stg_owid_dedup d
                    JOIN stg_keys k USING (iso_code, date)
                    WHERE k.rn BETWEEN %s AND %s;
                    """,
                    (base, upper),
                )

                # cazuri_zilnice
                t0 = time.perf_counter()
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
                    FROM tmp_batch b
                    LEFT JOIN {qident(schema)}.cazuri_zilnice t
                      ON t.cod_iso = b.iso_code AND t.data = NULLIF(b.date,'')::date
                    WHERE t.cod_iso IS NULL;
                    """
                )
                inserted_counts["cazuri_zilnice"] += cur.rowcount or 0
                cur.execute(
                    f"""
                    UPDATE {qident(schema)}.cazuri_zilnice t
                    SET total_cazuri = COALESCE(b.total_cases::bigint, t.total_cazuri),
                        cazuri_noi = COALESCE(b.new_cases::double precision, t.cazuri_noi),
                        cazuri_noi_netezite = COALESCE(b.new_cases_smoothed::double precision, t.cazuri_noi_netezite),
                        total_decese = COALESCE(b.total_deaths::bigint, t.total_decese),
                        decese_noi = COALESCE(b.new_deaths::double precision, t.decese_noi),
                        decese_noi_netezite = COALESCE(b.new_deaths_smoothed::double precision, t.decese_noi_netezite),
                        total_cazuri_la_milion = COALESCE(b.total_cases_per_million::double precision, t.total_cazuri_la_milion),
                        cazuri_noi_la_milion = COALESCE(b.new_cases_per_million::double precision, t.cazuri_noi_la_milion),
                        cazuri_noi_netezite_la_milion = COALESCE(b.new_cases_smoothed_per_million::double precision, t.cazuri_noi_netezite_la_milion),
                        total_decese_la_milion = COALESCE(b.total_deaths_per_million::double precision, t.total_decese_la_milion),
                        decese_noi_la_milion = COALESCE(b.new_deaths_per_million::double precision, t.decese_noi_la_milion),
                        decese_noi_netezite_la_milion = COALESCE(b.new_deaths_smoothed_per_million::double precision, t.decese_noi_netezite_la_milion),
                        rata_reproductie = COALESCE(b.reproduction_rate::double precision, t.rata_reproductie),
                        indice_strictete = COALESCE(b.stringency_index::double precision, t.indice_strictete)
                    FROM tmp_batch b
                    JOIN {qident(schema)}.cazuri_zilnice x ON x.cod_iso = b.iso_code AND x.data = NULLIF(b.date,'')::date
                    WHERE t.cod_iso = x.cod_iso AND t.data = x.data;
                    """
                )
                updated_counts["cazuri_zilnice"] += cur.rowcount or 0
                time_spent["cazuri_zilnice"] += time.perf_counter() - t0

                # testari_zilnice
                t0 = time.perf_counter()
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
                    FROM tmp_batch b
                    LEFT JOIN {qident(schema)}.testari_zilnice t
                      ON t.cod_iso = b.iso_code AND t.data = NULLIF(b.date,'')::date
                    WHERE t.cod_iso IS NULL;
                    """
                )
                inserted_counts["testari_zilnice"] += cur.rowcount or 0
                cur.execute(
                    f"""
                    UPDATE {qident(schema)}.testari_zilnice t
                    SET total_testari = COALESCE(b.total_tests::bigint, t.total_testari),
                        testari_noi = COALESCE(b.new_tests::double precision, t.testari_noi),
                        total_testari_la_mie = COALESCE(b.total_tests_per_thousand::double precision, t.total_testari_la_mie),
                        testari_noi_la_mie = COALESCE(b.new_tests_per_thousand::double precision, t.testari_noi_la_mie),
                        testari_noi_netezite = COALESCE(b.new_tests_smoothed::double precision, t.testari_noi_netezite),
                        testari_noi_netezite_la_mie = COALESCE(b.new_tests_smoothed_per_thousand::double precision, t.testari_noi_netezite_la_mie),
                        rata_pozitivitate = COALESCE(b.positive_rate::double precision, t.rata_pozitivitate),
                        teste_pe_caz = COALESCE(b.tests_per_case::double precision, t.teste_pe_caz),
                        unitati_testare = COALESCE(NULLIF(b.tests_units, ''), t.unitati_testare)
                    FROM tmp_batch b
                    JOIN {qident(schema)}.testari_zilnice x ON x.cod_iso = b.iso_code AND x.data = NULLIF(b.date,'')::date
                    WHERE t.cod_iso = x.cod_iso AND t.data = x.data;
                    """
                )
                updated_counts["testari_zilnice"] += cur.rowcount or 0
                time_spent["testari_zilnice"] += time.perf_counter() - t0

                # vaccinari_zilnice
                t0 = time.perf_counter()
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
                    FROM tmp_batch b
                    LEFT JOIN {qident(schema)}.vaccinari_zilnice t
                      ON t.cod_iso = b.iso_code AND t.data = NULLIF(b.date,'')::date
                    WHERE t.cod_iso IS NULL;
                    """
                )
                inserted_counts["vaccinari_zilnice"] += cur.rowcount or 0
                cur.execute(
                    f"""
                    UPDATE {qident(schema)}.vaccinari_zilnice t
                    SET total_vaccinari = COALESCE(b.total_vaccinations::bigint, t.total_vaccinari),
                        persoane_vaccinate = COALESCE(b.people_vaccinated::bigint, t.persoane_vaccinate),
                        persoane_vaccinate_complet = COALESCE(b.people_fully_vaccinated::bigint, t.persoane_vaccinate_complet),
                        total_boostere = COALESCE(b.total_boosters::bigint, t.total_boostere),
                        vaccinari_noi = COALESCE(b.new_vaccinations::double precision, t.vaccinari_noi),
                        vaccinari_noi_netezite = COALESCE(b.new_vaccinations_smoothed::double precision, t.vaccinari_noi_netezite),
                        total_vaccinari_la_suta = COALESCE(b.total_vaccinations_per_hundred::double precision, t.total_vaccinari_la_suta),
                        persoane_vaccinate_la_suta = COALESCE(b.people_vaccinated_per_hundred::double precision, t.persoane_vaccinate_la_suta),
                        persoane_vaccinate_complet_la_suta = COALESCE(b.people_fully_vaccinated_per_hundred::double precision, t.persoane_vaccinate_complet_la_suta),
                        total_boostere_la_suta = COALESCE(b.total_boosters_per_hundred::double precision, t.total_boostere_la_suta),
                        vaccinari_noi_netezite_la_milion = COALESCE(b.new_vaccinations_smoothed_per_million::double precision, t.vaccinari_noi_netezite_la_milion),
                        persoane_noi_vaccinate_netezite = COALESCE(b.new_people_vaccinated_smoothed::double precision, t.persoane_noi_vaccinate_netezite),
                        persoane_noi_vaccinate_netezite_la_suta = COALESCE(b.new_people_vaccinated_smoothed_per_hundred::double precision, t.persoane_noi_vaccinate_netezite_la_suta)
                    FROM tmp_batch b
                    JOIN {qident(schema)}.vaccinari_zilnice x ON x.cod_iso = b.iso_code AND x.data = NULLIF(b.date,'')::date
                    WHERE t.cod_iso = x.cod_iso AND t.data = x.data;
                    """
                )
                updated_counts["vaccinari_zilnice"] += cur.rowcount or 0
                time_spent["vaccinari_zilnice"] += time.perf_counter() - t0

                # spitalizari_zilnice
                t0 = time.perf_counter()
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
                    FROM tmp_batch b
                    LEFT JOIN {qident(schema)}.spitalizari_zilnice t
                      ON t.cod_iso = b.iso_code AND t.data = NULLIF(b.date,'')::date
                    WHERE t.cod_iso IS NULL;
                    """
                )
                inserted_counts["spitalizari_zilnice"] += cur.rowcount or 0
                cur.execute(
                    f"""
                    UPDATE {qident(schema)}.spitalizari_zilnice t
                    SET pacienti_uti = COALESCE(b.icu_patients::double precision, t.pacienti_uti),
                        pacienti_uti_la_milion = COALESCE(b.icu_patients_per_million::double precision, t.pacienti_uti_la_milion),
                        pacienti_spital = COALESCE(b.hosp_patients::double precision, t.pacienti_spital),
                        pacienti_spital_la_milion = COALESCE(b.hosp_patients_per_million::double precision, t.pacienti_spital_la_milion),
                        internari_uti_saptamanale = COALESCE(b.weekly_icu_admissions::double precision, t.internari_uti_saptamanale),
                        internari_uti_saptamanale_la_milion = COALESCE(b.weekly_icu_admissions_per_million::double precision, t.internari_uti_saptamanale_la_milion),
                        internari_spital_saptamanale = COALESCE(b.weekly_hosp_admissions::double precision, t.internari_spital_saptamanale),
                        internari_spital_saptamanale_la_milion = COALESCE(b.weekly_hosp_admissions_per_million::double precision, t.internari_spital_saptamanale_la_milion)
                    FROM tmp_batch b
                    JOIN {qident(schema)}.spitalizari_zilnice x ON x.cod_iso = b.iso_code AND x.data = NULLIF(b.date,'')::date
                    WHERE t.cod_iso = x.cod_iso AND t.data = x.data;
                    """
                )
                updated_counts["spitalizari_zilnice"] += cur.rowcount or 0
                time_spent["spitalizari_zilnice"] += time.perf_counter() - t0

                # mortalitate_exces_zilnica
                t0 = time.perf_counter()
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
                    FROM tmp_batch b
                    LEFT JOIN {qident(schema)}.mortalitate_exces_zilnica t
                      ON t.cod_iso = b.iso_code AND t.data = NULLIF(b.date,'')::date
                    WHERE t.cod_iso IS NULL;
                    """
                )
                inserted_counts["mortalitate_exces_zilnica"] += cur.rowcount or 0
                cur.execute(
                    f"""
                    UPDATE {qident(schema)}.mortalitate_exces_zilnica t
                    SET mortalitate_exces_cumulata_absoluta = COALESCE(b.excess_mortality_cumulative_absolute::double precision, t.mortalitate_exces_cumulata_absoluta),
                        mortalitate_exces_cumulata = COALESCE(b.excess_mortality_cumulative::double precision, t.mortalitate_exces_cumulata),
                        mortalitate_exces = COALESCE(b.excess_mortality::double precision, t.mortalitate_exces),
                        mortalitate_exces_cumulata_la_milion = COALESCE(b.excess_mortality_cumulative_per_million::double precision, t.mortalitate_exces_cumulata_la_milion)
                    FROM tmp_batch b
                    JOIN {qident(schema)}.mortalitate_exces_zilnica x ON x.cod_iso = b.iso_code AND x.data = NULLIF(b.date,'')::date
                    WHERE t.cod_iso = x.cod_iso AND t.data = x.data;
                    """
                )
                updated_counts["mortalitate_exces_zilnica"] += cur.rowcount or 0
                time_spent["mortalitate_exces_zilnica"] += time.perf_counter() - t0

                # finalizeaza batch
                conn.commit()
                processed += (upper - base + 1)
                gprog.add(upper - base + 1)
                base = upper + 1

            gprog.finish()

            # Populate metrics upserts din agregate
            for tbl in inserted_counts:
                metrics["upserts"][tbl] = {
                    "inserted": int(inserted_counts[tbl]),
                    "updated": int(updated_counts[tbl]),
                    "sec": float(time_spent[tbl])
                }

            if not keep_staging:
                log("Sterg masa de staging …")
                t0 = time.perf_counter()
                cur.execute(f"DROP TABLE IF EXISTS {qident(schema)}.stg_owid;")
                metrics["stages"]["drop_staging_sec"] = time.perf_counter() - t0
                c0 = time.perf_counter(); conn.commit(); metrics["stages"]["commit_after_drop_sec"] = time.perf_counter() - c0

            # Optional: volum final per tabel
            metrics["final_counts"] = {
                "locatii": table_count(cur, schema, "locatii"),
                "cazuri_zilnice": table_count(cur, schema, "cazuri_zilnice"),
                "testari_zilnice": table_count(cur, schema, "testari_zilnice"),
                "vaccinari_zilnice": table_count(cur, schema, "vaccinari_zilnice"),
                "spitalizari_zilnice": table_count(cur, schema, "spitalizari_zilnice"),
                "mortalitate_exces_zilnica": table_count(cur, schema, "mortalitate_exces_zilnica"),
            }

        # Commit final (in mod normal, deja am facut commit dupa fiecare etapa)
        commit_start = time.perf_counter()
        conn.commit()
        metrics["stages"]["commit_sec"] = time.perf_counter() - commit_start
        metrics["stages"]["total_sec"] = sum(v for k, v in metrics["stages"].items() if k.endswith("_sec"))
        log("Commit finalizat. ETL incheiat cu succes.")

    # Afiseaza raport de performanta
    print()  # newline dupa logurile in-flight
    etl_wall_end = datetime.datetime.now()
    etl_total = time.perf_counter() - etl_perf_start
    metrics["etl"] = {
        "start_iso": etl_wall_start.isoformat(timespec='seconds'),
        "end_iso": etl_wall_end.isoformat(timespec='seconds'),
        "duration_sec": etl_total,
    }
    report_html = render_html_report(metrics)
    report_path = cfg.get("report_path", "etl_report.html")
    try:
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_html)
        log(f"Raport salvat in {report_path}")
    except Exception as e:
        log(f"Eroare la scrierea raportului in {report_path}: {e}")



def render_html_report(metrics: dict) -> str:
    """Versiune robusta a generatorului de HTML folosind template.format si JSON pentru serii."""
    def fmt_sec(s: float) -> str:
        return f"{s:.2f}s"

    copy = metrics.get("copy", {})
    stages = metrics.get("stages", {})
    upserts = metrics.get("upserts", {})
    finals = metrics.get("final_counts", {})

    proc = int(copy.get("processed", 0))
    total = int(copy.get("total", 0))
    dur = float(copy.get("duration_sec", 0.0))
    size = int(copy.get("size_bytes", 0))
    rps = (proc / dur) if dur else 0.0
    mbps = (size / 1_000_000.0 / dur) if dur else 0.0

    # Serii pentru grafice (JSON pentru JS) – doar etape si upserturi

    stage_pairs = [
        ('Create staging', float(stages.get('create_staging_sec', 0.0))),
        ('COPY', float(stages.get('copy_sec', 0.0))),
        ('Dedup', float(stages.get('dedup_sec', 0.0))),
        ('Drop staging', float(stages.get('drop_staging_sec', 0.0))),
        ('Commit', float(stages.get('commit_sec', 0.0))),
    ]
    stage_pairs_sorted = sorted(stage_pairs, key=lambda x: x[1], reverse=True)
    JS_STAGE_LABELS = json.dumps([k for k, _ in stage_pairs_sorted])
    JS_STAGE_DATA = json.dumps([round(v, 2) for _, v in stage_pairs_sorted])

    # Donut de faze (COPY / Dedup / Upserts / Commit)
    up_total_sec = sum(float(upserts.get(t, {}).get('sec', 0.0)) for t in [
        'cazuri_zilnice','testari_zilnice','vaccinari_zilnice','spitalizari_zilnice','mortalitate_exces_zilnica'
    ])
    JS_PHASE_LABELS = json.dumps(['COPY','Dedup','Upserts','Commit'])
    JS_PHASE_DATA = json.dumps([
        round(float(stages.get('copy_sec', 0.0)), 2),
        round(float(stages.get('dedup_sec', 0.0)), 2),
        round(float(up_total_sec), 2),
        round(float(stages.get('commit_sec', 0.0)), 2),
    ])

    up_tables = ['locatii','cazuri','testari','vaccinari','spitalizari','mortalitate']
    up_ins = [
        int(upserts.get('locatii',{}).get('inserted',0)),
        int(upserts.get('cazuri_zilnice',{}).get('inserted',0)),
        int(upserts.get('testari_zilnice',{}).get('inserted',0)),
        int(upserts.get('vaccinari_zilnice',{}).get('inserted',0)),
        int(upserts.get('spitalizari_zilnice',{}).get('inserted',0)),
        int(upserts.get('mortalitate_exces_zilnica',{}).get('inserted',0))
    ]
    up_upd = [
        int(upserts.get('locatii',{}).get('updated',0)),
        int(upserts.get('cazuri_zilnice',{}).get('updated',0)),
        int(upserts.get('testari_zilnice',{}).get('updated',0)),
        int(upserts.get('vaccinari_zilnice',{}).get('updated',0)),
        int(upserts.get('spitalizari_zilnice',{}).get('updated',0)),
        int(upserts.get('mortalitate_exces_zilnica',{}).get('updated',0))
    ]
    JS_UP_TABLES = json.dumps(up_tables)
    JS_UP_INS = json.dumps(up_ins)
    JS_UP_UPD = json.dumps(up_upd)

    def tr(k, v):
        return f"<tr><td>{k}</td><td>{v}</td></tr>"

    stages_rows = "".join([
        tr("Create staging", fmt_sec(float(stages.get('create_staging_sec', 0.0)))),
        tr("COPY", fmt_sec(float(stages.get('copy_sec', 0.0)))),
        tr("Dedup", fmt_sec(float(stages.get('dedup_sec', 0.0)))),
        tr("Drop staging", fmt_sec(float(stages.get('drop_staging_sec', 0.0)))),
        tr("Commit", fmt_sec(float(stages.get('commit_sec', 0.0)))),
        tr("Total", fmt_sec(float(stages.get('total_sec', 0.0)))),
    ])

    staging_rows = int(stages.get('staging_rows', 0))
    dedup_rows = int(stages.get('dedup_rows', 0))
    retention = (100.0 * dedup_rows / staging_rows) if staging_rows else 0.0

    copy_rows = "".join([
        tr("Linii procesate", f"{proc:,} din {total:,}"),
        tr("Durata COPY", fmt_sec(dur)),
        tr("Linii/s", f"{rps:,.0f}"),
        tr("MB/s", f"{mbps:.2f}"),
        tr("Dimensiune fisier", f"{size/1_000_000.0:.2f} MB"),
        tr("Randuri staging", f"{staging_rows:,}"),
        tr("Randuri dupa dedup", f"{dedup_rows:,}"),
        tr("Retentie dupa dedup", f"{retention:.1f}%"),
    ])

    up_rows = []
    for table in ("locatii", "cazuri_zilnice", "testari_zilnice", "vaccinari_zilnice", "spitalizari_zilnice", "mortalitate_exces_zilnica"):
        u = upserts.get(table, {})
        up_rows.append(
            f"<tr><td>{table}</td><td>{int(u.get('inserted',0)):,}</td><td>{int(u.get('updated',0)):,}</td><td>{fmt_sec(float(u.get('sec',0.0)))}</td><td>{int(finals.get(table,0)):,}</td></tr>"
        )
    up_table_rows = "".join(up_rows)

    # tabel volume finale
    def tr2(k, v):
        return f"<tr><td style='color:#94a3b8'>{k}</td><td><b>{v}</b></td></tr>"
    final_rows = []
    name_map = {
        'locatii': 'locatii',
        'cazuri_zilnice': 'cazuri',
        'testari_zilnice': 'testari',
        'vaccinari_zilnice': 'vaccinari',
        'spitalizari_zilnice': 'spitalizari',
        'mortalitate_exces_zilnica': 'mortalitate_exces',
    }
    for key, label in name_map.items():
        final_rows.append(tr2(label, f"{int(finals.get(key,0)):,}"))
    FINAL_TABLE_ROWS = "".join(final_rows)

    template = """
<!DOCTYPE html>
<html lang=\"ro\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Raport ETL OWID</title>
  <link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">
  <link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap\" rel=\"stylesheet\">
  <script src=\"https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js\"></script>
  <style>
    :root {{ --bg:#0b1220; --panel:#0f172a; --border:#1f2937; --text:#e5e7eb; --muted:#94a3b8; }}
    * {{ box-sizing:border-box; }}
    body {{ font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin:0; color:var(--text); background: radial-gradient(1200px 600px at 10% -20%, #0b5, transparent), radial-gradient(1000px 800px at 120% -10%, #09f, transparent), var(--bg); }}
    header {{ padding:24px; border-bottom:1px solid var(--border); background:linear-gradient(90deg, rgba(34,197,94,.14), rgba(56,189,248,.12)); }}
    h1 {{ font-size:24px; margin:0; letter-spacing:.2px; }}
    main {{ padding:24px; }}
    h2 {{ font-size:16px; margin:16px 0 8px; color:var(--muted); font-weight:600; }}
    .grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(320px,1fr)); gap:16px; }}
    .panel {{ background:var(--panel); border:1px solid var(--border); border-radius:12px; padding:16px; box-shadow:0 6px 20px rgba(0,0,0,.28); }}
    table {{ border-collapse:collapse; width:100%; background:var(--panel); border:1px solid var(--border); border-radius:12px; overflow:hidden; }}
    th, td {{ border-bottom:1px solid var(--border); padding:10px 12px; font-size:14px; }}
    th {{ background:rgba(255,255,255,.04); text-align:left; color:var(--muted); font-weight:600; }}
    .kpis {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(190px,1fr)); gap:12px; margin: 12px 0 20px; }}
    .kpi {{ background:var(--panel); border:1px solid var(--border); border-radius:12px; padding:14px 16px; box-shadow:0 6px 18px rgba(0,0,0,.25); }}
    .small {{ color:var(--muted); font-size:12px; margin-bottom:6px; }}
    .value {{ font-weight:600; font-size:16px; letter-spacing:.2px; }}
    canvas {{ background:transparent; border:1px solid var(--border); border-radius:12px; padding:8px; }}
  </style>
</head>
<body>
  <header><h1>Raport ETL OWID</h1></header>
  <main>
    <div class=\"kpis\">
      <div class=\"kpi\"><div class=\"small\">Start ETL</div><div class=\"value\">{START_AT}</div></div>
      <div class=\"kpi\"><div class=\"small\">Final ETL</div><div class=\"value\">{END_AT}</div></div>
      <div class=\"kpi\"><div class=\"small\">Durata</div><div class=\"value\">{ELAPSED}</div></div>
      <div class=\"kpi\"><div class=\"small\">Linii procesate</div><div class=\"value\">{PROC} / {TOTAL}</div></div>
      <div class=\"kpi\"><div class=\"small\">Viteza COPY</div><div class=\"value\">{RPS} linii/s</div></div>
      <div class=\"kpi\"><div class=\"small\">Debit COPY</div><div class=\"value\">{MBPS} MB/s</div></div>
      <div class=\"kpi\"><div class=\"small\">Dimensiune fisier</div><div class=\"value\">{SIZE_MB} MB</div></div>
    </div>

    <div class=\"grid\" style=\"margin-bottom:16px\">
      <div class=\"panel\">
        <div class=\"small\">Durate etape (secunde)</div>
        <canvas id=\"stagesChart\"></canvas>
      </div>
      <div class=\"panel\">
        <div class=\"small\">Impartire timp pe faze</div>
        <canvas id=\"phasesChart\"></canvas>
      </div>
      <div class=\"panel\">
        <div class=\"small\">Upserts (insertari vs actualizari)</div>
        <canvas id=\"upsertsChart\"></canvas>
      </div>
    </div>

    <div class=\"grid\">
      <div class=\"panel\">
        <h2>Etape</h2>
        <table>
          <thead><tr><th>Etapa</th><th>Durata</th></tr></thead>
          <tbody>{STAGES_ROWS}</tbody>
        </table>
        <h2 style=\"margin-top:16px\">COPY & Dedup</h2>
        <table>
          <thead><tr><th>Indicator</th><th>Valoare</th></tr></thead>
          <tbody>{COPY_ROWS}</tbody>
        </table>
      </div>
      <div class=\"panel\">
        <h2>Upserts</h2>
        <table>
          <thead><tr><th>Tabel</th><th>Insertari</th><th>Actualizari</th><th>Durata</th><th>Total dupa</th></tr></thead>
          <tbody>{UP_TABLE_ROWS}</tbody>
        </table>
        <h2 style=\"margin-top:16px\">Tabele finale</h2>
        <table>
          <thead><tr><th>Tabel</th><th>Randuri</th></tr></thead>
          <tbody>{FINAL_TABLE_ROWS}</tbody>
        </table>
      </div>
    </div>
  </main>

  <script>
    const stageLabels = {JS_STAGE_LABELS};
    const stageData = {JS_STAGE_DATA};
    const phaseLabels = {JS_PHASE_LABELS};
    const phaseData = {JS_PHASE_DATA};
    const upTables = {JS_UP_TABLES};
    const upIns = {JS_UP_INS};
    const upUpd = {JS_UP_UPD};

    new Chart(document.getElementById('stagesChart'), {{
      type: 'bar',
      data: {{ labels: stageLabels, datasets: [{{ label: 'secunde', data: stageData, backgroundColor: '#60a5fa' }}] }},
      options: {{ responsive: true, indexAxis: 'y', plugins: {{ legend: {{ display: false }} }} }}
    }});

    new Chart(document.getElementById('phasesChart'), {{
      type: 'doughnut',
      data: {{ labels: phaseLabels, datasets: [{{ data: phaseData, backgroundColor: ['#38bdf8','#34d399','#fbbf24','#a78bfa'] }}] }},
      options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom' }} }} }}
    }});

    new Chart(document.getElementById('upsertsChart'), {{
      type: 'bar',
      data: {{
        labels: upTables,
        datasets: [
          {{ label: 'insertari', data: upIns, backgroundColor: '#22c55e' }},
          {{ label: 'actualizari', data: upUpd, backgroundColor: '#f59e0b' }}
        ]
      }},
      options: {{ responsive: true, scales: {{ x: {{ stacked: true }}, y: {{ stacked: true }} }} }}
    }});
  </script>
</body>
</html>
"""
    html = template.format(
        PROC=f"{proc:,}", TOTAL=f"{total:,}", DUR=fmt_sec(dur), RPS=f"{rps:,.0f}", MBPS=f"{mbps:.2f}",
        SIZE_MB=f"{size/1_000_000.0:.2f}",
        STAGES_ROWS=stages_rows, COPY_ROWS=copy_rows, UP_TABLE_ROWS=up_table_rows, FINAL_TABLE_ROWS=FINAL_TABLE_ROWS,
        JS_STAGE_LABELS=JS_STAGE_LABELS, JS_STAGE_DATA=JS_STAGE_DATA,
        JS_PHASE_LABELS=JS_PHASE_LABELS, JS_PHASE_DATA=JS_PHASE_DATA,
        JS_UP_TABLES=JS_UP_TABLES, JS_UP_INS=JS_UP_INS, JS_UP_UPD=JS_UP_UPD,
        START_AT=metrics.get('etl',{}).get('start_iso','-'), END_AT=metrics.get('etl',{}).get('end_iso','-'),
        ELAPSED=fmt_sec(float(metrics.get('etl',{}).get('duration_sec', 0.0)))
    )
    return html


if __name__ == "__main__":
    run()
 
