#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import os
import re
import sys
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple

import requests
import psycopg2
from psycopg2 import sql


DATA_LINK_PATH = Path("./data/data_link.json")
RAW_DIR = Path("./data/raw")
BRONZE_SCHEMA = "bronze"

# Extensions qu'on considère comme "ressource de données" téléchargeable
DATA_FILE_EXTS = (
    ".csv",
    ".tsv",
    ".txt",
    ".xlsx",
    ".xls",
    ".json",
    ".geojson",
    ".zip",
)

DEFAULT_TIMEOUT = 60


def _get_db_config() -> dict[str, Any]:
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "mspr813"),
        "user": os.getenv("POSTGRES_USER", "mspr813"),
        "password": os.getenv("POSTGRES_PASSWORD", "s5t4v5"),
    }


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"^_+|_+$", "", s)
    if not s:
        s = "dataset"
    return s


def safe_table_name(topic: str) -> str:
    base = slugify(topic)
    # table bronze: <topic>__raw
    return f"{base}__raw"


@dataclass
class Dataset:
    name: str
    topic: str
    url: str


@dataclass
class Resource:
    resource_url: str
    file_ext: str
    title: str | None = None


def load_datasets(path: Path) -> list[Dataset]:
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    out: list[Dataset] = []
    for d in data.get("datasets", []):
        out.append(Dataset(name=d["name"], topic=d["topic"], url=d["url"]))
    return out


def http_get(url: str, stream: bool = False) -> requests.Response:
    headers = {
        "User-Agent": "mspr813-bronze-loader/1.0",
        "Accept": "*/*",
    }
    r = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT, stream=stream)
    r.raise_for_status()
    return r


def is_datagouv_dataset(url: str) -> Tuple[bool, Optional[str]]:
    # ex: https://www.data.gouv.fr/datasets/menages
    m = re.match(r"^https?://(www\.)?data\.gouv\.fr/(fr/)?datasets/([^/?#]+)", url)
    if not m:
        return (False, None)
    slug = m.group(3)
    return (True, slug)


def choose_best_datagouv_resource(resources: list[dict[str, Any]]) -> Optional[Resource]:
    """
    Heuristique:
    - priorise CSV, puis XLSX/XLS, puis ZIP, puis JSON/GeoJSON
    - utilise de préférence "latest" ou "download_url" si présent, sinon "url"
    """
    scored: list[Tuple[int, Resource]] = []

    def score(ext: str) -> int:
        ext = ext.lower()
        if ext == ".csv":
            return 100
        if ext in (".xlsx", ".xls"):
            return 80
        if ext == ".zip":
            return 60
        if ext in (".json", ".geojson"):
            return 50
        if ext in (".tsv", ".txt"):
            return 40
        return 10

    def pick_url(res: dict[str, Any]) -> Optional[str]:
        for k in ("latest", "download_url", "url"):
            v = res.get(k)
            if isinstance(v, str) and v.startswith("http"):
                return v
        return None

    for res in resources:
        u = pick_url(res)
        if not u:
            continue

        u_low = u.lower()
        ext_found = None
        for ext in DATA_FILE_EXTS:
            if u_low.endswith(ext):
                ext_found = ext
                break

        if not ext_found:
            fmt = (res.get("format") or res.get("filetype") or "")
            fmt = str(fmt).lower().strip(". ")
            fmt_map = {
                "csv": ".csv",
                "xls": ".xls",
                "xlsx": ".xlsx",
                "json": ".json",
                "geojson": ".geojson",
                "zip": ".zip",
                "tsv": ".tsv",
            }
            if fmt in fmt_map:
                ext_found = fmt_map[fmt]

        if ext_found:
            scored.append((score(ext_found), Resource(resource_url=u, file_ext=ext_found, title=res.get("title"))))

    if not scored:
        return None
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


def resolve_resource_url(dataset: Dataset) -> Optional[Resource]:
    """
    - Si data.gouv dataset => API obligatoire (pas de parsing HTML)
    - Sinon => scan HTML (fallback) pour trouver un lien direct vers un fichier
    """
    is_dg, slug = is_datagouv_dataset(dataset.url)
    if is_dg and slug:
        api_url = f"https://www.data.gouv.fr/api/1/datasets/{slug}/"
        log(f"[{dataset.topic}] data.gouv API -> {api_url}")
        try:
            j = http_get(api_url).json()
            resources = j.get("resources", [])
            if not isinstance(resources, list) or not resources:
                log(f"[{dataset.topic}] Aucune ressource trouvée via l'API.")
                return None
            res = choose_best_datagouv_resource(resources)
            if not res:
                log(f"[{dataset.topic}] Ressources présentes mais aucune URL exploitable (csv/xlsx/zip/json).")
            return res
        except Exception as e:
            log(f"[{dataset.topic}] Erreur API data.gouv: {e}")
            return None

    # Fallback HTML seulement pour NON-data.gouv
    log(f"[{dataset.topic}] Fallback HTML scan -> {dataset.url}")
    try:
        html = http_get(dataset.url).text
    except Exception as e:
        log(f"[{dataset.topic}] Impossible de lire la page HTML: {e}")
        return None

    hrefs = re.findall(r'href="([^"]+)"', html, flags=re.IGNORECASE)
    abs_links: list[str] = []
    for h in hrefs:
        if h.startswith("//"):
            abs_links.append("https:" + h)
        elif h.startswith("http://") or h.startswith("https://"):
            abs_links.append(h)
        elif h.startswith("/"):
            base = re.match(r"^(https?://[^/]+)", dataset.url)
            if base:
                abs_links.append(base.group(1) + h)

    for link in abs_links:
        low = link.lower()
        for ext in DATA_FILE_EXTS:
            if low.endswith(ext):
                return Resource(resource_url=link, file_ext=ext)
    return None


def download_resource(dataset: Dataset, res: Resource) -> Optional[Path]:
    """
    Télécharge la resource dans data/raw/<topic>/, retourne le chemin local.
    """
    topic_dir = RAW_DIR / slugify(dataset.topic)
    topic_dir.mkdir(parents=True, exist_ok=True)

    # Nom de fichier
    now = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{slugify(dataset.topic)}_{now}{res.file_ext}"
    out_path = topic_dir / file_name

    log(f"[{dataset.topic}] Download -> {res.resource_url}")
    try:
        r = http_get(res.resource_url, stream=True)
        with out_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        log(f"[{dataset.topic}] Saved -> {out_path}")
        return out_path
    except Exception as e:
        log(f"[{dataset.topic}] Download failed: {e}")
        return None


def ensure_bronze_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(BRONZE_SCHEMA)))
    conn.commit()


def sniff_delimiter(sample: str) -> str:
    # CSV FR: souvent ';'
    # Essayons simple: si ';' plus fréquent que ','
    semi = sample.count(";")
    comma = sample.count(",")
    tab = sample.count("\t")
    if tab > semi and tab > comma:
        return "\t"
    if semi > comma:
        return ";"
    return ","


def read_csv_headers_and_rows(path: Path, max_preview_bytes: int = 200_000) -> Tuple[list[str], Iterable[list[str]], str]:
    """
    Retourne (headers, iterator_rows, delimiter) pour un fichier CSV/TSV/TXT.
    Les lignes sont renvoyées en listes de string.
    """
    # Détermine delimiter avec un sample
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        sample = f.read(max_preview_bytes)
    delim = "\t" if path.suffix.lower() == ".tsv" else sniff_delimiter(sample)

    def row_iter() -> Iterable[list[str]]:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as f2:
            reader = csv.reader(f2, delimiter=delim)
            for row in reader:
                yield ["" if v is None else str(v) for v in row]

    it = row_iter()
    try:
        headers = next(it)
    except StopIteration:
        return ([], iter(()), delim)

    # Clean headers
    headers = [slugify(h) if h else f"col_{i+1}" for i, h in enumerate(headers)]
    # dédoublonnage
    seen: dict[str, int] = {}
    uniq: list[str] = []
    for h in headers:
        if h not in seen:
            seen[h] = 1
            uniq.append(h)
        else:
            seen[h] += 1
            uniq.append(f"{h}_{seen[h]}")
    return (uniq, it, delim)


def extract_first_datafile_from_zip(zip_path: Path) -> Optional[Path]:
    """
    Extrait le premier fichier exploitable du zip vers le même dossier.
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            names = z.namelist()
            candidates = [n for n in names if n.lower().endswith(DATA_FILE_EXTS) and not n.endswith("/")]
            # priorise csv/xlsx
            def rank(n: str) -> int:
                nl = n.lower()
                if nl.endswith(".csv"):
                    return 100
                if nl.endswith(".xlsx") or nl.endswith(".xls"):
                    return 80
                if nl.endswith(".json") or nl.endswith(".geojson"):
                    return 60
                if nl.endswith(".tsv") or nl.endswith(".txt"):
                    return 50
                return 10
            candidates.sort(key=rank, reverse=True)
            if not candidates:
                return None
            pick = candidates[0]
            out_dir = zip_path.parent
            out_path = out_dir / Path(pick).name
            with z.open(pick) as src, out_path.open("wb") as dst:
                dst.write(src.read())
            return out_path
    except Exception:
        return None


def create_bronze_table(conn, table: str, columns: list[str]) -> None:
    cols = columns + ["_ingested_at", "_source_url", "_file_name", "_resource_url"]
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
                sql.Identifier(BRONZE_SCHEMA),
                sql.Identifier(table),
                sql.SQL(", ").join([sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in cols]),
            )
        )
    conn.commit()


def copy_rows_to_bronze(
    conn,
    table: str,
    columns: list[str],
    rows: Iterable[list[str]],
    dataset_url: str,
    file_name: str,
    resource_url: str,
    batch_size: int = 5000,
) -> int:
    """
    Charge via COPY en buffer CSV, par batch.
    """
    total = 0
    cols = columns + ["_ingested_at", "_source_url", "_file_name", "_resource_url"]
    copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',', QUOTE '\"', ESCAPE '\"')").format(
        sql.Identifier(BRONZE_SCHEMA),
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in cols),
    )

    ingested_at = dt.datetime.now().isoformat(timespec="seconds")

    def flush_batch(batch: list[list[str]]) -> int:
        if not batch:
            return 0
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        for r in batch:
            # pad / truncate
            r2 = (r + [""] * len(columns))[: len(columns)]
            r2 += [ingested_at, dataset_url, file_name, resource_url]
            w.writerow(r2)
        buf.seek(0)
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql.as_string(conn), buf)
        conn.commit()
        return len(batch)

    batch: list[list[str]] = []
    for row in rows:
        batch.append(["" if v is None else str(v) for v in row])
        if len(batch) >= batch_size:
            total += flush_batch(batch)
            batch = []
    total += flush_batch(batch)
    return total


def ingest_file_into_bronze(conn, dataset: Dataset, resource: Resource, file_path: Path) -> None:
    topic = dataset.topic
    table = safe_table_name(topic)

    # si zip -> extraire
    actual_path = file_path
    if actual_path.suffix.lower() == ".zip":
        extracted = extract_first_datafile_from_zip(actual_path)
        if not extracted:
            log(f"[{topic}] ZIP téléchargé mais aucun fichier data exploitable trouvé dedans.")
            return
        log(f"[{topic}] ZIP extracted -> {extracted}")
        actual_path = extracted

    ext = actual_path.suffix.lower()
    if ext not in (".csv", ".tsv", ".txt"):
        log(f"[{topic}] Format {ext} non géré pour chargement en bronze (CSV/TSV/TXT seulement).")
        log(f"[{topic}] Le fichier est quand même conservé en raw: {actual_path}")
        return

    headers, row_iter, delim = read_csv_headers_and_rows(actual_path)
    if not headers:
        log(f"[{topic}] Fichier vide / headers introuvables: {actual_path}")
        return

    log(f"[{topic}] Detected delimiter: {repr(delim)} | columns: {len(headers)}")
    ensure_bronze_schema(conn)
    create_bronze_table(conn, table, headers)

    # Recrée un iter rows avec le bon delimiter (on avait un iter déjà avancé),
    # donc on relit le fichier correctement (simple et fiable).
    def iter_rows() -> Iterable[list[str]]:
        with actual_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f, delimiter=delim)
            first = True
            for row in reader:
                if first:
                    first = False
                    continue
                yield row

    inserted = copy_rows_to_bronze(
        conn=conn,
        table=table,
        columns=headers,
        rows=iter_rows(),
        dataset_url=dataset.url,
        file_name=actual_path.name,
        resource_url=resource.resource_url,
    )
    log(f"[{topic}] Inserted rows: {inserted} into {BRONZE_SCHEMA}.{table}")


def main() -> int:
    try:
        datasets = load_datasets(DATA_LINK_PATH)
    except Exception as e:
        log(f"Erreur lecture data_link.json: {e}")
        return 1

    db_cfg = _get_db_config()
    log(f"DB -> host={db_cfg['host']} port={db_cfg['port']} db={db_cfg['dbname']} user={db_cfg['user']}")

    try:
        conn = psycopg2.connect(**db_cfg)
    except Exception as e:
        log(f"Connexion DB impossible: {e}")
        return 2

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    ok = 0
    ko = 0

    for ds in datasets:
        log(f"=== Dataset: {ds.topic} | {ds.name} ===")
        try:
            res = resolve_resource_url(ds)
            if not res:
                log(f"[{ds.topic}] Aucune ressource téléchargeable trouvée. Skip.")
                ko += 1
                continue

            file_path = download_resource(ds, res)
            if not file_path:
                ko += 1
                continue

            ingest_file_into_bronze(conn, ds, res, file_path)
            ok += 1

            # petite pause pour éviter de hammer les serveurs si boucle grosse
            time.sleep(0.5)

        except Exception as e:
            log(f"[{ds.topic}] Erreur inattendue: {e}")
            ko += 1

    try:
        conn.close()
    except Exception:
        pass

    log(f"Done. OK={ok} KO={ko}")
    return 0 if ok > 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
