"""Charge les datasets declares dans src/data/data_link.json.

Le script:
1) lit la liste des datasets,
2) tente de resoudre une URL de ressource telechargeable (Data.gouv API),
3) telecharge les fichiers dans src/data/raw,
4) genere un rapport JSON dans src/data/download_report.json.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Any

import requests


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
LINK_FILE = DATA_DIR / "data_link.json"
REPORT_FILE = DATA_DIR / "download_report.json"
LOG_FILE = DATA_DIR / "load_data.log"

# Fallback pour faciliter la transition depuis l'arborescence actuelle.
LEGACY_LINK_FILE = BASE_DIR.parent / "data" / "data_link.json"

PREFERRED_EXTENSIONS = (".csv", ".parquet", ".json", ".xlsx", ".xls", ".zip")


LOGGER = logging.getLogger("load_data")


def _setup_logging() -> None:
    """Configure des logs lisibles en console + fichier."""
    LOGGER.setLevel(logging.INFO)
    LOGGER.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)

    LOGGER.addHandler(console_handler)
    LOGGER.addHandler(file_handler)
    LOGGER.propagate = False


def _ensure_data_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def _ensure_link_file() -> None:
    """Garantit l'existence de src/data/data_link.json.

    Si le fichier n'existe pas encore, on recopie automatiquement le fichier legacy
    (data/data_link.json) quand il est disponible.
    """
    if LINK_FILE.exists():
        return
    if LEGACY_LINK_FILE.exists():
        shutil.copy2(LEGACY_LINK_FILE, LINK_FILE)
        LOGGER.info("data_link.json copie depuis %s", LEGACY_LINK_FILE)
        return
    raise FileNotFoundError(
        f"Fichier introuvable: {LINK_FILE}. "
        "Creez-le avec une cle 'datasets' contenant des objets {name, topic, url}."
    )


def _read_links() -> list[dict[str, Any]]:
    with LINK_FILE.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    datasets = payload.get("datasets", [])
    if not isinstance(datasets, list):
        raise ValueError("Le champ 'datasets' doit etre une liste.")
    return datasets


def _slug_from_data_gouv_dataset_url(url: str) -> str | None:
    match = re.search(r"/datasets/([^/?#]+)", url)
    return match.group(1) if match else None


def _resolve_download_url(url: str, timeout: int) -> tuple[str, str]:
    """Retourne (download_url, source_type).

    source_type:
    - 'resource': URL de ressource telechargeable resolue via API Data.gouv
    - 'direct': URL fournie directement
    """
    if "data.gouv.fr" not in url:
        return url, "direct"

    slug = _slug_from_data_gouv_dataset_url(url)
    if not slug:
        return url, "direct"

    api_url = f"https://www.data.gouv.fr/api/1/datasets/{slug}/"
    try:
        response = requests.get(api_url, timeout=timeout)
        response.raise_for_status()
        resources = response.json().get("resources", [])
    except Exception:
        return url, "direct"

    if not resources:
        return url, "direct"

    # On privilegie les ressources avec extension explicite.
    for resource in resources:
        resource_url = resource.get("url", "")
        if resource_url.lower().endswith(PREFERRED_EXTENSIONS):
            return resource_url, "resource"

    # Sinon on prend la premiere ressource disponible.
    candidate = resources[0].get("url")
    return (candidate, "resource") if candidate else (url, "direct")


def _sanitize_filename(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", value).strip("_") or "dataset"


def _extension_from_url_or_headers(url: str, headers: dict[str, str]) -> str:
    path_ext = Path(url.split("?")[0]).suffix.lower()
    if path_ext:
        return path_ext

    content_type = headers.get("Content-Type", "").lower()
    if "csv" in content_type:
        return ".csv"
    if "json" in content_type:
        return ".json"
    if "excel" in content_type or "spreadsheet" in content_type:
        return ".xlsx"
    if "zip" in content_type:
        return ".zip"
    return ".bin"


def _format_bytes(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"


def _print_progress(received: int, total: int | None, topic: str) -> None:
    bar_size = 28
    if total and total > 0:
        ratio = min(received / total, 1.0)
        filled = int(ratio * bar_size)
        bar = "#" * filled + "-" * (bar_size - filled)
        percent = f"{ratio * 100:6.2f}%"
        total_text = _format_bytes(total)
    else:
        bar = "#" * (received // (512 * 1024) % (bar_size + 1))
        bar = bar.ljust(bar_size, "-")
        percent = "  n/a "
        total_text = "unknown"

    line = (
        f"\r[{bar}] {percent} | "
        f"{_format_bytes(received)} / {total_text} | {topic[:24]}"
    )
    sys.stdout.write(line)
    sys.stdout.flush()


def _download_file(url: str, output_path: Path, timeout: int) -> dict[str, Any]:
    response = requests.get(url, stream=True, timeout=timeout)
    response.raise_for_status()

    total_size = int(response.headers.get("Content-Length", "0") or 0)
    expected_size: int | None = total_size if total_size > 0 else None
    received_size = 0
    last_render_ts = time.time()
    topic = output_path.stem

    with output_path.open("wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)
                received_size += len(chunk)
                now = time.time()
                if now - last_render_ts >= 0.08:
                    _print_progress(received_size, expected_size, topic)
                    last_render_ts = now

    _print_progress(received_size, expected_size, topic)
    sys.stdout.write("\n")

    return {
        "status_code": response.status_code,
        "content_type": response.headers.get("Content-Type"),
        "size_bytes": output_path.stat().st_size,
        "total_size_bytes": expected_size,
    }


def load_data(limit: int | None = None, timeout: int = 30, overwrite: bool = False) -> None:
    _ensure_data_dirs()
    _setup_logging()
    _ensure_link_file()

    datasets = _read_links()
    if limit is not None:
        datasets = datasets[:limit]

    LOGGER.info("Demarrage chargement datasets | total=%s | timeout=%ss | overwrite=%s", len(datasets), timeout, overwrite)

    report: list[dict[str, Any]] = []

    for index, dataset in enumerate(datasets, start=1):
        name = str(dataset.get("name", f"dataset_{index}"))
        topic = str(dataset.get("topic", f"topic_{index}"))
        source_url = str(dataset.get("url", "")).strip()

        if not source_url:
            report.append(
                {
                    "name": name,
                    "topic": topic,
                    "status": "skipped",
                    "reason": "missing_url",
                }
            )
            continue

        try:
            download_url, source_type = _resolve_download_url(source_url, timeout=timeout)
            head_headers: dict[str, str] = {}
            try:
                head_resp = requests.head(download_url, allow_redirects=True, timeout=timeout)
                head_headers = dict(head_resp.headers)
            except Exception:
                # Non bloquant: on tentera de deduire l'extension pendant le GET.
                pass

            file_stem = _sanitize_filename(topic)
            extension = _extension_from_url_or_headers(download_url, head_headers)
            output_path = RAW_DIR / f"{file_stem}{extension}"

            if output_path.exists() and not overwrite:
                report.append(
                    {
                        "name": name,
                        "topic": topic,
                        "status": "skipped",
                        "reason": "already_exists",
                        "file": str(output_path),
                    }
                )
                LOGGER.info("SKIP %s -> %s (deja present)", topic, output_path.name)
                continue

            LOGGER.info("START %s -> %s", topic, output_path.name)
            details = _download_file(download_url, output_path, timeout=timeout)
            report.append(
                {
                    "name": name,
                    "topic": topic,
                    "status": "downloaded",
                    "source_url": source_url,
                    "download_url": download_url,
                    "source_type": source_type,
                    "file": str(output_path),
                    **details,
                }
            )
            LOGGER.info(
                "OK %s -> %s | recu=%s | total=%s",
                topic,
                output_path.name,
                _format_bytes(int(details["size_bytes"])),
                _format_bytes(int(details["total_size_bytes"])) if details["total_size_bytes"] else "unknown",
            )

        except Exception as error:  # noqa: BLE001
            report.append(
                {
                    "name": name,
                    "topic": topic,
                    "status": "error",
                    "source_url": source_url,
                    "error": str(error),
                }
            )
            LOGGER.exception("ERR %s -> %s", topic, error)

    with REPORT_FILE.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=2, ensure_ascii=True)

    downloaded_count = sum(1 for item in report if item["status"] == "downloaded")
    error_count = sum(1 for item in report if item["status"] == "error")
    skipped_count = sum(1 for item in report if item["status"] == "skipped")

    LOGGER.info("=== Resume ===")
    LOGGER.info("Downloaded: %s", downloaded_count)
    LOGGER.info("Skipped:    %s", skipped_count)
    LOGGER.info("Errors:     %s", error_count)
    LOGGER.info("Report:     %s", REPORT_FILE)
    LOGGER.info("Logs:       %s", LOG_FILE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telecharge les datasets declares dans src/data/data_link.json")
    parser.add_argument("--limit", type=int, default=None, help="Nombre max de datasets a traiter")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout HTTP en secondes")
    parser.add_argument("--overwrite", action="store_true", help="Ecraser les fichiers deja telecharges")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    load_data(limit=arguments.limit, timeout=arguments.timeout, overwrite=arguments.overwrite)
