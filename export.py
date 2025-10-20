from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple
from urllib.parse import unquote

import requests

from getFabrik import (
    ACCOUNT_ORDER,
    SelectOptionParser,
    ensure_authenticated_session,
)  # type: ignore


ITEMS_ROOT = Path("itemsF")
ACCOUNT_PRODUCT_DIR_SUFFIX = "_I_P"
OUTPUT_ROOT_DIR = Path("CSVDATA")
ACCOUNT_OUTPUT_SUFFIX = "_P"
PRODUCTS_ENDPOINT = "/afterbuy/shop/produkte.aspx"
EXPORT_ENDPOINT = "/afterbuy/im-export.aspx"
GET_TIMEOUT = 30
POST_TIMEOUT = 180
STREAM_CHUNK_SIZE = 64 * 1024

DEFAULT_EXPPROD = "3"
DEFAULT_EXPORT_FORMAT_ID = "72402"
DEFAULT_EXPORT_DEFINITION = "72402"
DEFAULT_EXPORT_ENCODING = "1"
DEFAULT_SAVE_EXPORT_ENCODING = "1"

PRODUCT_REFERER_TEMPLATE = (
    "https://{domain}/afterbuy/shop/produkte.aspx?"
    "Su_Suchbegriff=&PRFilter=&Su_Suchbegriff_lg=0&PRFilter1=&Artikelnummer_Search=&"
    "level__Search=&Attributwert_Search=&EAN_Search=&Su_Listenlaenge=500&Su_Listenlaenge_Ges=500&"
    "MyFreifeld=0&Suche_BestandOP=&Suche_Bestand_Wert=0&MyFreifeldValue=&Suche_ABestandOP=&"
    "Suche_ABestand_Wert=0&Katalog_Filter={factory_id}&Katalog_Filter_Kat2=0&"
    "Katalog_Filter_Kat3=0&Katalog_Filter_Kat4=0&Katalog_Filter_Kat5=0&"
    "StandardProductIDValue_Search=&versandgruppe=&versandgruppe_art=0&vorlage=&"
    "vorlageart=0&Product_Search_Stocklocation_1=&Product_Search_Stocklocation_1_Value=&"
    "Product_Search_Stocklocation_2=&Product_Search_Stocklocation_2_Value=&"
    "Product_Search_Stocklocation_3=&Product_Search_Stocklocation_3_Value=&"
    "Product_Search_Stocklocation_4=&Product_Search_Stocklocation_4_Value=&"
    "productSearchSupplier1=0&productSearchSupplier2=0&productSearchSupplier3=0&"
    "productSearchSupplier4=0&ProductSearchSku=&LastSaleFrom=&ProductSearchMpn=&"
    "LastSaleTo=&ProductSearchFeatureId0=0&ProductSearchFeatureValue0=&"
    "ProductSearchFeatureId1=0&ProductSearchFeatureValue1=&"
    "ProductSearchFeatureId2=0&ProductSearchFeatureValue2=&"
    "ProductSearchFeatureId3=0&ProductSearchFeatureValue3=&"
    "ProductSearchFeatureId4=0&ProductSearchFeatureValue4=&"
    "productSearchUserTag1=0&productSearchUserTag2=0&productSearchUserTag3=0&"
    "productSearchUserTag4=0&SuchZusatzfeld1=&SuchZusatzfeld2=&SuchZusatzfeld3=&"
    "SuchZusatzfeld4=&SuchZusatzfeld5=&SuchZusatzfeld6=&spoid=0&art=SetAuswahl&ShowAdditionalFields=1"
)


@dataclass(slots=True)
class FactoryExportTask:
    """Container with data required to request a CSV export for a factory."""

    factory_id: str
    factory_name: str
    item_ids: List[str]
    source_path: Path

    def default_filename(self) -> str:
        """Build a filesystem-friendly filename for the factory export."""
        return build_filename(self.factory_name, self.factory_id)


@dataclass(slots=True)
class ExportConfig:
    """Holds resolved parameters for triggering the export download."""

    definition_id: str
    export_format_id: str
    export_encoding: str
    save_export_encoding: str
    expprod: Optional[str]


def build_filename(name: str, factory_id: str) -> str:
    """Produce a safe ASCII filename for the export."""
    base = f"{name}_{factory_id}".strip("_") if name else f"factory_{factory_id}"
    normalized = unicodedata.normalize("NFKD", base)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", ascii_name).strip("._-")
    if not cleaned:
        cleaned = f"factory_{factory_id}"
    return f"{cleaned}.csv"


def load_factory_from_json(path: Path) -> FactoryExportTask:
    """Load factory metadata (including item ids) from a JSON export file."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    factory_id = str(raw.get("factory_id") or "")
    factory_name = str(raw.get("factory_name") or "")
    item_ids_raw = raw.get("item_ids")
    if not factory_id:
        raise ValueError(f"Файл {path} не содержит 'factory_id'.")
    if not isinstance(item_ids_raw, Iterable):
        raise ValueError(f"Файл {path} не содержит список 'item_ids'.")

    item_ids: List[str] = []
    for item in item_ids_raw:
        value = str(item).strip()
        if value:
            item_ids.append(value)
    if not item_ids:
        raise ValueError(f"Файл {path} содержит пустой список 'item_ids'.")

    return FactoryExportTask(
        factory_id=factory_id,
        factory_name=factory_name,
        item_ids=item_ids,
        source_path=path,
    )


def build_export_form_payload(item_ids: Sequence[str]) -> List[Tuple[str, str]]:
    """Construct the POST form payload expected by the Afterbuy export endpoint."""
    if not item_ids:
        raise ValueError("Список item_ids пуст — нечего экспортировать.")

    form: List[Tuple[str, str]] = [("art2", "selectexportauswahl")]
    for item_id in item_ids:
        stripped = item_id.strip()
        if not stripped:
            continue
        form.append(("id", stripped))
        form.append((f"Bestand_{stripped}", "0"))
        form.append((f"ABestand_{stripped}", "0"))

    joined_ids = ",".join(item_ids)
    form.extend(
        [
            ("art", "selectexportauswahl"),
            ("updtart", ""),
            ("allmyupdtids", joined_ids),
            ("rsposition", "0"),
            ("spoid", "0"),
            ("ListerHistory_Stammid", ""),
        ]
    )
    return form


def build_export_definition_payload(
    item_ids: Sequence[str],
    *,
    expprod: Optional[str],
    export_format_id: Optional[str],
    export_encoding: str,
    save_export_encoding: str,
    definition: str,
) -> List[Tuple[str, str]]:
    """Construct the POST payload used by the im-export endpoint."""
    joined_ids = ",".join(item_ids)
    payload: List[Tuple[str, str]] = []
    if expprod is not None:
        payload.append(("expprod", expprod))
    if export_format_id is not None:
        payload.append(("ExportFormatID", export_format_id))
    payload.extend(
        [
            ("ExportEncoding", export_encoding),
            ("saveExportEncoding", save_export_encoding),
            ("id", joined_ids),
            ("art", "export"),
            ("definition", definition),
            ("isProductExport", "1"),
        ]
    )
    return payload


def detect_produkte_export_definition(
    session: requests.Session,
    domain: str,
    *,
    preferred_label: str = "ProdukteExport",
) -> Optional[str]:
    """Detect the definition id for the ProdukteExport entry on the export page."""
    url = f"https://{domain}{EXPORT_ENDPOINT}"
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "max-age=0",
        "Referer": url,
    }
    response = session.get(url, headers=headers, timeout=GET_TIMEOUT)
    response.raise_for_status()
    text_lower = response.text.lower()
    if "form-signin" in text_lower:
        raise RuntimeError("Сессия не авторизована — получена страница логина.")

    parser = SelectOptionParser("definition")
    parser.feed(response.text)
    preferred_lower = preferred_label.lower()
    fallback: Optional[str] = None
    for value, label in parser.options:
        value = value.strip()
        label_clean = label.strip()
        if not value or value == "0":
            continue
        if preferred_lower in label_clean.lower():
            return value
        if fallback is None:
            fallback = value
    return fallback


def _parse_content_disposition_filename(header_value: Optional[str]) -> Optional[str]:
    """Extract filename from Content-Disposition header if present."""
    if not header_value:
        return None
    value = header_value.strip()
    if not value:
        return None

    match = re.search(
        r'filename\*=(?:UTF-8\'\')(?P<name>[^;]+)',
        value,
        flags=re.IGNORECASE,
    )
    if match:
        raw = match.group("name").strip().strip('"')
        return unquote(raw)

    match = re.search(
        r'filename=(?P<name>"[^"]+"|[^;]+)',
        value,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    candidate = match.group("name").strip()
    if candidate.startswith('"') and candidate.endswith('"'):
        candidate = candidate[1:-1]
    return candidate


def _sanitize_candidate_filename(candidate: str, fallback: str) -> str:
    """Sanitize a filename provided by the server before persisting it."""
    leaf = candidate.split("/")[-1].split("\\")[-1]
    normalized = unicodedata.normalize("NFKD", leaf)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", ascii_name).strip("._-")
    if not cleaned:
        return fallback
    if not cleaned.lower().endswith(".csv"):
        cleaned = f"{cleaned}.csv"
    return cleaned[:160]


def prepare_product_page(
    session: requests.Session, domain: str, factory_id: str
) -> Tuple[str, str]:
    """Prime session state by loading the product page with catalog filter applied."""
    base_url = f"https://{domain}{PRODUCTS_ENDPOINT}"
    referer_url = PRODUCT_REFERER_TEMPLATE.format(domain=domain, factory_id=factory_id)
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "max-age=0",
        "Referer": base_url,
    }
    response = session.get(referer_url, headers=headers, timeout=GET_TIMEOUT)
    response.raise_for_status()
    text_lower = response.text.lower()
    if "form-signin" in text_lower:
        raise RuntimeError("Сессия не авторизована — получена страница логина.")
    return base_url, referer_url


def resolve_export_config(
    session: requests.Session,
    domain: str,
    *,
    definition_override: Optional[str],
    export_format_override: Optional[str],
    export_encoding: Optional[str],
    save_export_encoding: Optional[str],
    expprod: Optional[str],
) -> ExportConfig:
    """Determine the export settings for the current account."""
    if definition_override:
        definition_id = definition_override
    else:
        detected = detect_produkte_export_definition(session, domain)
        if not detected:
            raise RuntimeError(
                "Не удалось определить идентификатор определения 'ProdukteExport'. "
                "Укажите его вручную параметром --definition-id."
            )
        definition_id = detected

    export_format_id = export_format_override or definition_id
    encoding_value = export_encoding or DEFAULT_EXPORT_ENCODING
    save_encoding_value = save_export_encoding or DEFAULT_SAVE_EXPORT_ENCODING

    return ExportConfig(
        definition_id=definition_id,
        export_format_id=export_format_id,
        export_encoding=encoding_value,
        save_export_encoding=save_encoding_value,
        expprod=expprod,
    )


def download_factory_csv(
    session: requests.Session,
    domain: str,
    factory: FactoryExportTask,
    output_dir: Path,
    *,
    expprod: str,
    export_format_id: str,
    definition: str,
    export_encoding: str,
    save_export_encoding: str,
) -> Path:
    """Download and persist the CSV export for a specific factory."""
    output_dir.mkdir(parents=True, exist_ok=True)

    page_url, referer_with_query = prepare_product_page(
        session, domain, factory.factory_id
    )
    selection_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": referer_with_query,
        "Origin": f"https://{domain}",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "max-age=0",
    }
    selection_payload = build_export_form_payload(factory.item_ids)
    selection_response = session.post(
        page_url,
        data=selection_payload,
        headers=selection_headers,
        timeout=POST_TIMEOUT,
    )
    selection_response.raise_for_status()
    if "form-signin" in selection_response.text.lower():
        raise RuntimeError("Сессия не авторизована — получена страница логина.")
    selection_response.close()

    export_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": page_url,
        "Origin": f"https://{domain}",
        "Accept": "text/csv, text/plain, application/octet-stream, */*;q=0.8",
        "Cache-Control": "max-age=0",
    }
    export_payload = build_export_definition_payload(
        factory.item_ids,
        expprod=expprod,
        export_format_id=export_format_id,
        export_encoding=export_encoding,
        save_export_encoding=save_export_encoding,
        definition=definition,
    )
    export_url = f"https://{domain}{EXPORT_ENDPOINT}"
    response = session.post(
        export_url,
        data=export_payload,
        headers=export_headers,
        timeout=POST_TIMEOUT,
        stream=True,
    )
    response.raise_for_status()

    content_type = (response.headers.get("Content-Type") or "").lower()

    fallback_name = factory.default_filename()
    filename = fallback_name
    target_path = output_dir / filename

    chunk_iter = response.iter_content(chunk_size=STREAM_CHUNK_SIZE)
    first_chunk: Optional[bytes] = None
    for chunk in chunk_iter:
        if chunk:
            first_chunk = chunk
            break
    if first_chunk is None:
        first_chunk = b""

    if "text/html" in content_type and "csv" not in content_type:
        preview = first_chunk.decode("utf-8", errors="ignore")
        response.close()
        raise RuntimeError(
            "Ожидался CSV-файл, но получен HTML документ. "
            f"Возможна ошибка авторизации. Ответ сервера: {preview[:200]}"
        )

    if not first_chunk:
        response.close()
        raise RuntimeError("Ответ сервера пуст — экспорт не был сформирован.")

    with target_path.open("wb") as handle:
        if first_chunk:
            handle.write(first_chunk)
        for chunk in chunk_iter:
            if chunk:
                handle.write(chunk)

    response.close()
    return target_path


def _normalize_sequence(values: Optional[Sequence[str]]) -> List[str]:
    normalized: List[str] = []
    if not values:
        return normalized
    for raw in values:
        if raw is None:
            continue
        for piece in str(raw).split(","):
            cleaned = piece.strip()
            if cleaned:
                normalized.append(cleaned)
    return normalized


def discover_factories_for_account(
    account: str,
    factory_ids: Sequence[str],
    name_filters: Sequence[str],
    limit: Optional[int],
) -> List[FactoryExportTask]:
    account_key = account.upper()
    directory = ITEMS_ROOT / f"{account_key}{ACCOUNT_PRODUCT_DIR_SUFFIX}"
    if not directory.exists():
        logging.warning(
            "Каталог с фабриками не найден: %s (аккаунт %s)",
            directory,
            account_key,
        )
        return []

    id_set = {value.strip() for value in factory_ids if value.strip()}
    name_patterns = [value.lower() for value in name_filters if value]

    tasks: List[FactoryExportTask] = []
    json_files = sorted(directory.glob("*.json"))
    for path in json_files:
        try:
            task = load_factory_from_json(path)
        except Exception as exc:  # noqa: BLE001
            logging.error("Не удалось прочитать %s: %s", path, exc)
            continue
        if id_set and task.factory_id not in id_set:
            continue
        if name_patterns:
            candidate = (task.factory_name or "").lower()
            if not any(pattern in candidate for pattern in name_patterns):
                continue
        tasks.append(task)
        if limit is not None and len(tasks) >= limit:
            break
    return tasks


def find_existing_export(output_dir: Path, factory: FactoryExportTask) -> Optional[Path]:
    """Check whether a CSV for the factory already exists in the output folder."""
    if not output_dir.exists():
        return None
    pattern = f"*{factory.factory_id}*.csv"
    for candidate in output_dir.glob(pattern):
        if candidate.is_file():
            return candidate
    fallback = output_dir / factory.default_filename()
    return fallback if fallback.is_file() else None


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Экспорт CSV с товарами Afterbuy для выбранных фабрик."
    )
    parser.add_argument(
        "--account",
        choices=ACCOUNT_ORDER,
        type=str.upper,
        action="append",
        help="Ограничить обработку указанными аккаунтами (можно несколько). "
        "По умолчанию используются все доступные аккаунты.",
    )
    parser.add_argument(
        "--factory-id",
        dest="factory_ids",
        action="append",
        metavar="ID",
        help="Фильтровать фабрики по идентификатору (можно несколько значений или через запятую).",
    )
    parser.add_argument(
        "--factory-name",
        dest="factory_names",
        action="append",
        metavar="SUBSTR",
        help="Фильтр по части названия фабрики (регистронезависимо).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Ограничить число фабрик на аккаунт первыми N после фильтрации.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_ROOT_DIR,
        help="Каталог для сохранения CSV (по умолчанию CSVDATA).",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Пропускать фабрики, если найден уже существующий CSV с тем же factory_id.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Показать список фабрик и выйти без выполнения HTTP-запросов.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Включить подробный журнальный вывод.",
    )
    parser.add_argument(
        "--definition-id",
        default=None,
        help="Идентификатор определения (definition). По умолчанию определяется автоматически по записи 'ProdukteExport'.",
    )
    parser.add_argument(
        "--export-format-id",
        default=None,
        help="Значение параметра ExportFormatID. По умолчанию используется то же значение, что и для definition.",
    )
    parser.add_argument(
        "--expprod",
        default=DEFAULT_EXPPROD,
        help="Значение параметра expprod (по умолчанию 3).",
    )
    parser.add_argument(
        "--export-encoding",
        default=DEFAULT_EXPORT_ENCODING,
        help="Значение параметра ExportEncoding (по умолчанию 1).",
    )
    parser.add_argument(
        "--save-export-encoding",
        default=DEFAULT_SAVE_EXPORT_ENCODING,
        help="Значение параметра saveExportEncoding (по умолчанию 1).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)

    accounts = args.account if args.account else list(ACCOUNT_ORDER)
    accounts = [account.upper() for account in accounts]

    factory_ids = _normalize_sequence(args.factory_ids)
    name_filters = _normalize_sequence(args.factory_names)

    for account in accounts:
        tasks = discover_factories_for_account(
            account,
            factory_ids,
            name_filters,
            args.limit,
        )
        if not tasks:
            logging.info("Аккаунт %s: подходящих фабрик не найдено.", account)
            continue

        logging.info(
            "Аккаунт %s: отобрано %d фабрик для экспорта.",
            account,
            len(tasks),
        )

        if args.dry_run:
            for task in tasks:
                logging.info(
                    "  • %s (%s) — %d товаров (из %s)",
                    task.factory_name or "<без названия>",
                    task.factory_id,
                    len(task.item_ids),
                    task.source_path,
                )
            continue

        try:
            session, domain = ensure_authenticated_session(account)
        except Exception as exc:  # noqa: BLE001
            logging.error(
                "Не удалось подготовить авторизованную сессию для аккаунта %s: %s",
                account,
                exc,
            )
            continue

        try:
            export_config = resolve_export_config(
                session,
                domain,
                definition_override=args.definition_id,
                export_format_override=args.export_format_id,
                export_encoding=args.export_encoding,
                save_export_encoding=args.save_export_encoding,
                expprod=args.expprod,
            )
            if args.verbose:
                logging.debug(
                    "[%s] Используется definition=%s, export_format=%s, encoding=%s",
                    account,
                    export_config.definition_id,
                    export_config.export_format_id,
                    export_config.export_encoding,
                )

            account_output_dir = args.output_dir / f"{account}{ACCOUNT_OUTPUT_SUFFIX}"
            total = len(tasks)
            for index, task in enumerate(tasks, start=1):
                if args.skip_existing:
                    existing = find_existing_export(account_output_dir, task)
                    if existing:
                        logging.info(
                            "[%s %d/%d] Пропуск: CSV уже существует (%s).",
                            account,
                            index,
                            total,
                            existing,
                        )
                        continue

                logging.info(
                    "[%s %d/%d] Экспорт фабрики %s (%s) — %d товаров.",
                    account,
                    index,
                    total,
                    task.factory_name or "<без названия>",
                    task.factory_id,
                    len(task.item_ids),
                )
                try:
                    output_path = download_factory_csv(
                        session=session,
                        domain=domain,
                        factory=task,
                        output_dir=account_output_dir,
                        expprod=export_config.expprod,
                        export_format_id=export_config.export_format_id,
                        definition=export_config.definition_id,
                        export_encoding=export_config.export_encoding,
                        save_export_encoding=export_config.save_export_encoding,
                    )
                except Exception as exc:  # noqa: BLE001
                    logging.error(
                        "[%s %d/%d] Ошибка при экспорте %s (%s): %s",
                        account,
                        index,
                        total,
                        task.factory_name or "<без названия>",
                        task.factory_id,
                        exc,
                    )
                    continue

                logging.info(
                    "[%s %d/%d] CSV сохранён в %s",
                    account,
                    index,
                    total,
                    output_path,
                )
        finally:
            session.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
