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

import requests

from logging_utils import setup_logging
from getFabrik import (
    ACCOUNT_ORDER,
    SelectOptionParser,
    ensure_authenticated_session,
)  # type: ignore


ITEMS_ROOT = Path("itemsF")
ACCOUNT_LISTER_DIR_SUFFIX = "_I_L"
OUTPUT_ROOT_DIR = Path("CSVDATA")
ACCOUNT_OUTPUT_SUFFIX = "_L"
LISTER_ENDPOINT = "/afterbuy/ebayliste2.aspx"
EXPORT_ENDPOINT = "/afterbuy/im-export.aspx"
GET_TIMEOUT = 30
POST_TIMEOUT = 180
STREAM_CHUNK_SIZE = 64 * 1024

DEFAULT_EXPPROD = "3"
DEFAULT_EXPORT_DEFINITION = "72404"
DEFAULT_EXPORT_FORMAT_ID = "72404"
DEFAULT_EXPORT_ENCODING = "1"

LISTER_REFERER_TEMPLATE = (
    "https://{domain}/afterbuy/ebayliste2.aspx?"
    "art=SetAuswahl&lAWSuchwort=&lAWFilter=0&lAWFilter2=0&I_Stammartikel=&siteIDsuche=-1&"
    "lAWartikelnummer=&lAWKollektion={factory_id}&lAWKollektion1=-1&lAWKollektion2=-1&"
    "lAWKollektion3=-1&lAWKollektion4=-1&lAWKollektion5=-1&lAWean=&Vorlage=&Vorlageart=0&"
    "lAWebaykat=&lAWshopcat1=-1&lAWshopcat2=-1&lawmaxart=20&maxgesamt=500&BlockingReason=&"
    "DispatchTimeMax=-1&listerId=&ebayLister_DynamicPriceRules=-100&"
    "lAWSellerPaymentProfile=0&lAWSellerReturnPolicyProfile=0&lAWSellerShippingProfile=0"
)

LOGGER = logging.getLogger("exportLister")

@dataclass(slots=True)
class ListerExportTask:
    factory_id: str
    factory_name: str
    item_ids: List[str]
    source_path: Path

    def default_filename(self) -> str:
        return build_filename(self.factory_name, self.factory_id)


@dataclass(slots=True)
class ExportConfig:
    definition_id: str
    export_format_id: str
    export_encoding: str
    expprod: Optional[str]


def build_filename(name: str, factory_id: str) -> str:
    base = f"{name}_{factory_id}".strip("_") if name else f"factory_{factory_id}"
    normalized = unicodedata.normalize("NFKD", base)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", ascii_name).strip("._-")
    if not cleaned:
        cleaned = f"factory_{factory_id}"
    return f"{cleaned}.csv"


def load_factory_from_json(path: Path) -> ListerExportTask:
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

    return ListerExportTask(
        factory_id=factory_id,
        factory_name=factory_name,
        item_ids=item_ids,
        source_path=path,
    )


def build_selection_payload(item_ids: Sequence[str]) -> List[Tuple[str, str]]:
    if not item_ids:
        raise ValueError("Список item_ids пуст — нечего экспортировать.")

    payload: List[Tuple[str, str]] = [("art2", "selectexportauswahl"), ("Lister_Button", "Ausführen")]
    for item_id in item_ids:
        clean_id = item_id.strip()
        if not clean_id:
            continue
        payload.append(("id", clean_id))
        payload.append((f"said_{clean_id}", "0"))
        payload.append((f"vtid_{clean_id}", "0"))
        payload.append((f"Menge_{clean_id}", "0"))
        payload.append((f"vid_{clean_id}", "0"))

    joined_ids = ",".join(item_ids)
    payload.extend(
        [
            ("art", "selectexportauswahl"),
            ("updtart", ""),
            ("allmyupdtids", joined_ids),
            ("rsposition", "0"),
            ("mehrfachauswahl", "1"),
            ("arttmp", ""),
            ("idtmp", ""),
            ("lister", "ebay"),
            ("CopyToListerIds", ""),
        ]
    )
    return payload


def build_export_definition_payload(
    item_ids: Sequence[str],
    *,
    expprod: Optional[str],
    export_format_id: Optional[str],
    export_encoding: str,
    definition: str,
) -> List[Tuple[str, str]]:
    joined_ids = ",".join(item_ids)
    payload: List[Tuple[str, str]] = []
    if expprod is not None:
        payload.append(("expprod", expprod))
    payload.extend(
        [
            ("ExportEncoding", export_encoding),
        ]
    )
    if export_format_id is not None:
        payload.append(("ExportFormatID", export_format_id))
    payload.extend(
        [
            ("id", joined_ids),
            ("art", "export"),
            ("definition", definition),
        ]
    )
    return payload


def detect_lister_definition(
    session: requests.Session,
    domain: str,
    *,
    preferred_label: str = "Lister",
) -> Optional[str]:
    url = f"https://{domain}{EXPORT_ENDPOINT}"
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "max-age=0",
        "Referer": url,
    }
    response = session.get(url, headers=headers, timeout=GET_TIMEOUT)
    response.raise_for_status()
    if "form-signin" in response.text.lower():
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


def prepare_lister_page(
    session: requests.Session,
    domain: str,
    factory_id: str,
) -> Tuple[str, str]:
    base_url = f"https://{domain}{LISTER_ENDPOINT}"
    base_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "max-age=0",
    }
    base_response = session.get(base_url, headers=base_headers, timeout=GET_TIMEOUT)
    base_response.raise_for_status()
    if "form-signin" in base_response.text.lower():
        raise RuntimeError("Сессия не авторизована — получена страница логина.")

    referer_url = LISTER_REFERER_TEMPLATE.format(domain=domain, factory_id=factory_id)
    filter_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "max-age=0",
        "Referer": base_url,
    }
    filter_response = session.get(referer_url, headers=filter_headers, timeout=GET_TIMEOUT)
    filter_response.raise_for_status()
    if "form-signin" in filter_response.text.lower():
        raise RuntimeError("Сессия не авторизована — получена страница логина.")
    return base_url, referer_url


def download_lister_csv(
    session: requests.Session,
    domain: str,
    task: ListerExportTask,
    output_dir: Path,
    *,
    config: ExportConfig,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    base_url, referer_url = prepare_lister_page(session, domain, task.factory_id)

    selection_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": referer_url,
        "Origin": f"https://{domain}",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "max-age=0",
    }
    selection_payload = build_selection_payload(task.item_ids)

    for _ in range(2):
        selection_response = session.post(
            base_url,
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
        "Referer": base_url,
        "Origin": f"https://{domain}",
        "Accept": "text/csv, text/plain, application/octet-stream, */*;q=0.8",
        "Cache-Control": "max-age=0",
    }
    export_payload = build_export_definition_payload(
        task.item_ids,
        expprod=config.expprod,
        export_format_id=config.export_format_id,
        export_encoding=config.export_encoding,
        definition=config.definition_id,
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
    filename = task.default_filename()
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


def discover_lister_tasks(
    account: str,
    factory_ids: Sequence[str],
    name_filters: Sequence[str],
    limit: Optional[int],
) -> List[ListerExportTask]:
    account_key = account.upper()
    directory = ITEMS_ROOT / f"{account_key}{ACCOUNT_LISTER_DIR_SUFFIX}"
    if not directory.exists():
        LOGGER.warning(
            "Каталог с коллекциями не найден: %s (аккаунт %s)",
            directory,
            account_key,
        )
        return []

    id_set = {value.strip() for value in factory_ids if value.strip()}
    name_patterns = [value.lower() for value in name_filters if value]

    tasks: List[ListerExportTask] = []
    for path in sorted(directory.glob("*.json")):
        try:
            task = load_factory_from_json(path)
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Не удалось прочитать %s: %s", path, exc)
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


def find_existing_export(output_dir: Path, task: ListerExportTask) -> Optional[Path]:
    if not output_dir.exists():
        return None
    pattern = f"*{task.factory_id}*.csv"
    for candidate in output_dir.glob(pattern):
        if candidate.is_file():
            return candidate
    fallback = output_dir / task.default_filename()
    return fallback if fallback.is_file() else None


def configure_logging(verbose: bool) -> logging.Logger:
    console_level = logging.DEBUG if verbose else logging.INFO
    return setup_logging("exportLister", console_level=console_level)


def resolve_export_config(
    session: requests.Session,
    domain: str,
    *,
    definition_override: Optional[str],
    export_format_override: Optional[str],
    export_encoding: Optional[str],
    expprod: Optional[str],
) -> ExportConfig:
    if definition_override:
        definition_id = definition_override
    else:
        detected = detect_lister_definition(session, domain)
        if not detected:
            LOGGER.warning(
                "Не удалось определить definition автоматически, используется значение по умолчанию %s",
                DEFAULT_EXPORT_DEFINITION,
            )
            definition_id = DEFAULT_EXPORT_DEFINITION
        else:
            definition_id = detected

    export_format_id = export_format_override or definition_id or DEFAULT_EXPORT_FORMAT_ID
    encoding_value = export_encoding or DEFAULT_EXPORT_ENCODING

    return ExportConfig(
        definition_id=definition_id,
        export_format_id=export_format_id,
        export_encoding=encoding_value,
        expprod=expprod,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Экспорт CSV по eBay Lister для выбранных коллекций."
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
        help="Фильтровать коллекции по идентификатору (можно несколько значений или через запятую).",
    )
    parser.add_argument(
        "--factory-name",
        dest="factory_names",
        action="append",
        metavar="SUBSTR",
        help="Фильтр по части названия коллекции (регистронезависимо).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Ограничить число коллекций на аккаунт первыми N после фильтрации.",
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
        help="Пропускать коллекции, если найден уже существующий CSV с тем же factory_id.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Показать список коллекций и выйти без выполнения HTTP-запросов.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Включить подробный журнальный вывод.",
    )
    parser.add_argument(
        "--definition-id",
        default=None,
        help="Идентификатор определения (definition). По умолчанию определяется автоматически по записи 'Lister'.",
    )
    parser.add_argument(
        "--export-format-id",
        default=None,
        help="Значение параметра ExportFormatID. По умолчанию совпадает с definition.",
    )
    parser.add_argument(
        "--expprod",
        default=DEFAULT_EXPPROD,
        help="Значение параметра expprod (по умолчанию 3).",
    )
    parser.add_argument(
        "--export-encoding",
        default=DEFAULT_EXPORT_ENCODING,
        help="Значение параметра ExportEncoding (по умолчанию 1, UTF-8).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = configure_logging(args.verbose)

    accounts = args.account if args.account else list(ACCOUNT_ORDER)
    logger.info(
        "Старт экспорта листера. Аккаунты: %s",
        ", ".join(accounts) if accounts else "<нет>",
    )
    accounts = [account.upper() for account in accounts]

    factory_ids = _normalize_sequence(args.factory_ids)
    name_filters = _normalize_sequence(args.factory_names)

    for account in accounts:
        tasks = discover_lister_tasks(
            account,
            factory_ids,
            name_filters,
            args.limit,
        )
        if not tasks:
            LOGGER.info("Аккаунт %s: подходящих коллекций не найдено.", account)
            continue

        LOGGER.info(
            "Аккаунт %s: отобрано %d коллекций для экспорта.",
            account,
            len(tasks),
        )

        if args.dry_run:
            for task in tasks:
                LOGGER.info(
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
            LOGGER.error(
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
                expprod=args.expprod,
            )
            if args.verbose:
                LOGGER.debug(
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
                        LOGGER.info(
                            "[%s %d/%d] Пропуск: CSV уже существует (%s).",
                            account,
                            index,
                            total,
                            existing,
                        )
                        continue

                LOGGER.info(
                    "[%s %d/%d] Экспорт коллекции %s (%s) — %d товаров.",
                    account,
                    index,
                    total,
                    task.factory_name or "<без названия>",
                    task.factory_id,
                    len(task.item_ids),
                )
                try:
                    output_path = download_lister_csv(
                        session=session,
                        domain=domain,
                        task=task,
                        output_dir=account_output_dir,
                        config=export_config,
                    )
                except Exception as exc:  # noqa: BLE001
                    LOGGER.error(
                        "[%s %d/%d] Ошибка при экспорте %s (%s): %s",
                        account,
                        index,
                        total,
                        task.factory_name or "<без названия>",
                        task.factory_id,
                        exc,
                    )
                    continue

                LOGGER.info(
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
