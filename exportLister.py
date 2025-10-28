from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import tempfile
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Tuple


class EmptyItemIdsError(ValueError):
    """Raised when items file contains no item_ids."""

import requests
from requests.exceptions import Timeout

from logging_utils import setup_logging
from getFabrik import (
    ACCOUNT_ORDER,
    SelectOptionParser,
    ensure_authenticated_session,
)  # type: ignore


# ===== Константы =====
ITEMS_ROOT = Path("itemsF")
ACCOUNT_LISTER_DIR_SUFFIX = "_I_L"
OUTPUT_ROOT_DIR = Path("CSVDATA")
ACCOUNT_OUTPUT_SUFFIX = "_L"
LISTER_ENDPOINT = "/afterbuy/ebayliste2.aspx"
EXPORT_ENDPOINT = "/afterbuy/im-export.aspx"
GET_TIMEOUT = 30
POST_TIMEOUT = 180
STREAM_CHUNK_SIZE = 1024 * 1024  # 1 MiB

DEFAULT_EXPPROD = "3"
DEFAULT_EXPORT_DEFINITION = "72404"
DEFAULT_EXPORT_FORMAT_ID = "72404"
DEFAULT_EXPORT_ENCODING = "1"

LISTER_REFERER_TEMPLATE = (
    "https://{domain}/afterbuy/ebayliste2.aspx?"
    "art=SetAuswahl&lAWSuchwort=&lAWFilter=0&lAWFilter2=0&I_Stammartikel=&siteIDsuche=-1&"
    "lAWartikelnummer=&lAWKollektion={factory_id}&lAWKollektion1=-1&lAWKollektion2=-1&"
    "lAWKollektion3=-1&lAWKollektion4=-1&lAWKollektion5=-1&lAWean=&Vorlage=&Vorlageart=0&"
    "lAWebaykat=&lAWshopcat1=-1&lAWshopcat2=-1&lawmaxart=500&maxgesamt=15000&BlockingReason=&"
    "DispatchTimeMax=-1&listerId=&ebayLister_DynamicPriceRules=-100&"
    "lAWSellerPaymentProfile=0&lAWSellerReturnPolicyProfile=0&lAWSellerShippingProfile=0"
)

LOGGER = logging.getLogger("exportLister")


# ===== Модели =====
@dataclass(slots=True)
class ListerExportTask:
    factory_id: str
    factory_name: str
    item_ids: List[str]
    source_path: Path

    def default_filename(self) -> str:
        return build_filename(self.factory_name, self.factory_id)

    @property
    def expected_count(self) -> int:
        return len(self.item_ids)


@dataclass(slots=True)
class ExportConfig:
    definition_id: str
    export_format_id: str
    export_encoding: str
    expprod: Optional[str]


# ===== Утилиты =====
def build_filename(name: str, factory_id: str) -> str:
    base = f"{name}_{factory_id}".strip("_") if name else f"factory_{factory_id}"
    normalized = unicodedata.normalize("NFKD", base)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", ascii_name).strip("._-")
    if not cleaned:
        cleaned = f"factory_{factory_id}"
    return f"{cleaned}.csv"[:160]


def _atomic_write_bytes(target: Path, first_chunk: bytes, chunk_iter) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as tmp:
        tmp_path = Path(tmp.name)
        if first_chunk:
            tmp.write(first_chunk)
        for c in chunk_iter:
            if c:
                tmp.write(c)
        tmp.flush()
        os.fsync(tmp.fileno())
    tmp_path.replace(target)


def _normalize_sequence(values: Optional[Sequence[str]]) -> List[str]:
    seen, out = set(), []
    if not values:
        return out
    for raw in values:
        if raw is None:
            continue
        for piece in str(raw).split(","):
            cleaned = piece.strip()
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                out.append(cleaned)
    return out


def _count_csv_rows(path: Path) -> int:
    """Количество строк-товаров: пытаемся определить разделитель, вычитаем заголовок."""
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            sample = f.read(4096)
            if not sample:
                return 0
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
            except Exception:
                dialect = csv.get_dialect("excel")
            f.seek(0)
            reader = csv.reader(f, dialect)
            count = -1
            for _ in reader:
                count += 1
            return max(count, 0)
    except Exception:
        try:
            lines = [ln for ln in path.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip()]
            return max(len(lines) - 1, 0)
        except Exception:
            return 0


# ===== Загрузка задач =====
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
        raise EmptyItemIdsError(f"Файл {path} содержит пустой список 'item_ids'.")
    return ListerExportTask(factory_id, factory_name, item_ids, path)


def discover_lister_tasks(
    account: str,
    factory_ids: Sequence[str],
    name_filters: Sequence[str],
    limit: Optional[int],
) -> List[ListerExportTask]:
    account_key = account.upper()
    directory = ITEMS_ROOT / f"{account_key}{ACCOUNT_LISTER_DIR_SUFFIX}"
    if not directory.exists():
        LOGGER.warning("Каталог с коллекциями не найден: %s (аккаунт %s)", directory, account_key)
        return []
    id_set = {v.strip() for v in factory_ids if v.strip()}
    name_patterns = [v.lower() for v in name_filters if v]
    tasks: List[ListerExportTask] = []
    for path in sorted(directory.glob("*.json")):
        try:
            task = load_factory_from_json(path)
        except EmptyItemIdsError:
            LOGGER.debug("Пропуск %s: пустой список item_ids.", path)
            continue
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Не удалось прочитать %s: %s", path, exc)
            continue
        if id_set and task.factory_id not in id_set:
            continue
        if name_patterns:
            cand = (task.factory_name or "").lower()
            if not any(p in cand for p in name_patterns):
                continue
        tasks.append(task)
        if limit is not None and len(tasks) >= limit:
            break
    return tasks


def find_existing_export(output_dir: Path, task: ListerExportTask) -> Optional[Path]:
    p = output_dir / task.default_filename()
    return p if p.is_file() else None


def _normalize_ean_value(value) -> Optional[str]:
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return None
    return digits[-13:] if len(digits) >= 13 else digits


def cleanup_existing_outputs(account: str, csv_path: Path) -> None:
    ready_json_path = Path("readyJSON") / account / f"{csv_path.stem}.json"
    ready_html_dir = Path("readyhtml") / account

    eans: Set[str] = set()
    if ready_json_path.exists():
        try:
            data = json.loads(ready_json_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Не удалось прочитать JSON %s: %s", ready_json_path, exc)
        else:
            if isinstance(data, list):
                for entry in data:
                    if isinstance(entry, dict):
                        normalized = _normalize_ean_value(entry.get("ean") or entry.get("EAN"))
                        if normalized:
                            eans.add(normalized)

    removed_html = 0
    if ready_html_dir.is_dir() and eans:
        for ean in eans:
            html_path = ready_html_dir / f"{ean}.html"
            if html_path.exists():
                try:
                    html_path.unlink()
                    removed_html += 1
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("Не удалось удалить HTML %s: %s", html_path, exc)
        if removed_html:
            LOGGER.info("[%s] Удалено HTML файлов: %d (%s)", account, removed_html, csv_path.stem)

    if ready_json_path.exists():
        try:
            ready_json_path.unlink()
            LOGGER.info("[%s] Удалён JSON: %s", account, ready_json_path.name)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Не удалось удалить JSON %s: %s", ready_json_path, exc)

    if csv_path.exists():
        try:
            csv_path.unlink()
            LOGGER.info("[%s] Удалён CSV: %s", account, csv_path.name)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Не удалось удалить CSV %s: %s", csv_path, exc)


# ===== HTTP =====
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


def prepare_lister_page(session: requests.Session, domain: str, factory_id: str) -> Tuple[str, str]:
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
    payload.append(("ExportEncoding", export_encoding))
    if export_format_id is not None:
        payload.append(("ExportFormatID", export_format_id))
    payload.extend([("id", joined_ids), ("art", "export"), ("definition", definition)])
    return payload


def _select_and_export(
    session: requests.Session,
    *,
    base_url: str,
    referer_url: str,
    export_url: str,
    item_ids: Sequence[str],
    domain: str,
    config: ExportConfig,
    retries: int = 1,
):
    """Одна попытка = select, затем export; при ошибке повторяем пару."""
    selection_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": referer_url,
        "Origin": f"https://{domain}",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "max-age=0",
    }
    export_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": base_url,
        "Origin": f"https://{domain}",
        "Accept": "text/csv, text/plain, application/octet-stream, */*;q=0.8",
        "Cache-Control": "max-age=0",
    }

    selection_payload = build_selection_payload(item_ids)
    export_payload = build_export_definition_payload(
        item_ids,
        expprod=config.expprod,
        export_format_id=config.export_format_id,
        export_encoding=config.export_encoding,
        definition=config.definition_id,
    )

    for attempt in range(retries + 1):
        # select
        sel = session.post(base_url, data=selection_payload, headers=selection_headers, timeout=POST_TIMEOUT)
        try:
            sel.raise_for_status()
            if "form-signin" in sel.text.lower():
                raise RuntimeError("Сессия не авторизована — получена страница логина.")
        finally:
            sel.close()

        # export
        resp = session.post(export_url, data=export_payload, headers=export_headers, timeout=POST_TIMEOUT, stream=True)
        try:
            resp.raise_for_status()
            return resp
        except Exception:
            resp.close()
            if attempt == retries:
                raise


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
    export_url = f"https://{domain}{EXPORT_ENDPOINT}"

    response = _select_and_export(
        session,
        base_url=base_url,
        referer_url=referer_url,
        export_url=export_url,
        item_ids=task.item_ids,
        domain=domain,
        config=config,
        retries=1,
    )

    content_type = (response.headers.get("Content-Type") or "").lower()
    target_path = output_dir / task.default_filename()

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

    _atomic_write_bytes(target_path, first_chunk, chunk_iter)
    response.close()
    return target_path


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
    parser = argparse.ArgumentParser(description="Экспорт CSV по eBay Lister для выбранных коллекций.")
    parser.add_argument("--account", choices=ACCOUNT_ORDER, type=str.upper, action="append")
    parser.add_argument("--factory-id", dest="factory_ids", action="append", metavar="ID")
    parser.add_argument("--factory-name", dest="factory_names", action="append", metavar="SUBSTR")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_ROOT_DIR)
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--refresh-existing", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--definition-id", default=None)
    parser.add_argument("--export-format-id", default=None)
    parser.add_argument("--expprod", default=DEFAULT_EXPPROD)
    parser.add_argument("--export-encoding", default=DEFAULT_EXPORT_ENCODING)
    args = parser.parse_args()
    if args.skip_existing and args.refresh_existing:
        parser.error("Нельзя одновременно использовать --skip-existing и --refresh-existing.")
    return args


def main() -> None:
    args = parse_args()
    logger = configure_logging(args.verbose)

    accounts = args.account if args.account else list(ACCOUNT_ORDER)
    logger.info("Старт экспорта листера. Аккаунты: %s", ", ".join(accounts) if accounts else "<нет>")
    accounts = [account.upper() for account in accounts]

    factory_ids = _normalize_sequence(args.factory_ids)
    name_filters = _normalize_sequence(args.factory_names)

    for account in accounts:
        tasks = discover_lister_tasks(account, factory_ids, name_filters, args.limit)
        if not tasks:
            LOGGER.info("Аккаунт %s: подходящих коллекций не найдено.", account)
            continue

        LOGGER.info("Аккаунт %s: отобрано %d коллекций для экспорта.", account, len(tasks))

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

        session: Optional[requests.Session] = None
        try:
            session, domain = ensure_authenticated_session(account)

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
                    "[%s] definition=%s, export_format=%s, encoding=%s",
                    account,
                    export_config.definition_id,
                    export_config.export_format_id,
                    export_config.export_encoding,
                )

            account_output_dir = args.output_dir / f"{account}{ACCOUNT_OUTPUT_SUFFIX}"
            total = len(tasks)
            failed_tasks: List[ListerExportTask] = []
            succeeded = 0

            for index, task in enumerate(tasks, start=1):
                existing = find_existing_export(account_output_dir, task)
                if existing and args.skip_existing:
                    LOGGER.info(
                        "[%s %d/%d] Пропуск: CSV уже существует (%s).",
                        account,
                        index,
                        total,
                        existing,
                    )
                    continue
                if existing and args.refresh_existing:
                    LOGGER.info(
                        "[%s %d/%d] Обнаружен существующий CSV (%s). Удаление и повторный экспорт.",
                        account,
                        index,
                        total,
                        existing,
                    )
                    cleanup_existing_outputs(account, existing)

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
                    made = _count_csv_rows(output_path)
                    made2 = made
                    LOGGER.info(
                        "[%s %d/%d] CSV сохранён: %s — создано %d из %d",
                        account,
                        index,
                        total,
                        output_path.name,
                        made,
                        task.expected_count,
                    )
                    if made < task.expected_count:
                        # одна дополнительная попытка для конкретной коллекции
                        LOGGER.info(
                            "[%s %d/%d] Недостаёт строк (%d < %d). Повторная попытка.",
                            account, index, total, made, task.expected_count
                        )
                        output_path = download_lister_csv(
                            session=session,
                            domain=domain,
                            task=task,
                            output_dir=account_output_dir,
                            config=export_config,
                        )
                        made2 = _count_csv_rows(output_path)
                        LOGGER.info(
                            "[%s %d/%d] Повтор завершён: %s — создано %d из %d",
                            account, index, total, output_path.name, made2, task.expected_count
                        )
                    # success/failure accounting for main pass
                    if made >= task.expected_count or made2 >= task.expected_count:
                        succeeded += 1
                    else:
                        failed_tasks.append(task)
                except Timeout:
                    LOGGER.warning(
                        "[%s %d/%d] Timeout �?� ��?���?�?�'��. �?�?�'�?�?������Ő�? �?� �����?�?���� �?� �������� ��� final-pass...",
                        account,
                        index,
                        total,
                    )
                    try:
                        if session is not None:
                            session.close()
                    except Exception:
                        pass
                    session, domain = ensure_authenticated_session(account)
                    failed_tasks.append(task)
                    continue
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
                    failed_tasks.append(task)
                    continue
            # Final retry pass for failures
            if failed_tasks:
                LOGGER.info(
                    "[%s] �����?�?���� �����?�?����: %d. ��������� ���?���<�'��� �?� ���?�'�?�?������Ő�?...",
                    account,
                    len(failed_tasks),
                )
                # renew session before final pass
                try:
                    if session is not None:
                        session.close()
                except Exception:
                    pass
                session, domain = ensure_authenticated_session(account)

                still_failed: List[ListerExportTask] = []
                for task in failed_tasks:
                    try:
                        output_path = download_lister_csv(
                            session=session,
                            domain=domain,
                            task=task,
                            output_dir=account_output_dir,
                            config=export_config,
                        )
                        made = _count_csv_rows(output_path)
                        if made < task.expected_count:
                            # one more attempt
                            output_path = download_lister_csv(
                                session=session,
                                domain=domain,
                                task=task,
                                output_dir=account_output_dir,
                                config=export_config,
                            )
                            made = _count_csv_rows(output_path)
                        if made >= task.expected_count:
                            succeeded += 1
                            LOGGER.info(
                                "[%s] �����?�?����: %s �?�?���?���?�? %d ��� %d",
                                account,
                                output_path.name,
                                made,
                                task.expected_count,
                            )
                        else:
                            still_failed.append(task)
                            LOGGER.warning(
                                "[%s] �����?�?���� ��?�?���: %s (%d < %d)",
                                account,
                                output_path.name,
                                made,
                                task.expected_count,
                            )
                    except Exception as exc:
                        still_failed.append(task)
                        LOGGER.error(
                            "[%s] �?�?��+��� �� final-pass ��� %s (%s): %s",
                            account,
                            task.factory_name or "<�+��� �?�����?���?��?>",
                            task.factory_id,
                            exc,
                        )

                if still_failed:
                    LOGGER.warning(
                        "[%s] ���������: �?�?�:�?���?��?�? %d, ��?�?��� %d: %s",
                        account,
                        total,
                        len(still_failed),
                        ", ".join(t.factory_id for t in still_failed),
                    )
                else:
                    LOGGER.info("[%s] ���������: ���� ��������� ��������.", account)
                try:
                    ok_count = succeeded
                    fail_count = len(still_failed)
                    LOGGER.info("[%s] Summary: success %d, failed %d of %d", account, ok_count, fail_count, total)
                except Exception:
                    pass

        except Exception as exc:
            LOGGER.error("Не удалось подготовить авторизованную сессию для аккаунта %s: %s", account, exc)
            continue
        finally:
            if session is not None:
                try:
                    session.close()
                except Exception:
                    pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
