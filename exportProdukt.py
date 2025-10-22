from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import tempfile
import time
import unicodedata
import subprocess
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


# ===== Константы =====
ITEMS_ROOT = Path("itemsF")
ACCOUNT_PRODUCT_DIR_SUFFIX = "_I_P"
OUTPUT_ROOT_DIR = Path("CSVDATA")
ACCOUNT_OUTPUT_SUFFIX = "_P"
PRODUCTS_ENDPOINT = "/afterbuy/shop/produkte.aspx"
EXPORT_ENDPOINT = "/afterbuy/im-export.aspx"

# раздельные таймауты (connect, read)
GET_TIMEOUT = (15, 60)
POST_TIMEOUT = (15, 90)
STREAM_CHUNK_SIZE = 1024 * 1024  # 1 MiB

DEFAULT_EXPPROD = "3"
DEFAULT_EXPORT_ENCODING = "1"
DEFAULT_SAVE_EXPORT_ENCODING = "1"

PRODUCT_REFERER_TEMPLATE = (
    "https://{domain}/afterbuy/shop/produkte.aspx?"
    "Su_Suchbegriff=&PRFilter=&Su_Suchbegriff_lg=0&PRFilter1=&Artikelnummer_Search=&"
    "level__Search=&Attributwert_Search=&EAN_Search=&Su_Listenlaenge=500&Su_Listenlaenge_Ges=15000&"
    "MyFreifeld=0&Suche_BestandOP=&Suche_Bestand_Wert=0&MyFreifeldValue=&Suche_ABestandOP=&"
    "Suche_ABestand_Wert=0&Katalog_Filter={factory_id}&Katalog_Filter_Kat2=0&Katalog_Filter_Kat3=0&"
    "Katalog_Filter_Kat4=0&Katalog_Filter_Kat5=0&StandardProductIDValue_Search=&versandgruppe=&"
    "versandgruppe_art=0&vorlage=&vorlageart=0&Product_Search_Stocklocation_1=&"
    "Product_Search_Stocklocation_1_Value=&Product_Search_Stocklocation_2=&"
    "Product_Search_Stocklocation_2_Value=&Product_Search_Stocklocation_3=&"
    "Product_Search_Stocklocation_3_Value=&Product_Search_Stocklocation_4=&"
    "Product_Search_Stocklocation_4_Value=&productSearchSupplier1=0&productSearchSupplier2=0&"
    "productSearchSupplier3=0&productSearchSupplier4=0&ProductSearchSku=&LastSaleFrom=&"
    "ProductSearchMpn=&LastSaleTo=&ProductSearchFeatureId0=0&ProductSearchFeatureValue0=&"
    "ProductSearchFeatureId1=0=0&ProductSearchFeatureValue1=&ProductSearchFeatureId2=0&"
    "ProductSearchFeatureValue2=&ProductSearchFeatureId3=0&ProductSearchFeatureValue3=&"
    "ProductSearchFeatureId4=0&ProductSearchFeatureValue4=&productSearchUserTag1=0&"
    "productSearchUserTag2=0&productSearchUserTag3=0&productSearchUserTag4=0&SuchZusatzfeld1=&"
    "SuchZusatzfeld2=&SuchZusatzfeld3=&SuchZusatzfeld4=&SuchZusatzfeld5=&SuchZusatzfeld6=&"
    "spoid=0&art=SetAuswahl&ShowAdditionalFields=1"
)

LOGGER = logging.getLogger("exportProdukt")


# ===== Модели =====
@dataclass(slots=True)
class FactoryExportTask:
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
    save_export_encoding: str
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
    tmp: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=target.parent, delete=False, prefix=".partial_", suffix=".tmp"
        ) as f:
            tmp = Path(f.name)
            if first_chunk:
                f.write(first_chunk)
            for c in chunk_iter:
                if c:
                    f.write(c)
            f.flush()
            os.fsync(f.fileno())
        for attempt in range(5):
            try:
                tmp.replace(target)
                return
            except PermissionError:
                time.sleep(0.5 * (attempt + 1))
        tmp.replace(target)
    finally:
        if tmp and tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass


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
            lines = [
                ln for ln in path.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip()
            ]
            return max(len(lines) - 1, 0)
        except Exception:
            return 0


def _run_login_py(account: str) -> None:
    """Перелогиниться через внешний Login.py. Пробуем с параметром и без."""
    cmds = [
        [sys.executable, "Login.py", "--account", account],
        [sys.executable, "Login.py"],
    ]
    last_err: Optional[Exception] = None
    for cmd in cmds:
        try:
            LOGGER.info("[%s] Перелогин: запускаю %s", account, " ".join(cmd))
            subprocess.run(cmd, check=True)
            LOGGER.info("[%s] Перелогин завершён успешно", account)
            return
        except Exception as e:
            last_err = e
    LOGGER.warning("[%s] Перелогин через Login.py не удался: %s", account, last_err)


# ===== Работа с фабриками =====
def load_factory_from_json(path: Path) -> FactoryExportTask:
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
    return FactoryExportTask(factory_id, factory_name, item_ids, path)


def discover_factories_for_account(
    account: str,
    factory_ids: Sequence[str],
    name_filters: Sequence[str],
    limit: Optional[int],
) -> List[FactoryExportTask]:
    account_key = account.upper()
    directory = ITEMS_ROOT / f"{account_key}{ACCOUNT_PRODUCT_DIR_SUFFIX}"
    if not directory.exists():
        LOGGER.warning("Каталог с фабриками не найден: %s (аккаунт %s)", directory, account_key)
        return []
    id_set = {v.strip() for v in factory_ids if v.strip()}
    name_patterns = [v.lower() for v in name_filters if v]
    tasks: List[FactoryExportTask] = []
    json_files = sorted(directory.glob("*.json"))
    for path in json_files:
        try:
            task = load_factory_from_json(path)
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


# ===== HTTP-шаги =====
def prepare_product_page(session: requests.Session, domain: str, factory_id: str) -> Tuple[str, str]:
    base_url = f"https://{domain}{PRODUCTS_ENDPOINT}"
    referer_url = PRODUCT_REFERER_TEMPLATE.format(domain=domain, factory_id=factory_id)
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "max-age=0",
        "Referer": base_url,
        "Connection": "close",
    }
    LOGGER.debug("→ GET referer %s", referer_url)
    response = session.get(referer_url, headers=headers, timeout=GET_TIMEOUT)
    response.raise_for_status()
    LOGGER.debug("← GET referer ok, %d bytes", len(response.content))
    if "form-signin" in response.text.lower():
        raise RuntimeError("Сессия не авторизована — получена страница логина.")
    return base_url, referer_url


def build_export_form_payload(item_ids: Sequence[str]) -> List[Tuple[str, str]]:
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
    url = f"https://{domain}{EXPORT_ENDPOINT}"
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "max-age=0",
        "Referer": url,
        "Connection": "close",
    }
    LOGGER.debug("→ GET export page %s", url)
    response = session.get(url, headers=headers, timeout=GET_TIMEOUT)
    response.raise_for_status()
    LOGGER.debug("← GET export page ok")
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


def _select_and_export_pair(
    session: requests.Session,
    *,
    page_url: str,
    referer_url: str,
    export_url: str,
    item_ids: Sequence[str],
    domain: str,
    config: ExportConfig,
    retries: int = 2,
):
    """Пара: select → export. Повторяем пару при ошибке."""
    selection_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": referer_url,
        "Origin": f"https://{domain}",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "max-age=0",
        "Connection": "close",
    }
    export_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": page_url,
        "Origin": f"https://{domain}",
        "Accept": "text/csv, text/plain, application/octet-stream, */*;q=0.8",
        "Cache-Control": "max-age=0",
        "Connection": "close",
    }
    selection_payload = build_export_form_payload(item_ids)
    export_payload = build_export_definition_payload(
        item_ids=item_ids,
        expprod=config.expprod,
        export_format_id=config.export_format_id,
        export_encoding=config.export_encoding,
        save_export_encoding=config.save_export_encoding,
        definition=config.definition_id,
    )

    for attempt in range(retries + 1):
        LOGGER.debug("→ select (%d/%d)", attempt + 1, retries + 1)
        sel = session.post(page_url, data=selection_payload, headers=selection_headers, timeout=POST_TIMEOUT)
        try:
            sel.raise_for_status()
            if "form-signin" in sel.text.lower():
                raise RuntimeError("Сессия не авторизована — получена страница логина.")
        finally:
            sel.close()
        LOGGER.debug("← select ok")

        LOGGER.debug("→ export request")
        resp = session.post(
            export_url, data=export_payload, headers=export_headers, timeout=POST_TIMEOUT, stream=True
        )
        try:
            resp.raise_for_status()
            LOGGER.debug("← export headers ok")
            return resp
        except Exception:
            resp.close()
            LOGGER.debug("← export failed")
            if attempt == retries:
                raise


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
    output_dir.mkdir(parents=True, exist_ok=True)

    page_url, referer_with_query = prepare_product_page(session, domain, factory.factory_id)
    export_url = f"https://{domain}{EXPORT_ENDPOINT}"

    response = _select_and_export_pair(
        session,
        page_url=page_url,
        referer_url=referer_with_query,
        export_url=export_url,
        item_ids=factory.item_ids,
        domain=domain,
        config=ExportConfig(
            definition_id=definition,
            export_format_id=export_format_id,
            export_encoding=export_encoding,
            save_export_encoding=save_export_encoding,
            expprod=expprod,
        ),
        retries=1,
    )

    content_type = (response.headers.get("Content-Type") or "").lower()
    target_path = output_dir / factory.default_filename()

    LOGGER.debug("→ stream start to %s", target_path.name)
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
    LOGGER.debug("← stream done")
    return target_path


# ===== Конфиг/CLI =====
def configure_logging(verbose: bool) -> logging.Logger:
    console_level = logging.DEBUG if verbose else logging.INFO
    return setup_logging("exportProdukt", console_level=console_level)


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Экспорт CSV с товарами Afterbuy для выбранных фабрик.")
    parser.add_argument("--account", choices=ACCOUNT_ORDER, type=str.upper, action="append")
    parser.add_argument("--factory-id", dest="factory_ids", action="append", metavar="ID")
    parser.add_argument("--factory-name", dest="factory_names", action="append", metavar="SUBSTR")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_ROOT_DIR)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--definition-id", default=None)
    parser.add_argument("--export-format-id", default=None)
    parser.add_argument("--expprod", default=DEFAULT_EXPPROD)
    parser.add_argument("--export-encoding", default=DEFAULT_EXPORT_ENCODING)
    parser.add_argument("--save-export-encoding", default=DEFAULT_SAVE_EXPORT_ENCODING)
    return parser.parse_args()


# ===== Main =====
def main() -> None:
    args = parse_args()
    logger = configure_logging(args.verbose)

    accounts = args.account if args.account else list(ACCOUNT_ORDER)
    accounts = [account.upper() for account in accounts]
    logger.info("Старт экспорта. Аккаунты: %s", ", ".join(accounts) if accounts else "<нет>")

    factory_ids = _normalize_sequence(args.factory_ids)
    name_filters = _normalize_sequence(args.factory_names)

    for account in accounts:
        tasks = discover_factories_for_account(account, factory_ids, name_filters, args.limit)
        if not tasks:
            LOGGER.info("Аккаунт %s: фабрики не найдены.", account)
            continue

        LOGGER.info("Аккаунт %s: отобрано %d фабрик.", account, len(tasks))

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
                save_export_encoding=args.save_export_encoding,
                expprod=args.expprod,
            )
            account_output_dir = args.output_dir / f"{account}{ACCOUNT_OUTPUT_SUFFIX}"

            errors: list[tuple[str, str]] = []
            total = len(tasks)

            for i, t in enumerate(tasks, start=1):
                LOGGER.info("[%s %d/%d] Экспорт фабрики %s (%s) — %d товаров.",
                            account, i, total, t.factory_name or "<без названия>", t.factory_id, len(t.item_ids))

                # попытка 1
                try:
                    path = download_factory_csv(
                        session=session,
                        domain=domain,
                        factory=t,
                        output_dir=account_output_dir,
                        expprod=export_config.expprod,
                        export_format_id=export_config.export_format_id,
                        definition=export_config.definition_id,
                        export_encoding=export_config.export_encoding,
                        save_export_encoding=export_config.save_export_encoding,
                    )
                    made = _count_csv_rows(path)
                    exp = t.expected_count
                    LOGGER.info("[%s %d/%d] %s → %s | создано %d из %d",
                                account, i, total, t.factory_id, path.name, made, exp)

                    if made >= exp:
                        continue  # успех

                    # немедленный ретрай из-за «короткого» файла
                    LOGGER.info("[%s %d/%d] Недостаёт строк (%d < %d). Немедленный ретрай с перелогином.",
                                account, i, total, made, exp)
                    _run_login_py(account)
                    try:
                        if session is not None:
                            session.close()
                    except Exception:
                        pass
                    session, domain = ensure_authenticated_session(account)

                    # повтор
                    path = download_factory_csv(
                        session=session,
                        domain=domain,
                        factory=t,
                        output_dir=account_output_dir,
                        expprod=export_config.expprod,
                        export_format_id=export_config.export_format_id,
                        definition=export_config.definition_id,
                        export_encoding=export_config.export_encoding,
                        save_export_encoding=export_config.save_export_encoding,
                    )
                    made2 = _count_csv_rows(path)
                    LOGGER.info("[%s retry] %s → %s | создано %d из %d",
                                account, t.factory_id, path.name, made2, exp)
                    if made2 < exp:
                        errors.append((t.factory_id, f"Недостаточно строк: {made2} из {exp}"))

                except Exception as e:
                    # немедленный ретрай из-за ошибки скачивания
                    LOGGER.error("[%s %d/%d] Ошибка %s: %s. Немедленный ретрай с перелогином.",
                                 account, i, total, t.factory_id, e)
                    _run_login_py(account)
                    try:
                        if session is not None:
                            session.close()
                    except Exception:
                        pass
                    try:
                        session, domain = ensure_authenticated_session(account)
                        path = download_factory_csv(
                            session=session,
                            domain=domain,
                            factory=t,
                            output_dir=account_output_dir,
                            expprod=export_config.expprod,
                            export_format_id=export_config.export_format_id,
                            definition=export_config.definition_id,
                            export_encoding=export_config.export_encoding,
                            save_export_encoding=export_config.save_export_encoding,
                        )
                        made = _count_csv_rows(path)
                        LOGGER.info("[%s retry] %s → %s | создано %d из %d",
                                    account, t.factory_id, path.name, made, t.expected_count)
                        if made < t.expected_count:
                            errors.append((t.factory_id, f"Недостаточно строк: {made} из {t.expected_count}"))
                    except Exception as e2:
                        errors.append((t.factory_id, f"повторная ошибка: {e2}"))
                        LOGGER.error("[%s retry] Ошибка %s: %s", account, t.factory_id, e2)

            # Итог
            should = len(tasks)
            final_have = sum(1 for t in tasks if (account_output_dir / t.default_filename()).exists())
            LOGGER.info("[%s] ИТОГО: должно быть %d, создано %d, проблем %d",
                        account, should, final_have, len(errors))
            if errors:
                for fid, msg in errors[:100]:
                    LOGGER.info("  - %s: %s", fid, msg)

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
