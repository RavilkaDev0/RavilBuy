from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote

import pandas as pd

from logging_utils import setup_logging


DEFAULT_ACCOUNTS = ("JV", "XL")
DEFAULT_INPUT_BASE = Path("CSVDATA")
DEFAULT_INPUT_SUFFIX = "_P"
DEFAULT_OUTPUT_BASE = Path("readyhtml")
DEFAULT_EAN_COLUMN_CANDIDATES = ("ManufacturerPartNumber", "EAN", "Sku", "SKU")
DEFAULT_ID_COLUMN_CANDIDATES = ("StandardProductIDValue", "StandardProductIdValue", "ItemId", "StandardProductID")
DEFAULT_HTML_COLUMN_CANDIDATES = ("Beschreibung", "Description", "HTML")
DEFAULT_DELIMITER = ";"
ITEM_JSON_DIRS = {
    "JV": Path("itemsF") / "JV_I_P",
    "XL": Path("itemsF") / "XL_I_P",
}

LOGGER = logging.getLogger("makeHTML")


def load_expected_item_info(account: str, csv_path: Path) -> Tuple[Optional[int], Optional[str], Optional[Path]]:
    items_root = ITEM_JSON_DIRS.get(account)
    if not items_root:
        return None, None, None
    json_path = items_root / f"{csv_path.stem}.json"
    if not json_path.exists():
        return None, None, None
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        LOGGER.warning("Не удалось прочитать JSON %s: %s", json_path, exc)
        return None, None, json_path
    item_count_value = data.get("item_count")
    try:
        item_count = int(item_count_value) if item_count_value is not None else None
    except (TypeError, ValueError):
        item_count = None
    factory_name = data.get("factory_name")
    return item_count, factory_name, json_path


def load_csv_dataframe(path: Path, delimiter: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(
            path,
            sep=delimiter,
            engine="python",
            dtype=str,
            quotechar='"',
            escapechar="\\",
            na_filter=False,
        )
    except Exception as exc:
        LOGGER.error("Не удалось прочитать CSV %s: %s", path, exc)
        return pd.DataFrame()
    if df.empty:
        return df
    return df.fillna("")

def sanitize_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    if not cleaned:
        cleaned = "document"
    return cleaned[:150]


def detect_columns(headers: Iterable[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    header_set = {header.strip(): header.strip() for header in headers}

    ean_column = next(
        (header_set.get(candidate) for candidate in DEFAULT_EAN_COLUMN_CANDIDATES if header_set.get(candidate)),
        None,
    )
    id_column = next(
        (header_set.get(candidate) for candidate in DEFAULT_ID_COLUMN_CANDIDATES if header_set.get(candidate)),
        None,
    )
    html_column = next(
        (header_set.get(candidate) for candidate in DEFAULT_HTML_COLUMN_CANDIDATES if header_set.get(candidate)),
        None,
    )
    return ean_column, id_column, html_column

def decode_html_fragment(raw: str) -> str:
    return unquote(raw)


def convert_csv_to_html_files(
    account: str,
    csv_path: Path,
    output_dir: Path,
    overwrite: bool = True,
    delimiter: str = DEFAULT_DELIMITER,
) -> Tuple[int, int, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    success = 0
    skipped = 0

    df = load_csv_dataframe(csv_path, delimiter)
    if df.empty:
        LOGGER.warning("Файл %s не содержит данных — пропуск.", csv_path)
        return success, skipped, 0

    ean_column, id_column, html_column = detect_columns(df.columns)
    if not ean_column or not html_column:
        LOGGER.warning(
            "%s: не удалось определить столбцы (EAN=%s, HTML=%s) — пропуск.",
            csv_path,
            ean_column,
            html_column,
        )
        return success, skipped, len(df.index)
    if not id_column:
        LOGGER.warning(
            "%s: колонка StandardProductIDValue отсутствует — используем ManufacturerPartNumber для имени файла.",
            csv_path,
        )

    total_rows = len(df.index)
    mismatches: List[Tuple[int, pd.Series]] = []
    for index, row in df.iterrows():
        raw_ean = row.get(ean_column, "")
        raw_id = row.get(id_column, "") if id_column else ""
        raw_html = row.get(html_column, "")

        if pd.isna(raw_ean):
            raw_ean = ""
        if pd.isna(raw_id):
            raw_id = ""
        if pd.isna(raw_html):
            raw_html = ""

        ean = str(raw_ean).strip()
        std_id = str(raw_id).strip()
        if id_column:
            if not std_id:
                mismatches.append((index, row))
                LOGGER.warning(
                    "[%s] EAN %s: отсутствие StandardProductIDValue — запись пропущена.",
                    csv_path.name,
                    ean or "(пусто)",
                )
                skipped += 1
                continue
            if ean != std_id:
                mismatches.append((index, row))
                LOGGER.warning(
                    "[%s] Несовпадение идентификаторов: ManufacturerPartNumber='%s', StandardProductIDValue='%s' — запись пропущена.",
                    csv_path.name,
                    ean,
                    std_id,
                )
                skipped += 1
                continue
        else:
            if not ean:
                LOGGER.warning(
                    "[%s] Строка без значения в колонке '%s' — запись пропущена.",
                    csv_path.name,
                    ean_column,
                )
                skipped += 1
                continue

        if not ean:
            LOGGER.warning(
                "[%s] Строка без значения в колонке '%s' — запись пропущена.",
                csv_path.name,
                ean_column,
            )
            skipped += 1
            continue

        if not str(raw_html).strip():
            LOGGER.warning(
                "[%s] EAN %s: пустая колонка '%s' — запись пропущена.",
                csv_path.name,
                ean,
                html_column,
            )
            skipped += 1
            continue

        html_content = decode_html_fragment(str(raw_html))
        target_id = std_id if id_column else ean
        filename = sanitize_filename(target_id or ean) + ".html"
        target_path = output_dir / filename
        if target_path.exists() and not overwrite:
            LOGGER.debug("Файл %s уже существует — пропуск.", target_path)
            skipped += 1
            continue
        target_path.write_text(html_content, encoding="utf-8")
        success += 1

    return success, skipped, total_rows

def list_csv_files(input_dir: Path) -> List[Path]:
    if not input_dir.exists():
        LOGGER.warning("Каталог %s не найден — пропуск.", input_dir)
        return []
    return sorted(path for path in input_dir.glob("*.csv") if path.is_file())


def configure_logging(verbose: bool) -> logging.Logger:
    console_level = logging.DEBUG if verbose else logging.INFO
    return setup_logging("makeHTML", console_level=console_level)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Преобразование CSV описаний товаров в HTML файлы."
    )
    parser.add_argument(
        "--account",
        nargs="+",
        choices=DEFAULT_ACCOUNTS,
        help="Обрабатывать только указанные аккаунты (по умолчанию все).",
    )
    parser.add_argument(
        "--input-base",
        type=Path,
        default=DEFAULT_INPUT_BASE,
        help="Базовый каталог с CSV файлами (по умолчанию CSVDATA).",
    )
    parser.add_argument(
        "--output-base",
        type=Path,
        default=DEFAULT_OUTPUT_BASE,
        help="Базовый каталог для HTML файлов (по умолчанию readyhtml).",
    )
    parser.set_defaults(overwrite=True)
    parser.add_argument(
        "--no-overwrite",
        dest="overwrite",
        action="store_false",
        help="Не перезаписывать существующие HTML файлы.",
    )
    parser.add_argument(
        "--delimiter",
        default=DEFAULT_DELIMITER,
        help=f"Разделитель полей CSV (по умолчанию '{DEFAULT_DELIMITER}').",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Ограничить количество CSV файлов первыми N (на аккаунт).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Включить подробный лог.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = configure_logging(args.verbose)

    accounts = args.account or list(DEFAULT_ACCOUNTS)
    total_success = 0
    total_skipped = 0

    for account in accounts:
        input_dir = args.input_base / f"{account}{DEFAULT_INPUT_SUFFIX}"
        output_dir = args.output_base / account
        csv_files = list_csv_files(input_dir)
        if args.limit is not None:
            csv_files = csv_files[: args.limit]
        if not csv_files:
            logger.info("Аккаунт %s: CSV файлы не найдены в %s.", account, input_dir)
            continue

        logger.info("=== Аккаунт %s ===", account)
        account_success = 0
        account_skipped = 0
        for csv_path in csv_files:
            logger.info("[%s] Обработка %s...", account, csv_path.name)
            expected_count, factory_name, json_path = load_expected_item_info(account, csv_path)
            success, skipped, total_rows = convert_csv_to_html_files(
                account,
                csv_path,
                output_dir,
                overwrite=args.overwrite,
                delimiter=args.delimiter,
            )
            logger.info(
                "[%s] %s → создано %d HTML, пропущено %d строк.",
                account,
                csv_path.name,
                success,
                skipped,
            )
            logger.debug(
                "[%s] %s: всего строк %d, создано %d, пропущено %d.",
                account,
                csv_path.name,
                total_rows,
                success,
                skipped,
            )
            if total_rows != success + skipped:
                logger.warning(
                    "[%s] %s: несоответствие подсчёта строк (всего=%d, создано+пропущено=%d).",
                    account,
                    csv_path.name,
                    total_rows,
                    success + skipped,
                )
            if expected_count is not None:
                if success != expected_count:
                    logger.warning(
                        "[%s] %s: item_count=%d (из %s) не совпадает с созданными HTML=%d (строк в CSV=%d, пропущено=%d)%s",
                        account,
                        csv_path.stem,
                        expected_count,
                        json_path.name if json_path else "JSON",
                        success,
                        total_rows,
                        skipped,
                        f" (фабрика: {factory_name})" if factory_name else "",
                    )
                else:
                    logger.info(
                        "[%s] %s: количество HTML (%d) соответствует item_count.",
                        account,
                        csv_path.stem,
                        success,
                    )
            account_success += success
            account_skipped += skipped

        logger.info(
            "[%s] Готово: создано %d HTML файлов, пропущено %d строк. Результат в %s",
            account,
            account_success,
            account_skipped,
            output_dir,
        )
        total_success += account_success
        total_skipped += account_skipped

    logger.info(
        "Итог: создано %d HTML файлов, пропущено %d строк.",
        total_success,
        total_skipped,
    )


if __name__ == "__main__":
    main()
