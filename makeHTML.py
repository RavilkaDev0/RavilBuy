from __future__ import annotations

import argparse
import csv
import logging
import re
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
from urllib.parse import unquote

from logging_utils import setup_logging


DEFAULT_ACCOUNTS = ("JV", "XL")
DEFAULT_INPUT_BASE = Path("CSVDATA")
DEFAULT_INPUT_SUFFIX = "_P"
DEFAULT_OUTPUT_BASE = Path("readyhtml")
DEFAULT_EAN_COLUMN_CANDIDATES = ("ManufacturerPartNumber", "EAN", "Sku", "SKU")
DEFAULT_HTML_COLUMN_CANDIDATES = ("Beschreibung", "Description", "HTML")

LOGGER = logging.getLogger("makeHTML")


def sanitize_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    if not cleaned:
        cleaned = "document"
    return cleaned[:150]


def detect_columns(headers: Iterable[str]) -> Tuple[Optional[str], Optional[str]]:
    header_set = {header.strip(): header.strip() for header in headers}

    ean_column = next(
        (header_set.get(candidate) for candidate in DEFAULT_EAN_COLUMN_CANDIDATES if header_set.get(candidate)),
        None,
    )
    html_column = next(
        (header_set.get(candidate) for candidate in DEFAULT_HTML_COLUMN_CANDIDATES if header_set.get(candidate)),
        None,
    )
    return ean_column, html_column


def decode_html_fragment(raw: str) -> str:
    return unquote(raw)


def convert_csv_to_html_files(
    csv_path: Path,
    output_dir: Path,
    overwrite: bool = True,
) -> Tuple[int, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    success = 0
    skipped = 0

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        if reader.fieldnames is None:
            LOGGER.warning("Файл %s не содержит заголовков — пропуск.", csv_path)
            return success, skipped
        ean_column, html_column = detect_columns(reader.fieldnames)
        if not ean_column or not html_column:
            LOGGER.warning(
                "%s: не удалось определить столбцы (EAN=%s, HTML=%s) — пропуск.",
                csv_path,
                ean_column,
                html_column,
            )
            return success, skipped

        for row in reader:
            raw_ean = row.get(ean_column) or ""
            raw_html = row.get(html_column) or ""

            ean = raw_ean.strip()
            if not ean:
                LOGGER.warning(
                    "[%s] Строка без значения в колонке '%s' — запись пропущена.",
                    csv_path.name,
                    ean_column,
                )
                skipped += 1
                continue
            if not raw_html.strip():
                LOGGER.warning(
                    "[%s] EAN %s: пустая колонка '%s' — запись пропущена.",
                    csv_path.name,
                    ean,
                    html_column,
                )
                skipped += 1
                continue

            html_content = decode_html_fragment(raw_html)
            filename = sanitize_filename(ean) + ".html"
            target_path = output_dir / filename
            if target_path.exists() and not overwrite:
                LOGGER.debug("Файл %s уже существует — пропуск.", target_path)
                skipped += 1
                continue
            target_path.write_text(html_content, encoding="utf-8")
            success += 1
    return success, skipped


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
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Перезаписывать существующие HTML файлы.",
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
            success, skipped = convert_csv_to_html_files(
                csv_path,
                output_dir,
                overwrite=args.overwrite,
            )
            logger.info(
                "[%s] %s → создано %d HTML, пропущено %d строк.",
                account,
                csv_path.name,
                success,
                skipped,
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
