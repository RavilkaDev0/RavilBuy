"""Generate structured JSON files from JV/XL lister CSV exports."""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, Tuple

import xmltodict

CSV_ENCODING = "utf-8-sig"
CUSTOM_SPEC_COLUMN = "CustomItemSpecifics"
ARRAY_FIELDS = {"pictureurls", "shiptolocations", "ship_shippingrate"}

DATASETS = [
    {
        "name": "JV",
        "source_dir": Path("CSVDATA") / "JV_L",
        "output_dir": Path("readyJSON") / "JV",
        "log_file": Path("LOGs") / "make_json_jv.log",
        "ean_column": "EAN",
    },
    {
        "name": "XL",
        "source_dir": Path("CSVDATA") / "XL_L",
        "output_dir": Path("readyJSON") / "XL",
        "log_file": Path("LOGs") / "make_json_xl.log",
        "ean_column": "EAN",
    },
]


def main() -> None:
    total_created = 0

    for dataset in DATASETS:
        created = process_dataset(dataset)
        total_created += created

    print(f"Всего создано JSON: {total_created}")


def process_dataset(config: dict) -> int:
    source_dir: Path = config["source_dir"]
    output_dir: Path = config["output_dir"]
    log_file: Path = config["log_file"]
    ean_column: str = config["ean_column"]
    dataset_name: str = config["name"]

    if not source_dir.exists():
        print(f"[{dataset_name}] Каталог с CSV не найден: {source_dir}")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    if log_file.exists():
        log_file.unlink()

    csv_files = list(sorted(source_dir.glob("*.csv")))
    if not csv_files:
        print(f"[{dataset_name}] В каталоге {source_dir} нет CSV-файлов.")
        return 0

    total_created = 0
    failures: list[str] = []

    for csv_path in csv_files:
        created, csv_failures = process_csv(csv_path, output_dir, ean_column)
        total_created += created
        failures.extend(csv_failures)

    timestamp = datetime.now().isoformat(timespec="seconds")
    if failures:
        lines = [f"[{timestamp}] [{dataset_name}] Ошибок: {len(failures)}"]
        lines.extend(f"  - {failure}" for failure in failures)
        append_log(log_file, lines)
        print(lines[0])
    else:
        print(f"[{timestamp}] [{dataset_name}] Ошибок не обнаружено.")
    return total_created


def process_csv(csv_path: Path, output_dir: Path, ean_column: str) -> Tuple[int, list[str]]:
    rows = list(read_csv(csv_path))
    if not rows:
        return 0, [f"{csv_path.name}: CSV пуст"]

    structured_rows = []
    failures: list[str] = []

    for index, row in enumerate(rows, start=2):
        cleaned, issues = convert_row(row, ean_column)
        title = row.get("Artikelbeschreibung", "").strip()
        if issues:
            for issue in issues:
                label = f"{issue} (Artikelbeschreibung: {title or 'N/A'})"
                failures.append(f"{csv_path.name}: строка {index} — {label}")
        if cleaned:
            cleaned["Fabric"] = csv_path.name
            structured_rows.append(cleaned)

    output_file = output_dir / f"{csv_path.stem}.json"
    with output_file.open("w", encoding="utf-8") as fh:
        json.dump(structured_rows, fh, ensure_ascii=False, indent=2)

    return 1, failures


def convert_row(row: dict[str, str], ean_column: str) -> Tuple[dict | None, list[str]]:
    issues: list[str] = []
    if not any(value.strip() for value in row.values() if isinstance(value, str)):
        issues.append("пустая строка")
        return None, issues

    fields: dict[str, object] = {}
    for key, value in row.items():
        if key == CUSTOM_SPEC_COLUMN:
            continue
        cleaned = clean_value(key, value)
        if cleaned is not None:
            fields[key] = cleaned

    specifics = parse_item_specifics(row.get(CUSTOM_SPEC_COLUMN, ""))

    ean = normalize_ean(fields.get(ean_column))
    if not ean:
        ean = normalize_ean(fields.get("EAN"))
    if not ean:
        ean = extract_ean_from_specifics(specifics)
    if not ean:
        ean = normalize_ean(fields.get("Herstellernummer"))
    if not ean:
        ean = normalize_ean(row.get("Herstellernummer"))
    if not ean:
        issues.append("не найден корректный EAN (колонка, характеристики, Herstellernummer)")
        return None, issues

    flat_row: dict[str, object] = {"ean": ean}
    for key, value in fields.items():
        if key == "ean":
            continue
        flat_row[key] = value

    for key, value in specifics.items():
        target_key = key
        if target_key in flat_row:
            suffix_index = 1
            candidate = f"{target_key}_specific"
            while candidate in flat_row:
                suffix_index += 1
                candidate = f"{target_key}_specific{suffix_index}"
            target_key = candidate
        flat_row[target_key] = value

    return flat_row, issues


def normalize_ean(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        for item in value:
            candidate = normalize_ean(item)
            if candidate:
                return candidate
        return None
    text = str(value)
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 13:
        return digits[-13:]
    return None


def extract_ean_from_specifics(specs) -> str | None:
    if not isinstance(specs, dict):
        return None
    for key in ("EAN", "Herstellernummer"):
        candidate = normalize_ean(specs.get(key))
        if candidate:
            return candidate
    return None


def clean_value(key: str, value: str | None) -> object | None:
    if value is None:
        return None
    text = value.strip()
    if text == "":
        return None

    lowered_key = key.lower()

    if text in {"True", "False"}:
        return text == "True"

    numeric = try_parse_number(text)
    if numeric is not None:
        return numeric

    if lowered_key in ARRAY_FIELDS or (";" in text and lowered_key.endswith("urls")):
        return [part.strip() for part in text.split(";") if part.strip()]

    return text


def try_parse_number(value: str) -> object | None:
    normalized = value.replace(" ", "")
    normalized = normalized.replace(",", ".")
    try:
        if normalized.startswith("0") and normalized not in {"0", "0.0"} and not normalized.startswith("0."):
            return None
        if normalized.count(".") <= 1 and normalized.replace(".", "", 1).isdigit():
            num = float(normalized)
            if num.is_integer():
                return int(num)
            return num
    except ValueError:
        return None
    return None


def parse_item_specifics(raw_xml: str) -> dict[str, object]:
    if not raw_xml:
        return {}
    try:
        parsed = xmltodict.parse(raw_xml)
        specifics = parsed.get("ItemSpecifics", {}).get("NameValueList", [])
        if isinstance(specifics, dict):
            specifics = [specifics]
        result: dict[str, object] = {}
        for item in specifics:
            name = extract_text(item.get("Name"))
            if not name:
                continue
            value_field = item.get("Value")
            if isinstance(value_field, list):
                values = [extract_text(v) for v in value_field if extract_text(v)]
            else:
                text = extract_text(value_field)
                values = [text] if text else []
            if not values:
                continue
            result[name] = values[0] if len(values) == 1 else values
        return result
    except Exception:
        return {}


def extract_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return value.get("#cdata-section") or value.get("#text") or ""
    return str(value)


def read_csv(csv_path: Path) -> Iterable[dict[str, str]]:
    with csv_path.open(encoding=CSV_ENCODING, newline="") as fh:
        reader = csv.DictReader(
            fh,
            delimiter=";",
            quotechar="\"",
            strict=True,
            skipinitialspace=False,
        )
        yield from reader


def append_log(log_file: Path, lines: list[str]) -> None:
    if not lines:
        return
    with log_file.open("a", encoding="utf-8") as fh:
        for line in lines:
            fh.write(line + "\n")


if __name__ == "__main__":
    main()





