"""Create JV and XL SQLite databases from Ready JSON exports."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Tuple

OUTPUT_DIR = Path("DB")
LOG_FILE = Path("LOGs") / "make_db.log"


@dataclass(frozen=True)
class DatasetConfig:
    name: str
    json_dir: Path
    db_path: Path
    table_name: str


DATASETS: Tuple[DatasetConfig, ...] = (
    DatasetConfig(
        name="JV",
        json_dir=Path("Ready JSON") / "JV",
        db_path=OUTPUT_DIR / "JV_DB.db",
        table_name="jv_products",
    ),
    DatasetConfig(
        name="XL",
        json_dir=Path("Ready JSON") / "XL",
        db_path=OUTPUT_DIR / "XL_DB.db",
        table_name="xl_products",
    ),
)

COLUMNS = [
    "filename",
    "ean",
    "record_id",
    "Title",
    "Startpreis",
    "GalleryURL",
    "PictureURL",
    "PictureURLs",
    "Breite",
    "Höhe",
    "Länge",
    "Marke",
    "Produktart",
    "Farbe",
    "Material",
    "Stil",
    "Zimmer",
    "Herstellernummer",
    "Form",
]


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    if LOG_FILE.exists():
        LOG_FILE.unlink()

    for dataset in DATASETS:
        failures = process_dataset(dataset)
        timestamp = datetime.now().isoformat(timespec="seconds")
        if failures:
            lines = [
                f"[{timestamp}] {dataset.name}: ошибок при импорте: {len(failures)}",
                *[f"  - {msg}" for msg in failures],
            ]
            append_log(lines)
            print(lines[0])
        else:
            print(
                f"[{timestamp}] {dataset.name}: импорт завершён без ошибок. "
                f"База: {dataset.db_path}"
            )


def process_dataset(dataset: DatasetConfig) -> List[str]:
    if not dataset.json_dir.exists():
        return [f"{dataset.name}: каталог с JSON не найден ({dataset.json_dir})"]

    json_files = list(sorted(dataset.json_dir.glob("*.json")))
    if not json_files:
        return [f"{dataset.name}: в каталоге {dataset.json_dir} нет JSON-файлов."]

    connection = sqlite3.connect(dataset.db_path)
    try:
        initialize_database(connection, dataset.table_name)
        failures = ingest_files(connection, json_files, dataset)
    finally:
        connection.close()

    return failures


def initialize_database(connection: sqlite3.Connection, table_name: str) -> None:
    cursor = connection.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    column_defs = ", ".join(f'"{name}" TEXT' for name in COLUMNS)
    cursor.execute(f'CREATE TABLE "{table_name}" ({column_defs})')
    connection.commit()


def ingest_files(
    connection: sqlite3.Connection, json_files: List[Path], dataset: DatasetConfig
) -> List[str]:
    failures: List[str] = []
    cursor = connection.cursor()
    placeholders = ", ".join("?" for _ in COLUMNS)
    insert_sql = f'INSERT INTO "{dataset.table_name}" VALUES ({placeholders})'

    for json_path in json_files:
        try:
            records = json.loads(json_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            failures.append(
                f"{dataset.name}: {json_path.name} — ошибка чтения JSON ({exc})"
            )
            continue

        for idx, record in enumerate(_ensure_list_of_dict(records), start=1):
            row, warnings = build_row(json_path.name, record, idx)
            if warnings:
                failures.extend(f"{dataset.name}: {warning}" for warning in warnings)
                continue

            cursor.execute(insert_sql, row)
        connection.commit()

    return failures


def build_row(filename: str, record: Dict, index: int) -> Tuple[List[str], List[str]]:
    warnings: List[str] = []
    fields = record.get("fields")
    if not isinstance(fields, dict):
        warnings.append(f"{filename}: запись #{index} — отсутствует блок 'fields'")
        return [], warnings

    specifics = record.get("itemSpecifics")
    specifics = specifics if isinstance(specifics, dict) else {}

    ean_value = _serialize(record.get("ean") or fields.get("EAN"))
    if not ean_value:
        warnings.append(f"{filename}: запись #{index} — отсутствует значение EAN")
        return [], warnings

    record_id = ean_value

    row = [
        filename,
        ean_value,
        record_id,
        _get_field(fields, "Artikelbeschreibung"),
        _get_field(fields, "Startpreis"),
        _get_field(fields, "GalleryURL"),
        _get_field(fields, "PictureURL"),
        _get_field(fields, "pictureurls"),
        _get_spec(specifics, "Breite"),
        _get_spec(specifics, "Höhe"),
        _get_spec(specifics, "Länge"),
        _get_spec(specifics, "Marke"),
        _get_spec(specifics, "Produktart"),
        _get_spec(specifics, "Farbe"),
        _get_spec(specifics, "Material"),
        _get_spec(specifics, "Stil"),
        _get_spec(specifics, "Zimmer"),
        _get_spec(specifics, "Herstellernummer"),
        _get_spec(specifics, "Form"),
    ]
    return row, warnings


def _get_field(fields: Dict, key: str) -> str:
    return _serialize(fields.get(key))


def _get_spec(specs: Dict, key: str) -> str:
    return _serialize(specs.get(key))


def _serialize(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        parts = [part for part in (_serialize(v) for v in value) if part]
        return "; ".join(parts)
    if isinstance(value, bool):
        return "True" if value else "False"
    return str(value)


def _ensure_list_of_dict(records: object) -> List[Dict]:
    if not isinstance(records, list):
        return []
    return [record for record in records if isinstance(record, dict)]


def append_log(lines: List[str]) -> None:
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        for line in lines:
            fh.write(line + "\n")


if __name__ == "__main__":
    main()

