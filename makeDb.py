"""Create JV and XL SQLite databases from Ready JSON/CSV exports (4 DBs sequentially)."""

from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

OUTPUT_DIR = Path("DB")
LOG_FILE = Path("LOGs") / "make_db.log"

# ---------- Schemas ----------
COLUMNS_L: List[str] = [
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

COLUMNS_P: List[str] = [
    "Name",
    "StandardProductIDValue",
    "ManufacturerPartNumber",
    "Beschreibung",
]

# ---------- Config ----------
@dataclass(frozen=True)
class DatasetConfig:
    name: str
    json_or_csv_dir: Path
    db_path: Path
    table_name: str
    kind: str  # "L" for JSON Lister, "P" for CSV Produkt
    columns: Sequence[str]

DATASETS: Tuple[DatasetConfig, ...] = (
    DatasetConfig(
        name="JV",
        json_or_csv_dir=Path("readyJSON") / "JV",
        db_path=OUTPUT_DIR / "JV_LISTER.db",
        table_name="jv_products",
        kind="L",
        columns=COLUMNS_L,
    ),
    DatasetConfig(
        name="XL",
        json_or_csv_dir=Path("readyrJSON") / "XL",
        db_path=OUTPUT_DIR / "XL_LISTER.db",
        table_name="xl_products",
        kind="L",
        columns=COLUMNS_L,
    ),
    DatasetConfig(
        name="PRODUKT_JV",
        json_or_csv_dir=Path("CSVDATA") / "JV_P",
        db_path=OUTPUT_DIR / "JV_PRODUKT.db",
        table_name="produkt_products",
        kind="P",
        columns=COLUMNS_P,
    ),
    DatasetConfig(
        name="PRODUKT_XL",
        json_or_csv_dir=Path("CSVDATA") / "XL_P",
        db_path=OUTPUT_DIR / "XL_PRODUKT.db",
        table_name="produkt_xl_products",
        kind="P",
        columns=COLUMNS_P,
    ),
)

# ---------- Main ----------
def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    if LOG_FILE.exists():
        LOG_FILE.unlink()

    for ds in DATASETS:
        failures = process_dataset(ds)
        ts = datetime.now().isoformat(timespec="seconds")
        if failures:
            lines = [f"[{ts}] {ds.name}: ошибок при импорте: {len(failures)}"]
            lines += [f"  - {m}" for m in failures]
            append_log(lines)
            print(lines[0])
        else:
            print(f"[{ts}] {ds.name}: импорт завершён без ошибок. База: {ds.db_path}")

# ---------- Processing ----------
def process_dataset(ds: DatasetConfig) -> List[str]:
    if not ds.json_or_csv_dir.exists():
        return [f"{ds.name}: каталог не найден ({ds.json_or_csv_dir})"]

    pattern = "*.json" if ds.kind == "L" else "*.csv"
    files = sorted(ds.json_or_csv_dir.glob(pattern))
    if not files:
        return [f"{ds.name}: нет входных файлов ({ds.json_or_csv_dir})"]

    conn = sqlite3.connect(ds.db_path)
    try:
        initialize_database(conn, ds.table_name, ds.columns)
        if ds.kind == "L":
            failures = ingest_lister_json(conn, files, ds)
        else:
            failures = ingest_produkt_csv(conn, files, ds)
    finally:
        conn.close()
    return failures

def initialize_database(connection: sqlite3.Connection, table_name: str, columns: Sequence[str]) -> None:
    cur = connection.cursor()
    cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
    cur.execute(f'CREATE TABLE "{table_name}" ({col_defs})')
    connection.commit()

# ---------- Ingest: Lister (JSON) ----------
def ingest_lister_json(
    connection: sqlite3.Connection, json_files: List[Path], ds: DatasetConfig
) -> List[str]:
    failures: List[str] = []
    cur = connection.cursor()
    placeholders = ", ".join("?" for _ in ds.columns)
    ins = f'INSERT INTO "{ds.table_name}" VALUES ({placeholders})'

    for json_path in json_files:
        try:
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            failures.append(f"{ds.name}: {json_path.name} — ошибка JSON ({exc})")
            continue

        had_no_fields_warn = False
        for idx, rec in enumerate(_ensure_list_of_dict(payload), start=1):
            row, warn, no_fields = build_l_row(json_path.name, rec, idx)
            if warn:
                failures.extend(f"{ds.name}: {w}" for w in warn)
                continue
            cur.execute(ins, row)
            had_no_fields_warn = had_no_fields_warn or no_fields
        connection.commit()

    return failures

def build_l_row(filename: str, record: Dict, index: int) -> Tuple[List[str], List[str], bool]:
    warnings: List[str] = []

    fields, specifics, used_fallback = _extract_fields_and_specifics(record)

    ean_value = _ser(_extract_ean(fields, record))
    if not ean_value:
        warnings.append(f"{filename}: запись #{index} — отсутствует значение EAN")
        return [], warnings, used_fallback

    row = [
        filename,
        ean_value,
        ean_value,  # record_id
        _fld(fields, "Artikelbeschreibung") or _fld(fields, "Title") or _fld(fields, "Titel"),
        _fld(fields, "Startpreis") or _fld(fields, "Preis") or _fld(fields, "price"),
        _fld(fields, "GalleryURL"),
        _fld(fields, "PictureURL"),
        _fld(fields, "pictureurls") or _fld(fields, "PictureURLs"),
        _spec(specifics, "Breite") or _fld(fields, "Breite"),
        _spec(specifics, "Höhe") or _fld(fields, "Höhe"),
        _spec(specifics, "Länge") or _fld(fields, "Länge"),
        _spec(specifics, "Marke") or _fld(fields, "Marke") or _fld(fields, "Brand"),
        _spec(specifics, "Produktart") or _fld(fields, "Produktart"),
        _spec(specifics, "Farbe") or _fld(fields, "Farbe") or _fld(fields, "Color"),
        _spec(specifics, "Material") or _fld(fields, "Material"),
        _spec(specifics, "Stil") or _fld(fields, "Stil") or _fld(fields, "Style"),
        _spec(specifics, "Zimmer") or _fld(fields, "Zimmer"),
        _spec(specifics, "Herstellernummer") or _fld(fields, "Herstellernummer") or _fld(fields, "MPN"),
        _spec(specifics, "Form") or _fld(fields, "Form"),
    ]

    if used_fallback:
        warnings.append(f"{filename}: запись #{index} — использована fallback-схема без 'fields'")

    return row, warnings, used_fallback

# ---------- Ingest: Produkt (CSV) ----------
def ingest_produkt_csv(
    connection: sqlite3.Connection, csv_files: List[Path], ds: DatasetConfig
) -> List[str]:
    failures: List[str] = []
    cur = connection.cursor()
    placeholders = ", ".join("?" for _ in ds.columns)
    ins = f'INSERT INTO "{ds.table_name}" VALUES ({placeholders})'

    for csv_path in csv_files:
        try:
            with csv_path.open("r", encoding="utf-8", newline="") as f:
                sample = f.read(4096)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
                except Exception:
                    dialect = csv.get_dialect("excel")
                reader = csv.DictReader(f, dialect=dialect)

                missing = [c for c in ds.columns if not reader.fieldnames or c not in reader.fieldnames]
                if missing:
                    failures.append(f"{ds.name}: {csv_path.name} — отсутствуют столбцы: {', '.join(missing)}")

                for i, row in enumerate(reader, start=2):
                    vals = [_ser((row or {}).get(col, "")) for col in ds.columns]
                    cur.execute(ins, vals)
            connection.commit()
        except Exception as exc:
            failures.append(f"{ds.name}: {csv_path.name} — ошибка CSV ({exc})")

    return failures

# ---------- Helpers ----------
def _fld(fields: Dict[str, Any], key: str) -> str:
    return _ser(fields.get(key))

def _spec(specs: Dict[str, Any], key: str) -> str:
    return _ser(specs.get(key))

def _ser(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        parts = [p for p in (_ser(v) for v in value) if p]
        return "; ".join(parts)
    if isinstance(value, bool):
        return "True" if value else "False"
    return str(value)

def _ensure_list_of_dict(records: object) -> List[Dict]:
    if not isinstance(records, list):
        return []
    return [r for r in records if isinstance(r, dict)]

def _kvlist_to_dict(obj: Any, key_name: str = "key", value_name: str = "value") -> Dict[str, Any]:
    if isinstance(obj, list):
        out: Dict[str, Any] = {}
        for it in obj:
            if isinstance(it, dict) and key_name in it and value_name in it:
                k = str(it[key_name])
                v = it[value_name]
                if k in out:
                    prev = out[k]
                    out[k] = prev + [v] if isinstance(prev, list) else [prev, v]
                else:
                    out[k] = v
        return out
    return {}

def _first_nonempty(*vals: Any) -> str:
    for v in vals:
        s = _ser(v)
        if s:
            return s
    return ""

def _extract_fields_and_specifics(record: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], bool]:
    used_fallback = False
    # fields
    if isinstance(record.get("fields"), dict):
        fields: Dict[str, Any] = dict(record["fields"])
    elif isinstance(record.get("fields"), list):
        fields = _kvlist_to_dict(record["fields"])
        used_fallback = True
    else:
        fields = {}
        for path in (("data", "fields"), ("payload", "fields"), ("attributes",)):
            cur: Any = record
            ok = True
            for p in path:
                if isinstance(cur, dict) and p in cur:
                    cur = cur[p]
                else:
                    ok = False
                    break
            if ok:
                if isinstance(cur, dict):
                    fields = dict(cur)
                elif isinstance(cur, list):
                    fields = _kvlist_to_dict(cur)
                used_fallback = True
                break
        if not fields and isinstance(record, dict):
            fields = {k: v for k, v in record.items() if not isinstance(v, (dict, list))}
            used_fallback = True

    # specifics
    spec_src = record.get("itemSpecifics")
    if isinstance(spec_src, dict):
        specifics: Dict[str, Any] = spec_src
    elif isinstance(spec_src, list):
        specifics = _kvlist_to_dict(spec_src, key_name="name", value_name="value") or _kvlist_to_dict(spec_src)
    else:
        specifics = {}
        for key in ("specifics", "attributes", "props"):
            cand = record.get(key)
            if isinstance(cand, dict):
                specifics = cand
                break
            if isinstance(cand, list):
                specifics = _kvlist_to_dict(cand)
                break

    return fields, specifics, used_fallback

def _extract_ean(fields: Dict[str, Any], record: Dict[str, Any]) -> str:
    return _first_nonempty(
        record.get("ean"),
        fields.get("EAN"),
        fields.get("ean"),
        fields.get("ean13"),
        fields.get("Barcode"),
        fields.get("barcode"),
        record.get("barcode"),
        record.get("EAN"),
    )

def append_log(lines: Iterable[str]) -> None:
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        for line in lines:
            fh.write(line + "\n")

if __name__ == "__main__":
    main()
