"""Build separate SQLite databases from readyJSON and readyhtml exports.

Для каждого аккаунта (JV, XL) создаются два файла:
  * DB/<account>_json.db — выбранные поля из JSON-файлов;
  * DB/<account>_html.db — HTML-страницы, найденные по EAN у выбранного аккаунта.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

BASE_DIR = Path(__file__).resolve().parent
READY_JSON_DIR = BASE_DIR / "readyJSON"
READY_HTML_DIR = BASE_DIR / "readyhtml"
DB_DIR = BASE_DIR / "DB"

SELECTED_FIELDS: List[Tuple[str, str]] = [
    ("fabric", "Fabric"),
    ("id", "ID"),
    ("ean", "EAN"),
    ("artikelbeschreibung", "Artikelbeschreibung"),
    ("produktart", "Produktart"),
    ("maße", "Maße"),
    ("farbe", "Farbe"),
    ("breite", "Breite"),
    ("höhe", "Höhe"),
    ("länge", "Länge"),
    ("zimmer", "Zimmer"),
    ("galleryurl", "GalleryURL"),
    ("pictureurl", "PictureURL"),
    ("pictureurls", "pictureurls"),
]

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from getEANfromJSON import (  # noqa: E402
    ACCOUNTS,
    load_collections as ean_load_collections,
)


@dataclass
class ItemRecord:
    account: str
    factory_id: str
    factory_name: str
    json_path: Path
    payload: dict
    ean: Optional[str]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Собрать отдельные SQLite-базы из JSON и HTML данных."
    )
    parser.add_argument(
        "--accounts",
        nargs="*",
        choices=ACCOUNTS,
        default=list(ACCOUNTS),
        help="Какие аккаунты обработать (по умолчанию JV и XL).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Перезаписать существующие базы.",
    )
    parser.add_argument(
        "--skip-html",
        action="store_true",
        help="Не создавать HTML-базы.",
    )
    parser.add_argument(
        "--skip-json",
        action="store_true",
        help="Не создавать JSON-базы.",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Печатать прогресс для каждой фабрики.",
    )
    return parser.parse_args(argv)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_json_file(path: Path) -> List[dict]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        data = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def derive_factory_id(json_path: Path) -> str:
    stem = json_path.stem
    return stem.rsplit("_", 1)[-1]


def collect_items(account: str, progress: bool = False) -> List[ItemRecord]:
    json_dir = READY_JSON_DIR / account
    if not json_dir.exists():
        print(f"[{account}] Каталог JSON не найден: {json_dir}")
        return []

    try:
        collections = ean_load_collections(account)
    except Exception as exc:  # noqa: BLE001
        print(f"[{account}] Не удалось загрузить collections.json: {exc}")
        collections = {}

    items: List[ItemRecord] = []
    json_files = sorted(json_dir.glob("*.json"))
    for json_path in json_files:
        factory_id = derive_factory_id(json_path)
        factory_name = collections.get(factory_id) or json_path.stem.split("__")[0] or f"factory_{factory_id}"
        payloads = load_json_file(json_path)
        if progress:
            print(f"[{account}] {factory_name} ({factory_id}) — {len(payloads)} записей")
        for payload in payloads:
            if not isinstance(payload, dict):
                continue
            ean_raw = payload.get("ean")
            ean = str(ean_raw).strip() if ean_raw is not None else None
            items.append(
                ItemRecord(
                    account=account,
                    factory_id=factory_id,
                    factory_name=factory_name,
                    json_path=json_path,
                    payload=payload,
                    ean=ean or None,
                )
            )
    return items


def relative_to_base(path: Path) -> str:
    try:
        return str(path.relative_to(BASE_DIR))
    except ValueError:
        return str(path)


def create_html_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = OFF;
        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            factory_id TEXT,
            factory_name TEXT,
            ean TEXT NOT NULL,
            html_path TEXT NOT NULL,
            html TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_pages_ean ON pages (ean);
        CREATE INDEX IF NOT EXISTS idx_pages_factory ON pages (factory_id);
        """
    )


def normalize_value(value: object) -> Optional[object]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, str):
        return value
    # lists / dicts и прочее сериализуем обратно в JSON
    try:
        return json.dumps(value, ensure_ascii=False)
    except TypeError:
        return str(value)


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def write_json_database(account: str, items: List[ItemRecord], overwrite: bool) -> Path:
    db_path = DB_DIR / f"{account}_json.db"
    ensure_dir(db_path.parent)
    if db_path.exists():
        if overwrite:
            db_path.unlink()
        else:
            raise FileExistsError(f"Файл {db_path} уже существует. Используйте --overwrite.")

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = OFF;")
        column_defs = ""
        if SELECTED_FIELDS:
            fields_sql = ",\n            ".join(
                f"{quote_identifier(col)} TEXT" for _, col in SELECTED_FIELDS
            )
            column_defs = ",\n            " + fields_sql

        conn.executescript(
            f"""
            DROP TABLE IF EXISTS items;
            CREATE TABLE items (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                factory_id TEXT NOT NULL,
                factory_name TEXT{column_defs}
            );
            CREATE INDEX idx_items_factory ON items (factory_id);
            """
        )

        for key, column in SELECTED_FIELDS:
            if key == "ean":
                conn.execute(
                    f'CREATE INDEX idx_items_ean ON items ({quote_identifier(column)});'
                )
                break

        insert_columns = ["factory_id", "factory_name"] + [
            col for _, col in SELECTED_FIELDS
        ]
        insert_sql = (
            f'INSERT INTO items ({", ".join(quote_identifier(c) for c in insert_columns)}) '
            f'VALUES ({", ".join("?" for _ in insert_columns)})'
        )

        rows: List[Tuple[object, ...]] = []
        for item in items:
            values: List[object] = [
                item.factory_id,
                item.factory_name,
            ]
            lowered = {str(k).lower(): v for k, v in item.payload.items()}
            for key, _ in SELECTED_FIELDS:
                if key == "maße":
                    base_value = lowered.get("maße")
                    if base_value is None:
                        sizes = {
                            str(orig_key): orig_val
                            for orig_key, orig_val in item.payload.items()
                            if isinstance(orig_key, str)
                            and "maße" in orig_key.lower()
                        }
                        base_value = sizes or None
                    values.append(normalize_value(base_value))
                else:
                    values.append(normalize_value(lowered.get(key)))
            rows.append(tuple(values))

        conn.executemany(insert_sql, rows)
        conn.commit()
    finally:
        conn.close()
    return db_path


def load_html(account: str, ean: str) -> Optional[Tuple[str, str]]:
    html_path = READY_HTML_DIR / account / f"{ean}.html"
    if not html_path.exists():
        return None
    try:
        html_text = html_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        html_text = html_path.read_text(encoding="utf-8", errors="ignore")
    return relative_to_base(html_path), html_text


def write_html_database(account: str, items: List[ItemRecord], overwrite: bool) -> Optional[Path]:
    db_path = DB_DIR / f"{account}_html.db"
    ensure_dir(db_path.parent)
    if db_path.exists():
        if overwrite:
            db_path.unlink()
        else:
            raise FileExistsError(f"Файл {db_path} уже существует. Используйте --overwrite.")

    conn = sqlite3.connect(str(db_path))
    created_rows = 0
    try:
        create_html_schema(conn)
        for item in items:
            if not item.ean:
                continue
            html_payload = load_html(item.account, item.ean)
            if not html_payload:
                continue
            html_path, html_text = html_payload
            conn.execute(
                """
                INSERT INTO pages (factory_id, factory_name, ean, html_path, html)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    item.factory_id,
                    item.factory_name,
                    item.ean,
                    html_path,
                    html_text,
                ),
            )
            created_rows += 1
        conn.commit()
    finally:
        conn.close()

    if created_rows == 0:
        db_path.unlink(missing_ok=True)
        return None
    return db_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if args.skip_html and args.skip_json:
        print("Нечего делать: указаны оба флага --skip-json и --skip-html.")
        return 1

    if args.overwrite:
        ensure_dir(DB_DIR)
        for existing_db in DB_DIR.glob("*.db"):
            try:
                existing_db.unlink()
                print(f"Удалён старый файл БД: {existing_db}")
            except Exception as exc:  # noqa: BLE001
                print(f"Не удалось удалить {existing_db}: {exc}")

    for account in args.accounts:
        print(f"\n=== Обработка аккаунта {account} ===")
        items = collect_items(account, progress=args.progress)
        if not items:
            print(f"[{account}] Нет данных для обработки.")
            continue

        if not args.skip_json:
            try:
                json_db = write_json_database(
                    account, items, overwrite=args.overwrite
                )
                print(f"[{account}] JSON база: {json_db}")
            except FileExistsError as exc:
                print(exc)

        if not args.skip_html:
            try:
                html_db = write_html_database(account, items, overwrite=args.overwrite)
                if html_db:
                    print(f"[{account}] HTML база: {html_db}")
                else:
                    print(f"[{account}] HTML база не создана (нет файлов HTML).")
            except FileExistsError as exc:
                print(exc)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
