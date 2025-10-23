# -*- coding: utf-8 -*-
"""
build_ean_id_db.py
— Собирает пары (ID, EAN) из readyJSON/JV и readyJSON/XL
— Пишет в SQLite: DB/all_ean_id.db, таблица items(id TEXT, ean TEXT, acc TEXT, src TEXT)
Запуск:  python build_ean_id_db.py
"""

import os, re, json, glob, sys
import typing as t
import sqlite3
from datetime import datetime

BASES = [
    ("JV", os.path.join("readyJSON", "JV")),
    ("XL", os.path.join("readyJSON", "XL")),
]

DB_DIR = "DB"
DB_PATH = os.path.join(DB_DIR, "all_ean_id.db")

def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)

# ---------- извлечение ID/EAN ----------
def iter_products(obj: t.Any) -> t.Iterable[dict]:
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            kl = {k.lower() for k in cur.keys()}
            if ("id" in kl or "itemid" in kl or "item_id" in kl) and ("ean" in kl or any(re.search(r"ean", k, re.I) for k in cur.keys())):
                yield cur
            for v in cur.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(cur, list):
            for v in cur:
                if isinstance(v, (dict, list)):
                    stack.append(v)

def extract_id(d: dict) -> t.Optional[str]:
    for k, v in d.items():
        if k.lower() in {"id", "itemid", "item_id"} and v is not None:
            s = str(v).strip()
            if re.fullmatch(r"\d{6,}", s):
                return s
    # запасной поиск числового ID
    for v in d.values():
        if v is None:
            continue
        s = str(v).strip()
        if re.fullmatch(r"\d{6,}", s):
            return s
    return None

def extract_ean(d: dict) -> t.Optional[str]:
    # явное поле
    for k, v in d.items():
        if re.fullmatch(r"ean", k, flags=re.I) and v is not None:
            s = re.sub(r"\D", "", str(v))
            if 8 <= len(s) <= 18:
                return s
    # запасной: любая цифропоследовательность длиной 8..18
    for v in d.values():
        if v is None:
            continue
        s = re.sub(r"\D", "", str(v))
        if 8 <= len(s) <= 18:
            return s
    return None

# ---------- БД ----------
DDL = """
CREATE TABLE IF NOT EXISTS items(
    id  TEXT NOT NULL,
    ean TEXT NOT NULL,
    acc TEXT NOT NULL,      -- JV|XL
    src TEXT NOT NULL,      -- путь к json
    PRIMARY KEY (ean, id)   -- уникальность пары
);
"""

def ensure_db(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute(DDL)
    return con

def insert_batch(con: sqlite3.Connection, rows: list[tuple[str, str, str, str]]) -> tuple[int,int]:
    cur = con.cursor()
    ok = dup = 0
    for r in rows:
        try:
            cur.execute("INSERT OR IGNORE INTO items(id, ean, acc, src) VALUES(?,?,?,?)", r)
            if cur.rowcount == 0:
                dup += 1
            else:
                ok += 1
        except sqlite3.Error:
            dup += 1
    con.commit()
    return ok, dup

# ---------- main ----------
def main() -> int:
    print("=== START build_ean_id_db ===")
    con = ensure_db(DB_PATH)

    total_files = 0
    total_found = 0
    total_inserted = 0
    total_dups = 0

    for acc, jdir in BASES:
        files = sorted(glob.glob(os.path.join(jdir, "*.json")))
        if not files:
            continue
        log(f"{acc}: файлов JSON: {len(files)}")
        total_files += len(files)

        batch: list[tuple[str,str,str,str]] = []
        for fp in files:
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                log(f"{acc}: ошибка чтения {fp}: {e}")
                continue

            for prod in iter_products(data):
                item_id = extract_id(prod)
                ean = extract_ean(prod)
                if not item_id or not ean:
                    continue
                total_found += 1
                batch.append((item_id, ean, acc, fp))

                if len(batch) >= 5000:  # пакетная вставка
                    ok, dup = insert_batch(con, batch)
                    total_inserted += ok
                    total_dups += dup
                    batch.clear()

        if batch:
            ok, dup = insert_batch(con, batch)
            total_inserted += ok
            total_dups += dup

    log(f"JSON файлов: {total_files}")
    log(f"Найдено пар: {total_found}")
    log(f"Вставлено: {total_inserted}, дубликатов: {total_dups}")
    con.close()
    log(f"Готово. БД: {DB_PATH}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
