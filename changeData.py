from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from flask import Flask, abort, jsonify, render_template_string, request, url_for


@dataclass
class DatabaseSource:
    account: str
    path: Path
    farm_base: str
    count: int = field(init=False, default=0)

    def edit_url(self, item_id: Optional[str]) -> Optional[str]:
        if not item_id:
            return None
        return (
            f"{self.farm_base}/afterbuy/ebayliste2.aspx"
            f"?art=edit&id={item_id}&rsposition=0&rssuchbegriff="
        )


DATABASES: List[DatabaseSource] = [
    DatabaseSource("JV", Path("DB") / "JV_json.db", "https://farm01.afterbuy.de"),
    DatabaseSource("XL", Path("DB") / "XL_json.db", "https://farm04.afterbuy.de"),
]

app = Flask(__name__)


def _count_rows(path: Path) -> int:
    with sqlite3.connect(path) as conn:
        cur = conn.execute("SELECT COUNT(*) FROM items")
        value, = cur.fetchone()
    return int(value or 0)


def _initialize_counts() -> None:
    total = 0
    for source in DATABASES:
        if not source.path.exists():
            source.count = 0
            continue
        source.count = _count_rows(source.path)
        total += source.count
    return total


def _parse_picture_urls(raw_value: Optional[str]) -> List[str]:
    if not raw_value:
        return []
    candidates: Iterable[str]
    value = raw_value.strip()
    if not value:
        return []
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        separators = [",", "\n", ";"]
        for sep in separators:
            if sep in value:
                candidates = (part.strip() for part in value.split(sep))
                break
        else:
            candidates = (value,)
    else:
        if isinstance(decoded, str):
            candidates = (decoded,)
        elif isinstance(decoded, Iterable):
            candidates = (str(item).strip() for item in decoded if item)
        else:
            candidates = ()
    result: List[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not candidate:
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        result.append(candidate)
    return result


def _build_image_list(row: Dict[str, Optional[str]]) -> List[str]:
    images: List[str] = []
    for key in ("GalleryURL", "PictureURL"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            images.append(value.strip())
    images.extend(_parse_picture_urls(row.get("pictureurls")))
    deduped: List[str] = []
    seen: set[str] = set()
    for url in images:
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def _fetch_row(source: DatabaseSource, offset: int) -> Tuple[Dict[str, Optional[str]], Sequence[str]]:
    with sqlite3.connect(source.path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            "SELECT * FROM items ORDER BY row_id LIMIT 1 OFFSET ?",
            (offset,),
        )
        row = cur.fetchone()
        if row is None:
            raise IndexError(f"Row at offset {offset} not found in {source.account}.")
        keys = row.keys()
        data = {key: row[key] for key in keys}
    return data, keys


def _resolve_item(global_index: int) -> Tuple[DatabaseSource, Dict[str, Optional[str]], Sequence[str], int]:
    remaining = global_index
    for source in DATABASES:
        if remaining < source.count:
            data, keys = _fetch_row(source, remaining)
            return source, data, keys, remaining
        remaining -= source.count
    raise IndexError(f"Index {global_index} outside dataset range.")


TOTAL_COUNT = _initialize_counts()


def _ensure_index(idx: int) -> int:
    if TOTAL_COUNT == 0:
        abort(503, description="Нет данных для отображения.")
    if idx < 0 or idx >= TOTAL_COUNT:
        abort(404, description="Элемент с указанным индексом не найден.")
    return idx


def _prepare_item_payload(idx: int) -> Dict[str, object]:
    source, row, field_order, local_index = _resolve_item(idx)
    images = _build_image_list(row)
    edit_link = source.edit_url(row.get("ID"))

    field_pairs: List[Tuple[str, Optional[str]]] = [
        (key, row.get(key)) for key in field_order
    ]

    return {
        "account": source.account,
        "farm_base": source.farm_base,
        "edit_url": edit_link,
        "images": images,
        "fields": field_pairs,
        "global_index": idx,
        "local_index": local_index,
        "account_total": source.count,
        "total": TOTAL_COUNT,
    }


HTML_TEMPLATE = """
<!doctype html>
<html lang="ru">
  <head>
    <meta charset="utf-8" />
    <title>Просмотр товаров Afterbuy</title>
    <style>
      body {
        font-family: Arial, sans-serif;
        margin: 0;
        padding: 24px;
        background-color: #f5f7fa;
        color: #1f2937;
      }
      header {
        margin-bottom: 24px;
      }
      main {
        max-width: 1200px;
        margin: 0 auto;
        background: #ffffff;
        border-radius: 12px;
        box-shadow: 0 12px 40px rgba(15, 23, 42, 0.08);
        padding: 32px;
      }
      .meta {
        display: flex;
        flex-wrap: wrap;
        gap: 16px;
        margin-bottom: 24px;
      }
      .meta > div {
        background: #f9fafb;
        border-radius: 8px;
        padding: 12px 16px;
        min-width: 200px;
      }
      .nav {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 24px;
      }
      .nav a, .nav span {
        font-weight: 600;
        color: #2563eb;
        text-decoration: none;
      }
      .nav a:hover {
        text-decoration: underline;
      }
      .images {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
        gap: 16px;
        margin-bottom: 32px;
      }
      .images img {
        width: 100%;
        height: 200px;
        object-fit: cover;
        border-radius: 12px;
        box-shadow: 0 8px 24px rgba(15, 23, 42, 0.12);
      }
      table {
        width: 100%;
        border-collapse: collapse;
        background: #f9fafb;
        border-radius: 12px;
        overflow: hidden;
      }
      th, td {
        border-bottom: 1px solid #e5e7eb;
        padding: 12px 16px;
        text-align: left;
        vertical-align: top;
      }
      th {
        width: 240px;
        background: #f3f4f6;
        text-transform: uppercase;
        font-size: 13px;
        letter-spacing: 0.06em;
        color: #4b5563;
      }
      tr:last-child td {
        border-bottom: none;
      }
      .empty {
        color: #9ca3af;
        font-style: italic;
      }
      .link-block {
        margin-bottom: 24px;
      }
      .link-block a {
        display: inline-block;
        background: #2563eb;
        color: #ffffff;
        padding: 10px 18px;
        border-radius: 8px;
        text-decoration: none;
        font-weight: 600;
      }
      .link-block a:hover {
        background: #1d4ed8;
      }
    </style>
  </head>
  <body>
    <main>
      <header>
        <h1>Просмотр товаров Afterbuy</h1>
      </header>
      <div class="nav">
        {% if prev_idx is not none %}
          <a href="{{ url_for('show_item', idx=prev_idx) }}">&larr; Предыдущий</a>
        {% else %}
          <span></span>
        {% endif %}
        <span>#{{ payload.global_index + 1 }} из {{ payload.total }}</span>
        {% if next_idx is not none %}
          <a href="{{ url_for('show_item', idx=next_idx) }}">Следующий &rarr;</a>
        {% else %}
          <span></span>
        {% endif %}
      </div>
      <section class="meta">
        <div><strong>Аккаунт:</strong> {{ payload.account }}</div>
        <div><strong>Позиция в аккаунте:</strong> {{ payload.local_index + 1 }} / {{ payload.account_total }}</div>
        <div><strong>Источник БД:</strong> {{ payload.farm_base }}</div>
      </section>
      {% if payload.edit_url %}
      <div class="link-block">
        <a href="{{ payload.edit_url }}" target="_blank" rel="noopener noreferrer">
          Открыть товар в Afterbuy
        </a>
      </div>
      {% endif %}
      {% if payload.images %}
      <section class="images">
        {% for img in payload.images %}
          <a href="{{ img }}" target="_blank" rel="noopener noreferrer">
            <img src="{{ img }}" alt="Фото товара {{ loop.index }}" loading="lazy" />
          </a>
        {% endfor %}
      </section>
      {% endif %}
      <section>
        <table>
          <tbody>
            {% for key, value in payload.fields %}
              <tr>
                <th>{{ key }}</th>
                <td>
                  {% if value is none or (value is string and not value.strip()) %}
                    <span class="empty">нет данных</span>
                  {% elif key == 'pictureurls' %}
                    <pre>{{ pictureurls_display }}</pre>
                  {% else %}
                    {{ value }}
                  {% endif %}
                </td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
      </section>
    </main>
  </body>
</html>
"""


@app.route("/")
def show_item() -> str:
    idx = request.args.get("idx", default="0")
    try:
        idx_int = int(idx)
    except ValueError:
        abort(400, description="Индекс должен быть числом.")
    idx_int = _ensure_index(idx_int)
    payload = _prepare_item_payload(idx_int)

    prev_idx = idx_int - 1 if idx_int > 0 else None
    next_idx = idx_int + 1 if idx_int + 1 < TOTAL_COUNT else None

    pictureurls_raw = None
    for key, value in payload["fields"]:
        if key == "pictureurls":
            pictureurls_raw = value
            break
    if isinstance(pictureurls_raw, str):
        try:
            parsed = json.loads(pictureurls_raw)
            pictureurls_display = json.dumps(parsed, ensure_ascii=False, indent=2)
        except json.JSONDecodeError:
            pictureurls_display = pictureurls_raw
    else:
        pictureurls_display = pictureurls_raw or ""

    return render_template_string(
        HTML_TEMPLATE,
        payload=payload,
        prev_idx=prev_idx,
        next_idx=next_idx,
        pictureurls_display=pictureurls_display,
    )


@app.route("/api/items/<int:idx>")
def get_item(idx: int):
    idx = _ensure_index(idx)
    payload = _prepare_item_payload(idx)
    return jsonify(payload)


if __name__ == "__main__":
    if TOTAL_COUNT == 0:
        print("В базе нет данных для отображения. Проверьте файлы DB/JV_json.db и DB/XL_json.db.")
    app.run(debug=True, port=5000)
