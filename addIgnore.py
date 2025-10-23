from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


ALL_FABRIKS: Dict[str, Path] = {
    "JV_F_P": Path("Fabriks") / "JV_F_P" / "factories.json",
    "XL_F_P": Path("Fabriks") / "XL_F_P" / "factories.json",
    "JV_F_L": Path("Fabriks") / "JV_F_L" / "collections.json",
    "XL_F_L": Path("Fabriks") / "XL_F_L" / "collections.json",
}

IGNORE_FABRIKS: Dict[str, Path] = {
    "JV_F_L": Path("Ignore") / "JV_L.json",
    "XL_F_L": Path("Ignore") / "XL_L.json",
}


def _ensure_ignore_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("[]", encoding="utf-8")


def _load_json_list(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _save_json_list(path: Path, data: Iterable[Dict[str, str]]) -> None:
    _ensure_ignore_file(path)
    path.write_text(json.dumps(list(data), ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_entry(id_value: str, name: Optional[str]) -> Dict[str, str]:
    entry: Dict[str, str] = {"id": str(id_value)}
    if name:
        entry["name"] = name
    return entry


def _find_factory_name(factory_type: str, factory_id: str) -> Optional[str]:
    source = ALL_FABRIKS.get(factory_type)
    if source is None or not source.exists():
        return None
    try:
        factories = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if isinstance(factories, list):
        for item in factories:
            if not isinstance(item, dict):
                continue
            value = str(item.get("id") or item.get("ID") or "")
            if value == str(factory_id):
                raw_name = item.get("name") or item.get("Name") or ""
                return str(raw_name) if raw_name else None
    return None


def add_ignore_entry(
    factory_type: str,
    factory_id: str,
    *,
    name: Optional[str] = None,
    overwrite_name: bool = False,
) -> Tuple[str, Dict[str, str]]:
    if factory_type not in IGNORE_FABRIKS:
        raise ValueError(f"Неизвестный тип фабрики: {factory_type}")

    target = IGNORE_FABRIKS[factory_type]
    _ensure_ignore_file(target)
    entries = _load_json_list(target)

    factory_id_str = str(factory_id)
    resolved_name = name or _find_factory_name(factory_type, factory_id_str)
    entry = _normalize_entry(factory_id_str, resolved_name)

    for existing in entries:
        if str(existing.get("id")) != factory_id_str:
            continue
        if resolved_name and (overwrite_name or not existing.get("name")):
            existing["name"] = resolved_name
            _save_json_list(target, entries)
            return ("updated", existing)
        return ("exists", existing)

    entries.append(entry)
    _save_json_list(target, entries)
    return ("added", entry)


def show_ignore_list(factory_type: str) -> None:
    target = IGNORE_FABRIKS.get(factory_type)
    if target is None:
        raise ValueError(f"Неизвестный тип фабрики: {factory_type}")
    entries = _load_json_list(target)
    if not entries:
        print(f"[INFO] Ignore список для {factory_type} пуст.", file=sys.stderr)
        return
    print(f"Ignore список для {factory_type}:")
    for item in entries:
        print(f"  - {item.get('id')} :: {item.get('name', '')}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Добавить фабрики в файлы Ignore/* из имеющихся выгрузок."
    )
    parser.add_argument(
        "--type",
        dest="factory_type",
        choices=sorted(ALL_FABRIKS.keys()),
        help="Тип фабрики (см. ALL_FABRIKS).",
    )
    parser.add_argument(
        "--id",
        dest="factory_id",
        help="Идентификатор фабрики, которую нужно добавить в ignore.",
    )
    parser.add_argument(
        "--name",
        dest="factory_name",
        help="Название фабрики (если не указано — будет найдено автоматически).",
    )
    parser.add_argument(
        "--overwrite-name",
        action="store_true",
        help="Перезаписать имя, если оно уже есть в ignore.",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Показать текущий список ignore для указанного типа и выйти.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.show:
        if not args.factory_type:
            print("[ERROR] Для вывода списка нужно указать --type.", file=sys.stderr)
            sys.exit(1)
        show_ignore_list(args.factory_type)
        return

    if not args.factory_type or not args.factory_id:
        print("[ERROR] Необходимо указать --type и --id.", file=sys.stderr)
        sys.exit(1)

    status, entry = add_ignore_entry(
        args.factory_type,
        args.factory_id,
        name=args.factory_name,
        overwrite_name=args.overwrite_name,
    )
    if status == "added":
        print(
            f"[OK] Добавлено в {args.factory_type}: {entry.get('id')} ({entry.get('name', '')})"
        )
    elif status == "updated":
        print(
            f"[OK] Обновлено имя в {args.factory_type}: {entry.get('id')} ({entry.get('name', '')})"
        )
    else:
        print(
            f"[SKIP] Запись уже существует в {args.factory_type}: {entry.get('id')} ({entry.get('name', '')})"
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
