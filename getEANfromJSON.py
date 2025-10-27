"""Extract unique EANs for a selected Afterbuy factory."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

BASE_DIR = Path(__file__).resolve().parent
READY_ROOT = BASE_DIR / "readyJSON"
COLLECTIONS_ROOT = BASE_DIR / "Fabriks"
ACCOUNTS = ("JV", "XL")
DEFAULT_FORMAT = "text"
FORMATS = ("text", "json", "csv")


class FactoryLookupError(RuntimeError):
    pass


def normalize_account(value: str) -> str:
    account = value.upper()
    if account not in ACCOUNTS:
        raise argparse.ArgumentTypeError(f"Unknown account '{value}'. Use one of: {', '.join(ACCOUNTS)}")
    return account


def load_collections(account: str) -> Dict[str, str]:
    path = COLLECTIONS_ROOT / f"{account}_F_L" / "collections.json"
    if not path.exists():
        raise FileNotFoundError(f"Collections file not found: {path}")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Failed to parse {path}: {exc}") from exc
    collections: Dict[str, str] = {}
    for entry in raw:
        factory_id = str(entry.get("id", "")).strip()
        name = str(entry.get("name", "")).strip()
        if factory_id:
            collections[factory_id] = name or f"factory_{factory_id}"
    return collections


def build_ready_index(account: str) -> Dict[str, List[Path]]:
    ready_dir = READY_ROOT / account
    if not ready_dir.exists():
        return {}
    mapping: Dict[str, List[Path]] = {}
    for path in ready_dir.glob("*.json"):
        stem = path.stem
        if "_" not in stem:
            continue
        factory_id = stem.rsplit("_", 1)[-1]
        if not factory_id:
            continue
        mapping.setdefault(factory_id, []).append(path)
    for paths in mapping.values():
        paths.sort()
    return mapping


def list_factories(
    account: str,
    collections: Dict[str, str],
    ready_index: Dict[str, List[Path]],
    search: Optional[str] = None,
) -> None:
    rows: List[Tuple[str, str, bool]] = []
    term = search.casefold() if search else None
    for factory_id, name in sorted(collections.items(), key=lambda item: item[1].casefold()):
        if term and term not in factory_id.casefold() and term not in name.casefold():
            continue
        rows.append((factory_id, name, factory_id in ready_index))

    if not rows:
        print("Нет фабрик, подходящих под критерии.", file=sys.stderr)
        return

    print(f"[{account}] Доступные фабрики (✓ — есть JSON в readyJSON/{account}):")
    width = max(len(row[0]) for row in rows)
    for factory_id, name, has_ready in rows:
        marker = "✓" if has_ready else " "
        print(f" {marker} {factory_id.rjust(width)}  {name}")


def resolve_factory(
    collections: Dict[str, str],
    factory_id: Optional[str],
    factory_name: Optional[str],
) -> Tuple[str, str]:
    if factory_id:
        factory_id = factory_id.strip()
        if factory_id not in collections:
            raise FactoryLookupError(f"Factory id '{factory_id}' not found in collections.")
        return factory_id, collections[factory_id]

    if not factory_name:
        raise FactoryLookupError("Factory id or name must be provided.")

    name_query = factory_name.strip().casefold()
    exact_matches = [(fid, fname) for fid, fname in collections.items() if fname.casefold() == name_query]
    if len(exact_matches) == 1:
        return exact_matches[0]

    partial_matches = [
        (fid, fname)
        for fid, fname in collections.items()
        if name_query in fname.casefold() or name_query in fid.casefold()
    ]
    if len(partial_matches) == 1:
        return partial_matches[0]
    if not partial_matches:
        raise FactoryLookupError(f"Factory '{factory_name}' not found.")
    options = ", ".join(f"{fid} ({fname})" for fid, fname in partial_matches[:10])
    raise FactoryLookupError(
        f"Factory name '{factory_name}' is ambiguous. Candidates: {options}"
    )


def extract_eans(
    paths: Sequence[Path],
    *,
    dedupe: bool = True,
    include_empty: bool = False,
) -> Tuple[List[str], int, int, List[str]]:
    seen: set[str] = set()
    eans: List[str] = []
    empties = 0
    empty_details: List[str] = []
    total = 0

    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Failed to parse {path}: {exc}") from exc
        if not isinstance(payload, list):
            raise ValueError(f"Unexpected JSON structure in {path}: expected list.")
        for idx, item in enumerate(payload, start=1):
            total += 1
            ean_raw = item.get("ean") if isinstance(item, dict) else None
            if ean_raw is None:
                empties += 1
                if include_empty:
                    empty_details.append(f"{path.name}:{idx}")
                continue
            ean = str(ean_raw).strip()
            if not ean:
                empties += 1
                if include_empty:
                    empty_details.append(f"{path.name}:{idx}")
                continue
            if dedupe:
                if ean in seen:
                    continue
                seen.add(ean)
            eans.append(ean)

    return eans, total, empties, empty_details


def format_output(
    *,
    output_format: str,
    eans: Sequence[str],
    account: str,
    factory_id: str,
    factory_name: str,
    source_files: Sequence[Path],
    total_items: int,
    empty_count: int,
    empty_details: Sequence[str],
) -> str:
    relative_sources: List[str] = []
    for path in source_files:
        try:
            relative_sources.append(str(path.relative_to(BASE_DIR)))
        except ValueError:
            relative_sources.append(str(path))
    if output_format == "json":
        payload = {
            "account": account,
            "factory_id": factory_id,
            "factory_name": factory_name,
            "source_files": relative_sources,
            "total_items": total_items,
            "ean_count": len(eans),
            "blank_ean_count": empty_count,
            "eans": list(eans),
        }
        if empty_details:
            payload["blank_entries"] = list(empty_details)
        return json.dumps(payload, ensure_ascii=False, indent=2)
    if output_format == "csv":
        lines = ["ean"]
        lines.extend(eans)
        return "\n".join(lines)

    header = (
        f"EANs для {factory_name} [{factory_id}] (аккаунт {account})\n"
        f"Всего записей: {total_items}. EAN: {len(eans)}. Пустых EAN: {empty_count}."
    )
    lines = [header]
    lines.extend(eans)
    return "\n".join(lines)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Вывести список EAN по выбранной фабрике из readyJSON."
    )
    parser.add_argument(
        "--account",
        default="JV",
        type=normalize_account,
        help="Аккаунт Afterbuy (по умолчанию JV).",
    )
    parser.add_argument(
        "--factory-id",
        help="Идентификатор фабрики из collections.json.",
    )
    parser.add_argument(
        "--factory-name",
        help="Название фабрики (поиск без учета регистра).",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Показать доступные фабрики и завершить работу.",
    )
    parser.add_argument(
        "--search",
        help="Фильтр по части идентификатора/названия для --list.",
    )
    parser.add_argument(
        "--format",
        default=DEFAULT_FORMAT,
        choices=FORMATS,
        help="Формат вывода: text, json, csv (по умолчанию text).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Сохранить результат в файл вместо вывода в stdout.",
    )
    parser.add_argument(
        "--no-dedupe",
        dest="dedupe",
        action="store_false",
        help="Не удалять повторяющиеся EAN.",
    )
    parser.set_defaults(dedupe=True)
    parser.add_argument(
        "--include-empty",
        action="store_true",
        help="Собирать список позиций без EAN (полезно для диагностики).",
    )
    return parser.parse_args(argv)


def ensure_output_target(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    try:
        collections = load_collections(args.account)
    except (FileNotFoundError, ValueError) as exc:
        print(exc, file=sys.stderr)
        return 1

    ready_index = build_ready_index(args.account)

    if args.list:
        list_factories(args.account, collections, ready_index, args.search)
        return 0

    try:
        factory_id, factory_name = resolve_factory(collections, args.factory_id, args.factory_name)
    except FactoryLookupError as exc:
        print(exc, file=sys.stderr)
        return 1

    source_files = ready_index.get(factory_id, [])
    if not source_files:
        print(
            f"Файлы readyJSON для фабрики {factory_name} ({factory_id}) не найдены.",
            file=sys.stderr,
        )
        return 1

    try:
        eans, total_items, empty_count, empty_details = extract_eans(
            source_files,
            dedupe=args.dedupe,
            include_empty=args.include_empty,
        )
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1

    output = format_output(
        output_format=args.format,
        eans=eans,
        account=args.account,
        factory_id=factory_id,
        factory_name=factory_name,
        source_files=source_files,
        total_items=total_items,
        empty_count=empty_count,
        empty_details=empty_details,
    )

    if args.output:
        ensure_output_target(args.output)
        args.output.write_text(output, encoding="utf-8")
    else:
        print(output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
