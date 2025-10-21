from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional


@dataclass(frozen=True)
class Target:
    id: str
    path: Path
    label: str


TARGETS: List[Target] = [
    Target("csv_jv_l", Path("CSVDATA") / "JV_L", "CSVDATA/JV_L"),
    Target("csv_jv_p", Path("CSVDATA") / "JV_P", "CSVDATA/JV_P"),
    Target("csv_xl_l", Path("CSVDATA") / "XL_L", "CSVDATA/XL_L"),
    Target("csv_xl_p", Path("CSVDATA") / "XL_P", "CSVDATA/XL_P"),
    Target("db", Path("DB"), "DB"),
    Target("fabriks_jv_l", Path("Fabriks") / "JV_F_L", "Fabriks/JV_F_L"),
    Target("fabriks_jv_p", Path("Fabriks") / "JV_F_P", "Fabriks/JV_F_P"),
    Target("fabriks_xl_l", Path("Fabriks") / "XL_F_L", "Fabriks/XL_F_L"),
    Target("fabriks_xl_p", Path("Fabriks") / "XL_F_P", "Fabriks/XL_F_P"),
    Target("logs", Path("LOGs"), "LOGs"),
    Target("items_jv_l", Path("itemsF") / "JV_I_L", "itemsF/JV_I_L"),
    Target("items_jv_p", Path("itemsF") / "JV_I_P", "itemsF/JV_I_P"),
    Target("items_xl_l", Path("itemsF") / "XL_I_L", "itemsF/XL_I_L"),
    Target("items_xl_p", Path("itemsF") / "XL_I_P", "itemsF/XL_I_P"),
    Target("sessions", Path("sessions"), "sessions"),
    Target("readyhtml_jv", Path("readyhtml") / "JV", "readyhtml/JV"),
    Target("readyhtml_xl", Path("readyhtml") / "XL", "readyhtml/XL"),
    Target("readyjson_jv", Path("readyJSON") / "JV", "readyJSON/JV"),
    Target("readyjson_xl", Path("readyJSON") / "XL", "readyJSON/XL"),
]

TARGET_MAP: Dict[str, Target] = {target.id: target for target in TARGETS}


def compute_file_count(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_dir():
        return sum(1 for entry in path.rglob("*") if entry.is_file())
    return 1


def get_target_info(target: Target) -> Dict[str, object]:
    exists = target.path.exists()
    file_count = compute_file_count(target.path)
    return {
        "id": target.id,
        "label": target.label,
        "path": str(target.path),
        "exists": exists,
        "file_count": file_count,
    }


def list_targets(selected_ids: Optional[Iterable[str]] = None) -> List[Dict[str, object]]:
    if selected_ids is None:
        targets = TARGETS
    else:
        targets = [TARGET_MAP[target_id] for target_id in selected_ids if target_id in TARGET_MAP]
    return [get_target_info(target) for target in targets]


def resolve_targets(selected_ids: Optional[Iterable[str]]) -> List[Target]:
    if not selected_ids:
        return TARGETS
    resolved: List[Target] = []
    for target_id in selected_ids:
        if target_id not in TARGET_MAP:
            raise ValueError(f"Неизвестный идентификатор пути: {target_id}")
        resolved.append(TARGET_MAP[target_id])
    return resolved


def clean_target(target: Target, *, dry_run: bool) -> Dict[str, object]:
    info_before = get_target_info(target)
    if dry_run:
        return {
            **info_before,
            "status": "dry-run",
            "files_before": info_before["file_count"],
            "files_after": info_before["file_count"],
        }

    path = target.path
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    path.mkdir(parents=True, exist_ok=True)

    info_after = get_target_info(target)
    return {
        **info_after,
        "status": "ok",
        "files_before": info_before["file_count"],
        "files_after": info_after["file_count"],
    }


def clean_targets(selected_ids: Optional[Iterable[str]], *, dry_run: bool) -> List[Dict[str, object]]:
    resolved_targets = resolve_targets(selected_ids)
    results: List[Dict[str, object]] = []
    for target in resolved_targets:
        result = clean_target(target, dry_run=dry_run)
        results.append(result)
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Быстро очистить рабочие каталоги проекта."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Показать, что будет удалено, без выполнения.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Не запрашивать подтверждение.",
    )
    parser.add_argument(
        "--target",
        action="append",
        dest="targets",
        choices=[target.id for target in TARGETS],
        help="Ограничить очистку перечисленными идентификаторами путей (можно указывать несколько раз).",
    )
    parser.add_argument(
        "--list-targets",
        action="store_true",
        help="Показать список доступных путей и выйти.",
    )
    return parser.parse_args()


def confirm(prompt: str) -> bool:
    try:
        answer = input(prompt).strip().lower()
    except EOFError:
        return False
    return answer in {"y", "yes", "д", "да"}


def main() -> None:
    args = parse_args()
    selected_ids = args.targets or [target.id for target in TARGETS]

    if args.list_targets:
        for info in list_targets(selected_ids):
            exists = "да" if info["exists"] else "нет"
            print(f"{info['id']}: {info['label']} — файлов: {info['file_count']} (существует: {exists})")
        return

    infos = list_targets(selected_ids)
    if not args.force and not args.dry_run:
        print("БУДЕТ ОЧИЩЕНО содержимое следующих путей:")
        for info in infos:
            exists = "нет" if not info["exists"] else f"{info['file_count']} файлов"
            print(f"  - {info['label']} ({exists})")
        if not confirm("Продолжить? [y/N]: "):
            print("Отменено.")
            return

    results = clean_targets(selected_ids, dry_run=args.dry_run)
    for result in results:
        label = result["label"]
        if args.dry_run:
            print(f"[DRY] {label} — файлов: {result['file_count']}")
        else:
            before = result.get("files_before", 0)
            after = result.get("files_after", result.get("file_count", 0))
            print(f"[OK] {label} — было {before}, стало {after}")

    if args.dry_run:
        print("Режим dry-run: данные не удалялись.")
    else:
        print("Очистка завершена.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)


