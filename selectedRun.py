from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

from logging_utils import setup_logging
from getFabrik import ensure_authenticated_session  # type: ignore
import getItems  # type: ignore


def load_selection(path: Path) -> Dict[str, List[str]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    out: Dict[str, List[str]] = {}
    for k, v in data.items():
        out[k.upper()] = [str(x) for x in v if str(x)]
    return out


def load_collections(account: str) -> Dict[str, str]:
    path = Path("Fabriks") / f"{account}_F_L" / "collections.json"
    names: Dict[str, str] = {}
    if path.exists():
        try:
            for item in json.loads(path.read_text(encoding="utf-8")):
                fid = str(item.get("id", ""))
                name = str(item.get("name", ""))
                if fid:
                    names[fid] = name
        except Exception:
            pass
    return names


def save_items_file(account: str, factory_id: str, factory_name: str, item_ids: Sequence[str]) -> Path:
    safe_name = getItems.sanitize_filename(factory_name)
    out_dir = Path("itemsF") / f"{account}_I_L"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{safe_name}_{factory_id}.json"
    payload = {
        "factory_id": factory_id,
        "factory_name": factory_name,
        "item_count": len(item_ids),
        "item_ids": list(item_ids),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def build_export_args(selection: Dict[str, List[str]], verbose: bool) -> List[str]:
    args: List[str] = ["--skip-existing"]
    for acc, ids in selection.items():
        if not ids:
            continue
        args += ["--account", acc]
        for fid in ids:
            args += ["--factory-id", fid]
    if verbose:
        args.append("--verbose")
    return args


def run_export(selection: Dict[str, List[str]], verbose: bool, logger: logging.Logger) -> None:
    import subprocess, sys

    export = [sys.executable, str(Path("exportLister.py").resolve())]
    export += build_export_args(selection, verbose)
    logger.info("Запуск exportLister с фильтрами: %s", " ".join(export[1:]))
    subprocess.run(export, check=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Запустить парсинг только выбранных фабрик")
    parser.add_argument("--selection-file", required=True, type=Path)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logger = setup_logging("selectedRun", console_level=(logging.DEBUG if args.verbose else logging.INFO))

    selection = load_selection(args.selection_file)
    if not any(selection.values()):
        logger.warning("Нет выбранных фабрик. Завершение.")
        return

    for account, ids in selection.items():
        if not ids:
            continue
        logger.info("[%s] Подготовка фабрик: %s", account, ", ".join(ids))
        names = load_collections(account)
        session, domain = ensure_authenticated_session(account)
        try:
            for fid in ids:
                name = names.get(fid, f"factory_{fid}")
                items = getItems.fetch_item_ids(
                    session,
                    domain,
                    getItems.LISTER_PATH,
                    dict(getItems.LISTER_PARAMS),
                    getItems.DATASETS["lister"]["id_param"],  # type: ignore[index]
                    fid,
                    getItems.DATASETS["lister"]["page_size_keys"],  # type: ignore[index]
                    referer_path=getItems.DATASETS["lister"]["referer_path"],  # type: ignore[index]
                    hidden_input=getItems.HIDDEN_INPUT_NAME,
                    timeout=60,
                    logger=logger,
                )
                out = save_items_file(account, fid, name, items)
                logger.info("[%s] %s -> %d товаров (%s)", account, name, len(items), out)
        finally:
            session.close()

    run_export(selection, args.verbose, logger)


if __name__ == "__main__":
    main()

