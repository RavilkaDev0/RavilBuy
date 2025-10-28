from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from logging_utils import setup_logging
from getFabrik import ensure_authenticated_session  # type: ignore
import getItems  # type: ignore
import makeJson  # type: ignore

TASKS_FILE = Path("Site") / "data" / "selection_tasks.json"
DEFAULT_TASK_FLAGS = {
    "getitems": True,
    "exportlister": True,
    "makejson": True,
    "exporthtml": True,
}

def load_legacy_tasks(selection: Dict[str, List[str]]) -> Dict[str, bool]:
    selection_has_itemsF = False
    selection_has_ready_json = False
    selection_has_ready_html = False
    for acc, ids in selection.items():
        if not ids:
            continue
        items_dir = Path("itemsF") / f"{acc}_I_L"
        json_dir = Path("readyJSON") / acc
        html_dir = Path("readyhtml") / acc
        for fid in ids:
            if list(items_dir.glob(f"*{fid}*.json")):
                selection_has_itemsF = True
            if list(json_dir.glob(f"*{fid}*.json")):
                selection_has_ready_json = True
            if list(html_dir.glob(f"*{fid}*.html")):
                selection_has_ready_html = True
    flags = DEFAULT_TASK_FLAGS.copy()
    if selection_has_itemsF and not selection_has_ready_json and not selection_has_ready_html:
        flags.update({"getitems": False, "exportlister": True, "makejson": True, "exporthtml": True})
    return flags


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
    args: List[str] = ["--refresh-existing"]
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
    export = [sys.executable, str(Path("exportLister.py").resolve())]
    export += build_export_args(selection, verbose)
    logger.info("Запуск exportLister с фильтрами: %s", " ".join(export[1:]))
    subprocess.run(export, check=False)


def load_task_flags(path: Path) -> Dict[str, bool]:
    flags = DEFAULT_TASK_FLAGS.copy()
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                for key in flags:
                    if key in data:
                        flags[key] = bool(data[key])
        except Exception:
            pass
    else:
        # auto-create default config
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(flags, ensure_ascii=False, indent=2), encoding="utf-8")
    return flags


def run_make_json(selection: Dict[str, List[str]], logger: logging.Logger) -> None:
    for account, ids in selection.items():
        if not ids:
            continue
        csv_dir = Path("CSVDATA") / f"{account.upper()}_L"
        output_dir = Path("readyJSON") / account.upper()
        output_dir.mkdir(parents=True, exist_ok=True)
        ean_column = "EAN"
        for dataset in makeJson.DATASETS:
            if dataset.get("name", "").upper() == account.upper():
                ean_column = dataset.get("ean_column", "EAN")
                break

        for factory_id in ids:
            matches = sorted(csv_dir.glob(f"*{factory_id}*.csv"))
            if not matches:
                logger.warning("[%s] CSV для фабрики %s не найден в %s", account, factory_id, csv_dir)
                continue
            for csv_path in matches:
                try:
                    created, failures = makeJson.process_csv(csv_path, output_dir, ean_column)
                except Exception as exc:  # noqa: BLE001
                    logger.error("[%s] Ошибка обработки %s: %s", account, csv_path.name, exc)
                    continue
                if failures:
                    for failure in failures:
                        logger.warning("[%s] %s", account, failure)
                logger.info(
                    "[%s] CSV %s -> создано JSON: %d",
                    account,
                    csv_path.name,
                    created,
                )


def collect_html_targets(account: str, factory_ids: Sequence[str], logger: logging.Logger) -> Dict[str, str]:
    ready_dir = Path("readyJSON") / account
    targets: Dict[str, str] = {}
    for factory_id in factory_ids:
        json_files = list(ready_dir.glob(f"*{factory_id}.json"))
        if not json_files:
            logger.warning("[%s] Не найден JSON-файл для фабрики %s в %s", account, factory_id, ready_dir)
        for path in json_files:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:  # noqa: BLE001
                logger.error("[%s] Ошибка чтения %s: %s", account, path, exc)
                continue
            if not isinstance(data, list):
                continue
            for entry in data:
                if not isinstance(entry, dict):
                    continue
                ean = entry.get("ean") or entry.get("EAN")
                item_id = entry.get("ID") or entry.get("id") or entry.get("ItemID")
                if not ean or not item_id:
                    continue
                ean_str = str(ean).strip()
                item_str = str(item_id).strip()
                if not ean_str or not item_str:
                    continue
                targets[ean_str] = item_str
    return targets


def export_selected_html(selection: Dict[str, List[str]], logger: logging.Logger) -> None:
    try:
        import exportHTML  # type: ignore
    except ImportError as exc:  # noqa: BLE001
        logger.error("Не удалось импортировать exportHTML.py: %s", exc)
        return

    for account, factory_ids in selection.items():
        if not factory_ids:
            continue
        targets = collect_html_targets(account, factory_ids, logger)
        if not targets:
            logger.warning("[%s] Нет товаров для выгрузки HTML.", account)
            continue

        cfg = exportHTML.cfg_by_acc(account)
        out_dir = Path(cfg["out_dir"])
        out_dir.mkdir(parents=True, exist_ok=True)

        saved = skipped = errors = 0

        def apply_result(status: str, details: Optional[str], ean: str) -> None:
            nonlocal saved, skipped, errors
            if status == "saved":
                saved += 1
            elif status == "skip":
                skipped += 1
                if details:
                    logger.warning("[%s] %s", account, details)
            elif status == "error":
                errors += 1
                if details:
                    logger.error("[%s] %s", account, details)

        logger.info("[%s] Выгрузка HTML для %d товаров", account, len(targets))
        exportHTML._reset_session()
        exportHTML.get_cookies(account, cfg["cookies_file"], force_login=True)

        future_to_ean: Dict[object, str] = {}

        with ThreadPoolExecutor(max_workers=exportHTML.MAX_WORKERS) as executor:
            active: set = set()
            for idx, (ean, item_id) in enumerate(targets.items(), start=1):
                if idx > 1 and (idx - 1) % exportHTML.RELOGIN_FILE_CHUNK == 0:
                    logger.info("[%s] Повторная авторизация после %d товаров", account, idx - 1)
                    exportHTML._reset_session()
                    exportHTML.get_cookies(account, cfg["cookies_file"], force_login=True)

                future = executor.submit(
                    exportHTML.process_product,
                    item_id,
                    ean,
                    cfg["farm"],
                    cfg["out_dir"],
                    account,
                    cfg["cookies_file"],
                )
                future_to_ean[future] = ean
                active.add(future)

                if len(active) >= exportHTML.SUBMIT_CHUNK:
                    done, active = wait(active, return_when=FIRST_COMPLETED)
                    for fut in done:
                        ean_value = future_to_ean.pop(fut, "unknown")
                        try:
                            status, details = fut.result()
                        except Exception as exc:  # noqa: BLE001
                            errors += 1
                            logger.error("[%s] Ошибка потока для %s: %s", account, ean_value, exc)
                            continue
                        apply_result(status, details, ean_value)

            for fut in as_completed(active):
                ean_value = future_to_ean.pop(fut, "unknown")
                try:
                    status, details = fut.result()
                except Exception as exc:  # noqa: BLE001
                    errors += 1
                    logger.error("[%s] Ошибка потока для %s: %s", account, ean_value, exc)
                    continue
                apply_result(status, details, ean_value)

        min_size = max(64, exportHTML.MIN_HTML_LENGTH // 2)
        pending: List[Tuple[str, str]] = []
        for ean, item_id in targets.items():
            html_path = out_dir / f"{ean}.html"
            if not html_path.exists() or html_path.stat().st_size < min_size:
                pending.append((item_id, ean))

        if pending:
            logger.warning("[%s] Повторная попытка выгрузки %d HTML", account, len(pending))
            exportHTML._reset_session()
            exportHTML.get_cookies(account, cfg["cookies_file"], force_login=True)
            for item_id, ean in pending:
                status, details = exportHTML.process_product(
                    item_id,
                    ean,
                    cfg["farm"],
                    cfg["out_dir"],
                    account,
                    cfg["cookies_file"],
                )
                apply_result(status, details, ean)

        logger.info("[%s] HTML: сохранено %d, пропущено %d, ошибок %d", account, saved, skipped, errors)


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

    task_flags = load_task_flags(TASKS_FILE)
    enabled_steps = [name for name, enabled in task_flags.items() if enabled]
    logger.info("Включенные этапы: %s", ", ".join(enabled_steps) if enabled_steps else "нет")

    if task_flags["getitems"]:
        for account, ids in selection.items():
            if not ids:
                continue
            logger.info("[%s] getItems: %s", account, ", ".join(ids))
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
    else:
        logger.info("Этап getItems пропущен по настройке.")

    if task_flags["exportlister"]:
        run_export(selection, args.verbose, logger)
    else:
        logger.info("Этап exportLister пропущен по настройке.")

    if task_flags["makejson"]:
        run_make_json(selection, logger)
    else:
        logger.info("Этап makeJson пропущен по настройке.")

    if task_flags["exporthtml"]:
        export_selected_html(selection, logger)
    else:
        logger.info("Этап exportHTML пропущен по настройке.")


if __name__ == "__main__":
    main()

