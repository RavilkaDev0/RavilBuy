import argparse
import json
import logging
from pathlib import Path

from logging_utils import setup_logging

ALL_FABRIKS = {
    "JV_F_L": Path("Fabriks") / "JV_F_L" / "collections.json",
    "XL_F_L": Path("Fabriks") / "XL_F_L" / "collections.json",
}


KILL_FABRIKS = {
    "JV_F_L": Path("Ignore") / "JV_L.json",
    "XL_F_L": Path("Ignore") / "XL_L.json",
}

LOGGER = logging.getLogger("killFabriks")


def kill_fabriks() -> None:
    for fabrik_key, fabrik_path in ALL_FABRIKS.items():
        kill_path = KILL_FABRIKS.get(fabrik_key)

        if not fabrik_path.exists():
            LOGGER.warning("[%s] Файл коллекций не найден: %s", fabrik_key, fabrik_path)
            continue
        if not kill_path or not kill_path.exists():
            LOGGER.info("[%s] Нет списка на удаление (пропуск): %s", fabrik_key, kill_path)
            continue

        with open(fabrik_path, "r", encoding="utf-8") as f:
            fabrik_data = json.load(f)

        with open(kill_path, "r", encoding="utf-8") as f:
            kill_data = json.load(f)

        kill_ids = {str(item.get("id", "")).strip() for item in kill_data}
        before = list(fabrik_data)
        removed = [item for item in before if str(item.get("id", "")).strip() in kill_ids]
        filtered_data = [item for item in before if str(item.get("id", "")).strip() not in kill_ids]

        with open(fabrik_path, "w", encoding="utf-8") as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=2)

        # Логируем какие фабрики удалены
        if removed:
            sample = ", ".join(f"{r.get('name','')} ({r.get('id','')})" for r in removed[:20])
            more = "" if len(removed) <= 20 else f", и ещё {len(removed) - 20}"
            LOGGER.info("[%s] Удалено фабрик: %d: %s%s", fabrik_key, len(removed), sample, more)
        else:
            LOGGER.info("[%s] Нечего удалять — совпадений не найдено", fabrik_key)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Удалить фабрики из коллекций по списку Ignore")
    parser.add_argument("--verbose", action="store_true", help="Подробный вывод")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _ = setup_logging("killFabriks", console_level=(logging.DEBUG if args.verbose else logging.INFO))
    kill_fabriks()
if __name__ == "__main__":
    main()
