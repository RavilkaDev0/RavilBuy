from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence

from logging_utils import setup_logging


@dataclass
class Step:
    name: str
    script: str
    args: Sequence[str] = ()


def find_script(root: Path, candidate: str) -> Path:
    path = root / candidate
    if path.exists():
        return path
    # Доп. соответствия (на случай опечаток)
    aliases = {
        "Logi.py": "Login.py",
    }
    alt = aliases.get(candidate)
    if alt:
        alt_path = root / alt
        if alt_path.exists():
            return alt_path
    return path


def run_step(python_exe: str, step: Step, cwd: Path, pass_verbose: bool, logger) -> int:
    script_path = find_script(cwd, step.script)
    if not script_path.exists():
        logger.error("Шаг '%s': файл не найден: %s", step.name, script_path)
        return 2

    cmd: List[str] = [python_exe, str(script_path)]
    if pass_verbose:
        cmd.append("--verbose")
    cmd.extend(step.args)

    logger.info("▶ %s", step.name)
    logger.debug("Команда: %s", " ".join(cmd))

    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")

    try:
        proc = subprocess.run(cmd, cwd=str(cwd), env=env)
        rc = proc.returncode
    except Exception as exc:  # noqa: BLE001
        logger.error("Шаг '%s' завершился исключением: %s", step.name, exc)
        return 1

    if rc == 0:
        logger.info("✔ %s", step.name)
    else:
        logger.error("✖ %s (код %d)", step.name, rc)
    return rc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Последовательно выполнить все скрипты пайплайна")
    parser.add_argument("--python", default=sys.executable, help="Путь к интерпретатору Python")
    parser.add_argument("--keep-going", action="store_true", help="Не останавливать выполнение при ошибке шага")
    parser.add_argument("--verbose", action="store_true", help="Подробный вывод и проксирование --verbose в дочерние скрипты")
    parser.add_argument(
        "--steps",
        help="Порядок шагов: login,getfabrik,kill,getitems,exportlister,makejson,exporthtml,makedb,selectedrun",
    )
    parser.add_argument(
        "--selection-file",
        type=Path,
        help="JSON с выбранными фабриками для шага selectedrun",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = setup_logging("start", console_level=(10 if args.verbose else 20))

    project_root = Path(__file__).resolve().parent

    registry: Dict[str, Step] = {
        "login": Step("Авторизация", "Login.py"),
        "getfabrik": Step("Загрузка фабрик/коллекций", "getFabrik.py"),
        "kill": Step("Очистка фабрик", "killFabriks.py"),
        "getitems": Step("Получение товаров", "getItems.py"),
        "exportlister": Step("Экспорт Lister CSV", "exportLister.py"),
        "makejson": Step("Сборка JSON", "makeJson.py"),
        "exporthtml": Step("Экспорт HTML", "exportHTML.py"),
        "makedb": Step("Обновление SQLite БД", "makeDB.py", args=("--overwrite",)),
    }

    if args.selection_file:
        selection_path = args.selection_file.expanduser().resolve()
        registry["selectedrun"] = Step(
            "Выборочный пайплайн", "selectedRun.py", args=("--selection-file", str(selection_path))
        )

    default_order = [
        "login",
        "getfabrik",
        "kill",
        "getitems",
        "exportlister",
        "makejson",
        "exporthtml",
        "makedb",
    ]

    order = default_order
    if args.steps:
        keys = [k.strip().lower() for k in args.steps.split(",") if k.strip()]
        order = [k for k in keys if k in registry] or default_order

    steps: List[Step] = [registry[k] for k in order]

    overall_rc = 0
    for step in steps:
        rc = run_step(args.python, step, project_root, args.verbose, logger)
        if rc != 0:
            overall_rc = rc
            if not args.keep_going:
                logger.error("Остановка пайплайна на шаге: %s", step.name)
                sys.exit(overall_rc)

    if overall_rc == 0:
        logger.info("Пайплайн завершён успешно")
    else:
        logger.warning("Пайплайн завершён с ошибками (код %d)", overall_rc)
    sys.exit(overall_rc)


if __name__ == "__main__":
    main()

