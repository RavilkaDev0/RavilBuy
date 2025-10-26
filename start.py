from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence

from logging_utils import setup_logging


@dataclass
class Step:
    name: str
    script: str
    args: Sequence[str] = ()


def find_script(root: Path, candidate: str) -> Path:
    # На Windows регистр не важен, но для переносимости пробуем точные имена
    path = root / candidate
    if path.exists():
        return path
    # Подстрахуемся: иногда пишут Logi.py вместо Login.py
    aliases = {
        "Logi.py": "Login.py",
    }
    alt = aliases.get(candidate)
    if alt:
        p = root / alt
        if p.exists():
            return p
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = setup_logging("start", console_level=(10 if args.verbose else 20))

    project_root = Path(__file__).resolve().parent

    steps: List[Step] = [
        Step("Авторизация", "Login.py"),
        Step("Загрузка фабрик/коллекций", "getFabrik.py"),
        Step("Очистка фабрик", "killFabriks.py"),
        Step("Получение товаров", "getItems.py"),
        Step("Экспорт Lister CSV", "exportLister.py"),
        Step("Сборка JSON", "makeJson.py"),
        Step("Экспорт HTML", "exportHTML.py"),
    ]

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

