from __future__ import annotations

import argparse
import logging
import sys
from typing import Callable, Dict, List, Optional, Sequence

from Login import main as login_main  # type: ignore
from exportLister import main as export_lister_main  # type: ignore
from exportProdukt import main as export_product_main  # type: ignore
from getFabrik import main as fabrik_main  # type: ignore
from getItems import main as items_main  # type: ignore


StepFunc = Callable[[], None]

STEPS: Dict[str, StepFunc] = {
    "login": login_main,
    "factories": fabrik_main,
    "items": items_main,
    "product-export": export_product_main,
    "lister-export": export_lister_main,
}


def run_step(name: str, func: StepFunc) -> None:
    logging.info("=== Запуск этапа: %s ===", name)
    original_argv = sys.argv
    try:
        sys.argv = [f"{name}.py"]
        try:
            func()
        except SystemExit as exc:
            exit_code = exc.code if isinstance(exc.code, int) else 0
            if exit_code not in (0, None):
                raise
    finally:
        sys.argv = original_argv


def plan_steps(selected: Optional[Sequence[str]], skipped: Sequence[str]) -> List[str]:
    ordered_names = list(STEPS.keys())
    if selected:
        plan = [step for step in ordered_names if step in selected]
    else:
        plan = ordered_names
    skip_set = set(skipped)
    return [step for step in plan if step not in skip_set]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Команда для запуска цепочки экспорта Afterbuy."
    )
    parser.add_argument(
        "--steps",
        nargs="+",
        choices=STEPS.keys(),
        help="Выполнить только перечисленные этапы (по умолчанию выполняются все).",
    )
    parser.add_argument(
        "--skip",
        nargs="+",
        choices=STEPS.keys(),
        default=[],
        help="Пропустить перечисленные этапы.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Уровень логирования (по умолчанию INFO).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    plan = plan_steps(args.steps, args.skip)
    if not plan:
        logging.info("Нет этапов для выполнения. Завершено.")
        return

    for step_name in plan:
        func = STEPS[step_name]
        try:
            run_step(step_name, func)
        except SystemExit as exc:
            exit_code = exc.code if isinstance(exc.code, int) else 0
            logging.error("Этап '%s' завершился с кодом %s.", step_name, exit_code)
            sys.exit(exit_code)
        except Exception as exc:
            logging.exception("Этап '%s' завершился с ошибкой: %s", step_name, exc)
            sys.exit(1)


if __name__ == "__main__":
    main()
