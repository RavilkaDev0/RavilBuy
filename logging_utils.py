from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

from loguru import logger as _logger


LOG_DIR = Path("LOGs")
FILE_TIME_FORMAT = "YYYY-MM-DD HH:mm:ss"
CONSOLE_TIME_FORMAT = "HH:mm:ss"


class InterceptHandler(logging.Handler):
    """Forward standard logging records to Loguru."""

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - glue code
        try:
            level = _logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        _logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def _level_name(level: int) -> str:
    try:
        return logging.getLevelName(level)
    except Exception:
        return str(level)


def setup_logging(
    name: str,
    *,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
):
    """Configure Loguru sinks and bridge std logging to Loguru.

    Returns a Loguru logger for convenience; standard logging loggers
    will also be intercepted and written to the same sinks.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Intercept std logging to loguru
    root = logging.getLogger()
    root.handlers = [InterceptHandler()]
    root.setLevel(logging.NOTSET)

    # Reset loguru to avoid duplicate sinks on reconfiguration
    try:
        _logger.remove()
    except Exception:
        pass

    def _ensure_scope(record):  # type: ignore[override]
        extra = record["extra"]
        extra.setdefault("app", name)
        scope = extra.get("app", name)
        account = extra.get("account")
        if account:
            scope = f"{scope}/{account}"
        extra["scope"] = scope
        return True

    # Console sink (compact, colorized)
    _logger.add(
        sys.stderr,
        level=console_level,
        format=f"{{time:{CONSOLE_TIME_FORMAT}}} | <level>{{level:<8}}</level> | {{extra[scope]:<18}} | {{message}}",
        colorize=True,
        enqueue=False,
        diagnose=False,
        filter=_ensure_scope,
    )

    # File sink per logical logger name
    _logger.add(
        LOG_DIR / f"{name}.log",
        level=file_level,
        format=f"{{time:{FILE_TIME_FORMAT}}} | {{level:<8}} | {{extra[scope]:<18}} | {{message}}",
        encoding="utf-8",
        enqueue=True,
        backtrace=False,
        diagnose=False,
        rotation="00:00",
        retention="14 days",
        compression="zip",
        filter=_ensure_scope,
    )

    # Сообщение о конфигурации (через std logging, чтобы работали %s плейсхолдеры в коде)
    logging.getLogger(name).debug(
        "Logger '%s' configured (console_level=%s, file_level=%s)",
        name,
        _level_name(console_level),
        _level_name(file_level),
    )

    # Возвращаем совместимый std-логгер; записи перехватываются Loguru
    return logging.getLogger(name)


def get_logger(app: str, **extra):
    """Вернуть Loguru-логгер с привязанными полями (app, account, ...)."""
    return _logger.bind(app=app, **extra)


def add_account_file_sink(app: str, account: str, *, level: int = logging.INFO) -> int:
    """Добавить отдельный файл-лог под аккаунт, вернуть id sink'а."""

    path = LOG_DIR / f"{app}_{account}.log"

    def _ensure_scope(record):
        extra = record["extra"]
        extra.setdefault("app", app)
        extra.setdefault("account", account)
        scope = f"{extra['app']}/{extra['account']}"
        extra["scope"] = scope
        return True

    return _logger.add(
        path,
        level=level,
        format=f"{{time:{FILE_TIME_FORMAT}}} | {{level:<8}} | {{extra[scope]:<18}} | {{message}}",
        encoding="utf-8",
        enqueue=True,
        backtrace=False,
        diagnose=False,
        rotation="00:00",
        retention="30 days",
        compression="zip",
        filter=_ensure_scope,
    )

