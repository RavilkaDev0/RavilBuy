import argparse
import copy
import json
import logging
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import requests

from logging_utils import setup_logging
from getFabrik import ACCOUNT_ORDER, ensure_authenticated_session  # type: ignore


# ===== Константы и пути =====

LISTER_COLLECTION_FILES = {
    "JV": Path("Fabriks") / "JV_F_L" / "collections.json",
    "XL": Path("Fabriks") / "XL_F_L" / "collections.json",
}

LISTER_OUTPUT_DIRS = {
    "JV": Path("itemsF") / "JV_I_L",
    "XL": Path("itemsF") / "XL_I_L",
}

MAX_WORKERS = 5
LOG_DIR = Path("LOGs")

LISTER_PATH = "/afterbuy/ebayliste2.aspx"

LISTER_PARAMS = {
    "art": "SetAuswahl",
    "lAWSuchwort": "",
    "lAWFilter": "0",
    "lAWFilter2": "0",
    "I_Stammartikel": "",
    "siteIDsuche": "-1",
    "lAWartikelnummer": "",
    "lAWKollektion": "",
    "lAWKollektion1": "-1",
    "lAWKollektion2": "-1",
    "lAWKollektion3": "-1",
    "lAWKollektion4": "-1",
    "lAWKollektion5": "-1",
    "lAWean": "",
    "Vorlage": "",
    "Vorlageart": "0",
    "lAWebaykat": "",
    "lAWshopcat1": "-1",
    "lAWshopcat2": "-1",
    "lawmaxart": "500",
    "maxgesamt": "15000",
    "BlockingReason": "",
    "DispatchTimeMax": "-1",
    "listerId": "",
    "ebayLister_DynamicPriceRules": "-100",
    "lAWSellerPaymentProfile": "0",
    "lAWSellerReturnPolicyProfile": "0",
    "lAWSellerShippingProfile": "0",
}

HIDDEN_INPUT_NAME = "allmyupdtids"


DATASETS: Dict[str, Dict[str, object]] = {
    "lister": {
        "entity_files": LISTER_COLLECTION_FILES,
        "output_dirs": LISTER_OUTPUT_DIRS,
        "base_params": LISTER_PARAMS,
        "endpoint": LISTER_PATH,
        "id_param": "lAWKollektion",
        "page_size_keys": ("lawmaxart", "maxgesamt"),
        "label": "Коллекции листера",
        "console_prefix": "LST",
        "referer_path": "/afterbuy/ebayliste2.aspx?art=SetAuswahl",
        "hidden_input": HIDDEN_INPUT_NAME,
        "timeout": 60,
    }
}

# Наборы которые понимает CLI
DATASET_CHOICES = ("product", "lister", "all")

LOGGER = logging.getLogger("getItems")


# ===== Логирование =====

def configure_logging(verbose: bool) -> logging.Logger:
    console_level = logging.DEBUG if verbose else logging.INFO
    return setup_logging("getItems", console_level=console_level)


def setup_logger(account: str) -> logging.Logger:
    LOG_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger(f"getItems_{account}")

    # сбрасываем старые хендлеры чтобы не дублировать лог строки
    if logger.handlers:
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()

    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(
        LOG_DIR / f"getItems_{account}.log", encoding="utf-8"
    )
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# ===== HTML парсер для скрытого input =====

class HiddenInputParser(HTMLParser):
    def __init__(self, target_name: str) -> None:
        super().__init__()
        self.target_name = target_name.lower()
        self.value: Optional[str] = None

    def handle_starttag(
        self, tag: str, attrs: List[Tuple[str, Optional[str]]]
    ) -> None:
        if tag.lower() != "input" or self.value is not None:
            return
        attr_map = {k.lower(): (v or "") for k, v in attrs}
        if attr_map.get("name", "").lower() == self.target_name:
            self.value = attr_map.get("value", "")


# ===== Вспомогательные функции =====

def clone_cookie_jar(
    jar: requests.cookies.RequestsCookieJar,
) -> requests.cookies.RequestsCookieJar:
    cloned = requests.cookies.RequestsCookieJar()
    for cookie in jar:
        cloned.set_cookie(copy.copy(cookie))
    return cloned


def load_entities(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Factory file not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"Factory file {path} is not a list.")

    result: List[Dict[str, str]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        factory_id = str(item.get("id", "")).strip()
        name = str(item.get("name", "")).strip()
        if not factory_id or not name:
            continue
        result.append({"id": factory_id, "name": name})
    return result


def update_entity_counts_file(
    path: Path, updates: Dict[str, Tuple[str, int]]
) -> None:
    if not updates or not path.exists():
        return
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        LOGGER.warning("Не удалось обновить файл фабрик %s: %s", path, exc)
        return
    if not isinstance(data, list):
        LOGGER.warning(
            "Ожидался список в файле %s, пропускаем обновление item_count",
            path,
        )
        return

    changed = False
    for entry in data:
        if not isinstance(entry, dict):
            continue
        entity_id = str(entry.get("id", "")).strip()
        if not entity_id:
            continue
        info = updates.get(entity_id)
        if info is None:
            continue
        name, count = info
        if name:
            current_name = str(entry.get("name", "")).strip()
            if current_name != name:
                entry["name"] = name
                changed = True
        if entry.get("item_count") != count:
            entry["item_count"] = count
            changed = True

    if not changed:
        return

    try:
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError as exc:
        LOGGER.warning(
            "Не удалось сохранить обновлённый файл фабрик %s: %s", path, exc
        )


def read_item_count_from_file(path: Path) -> Optional[int]:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    value = data.get("item_count")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name.strip())
    cleaned = cleaned.strip("_")
    if not cleaned:
        cleaned = "factory"
    return cleaned[:150]


def fetch_item_ids(
    session: requests.Session,
    domain: str,
    endpoint: str,
    base_params: Dict[str, str],
    id_param: str,
    entity_id: str,
    page_size_keys: Sequence[str],
    referer_path: Optional[str] = None,
    hidden_input: str = HIDDEN_INPUT_NAME,
    offset_param: str = "rsposition",
    timeout: int = 60,
    logger: Optional[logging.Logger] = None,
) -> List[str]:
    base_url = (
        endpoint if endpoint.startswith("http") else f"https://{domain}{endpoint}"
    )

    if referer_path:
        referer = (
            referer_path
            if referer_path.startswith("http")
            else f"https://{domain}{referer_path}"
        )
    else:
        referer = base_url

    collected: List[str] = []
    seen: set[str] = set()
    offset = 0

    # определяем размер страницы
    page_size: Optional[int] = None
    for key in page_size_keys:
        value = base_params.get(key)
        if not value:
            continue
        try:
            candidate = int(value)
            if candidate > 0:
                page_size = candidate
                break
        except ValueError:
            continue
    if page_size is None:
        page_size = 500

    headers = {"Referer": referer}

    while True:
        params = base_params.copy()
        params[id_param] = entity_id
        if offset:
            params[offset_param] = str(offset)

        response = session.get(
            base_url, params=params, headers=headers, timeout=timeout
        )
        response.raise_for_status()

        parser = HiddenInputParser(hidden_input)
        parser.feed(response.text)
        value = (parser.value or "").strip()
        if not value:
            break

        page_ids = [token for token in value.split(",") if token]

        new_count = 0
        for item_id in page_ids:
            if item_id not in seen:
                seen.add(item_id)
                collected.append(item_id)
                new_count += 1

        if logger:
            logger.debug(
                "offset=%s: новых %d, всего %d",
                offset,
                new_count,
                len(collected),
            )

        # стоп если страница меньше полного размера
        if new_count == 0 or len(page_ids) < page_size:
            break

        offset += page_size

    return collected


def process_entities(
    account: str,
    entities: Sequence[Dict[str, str]],
    config: Dict[str, object],
    logger: logging.Logger,
) -> List[Dict[str, str]]:
    # сессия / домен от afterbuy
    session, domain = ensure_authenticated_session(account)

    output_map = config["output_dirs"]  # type: ignore[index]
    output_root: Path = output_map[account]  # type: ignore[index]
    output_root.mkdir(parents=True, exist_ok=True)

    count_updates: Dict[str, Tuple[str, int]] = {}
    counts_path: Optional[Path] = None

    count_map = config.get("count_files")  # type: ignore[assignment]
    if isinstance(count_map, dict):
        count_target = count_map.get(account)
        if count_target:
            counts_path = Path(count_target)

    # сохраняем заголовки и куки чтобы потом параллелить без повторного логина
    headers_template = dict(session.headers)
    cookies_template = clone_cookie_jar(session.cookies)
    session.close()

    dataset_label = config["label"]  # type: ignore[index]
    console_prefix = config.get("console_prefix", dataset_label)  # type: ignore[arg-type]
    base_params: Dict[str, str] = config["base_params"]  # type: ignore[index]
    endpoint: str = config["endpoint"]  # type: ignore[index]
    id_param: str = config["id_param"]  # type: ignore[index]
    page_size_keys: Sequence[str] = config["page_size_keys"]  # type: ignore[index]
    referer_path: Optional[str] = config.get("referer_path")  # type: ignore[arg-type]
    hidden_input: str = config.get("hidden_input", HIDDEN_INPUT_NAME)  # type: ignore[arg-type]
    offset_param: str = config.get("offset_param", "rsposition")  # type: ignore[arg-type]
    timeout: int = int(config.get("timeout", 60))  # type: ignore[arg-type]

    def handle_entity(entity: Dict[str, str]) -> Tuple[str, str, int, Path]:
        entity_id = entity["id"]
        entity_name = entity["name"]

        local_session = requests.Session()
        local_session.headers.update(headers_template)
        local_session.cookies = clone_cookie_jar(cookies_template)
        try:
            item_ids = fetch_item_ids(
                local_session,
                domain,
                endpoint,
                base_params,
                id_param,
                entity_id,
                page_size_keys,
                referer_path=referer_path,
                hidden_input=hidden_input,
                offset_param=offset_param,
                timeout=timeout,
                logger=logger,
            )
        finally:
            local_session.close()

        safe_name = sanitize_filename(entity_name)
        output_path = output_root / f"{safe_name}_{entity_id}.json"

        payload = {
            "factory_id": entity_id,
            "factory_name": entity_name,
            "item_count": len(item_ids),
            "item_ids": item_ids,
        }

        output_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        return entity_name, entity_id, len(item_ids), output_path

    failed_entities: List[Dict[str, str]] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_entity = {
            executor.submit(handle_entity, entity): entity for entity in entities
        }
        for future in as_completed(future_to_entity):
            entity = future_to_entity[future]
            try:
                entity_name, entity_id, count, output_path = future.result()

                stored_count = read_item_count_from_file(output_path) or count
                count_updates[entity_id] = (entity_name, stored_count)

                message = (
                    f"[{account}][{console_prefix}] "
                    f"{entity_name} ({entity_id}) -> "
                    f"{stored_count} items saved to {output_path}"
                )
                LOGGER.info(message)

            except Exception as exc:
                failed_entities.append(entity)
                error_msg = (
                    f"[ERROR] {account} / {entity.get('name')} "
                    f"({entity.get('id')}): {exc}"
                )
                LOGGER.error(error_msg)

    if counts_path and count_updates:
        update_entity_counts_file(counts_path, count_updates)

    if failed_entities:
        LOGGER.warning(
            "Фабрики с ошибками (%d шт.): %s",
            len(failed_entities),
            ", ".join(
                f"{f['name']} ({f['id']})" for f in failed_entities
            ),
        )
    else:
        LOGGER.info("Все фабрики обработаны успешно")

    return failed_entities


# ===== main() =====

def main() -> None:
    # гарантируем UTF-8 вывода
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        try:
            sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        except Exception:
            pass

    parser = argparse.ArgumentParser(
        description="Скачать перечисления товаров из каталогов и листера Afterbuy."
    )
    parser.add_argument(
        "--account",
        choices=ACCOUNT_ORDER,
        type=str.upper,
        help="Обрабатывать только указанный аккаунт (по умолчанию — оба).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Ограничить число фабрик/коллекций первыми N (для отладки).",
    )
    parser.add_argument(
        "--dataset",
        choices=DATASET_CHOICES,
        default="all",
        help="Выбрать набор данных: product, lister или all (по умолчанию).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Включить подробное логирование (DEBUG).",
    )
    args = parser.parse_args()

    # настраиваем логгер консоли
    _ = configure_logging(args.verbose)

    # список аккаунтов
    accounts = ACCOUNT_ORDER if args.account is None else [args.account]

    # нормализация набора датасетов
    if args.dataset == "all":
        # сейчас доступен только lister
        dataset_keys: List[str] = ["lister"]
    else:
        dataset_keys = [args.dataset]

    LOGGER.info(
        "Старт getItems. Аккаунты: %s. Наборы: %s",
        ", ".join(accounts),
        ", ".join(dataset_keys),
    )

    # обрабатываем каждый датасет
    for dataset_key in dataset_keys:
        if dataset_key not in DATASETS:
            LOGGER.error("Неизвестный набор '%s'. Доступно: %s",
                         dataset_key, ", ".join(DATASETS.keys()))
            continue

        config = DATASETS[dataset_key]
        dataset_label = config["label"]  # type: ignore[index]
        entity_files_map = config["entity_files"]  # type: ignore[index]

        tasks: List[Tuple[str, Sequence[Dict[str, str]]]] = []

        # собираем задачи по аккаунтам
        for account in accounts:
            entity_path = entity_files_map.get(account)  # type: ignore[attr-defined]
            if entity_path is None:
                message = (
                    f"Нет файла с данными ({dataset_label}) "
                    f"для аккаунта {account}"
                )
                LOGGER.warning(message)
                continue

            entity_path = Path(entity_path)
            if not entity_path.exists():
                message = (
                    f"Файл {entity_path} не найден для {account} "
                    f"({dataset_label})"
                )
                LOGGER.warning(message)
                continue

            entities = load_entities(entity_path)

            if args.limit is not None:
                entities = entities[: args.limit]

            if not entities:
                message = (
                    f"Нет записей в {entity_path} для {account} "
                    f"({dataset_label})"
                )
                LOGGER.warning(message)
                continue

            tasks.append((account, entities))

        if not tasks:
            continue

        tasks_dict = {account: entities for account, entities in tasks}
        LOGGER.info("=== %s ===", dataset_label)

        max_workers = min(len(tasks), MAX_WORKERS) or 1
        retry_plan: Dict[str, List[Dict[str, str]]] = {}

        # первый проход: параллельно по аккаунтам
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_account = {
                executor.submit(
                    process_entities,
                    account,
                    entities,
                    config,
                    LOGGER,
                ): account
                for account, entities in tasks
            }

            for future in as_completed(future_to_account):
                account = future_to_account[future]
                try:
                    failures = future.result()
                    if failures:
                        retry_plan.setdefault(account, []).extend(failures)
                except Exception as exc:
                    LOGGER.error(
                        "[%s] Ошибка выполнения задач: %s", account, exc
                    )
                    retry_plan.setdefault(account, []).extend(
                        list(tasks_dict.get(account, []))
                    )

        # повторная попытка для упавших фабрик
        if not retry_plan:
            continue

        LOGGER.info("Повторная попытка для неудачных фабрик...")
        for account, pending in retry_plan.items():
            unique: Dict[str, Dict[str, str]] = {}
            for entity in pending:
                unique[entity["id"]] = entity
            remaining = list(unique.values())
            if not remaining:
                continue

            failures = process_entities(account, remaining, config, LOGGER)
            if failures:
                names = ", ".join(
                    f"{f['name']} ({f['id']})" for f in failures
                )
                LOGGER.warning(
                    "После повторной попытки для %s остались ошибки: %s",
                    account,
                    names,
                )
            else:
                LOGGER.info(
                    "Все фабрики успешно обработаны после повторной попытки для %s",
                    account,
                )

    LOGGER.info("Завершение getItems.")


if __name__ == "__main__":
    main()
