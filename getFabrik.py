import argparse
import html
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import requests

from logging_utils import setup_logging

from Login import (  # type: ignore
    ACCOUNT_DOMAINS,
    AfterbuyClient,
    SESSION_DIR,
    USER_AGENT,
    extract_accounts,
    read_env_file,
    save_cookies,
)


CATALOG_ENDPOINT = "/afterbuy/Interfaces/Catalogs.aspx"
CATALOG_PARAMS = {
    "selectedName": "Katalog_Filter",
    "selectBoxId": "katList",
    "cssClasses": "ab-form-control input-sm form-control",
}
CATALOG_REFERER = (
    "https://{domain}/afterbuy/shop/produkte.aspx?newsearch=1&DT=1"
)

LISTER_PAGE_PATH = "/afterbuy/ebayliste2.aspx"
LISTER_PARAMS = {"newsearch": "1", "DT": "1"}
LISTER_SELECT_NAME = "lAWKollektion"

ACCOUNT_ORDER = ["JV", "XL"]
CATALOG_OUTPUTS = {
    "JV": Path("Fabriks") / "JV_F_P" / "factories.json",
    "XL": Path("Fabriks") / "XL_F_P" / "factories.json",
}
LISTER_OUTPUTS = {
    "JV": Path("Fabriks") / "JV_F_L" / "collections.json",
    "XL": Path("Fabriks") / "XL_F_L" / "collections.json",
}
TARGET_CHOICES = ("catalog", "lister", "all")
LOG_DIR = Path("LOGs")

LOGGER = logging.getLogger("getFabrik")


def configure_logging(verbose: bool) -> logging.Logger:
    console_level = logging.DEBUG if verbose else logging.INFO
    return setup_logging("getFabrik", console_level=console_level)


class OptionCollector(HTMLParser):
    """Collect <option> tags from an HTML fragment."""

    def __init__(self) -> None:
        super().__init__()
        self._collecting = False
        self._current_value: Optional[str] = None
        self._buffer: List[str] = []
        self.options: List[Tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "option":
            return
        self._collecting = True
        self._buffer.clear()
        self._current_value = None
        for key, value in attrs:
            if key.lower() == "value":
                self._current_value = value or ""

    def handle_data(self, data: str) -> None:
        if self._collecting:
            self._buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "option":
            return
        if self._current_value is not None:
            text = html.unescape("".join(self._buffer).strip())
            value = html.unescape(self._current_value.strip())
            self.options.append((value, text))
        self._buffer.clear()
        self._current_value = None
        self._collecting = False


class SelectOptionParser(HTMLParser):
    """Collect <option> values from a specific <select name=...>."""

    def __init__(self, select_name: str) -> None:
        super().__init__()
        self.select_name = select_name
        self._active = False
        self._current_value: Optional[str] = None
        self._buffer: List[str] = []
        self.options: List[Tuple[str, str]] = []

    def handle_starttag(
        self, tag: str, attrs: List[Tuple[str, Optional[str]]]
    ) -> None:
        tag_lower = tag.lower()
        if tag_lower == "select":
            attr_dict = {k.lower(): (v or "") for k, v in attrs}
            if attr_dict.get("name") == self.select_name:
                self._active = True
            return
        if not self._active or tag_lower != "option":
            return
        self._current_value = None
        self._buffer.clear()
        for key, value in attrs:
            if key.lower() == "value":
                self._current_value = value or ""

    def handle_data(self, data: str) -> None:
        if self._active and self._current_value is not None:
            self._buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag_lower = tag.lower()
        if tag_lower == "select":
            self._active = False
            return
        if not self._active or tag_lower != "option":
            return
        if self._current_value is not None:
            text = html.unescape("".join(self._buffer).strip())
            value = html.unescape(self._current_value.strip())
            self.options.append((value, text))
        self._buffer.clear()
        self._current_value = None


def parse_catalog_factories(html_fragment: str) -> List[Dict[str, str]]:
    parser = OptionCollector()
    parser.feed(html_fragment)
    factories: List[Dict[str, str]] = []
    for value, raw_text in parser.options:
        stripped = value.strip()
        if not stripped.isdigit():
            continue
        if int(stripped) <= 0:
            continue
        name = raw_text.strip()
        if "[" in name:
            name = name.split("[", 1)[0].strip()
        factories.append({"id": stripped, "name": name})
    return factories


def parse_lister_collections(page_html: str) -> List[Dict[str, str]]:
    parser = SelectOptionParser(LISTER_SELECT_NAME)
    parser.feed(page_html)
    collections: List[Dict[str, str]] = []
    for value, label in parser.options:
        if value in {"0", "-1"}:
            continue
        collections.append({"id": value, "name": label})
    return collections


def load_cookies(session: requests.Session, cookies_path: Path) -> None:
    data = json.loads(cookies_path.read_text(encoding="utf-8"))
    for cookie in data:
        session.cookies.set(
            cookie["name"],
            cookie["value"],
            domain=cookie.get("domain"),
            path=cookie.get("path") or "/",
            secure=cookie.get("secure", False),
        )


def validate_session(session: requests.Session, domain: str) -> bool:
    url = f"https://{domain}/afterbuy/administration.aspx"
    response = session.get(url, timeout=30)
    if response.status_code != 200:
        return False
    content = response.text.lower()
    if "form-signin" in content:
        return False
    if 'name="hiddenform"' in content:
        return False
    if "<title>working" in content:
        return False
    return True


def ensure_authenticated_session(account: str) -> Tuple[requests.Session, str]:
    key = account.upper()
    if key not in ACCOUNT_DOMAINS:
        raise ValueError(f"Unknown account '{account}'.")
    domain = ACCOUNT_DOMAINS[key]

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    cookies_file = SESSION_DIR / f"{key.lower()}_cookies.json"
    if cookies_file.exists():
        load_cookies(session, cookies_file)
        if validate_session(session, domain):
            return session, domain

    creds = get_credentials(key)
    client = AfterbuyClient(creds["login"], creds["password"], domain)
    session = client.login()
    save_cookies(key, session)
    return session, domain


def get_credentials(account: str) -> Dict[str, str]:
    env = read_env_file(Path(".env"))
    accounts = extract_accounts(env)
    if account not in accounts:
        raise ValueError(f"Credentials for '{account}' not found in .env.")
    return accounts[account]


def fetch_catalog_factories(session: requests.Session, domain: str) -> List[Dict[str, str]]:
    url = f"https://{domain}{CATALOG_ENDPOINT}"
    headers = {
        "Referer": CATALOG_REFERER.format(domain=domain),
        "Accept": "text/html, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }
    response = session.get(url, params=CATALOG_PARAMS, headers=headers, timeout=30)
    response.raise_for_status()
    if "form-signin" in response.text.lower():
        raise RuntimeError("Session is not authenticated; received login page.")
    return parse_catalog_factories(response.text)


def fetch_lister_collections(session: requests.Session, domain: str) -> List[Dict[str, str]]:
    url = f"https://{domain}{LISTER_PAGE_PATH}"
    headers = {
        "Referer": url,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    response = session.get(url, params=LISTER_PARAMS, headers=headers, timeout=30)
    response.raise_for_status()
    if "form-signin" in response.text.lower():
        raise RuntimeError("Session is not authenticated; received login page.")
    return parse_lister_collections(response.text)


def save_json(path: Path, payload: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def setup_logger(account: str) -> logging.Logger:
    LOG_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger(f"getFabrik_{account}")
    if logger.handlers:
        for handler in logger.handlers:
            handler.close()
        logger.handlers.clear()

    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(
        LOG_DIR / f"getFabrik_{account}.log", encoding="utf-8"
    )
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def process_account(
    account: str, targets: Sequence[str], logger: logging.Logger
) -> List[Tuple[str, Path, int]]:
    session, domain = ensure_authenticated_session(account)
    results: List[Tuple[str, Path, int]] = []
    try:
        if "catalog" in targets:
            factories = fetch_catalog_factories(session, domain)
            output_path = CATALOG_OUTPUTS[account]
            save_json(output_path, factories)
            results.append(("catalog", output_path, len(factories)))
            logger.info(
                "Каталоги: сохранено %d записей в %s", len(factories), output_path
            )
        if "lister" in targets:
            collections = fetch_lister_collections(session, domain)
            output_path = LISTER_OUTPUTS[account]
            save_json(output_path, collections)
            results.append(("lister", output_path, len(collections)))
            logger.info(
                "Коллекции Lister: сохранено %d записей в %s",
                len(collections),
                output_path,
            )
    finally:
        session.close()
    return results


def main() -> None:
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        try:
            sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        except Exception:
            pass

    parser = argparse.ArgumentParser(
        description="Fetch Afterbuy catalog factories and/or eBay Lister collections."
    )
    parser.add_argument(
        "--account",
        choices=ACCOUNT_ORDER,
        type=str.upper,
        help="Process only the specified account (default: JV then XL).",
    )
    parser.add_argument(
        "--target",
        choices=TARGET_CHOICES,
        default="all",
        help="Data to export: catalog, lister, or all (default).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable detailed logging (DEBUG level).",
    )
    args = parser.parse_args()

    _ = configure_logging(args.verbose)

    accounts = ACCOUNT_ORDER if args.account is None else [args.account]
    target_list: Sequence[str]
    if args.target == "all":
        target_list = ("catalog", "lister")
    else:
        target_list = (args.target,)

    LOGGER.info("Старт getFabrik. Аккаунты: %s. Цели: %s", ", ".join(accounts), ", ".join(target_list))

    max_workers = min(len(accounts), 10) or 1
    loggers = {account: setup_logger(account) for account in accounts}
    # Очистка предыдущих логов
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_account = {
            executor.submit(process_account, account, target_list, loggers[account]): account
            for account in accounts
        }
        for future in as_completed(future_to_account):
            account = future_to_account[future]
            logger = loggers[account]
            try:
                results = future.result()
            except Exception as exc:
                LOGGER.error("[%s] Ошибка обработки аккаунта: %s", account, exc)
                logger.error("Ошибка обработки аккаунта: %s", exc, exc_info=True)
                continue
            for kind, path, count in results:
                label = "Catalog factories" if kind == "catalog" else "Lister collections"
                LOGGER.info("[%s] %s: сохранено %d записей (%s)", account, label, count, path)
                logger.info("%s: сохранено %d записей (%s)", label, count, path)

    LOGGER.info("Завершение getFabrik.")

if __name__ == "__main__":
    main()

