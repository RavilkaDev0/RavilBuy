import os, re, sys, json, glob, time, threading
import typing as t
import requests
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, as_completed
from requests.adapters import HTTPAdapter
from requests.utils import cookiejar_from_dict, dict_from_cookiejar
from loguru import logger

# ---------- утилиты ----------
def log(msg: str, level: str = "INFO") -> None:
    logger.opt(depth=1).log(level.upper(), msg)

DEFAULT_ACCOUNT = os.environ.get("AFTERBUY_ACCOUNT", "JV").upper()
ACCOUNTS = {"JV", "XL"}

def cfg_by_acc(acc: str) -> dict:
    if acc == "XL":
        return {
            "farm": "https://farm04.afterbuy.de",
            "json_dir": os.path.join("readyJSON", "XL"),
            "out_dir": os.path.join("readyhtml", "XL"),
            "cookies_file": os.path.join("sessions", "xl_cookies.json"),
        }
    # JV
    return {
        "farm": "https://farm01.afterbuy.de",
        "json_dir": os.path.join("readyJSON", "JV"),
        "out_dir": os.path.join("readyhtml", "JV"),
        "cookies_file": os.path.join("sessions", "jv_cookies.json"),
    }

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "ru,en-US;q=0.9,en;q=0.8,de;q=0.7",
    "cache-control": "no-cache",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
}

ID_KEYS = {"ID"}
EAN_KEYS = {"EAN"}

LOG_LEVEL = os.environ.get("EXPORTHTML_LOG_LEVEL", "INFO").upper()
LOG_FILE = os.environ.get("EXPORTHTML_LOG_FILE")

logger.remove()
logger.add(sys.stdout, level=LOG_LEVEL, format="[{time:YYYY-MM-DD HH:mm:ss}] {level:<7} {message}", enqueue=True)
if LOG_FILE:
    try:
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        logger.add(
            LOG_FILE,
            level=LOG_LEVEL,
            format="[{time:YYYY-MM-DD HH:mm:ss}] {level:<7} {message}",
            encoding="utf-8",
            enqueue=True,
        )
    except Exception as exc:
        logger.warning(f"Не удалось подключить лог-файл '{LOG_FILE}': {exc}")

def _env_int(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, default))
        return value if value > 0 else default
    except (TypeError, ValueError):
        return default

def _env_float(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name, default))
        return value if value >= 0 else default
    except (TypeError, ValueError):
        return default

DEFAULT_WORKERS = min(8, max(2, os.cpu_count() or 4))
MAX_WORKERS = _env_int("EXPORTHTML_WORKERS", DEFAULT_WORKERS)
REQUEST_DELAY = _env_float("EXPORTHTML_DELAY", 0.0)
SUBMIT_CHUNK = max(MAX_WORKERS * 4, MAX_WORKERS)
_THREAD_LOCAL = threading.local()

# ---------- парсер JSON ----------
def iter_products(obj: t.Any) -> t.Iterable[dict]:
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            kl = {k.lower() for k in cur.keys()}
            if (kl & {k.lower() for k in ID_KEYS}) and (kl & {k.lower() for k in EAN_KEYS}):
                yield cur
            for v in cur.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(cur, list):
            for v in cur:
                if isinstance(v, (dict, list)):
                    stack.append(v)

def extract_id(d: dict) -> t.Optional[str]:
    for k, v in d.items():
        if k.lower() in {"id", "itemid", "item_id"} and v is not None:
            s = str(v).strip()
            if s.isdigit():
                return s
    for v in d.values():
        if v is None:
            continue
        s = str(v).strip()
        if re.fullmatch(r"\d{6,}", s):
            return s
    return None

def extract_ean(d: dict) -> t.Optional[str]:
    for k, v in d.items():
        if k.lower() == "ean" and v is not None:
            s = re.sub(r"\D", "", str(v))
            if 8 <= len(s) <= 18:
                return s
    for v in d.values():
        if v is None:
            continue
        s = re.sub(r"\D", "", str(v))
        if 8 <= len(s) <= 18:
            return s
    return None

# ---------- куки ----------
def load_cookies(session: requests.Session, cookies_json_path: str) -> t.Optional[requests.cookies.RequestsCookieJar]:
    if not os.path.isfile(cookies_json_path):
        log(f"Куки не найдены: {cookies_json_path}", "WARNING")
        return None
    try:
        with open(cookies_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        cookies_list = data.get("cookies") if isinstance(data, dict) else data
        if not isinstance(cookies_list, list):
            log("Формат cookies.json не распознан", "WARNING")
            return None
        jar = requests.cookies.RequestsCookieJar()
        for c in cookies_list:
            if not isinstance(c, dict):
                continue
            jar.set(
                c.get("name"),
                c.get("value"),
                domain=c.get("domain"),
                path=c.get("path", "/"),
                secure=c.get("secure", False),
            )
        session.cookies = jar
        log(f"Куки загружены: {cookies_json_path}")
        return jar
    except Exception as e:
        log(f"Ошибка загрузки куки: {e}", "ERROR")
    return None

# ---------- обрезка HTML выше EBdescription ----------
_EB_RE = re.compile(r'(?is)<div\s+id=["\']EBdescription["\'][^>]*>')

def trim_to_ebdescription(html: str) -> str:
    m = _EB_RE.search(html)
    if not m:
        return html  # если блока нет — ничего не режем
    return html[m.end():]  # всё ДО и ВКЛЮЧАЯ тег удаляем

# ---------- загрузка HTML ----------
def fetch_html(session: requests.Session, farm: str, item_id: str) -> str:
    url = f"{farm}/afterbuy/ebayListerVorschau.aspx?itemid={item_id}"
    headers = HEADERS.copy()
    headers["referer"] = f"{farm}/afterbuy/ebayliste2.aspx?art=edit&id={item_id}&rsposition=0&rssuchbegriff="
    r = session.get(url, headers=headers, allow_redirects=True, timeout=60)
    if r.status_code != 200:
        return ""
    return r.text or ""

def _get_session(cookie_dict: t.Mapping[str, str]) -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)
        if cookie_dict:
            session.cookies = cookiejar_from_dict(cookie_dict)
        adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS * 2)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _THREAD_LOCAL.session = session
    return session

def process_product(
    item_id: str,
    ean: str,
    farm: str,
    out_dir: str,
    cookie_dict: t.Mapping[str, str],
) -> tuple[str, t.Optional[str]]:
    session = _get_session(cookie_dict)
    try:
        html = fetch_html(session, farm, item_id)
    except requests.RequestException as exc:
        return "error", f"HTTP error id={item_id}: {exc}"

    if not html or len(html) < 256:
        if REQUEST_DELAY:
            time.sleep(REQUEST_DELAY)
        log(f"EAN {ean}: получен пустой HTML, пропуск (item {item_id})", "DEBUG")
        return "skip", None

    html = trim_to_ebdescription(html)
    out_path = os.path.join(out_dir, f"{ean}.html")
    try:
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            f.write(html)
    except Exception as exc:
        return "error", f"Save error EAN={ean} id={item_id}: {exc}"

    log(f"EAN {ean}: сохранён HTML (item {item_id})", "DEBUG")
    if REQUEST_DELAY:
        time.sleep(REQUEST_DELAY)
    return "saved", None

# ---------- main ----------
def main() -> int:
    acc = DEFAULT_ACCOUNT if DEFAULT_ACCOUNT in ACCOUNTS else "JV"
    cfg = cfg_by_acc(acc)
    farm = cfg["farm"]
    json_dir = cfg["json_dir"]
    out_dir = cfg["out_dir"]
    cookies_file = cfg["cookies_file"]

    os.makedirs(out_dir, exist_ok=True)

    json_files = sorted(glob.glob(os.path.join(json_dir, "*.json")))
    log("=== START exportHTML ===")
    log(f"JSON файлов для обработки: {len(json_files)}")
    log(f"Используемых потоков: {MAX_WORKERS}")

    base_session = requests.Session()
    base_session.headers.update(HEADERS)
    cookie_jar = load_cookies(base_session, cookies_file)
    cookie_dict: t.Mapping[str, str] = dict_from_cookiejar(cookie_jar) if cookie_jar else {}
    base_session.close()

    saved = skipped = errors = 0
    total_products = 0
    scheduled = 0

    def handle_future(future) -> None:
        nonlocal saved, skipped, errors
        try:
            status, details = future.result()
        except Exception as exc:
            errors += 1
            log(f"Unexpected worker error: {exc}", "ERROR")
            return
        if status == "saved":
            saved += 1
        elif status == "skip":
            skipped += 1
        elif status == "error":
            errors += 1
            if details:
                log(details, "ERROR")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        active: t.Set[t.Any] = set()
        for p in json_files:
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                log(f"Ошибка чтения {p}: {e}", "ERROR")
                continue

            file_total = 0
            file_scheduled = 0
            file_missing = 0
            for prod in iter_products(data):
                total_products += 1
                file_total += 1
                item_id = extract_id(prod)
                ean = extract_ean(prod)
                if not item_id or not ean:
                    skipped += 1
                    file_missing += 1
                    log(f"{os.path.basename(p)}: запись без ID/EAN пропущена", "DEBUG")
                    continue

                future = executor.submit(process_product, item_id, ean, farm, out_dir, cookie_dict)
                active.add(future)
                scheduled += 1
                file_scheduled += 1

                if len(active) >= SUBMIT_CHUNK:
                    done, active = wait(active, return_when=FIRST_COMPLETED)
                    for fut in done:
                        handle_future(fut)

            log(
                f"{os.path.basename(p)}: найдено {file_total}, поставлено {file_scheduled}, пропущено без ID/EAN {file_missing}"
            )

        log(f"Передано на выгрузку: {scheduled} товаров (из найденных {total_products})")
        for fut in as_completed(active):
            handle_future(fut)

    log(f"Готово. Сохранено: {saved}, пропущено: {skipped}, ошибок: {errors}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
