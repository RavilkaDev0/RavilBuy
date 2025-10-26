import os, re, sys, json, glob, time, random
from datetime import datetime
import typing as t
import requests

# ---------- утилиты ----------
def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)

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
def load_cookies(session: requests.Session, cookies_json_path: str) -> None:
    if not os.path.isfile(cookies_json_path):
        log(f"Куки не найдены: {cookies_json_path}")
        return
    try:
        with open(cookies_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        cookies_list = data.get("cookies") if isinstance(data, dict) else data
        if not isinstance(cookies_list, list):
            log("Формат cookies.json не распознан")
            return
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
    except Exception as e:
        log(f"Ошибка загрузки куки: {e}")

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
    total = 0
    for p in json_files:
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            total += sum(1 for _ in iter_products(data))
        except Exception:
            pass

    print("=== START exportHTML ===")
    log(f"Найдено товаров: {total}")

    s = requests.Session()
    s.headers.update(HEADERS)
    load_cookies(s, cookies_file)

    saved = skipped = errors = 0

    for p in json_files:
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            log(f"Ошибка чтения {p}: {e}")
            continue

        for prod in iter_products(data):
            item_id = extract_id(prod)
            ean = extract_ean(prod)
            if not item_id or not ean:
                skipped += 1
                continue
            try:
                html = fetch_html(s, farm, item_id)
                if not html or len(html) < 256:
                    skipped += 1
                    continue

                # резка по <div id="EBdescription">
                html = trim_to_ebdescription(html)

                out_path = os.path.join(out_dir, f"{ean}.html")
                with open(out_path, "w", encoding="utf-8", newline="") as f:
                    f.write(html)  # перезапись
                saved += 1

            except requests.RequestException as e:
                errors += 1
                log(f"HTTP error id={item_id}: {e}")
            except Exception as e:
                errors += 1
                log(f"Save error EAN={ean} id={item_id}: {e}")

            time.sleep(0.05 + random.random() * 0.05)

    log(f"Готово. Сохранено: {saved}, пропущено: {skipped}, ошибок: {errors}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
