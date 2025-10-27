from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

from flask import (
    Flask,
    redirect,
    render_template,
    request,
    url_for,
    flash,
    send_from_directory,
    jsonify,
    abort,
)

BASE_DIR = Path(__file__).resolve().parent.parent
SITE_DIR = Path(__file__).resolve().parent

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

IGNORE_DIR = BASE_DIR / "Ignore"
FABRIKS_DIR = BASE_DIR / "Fabriks"
SELECTION_FILE = SITE_DIR / "data" / "selection.json"
LOGS_DIR = BASE_DIR / "LOGs"
PIDS_FILE = SITE_DIR / "data" / "pids.json"

app = Flask(__name__, static_folder='static', template_folder='templates', static_url_path='/static')
# Windows workaround: предотвращаем чтение реестра в mimetypes и заранее
# регистрируем базовые типы, чтобы избежать PermissionError при выдаче статики.
try:
    import mimetypes as _m
    _m.inited = True
    _m.add_type('text/css', '.css')
    _m.add_type('application/javascript', '.js')
    _m.add_type('image/svg+xml', '.svg')
except Exception:
    pass

@app.get('/static/style.css')
def _static_css():
    return send_from_directory(str(SITE_DIR / 'static'), 'style.css', mimetype='text/css')
app.secret_key = os.environ.get("SITE_SECRET", "dev-secret")


ACCOUNTS = ["JV", "XL"]

from getEANfromJSON import (  # noqa: E402
    build_ready_index as ean_build_ready_index,
    extract_eans as ean_extract_eans,
    load_collections as ean_load_collections,
)


def _read_json_robust(path: Path):
    try:
        raw = path.read_bytes()
    except Exception:
        return None
    for enc in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
        try:
            text = raw.decode(enc)
            return json.loads(text)
        except Exception:
            continue
    try:
        return json.loads(raw.decode("utf-8", errors="ignore"))
    except Exception:
        return None


def load_factories() -> Dict[str, List[Dict[str, str]]]:
    result: Dict[str, List[Dict[str, str]]] = {}
    for acc in ACCOUNTS:
        path = FABRIKS_DIR / f"{acc}_F_L" / "collections.json"
        if not path.exists():
            result[acc] = []
            continue
        data = _read_json_robust(path)
        if isinstance(data, list):
            result[acc] = data  # type: ignore[assignment]
        else:
            result[acc] = []
    return result


def load_ignore() -> Dict[str, List[Dict[str, str]]]:
    mapping = {"JV": "JV_L.json", "XL": "XL_L.json"}
    out: Dict[str, List[Dict[str, str]]] = {}
    for acc, fname in mapping.items():
        p = IGNORE_DIR / fname
        if not p.exists():
            out[acc] = []
            continue
        data = _read_json_robust(p)
        if isinstance(data, list):
            out[acc] = data  # type: ignore[assignment]
        else:
            out[acc] = []
    return out


def save_ignore(data: Dict[str, List[Dict[str, str]]]) -> None:
    IGNORE_DIR.mkdir(parents=True, exist_ok=True)
    paths = {"JV": IGNORE_DIR / "JV_L.json", "XL": IGNORE_DIR / "XL_L.json"}
    for acc, items in data.items():
        p = paths.get(acc)
        if not p:
            continue
        p.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


@app.route("/")
def index():
    counts = {acc: len(load_factories().get(acc, [])) for acc in ACCOUNTS}
    return render_template("index.html", counts=counts)


@app.route("/pipeline")
def page_pipeline():
    return render_template("pipeline.html")


@app.route("/getfabrik")
def page_getfabrik():
    return redirect(url_for("page_ignore"))


@app.route("/ignore")
def page_ignore():
    def _safe_list(items: object) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    out.append({
                        "id": str(it.get("id", "")),
                        "name": str(it.get("name", "")),
                    })
        out.sort(key=lambda x: x["name"].lower())
        return out

    raw_factories = load_factories()
    counts = {acc: len(raw_factories.get(acc, [])) for acc in ACCOUNTS}
    raw_ignore = load_ignore()

    ignore_ids = {
        "JV": {str(i.get("id", "")) for i in raw_ignore.get("JV", []) if isinstance(i, dict)},
        "XL": {str(i.get("id", "")) for i in raw_ignore.get("XL", []) if isinstance(i, dict)},
    }
    filtered_factories: dict[str, list[dict[str, str]]] = {"JV": [], "XL": []}
    for acc in ("JV", "XL"):
        for it in _safe_list(raw_factories.get(acc, [])):
            if it["id"] not in ignore_ids[acc]:
                filtered_factories[acc].append(it)
    view_ignore = {k: _safe_list(v) for k, v in raw_ignore.items()}
    return render_template("ignore.html", factories=filtered_factories, ignore=view_ignore, counts=counts)

@app.route("/selected")
def page_selected():
    factories = load_factories()
    return render_template("selected.html", factories=factories)


def _factory_view() -> tuple[dict[str, list[dict[str, object]]], dict[str, dict[str, int]]]:
    raw_factories = load_factories()
    ready_index = {acc: ean_build_ready_index(acc) for acc in ACCOUNTS}

    view: dict[str, list[dict[str, object]]] = {acc: [] for acc in ACCOUNTS}
    stats: dict[str, dict[str, int]] = {}

    for acc in ACCOUNTS:
        entries: list[dict[str, object]] = []
        raw_list = raw_factories.get(acc, [])
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            factory_id = str(item.get("id", "")).strip()
            if not factory_id:
                continue
            name = str(item.get("name", "")).strip() or f"factory_{factory_id}"
            has_ready = factory_id in ready_index.get(acc, {})
            entries.append({
                "id": factory_id,
                "name": name,
                "has_ready": has_ready,
            })
        entries.sort(key=lambda x: str(x["name"]).lower())
        view[acc] = entries
        stats[acc] = {
            "total": len(entries),
            "ready": sum(1 for entry in entries if entry["has_ready"]),
        }
    return view, stats


@app.route("/ean")
def page_ean():
    factories, stats = _factory_view()
    requested = request.args.get("account", default="JV", type=str) or "JV"
    requested = requested.upper()
    if requested not in ACCOUNTS:
        requested = next((acc for acc in ACCOUNTS if factories.get(acc)), ACCOUNTS[0])
    initial_account = requested
    api_template = url_for("api_ean_factory", account="ACCOUNT_PLACEHOLDER", factory_id="FACTORY_PLACEHOLDER")
    return render_template(
        "ean.html",
        factories=factories,
        stats=stats,
        accounts=ACCOUNTS,
        initial_account=initial_account,
        api_template=api_template,
    )


@app.get("/ean/<account>/<factory_id>")
def api_ean_factory(account: str, factory_id: str):
    account = (account or "").upper()
    factory_id = str(factory_id or "").strip()
    if account not in ACCOUNTS:
        return jsonify({"error": "Неизвестный аккаунт.", "code": "unknown_account"}), 404
    try:
        collections = ean_load_collections(account)
    except FileNotFoundError:
        return jsonify({"error": "Файл collections.json не найден.", "code": "missing_collections"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc), "code": "invalid_collections"}), 500

    if factory_id not in collections:
        return jsonify({"error": "Фабрика не найдена.", "code": "unknown_factory"}), 404

    ready_index = ean_build_ready_index(account)
    sources = ready_index.get(factory_id, [])
    if not sources:
        return jsonify({"error": "Для этой фабрики нет готовых JSON.", "code": "no_ready_json"}), 404

    try:
        eans, total_items, empty_count, empty_details = ean_extract_eans(
            sources,
            dedupe=True,
            include_empty=True,
        )
    except ValueError as exc:
        return jsonify({"error": str(exc), "code": "parse_error"}), 500

    source_files: list[str] = []
    for path in sources:
        try:
            source_files.append(str(path.relative_to(BASE_DIR)))
        except ValueError:
            source_files.append(str(path))

    payload = {
        "account": account,
        "factory_id": factory_id,
        "factory_name": collections[factory_id],
        "eans": eans,
        "ean_count": len(eans),
        "total_items": total_items,
        "blank_ean_count": empty_count,
        "blank_entries": empty_details,
        "sources": source_files,
    }
    return jsonify(payload)


@app.route("/logs")
def page_logs():
    LOGS_DIR.mkdir(exist_ok=True)
    files = sorted([p.name for p in LOGS_DIR.glob("*.log")])
    return render_template("logs.html", files=files)


@app.get("/logs/stream")
def logs_stream():
    name = request.args.get("name", type=str)
    pos = request.args.get("pos", default=None, type=int)
    if not name or "/" in name or "\\" in name:
        abort(400)
    path = LOGS_DIR / name
    if not path.exists() or not path.is_file():
        abort(404)
    size = path.stat().st_size
    if pos is None:
        # Вернуть хвост ~10Кб при первом запросе
        start = max(0, size - 10_000)
    else:
        start = max(0, min(pos, size))
    try:
        with open(path, "rb") as f:
            f.seek(start)
            data = f.read()
        # Робастное декодирование логов
        text = None
        for enc in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
            try:
                text = data.decode(enc)
                break
            except Exception:
                continue
        if text is None:
            text = data.decode("utf-8", errors="ignore")
    except Exception:
        text = ""
    return jsonify({"pos": size, "data": text})


def _python() -> str:
    return sys.executable


# ---- PIDs tracking helpers ----
def _pids_load() -> list[int]:
    try:
        PIDS_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = json.loads(PIDS_FILE.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [int(x) for x in data]
    except Exception:
        pass
    return []


def _pids_save(pids: list[int]) -> None:
    try:
        PIDS_FILE.parent.mkdir(parents=True, exist_ok=True)
        PIDS_FILE.write_text(json.dumps(pids, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _pids_add(pid: int) -> None:
    pids = _pids_load()
    if pid not in pids:
        pids.append(pid)
        _pids_save(pids)


@app.post("/run/start")
def run_start():
    steps = request.form.get("steps", "").strip()
    args = [
        _python(),
        str(BASE_DIR / "start.py"),
    ]
    if steps:
        args += ["--steps", steps]
    if request.form.get("verbose"):
        args.append("--verbose")

    proc = subprocess.Popen(args, cwd=str(BASE_DIR))
    _pids_add(proc.pid)
    flash("Пайплайн запущен")
    return redirect(url_for("page_pipeline"))


@app.post("/run/getfabrik")
def run_getfabrik():
    args = [_python(), str(BASE_DIR / "getFabrik.py")]
    if request.form.get("verbose"):
        args.append("--verbose")
    proc = subprocess.Popen(args, cwd=str(BASE_DIR))
    _pids_add(proc.pid)
    flash("getFabrik запущен")
    return redirect(url_for("page_getfabrik"))


@app.post("/ignore/add")
def add_ignore():
    acc = request.form.get("account", "JV").upper()
    selected = request.form.getlist("factory_id")
    selected_set = {s.strip() for s in selected if s.strip()}

    data = load_factories()
    mapJV = {str(f.get("id")): str(f.get("name", "")) for f in data.get("JV", [])}
    mapXL = {str(f.get("id")): str(f.get("name", "")) for f in data.get("XL", [])}

    added = 0
    for token in selected_set:
        if ":" in token:
            acc2, fid = token.split(":", 1)
            acc2 = acc2.upper()
        else:
            acc2, fid = acc, token
        name = (mapJV.get(fid) if acc2 == "JV" else mapXL.get(fid, ""))
        target_type = f"{acc2}_F_L"
        cmd = [sys.executable, str(BASE_DIR / "addIgnore.py"), "--type", target_type, "--id", fid]
        if name:
            cmd += ["--name", name]
        try:
            subprocess.run(cmd, cwd=str(BASE_DIR))
            added += 1
        except Exception:
            pass
    flash(f"В игнор добавлено: {added}")
    return redirect(url_for("page_ignore"))


@app.post("/selected/run")
def run_selected():
    # РЎРѕР±РёСЂР°РµРј РІС‹Р±РѕСЂ РїРѕР»СЊР·РѕРІР°С‚РµР»СЏ Рё Р·Р°РїСѓСЃРєР°РµРј СЃРїРµС†РёР°Р»РёР·РёСЂРѕРІР°РЅРЅС‹Р№ СЃРєСЂРёРїС‚
    selection: Dict[str, List[str]] = {acc: [] for acc in ACCOUNTS}
    for acc in ACCOUNTS:
        ids = request.form.getlist(f"sel_{acc}")
        selection[acc] = [i for i in ids if i]

    SELECTION_FILE.parent.mkdir(parents=True, exist_ok=True)
    SELECTION_FILE.write_text(json.dumps(selection, ensure_ascii=False, indent=2), encoding="utf-8")

    args = [_python(), str(BASE_DIR / "selectedRun.py"), "--selection-file", str(SELECTION_FILE)]
    if request.form.get("verbose"):
        args.append("--verbose")
    proc = subprocess.Popen(args, cwd=str(BASE_DIR))
    _pids_add(proc.pid)
    flash("Запущен разовый парсинг выбранных фабрик")
    return redirect(url_for("page_selected"))


@app.post("/run/stop")
def run_stop():
    pids = _pids_load()
    killed = 0
    remaining: list[int] = []
    for pid in pids:
        try:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                try:
                    os.kill(pid, 15)
                except Exception:
                    pass
                try:
                    os.kill(pid, 9)
                except Exception:
                    pass
            killed += 1
        except Exception:
            remaining.append(pid)
    _pids_save(remaining)
    flash(f"Остановлено процессов: {killed}")
    return redirect(url_for("page_pipeline"))


def create_app():
    return app


if __name__ == "__main__":
    # Отключаем отладчик Flask, чтобы избежать загрузки ресурсов дебаггера
    # (на некоторых системах это триггерит чтение реестра Windows и PermissionError)
    app.run(host="0.0.0.0", port=8000, debug=False)





