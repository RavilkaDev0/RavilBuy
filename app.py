from __future__ import annotations

import json
import threading
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

from flask import Flask, jsonify, request, send_from_directory

from addIgnore import IGNORE_FABRIKS, add_ignore_entry
from clean_data import list_targets as list_clean_targets

BASE_DIR = Path(__file__).resolve().parent
SITE_DIR = BASE_DIR / "Site"

app = Flask(
    __name__,
    static_folder=str(SITE_DIR),
    static_url_path="",
    template_folder=str(SITE_DIR),
)

_RUN_LOCK = threading.Lock()
_RUN_PROCESS: Optional[subprocess.Popen[str]] = None
_CLEAN_LOCK = threading.Lock()
_CLEAN_PROCESS: Optional[subprocess.Popen[str]] = None


@app.route("/")
def index() -> str:
    return app.send_static_file("index.html")


@app.route("/add-ignore")
def add_ignore_page() -> str:
    return app.send_static_file("add-ignore.html")


@app.route("/Fabriks/<path:filepath>")
def serve_fabriks(filepath: str):
    return send_from_directory(BASE_DIR / "Fabriks", filepath)


@app.route("/Ignore/<path:filepath>")
def serve_ignore(filepath: str):
    return send_from_directory(BASE_DIR / "Ignore", filepath)


@app.route("/CSVDATA/<path:filepath>")
def serve_csvdata(filepath: str):
    return send_from_directory(BASE_DIR / "CSVDATA", filepath)


@app.route("/itemsF/<path:filepath>")
def serve_itemsf(filepath: str):
    return send_from_directory(BASE_DIR / "itemsF", filepath)


@app.route("/readyhtml/<path:filepath>")
def serve_readyhtml(filepath: str):
    return send_from_directory(BASE_DIR / "readyhtml", filepath)


def _collect_ignore_keys() -> List[str]:
    keys: Set[str] = set()
    for factory_type, path in IGNORE_FABRIKS.items():
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, list):
            continue
        for item in payload:
            if isinstance(item, dict) and "id" in item:
                keys.add(f"{factory_type}::{item['id']}")
    return sorted(keys)


@app.post("/api/add-ignore")
def api_add_ignore():
    payload = request.get_json(silent=True) or {}
    selections = payload.get("selections")
    overwrite = bool(payload.get("overwrite"))
    results: List[Dict[str, str]] = []

    if not isinstance(selections, list) or not selections:
        return jsonify(
            {
                "results": [
                    {
                        "status": "error",
                        "message": "Не переданы фабрики для добавления.",
                        "type": "",
                        "id": "",
                        "name": "",
                    }
                ]
            }
        ), 400

    for entry in selections:
        if not isinstance(entry, dict):
            continue
        factory_type = entry.get("type")
        factory_id = entry.get("id")
        factory_name = entry.get("name")
        if not factory_type or not factory_id:
            results.append(
                {
                    "status": "error",
                    "type": factory_type or "",
                    "id": factory_id or "",
                    "name": factory_name or "",
                    "message": "Отсутствует тип или идентификатор фабрики.",
                }
            )
            continue
        try:
            status, data = add_ignore_entry(
                factory_type,
                factory_id,
                name=factory_name,
                overwrite_name=overwrite,
            )
            results.append(
                {
                    "status": status,
                    "type": factory_type,
                    "id": data.get("id", ""),
                    "name": data.get("name", ""),
                    "message": "",
                }
            )
        except Exception as exc:  # noqa: BLE001
            results.append(
                {
                    "status": "error",
                    "type": factory_type,
                    "id": str(factory_id),
                    "name": factory_name or "",
                    "message": str(exc),
                }
            )

    return jsonify({"results": results, "ignore_keys": _collect_ignore_keys()})


@app.post("/api/run-main")
def api_run_main():
    payload = request.get_json(silent=True) or {}

    def normalize_list(value) -> List[str]:
        if isinstance(value, list):
            return [str(item) for item in value if item]
        return []

    steps = normalize_list(payload.get("steps"))
    log_level = str(payload.get("log_level") or "INFO")

    args = [sys.executable, "main.py"]
    if steps:
        args.append("--steps")
        args.extend(steps)
    if log_level:
        args.extend(["--log-level", log_level])

    with _RUN_LOCK:
        global _RUN_PROCESS
        if _RUN_PROCESS and _RUN_PROCESS.poll() is None:
            return (
                jsonify({"error": "Процесс уже выполняется. Дождитесь завершения или остановите его."}),
                409,
            )
        try:
            proc = subprocess.Popen(
                args,
                cwd=BASE_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": f"Не удалось запустить main.py: {exc}"}), 500
        _RUN_PROCESS = proc

    try:
        stdout, stderr = proc.communicate()
        returncode = proc.returncode
    finally:
        with _RUN_LOCK:
            if _RUN_PROCESS is proc:
                _RUN_PROCESS = None

    return jsonify(
        {
            "returncode": returncode,
            "stdout": stdout or "",
            "stderr": stderr or "",
            "command": " ".join(shlex.quote(str(arg)) for arg in args),
        }
    )


@app.post("/api/stop-main")
def api_stop_main():
    with _RUN_LOCK:
        proc = _RUN_PROCESS
        if not proc or proc.poll() is not None:
            return jsonify({"message": "Процесс не выполняется."}), 200
        proc.terminate()

    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    finally:
        with _RUN_LOCK:
            if _RUN_PROCESS is proc:
                _RUN_PROCESS = None

    return jsonify({"message": "Попытка остановить процесс выполнена."})


@app.get("/api/clean-targets")
def api_clean_targets():
    try:
        targets = list_clean_targets()
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": f"Не удалось получить список путей: {exc}"}), 500
    return jsonify({"targets": targets})


@app.post("/api/run-clean")
def api_run_clean():
    payload = request.get_json(silent=True) or {}
    dry_run = bool(payload.get("dry_run"))
    force = bool(payload.get("force"))
    targets_payload = payload.get("targets")

    def normalize_list(value) -> List[str]:
        if isinstance(value, list):
            return [str(item) for item in value if item]
        return []

    targets = normalize_list(targets_payload)

    args = [sys.executable, "clean_data.py"]
    if dry_run:
        args.append("--dry-run")
    if force or not dry_run:
        args.append("--force")
    if targets:
        for target_id in targets:
            args.extend(["--target", target_id])

    with _CLEAN_LOCK:
        global _CLEAN_PROCESS
        if _CLEAN_PROCESS and _CLEAN_PROCESS.poll() is None:
            return (
                jsonify({"error": "Очистка уже выполняется. Дождитесь завершения или остановите её."}),
                409,
            )
        try:
            proc = subprocess.Popen(
                args,
                cwd=BASE_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except Exception as exc:  # noqa: BLE001
            return jsonify({"error": f"Не удалось запустить clean_data.py: {exc}"}), 500
        _CLEAN_PROCESS = proc

    try:
        stdout, stderr = proc.communicate()
        returncode = proc.returncode
    finally:
        with _CLEAN_LOCK:
            if _CLEAN_PROCESS is proc:
                _CLEAN_PROCESS = None

    return jsonify(
        {
            "returncode": returncode,
            "stdout": stdout or "",
            "stderr": stderr or "",
            "command": " ".join(shlex.quote(str(arg)) for arg in args),
        }
    )


@app.post("/api/stop-clean")
def api_stop_clean():
    with _CLEAN_LOCK:
        proc = _CLEAN_PROCESS
        if not proc or proc.poll() is not None:
            return jsonify({"message": "Очистка не выполняется."}), 200
        proc.terminate()

    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    finally:
        with _CLEAN_LOCK:
            if _CLEAN_PROCESS is proc:
                _CLEAN_PROCESS = None

    return jsonify({"message": "Попытка остановить очистку выполнена."})


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
