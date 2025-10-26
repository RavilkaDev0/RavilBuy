from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TARGETS = [ROOT / 'templates', ROOT / 'static', ROOT.parent / 'Ignore', ROOT.parent / 'Fabriks']
EXTS = {'.html', '.css', '.js', '.json'}


def decode_best(data: bytes) -> str:
    try:
        return data.decode('utf-8')
    except UnicodeDecodeError:
        pass
    try:
        return data.decode('cp1251')
    except UnicodeDecodeError:
        pass
    return data.decode('latin-1')


def fix_file(path: Path) -> bool:
    raw = path.read_bytes()
    text = decode_best(raw)
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    try:
        raw2 = text.encode('utf-8')
    except Exception:
        return False
    if raw2 != raw:
        path.write_bytes(raw2)
        return True
    return False


def main() -> int:
    changed = 0
    for base in TARGETS:
        if not base.exists():
            continue
        for p in base.rglob('*'):
            if p.is_file() and p.suffix.lower() in EXTS:
                try:
                    if fix_file(p):
                        changed += 1
                        print('[fixed]', p)
                except Exception as e:
                    print('[skip]', p, ':', e)
    print('Done. Files fixed:', changed)
    return 0


if __name__ == '__main__':
    sys.exit(main())


