import json
from pathlib import Path

ALL_FABRIKS = {
    "JV_F_P": Path("Fabriks") / "JV_F_P" / "factories.json",
    "XL_F_P": Path("Fabriks") / "XL_F_P" / "factories.json",
    "JV_F_L": Path("Fabriks") / "JV_F_L" / "collections.json",
    "XL_F_L": Path("Fabriks") / "XL_F_L" / "collections.json",
}


KILL_FABRIKS = {
    "JV_F_P": Path("Ignore") / "JV_P.json",
    "XL_F_P": Path("Ignore") / "XL_P.json",
    "JV_F_L": Path("Ignore") / "JV_L.json",
    "XL_F_L": Path("Ignore") / "XL_L.json",
}

def kill_fabriks():
    for fabrik_key, fabrik_path in ALL_FABRIKS.items():
        kill_path = KILL_FABRIKS[fabrik_key]
        
        if not fabrik_path.exists() or not kill_path.exists():
            continue
        
        with open(fabrik_path, 'r', encoding='utf-8') as f:
            fabrik_data = json.load(f)
        
        with open(kill_path, 'r', encoding='utf-8') as f:
            kill_data = json.load(f)
        kill_ids = {item['id'] for item in kill_data}
        filtered_data = [item for item in fabrik_data if item['id'] not in kill_ids]
        with open(fabrik_path, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=2)
            
def main():
    kill_fabriks()
if __name__ == "__main__":
    main()