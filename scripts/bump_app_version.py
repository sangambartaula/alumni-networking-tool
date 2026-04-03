from decimal import Decimal
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parent.parent
VERSION_FILE = ROOT / "app_version.py"
MANIFEST_FILE = ROOT / "app_update.json"
STEP = Decimal("0.1")


def read_current_version() -> Decimal:
    namespace = {}
    code = VERSION_FILE.read_text(encoding="utf-8")
    exec(code, namespace)
    return Decimal(str(namespace["APP_VERSION"]))


def write_version_file(version: Decimal) -> None:
    VERSION_FILE.write_text(f'APP_VERSION = "{version}"\n', encoding="utf-8")


def write_manifest_file(version: Decimal) -> None:
    data = {
        "version": str(version),
        "download_url": "https://github.com/sangambartaula/alumni-networking-tool/releases/latest",
        "notes": "Desktop build update",
    }
    MANIFEST_FILE.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    current = read_current_version()
    next_version = current + STEP
    write_version_file(next_version)
    write_manifest_file(next_version)
    print(f"Bumped app version: {current} -> {next_version}")


if __name__ == "__main__":
    main()
