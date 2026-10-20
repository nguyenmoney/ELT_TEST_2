"""Buoc E (Extract): tai file parquet 1 thang cua NYC Yellow Taxi.

Chay:  uv run python -m nyc_elt.download 2024-06
"""
import os
import re
import sys
from pathlib import Path

import pyarrow.parquet as pq
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))


def validate_month(month: str) -> str:
    """Chi nhan dang YYYY-MM (vd 2024-06)."""
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError(f"month phai co dang YYYY-MM, nhan duoc: {month!r}")
    return month


def is_valid_parquet(path: Path) -> bool:
    """File hop le neu doc duoc metadata va co it nhat 1 dong."""
    try:
        return pq.ParquetFile(path).metadata.num_rows > 0
    except Exception:
        return False


def download(month: str) -> Path:
    month = validate_month(month)
    filename = f"yellow_tripdata_{month}.parquet"
    out = DATA_DIR / "raw" / filename
    out.parent.mkdir(parents=True, exist_ok=True)

    if out.exists():
        if is_valid_parquet(out):
            print(f"Da co file hop le {out}, bo qua")
            return out
        print(f"File {out} hong, tai lai")
        out.unlink()

    url = f"{BASE_URL}/{filename}"
    tmp = out.with_suffix(".parquet.part")
    print(f"Dang tai {url}")
    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
        if not is_valid_parquet(tmp):
            raise RuntimeError(f"File tai ve khong phai parquet hop le: {url}")
        tmp.replace(out)  # chi doi ten khi da tai xong va kiem tra xong
    finally:
        tmp.unlink(missing_ok=True)

    print(f"Xong: {out} ({out.stat().st_size / 1e6:.1f} MB)")
    return out


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Dung: python -m nyc_elt.download YYYY-MM")
    download(sys.argv[1])