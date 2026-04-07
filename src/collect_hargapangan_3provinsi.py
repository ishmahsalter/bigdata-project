"""
=============================================================
  KOLEKTOR DATA HARGA PANGAN - 3 PROVINSI
  Sumber: PIHPS Nasional - Bank Indonesia
  URL: https://www.bi.go.id/hargapangan
=============================================================
  Provinsi yang dikumpulkan:
  - DKI Jakarta
  - Bali
  - Sumatera Utara

  Fitur:
  - Semua jenis pasar (Tradisional, Modern, Pedagang Besar, Produsen)
  - Kurun waktu: 3 tahun terakhir (Januari 2023 - Desember 2025)
  - Output: CSV dan JSON (folder: hasil_3provinsi/)
  - Resume otomatis jika proses terputus

  ESTIMASI:
  - Total request: ~4 jenis pasar × 3 provinsi × 36 bulan = ~432 request
  - Waktu: ~15-30 menit
=============================================================
"""

import urllib.request
import urllib.parse
import urllib.error
import json
import time
import os
import csv
import re
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# ─────────────────────────────────────────
#  KONFIGURASI
# ─────────────────────────────────────────
BASE_URL   = "https://www.bi.go.id/hargapangan"
OUTPUT_DIR = "hasil_3provinsi"
PROGRESS_DIR = os.path.join(OUTPUT_DIR, "progress")

DELAY_BETWEEN_REQUESTS = 1.5    # detik
MAX_RETRIES    = 3
REQUEST_TIMEOUT = 60

# 3 tahun terakhir (Januari 2023 - Desember 2025)
START_DATE = date(2023, 1, 1)
END_DATE   = date(2025, 12, 1)

# Provinsi target (akan dicocokkan dengan data dari API, case-insensitive)
TARGET_PROVINCES = [
    "dki jakarta",
    "bali",
    "sumatera utara",
]

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(PROGRESS_DIR, exist_ok=True)

# ─────────────────────────────────────────
#  HEADERS HTTP
# ─────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bi.go.id/hargapangan",
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8",
}

# ─────────────────────────────────────────
#  HELPER: HTTP GET dengan retry
# ─────────────────────────────────────────
def http_get(url):
    req = urllib.request.Request(url, headers=HEADERS)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as r:
                raw    = r.read().decode("utf-8", errors="replace")
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and "data" in parsed:
                    return parsed["data"]
                return parsed
        except urllib.error.HTTPError as e:
            wait = DELAY_BETWEEN_REQUESTS * attempt * 2
            print(f"    [WARN] HTTP {e.code} — percobaan {attempt}/{MAX_RETRIES} (tunggu {wait:.0f}s)")
            if attempt < MAX_RETRIES:
                time.sleep(wait)
        except Exception as e:
            wait = DELAY_BETWEEN_REQUESTS * attempt * 2
            print(f"    [WARN] {type(e).__name__} percobaan {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(wait)
    return None


def progress_path(fname):
    return os.path.join(PROGRESS_DIR, fname)

def save_json(fname, obj):
    with open(progress_path(fname), "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)

def load_json(fname):
    p = progress_path(fname)
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    return None


# ─────────────────────────────────────────
#  AMBIL MASTER DATA
# ─────────────────────────────────────────
def get_price_types():
    data = http_get(f"{BASE_URL}/WebSite/Home/GetType")
    if not data:
        # Fallback hardcoded jika API gagal
        return [
            {"price_type_id": 1, "price_type_name": "Pasar Tradisional"},
            {"price_type_id": 2, "price_type_name": "Pasar Modern"},
            {"price_type_id": 3, "price_type_name": "Pedagang Besar"},
            {"price_type_id": 4, "price_type_name": "Produsen"},
        ]
    return data

def get_provinces():
    data = http_get(f"{BASE_URL}/WebSite/Home/GetProvinceAll")
    if not data:
        return []
    # Hanya ambil provinsi yang ada di TARGET_PROVINCES (cocokkan nama)
    filtered = []
    for p in data:
        prov_id   = p.get("province_id", 0)
        prov_name = (p.get("province_name") or "").strip()
        if prov_id == 0:
            continue  # skip "Semua Provinsi"
        if prov_name.lower() in TARGET_PROVINCES:
            filtered.append(p)
    return filtered


# ─────────────────────────────────────────
#  AMBIL DATA HARGA
# ─────────────────────────────────────────
def get_price_data(price_type_id, province_id, start_str, end_str):
    params = {
        "price_type_id": price_type_id,
        "start_date":    start_str,
        "end_date":      end_str,
        "province_id":   province_id,
        "regency_id":    "",
        "market_id":     "",
        "tipe_laporan":  1,
    }
    qs  = urllib.parse.urlencode(params)
    url = f"{BASE_URL}/WebSite/TabelHarga/GetGridDataDaerah?{qs}"
    return http_get(url)


def parse_rows(data_list, price_type_name, province_name, year_month):
    """
    Ubah raw JSON list → flat dict list (Long Format).
    Kolom: tanggal, bulan, tahun, price_type, province, category, commodity, price
    """
    rows = []
    if not data_list:
        return rows

    current_category = ""
    date_pattern = re.compile(r"^\d{2}/\d{2}/\d{4}$")
    date_cols    = [k for k in data_list[0].keys() if date_pattern.match(k)]

    for item in data_list:
        level = item.get("level")
        name  = (item.get("name") or "").strip()

        if level == 1:
            current_category = name
            continue  # baris header kategori

        for dc in date_cols:
            price_str = item.get(dc, "")
            if not price_str:
                continue
            try:
                price = float(str(price_str).replace(",", "").strip())
            except (ValueError, TypeError):
                continue
            # Pecah tanggal: DC format DD/MM/YYYY
            day, month, year = dc.split("/")
            rows.append({
                "tanggal":    day,
                "bulan":      month,
                "tahun":      year,
                "price_type": price_type_name,
                "province":   province_name,
                "category":   current_category,
                "commodity":  name,
                "price":      price,
            })
    return rows


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────
def main():
    print("=" * 66)
    print("  KOLEKTOR DATA HARGA PANGAN - 3 PROVINSI")
    print("  Bank Indonesia (PIHPS Nasional)")
    print("=" * 66)
    print(f"  Provinsi : DKI Jakarta | Bali | Sumatera Utara")
    print(f"  Rentang  : {START_DATE.strftime('%B %Y')} - {END_DATE.strftime('%B %Y')}")
    print(f"  Output   : {OUTPUT_DIR}/")
    print("=" * 66)

    # == Master data ==
    print("\n[STEP 1] Ambil daftar jenis pasar...")
    price_types = load_json("price_types.json") or get_price_types()
    save_json("price_types.json", price_types)
    for pt in price_types:
        print(f"  [{pt['price_type_id']}] {pt['price_type_name']}")

    print("\n[STEP 2] Ambil daftar provinsi (filter 3 target)...")
    provinces = load_json("provinces.json") or get_provinces()
    if not provinces:
        print("  [ERROR] Gagal mendapatkan daftar provinsi. Cek koneksi internet.")
        return
    save_json("provinces.json", provinces)

    # Verifikasi provinsi yang ditemukan
    found_names = [p["province_name"] for p in provinces]
    print(f"  Provinsi ditemukan ({len(provinces)}):")
    for name in found_names:
        print(f"    ✓ {name}")

    # Cek apakah semua target ditemukan
    for target in TARGET_PROVINCES:
        if not any(target == p["province_name"].lower() for p in provinces):
            print(f"    ⚠ TIDAK DITEMUKAN: '{target}' — periksa ejaan nama di API")

    # == Daftar bulan ==
    months = []
    cur = START_DATE
    while cur <= END_DATE:
        months.append(cur)
        cur += relativedelta(months=1)
    print(f"\n[STEP 3] Total bulan: {len(months)}")
    print(f"  ({months[0].strftime('%b %Y')} s/d {months[-1].strftime('%b %Y')})")

    # == Load progress ==
    done_keys_path = progress_path("done_keys.txt")
    done_keys = set()
    if os.path.exists(done_keys_path):
        with open(done_keys_path) as f:
            done_keys = set(line.strip() for line in f if line.strip())
        print(f"\n  ↺  Melanjutkan... {len(done_keys)} kombinasi sudah selesai.")

    # == Loop utama ==
    csv_path   = os.path.join(OUTPUT_DIR, "harga_pangan_3provinsi.csv")
    fieldnames = ["tanggal", "bulan", "tahun", "price_type", "province",
                  "category", "commodity", "price"]
    file_mode  = "a" if done_keys else "w"

    total_combos   = len(price_types) * len(provinces) * len(months)
    total_records  = 0
    processed      = 0
    skipped        = 0

    print(f"\n[STEP 4] Mulai koleksi data...")
    print(f"  Kombinasi: {len(price_types)} pasar × {len(provinces)} provinsi × {len(months)} bulan = {total_combos} request")
    print(f"  Estimasi waktu: ~{int(total_combos * DELAY_BETWEEN_REQUESTS / 60)} menit")
    print()

    try:
        with open(csv_path, file_mode, newline="", encoding="utf-8-sig") as csv_file, \
             open(done_keys_path, "a") as kf:

            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            if file_mode == "w":
                writer.writeheader()

            for pt in price_types:
                pt_id   = pt["price_type_id"]
                pt_name = pt["price_type_name"]

                for prov in provinces:
                    prov_id   = prov["province_id"]
                    prov_name = prov["province_name"]

                    for month_dt in months:
                        year_month = month_dt.strftime("%Y-%m")
                        combo_key  = f"{pt_id}|{prov_id}|{year_month}"

                        if combo_key in done_keys:
                            skipped   += 1
                            processed += 1
                            continue

                        # Log progres setiap 5 request
                        if processed % 5 == 0:
                            pct = processed / total_combos * 100 if total_combos else 0
                            print(f"  [{processed:>4}/{total_combos}] {pct:5.1f}% | "
                                  f"Records: {total_records:>6,} | "
                                  f"{pt_name[:14]:14s} | {prov_name[:20]:20s} | {year_month}")

                        first_day = month_dt.strftime("%Y-%m-%d")
                        last_day  = (month_dt + relativedelta(months=1) - relativedelta(days=1)).strftime("%Y-%m-%d")

                        raw  = get_price_data(pt_id, prov_id, first_day, last_day)
                        rows = parse_rows(raw, pt_name, prov_name, year_month)

                        if rows:
                            writer.writerows(rows)
                            csv_file.flush()
                            total_records += len(rows)

                        done_keys.add(combo_key)
                        kf.write(combo_key + "\n")
                        kf.flush()
                        processed += 1
                        time.sleep(DELAY_BETWEEN_REQUESTS)

    except KeyboardInterrupt:
        print("\n\n  ⚠ DIHENTIKAN oleh pengguna. Jalankan ulang untuk melanjutkan.")

    # == Konversi CSV → JSON ==
    print(f"\n✓ Pengumpulan selesai! {total_records:,} records baru ditambahkan.")
    print("  Membuat file JSON...")

    json_path  = os.path.join(OUTPUT_DIR, "harga_pangan_3provinsi.json")
    total_rows = 0
    with open(csv_path, encoding="utf-8-sig", newline="") as fc, \
         open(json_path, "w", encoding="utf-8") as fj:
        reader = csv.DictReader(fc)
        fj.write("[\n")
        first = True
        for row in reader:
            row["price"] = float(row["price"]) if row.get("price") else None
            if not first:
                fj.write(",\n")
            fj.write(json.dumps(row, ensure_ascii=False))
            first      = False
            total_rows += 1
        fj.write("\n]")

    csv_mb  = os.path.getsize(csv_path)  / 1_048_576
    json_mb = os.path.getsize(json_path) / 1_048_576

    print()
    print("═" * 66)
    print("  HASIL AKHIR")
    print("═" * 66)
    print(f"  Total records  : {total_rows:,}")
    print(f"  Periode        : {START_DATE.strftime('%b %Y')} - {END_DATE.strftime('%b %Y')}")
    print(f"  Provinsi       : {', '.join(found_names)}")
    print(f"  Jenis pasar    : {len(price_types)}")
    print()
    print(f"  📄 CSV  [{csv_mb:.1f} MB] : {csv_path}")
    print(f"  📄 JSON [{json_mb:.1f} MB] : {json_path}")
    print()
    print("  Kolom dataset:")
    print("    tanggal    — Hari (DD)")
    print("    bulan      — Bulan (MM)")
    print("    tahun      — Tahun (YYYY)")
    print("    price_type — Jenis pasar")
    print("    province   — Provinsi")
    print("    category   — Kategori komoditas (Beras, Daging, dll)")
    print("    commodity  — Nama komoditas spesifik")
    print("    price      — Harga (Rupiah/kg)")
    print("═" * 66)


if __name__ == "__main__":
    from dateutil.relativedelta import relativedelta
    main()
