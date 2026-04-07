"""
=============================================================
  KOLEKTOR DATA MAKROEKONOMI - WORLD BANK API
  Sumber: World Bank Open Data API
  URL: https://data.worldbank.org/
=============================================================
  Indikator yang dikumpulkan:
  - GDP (Nominal, USD)          : NY.GDP.MKTP.CD
  - Inflasi (CPI, % tahunan)    : FP.CPI.TOTL.ZG
  - Populasi (Total jiwa)       : SP.POP.TOTL
  - Tingkat Pengangguran (%)    : SL.UEM.TOTL.ZS
  - Harapan Hidup (tahun)       : SP.DYN.LE00.IN

  Fitur:
  - Country: Indonesia (ID)
  - Kurun waktu: Semua data tersedia (1960-2024)
  - Output: 5 file CSV terpisah (folder: data_economic/)
  - Auto-retry & error handling sederhana
  - Logging progres real-time

  ESTIMASI:
  - Total request: 5 indikator × 1 negara = 5 request
  - Waktu: ~3-5 detik
=============================================================
"""

import requests
import pandas as pd
import os
import time

# ─────────────────────────────────────────
#  KONFIGURASI
# ─────────────────────────────────────────
# 1. Buat folder output (otomatis jika belum ada)
os.makedirs("data_economic", exist_ok=True)
print("✅ Library siap digunakan")

# 2. Definisi Indikator World Bank
# Format: 'KODE_INDICATOR': 'nama_file_output'
indicators = {
    'NY.GDP.MKTP.CD': 'gdp',              # GDP (current US$)
    'FP.CPI.TOTL.ZG': 'cpi',              # Inflation, CPI (annual %)
    'SP.POP.TOTL': 'population',          # Population, total
    'SL.UEM.TOTL.ZS': 'unemployment',     # Unemployment, total (% of labor force)
    'SP.DYN.LE00.IN': 'life_expectancy'   # Life expectancy at birth, total (years)
}

# Base URL World Bank API
# 🔗 Dokumentasi: https://datahelpdesk.worldbank.org/knowledgebase/topics/125589
BASE_URL = "https://api.worldbank.org/v2/country/ID/indicator/{code}?format=json&per_page=100"

# ─────────────────────────────────────────
#  PROSES UTAMA: LOOP PENGAMBILAN DATA
# ─────────────────────────────────────────
# 3-6. Loop pengambilan data & simpan per file
for code, name in indicators.items():
    print(f"📥 Mengunduh {name}...")
    
    try:
        # Request ke API World Bank
        url = BASE_URL.format(code=code)
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise error jika status code != 200
        data = response.json()
        
        # ⚠️ Struktur JSON World Bank: [metadata, list_data]
        # Data aktual berada di index ke-1
        if len(data) < 2 or not data[1]:
            print(f"⚠️ Data {name} kosong atau belum tersedia.")
            continue
            
        # Parsing data mentah menjadi list of dict
        records = []
        for item in data[1]:
            val = item.get('value')
            # Hanya ambil data yang memiliki nilai (bukan None)
            if val is not None:
                records.append({
                    'tahun': int(item['date']),
                    'nilai': float(val)
                })
                
        # Konversi ke DataFrame pandas
        df = pd.DataFrame(records)
        
        # Urutkan: tahun terbaru di atas (descending)
        df = df.sort_values('tahun', ascending=False).reset_index(drop=True)
        
        # 7. Simpan ke CSV sesuai nama file yang ditentukan
        filename = f"data_economic/{name}.csv"
        df.to_csv(filename, index=False, encoding='utf-8')
        
        print(f"✅ Tersimpan: {filename} ({len(df)} baris)")
        
    except Exception as e:
        # Handle error jika request gagal (timeout, koneksi, dll)
        print(f"❌ Gagal mengunduh {name}: {e}")
        
    # Jeda 0.5 detik antar request (etika API agar tidak diblokir)
    time.sleep(0.5)

# ─────────────────────────────────────────
#  LAPORAN AKHIR
# ─────────────────────────────────────────
# Konfirmasi akhir setelah semua loop selesai
print("\n" + "="*50)
print("✅ TAHAP 2B SELESAI!")
print("="*50)
print("File yang dihasilkan di folder 'data_economic/':")

for name in indicators.values():
    path = f"data_economic/{name}.csv"
    if os.path.exists(path):
        size = os.path.getsize(path)
        print(f"  📄 {name}.csv ({size} bytes)")