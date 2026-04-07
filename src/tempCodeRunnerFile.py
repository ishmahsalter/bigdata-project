import pandas as pd
import os

# Konfigurasi Folder
RAW_DIR = 'data/raw'
CLEANED_DIR = 'data/cleaned'
os.makedirs(CLEANED_DIR, exist_ok=True)

def clean_standard_data(file_name, indicator_name):
    """Fungsi untuk membersihkan data standar (CPI, GDP, dll)"""
    path = os.path.join(RAW_DIR, file_name)
    if not os.path.exists(path):
        return
    
    print(f"Cleaning: {file_name}...")
    df = pd.read_csv(path)
    
    # 1. Handling Anomali: Pastikan kolom tahun dan nilai adalah angka
    df['tahun'] = pd.to_numeric(df['tahun'], errors='coerce')
    df['nilai'] = pd.to_numeric(df['nilai'], errors='coerce')
    
    # 2. Hapus data yang kosong (NaN) setelah konversi
    df = df.dropna(subset=['tahun', 'nilai'])
    
    # 3. Rename kolom 'nilai' agar spesifik
    df = df.rename(columns={'nilai': f'nilai_{indicator_name}'})
    
    # 4. Pastikan tahun adalah integer (bukan float)
    df['tahun'] = df['tahun'].astype(int)
    
    # Simpan hasil
    output_path = os.path.join(CLEANED_DIR, f'cleaned_{file_name}')
    df.to_csv(output_path, index=False)
    print(f"Berhasil menyimpan ke: {output_path}")

def clean_food_price_data():
    """Fungsi khusus untuk harga pangan (Agregasi Rata-rata)"""
    file_name = 'harga_pangan_3provinsi.csv'
    path = os.path.join(RAW_DIR, file_name)
    
    if not os.path.exists(path):
        print(f"File {file_name} tidak ditemukan.")
        return

    print(f"Cleaning & Aggregating: {file_name}...")
    df = pd.read_csv(path)
    
    # DEBUG: Mari kita lihat nama kolom aslinya jika error lagi
    # print(df.columns) 

    # Solusi: Paksa kolom kedua menjadi 'nilai' jika namanya bukan 'nilai'
    # Asumsinya formatnya: [provinsi, tahun, nilai] atau [tahun, provinsi, nilai]
    if 'nilai' not in df.columns:
        # Kita ambil kolom terakhir sebagai kolom nilai (biasanya harga ada di akhir)
        col_nilai = df.columns[-1]
        df = df.rename(columns={col_nilai: 'nilai'})
        print(f"Kolom '{col_nilai}' dideteksi sebagai kolom nilai.")

    # Bersihkan anomali tipe data
    df['tahun'] = pd.to_numeric(df['tahun'], errors='coerce')
    df['nilai'] = pd.to_numeric(df['nilai'], errors='coerce')
    df = df.dropna(subset=['tahun', 'nilai'])

    # Agregasi: Ambil rata-rata harga pangan per tahun
    df_avg = df.groupby('tahun')['nilai'].mean().reset_index()
    
    # Rename kolom hasil akhir
    df_avg = df_avg.rename(columns={'nilai': 'nilai_harga_pangan'})
    df_avg['tahun'] = df_avg['tahun'].astype(int)
    
    output_path = os.path.join(CLEANED_DIR, f'cleaned_{file_name}')
    df_avg.to_csv(output_path, index=False)
    print(f"Berhasil agregasi harga pangan ke: {output_path}")

if __name__ == "__main__":
    # List file standar
    files = {
        'cpi.csv': 'cpi',
        'gdp.csv': 'gdp',
        'life_expectancy.csv': 'life_expectancy',
        'population.csv': 'population',
        'unemployment.csv': 'unemployment'
    }
    
    # Jalankan proses pembersihan
    for file, name in files.items():
        clean_standard_data(file, name)
        
    clean_food_price_data()
    print("\n[DONE] Semua data di folder RAW telah dibersihkan dan dipindah ke folder CLEANED.")