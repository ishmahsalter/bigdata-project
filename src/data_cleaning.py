import pandas as pd
import os

def clean_pihps_data():
    print("--- 1. Memulai Cleaning Data PIHPS (Dalvyn) ---")
    # Perbaikan path: Langsung ke folder data karena kita jalankan dari root
    file_path = 'data/raw/harga_pangan_3provinsi.csv'
    
    if not os.path.exists(file_path):
        print(f"❌ File {file_path} tidak ditemukan. Lewati cleaning PIHPS.\n")
        return

    try:
        df = pd.read_csv(file_path)
        
        # 1. Handle missing values
        df = df.dropna(subset=['price'])
        
        # 2. Hapus duplikasi
        df = df.drop_duplicates()

        # 3. Standarisasi tanggal
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
        
        # 4 & 5. Standarisasi provinsi & komoditas
        if 'province' in df.columns:
            df['province'] = df['province'].str.title().str.strip()
        if 'commodity' in df.columns:
            df['commodity'] = df['commodity'].str.title().str.strip()

        # 6. Normalisasi kolom
        kolom_target = ['date', 'province', 'price']
        kolom_tersedia = [kol for kol in kolom_target if kol in df.columns]
        df = df[kolom_tersedia]

        # 7. Simpan data bersih
        output_dir = 'data/cleaned'
        os.makedirs(output_dir, exist_ok=True)
        output_path = f'{output_dir}/cleaned_harga_pangan.csv'
        df.to_csv(output_path, index=False)
        print(f"✅ Data PIHPS berhasil dibersihkan dan disimpan di: {output_path}\n")
    except Exception as e:
        print(f"❌ Error saat memproses data PIHPS: {e}\n")

def clean_worldbank_data():
    print("--- 2. Memulai Cleaning Data World Bank (Zalfa) ---")
    wb_files = ['cpi.csv', 'gdp.csv', 'life_expectancy.csv', 'population.csv', 'unemployment.csv']
    output_dir = 'data/cleaned'
    os.makedirs(output_dir, exist_ok=True)

    for file in wb_files:
        file_path = f'data/raw/{file}'
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                df = df.dropna() # Hapus baris kosong
                df = df.drop_duplicates() # Hapus duplikat
                
                output_path = f'{output_dir}/cleaned_{file}'
                df.to_csv(output_path, index=False)
                print(f"✅ Data {file} berhasil dibersihkan.")
            except Exception as e:
                print(f"❌ Error saat memproses {file}: {e}")
        else:
            print(f"⚠️ File {file} belum ada, dilewati.")
    print("\nProses cleaning data World Bank selesai!")

if __name__ == "__main__":
    print("=== MEMULAI PIPELINE DATA CLEANING (TAHAP 3) ===\n")
    clean_pihps_data()
    clean_worldbank_data()
    print("\n=== SEMUA DATA BERHASIL DIBERSIHKAN ===")