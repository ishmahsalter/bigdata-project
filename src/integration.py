import pandas as pd
import os

CLEANED_DIR = 'data/cleaned'
FINAL_DIR = 'data/final'

os.makedirs(FINAL_DIR, exist_ok=True)

def integrate_all_data():
    print("--- Memulai Proses Integrasi Data ---")
 
    cleaned_files = [f for f in os.listdir(CLEANED_DIR) if f.startswith('cleaned_') and f.endswith('.csv')]
    
    if not cleaned_files:
        print("Error: Tidak ada file di folder 'data/cleaned'. Jalankan 'data_cleaning.py' terlebih dahulu.")
        return

    base_file_path = os.path.join(CLEANED_DIR, cleaned_files[0])
    df_final = pd.read_csv(base_file_path)
    print(f"Menggunakan {cleaned_files[0]} sebagai basis.")

    for file_name in cleaned_files[1:]:
        print(f"Menggabungkan {file_name}...")
        file_path = os.path.join(CLEANED_DIR, file_name)
        df_temp = pd.read_csv(file_path)
        

        df_final = pd.merge(df_final, df_temp, on='tahun', how='outer')

    df_final = df_final.sort_values(by='tahun', ascending=False).reset_index(drop=True)

    output_path = os.path.join(FINAL_DIR, 'final_dataset.csv')
    df_final.to_csv(output_path, index=False)
    
    print("-" * 40)
    print(f"BERHASIL! Data terintegrasi disimpan di: {output_path}")
    print(f"Total Kolom: {df_final.shape[1]}")
    print(f"Total Baris: {df_final.shape[0]}")
    print("\nPreview 5 data teratas:")
    print(df_final.head())

if __name__ == "__main__":
    integrate_all_data()