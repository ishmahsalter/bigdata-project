import pandas as pd
import os
import sys

# Menambahkan root directory ke sys.path agar bisa mengimpor utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.error_handler import get_logger

# Inisialisasi logger
logger = get_logger("Integration_Tahap6")

# Konfigurasi Path
DATA_CLEANED_DIR = 'data/cleaned/'
DATA_FINAL_DIR = 'data/final/'

def load_data():
    """Langkah 1 & 2: Load Data Bersih dan Data World Bank"""
    try:
        print("Memuat data...")
        # Load harga pangan (Pastikan sudah ada kolom 'date' dari Daffa)
        df_pangan = pd.read_csv(os.path.join(DATA_CLEANED_DIR, 'cleaned_harga_pangan.csv'))
        
        # Load data ekonomi World Bank & RENAME kolom 'nilai' agar tidak bentrok
        df_gdp = pd.read_csv(os.path.join(DATA_CLEANED_DIR, 'cleaned_gdp.csv')).rename(columns={'nilai': 'gdp_value'})
        df_cpi = pd.read_csv(os.path.join(DATA_CLEANED_DIR, 'cleaned_cpi.csv')).rename(columns={'nilai': 'cpi_value'})
        df_pop = pd.read_csv(os.path.join(DATA_CLEANED_DIR, 'cleaned_population.csv')).rename(columns={'nilai': 'pop_value'})
        df_unemp = pd.read_csv(os.path.join(DATA_CLEANED_DIR, 'cleaned_unemployment.csv')).rename(columns={'nilai': 'unemp_value'})
        df_life = pd.read_csv(os.path.join(DATA_CLEANED_DIR, 'cleaned_life_expectancy.csv')).rename(columns={'nilai': 'life_value'})
        
        return df_pangan, df_gdp, df_cpi, df_pop, df_unemp, df_life
    except Exception as e:
        logger.error(f"Gagal memuat data: {e}")
        raise

def merge_datasets(df_pangan, df_gdp, df_cpi, df_pop, df_unemp, df_life):
    """Langkah 3: Merge Dataset"""
    try:
        print("Menggabungkan data...")
        
        # Ubah string ke datetime dan ekstrak menjadi kolom 'tahun'
        df_pangan['date'] = pd.to_datetime(df_pangan['date'])
        df_pangan['tahun'] = df_pangan['date'].dt.year
        
        # Gabungkan semua data World Bank menggunakan kolom 'tahun'
        df_ekonomi = df_gdp.merge(df_cpi, on='tahun', how='left') \
                           .merge(df_pop, on='tahun', how='left') \
                           .merge(df_unemp, on='tahun', how='left') \
                           .merge(df_life, on='tahun', how='left')
                           
        # Merge data pangan dengan data ekonomi berdasarkan 'tahun'
        df_integrated = df_pangan.merge(df_ekonomi, on='tahun', how='left')
        
        df_integrated.to_csv(os.path.join(DATA_FINAL_DIR, 'data_integrated.csv'), index=False)
        return df_integrated
    except Exception as e:
        logger.error(f"Gagal menggabungkan data: {e}")
        raise

def feature_engineering(df):
    """Langkah 4: Feature Engineering"""
    try:
        print("Menerapkan feature engineering...")
        
        # 1. Rasio Harga terhadap GDP (Keterjangkauan Harga)
        if 'price' in df.columns and 'gdp_value' in df.columns:
            df['price_to_gdp_ratio'] = df['price'] / df['gdp_value']
            
        # 2. Rasio Harga terhadap Inflasi (CPI)
        if 'price' in df.columns and 'cpi_value' in df.columns:
            df['price_to_cpi_ratio'] = df['price'] / df['cpi_value']
            
        # 3. Menambahkan fitur Bulan untuk analisis musiman
        df['month'] = df['date'].dt.month
        
        df.to_csv(os.path.join(DATA_FINAL_DIR, 'data_with_features.csv'), index=False)
        return df
    except Exception as e:
        logger.error(f"Gagal melakukan feature engineering: {e}")
        raise

def export_final_dataset(df):
    """Langkah 5: Export Final Dataset"""
    try:
        print("Menyimpan final dataset...")
        output_path = os.path.join(DATA_FINAL_DIR, 'final_dataset.csv')
        df.to_csv(output_path, index=False)
        print(f"Selesai! Data final berhasil disimpan di: {output_path}")
    except Exception as e:
        logger.error(f"Gagal menyimpan data final: {e}")
        raise

if __name__ == "__main__":
    os.makedirs(DATA_FINAL_DIR, exist_ok=True)
    
    logger.info("Memulai proses integrasi Tahap 6...")
    df_pangan, df_gdp, df_cpi, df_pop, df_unemp, df_life = load_data()
    df_integrated = merge_datasets(df_pangan, df_gdp, df_cpi, df_pop, df_unemp, df_life)
    df_features = feature_engineering(df_integrated)
    export_final_dataset(df_features)
    logger.info("Proses integrasi Tahap 6 selesai.")