import pandas as pd
import pytest

# ============================================================
# FUNGSI-FUNGSI YANG AKAN DI-TEST
# ============================================================

def cek_kolom(df, expected_columns):
    return all(col in df.columns for col in expected_columns)

def cek_missing_values(df):
    return df.isnull().sum().sum()

def cek_duplikat(df):
    return df.duplicated().sum()

def cek_harga_valid(df):
    return len(df[df["price"] <= 0])

def cek_provinsi_valid(df, expected_provinces):
    return [p for p in df["province"].unique() if p not in expected_provinces]

def cek_tanggal_valid(df):
    return len(df[
        (df["tanggal"] < 1) | (df["tanggal"] > 31) |
        (df["bulan"] < 1) | (df["bulan"] > 12) |
        (df["tahun"] < 2000) | (df["tahun"] > 2026)
    ])

# ============================================================
# DATA DUMMY UNTUK UNIT TEST
# ============================================================

@pytest.fixture
def sample_df():
    data = {
        "tanggal": [2, 3, 4],
        "bulan": [1, 1, 1],
        "tahun": [2023, 2023, 2023],
        "price_type": ["Pasar Tradisional", "Pasar Modern", "Pedagang Besar"],
        "province": ["Sumatera Utara", "DKI Jakarta", "Bali"],
        "category": ["Beras", "Minyak Goreng", "Gula Pasir"],
        "commodity": ["Beras Kualitas Bawah I", "Minyak Goreng Kemasan", "Gula Pasir Premium"],
        "price": [10450.0, 17500.0, 18650.0]
    }
    return pd.DataFrame(data)

# ============================================================
# UNIT TESTS
# ============================================================

def test_cek_kolom_lengkap(sample_df):
    expected = ["tanggal", "bulan", "tahun", "price_type", "province", "category", "commodity", "price"]
    assert cek_kolom(sample_df, expected) == True

def test_cek_kolom_kurang(sample_df):
    expected = ["tanggal", "bulan", "tahun", "kolom_tidak_ada"]
    assert cek_kolom(sample_df, expected) == False

def test_cek_missing_values_bersih(sample_df):
    assert cek_missing_values(sample_df) == 0

def test_cek_missing_values_ada(sample_df):
    sample_df.loc[0, "price"] = None
    assert cek_missing_values(sample_df) > 0

def test_cek_duplikat_bersih(sample_df):
    assert cek_duplikat(sample_df) == 0

def test_cek_duplikat_ada(sample_df):
    df_duplikat = pd.concat([sample_df, sample_df]).reset_index(drop=True)
    assert cek_duplikat(df_duplikat) > 0

def test_cek_harga_valid(sample_df):
    assert cek_harga_valid(sample_df) == 0

def test_cek_harga_invalid(sample_df):
    sample_df.loc[0, "price"] = -100
    assert cek_harga_valid(sample_df) > 0

def test_cek_provinsi_valid(sample_df):
    expected = ["DKI Jakarta", "Bali", "Sumatera Utara"]
    assert cek_provinsi_valid(sample_df, expected) == []

def test_cek_provinsi_invalid(sample_df):
    expected = ["DKI Jakarta", "Bali"]
    assert len(cek_provinsi_valid(sample_df, expected)) > 0

def test_cek_tanggal_valid(sample_df):
    assert cek_tanggal_valid(sample_df) == 0

def test_cek_tanggal_invalid(sample_df):
    sample_df.loc[0, "bulan"] = 13
    assert cek_tanggal_valid(sample_df) > 0