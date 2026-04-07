
# 📊 PROYEK 1 BIG DATA - COLLECTING DATA

**Repository:** https://github.com/ishmahsalter/bigdata-project

---

## 📋 DESKRIPSI PROYEK

Proyek ini bertujuan untuk mengumpulkan data harga pangan dari 34 provinsi di Indonesia (sumber PIHPS) dan data makro ekonomi Indonesia (sumber World Bank API). Data yang terkumpul akan diproses menggunakan PySpark untuk analisis lebih lanjut.

---

## 👥 ANGGOTA KELOMPOK

| No | Nama | NIM | Tugas |
|----|------|-----|-------|
| 1 | Hilmy Affayyad Akbar | H071241013 | Project Setup & Error Handler |
| 2 | Muhammad Daffa Usman | H071241014 | Data Cleaning & Standardization |
| 3 | Ishmah Nurwasilah | H071241019 | Code Reviewer, Merger & Documentation |
| 4 | Andi Sophie Banuna Amrie | H071241022 | Testing & Quality Assurance |
| 5 | Dalvyn Suhada | H071241035 | Food Price Data Collector (PIHPS) |
| 6 | Zalfa Syauqiyah Hamka | H071241041 | Economic Data Collector (World Bank) |
| 7 | Nur Atika Binti Ardi | H071241049 | Data Integration & Feature Engineer |

---

## 📁 STRUKTUR FOLDER

```
bigdata-project/
│
├── src/                          # Tempat script Python utama
│   ├── clean_data.py            # → Daffa
│   ├── collect_worldbank.py     # → Zalfa
│   ├── integration.py           # → Nur Atika
│   └── main_pipeline.py         # → Ishmah (merge semua)
│
├── data/
│   ├── raw/                      # → Dalvyn (upload data PIHPS)
│   ├── cleaned/                  # → Daffa (output)
│   └── final/                    # → Nur Atika (output)
│
├── utils/                        # → Hilmy
│   └── error_handler.py
│
├── logs/                         # → Sophie
│   └── test_reports.log
│
├── notebooks/                    # Jupyter notebooks (opsional)
├── tests/                        # Unit tests (opsional)
├── docs/                         # Dokumentasi
├── requirements.txt              # Dependencies
├── .gitignore                    # File yang diabaikan Git
└── README.md                     # Dokumentasi proyek
```

---

## 🛠️ LANGKAH PERSIAPAN (SETUP)

Ikuti langkah-langkah di bawah ini untuk memulai kolaborasi di lokal komputer masing-masing:

### 1. Clone Repository

Buka terminal/CMD, lalu ketik:

```bash
git clone https://github.com/ishmahsalter/bigdata-project.git
```

### 2. Masuk ke Folder Proyek

```bash
cd bigdata-project
```

### 3. Buat Virtual Environment (Opsional tapi Direkomendasikan)

```bash
python -m venv .venv
```

**Aktifkan virtual environment:**
- **Windows:** `.venv\Scripts\activate`
- **Mac/Linux:** `source .venv/bin/activate`

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 📋 PEMBAGIAN TUGAS ANGGOTA

| Nama | Target Folder | File Output | Deskripsi Tugas |
|------|---------------|-------------|-----------------|
| **Hilmy** | `utils/` | `error_handler.py` | Membuat sistem penanganan error agar program tidak crash |
| **Daffa** | `src/` | `clean_data.py` | Melakukan cleaning data (hapus missing value, normalisasi, dll) |
| **Zalfa** | `src/` | `collect_worldbank.py` | Membuat script pengumpul data dari API World Bank |
| **Dalvyn** | `data/raw/` | `harga_pangan_lenkgap.csv` | Mengunggah data PIHPS yang sudah dikumpulkan |
| **Ishmah** | `src/` | `main_pipeline.py` | Review kode dari teman-teman dan melakukan merge ke main |
| **Sophie** | `logs/` | `test_reports.log` | Menjalankan testing program dan Quality Assurance (QA) |
| **Nur Atika** | `src/` & `data/final/` | `integration.py` | Integrasi seluruh script dan pembuatan fitur baru |

---

## 🚀 WORKFLOW KERJA (GIT)

Agar kode tidak berantakan, pastikan selalu menggunakan urutan ini:

### 1. Update Lokal (Sebelum Mulai Koding)

```bash
git pull origin main
```

### 2. Simpan Pekerjaan

```bash
git add .
git commit -m "Deskripsi apa yang kamu kerjakan"
git push origin main
```

### 3. Contoh Pesan Commit yang Baik

```bash
git commit -m "Add error handler utility"
git commit -m "Fix missing value handling in clean_data"
git commit -m "Update README with setup instructions"
```

---

## 📦 HASIL DATASET

| Dataset | Jumlah Records | Periode | Provinsi | Format |
|---------|----------------|---------|----------|--------|
| PIHPS Food Price | 3.097.451 | Apr 2021 - Apr 2026 | 34 | CSV, JSON |
| World Bank Data | (belum dikumpulkan) | - | - | CSV |

**Kolom dataset PIHPS:**
- `date` - Tanggal harga (DD/MM/YYYY)
- `year_month` - Tahun & bulan (YYYY-MM)
- `price_type` - Jenis pasar
- `province` - Provinsi
- `category` - Kategori komoditas (Beras, Daging, dll)
- `commodity` - Nama komoditas spesifik
- `price` - Harga (Rupiah/kg)

---

## ⚠️ CATATAN PENTING

1. **Selalu `git pull` sebelum mulai koding** untuk menghindari konflik
2. **Test kode di lokal** sebelum di-push ke GitHub
3. **Gunakan pesan commit yang jelas** (jangan cuma "update")
4. **Jangan push file besar (>100MB)** ke GitHub (gunakan Git LFS atau Google Drive)
5. **Jika ada konflik**, selesaikan di lokal sebelum push

---

## 📞 KONTAK TIM

| Nama | Peran |
|------|-------|
| Hilmy | Coordinator |
| Ishmah | Documentation & Code Reviewer |


*Last Updated: April 2026*
```

**Selesai!** 😊🔥
