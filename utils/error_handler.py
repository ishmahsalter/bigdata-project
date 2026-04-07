import logging
import os

# 1. Dapatkan lokasi folder utama proyek secara otomatis
# __file__ adalah lokasi utils/error_handler.py
# os.path.dirname dua kali akan membawa kita naik ke folder 'bigdata-project'
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 2. Tentukan lokasi folder logs
LOG_DIR = os.path.join(BASE_DIR, 'logs')
LOG_FILE = os.path.join(LOG_DIR, 'project_error.log')

# Pastikan folder logs tersedia
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Konfigurasi logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_logger(module_name):
    return logging.getLogger(module_name)

# Contoh penggunaan (bisa dihapus nanti saat digabung)
if __name__ == "__main__":
    logger = get_logger("Setup_Test")
    logger.info("Sistem logging berhasil diinisialisasi dengan path otomatis.")