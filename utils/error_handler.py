import logging
import os

# Pastikan folder logs tersedia
if not os.path.exists('../logs'):
    os.makedirs('../logs')

# Konfigurasi logging
logging.basicConfig(
    filename='../logs/project_error.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_logger(module_name):
    return logging.getLogger(module_name)

# Contoh penggunaan (bisa dihapus nanti saat digabung)
if __name__ == "__main__":
    logger = get_logger("Setup_Test")
    logger.info("Sistem logging berhasil diinisialisasi.")
    # logger.error("Ini contoh jika terjadi error.")