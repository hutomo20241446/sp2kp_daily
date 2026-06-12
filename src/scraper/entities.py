# src/scraper/entities.py
from dataclasses import dataclass
from datetime import date


@dataclass
class HargaHarian:
    """Satu baris data harga harian dari SP2KP (sudah bersih, siap upsert)."""
    provinsi:       str
    kabupaten_kota: str
    komoditas:      str
    unit:           str
    tanggal:        date    # sudah dikonversi ke date Python
    harga:          float | None

    def key(self) -> tuple:
        """Unique key untuk deduplication."""
        return (self.kabupaten_kota, self.komoditas, self.tanggal)
