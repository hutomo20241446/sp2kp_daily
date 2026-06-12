# src/transform/transformer.py
from src.scraper.entities import HargaHarian
from src.config.logger import logger

# Normalisasi satuan — sesuai pipeline lama
UNIT_MAP = {
    "kg":  "kilogram",
    "lt":  "liter",
    "Kg":  "kilogram",
    "Lt":  "liter",
    "KG":  "kilogram",
    "LT":  "liter",
}


def transform(records: list[HargaHarian]) -> list[HargaHarian]:
    """
    Bersihkan dan deduplikasi kumpulan HargaHarian.

    Langkah:
    1. Strip whitespace pada field string
    2. Normalisasi unit (kg→kilogram, lt→liter)
    3. Deduplikasi berdasarkan (kabupaten_kota, komoditas, tanggal)

    Mengembalikan list bersih, siap untuk upsert.
    """
    cleaned  : list[HargaHarian] = []
    seen_keys: set = set()
    dup_count = 0

    for r in records:
        # -- 1. Strip --
        r.provinsi       = r.provinsi.strip()
        r.kabupaten_kota = r.kabupaten_kota.strip()
        r.komoditas      = r.komoditas.strip()
        r.unit           = r.unit.strip()

        # -- 2. Normalisasi unit --
        r.unit = UNIT_MAP.get(r.unit, r.unit.lower())

        # -- 3. Deduplikasi --
        key = r.key()
        if key in seen_keys:
            dup_count += 1
            continue
        seen_keys.add(key)
        cleaned.append(r)

    if dup_count:
        logger.warning(f"Transform: {dup_count} duplikat dibuang dari {len(records)} record")

    logger.info(f"Transform selesai: {len(cleaned)} record bersih")
    return cleaned
