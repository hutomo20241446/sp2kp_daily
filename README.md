# SP2KP Daily ETL

Pipeline ETL otomatis untuk mengambil data **harga pangan harian** dari website **SP2KP Kementerian Perdagangan RI**, melakukan transformasi data, kemudian melakukan **upsert** ke database PostgreSQL (Supabase) menggunakan pendekatan dimensional modeling (Star Schema).

---

# Arsitektur Pipeline

```
              SP2KP Website
                     │
                     ▼
             Playwright Scraper
                     │
                     ▼
          Data Cleaning & Transform
                     │
                     ▼
      Deduplication & Unit Normalization
                     │
                     ▼
        Upsert ke PostgreSQL (Supabase)
                     │
                     ▼
      Star Schema Data Warehouse
```

---

# Tahapan Program

Program dijalankan sebagai pipeline ETL yang terdiri dari beberapa tahap.

## 1. Inisialisasi Konfigurasi

Program memuat konfigurasi dari environment variable, meliputi:

- target tanggal scraping
- provinsi
- jumlah worker
- mode headless browser
- retry maksimum
- koneksi database PostgreSQL

Konfigurasi disimpan pada:

```
src/config/settings.py
```

---

## 2. Validasi Kelengkapan Data Database

Sebelum melakukan scraping, program memeriksa apakah data pada tanggal target sudah lengkap.

Proses ini menghitung jumlah record yang memiliki harga (non NULL) pada tabel:

```
fact_harga_harian
```

Jika jumlah data telah mencapai threshold:

```
35 kabupaten/kota × 17 komoditas
=
595 record
```

maka scraping dihentikan karena data sudah lengkap.

Jika belum lengkap, program hanya akan melakukan scraping ulang pada kabupaten/kota yang masih belum memiliki data.

---

## 3. Membuka Browser

Program menggunakan **Playwright Chromium**.

Fitur browser:

- headless mode
- reusable browser
- reusable page per worker
- custom User Agent
- disable sandbox
- disable dev-shm

File:

```
src/scraper/browser_factory.py
```

---

## 4. Mengambil Daftar Kabupaten/Kota

Program mengambil daftar kabupaten secara dinamis dari dropdown website SP2KP.

Jika proses gagal, maka digunakan daftar fallback yang telah disediakan pada source code.

File:

```
src/scraper/sp2kp_scraper.py
```

---

## 5. Parallel Scraping

Program menggunakan asynchronous worker.

Setiap worker memiliki:

- browser tab sendiri
- page sendiri
- session sendiri

Masing-masing worker akan mengambil beberapa kabupaten secara paralel.

---

## 6. Scraping Harga Harian

Untuk setiap kabupaten:

1. memilih kabupaten
2. memilih tanggal
3. memuat tabel harga
4. membaca seluruh komoditas
5. mengubah harga menjadi angka
6. membuat object `HargaHarian`

Object yang dihasilkan:

```python
HargaHarian(
    provinsi,
    kabupaten_kota,
    komoditas,
    unit,
    tanggal,
    harga
)
```

---

## 7. Data Cleaning

Tahap transform melakukan:

### Strip whitespace

Contoh

```
"  Beras Premium "
↓

"Beras Premium"
```

---

### Normalisasi satuan

Contoh

```
kg
Kg
KG
↓

kilogram
```

```
lt
LT
↓

liter
```

---

### Deduplikasi

Key deduplikasi:

```
(
kabupaten,
komoditas,
tanggal
)
```

Jika terdapat data ganda maka hanya satu record yang dipertahankan.

---

## 8. Upsert Database

Program menggunakan strategi Upsert.

Tahapan:

### Upsert dimensi wilayah

```
dim_wilayah
```

---

### Upsert dimensi komoditas

```
dim_komoditas
```

---

### Mengambil surrogate key

Program melakukan lookup terhadap:

- wilayah_key

- komoditas_key

---

### Upsert Fact Table

Record kemudian dimasukkan ke:

```
fact_harga_harian
```

Menggunakan:

```
INSERT ...

ON CONFLICT DO UPDATE
```

Sehingga data lama akan diperbarui tanpa menghasilkan duplikasi.

---

## 9. Logging

Seluruh proses menghasilkan log:

- status worker
- jumlah data
- retry
- duplicate
- progress scraping
- progress upsert
- status database

---

# Struktur Project

```
src
│
├── config
│   ├── logger.py
│   └── settings.py
│
├── scraper
│   ├── browser_factory.py
│   ├── page_session.py
│   ├── entities.py
│   └── sp2kp_scraper.py
│
├── transform
│   └── transformer.py
│
├── load
│   └── supabase_loader.py
│
├── pipeline.py
└── main.py
```

## Logging

Logging mencakup:

- progress ETL
- progress worker
- jumlah record
- duplicate record
- status database
- kelengkapan data

---

# Teknologi yang Digunakan

- Python
- Playwright
- Asyncio
- PostgreSQL
- Supabase
- Psycopg3
- GitHub Actions
- Docker

---

# Database Model

Pipeline dirancang untuk data warehouse sederhana menggunakan Star Schema.

```
               dim_wilayah
                     │
                     │
                     ▼
             fact_harga_harian
                     ▲
                     │
                     │
             dim_komoditas
```

---

# Database Model

Pipeline menggunakan **Star Schema** yang terdiri dari dua tabel dimensi dan satu tabel fakta.

```
                  dim_wilayah
                ┌───────────────┐
                │ wilayah_key PK│
                │ provinsi      │
                │ kabupaten_kota│
                └───────┬───────┘
                        │
                        ▼
             fact_harga_harian
      ┌─────────────────────────────┐
      │ tanggal (PK)                │
      │ wilayah_key (PK, FK)        │
      │ komoditas_key (PK, FK)      │
      │ harga                       │
      └─────────────────────────────┘
                      ▲
                      │
                      │
                dim_komoditas
             ┌──────────────────┐
             │ komoditas_key PK │
             │ komoditas        │
             │ unit             │
             └──────────────────┘
```

## dim_wilayah

| Field | Tipe Data |
|-------|-----------|
| wilayah_key | INTEGER (PK) |
| provinsi | VARCHAR(100) |
| kabupaten_kota | VARCHAR(100) |

**Unique Key:** `(provinsi, kabupaten_kota)`

---

## dim_komoditas

| Field | Tipe Data |
|-------|-----------|
| komoditas_key | INTEGER (PK) |
| komoditas | VARCHAR(150) |
| unit | VARCHAR(10) |

**Unique Key:** `(komoditas, unit)`

---

## fact_harga_harian

| Field | Tipe Data |
|-------|-----------|
| tanggal | DATE (PK) |
| wilayah_key | INTEGER (PK, FK → dim_wilayah) |
| komoditas_key | INTEGER (PK, FK → dim_komoditas) |
| harga | NUMERIC(12,2) |

**Primary Key:** `(tanggal, wilayah_key, komoditas_key)`

Setiap record merepresentasikan **harga satu komoditas pada satu wilayah untuk satu tanggal tertentu**.

---

# Fitur

- Parallel scraping menggunakan Playwright Async.
- Mendukung incremental ETL.
- Idempotent melalui mekanisme Upsert.
- Menggunakan Star Schema sehingga siap untuk analisis dan Business Intelligence.
- Memiliki validasi kelengkapan data sebelum scraping.
- Dapat dijalankan secara otomatis menggunakan GitHub Actions maupun Docker.
