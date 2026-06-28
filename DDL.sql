-- =========================================================
-- DIM_WILAYAH
-- =========================================================

CREATE TABLE dim_wilayah (
    wilayah_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    provinsi VARCHAR(100) NOT NULL,
    kabupaten_kota VARCHAR(100) NOT NULL,

    CONSTRAINT uq_wilayah
    UNIQUE (provinsi, kabupaten_kota)
);

-- =========================================================
-- DIM_KOMODITAS
-- =========================================================

CREATE TABLE dim_komoditas (
    komoditas_key INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    komoditas VARCHAR(150) NOT NULL,
    unit VARCHAR(10) NOT NULL,

    CONSTRAINT uq_komoditas
    UNIQUE (komoditas, unit)
);

-- =========================================================
-- FACT_HARGA_HARIAN
-- =========================================================

CREATE TABLE fact_harga_harian (
    tanggal DATE NOT NULL,

    wilayah_key INTEGER NOT NULL,
    komoditas_key INTEGER NOT NULL,

    harga NUMERIC(12,2),

    CONSTRAINT pk_fact_harga_harian
    PRIMARY KEY (
        tanggal,
        wilayah_key,
        komoditas_key
    ),

    CONSTRAINT fk_fact_wilayah
    FOREIGN KEY (wilayah_key)
    REFERENCES dim_wilayah(wilayah_key),

    CONSTRAINT fk_fact_komoditas
    FOREIGN KEY (komoditas_key)
    REFERENCES dim_komoditas(komoditas_key)

)
-- PARTITION BY RANGE (tanggal);

