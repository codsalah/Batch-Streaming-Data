-- =====================================================
-- Database: SeismicDWH (PostgreSQL)
-- =====================================================

-- ================================
-- DIMENSION: TIME
-- ================================

CREATE TABLE IF NOT EXISTS dim_time (
    time_id        BIGSERIAL PRIMARY KEY,
    full_timestamp TIMESTAMP NOT NULL,
    date           DATE NOT NULL,
    year           INT NOT NULL,
    quarter        INT NOT NULL,
    month          INT NOT NULL,
    day            INT NOT NULL,
    hour           INT NOT NULL,
    day_of_week    INT NOT NULL,
    UNIQUE(full_timestamp)
);

CREATE INDEX idx_dim_time_date ON dim_time(date);


-- ================================
-- DIMENSION: LOCATION
-- ================================

CREATE TABLE IF NOT EXISTS dim_location (
    location_id BIGSERIAL PRIMARY KEY,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    country     VARCHAR(100),
    UNIQUE(latitude, longitude)
);

CREATE INDEX idx_dim_location_country ON dim_location(country);


-- ================================
-- DIMENSION: AIRPORT (SCD Type 2)
-- ================================

CREATE TABLE IF NOT EXISTS dim_airport (
    airport_id BIGSERIAL PRIMARY KEY,

    ident VARCHAR(50),
    icao  VARCHAR(50),
    iata  VARCHAR(50),
    name  VARCHAR(255),

    airport_type       VARCHAR(50),
    scheduled_service  VARCHAR(50),
    timezone           VARCHAR(100),
    municipality       VARCHAR(255),
    administrative_region VARCHAR(255),

    location_id BIGINT REFERENCES dim_location(location_id),

    valid_from TIMESTAMP NOT NULL,
    valid_to   TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_dim_airport_current ON dim_airport(is_current);


-- ================================
-- DIMENSION: STATION
-- ================================

CREATE TABLE IF NOT EXISTS dim_station (
    station_id BIGSERIAL PRIMARY KEY,

    location_id BIGINT REFERENCES dim_location(location_id),
    station_region VARCHAR(255),
    version        VARCHAR(50),
    high_precision BOOLEAN,
    private        BOOLEAN
);


-- ================================
-- DIMENSION: EARTHQUAKE CURRENT
-- ================================

CREATE TABLE IF NOT EXISTS dim_earthquake_current (
    earthquake_id BIGSERIAL PRIMARY KEY,

    source_id VARCHAR(100),
    catalog   VARCHAR(100),

    mag       DOUBLE PRECISION,
    mag_type  VARCHAR(50),
    depth     DOUBLE PRECISION,
    event_type VARCHAR(50),
    flynn_region VARCHAR(255),

    location_id BIGINT REFERENCES dim_location(location_id),
    time_id     BIGINT REFERENCES dim_time(time_id),

    last_update TIMESTAMP
);

CREATE INDEX idx_eq_current_source ON dim_earthquake_current(source_id);


-- ================================
-- DIMENSION: EARTHQUAKE HISTORY
-- ================================

CREATE TABLE IF NOT EXISTS dim_earthquake_history (
    history_id BIGSERIAL PRIMARY KEY,

    earthquake_id BIGINT,
    source_id VARCHAR(100),
    catalog   VARCHAR(100),

    mag       DOUBLE PRECISION,
    mag_type  VARCHAR(50),
    depth     DOUBLE PRECISION,
    event_type VARCHAR(50),
    flynn_region VARCHAR(255),

    location_id BIGINT REFERENCES dim_location(location_id),
    time_id     BIGINT REFERENCES dim_time(time_id),

    action VARCHAR(50),

    valid_from TIMESTAMP NOT NULL,
    valid_to   TIMESTAMP
);

CREATE INDEX idx_eq_history_eqid ON dim_earthquake_history(earthquake_id);


-- ================================
-- FACT: GROUND MOTION
-- ================================

CREATE TABLE IF NOT EXISTS fact_ground_motion (
    id BIGSERIAL PRIMARY KEY,

    location_id   BIGINT REFERENCES dim_location(location_id),
    station_id    BIGINT REFERENCES dim_station(station_id),
    earthquake_id BIGINT REFERENCES dim_earthquake_current(earthquake_id),
    time_id       BIGINT REFERENCES dim_time(time_id),

    PGA DOUBLE PRECISION,
    PGV DOUBLE PRECISION,
    PGD DOUBLE PRECISION,

    PGA_EW DOUBLE PRECISION,
    PGV_EW DOUBLE PRECISION,
    PGD_EW DOUBLE PRECISION,

    PGA_NS DOUBLE PRECISION,
    PGV_NS DOUBLE PRECISION,
    PGD_NS DOUBLE PRECISION,

    PGA_UD DOUBLE PRECISION,
    PGV_UD DOUBLE PRECISION,
    PGD_UD DOUBLE PRECISION,

    Max_PGA DOUBLE PRECISION,
    Max_PGV DOUBLE PRECISION,
    Max_PGD DOUBLE PRECISION,

    Shindo VARCHAR(50),
    Max_Shindo VARCHAR(50),
    CalcShindo DOUBLE PRECISION,
    Max_CalcShindo DOUBLE PRECISION,

    Intensity DOUBLE PRECISION,
    Max_Intensity DOUBLE PRECISION,

    LPGM DOUBLE PRECISION,
    Max_LPGM DOUBLE PRECISION,

    Sva30 DOUBLE PRECISION,
    Max_Sva30 DOUBLE PRECISION
);


-- ================================
-- FACT: IMPACT
-- ================================

CREATE TABLE IF NOT EXISTS fact_impact (
    id BIGSERIAL PRIMARY KEY,

    earthquake_id BIGINT REFERENCES dim_earthquake_current(earthquake_id),
    airport_id    BIGINT REFERENCES dim_airport(airport_id),
    time_id       BIGINT REFERENCES dim_time(time_id),

    distance_km DOUBLE PRECISION,
    proximity   DOUBLE PRECISION,
    estimated_PGA DOUBLE PRECISION,
    estimated_intensity DOUBLE PRECISION,
    risk_score DOUBLE PRECISION
);
