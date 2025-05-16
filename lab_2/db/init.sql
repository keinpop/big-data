CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk    SERIAL PRIMARY KEY,
    customer_id    BIGINT    UNIQUE,     
    first_name     TEXT     NOT NULL,
    last_name      TEXT     NOT NULL,
    age            INT,
    email          TEXT,
    country        TEXT,
    postal_code    TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_sk     SERIAL PRIMARY KEY,
    sale_date   DATE     UNIQUE,
    year        INT,
    quarter     INT,
    month       INT,
    day         INT,
    weekday     INT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_sk         SERIAL PRIMARY KEY,
    product_id         BIGINT   UNIQUE,   
    name               TEXT     NOT NULL,
    category           TEXT,
    weight             NUMERIC,
    color              TEXT,
    size               TEXT,
    brand              TEXT,
    material           TEXT,
    description        TEXT,
    rating             NUMERIC,
    reviews            INT,
    release_date       DATE,
    expiry_date        DATE,
    unit_price         NUMERIC       
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_sk      SERIAL PRIMARY KEY,
    seller_id      BIGINT   UNIQUE,      
    first_name     TEXT     NOT NULL,
    last_name      TEXT     NOT NULL,
    email          TEXT,
    country        TEXT,
    postal_code    TEXT
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_sk       SERIAL PRIMARY KEY,
    name           TEXT     UNIQUE,    
    location       TEXT,                
    city           TEXT,
    state          TEXT,
    country        TEXT,
    phone          TEXT,
    email          TEXT
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_sk    SERIAL PRIMARY KEY,
    name           TEXT     UNIQUE,    
    contact        TEXT,
    email          TEXT,
    phone          TEXT,
    address        TEXT,
    city           TEXT,
    country        TEXT
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_sk           SERIAL PRIMARY KEY,
    date_sk           INT  NOT NULL REFERENCES dim_date(date_sk),
    customer_sk       INT  NOT NULL REFERENCES dim_customer(customer_sk),
    seller_sk         INT  NOT NULL REFERENCES dim_seller(seller_sk),
    product_sk        INT  NOT NULL REFERENCES dim_product(product_sk),
    store_sk          INT  NOT NULL REFERENCES dim_store(store_sk),
    supplier_sk       INT  NOT NULL REFERENCES dim_supplier(supplier_sk),
    sale_quantity     INT,
    sale_total_price  NUMERIC,
    unit_price        NUMERIC
);

CREATE TABLE if not exists tmp_data
(
    id                       BIGINT,
    customer_first_name      TEXT,
    customer_last_name       TEXT,
    customer_age             INT,
    customer_email           TEXT,
    customer_country         TEXT,
    customer_postal_code     TEXT,
    customer_pet_type        TEXT,
    customer_pet_name        TEXT,
    customer_pet_breed       TEXT,
    seller_first_name        TEXT,
    seller_last_name         TEXT,
    seller_email             TEXT,
    seller_country           TEXT,
    seller_postal_code       TEXT,
    product_name             TEXT,
    product_category         TEXT,
    product_price            NUMERIC,
    product_quantity         INT,
    sale_date                DATE,
    sale_customer_id         BIGINT,
    sale_seller_id           BIGINT,
    sale_product_id          BIGINT,
    sale_quantity            INT,
    sale_total_price         NUMERIC,
    store_name               TEXT,
    store_location           TEXT,
    store_city               TEXT,
    store_state              TEXT,
    store_country            TEXT,
    store_phone              TEXT,
    store_email              TEXT,
    pet_category             TEXT,
    product_weight           NUMERIC,
    product_color            TEXT,
    product_size             TEXT,
    product_brand            TEXT,
    product_material         TEXT,
    product_description      TEXT,
    product_rating           NUMERIC,
    product_reviews          INT,
    product_release_date     DATE,
    product_expiry_date      DATE,
    supplier_name            TEXT,
    supplier_contact         TEXT,
    supplier_email           TEXT,
    supplier_phone           TEXT,
    supplier_address         TEXT,
    supplier_city            TEXT,
    supplier_country         TEXT
);
COPY tmp_data
    FROM '/dataset/MOCK_DATA.csv' 
    WITH (FORMAT csv, HEADER true);
COPY tmp_data
    FROM '/dataset/MOCK_DATA (1).csv' 
    WITH (FORMAT csv, HEADER true);
COPY tmp_data
    FROM '/dataset/MOCK_DATA (2).csv' 
    WITH (FORMAT csv, HEADER true);
COPY tmp_data
    FROM '/dataset/MOCK_DATA (3).csv' 
    WITH (FORMAT csv, HEADER true);
COPY tmp_data
    FROM '/dataset/MOCK_DATA (4).csv' 
    WITH (FORMAT csv, HEADER true);
COPY tmp_data
    FROM '/dataset/MOCK_DATA (5).csv' 
    WITH (FORMAT csv, HEADER true);
COPY tmp_data
    FROM '/dataset/MOCK_DATA (6).csv' 
    WITH (FORMAT csv, HEADER true);
COPY tmp_data
    FROM '/dataset/MOCK_DATA (7).csv' 
    WITH (FORMAT csv, HEADER true);
COPY tmp_data
    FROM '/dataset/MOCK_DATA (8).csv' 
    WITH (FORMAT csv, HEADER true);
COPY tmp_data
    FROM '/dataset/MOCK_DATA (9).csv' 
    WITH (FORMAT csv, HEADER true);

-- Проверить, что до сюда скрипт дошел и все ок
SELECT * FROM tmp_data LIMIT 1;