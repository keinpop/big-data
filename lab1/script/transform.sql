-- Таблица клиентов
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INT,
    email TEXT UNIQUE,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

-- Таблица продавцов
CREATE TABLE sellers (
    seller_id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT UNIQUE,
    country TEXT,
    postal_code TEXT
);

-- Таблица товаров
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name TEXT,
    category TEXT,
    price NUMERIC(10, 2),
    weight NUMERIC(10, 2),
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC(3, 1),
    reviews INT,
    release_date DATE,
    expiry_date DATE
);

-- Таблица магазинов
CREATE TABLE stores (
    store_id SERIAL PRIMARY KEY,
    name TEXT,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT
);

-- Таблица поставщиков
CREATE TABLE suppliers (
    supplier_id SERIAL PRIMARY KEY,
    name TEXT,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

-- Фактовая таблица продаж
CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    seller_id INT REFERENCES sellers(seller_id),
    product_id INT REFERENCES products(product_id),
    store_id INT REFERENCES stores(store_id),
    supplier_id INT REFERENCES suppliers(supplier_id),
    sale_date DATE,
    quantity INT,
    total_price NUMERIC(10, 2)
);

-- -- Перенос данных
-- Вставляем уникальных клиентов
INSERT INTO customers (first_name, last_name, age, email, country, postal_code, pet_type, pet_name, pet_breed)
SELECT DISTINCT customer_first_name, customer_last_name, customer_age, customer_email, customer_country, customer_postal_code, customer_pet_type, customer_pet_name, customer_pet_breed
FROM main_data;

-- Вставляем уникальных продавцов
INSERT INTO sellers (first_name, last_name, email, country, postal_code)
SELECT DISTINCT seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code
FROM main_data;

-- Вставляем уникальные товары
INSERT INTO products (name, category, price, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date)
SELECT DISTINCT product_name, product_category, product_price, product_weight, product_color, product_size, product_brand, product_material, product_description, product_rating, product_reviews, product_release_date, product_expiry_date
FROM main_data;

-- Вставляем уникальные магазины
INSERT INTO stores (name, location, city, state, country, phone, email)
SELECT DISTINCT store_name, store_location, store_city, store_state, store_country, store_phone, store_email
FROM main_data;

-- Вставляем уникальных поставщиков
INSERT INTO suppliers (name, contact, email, phone, address, city, country)
SELECT DISTINCT supplier_name, supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country
FROM main_data;

-- Заполняем фактовую таблицу продаж
INSERT INTO fact_sales (customer_id, seller_id, product_id, store_id, supplier_id, sale_date, quantity, total_price)
SELECT 
    c.customer_id, 
    s.seller_id, 
    p.product_id, 
    st.store_id, 
    sp.supplier_id, 
    m.sale_date, 
    m.sale_quantity, 
    m.sale_total_price
FROM main_data m
JOIN customers c ON m.customer_email = c.email
JOIN sellers s ON m.seller_email = s.email
JOIN products p ON m.product_name = p.name AND m.product_category = p.category
JOIN stores st ON m.store_name = st.name AND m.store_city = st.city
JOIN suppliers sp ON m.supplier_name = sp.name AND m.supplier_email = sp.email
LIMIT 1000 OFFSET 10;

-- Чистим временные данные
DROP TABLE main_data;
