CREATE TABLE IF NOT EXISTS mock_data (
    id INTEGER,
    customer_first_name VARCHAR,
    customer_last_name VARCHAR,
    customer_age INTEGER,
    customer_email VARCHAR,
    customer_country VARCHAR,
    customer_postal_code VARCHAR,
    customer_pet_type VARCHAR,
    customer_pet_name VARCHAR,
    customer_pet_breed VARCHAR,
    seller_first_name VARCHAR,
    seller_last_name VARCHAR,
    seller_email VARCHAR,
    seller_country VARCHAR,
    seller_postal_code VARCHAR,
    product_name VARCHAR,
    product_category VARCHAR,
    product_price NUMERIC,
    product_quantity INTEGER,
    sale_date VARCHAR,
    sale_customer_id INTEGER,
    sale_seller_id INTEGER,
    sale_product_id INTEGER,
    sale_quantity INTEGER,
    sale_total_price NUMERIC,
    store_name VARCHAR,
    store_location VARCHAR,
    store_city VARCHAR,
    store_state VARCHAR,
    store_country VARCHAR,
    store_phone VARCHAR,
    store_email VARCHAR,
    pet_category VARCHAR,
    product_weight NUMERIC,
    product_color VARCHAR,
    product_size VARCHAR,
    product_brand VARCHAR,
    product_material VARCHAR,
    product_description TEXT,
    product_rating NUMERIC,
    product_reviews INTEGER,
    product_release_date VARCHAR,
    product_expiry_date VARCHAR,
    supplier_name VARCHAR,
    supplier_contact VARCHAR,
    supplier_email VARCHAR,
    supplier_phone VARCHAR,
    supplier_address VARCHAR,
    supplier_city VARCHAR,
    supplier_country VARCHAR
);

COPY mock_data FROM '/csv/MOCK_DATA.csv' DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (1).csv' DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (2).csv' DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (3).csv' DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (4).csv' DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (5).csv' DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (6).csv' DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (7).csv' DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (8).csv' DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (9).csv' DELIMITER ',' CSV HEADER;

