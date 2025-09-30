-- Create users table
CREATE TABLE IF NOT EXISTS users (
    customer_id VARCHAR(20) PRIMARY KEY,
    segment VARCHAR(50),
    region VARCHAR(50),
    state VARCHAR(50),
    city VARCHAR(50)
);

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(30) PRIMARY KEY,
    category VARCHAR(50),
    sub_category VARCHAR(50)
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    row_id SERIAL PRIMARY KEY,
    order_id VARCHAR(30) NOT NULL,
    order_date DATE NOT NULL,
    ship_date DATE,
    ship_mode VARCHAR(50),
    order_priority VARCHAR(30),
    customer_id VARCHAR(20) REFERENCES users(customer_id),
    product_id VARCHAR(30) REFERENCES products(product_id),
    sales NUMERIC(10, 2),
    quantity INTEGER,
    discount NUMERIC(5, 2),
    profit NUMERIC(10, 4)
);