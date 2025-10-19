USER_INSERT_SQL = """
    INSERT INTO users (customer_id, segment, region, state, city)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) DO NOTHING
"""
PRODUCT_INSERT_SQL = """
    INSERT INTO products (product_id, category, sub_category, product_name)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (product_id) DO NOTHING
"""
ORDER_INSERT_SQL = """
    INSERT INTO orders (
        order_id, order_date, ship_date, ship_mode,
        customer_id, product_id, sales, quantity, discount, profit
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""