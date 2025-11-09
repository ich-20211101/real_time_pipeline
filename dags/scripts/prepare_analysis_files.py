import pandas as pd

def prepare_analysis_files():
    base_path = "/opt/airflow/data/"
    orders = pd.read_csv(base_path + "orders.csv")
    products = pd.read_csv(base_path + "products.csv")
    users = pd.read_csv(base_path + "users.csv")

    orders["order_date"] = pd.to_datetime(orders["order_date"])

    df = orders.merge(products, on="product_id", how="left") \
                .merge(users, on="customer_id", how="left")

    # Sales pattern
    daily = (
        df.groupby(df["order_date"].dt.normalize())
        .agg(total_sales = ("sales", "sum"),
             total_profit = ("profit", "sum"),
             total_quantity = ("quantity", "sum"))
        .reset_index()
        .rename(columns={"order_date": "date"})
    )
    daily["day_of_week"] = daily["date"].dt.day_name()
    daily["month"] = daily["date"].dt.month
    daily.to_csv(base_path + "daily_sales_pattern.csv", index = False)

    # Customer segmentation
    customer = (
        df.groupby("customer_id")
        .agg(total_sales = ("sales", "sum"),
             total_profit = ("profit", "sum"),
             total_quantity = ("quantity", "sum"),
             avg_discount = ("discount", "mean"))
        .reset_index()
        .merge(users, on="customer_id", how="left")
    )
    customer.to_csv(base_path + "customer_segmentation.csv", index = False)

    # Outlier detection
    outliers = df[[
        "order_id", "order_date", "sales", "quantity", "profit",
        "category", "sub_category", "product_name"
    ]]
    outliers.to_csv(base_path + "outlier_detection.csv", index = False)

    # Profitability by product category
    profit_by_category = (
        df.groupby(["category", "sub_category"])
        .agg(total_sales = ("sales", "sum"),
             total_profit = ("profit", "sum"),
             avg_discount = ("discount", "sum"),
             total_quantity = ("quantity", "sum"))
        .reset_index()
    )
    profit_by_category.to_csv(base_path + "product_profitability.csv", index=False)

    print("✅ Analysis ready.")

if __name__ == "__main__":
    prepare_analysis_files()