import pandas as pd
import random
import uuid
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.types import Date, String, Float, Integer, DateTime, Numeric


# ========== НАСТРОЙКИ ==========
NUM_PRODUCTS = 100
NUM_RECEIPTS = 10000
START_DATE = datetime.strptime("2024-05-01", "%Y-%m-%d").date()
END_DATE = datetime.today().date()
FIXED_AGENT_ID = '3f5907d2-95b7-11ea-0a80-03320003115a'
STORES = [f"Магазин {i}" for i in range(1, 6)]
UNIQUE_AGENTS = [str(uuid.uuid4()) for _ in range(250)]
CATEGORIES = {
    "Категория 1": [f"Товар {i}" for i in range(1, 11)],
    "Категория 2": [f"Товар {i}" for i in range(11, 21)],
    "Категория 3": [f"Товар {i}" for i in range(21, 31)],
    "Категория 4": [f"Товар {i}" for i in range(31, 41)],
    "Категория 5": [f"Товар {i}" for i in range(41, 51)],
    "Категория 6": [f"Товар {i}" for i in range(51, 61)],
    "Категория 7": [f"Товар {i}" for i in range(61, 71)],
    "Категория 8": [f"Товар {i}" for i in range(71, 81)],
    "Категория 9": [f"Товар {i}" for i in range(81, 91)],
    "Категория 10": [f"Товар {i}" for i in range(91, 101)]
}

# ========== ШАГ 1: ГЕНЕРАЦИЯ СПРАВОЧНИКА ТОВАРОВ ==========
products = []
for _ in range(NUM_PRODUCTS):
    category = random.choice(list(CATEGORIES.keys()))
    name = random.choice(CATEGORIES[category])
    buy_price = round(random.uniform(10, 200), 2)
    sell_price = round(buy_price * random.uniform(1.2, 1.8), 2)
    products.append({
        "product_id": str(uuid.uuid4()),
        "product_name": name,
        "type_category_product": category,
        "price": buy_price,
        "product_price": sell_price
    })

products_df = pd.DataFrame(products)

# ========== ШАГ 2: ГЕНЕРАЦИЯ RETAIL_SALES ==========
retail_sales = []
for _ in range(NUM_RECEIPTS):
    date = START_DATE + timedelta(days=random.randint(0, (END_DATE - START_DATE).days))
    first_demand_date = START_DATE + timedelta(days=random.randint(0, (END_DATE - START_DATE).days))
    raw_created = datetime.combine(date, datetime.min.time()) + timedelta(minutes=random.randint(0, 1440))
    store = random.choice(STORES)
    agent_id = random.choice(UNIQUE_AGENTS) if random.random() < 0.5 else FIXED_AGENT_ID
    product = products_df.sample(1).iloc[0]
    quantity = random.randint(0, 4)
    discount = random.uniform(0, 10)
    receipt_id = str(uuid.uuid4())

    retail_sales.append({
        "date": date,
        'first_demand_date': first_demand_date,
        "agent_id": agent_id,
        "product_price": product["product_price"],
        "product_quantity": quantity,
        "product_discount": discount,
        "receipt_id_uuid": receipt_id,
        "products_id": product["product_id"],
        "products_name": product["product_name"],
        "products_type_category": product["type_category_product"],
        "products_buy_price": product["price"],
        "retail_store_name": store,
        "raw_created": raw_created
    })

retail_sales_df = pd.DataFrame(retail_sales)

# ========== ШАГ 3: ГЕНЕРАЦИЯ SALES_STOCKS_ON_DAYS ==========
date_range = pd.date_range(START_DATE, END_DATE)
sales_stocks = []

for _, product in products_df.iterrows():
    for single_date in date_range:
        daily_sales_quantity = random.randint(0, 5)
        remaining_quantity = random.randint(0, 50)
        discount = random.uniform(0, 10)

        sales_stocks.append({
            "date_year": single_date,
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "type_category_product": product["type_category_product"],
            "quantity": remaining_quantity,
            "price": product["price"],
            "product_price": product["product_price"],
            "product_quantity": daily_sales_quantity,
            "product_discount": discount
        })

sales_stocks_df = pd.DataFrame(sales_stocks)


# ========== ТИПЫ ДАННЫХ ==========
dtype_mapping_retail = {
    "date": Date(),
    'first_demand_date': Date(),
    "agent_id": String(),
    "product_price": Numeric(10, 1),    
    "product_quantity": Integer(),
    "product_discount": Numeric(10, 1),
    "receipt_id_uuid": String(),
    "products_id": String(),
    "products_name": String(),
    "products_type_category": String(),
    "products_buy_price": Numeric(10, 1),
    "retail_store_name": String(),
    "raw_created": DateTime()
}

dtype_mapping_stocks = {
    "date_year": Date(),
    "product_id": String(),
    "product_name": String(),
    "type_category_product": String(),
    "quantity": Integer(),
    "price": Numeric(10, 1),
    "product_price": Numeric(10, 1),
    "product_quantity": Integer(),
    "product_discount": Numeric(10, 1)
}


# ========== ПРИМЕР ВЫВОДА ==========
print("\nRetail Sales Sample:")
print(retail_sales_df.head())
print(retail_sales_df.dtypes)
print("\nSales Stocks on Days Sample:")
print(sales_stocks_df.head())
print(sales_stocks_df.dtypes)

# ========== ЗАГРУЗКА В БАЗУ ДАННЫХ ==========

# Подключение
engine = create_engine("postgresql+psycopg2://user1:s7a-q7s-LC7-nPC@rc1b-i444blrtihngfqnv.mdb.yandexcloud.net:6432/db2")

# Загрузка

sales_stocks_df.to_sql("sales_stocks_on_days", engine, schema="fake_ms", if_exists="replace", index=False, dtype=dtype_mapping_stocks)
retail_sales_df.to_sql("retail_sales", engine, schema="fake_ms", if_exists="replace", index=False, dtype=dtype_mapping_retail)

print("✅ Данные успешно загружены в базу данных.")
