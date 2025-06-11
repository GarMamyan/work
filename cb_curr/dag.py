from dop_info.transform import TransformDopInfo
import pandas as pd
import requests
from datetime import datetime


start_date="2024-12-26"
today = datetime.today()
end_date = today.strftime("%d/%m/%Y")
start_date_fmt = datetime.strptime(start_date, "%Y-%m-%d").strftime("%d/%m/%Y")   

df = TransformDopInfo.transform_dop_info(start_date_fmt, end_date)
print(df)