from cb_curr.dop_info.api_operatio.api import DopInfoApi
from cb_curr.dop_info.extract import ExtractDopInfo
from cb_curr.dop_info.transform import TransformDopInfo
from datetime import datetime


start_date="2024-12-26"
today = datetime.today()
end_date = today.strftime("%d/%m/%Y")
start_date_fmt = datetime.strptime(start_date, "%Y-%m-%d").strftime("%d/%m/%Y")   

xml= DopInfoApi.get_usd_cb_hml(start_date_fmt,end_date)
miss_df = ExtractDopInfo.get_usd_cb(xml)
df = TransformDopInfo.transform_dop_info(miss_df,start_date_fmt, end_date)
print(df)