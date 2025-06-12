#тут оработка такого что получили в экстракте
from dop_info.extract import ExtractDopInfo
from xml.etree import ElementTree as ET
import pandas as pd
from datetime import datetime


class TransformDopInfo:

    @staticmethod
    def transform_dop_info(miss_df: pd.DataFrame ,start_date_fmt, end_date):
        
        df = miss_df
        # Создаем полный диапазон дат
        full_range = pd.date_range(start=start_date_fmt, end=end_date)
        df = df.reindex(full_range)

        # Заполняем пропуски предыдущими значениями
        df.ffill(inplace=True)

        # Переименовываем индекс обратно в колонку
        df.reset_index(inplace=True)
        df.rename(columns={"index": "date"}, inplace=True)
        return df
    