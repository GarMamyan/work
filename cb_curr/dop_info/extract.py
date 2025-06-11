# тут класс который вызывает функции из api operation нужен он, когда сложные извлечения с разбивкой по период, у тебя по сути будет просто функция которая один раз вызывает функцию и возвращает ее ответ
# тут класс который вызывает функции из api operation нужен он, когда сложные извлечения с разбивкой по период, у тебя по сути будет просто функция которая один раз вызывает функцию и возвращает ее ответ
from dop_info.api_operatio.api import DopInfoApi
from xml.etree import ElementTree as ET
import pandas as pd
from datetime import datetime

class ExtractDopInfo:

    def get_usd_cb(xml):
        response = xml
        from xml.etree import ElementTree as ET
        root = ET.fromstring(response.text)

        records = []
        for record in root.findall("Record"):
            date = datetime.strptime(record.attrib["Date"], "%d.%m.%Y")
            value = float(record.find("Value").text.replace(",", "."))
            records.append((date, value))

        df = pd.DataFrame(records, columns=["date", "usd_to_rub"])
        df.sort_values("date", inplace=True)
        df.set_index("date", inplace=True)
        return df
