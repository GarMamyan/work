### тут обращение к апи
import requests
import logging
import time


from urls import settings



logging.basicConfig(level=logging.DEBUG)


class DopInfoApi:

    def get_usd_cb_hml(start_date_fmt,end_date):
        response = requests.get(
            settings.USD_CB.format(start_date_fmt=start_date_fmt, end_date = end_date)
        )
        return response





    # def get_usd_cb():
    #     params = get_usd_cb_params()
    #     response = requests.get(settings.USD_CB)
    #     logging.info(f"get_usd_cb with params {params}")
    #     meta = response.json()['meta']  # Пример меты
    #                                     #{'href': 'https://api.moysklad.ru/api/remap/1.2/entity/assortment',
    #                                     #  'type': 'assortment',
    #                                     #  'mediaType': 'application/json',
    #                                     #  'size': 985,
    #                                     #  'limit': 1000,
    #                                     #  'offset': 0}
    #     data = response.json()['rows']
    #     return meta, data
    







