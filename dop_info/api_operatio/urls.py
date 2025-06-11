from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Settings for the application.
    """

    USD_CB: str = "https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date_fmt}&date_req2={end_date}&VAL_NM_RQ=R01235"
    
    
settings = Settings()