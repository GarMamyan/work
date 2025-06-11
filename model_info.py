from sqlalchemy import Column, Integer, Float, String, Date, DateTime, JSON, BigInteger, Text
from sqlalchemy.ext.declarative import declarative_base

# Базовый класс для всех моделей
Base = declarative_base()



class StateTableDirect(Base):
    __tablename__ = "state"
    __table_args__ = {'schema': 'dop_info'}

    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    value = Column(Date)
    
    
## тут добавить класс для курса название таблицы usd_cd



def create_filtered_base_dop_info(name_client):
    def is_model_of_schema(cls, schema):
        # Проверяем наличие __table_args__ и соответствие схеме
        if hasattr(cls, '__table_args__') and isinstance(cls.__table_args__, dict):
            return cls.__table_args__.get('schema') == schema
        return False

    # Получаем все зарегистрированные модели через _class_registry
    filtered_classes = [
        cls for cls in Base.registry._class_registry.values()
    if isinstance(cls, type) and is_model_of_schema(cls, 'direct')]

    # Создаем новый базовый класс
    new_base = declarative_base()
    # name_client
    # Переносим метаданные только для нужных моделей
    for cls in filtered_classes:

        cls.__table_args__['schema'] = name_client
        cls.__table__.schema = name_client
        cls.__table__.tometadata(new_base.metadata)

    return new_base