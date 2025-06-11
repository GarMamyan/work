from datetime import datetime, timedelta

from test_new_prod.bd.bd_operation import BdOperation
from test_new_prod.personal_client_dags.config_personal import DATE_DEFAULT_STATE_PERSONAL
from test_new_prod.src.state import StateABC
from test_new_prod.bd.models.models_direct import StateTableDirect



class StateDirect(StateABC):
    table = StateTableDirect
    
    def __init__(self, bd: BdOperation):
        self.bd = bd
        self.default = datetime.strptime(DATE_DEFAULT_STATE_PERSONAL, "%Y-%m-%d").date()
        self.usd_cd = self.default


        self.delta_default = timedelta(days=15)
        self.date_to_default = datetime.now().date()
        self._actual_state()

    def _actual_state(self):
        """
        Загружаем текущее состояние из базы данных
        """
        cur_state = self.bd.load_table_from_db(self.table)
        for state in cur_state.to_dict(orient="records"):
            setattr(self, state["name"], state["value"])

    def get_value_from(self, name):
        cur_state = getattr(self, name)
        if cur_state != self.default:
            return cur_state - getattr(self, 'delta_default')
        return cur_state

    def update_state(self, name: str, value: str):
        """
        Обновляем состояние в базе данных
        """
        self.bd.update_table(self.table, name, value)