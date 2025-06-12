from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from test_new_prod.bd.bd_operation import PostgreSqlORM, GoogleSheetsApi

from dop_info.api_operatio.api import DopInfoApi
from dop_info.extract import ExtractDopInfo
from dop_info.transform import TransformDopInfo
from datetime import datetime, timedelta

from test_new_prod.personal_client_dags.config_personal import CRED
from test_new_prod.personal_client_dags.wolmer.config_wolmer import ACCOUNT_NAME, DIRECT, URL_PRICE_DIRECT, FILE_PATH



date="2024-12-26"
start_date=datetime.strptime(date, "%Y-%m-%d")
today = datetime.today()
end_date = today.strftime("%d/%m/%Y")
start_date_fmt = datetime.strptime(date, "%Y-%m-%d").strftime("%d/%m/%Y")   

xml= DopInfoApi.get_usd_cb_hml(start_date_fmt,end_date)
miss_df = ExtractDopInfo.get_usd_cb(xml)
df = TransformDopInfo.transform_dop_info(miss_df,start_date, today)
print(df)




@task(
    retries=3, retry_delay=timedelta(minutes=3), max_retry_delay=timedelta(minutes=40)
)
def create_prod():
    bd = PostgreSqlORM(ACCOUNT_NAME,'dop_info', CRED)
    bd._create_all()

task(
    retries=3, retry_delay=timedelta(minutes=3), max_retry_delay=timedelta(minutes=40)
)
def performance_report():
    account = AccountDirect(DIRECT, ['e-16660845', 'e-16664206','e-16596221'])
    pg = PostgreSqlORM(ACCOUNT_NAME,'dop_info', CRED)
    state = StateDirect(pg)
    api = DirectApi(account)
    extract_object = ExtractDirect(state, api, account)
    loader = Loader(pg, state)
    performance_report = extract_object.get_performance_report()
    transform_performance_report_frame = TransformDirect.transform_performance_report(performance_report)
    loader.load_data_from_date(transform_performance_report_frame, PerformanceReportDirect)
    return True

@task(
    retries=3, retry_delay=timedelta(minutes=15), max_retry_delay=timedelta(minutes=40)
)
def load_stat_direct_mp():
    google_services_for_price = GoogleSheetsApi(FILE_PATH, URL_PRICE_DIRECT)
    pg = PostgreSqlORM(ACCOUNT_NAME,'direct', CRED)
    state = StateDirect(pg)
    loader = Loader(pg, state)
    price = google_services_for_price.load_table_from_db("direct_mp")
    price = TransformDirect.transform_direct_mp(price)
    loader.load_data(price, PerfomanceDirectMP)

@dag(
    default_args={
        "owner": "prod_bi",
        "start_date": days_ago(1),
        "poke_interval": 600,
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
    },
    schedule_interval="0 0 * * *",  # 3:00 МСК (UTC+3)
    catchup=False,
    dag_id=f"v2_direct_{ACCOUNT_NAME}",
    tags = ["1. v2", "2. direct", "3. personal", ACCOUNT_NAME],
    description="Пример DAG wolmer",
)
def example_dag():
    create_prod_task = create_prod()
    performance_report_task = performance_report()
    load_stat_direct_mp_task = load_stat_direct_mp()

    create_prod_task >> [performance_report_task,load_stat_direct_mp_task]

# Создание экземпляра DAG
dag_instance = example_dag()
    