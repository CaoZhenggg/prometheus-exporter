from fastapi import FastAPI
from fastapi_utilities import repeat_at
from prometheus_client import make_asgi_app
from prometheus_client import Gauge
import oracledb
import mysql.connector


tables = {
    "oracle": {
        "FLXUSER": [
            "BAT_WIP_CELL_PARA"
        ]
    },
    "doris": {
        "sy_ods": [
            "ODS_MES_BAT_WIP_CELL_PARA"
        ]
    }
}


# Create app
app = FastAPI(debug=False)

# Add prometheus asgi middleware to route /metrics requests
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


# Gen oracle metrics
oracle_guage_objects = {}

oracle_tables = tables["oracle"]

for k in oracle_tables:
    for v in oracle_tables[k]:
        metric_name = 'oracle_' + k.lower() + '_' + v.lower() + '_rows'
        guage_name = 'guage_' + metric_name

        exec(f"{guage_name} = Gauge(metric_name, 'Oracle ' + k + '.' + v + ' table rows number')")

        guage = eval('guage_' + metric_name)

        oracle_guage_objects[metric_name] = guage

# Gen doris metrics
doris_guage_objects = {}

doris_tables = tables["doris"]

for k in doris_tables:
    for v in doris_tables[k]:
        metric_name = 'doris_' + k.lower() + '_' + v.lower() + '_rows'
        guage_name = 'guage_' + metric_name

        exec(f"{guage_name} = Gauge(metric_name, 'Doris ' + k + '.' + v + ' table rows number')")

        guage = eval('guage_' + metric_name)

        doris_guage_objects[metric_name] = guage


def oracle_query(host: str, port: int, service_name:str,
                 user: str, password: str,
                 schema: str, table: str) -> str:

    sql = ("select count(1) FROM " + schema + '.' + table + " WHERE (CREATEDON >= trunc(sysdate)-1+9/24-1/3 AND CREATEDON < trunc(sysdate)+9/24-1/3)")

    connection = oracledb.connect(host=host, port=port, service_name=service_name,
                                  user=user, password=password)
    cursor = connection.cursor()
    cursor.execute(sql)

    result = cursor.fetchall()[0][0]

    cursor.close()
    connection.close()

    return result

def doris_query(host: str, port: int,
                user: str, password: str,
                database: str, table: str) -> str:
    
    sql = ("select count(1) from " + database + '.' + table + " where  createdon >= DATE_FORMAT(DATE_SUB(CURDATE(),INTERVAL 1 DAY), '%Y-%m-%d 01:00:00') and createdon  < DATE_FORMAT(CURRENT_DATE(),'%Y-%m-%d 01:00:00')")
    
    connection = mysql.connector.connect(host=host, port=port,
                                         user=user, password=password,
                                         database=database)
    cursor = connection.cursor(buffered=True)
    cursor.execute(sql)
    
    # connection.commit()

    result = cursor.fetchall()[0][0]

    cursor.close()
    connection.close()

    return result

@app.on_event("startup")
@repeat_at(cron="0 2 * * *")
def update_oracle_table_rows_metric():
    for k in oracle_tables:
        for v in oracle_tables[k]:

            oracle_rows_num = oracle_query(host='10.xxx.xxx.xxx',
                                           port=1521,
                                           service_name='symes',
                                           user='BI',
                                           password='Pxxx',
                                           schema=k,
                                           table=v)

            oracle_metric_name = 'oracle_' + k.lower() + '_' + v.lower() + '_rows'
            oracle_guage_objects[oracle_metric_name].set(oracle_rows_num)

@app.on_event("startup")
@repeat_at(cron="0 2 * * *")
def update_doris_table_rows_metric():
    for k in doris_tables:
        for v in doris_tables[k]:

            doris_rows_num = doris_query(host='10.xxx.xxx.xxx',
                                         port=9030,
                                         user='flink_xxx',
                                         password='Pxxx',
                                         database=k,
                                         table=v)

            doris_metric_name = 'doris_' + k.lower() + '_' + v.lower() + '_rows'
            doris_guage_objects[doris_metric_name].set(doris_rows_num)
