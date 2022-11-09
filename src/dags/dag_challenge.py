import json
import requests
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta


default_args = {
    'start_date': datetime(year=2022, month=9, day=27),
    'retries': 5,
    'retries_delay' : timedelta(minutes=5)
}

############################################################

def extract(url: str, ti) -> None:
    res = requests.get(url)
    json_data = json.loads(res.content)
    ti.xcom_push(key='mla1577_extracted', value=json_data['results'])

############################################################

def transform(ti) -> None:
    products = ti.xcom_pull(key='mla1577_extracted', task_ids=['extract'])[0]
    #
    transformed_dml = 'BEGIN;\n'
    transformed = []
    for product in products:
        created_datetime = datetime.now()
        dml_code = "INSERT INTO prod.most_rel_microw (id, site_id, title, price, sold_quantity, thumbnail, created_date) VALUES ( '"+str(product['id'])+"', '"+str(product['site_id'])+"', '"+str(product['title'])+"', "+str(product['price'])+", "+str(product['sold_quantity'])+", '"+str(product['thumbnail'])+"', '"+ datetime.strftime(created_datetime,'%Y-%m-%d %H:%M:%S') +"');\n"
        transformed_dml = transformed_dml + dml_code
        #
        transformed.append({
            'id': product['id'],
            'site_id': product['site_id'],
            'title': product['title'],
            'price': product['price'],
            'sold_quantity': product['sold_quantity'],
            'thumbnail': product['thumbnail'],
            'created_date': datetime.strftime(created_datetime,'%Y-%m-%d %H:%M:%S')
        })
    transformed_dml = transformed_dml + 'COMMIT;\n'

    f = open("/opt/airflow/dags/sql/MLA1577.sql", "w")
    f.write(transformed_dml)
    f.close()
    ti.xcom_push(key='mla1577_transformed', value=str(transformed))

############################################################

def mail_alert_check(ti):
    products = ti.xcom_pull(key='mla1577_transformed', task_ids=['transform'])[0]

    print(products)
    products = products.replace('\'','\"')
    products = json.loads(str(products))

    send_alert_flag = False
    mail_alert_info = []

    for product in products:
        if ( product['price'] * product['sold_quantity'] ) > 7000000:
            #send mail
            product['earned'] = product['price'] * product['sold_quantity']
            mail_alert_info.append(product)
            send_alert_flag = True

    if send_alert_flag:

        f = open("/opt/airflow/dags/json/MLA1577_7M_threshold.json", "w")
        f.write(str( json.dumps(mail_alert_info, indent=4)) )
        f.close()

        return 'send_mail_alert'
    else:
        return 'load'


############################################################

with DAG(
    dag_id='etl_mla1577',
    default_args=default_args,
    schedule_interval='@daily',
    description='ETL pipeline for processing MLA1577'
) as dag:

    # Task 1 - Fetch products data from the API
    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
        op_kwargs={'url': 'https://api.mercadolibre.com/sites/MLA/search?category=MLA1577#json'}
    )

    # Task 2 - Transform fetched products
    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    # Task 3 - Check data
    task_mail_alert_check = BranchPythonOperator(
        task_id='mail_alert_check',
        python_callable=mail_alert_check
    )

    # Task 4 - Send alert via mail
    task_send_email = EmailOperator(
        task_id='send_mail_alert',
        to=Variable.get("on_alert_mail_to"),
        subject='Alert on MLA1577 category',
        files=["/opt/airflow/dags/json/MLA1577_7M_threshold.json"],
        html_content="""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <title>Alert on MLA1577 category</title>
        </head>
        <body>
            <h1>Se han vendido articulos por más de $7.000.000 para la categoria MLA1577.</h1>

            <p>Informacion sobre los items en el archivo adjunto.</p>

            Fecha: {{ ds }}
        </body>
        """
    )

    # Task 5 - Save products to Postgres
    task_load = PostgresOperator(
        task_id='load',
        trigger_rule='none_failed_min_one_success',
        postgres_conn_id='postgres',
        sql= 'sql/MLA1577.sql'
    )


    task_extract >> task_transform >> task_mail_alert_check >> [task_send_email, task_load]
    task_send_email >> task_load
