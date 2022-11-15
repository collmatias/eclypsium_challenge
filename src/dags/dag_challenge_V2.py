from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from jinja2 import Environment
import json
import requests
import pandas as pd


default_args = {
    'start_date': datetime(year=2022, month=11, day=9),
    'retries': 5,
    'retries_delay' : timedelta(minutes=5)
}

############################################################

def extract(ti):
    """Extracts data from MercadoLibre API, check if the API is available"""
    product_code = Variable.get("product_code")
    #BASE_URL = "https://api.mercadolibre.com/sites/MLA/search"
    BASE_URL =  Variable.get("mla_base_url")
    API = BASE_URL + "?category={code}#json"
    #
    mla_url = API.format(code=product_code)
    resp = requests.get(mla_url)
    #
    if resp.status_code == 200:
        df = pd.DataFrame.from_dict(resp.json()["results"]).get(['id', 'site_id', 'title','price','sold_quantity','thumbnail'])
        products_data = df.to_dict()
        ti.xcom_push(key=product_code+'_extracted', value=products_data )
        return "data_quality_check"
    else:
        ti.xcom_push(key=product_code+'_API_error', value=str(resp.reason) )
        return "api_error_handler"

############################################################

def api_error_handler(ti):
    """Creates the html_content to mail the API error"""
    product_code = Variable.get("product_code")
    resp_reason = ti.xcom_pull(key=product_code+'_API_error', task_ids=['extract'])
    #
    html_content=Environment().from_string("""<!DOCTYPE html>
                                                <html lang="en">
                                                <head>
                                                    <title>Alert in {{ code }} MLA API</title>
                                                </head>
                                                <body>
                                                    <h1>Error MLA API: {{ resp_reason }}</h1>
                                                </body>""").render(code=product_code,resp_reason=resp_reason)
                                                
    ti.xcom_push(key='html_content', value=str(html_content))

############################################################

def data_quality_check(ti):
    """Types of data quality checks
    Data quality checks can be run on different components of the dataset:

    Column: Common checks include confirming uniqueness of a key column, restricting the amount of NULL values that are allowed, 
    defining logical bounds for numeric or date columns, and defining valid options for categorical variables.

    Row: Common checks include comparing the values in several columns for each row, and ensuring that there are no mutually exclusive values 
    present in the same row.

    Table: Common checks include checking against a template schema to verify that it has not changed, and checking that a table has a minimum row count.
    Over several tables: Common checks include data parity checks across several tables. You can also create logical checks like making sure a customer marked as inactive has no purchases listed in any of the other tables.
        
    It is also important to distinguish between the two types of control in data quality:

    Static control: Runs checks on Spark or Pandas dataframes, or on existing tables in a database.
    These checks can be performed anywhere in a typical ETL pipeline and might halt the pipeline in case of a failed check to prevent 
    further processing of data with quality issues."""

    product_code = Variable.get("product_code")
    data_error_flag = False
    data_error = []
    products_data = ti.xcom_pull(key=product_code+'_extracted', task_ids=['extract'])[0]
    df = pd.DataFrame.from_dict(products_data)
    #
    # Check data Quality whit Pandas here!
    # Example: We need to have 50 diferent id products to be Ok.
    #
    #if df.nunique().id != 49:      # To test check Uncomment!
    if df.nunique().id != 50:       # To test check Comment!
        #data_error.append("Error: No hay 49 registros. Existen "+str(df.nunique().id)+" id diferentes.")   # To test check Uncomment!
        data_error.append("Error: No hay 50 registros. Existen "+str(df.nunique().id)+" id diferentes.")    # To test check Comment!
    #
    pd.to_numeric(df['price'], errors='coerce').fillna(0)
    pd.to_numeric(df['sold_quantity'], errors='coerce').fillna(0)
    #
    #
    #print(df.head(10))
    #
    #
    #
    if len(data_error)>0:
        data_error_flag = True
    #
    html_content=Environment().from_string("""<!DOCTYPE html>
                                                <html lang="en">
                                                <head>
                                                    <title>Alert in {{ code }} MLA API</title>
                                                </head>
                                                <body>
                                                    <h1>Data error MLA API: {{ error }}</h1>
                                                </body>""").render(code=product_code,error=str(data_error))
    #
    if data_error_flag:
        json_data = df.to_dict()
        ti.xcom_push(key=product_code+'_checked', value=str(json_data))
        ti.xcom_push(key='html_content', value=str(html_content))
        return "send_data_error_mail"
    else:
        json_data = df.to_dict()
        ti.xcom_push(key=product_code+'_checked', value=str(json_data))
        return "transform"

############################################################

def transform(ti) -> None:
    """This function generates the DML code for the load in the database"""
    product_code = Variable.get("product_code")
    products_str = ti.xcom_pull(key=product_code+'_checked', task_ids=['data_quality_check'])[0]
    product = json.loads(products_str.replace("\'","\""))
    #
    transformed_dml = 'BEGIN;\n'
    transformed = []
    for index_num in product['id'].keys():
        created_datetime = datetime.now()
        dml_code = "INSERT INTO prod.most_relevant_product (id, site_id, title, price, sold_quantity, thumbnail, created_date) VALUES ( '"+str(product['id'][index_num])+"', '"+str(product['site_id'][index_num])+"', '"+str(product['title'][index_num])+"', "+str(product['price'][index_num])+", "+str(product['sold_quantity'][index_num])+", '"+str(product['thumbnail'][index_num])+"', '"+ datetime.strftime(created_datetime,'%Y-%m-%d %H:%M:%S') +"');\n"
        transformed_dml = transformed_dml + dml_code
        #
        transformed.append({
            'id': product['id'][index_num],
            'site_id': product['site_id'][index_num],
            'title': product['title'][index_num],
            'price': product['price'][index_num],
            'sold_quantity': product['sold_quantity'][index_num],
            'thumbnail': product['thumbnail'][index_num],
            'created_date': datetime.strftime(created_datetime,'%Y-%m-%d %H:%M:%S')
        })
    transformed_dml = transformed_dml + 'COMMIT;\n'

    f = open("/opt/airflow/dags/sql/"+ product_code+ ".sql", "w")
    f.write(transformed_dml)
    f.close()
    #
    ti.xcom_push(key=product_code+'_transformed', value=str(transformed))

############################################################

def products_alert_check(ti) -> None:
    """This feature checks the amount of money earned and generates a report if any is greater than 7M"""
    product_code = Variable.get("product_code")
    products = ti.xcom_pull(key=product_code+'_transformed', task_ids=['transform'])[0]
    #
    products = products.replace('\'','\"')
    products = json.loads(str(products))
    #
    send_alert_flag = False
    mail_alert_info = []
    #
    for product in products:
        if ( product['price'] * product['sold_quantity'] ) > 7000000:
            #send mail
            product['earned'] = product['price'] * product['sold_quantity']
            mail_alert_info.append(product)
            send_alert_flag = True
    #
    if send_alert_flag:
        #
        f = open("/opt/airflow/dags/json/"+ product_code +"_7M_threshold.json", "w")
        f.write(str( json.dumps(mail_alert_info, indent=4)) )
        f.close()
        #
        html_content=Environment().from_string("""<!DOCTYPE html>
                                                    <html lang="en">
                                                    <head>
                                                        <title>Alert in {{ product_code }} category</title>
                                                    </head>
                                                    <body>
                                                        <h1>Se han vendido articulos por más de $7.000.000 para la categoria {{ product_code }}.</h1>
                                                        <ul id="products">
                                                        {% for item in products %}
                                                            <li><a href="{{ item.thumbnail }}">{{ item.title }}</a> -> Precio: $ {{ item.price }}</li>
                                                        {% endfor %}
                                                        </ul>
                                                    </body>""").render(product_code=product_code,products=mail_alert_info)
        ti.xcom_push(key='html_content', value=str(html_content))
        return 'send_product_email'
    else:
        return 'postgres_load'


############################################################

with DAG(
    dag_id='etl_MLA',
    default_args=default_args,
    schedule_interval='@daily',
    description='ETL pipeline for processing '+ Variable.get("product_code")
) as dag:


    # Task extract - Fetch products data from the API
    task_extract = BranchPythonOperator(
        task_id='extract',
        python_callable=extract
    )

    # Task api_error_handler - Transform fetched products
    task_api_error_handler = PythonOperator(
        task_id='api_error_handler',
        python_callable=api_error_handler
    )

    # Task data_quality_check - Transform fetched products
    task_data_quality_check = BranchPythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check
    )

    # Task send_dag_error_mail - Send alert via mail
    task_send_data_error_mail = EmailOperator(
        task_id='send_data_error_mail',
        to=Variable.get("on_alert_mail_to"),
        subject="Error in DAG ",
        files=[],
        html_content = """{{ task_instance.xcom_pull(key='html_content', task_ids=['data_quality_check']) }}"""
        )

    # Task send_dag_error_mail - Send alert via mail
    task_send_dag_error_mail = EmailOperator(
        task_id='send_dag_error_mail',
        to=Variable.get("on_alert_mail_to"),
        subject="Error in DAG",
        files=[],
        html_content = """{{ task_instance.xcom_pull(key='html_content', task_ids=['api_error_handler']) }}"""
        )

    # Task transform - Transform fetched products
    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    # Task products_alert_check - Check data
    task_products_alert_check = BranchPythonOperator(
        task_id='products_alert_check',
        python_callable=products_alert_check
    )

    # Task send_product_email - Send alert via mail
    task_send_product_email = EmailOperator(
        task_id='send_product_email',
        to=Variable.get("on_alert_mail_to"),
        subject="Alert in "+ Variable.get("product_code") +" category",
        files=["/opt/airflow/dags/json/"+ Variable.get("product_code") +"_7M_threshold.json"],
        html_content = """{{ task_instance.xcom_pull(key='html_content', task_ids=['products_alert_check']) }}"""
        )

    # Task postgres_load - Save products to Postgres
    # https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/operators/postgres_operator_howto_guide.html
    task_postgres_load = PostgresOperator(
        task_id='postgres_load',
        trigger_rule='none_failed_min_one_success',
        postgres_conn_id='postgres',
        sql= "sql/"+ Variable.get("product_code") +".sql"
    )

    task_extract >> task_data_quality_check >> task_transform >> task_products_alert_check >> [task_send_product_email, task_postgres_load]
    task_extract >> task_api_error_handler >> task_send_dag_error_mail
    task_data_quality_check >> task_send_data_error_mail >> task_transform
    task_send_product_email >> task_postgres_load
