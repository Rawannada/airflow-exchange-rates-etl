from __future__ import annotations

# استيراد مكتبات Airflow و MySQL
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook 
from airflow.models import Variable

# استيراد مكتبات ETL
import requests
import pandas as pd
from datetime import datetime, timedelta
import io 
import time 


# -------------------------------------------------------------
# 1. الثوابت وإعدادات الـ ETL (تحديث أسماء الجداول)
# -------------------------------------------------------------

DB_CONN_ID = "books_mysql_connection" 
# 1. الجدول اليومي (سجل التشغيل الحالي)
RUN_RATES_TABLE = "run_exchange_rates" 
# 2. الجدول التاريخي (يحتفظ بجميع السجلات السابقة)
HISTORICAL_RATES_TABLE = "historical_exchange_rates" 
FINAL_BASE_CURRENCY = "EGP"
TARGET_CURRENCIES_CODES = ["USD", "EUR", "GBP", "JPY", "EGP"] 


# -------------------------------------------------------------
# 2. المهمة الأولى: إنشاء الجداول في MySQL (تعديل)
# -------------------------------------------------------------

def create_exchange_rates_mysql_tables():
    """ تنشئ جدولي السجل اليومي والتاريخي. """
    mysql_hook = MySqlHook(mysql_conn_id=DB_CONN_ID)
    
    # 1. إنشاء الجدول التاريخي (Historical Table)
    create_historical_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {HISTORICAL_RATES_TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        base_currency VARCHAR(3) NOT NULL,
        target_currency VARCHAR(3) NOT NULL,
        rate DECIMAL(10, 6),
        date DATE,
        fetch_timestamp DATETIME
    );
    """
    mysql_hook.run(create_historical_table_sql)
    print(f"✅ تم التأكد من وجود الجدول التاريخي: {HISTORICAL_RATES_TABLE}")

    # 2. إنشاء جدول السجل اليومي (Run Log Table)
    # نستخدم نفس الهيكل
    create_run_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {RUN_RATES_TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        base_currency VARCHAR(3) NOT NULL,
        target_currency VARCHAR(3) NOT NULL,
        rate DECIMAL(10, 6),
        date DATE,
        fetch_timestamp DATETIME
    );
    """
    mysql_hook.run(create_run_table_sql)
    print(f"✅ تم التأكد من وجود جدول السجل اليومي: {RUN_RATES_TABLE}")


# -------------------------------------------------------------
# 3. المهمة الثانية: جلب البيانات وتنظيمها (Extract & Transform)
# (هذه المهمة لم تتغير)
# -------------------------------------------------------------

def fetch_exchange_rates_data():
    """ تسحب البيانات من API، تحولها إلى EGP base، وترجعها كسلسلة JSON عبر XComs. """
    
    BASE_URL = "http://api.exchangerate.host/live"
    
    try:
        # استرجاع المفتاح من Airflow Variables
        access_key = Variable.get("EXCHANGE_RATE_API_KEY") 
    except KeyError:
        access_key = "6c172eeab6163b8381ce68768b2900e9" 
    
    symbols_string = ",".join(TARGET_CURRENCIES_CODES)
    params = {"access_key": access_key, "currencies": symbols_string}
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status() 
        data = response.json()
        
        timestamp = data.get("timestamp")
        today_date = datetime.fromtimestamp(timestamp).date().isoformat()
        quotes_data = data.get('quotes', {})
        api_base_currency = data.get('source') 
        
        base_to_egp_key = f"{api_base_currency}EGP" 
        base_to_egp_rate = quotes_data.get(base_to_egp_key)
        
        if not base_to_egp_rate or base_to_egp_rate == 0:
            raise ValueError(f"❌ لم يتم العثور على سعر {base_to_egp_key}. لا يمكن التحويل.")

        rates_records = []
        for key, rate in quotes_data.items():
            target_currency = key[3:] 
            egp_rate = base_to_egp_rate / rate 
            
            if target_currency in TARGET_CURRENCIES_CODES:
                 rates_records.append({
                    'base_currency': FINAL_BASE_CURRENCY, 
                    'target_currency': target_currency,
                    'rate': egp_rate,
                    'date': today_date,
                    'fetch_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
                })
        
        daily_rates_df = pd.DataFrame(rates_records).drop_duplicates(subset=['target_currency'])
        return daily_rates_df.to_json()
        
    except Exception as e:
        print(f"❌ فشل تنفيذ مهمة سحب البيانات: {e}")
        raise 

# -------------------------------------------------------------
# 4. المهمة الثالثة: التحميل إلى MySQL (تعديل - التحميل المزدوج)
# -------------------------------------------------------------

def load_exchange_rates_to_mysql(ti):
    """ تستقبل الـ DataFrame من التاسك السابق وتحمله إلى جدولي السجل والتحميل. """
    
    json_data = ti.xcom_pull(task_ids='fetch_data_task')
    if not json_data:
        raise ValueError("❌ فشل استرجاع البيانات من التاسك السابق.")
        
    daily_rates_df = pd.read_json(io.StringIO(json_data))
    
    hook = MySqlHook(mysql_conn_id=DB_CONN_ID)
    data_to_insert = [tuple(row) for row in daily_rates_df.values]
    
    target_fields = [
        'base_currency', 'target_currency', 'rate', 'date', 'fetch_timestamp'
    ]

    # 1. مسح وإدراج في جدول السجل اليومي (Run Table - Output)
    try:
        # مسح البيانات السابقة
        hook.run(f"TRUNCATE TABLE {RUN_RATES_TABLE};")
        print(f"✅ تم مسح بيانات الجدول السابق: {RUN_RATES_TABLE}")
        
        # إدراج البيانات الجديدة (التي تمثل الناتج الحالي)
        hook.insert_rows(
            table=RUN_RATES_TABLE,
            rows=data_to_insert,
            target_fields=target_fields,
        )
        print(f"✅ تم تحميل {len(data_to_insert)} سجل بنجاح إلى جدول السجل اليومي: {RUN_RATES_TABLE}.")
        
    except Exception as e:
        print(f"❌ فشل تحميل البيانات إلى جدول السجل اليومي: {e}")
        raise
        
    # 2. إدراج في الجدول التاريخي (Historical Table - Archive)
    try:
        hook.insert_rows(
            table=HISTORICAL_RATES_TABLE,
            rows=data_to_insert,
            target_fields=target_fields,
        )
        print(f"✅ تم أرشفة {len(data_to_insert)} سجل بنجاح إلى الجدول التاريخي: {HISTORICAL_RATES_TABLE}.")
        
    except Exception as e:
        print(f"❌ فشل أرشفة البيانات إلى الجدول التاريخي: {e}")
        raise


# -------------------------------------------------------------
# 5. تعريف الـ DAG
# -------------------------------------------------------------

SCHEDULE_CRON = "0 9,13,17 * * *" # في تمام 09:00، 13:00، و 17:00 UTC

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 7),
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'egp_exchange_rates_historical_pipeline', # تم تغيير اسم الـ DAG
    default_args=default_args,
    description='Fetches exchange rates (EGP base) and loads them to a Run Table and a Historical Table.',
    schedule_interval=SCHEDULE_CRON, 
    catchup=False, 
    tags=['currency', 'egp', 'mysql', 'historical']
) as dag:
    
    # 1. إنشاء الجدول
    create_tables_task = PythonOperator(
        task_id='create_rates_tables',
        python_callable=create_exchange_rates_mysql_tables,
    )

    # 2. استخلاص وتحويل
    fetch_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_exchange_rates_data,
        provide_context=True, 
    )

    # 3. تحميل البيانات
    load_task = PythonOperator(
        task_id='load_to_mysql_task',
        python_callable=load_exchange_rates_to_mysql,
        provide_context=True,
    )
    
    # تحديد تسلسل المهام
    create_tables_task >> fetch_task >> load_task