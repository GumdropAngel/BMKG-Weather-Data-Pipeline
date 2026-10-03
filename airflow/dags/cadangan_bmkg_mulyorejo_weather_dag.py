from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import os

#basic DAG configuration
default_args = {
    "owner": "raja",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

ADM4_CODE = "35.78.26.1001" #public administrative area code (code for Mulyorejo District)
URL = f"https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4={ADM4_CODE}" #public API

DATA_DIR = "/opt/airflow/data_bmkg_mulyorejo" #directory where data will be stored
os.makedirs(DATA_DIR, exist_ok=True)

def extract_mulyorejo(ds, **kwargs):
    """Extract: ambil data prakiraan cuaca Mulyorejo dari API BMKG."""
    response = requests.get(URL, timeout=20)
    response.raise_for_status()
    data = response.json()
    file_path = os.path.join(DATA_DIR, f"bmkg_mulyorejo_raw_{ds}.json")
    with open(file_path, "w") as f:
        json.dump(data, f)
    print(f"[EXTRACT] File JSON mentah disimpan ke: {file_path}")

CLEAN_LATEST_PATH = os.path.join(DATA_DIR, "bmkg_mulyorejo_clean_latest.csv")

def transform_mulyorejo(ds, **kwargs):
    """Transform: ubah JSON mentah menjadi format CSV bersih yang terdeduplikasi."""
    raw_path = os.path.join(DATA_DIR, f"bmkg_mulyorejo_raw_{ds}.json")
    with open(raw_path) as f:
        raw = json.load(f)
    records = []
    root_loc = raw.get("lokasi", {})
    data_list = raw.get("data", [])

    for lokasi_item in data_list:
        loc = lokasi_item.get("lokasi", {})
        cuaca_groups = lokasi_item.get("cuaca", [])
        for group in cuaca_groups:
            for item in group:
                records.append({
                    "provinsi": loc.get("provinsi", root_loc.get("provinsi")),
                    "kotkab": loc.get("kotkab", root_loc.get("kotkab")),
                    "kecamatan": loc.get("kecamatan", root_loc.get("kecamatan")),
                    "desa": loc.get("desa", root_loc.get("desa")),
                    "lat": loc.get("lat", root_loc.get("lat")),
                    "lon": loc.get("lon", root_loc.get("lon")),
                    "timezone": str(loc.get("timezone", root_loc.get("timezone"))),
                    "datetime_utc": str(item.get("utc_datetime")),
                    "datetime_local": str(item.get("local_datetime")),
                    "t": str(item.get("t")),
                    "humidity": str(item.get("hu")),
                    "weather_code": str(item.get("weather")),
                    "weather_desc": str(item.get("weather_desc")),
                    "rain_mm": str(item.get("tp")),
                    "cloud_cover": str(item.get("tcc")),
                    "wd": str(item.get("wd")),
                    "wd_to": str(item.get("wd_to")),
                    "ws": str(item.get("ws")),
                    "visibility": str(item.get("vs")),
                    "visibility_text": str(item.get("vs_text")),
                    "image_icon": str(item.get("image")),
                    "source": str(item.get("source")),
                })

    df_new = pd.DataFrame(records)

    if df_new.empty:
        raise AirflowSkipException("[TRANSFORM] Tidak ada data yang terbaca dari JSON, cek struktur API.")

    #convert the data types of all columns to strings to ensure safe joins
    df_new = df_new.astype(str)
    clean_latest_path = os.path.join(DATA_DIR, "bmkg_mulyorejo_clean_latest.csv")
    clean_path = os.path.join(DATA_DIR, f"bmkg_mulyorejo_clean_{ds}.csv")

    #deduplication and filtering
    if os.path.exists(clean_latest_path):
        df_latest = pd.read_csv(clean_latest_path, dtype=str)
        key_cols = df_new.columns.tolist()
        
        #combining old and new data, dropping duplicates
        combined = pd.concat([df_latest, df_new], ignore_index=True)
        combined_nodup = combined.drop_duplicates(subset=key_cols, keep='first')

        #checking if there is no new data added
        if len(combined_nodup) == len(df_latest):
            raise AirflowSkipException("[TRANSFORM] Tidak ada baris baru yang unik untuk di-append.")

        #retrieving new data to be saved as a daily archive
        df_new_unique = combined_nodup.tail(len(combined_nodup) - len(df_latest))

        #saving the archive CSV for this run
        df_new_unique.to_csv(clean_path, index=False)
        
        #overwrite the latest CSV to update it with all the cleaned data
        combined_nodup.to_csv(clean_latest_path, index=False)
        print(f"[TRANSFORM] Data baru berhasil ditambahkan. Arsip disimpan di {clean_path}.")
    else:
        #first time, saving all data
        df_new.to_csv(clean_path, index=False)
        df_new.to_csv(clean_latest_path, index=False)
        print(f"[TRANSFORM] File 'latest' dibuat pertama kali dan data disimpan di: {clean_path}")

with DAG(
    dag_id="bmkg_mulyorejo_weather_dag",
    default_args=default_args,
    description="ETL prakiraan cuaca BMKG untuk Kec. Mulyorejo, Surabaya",
    schedule="*/5 * * * *",  #every 5 minutes
    start_date=datetime(2025, 11, 5),
    catchup=False,
    tags=["bmkg", "cuaca", "mulyorejo"],
) as dag:

    t1_extract = PythonOperator(
        task_id="extract_bmkg_mulyorejo",
        python_callable=extract_mulyorejo,
        op_kwargs={"ds": "{{ ts }}"},
    )

    t2_transform = PythonOperator(
        task_id="transform_bmkg_mulyorejo",
        python_callable=transform_mulyorejo,
        op_kwargs={"ds": "{{ ts }}"},
    )

    t1_extract >> t2_transform