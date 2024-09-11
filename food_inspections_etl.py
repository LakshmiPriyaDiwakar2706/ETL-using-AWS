import pandas as pd
import json
import requests
import s3fs
from datetime import datetime
import requests

def run_food_inspections_etl():
    url = "https://data.cityofchicago.org/resource/4ijn-s7e5.json?$limit=125000"
    r = requests.get(url)

    response_dict = r.json()

    data = pd.DataFrame(response_dict)

    data = data.drop([':@computed_region_awaf_s7ux',
        ':@computed_region_6mkv_f3dw', ':@computed_region_vrxf_vc4k',
        ':@computed_region_bdys_3d7i', ':@computed_region_43wa_7qmu'],axis=1)

    data.to_csv("s3://lp-food-inspections-airflow/food_inspections.csv")