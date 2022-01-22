from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta
from sre_constants import SUCCESS
from dateutil.relativedelta import *

import requests
import os
import pytz
import datetime
import logging

from config_local import ENERGY_API_URL, ENERGY_API_UUID, ENERGY_API_TOKEN

logging.basicConfig(level=logging.INFO)

DAYS_INTERVAL = 1
PARENT_DIR = '/data'
FILENAME = 'datafile.txt'
SUCCESS_FILENAME = '.success'
FOLDER_FORMAT = '%Y-%m-%d' # date format to create the data folder
QUERY_FORMAT = '%Y-%m-%d %H:%M:%SZ' # date format to query the Energy API
DASHBOARD = 'energy'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

class EnergyApi:
    """This is a class for accessing data from the energy API."""

    def __init__(self, date, 
                api_version='/api/2.0/',):
        self.date_str = date
        self.base_url = ENERGY_API_URL
        self.api_version = api_version
        self.installation_uuid = ENERGY_API_UUID
        self.access_token = ENERGY_API_TOKEN
        self.days_interval = DAYS_INTERVAL
        self.query_format = QUERY_FORMAT
        self.tz = None
        self.path = os.path.join(PARENT_DIR, self.date_str, 'raw')
        self.dashboard = DASHBOARD
        os.makedirs(self.path)
        logging.info(f'Collecting data for {self.base_url}{self.api_version}')
        
    def fetch_data(self):
        to_datetime_obj = self._datetime_str2obj(self.date_str, FOLDER_FORMAT)
        end_time_obj = to_datetime_obj.replace(second=0, microsecond=0, minute=59, hour=23)
        start_time_obj = to_datetime_obj
        print(f'Start time : {start_time_obj} - End time: {end_time_obj}')
        self.fetch_between_day(end_time_obj, start_time_obj)

    def url_for(self, resource):
        str_url = f'{self.base_url}{self.api_version}{resource}/{self.installation_uuid}/{self.access_token}'
        print(str_url)
        return str_url

    def _datetime_to_utc(self, time_obj):
        utc_time_obj =time_obj.astimezone(pytz.utc)
        return utc_time_obj

    def get(self, resource, params=None, raw=False):
        headers = requests.structures.CaseInsensitiveDict()
        headers['Accept'] = 'application/json'
        headers['Accept-Language'] = 'en'

        if params is not None:
            rsp = requests.get(self.url_for(resource), headers=headers, params=params)
        else:
            rsp = requests.get(self.url_for(resource), headers=headers)

        if rsp.status_code == 200:
            if raw:
                return rsp.content
            else:
                return rsp.json()
        elif rsp.status_code == 401:
            raise Exception(f'auth failed for resource: {self.url_for(resource)}')
        else:
            raise Exception(f'Failed calling resource: {self.url_for(resource)}: status code: {rsp.status_code} reply: {rsp.content}')

    def utc_to_local(self, utc_dt):
        local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(self.tz)
        return self.tz.normalize(local_dt)

    def unix2datetime(self, unix_time):
        datetime_obj = datetime.datetime.fromtimestamp(unix_time/1000)
        utc_time_obj = self._datetime_to_utc(datetime_obj)
        aware_datetime_obj = self._create_aware_time_object(utc_time_obj)
        return aware_datetime_obj

    def fetch_between_day(self, start_time, end_time):
        data_list=[]
        logging.info(f'Fetching data for day starting at {start_time.isoformat("T") + "Z"}')
        logging.info(f'Sending data starting at {start_time} and ending at {end_time}')
        for data in self.iter_data(start_time, end_time):
            logging.debug(data)
            data_list.append(data)
        logging.debug(data_list)
    
    def _datetime_str2obj(self, input_time, format):
        time_obj=datetime.datetime.strptime(input_time, format)
        return time_obj
        
    def _datetime_obj2string(self,input_time):
        time_str=datetime.datetime.strftime(input_time, self.query_format)
        return time_str

    def _create_aware_time_object(self, naive_time_obj):
        '''creates aware time objects assigning +00:00 for utc'''
        aware_time_obj = naive_time_obj.replace(tzinfo=pytz.UTC)
        return aware_time_obj

    def write_textfile(self, label, data):
        filename = os.path.join(self.path, FILENAME)
        with open(filename, 'a') as f:
            f.write(f'{label},{data}')
    
    def write_success(self):
        success_file = os.path.join(self.path, SUCCESS_FILENAME)
        with open(success_file, 'w') as f:
            f.write(f'')

    def _validate_length(self, item_object):
        '''check length of received list data. 144 is 10mins for 24hrs'''
        if len(item_object['data']) < 144:
            raise Exception(f"{item_object['label']} data is not full, length: {len(item_object['data'])}")

    def iter_data(self, start_time_obj, end_time_obj):
        
        params = {
            'format':'json',
            'language': 'en',
            'Accept-Language' : 'en',
            'date_start' : self._datetime_obj2string(start_time_obj),
            'date_end' : self._datetime_obj2string(end_time_obj),
            'base_columns':'total'
        }

        rsp = self.get(self.dashboard, params=params)
        data = rsp
        for item in data:
            self._validate_length(item)
            logging.debug(item)
            logging.debug(f"{item['label']}, length: {len(item['data'])}")
            self.write_textfile(item['label'], item['data'])

            for i,value_list in enumerate(item['data']):
                item['data'][i][0] = self.unix2datetime(value_list[0])
                installation_id = item['itemid']
                installation_label = item['label']
                type = item['type']
                energy = item['data'][i][1]

                points = {
                        'installation_label' : installation_label,
                        'points': {
                            'energy': energy
                        },
                        'timestamp': item['data'][i][0] # time object
                    }
                logging.debug(points)
                yield points
        self.write_success()
   

def _call_it(date):
    s = EnergyApi(date)
    s.fetch_data()

with DAG(
    'my_dag',
    default_args=default_args,
    description='Download energy data in txt DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime.datetime(2022, 1, 1),
    catchup=True,
    tags=['energy_api'],
) as dag:


    t1 = PythonOperator(
        task_id="run_energy_api",
        python_callable=_call_it,
        op_kwargs={
            'date': '{{ ds }}'
            }
    )
