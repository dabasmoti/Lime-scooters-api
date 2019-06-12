# -*- coding: utf-8 -*-
"""
Created on Mon May 13 13:50:19 2019

@author: z
"""

from pandas.io.json import json_normalize
import requests
import json 
import pandas as pd
import time
import schedule
import datetime

def lime_head():
    '''
    TODO - Add Authorization key!
    '''
    params_ = {"Authorization":'Bearer  }
    return params_


def get(url_,params_):
    gr = requests.get(url_,headers=params_)
    con = json.loads(gr.content)
    df_ = json_normalize(con['data']['attributes']['bikes'],record_prefix=False)
    df_['datetime'] = datetime.datetime.now()
    df_['datetime'] = pd.to_datetime(df_['datetime'])
    return df_


def get_url(a,b):
    return  ' https://web-production.lime.bike/api/rider/v1/views/map?ne_lat={0}&ne_lng={1}&sw_lat={0}&sw_lng={1}&user_latitude={0}&user_longitude={1}&zoom=1000'.format(a,b)


def unique(csv_path=None):
    global df_all
    if csv_path is not  None:
        df_all = pd.read_csv(csv_path,parse_dates = ['datetime'],float_precision='round_trip')
    else:
        df_all = get(get_url(lat[0],long[0]),params_=lime_head())
    get_df = pd.DataFrame()
    for j in range(len(lat)):
        try:
            get_df = pd.concat([get_df,get(get_url(lat[j],long[j]),params_=lime_head())])
        except:
            continue
    time.sleep(1)
    get_df = get_df.round({'attributes.latitude':6,'attributes.longitude':6})
    get_df.drop_duplicates(subset=['attributes.plate_number'],inplace=True,keep='last')
    df_all = pd.concat([df_all,get_df.loc[:,columns]],ignore_index=True,sort=False)
    df_all.to_csv(csv_path,index=False,float_format="%.6f")    
    print('Number of unique limes: {}'.format(df_all['attributes.plate_number'].unique().size))




def  fix_hour(num):
    if num<10:
        return str('0'+str(num))
    else:
        return str(num)
    
    
def create_sched(sched,func,arg):
    interval,h =0,0 
    for i in range(sched[2]):
        if  interval==60:
            h += 1
            interval = 0
            schedule.every().day.at('{}:{}'.format(fix_hour(sched[0]+h),fix_hour(interval))).do(func,arg)
            print('Schedule configured on {}:{}'.format(fix_hour(sched[0]+h),fix_hour(interval)))
            interval += sched[1]
        else:
            schedule.every().day.at('{}:{}'.format(fix_hour(sched[0]+h),fix_hour(interval))).do(func,arg)
            print('Schedule configured on {}:{}'.format(fix_hour(sched[0]+h),fix_hour(interval)))
            interval += sched[1]
    while True:
        schedule.run_pending()
        time.sleep(1)


global lat
lat = [32.092076,32.084231,32.073095,32.070538,32.063919,32.063919,32.124504,32.113513,32.103737,32.084741,32.073459,32.054153,32.051052,32.066788,32.057688]
global long
long = [34.782682,34.776095,34.767598,34.769798,34.772824,34.772824,34.803814,34.804189,34.804630,34.798515,34.793204,34.784843,34.789896,34.808532,34.809782]
global columns
columns = ['attributes.battery_level', 'attributes.last_activity_at','attributes.latitude','attributes.longitude','attributes.meter_range', 'attributes.plate_number','datetime', 'id']

csv_path = r'D:\lime\unique_limes_v1.csv'
unique()
schedule.every(7).minutes.do(unique,csv_path)
while True:
    schedule.run_pending()
    time.sleep(1)
