# -*- coding: utf-8 -*-
"""
Created on Sun May 19 13:05:37 2019

@author: z
"""

import pandas as pd
import vec_haversine
import datetime
import numpy as np


def clean_noise(d_df,sec=500):
    d_df = d_df.sort_values(by=['date','hour'])
    d_df['pre_date'] = d_df.loc[0,'date']
    a_coords = np.array(d_df.loc[:,['attributes.latitude','attributes.longitude']])
    b_coords = np.zeros((a_coords.shape[0],a_coords.shape[1]))
    b_coords[0]= a_coords[0]
    b_coords[1:] = a_coords[:-1]
    d_df['distance'] = vec_haversine.vec_haversine(a_coords[:,0],a_coords[:,1],b_coords[:,0],b_coords[:,1])
    d_df.loc[1:,'pre_date'] = d_df.loc[0:(df_.shape[0]-2),'date'].values
    d_df['time_delta'] = (d_df.loc[:,'date'] - d_df.loc[:,'pre_date']).dt.total_seconds()
    d_df  = d_df[(d_df['time_delta']==0) | (d_df['time_delta']>500)]
    d_df.reset_index(inplace=True,drop=True)
    return d_df


def dates(d_df,sec=500):
    d_df = d_df.sort_values(by=['date','hour'])
    d_df['pre_date'] = d_df.loc[0,'date']
    d_df.loc[1:,'pre_date'] = d_df.loc[0:(df_.shape[0]-2),'date'].values
    d_df['time_delta'] = d_df.loc[:,'date'] - d_df.loc[:,'pre_date']
    d_df = pd.DataFrame(d_df[(d_df['time_delta']>(pd.Timedelta(sec, unit='s'))) | ((d_df['time_delta']==pd.Timedelta(0, unit='s')))])
    return d_df



def charge(df__):
    df__['pre_battery'] = df__.loc[0,'attributes.battery_level']
    df__.loc[1:,'pre_battery'] = df__.loc[0:(df__.shape[0]-2),'attributes.battery_level'].values
    df__['charge'] = False
    df__.loc[(((df_['attributes.battery_level'] == 'high')&(df__['time_delta']>=7200))&((df__['pre_battery']=='low')|(df__['pre_battery']=='medium'))),'charge'] = True
    return df__

    

def revenue(df,speed=20,const_rev=5,const_charge=22):
    df['duration'] = df['distance'].apply(lambda x: x/(speed/60))
    df['min_cost_ILS'] = df.duration*0.5
    df['constant_revenue']=const_rev
    df['total_revenue']=df.min_cost_ILS+df.constant_revenue
    df['date'] = df['datetime'].dt.date
    df.loc[df.charge==False,'expenses'] = 0
    df.loc[df.charge==True,'expenses'] = const_charge
    return df

def check_obj_columns(dfx):
    tdf = dfx.select_dtypes(include=['object']).applymap(type)
    for col in tdf:
        if len(set(tdf[col].values)) > 1:
            print("Column {} has mixed object types.".format(col))

                
data = pd.read_csv(r'D:\lime\unique_limes_v1.csv',parse_dates=['datetime'])
df_data = data.copy()
df_data = df_data.round({'attributes.latitude':6,'attributes.longitude':6})
df_data.drop_duplicates(subset=['attributes.latitude','attributes.longitude','attributes.plate_number'],inplace=True,keep='last')
df_data.reset_index(drop=True,inplace=True)  
df_data['date'] = pd.to_datetime(df_data['attributes.last_activity_at'].apply(lambda x: x.split('T')[0].replace('-','/')+' '+x.split('T')[1].split('.')[0]),format='%Y/%m/%d')
df_data['date'] = pd.to_datetime(df_data['attributes.last_activity_at'].apply(lambda x: x.split('T')[0].replace('-','/')))

df_data['hour'] = df_data['attributes.last_activity_at'].apply(lambda x: x.split('T')[1].split('.')[0])
df_data = df_data.sort_values(by=['date','hour'])
              
    

clean_df = pd.DataFrame(data=df_data.iloc[0,:],columns=list(df_data.columns))
plates = df_data['attributes.plate_number'].unique()
for plate in range(len(plates)):
    print(plate)
    df_ =  df_data[df_data['attributes.plate_number']==plates[plate]].reset_index(drop=True)
    df_ = clean_noise(df_)
    df_ = charge(df_)
    clean_df = pd.concat([clean_df,df_])
clean_df.reset_index(inplace=True,drop=True)
clean_df = revenue(clean_df)
#clean_df= clean_df.sort_values(by=['date','hour']).drop_duplicates(subset=['attributes.latitude','attributes.longitude','attributes.plate_number'],inplace=True,keep='last')
clean_df_flt = clean_df[(clean_df.distance>0.5)&(clean_df.charge==False)]

riders = clean_df_flt.groupby(clean_df_flt.date[clean_df_flt.charge==False])['total_revenue'].agg(['sum','count'])
chargers = clean_df.groupby(clean_df.date[clean_df.charge==True])['expenses'].agg(['sum','count'])
combine = riders.join(chargers,how='outer',lsuffix='_riders', rsuffix='_charge')
path = 'reports/Limes_PnL_Report_'+str(datetime.datetime.now()).split(" ")[0].replace("-","_")+'.xlsx'
#clean_df_flt.to_csv(path,index=False)
writer = pd.ExcelWriter(path, engine='xlsxwriter')
riders.to_excel(writer, sheet_name='riders')
chargers.to_excel(writer, sheet_name='chargers')
combine.to_excel(writer, sheet_name='combine')
clean_df.to_excel(writer, sheet_name='Data')
writer.save()

df_unique_per_day = clean_df_flt.groupby(['date','attributes.plate_number'])['attributes.plate_number'].count().sort_values(ascending=False)

