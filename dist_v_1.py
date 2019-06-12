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
    #d_df = pd.DataFrame(d_df[(d_df['time_delta']>(pd.Timedelta(sec, unit='s'))) | ((d_df['time_delta']==pd.Timedelta(0, unit='s')))])
    d_df.reset_index(inplace=True,drop=True)
    return d_df


def dates(d_df,sec=500):
    d_df = d_df.sort_values(by=['date','hour'])
    d_df['pre_date'] = d_df.loc[0,'date']
    d_df.loc[1:,'pre_date'] = d_df.loc[0:(df_.shape[0]-2),'date'].values
    d_df['time_delta'] = d_df.loc[:,'date'] - d_df.loc[:,'pre_date']
    d_df = pd.DataFrame(d_df[(d_df['time_delta']>(pd.Timedelta(sec, unit='s'))) | ((d_df['time_delta']==pd.Timedelta(0, unit='s')))])
    return d_df


def dates2(d_df,sec=500):
    d_df = d_df.sort_values(by=['date','hour'])
    d_df['pre_date'] = d_df.loc[0,'date']
    d_df.loc[1:,'pre_date'] = d_df.loc[0:(df_.shape[0]-2),'date'].values
    d_df['time_delta'] = d_df.loc[:,'date'] - d_df.loc[:,'pre_date']
    mask = (d_df['time_delta'].dt
          .seconds
          .between(0,sec,
                   inclusive=False)
       )

    d_df = d_df[mask]
    return d_df

def clean_noise1(d_df,sec=500):
    d_df = d_df.sort_values(by=['date','hour'])
    d_df['pre_date'] = d_df.loc[0,'date']
    a_coords = np.array(d_df.loc[:,['attributes.latitude','attributes.longitude']])
    b_coords = np.zeros((a_coords.shape[0],a_coords.shape[1]))
    b_coords[0]= a_coords[0]
    b_coords[1:] = a_coords[:-1]
    d_df['distance'] = vec_haversine.vec_haversine(a_coords[:,0],a_coords[:,1],b_coords[:,0],b_coords[:,1])
    d_df.loc[1:,'pre_date'] = d_df.loc[0:(df_.shape[0]-2),'date'].values
    d_df['time_delta'] = d_df.loc[:,'date'] - d_df.loc[:,'pre_date']
    mask = (d_df['time_delta'].dt
          .seconds
          .between(0,sec,
                   inclusive=False)
       )

    d_df = d_df[mask]
    d_df.reset_index(inplace=True,drop=True)
    return d_df


def charge(df__):
    df__['pre_battery'] = df__.loc[0,'attributes.battery_level']
    df__.loc[1:,'pre_battery'] = df__.loc[0:(df__.shape[0]-2),'attributes.battery_level'].values
    df__['charge'] = False
    df__.loc[(((df_['attributes.battery_level'] == 'high')&(df__['time_delta']>=7200))&((df__['pre_battery']=='low')|(df__['pre_battery']=='medium'))),'charge'] = True
    return df__

    

def charge1(df__):
    for i in range(df_.shape[0]-1, -1, -1):
            if i != 0 and ((df_.loc[i,'attributes.battery_level'] == 'high')&(df_.loc[i,'time_delta'] >=7200 ))&((df_.loc[i-1,'attributes.battery_level'] == 'low')|(df_.loc[i-1,'attributes.battery_level'] == 'medium')):
                df_.loc[i,'charge']= True
            else:
                df_.loc[i,'charge']= False
    return df__

def revenue(df,speed=20,const_rev=5,const_charge=30):
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

                
data = pd.read_csv(r'unique_limes_v1.csv',parse_dates=['datetime'])
df_data = data.copy()
df_data = df_data.round({'attributes.latitude':6,'attributes.longitude':6})
df_data.drop_duplicates(subset=['attributes.latitude','attributes.longitude','attributes.plate_number'],inplace=True,keep='last')
df_data.reset_index(drop=True,inplace=True)  
df_data['date'] = pd.to_datetime(df_data['attributes.last_activity_at'].apply(lambda x: x.split('T')[0].replace('-','/')+' '+x.split('T')[1].split('.')[0]),format='%Y/%m/%d')
df_data['hour'] = df_data['attributes.last_activity_at'].apply(lambda x: x.split('T')[1].split('.')[0])
df_data = df_data.sort_values(by=['date','hour'])
              
    

clean_df = pd.DataFrame(data=df_data.iloc[0,:],columns=list(df_data.columns))
plates = df_data['attributes.plate_number'].unique()
for plate in range(len(plates)):
    #print(plate)
    time0= time.time()
    df_ =  df_data[df_data['attributes.plate_number']==plates[plate]].reset_index(drop=True)
# =============================================================================
#     df_ = clean_noise(df_)
#     df_ = charge(df_)
#     clean_df = pd.concat([clean_df,df_])
# =============================================================================
    print(time.time()-time0)

clean_df.reset_index(inplace=True,drop=True)
clean_df = revenue(clean_df)
#clean_df= clean_df.sort_values(by=['date','hour']).drop_duplicates(subset=['attributes.latitude','attributes.longitude','attributes.plate_number'],inplace=True,keep='last')
clean_df_flt = clean_df[clean_df.distance>0.5]

riders = clean_df_flt.groupby(clean_df_flt.date[clean_df_flt.charge==False])['total_revenue'].agg(['sum','count'])
chargers = clean_df_flt.groupby(clean_df_flt.date[clean_df_flt.charge==True])['expenses'].agg(['sum','count'])
combine = riders.join(chargers,how='outer',lsuffix='_riders', rsuffix='_charge')
path = 'reports/Limes_PnL_Report_'+str(datetime.datetime.now()).split(" ")[0].replace("-","_")+'.xlsx'
#clean_df_flt.to_csv(path,index=False)
writer = pd.ExcelWriter(path, engine='xlsxwriter')
riders.to_excel(writer, sheet_name='riders')
chargers.to_excel(writer, sheet_name='chargers')
combine.to_excel(writer, sheet_name='combine')
clean_df_flt.to_excel(writer, sheet_name='Data')
writer.save()


'''
a = clean_df_flt[clean_df_flt['attributes.plate_number']=='XXX-799']
a.drop_duplicates(subset=['attributes.latitude','attributes.longitude','attributes.plate_number'],inplace=True,keep='last')
a=a.round({'attributes.latitude':5,'attributes.longitude':5})
b=dict(a['attributes.latitude'])


a = df_data[df_data['attributes.plate_number']==plates[plate]].reset_index(drop=True)
a_coords = np.array(a.loc[:,['attributes.latitude','attributes.longitude']])
b_coords = np.zeros((a_coords.shape[0],a_coords.shape[1]))
b_coords[0]= a_coords[0]

b_coords[1:] = a_coords[:-1]
a_coords[:,0]
import vec_haversine
def clean_noiset(d_df):
    d_df = d_df.sort_values(by='datetime')
    for i in range(d_df.shape[0]-1, -1, -1):
        if i==0 :
            a = d_df.loc[i,['attributes.latitude','attributes.longitude']]
            dist =  haversine.distance(tuple(a),tuple(a))
            d_df.loc[i,'distance'] = dist
        else:
            a = d_df.loc[i,['attributes.latitude','attributes.longitude']]
            b = d_df.loc[i-1,['attributes.latitude','attributes.longitude']]
            dist =  haversine.distance(tuple(a),tuple(b))
            d_df.loc[i,'distance'] = dist
    return d_df
time0 = time.time()
vec_haversine.vec_haversine(a_coords[:,0],a_coords[:,1],b_coords[:,0],b_coords[:,1])
print(time.time()-time0)
time0 = time.time()
clean_noiset(a)
print(time.time()-time0)
t_delta = (a.loc[i,'date']-a.loc[i-1,'date']).components

g = np.concatenate((a.loc[:,'date'],np.concatenate((np.array(a.loc[0,'date'],a.loc[:,'date'][:-1])),axis=1)))

g = np.concatenate((np.array(a.loc[0,'date']),a.loc[:,'date'][:-1]),axis=0)

'''
a = df_data[df_data['attributes.plate_number']=='XXX-799'].reset_index(drop=True)
d= np.array(a.loc[:,'date'][0])
dd= np.array(a.loc[:,'date'][:-1])

df_['pre_date'] = np.datetime64(df_.loc[0,'date'])
df_.loc[:,'pre_date'][1:] = df_.loc[:,'date'][:-1]
df_['time_delta'] = df_.loc[:,'date'] - df_.loc[:,'pre_date']
df_=a[(a['time_delta']>(pd.Timedelta(500, unit='s'))) | ((a['time_delta']==pd.Timedelta(0, unit='s')))]

a = np.datetime64(df_.loc[0,'date'])
a[1:] = np.array(df_.loc[:,'attributes.last_activity_at'][:-1])

df_ =  df_data[df_data['attributes.plate_number']==plates[plate]].reset_index(drop=True)

df_ =  df_data[df_data['attributes.plate_number']==plates[plate]].reset_index(drop=True)

time0= time.time()
df_ =  df_data[df_data['attributes.plate_number']==plates[plate]].reset_index(drop=True)
print(time.time()-time0)

time0= time.time()
df_ =  df_data.iloc[df_data['attributes.plate_number']==plates[plate],:].reset_index(drop=True)
print(time.time()-time0)

time0= time.time()
df_ = clean_noise(df_data)
print(time.time()-time0)

time0= time.time()
df_ = charge(df_)
print(time.time()-time0)
time0= time.time()
df_ = charge1(df_)
print(time.time()-time0)





df_['pre_date'] = df_.loc[0,'date']
df_.loc[1:,'pre_date'] = df_.loc[0:(df_.shape[0]-2),'date'].values

df_['pre_battery'] = df_.loc[0,'attributes.battery_level']
df_.loc[1:,'pre_battery'] = df_.loc[0:(df_.shape[0]-2),'attributes.battery_level'].values
df_['charge'] = False
df_.loc[(((df_['attributes.battery_level'] == 'high')&(df_['time_delta']>=pd.Timedelta(2, unit='h')))&((df_['pre_battery']=='low')|(df_['pre_battery']=='medium'))),'charge'] = True



df_data.groupby('just_date').agg({'attributes.plate_number':['nunique', 'unique'],})
'''
dist = haversine.distance((32.091242,34.783005),(32.075377,34.765634))
data.to_csv(r'limes_data.csv')

api_key = 'AIzaSyByEPBWgiTdBNgjagFHP-uBUDgtbNKq0GA' 

import googlemaps 
# Requires API key 
gmaps = googlemaps.Client(key=api_key)  
# Requires cities name 
distance = gmaps.distance_matrix((32.068286,34.764821000000005),(32.076879999999996,34.771529),mode='bicycling')['rows'][0]['elements'][0]       
'''