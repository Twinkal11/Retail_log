import pandas as pd

df = pd.read_csv("/content/sunil12.csv")

df

df['OrderNumber']=df['OrderNumber'].str[2:].astype(int)

df['Orderdate']=pd.to_datetime(df['Orderdate'],format='%m/%d/%Y')
df['Duedate']=pd.to_datetime(df['Duedate'],format='%m-%d-%Y')
df['Shipdate']=pd.to_datetime(df['Shipdate'],format='%m-%d-%Y')

df

from datetime import timedelta

df1=df.copy()
for i in range(1,3):
  #df['OrderNumber']=df['OrderNumber'].str[2:].astype(int)
  df['OrderNumber']=df['OrderNumber']+1000*i
  df['UnitPrice']= df['UnitPrice']+i*(500)
  df['Orderdate']= df['Orderdate']+pd.to_timedelta(1*i,unit='d')
  df['Duedate'] = df['Duedate']+pd.to_timedelta(1*i,unit='d')
  df['Shipdate']=df['Shipdate']+pd.to_timedelta(i*i,unit='d')
  df1=pd.concat([df1,df])

df1['OrderNumber'] = 'SO' + df1['OrderNumber'].astype(str)

df1

df1.to_csv("sss.csv")