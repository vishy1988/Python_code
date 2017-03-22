


import datetime as dt
import pandas as pd
import pyodbc
import sqlalchemy
from pandas import Series,DataFrame
import numpy as np
cnx=sqlalchemy.create_engine("mssql+pyodbc://bhatt:DoubleBubble@172.31.163.135:1433/Basis?driver=/opt/microsoft/sqlncli/lib64/libsqlncli-11.0.so.1790.0")
cnx1 = pyodbc.connect('driver=/opt/microsoft/msodbcsql/lib64/libmsodbcsql-11.0.so.2270.0;server=SRVWUDEN0835;database=basis;uid=bhatt; pwd=DoubleBubble')

def load_table(date1):
    strDate1 = date1.strftime('%m/%d/%Y')
    stmt = "insert into Customer_Factor_Queue (ID,Date)  select distinct Send_Customer, Send_Date from Pretty_Txns where Send_Date = '%s' and Send_Customer is not null union select distinct Pay_Customer, Pay_Date from Pretty_Txns where Pay_Date = '%s' and Pay_Customer is not null" % (strDate1,strDate1)
    curs=cnx1.cursor()
    curs.execute(stmt)
    curs.commit()

def make_pid(n):

    stmt="execute mark_customer_queue "+str(n)
    curs=cnx1.cursor()
    curs.execute(stmt)
    pid=curs.fetchone()[0]
    curs.commit()
    return pid

def data(pid):
     sql1 ="""select T.*,id.[Date]
     from Pretty_Txns T with (nolock)
     join Customer_Factor_Queue as id with (nolock)  on id.ID=T.Send_Customer and T.Send_Date<id.[Date]
     where Send_Customer is not null and id.pid = """+str(pid)
     sql2 ="""select T.*,id.[Date]
     from Pretty_Txns T with (nolock)
     join Customer_Factor_Queue as id with (nolock) on id.ID=T.Pay_Customer and T.Pay_Date<id.[Date]
     where Pay_Customer is not null and id.pid = """+str(pid)
     df_send = pd.read_sql(sql1,cnx)
     df_pay = pd.read_sql(sql2,cnx)
     return {"df_send":df_send,"df_pay":df_pay}



def chop_time(d,dict):
    u=dict['df_send']['Send_Date']>(dict['df_send']['Date']-dt.timedelta(d))
    dict['df_send']=dict['df_send'].loc[u,]
    u=dict['df_pay']['Pay_Date']>(dict['df_pay']['Date']-dt.timedelta(d))
    dict['df_pay']=dict['df_pay'].loc[u,]
    return dict
#def bind_customer(dict):
   # dict['df_send']['Send_Customer']=zip(dict['df_send']['Send_Customer'],dict['df_send']['Date'])
   # dict['df_pay']['Pay_Customer']=zip(dict['df_pay']['Pay_Customer'],dict['df_pay']['Date'])
   # return dict

def delete_queue(pid):
    
    curs=cnx1.cursor()
    stmt="delete from Customer_Factor_Queue where pid = "+str(pid)
    curs.execute(stmt)
    curs.commit()
    curs.close()






    



def run(dict,date1,d=0):
    strDate1 = date1.strftime('%m/%d/%Y')
    date2 = date1 - dt.timedelta(days=d)
    strDate2 = date1.strftime('%m/%d/%Y')
    df_send = dict["df_send"]
    df_pay = dict["df_pay"]

    df_send['time_diff'] = pd.to_numeric(df_send['Pay_Time']-df_send['Send_Time'])
    df_send['time_diff'] = df_send['time_diff']/1000000000

    df_pay['time_diff'] = pd.to_numeric(df_pay['Pay_Time']-df_pay['Send_Time'])
    df_pay['time_diff'] = df_pay['time_diff']/1000000000
    df1 = df_send.loc[:,['Send_Agent','Send_Customer','Pay_Customer','Pay_Country','Send_Time','Pay_Time','Send_Amount','Pay_Amount','time_diff','MTCN','Send_Phone','PAY_CHANNEL','Send_ID_Number','Product']]
    df2 = df_pay.loc[:,['Pay_Agent','Pay_Customer','Send_Customer','Send_Country','Send_Time','Pay_Time','Send_Amount','Pay_Amount','time_diff','MTCN','Pay_Phone','Send_Channel','Pay_ID_Number','Product']]
    grouped = df1.groupby('Send_Customer')
    def repeat_count(s):
        return (s.shift() == s).sum()
    def banded_txn1(s):
        return((s >=900.00) &( s <1000.00)).sum()
    def banded_txn2(s):
        return((s >=1800.00) &( s < 2000.00)).sum()
    def banded_txn3(s):
        return((s >=2500.00) &( s < 3000.00)).sum()
    def banded_txn4(s):
        return((s >=7400.00) &( s < 7500.00)).sum()
    def banded_txn5(s):
        return((s >=9900.00) &( s < 9999.00)).sum()
    result = grouped.agg({'Send_Agent':'nunique','Pay_Customer':'nunique','Pay_Country':'nunique','time_diff':['min','max','mean'],'MTCN':'size','Send_Phone':'nunique','PAY_CHANNEL':'nunique','Send_ID_Number':'nunique','Product':'nunique','Send_Amount':['mean',banded_txn1,banded_txn2,banded_txn3,banded_txn4,banded_txn5]})
    grouped = df2.groupby('Pay_Customer')
    result1 = grouped.agg({'Pay_Agent':'nunique','Send_Customer':'nunique','Send_Country':'nunique','time_diff':['min','max','mean'],'MTCN':'size','Pay_Phone':'nunique','Send_Channel':'nunique','Pay_ID_Number':'nunique','Product':'nunique','Pay_Amount':['mean',banded_txn1,banded_txn2,banded_txn3,banded_txn4,banded_txn5]})
    Result = result.merge(result1,left_index=True,right_index=True,how='outer')
    Result.index.names = ['Customer']
    df1 = df_send.loc[:,['Send_Customer','Pay_Customer']]
    df2 = df_pay.loc[:,['Send_Customer','Pay_Customer']]
    df1.columns = ['id1','id2']
    df2.columns = ['id1','id2']
    df4 = pd.concat([df1,df2],axis=0)
    df4 = df4.drop_duplicates()
    df4 = df4['id2'].groupby(df4['id1']).nunique()
    df4 = df4.to_frame()
    df4.columns = ['Total_Customer_Count']
    df4.index.names = ['Customer']
    Final = Result.merge(df4,left_index=True,right_index=True,how='outer')
    df1 = df_send.loc[:,['Send_Agent','Send_Customer','Send_Phone','Send_Channel','Send_ID_Number']]
    df2 = df_pay.loc[:,['Pay_Agent','Pay_Customer','Pay_Phone','Pay_Channel','Pay_ID_Number']]
    df1.columns = ['Agent','Customer','Phone','Channel','ID_Number']
    df2.columns = ['Agent','Customer','Phone','Channel','ID_Number']
    df3 = pd.concat([df1,df2],axis=0)
    df3 = df3.drop_duplicates()
    df3 = df3.groupby('Customer').agg({'Agent':'nunique','Phone':'nunique','Channel':'nunique','ID_Number':'nunique'})
    df3.columns = ['Total_Agent_Count','Total_Phone_Count','Total_Channel_Count','Total_ID_Count']
    Final = Final.merge(df3,left_index=True,right_index=True,how='outer')
    df1 = df_send.loc[:,['Send_Customer','Send_Amount','Pay_Country']]
    df2 = df_pay.loc[:,['Pay_Customer','Pay_Amount','Send_Country']]
    df3 = df1.groupby(['Send_Customer','Pay_Country']).agg({'Send_Amount':'sum'})
    df3 = df3.reset_index(level=['Send_Customer','Pay_Country'])
    df4 = df3.groupby('Send_Customer').agg({'Send_Amount':'max'})
    df4 = df4.reset_index(level=['Send_Customer'])
    df5 = pd.merge(df4,df3,how='left')
    df5 = df5.drop_duplicates(['Send_Customer','Send_Amount'])
    df5 = df5.drop(labels=['Send_Amount'],axis=1)
    df5 = df5.set_index('Send_Customer')
    df5.index.names = ['Customer']
    df5.columns  = ['Primary_Send_Country']
    df3 = df2.groupby(['Pay_Customer','Send_Country']).agg({'Pay_Amount':'sum'})
    df3 = df3.reset_index(level=['Pay_Customer','Send_Country'])
    df4 = df3.groupby('Pay_Customer').agg({'Pay_Amount':'max'})
    df4 = df4.reset_index(level=['Pay_Customer'])
    df6 = pd.merge(df4,df3,how='left')
    df6 = df6.drop_duplicates(['Pay_Customer','Pay_Amount'])
    df6 = df6.set_index('Pay_Customer')
    df6.index.names = ['Customer']
    df6 = df6.drop(labels=['Pay_Amount'],axis=1)
    df6.columns = ['Primary_Pay_Country']
    df7 = pd.merge(df5,df6,left_index='True',right_index='True',how='outer')
    Final = pd.merge(Final,df7,left_index=True,right_index=True,how='outer')
    df1.columns = ['Customer','Amount','Country']
    df2.columns = ['Customer','Amount','Country']
    df3 = pd.concat([df1,df2],axis=0)
    df4 = df3.groupby(['Customer','Country']).agg({'Amount':'sum'})
    df4 = df4.reset_index(level=['Customer','Country'])
    df5 = df4.groupby(['Customer']).agg({'Amount':'max'})
    df5 = df5.reset_index(level=['Customer'])
    df6 = pd.merge(df5,df4,how='left')
    df6 = df6.drop_duplicates(['Customer','Amount'])
    df6 = df6.drop(labels=['Amount'],axis=1)
    df6 = df6.set_index('Customer')
    df6.columns = ['Primary_Country']
    Final = pd.merge(Final,df6,left_index=True,right_index=True,how='outer')
    sql2 = "select Country_Code, Rating from GeoRisk"
    df3 = pd.read_sql(sql2,cnx)
    df1.columns = ['Customer','Amount','Country_Code']
    df2.columns = ['Customer','Amount','Country_Code']
    df4 = pd.merge(df1,df3,how='left')
    df4 = df4[df4['Rating']=='Higher']
    dfa = df4.groupby('Customer').agg({'Country_Code':'nunique','Amount':'sum'})
    dfa.columns = ['HRJ_Send_Amount','HRJ_Send_Country_Count']
    df5 = pd.merge(df2,df3,how='left')
    df5 = df5[df5['Rating']=='Higher']
    dfb = df5.groupby('Customer').agg({'Country_Code':'nunique','Amount':'sum'})
    dfb.columns = ['HRJ_Pay_Amount','HRJ_Pay_Country_Count']
    dfc = pd.merge(dfa,dfb,left_index='True',right_index='True',how='outer')
    Final = pd.merge(Final,dfc,left_index=True,right_index=True,how='outer')
    df1 = df_send.loc[:,['Send_Customer','Send_Time']]
    df2 = df_pay.loc[:,['Pay_Customer','Pay_Time']]
    df1.columns = ['Customer','Time']
    df2.columns = ['Customer','Time']
    df3 = pd.concat([df1,df2],axis=0)
    df3 = df3.drop_duplicates()
    df3 = df3.reset_index()
    df3['Flipped_Txns_Count'] = df3.groupby('Customer')['Time'].apply(lambda x : x.diff().abs()<=dt.timedelta(seconds=600))
    df3 = df3[df3['Flipped_Txns_Count']==True]
    df3 = df3.groupby('Customer').agg({'Flipped_Txns_Count':'size'})
    Final = pd.merge(Final,df3,left_index=True,right_index=True,how='outer')
    df1 = df_send.loc[:,['Send_Customer','Pay_Customer','time_diff']]
    df1 = df1[df1['time_diff']<=600]
    df2 = df1.groupby('Send_Customer').agg({'time_diff':'size'})
    df2.index.names = ['Customer']
    df2.columns = ['Send_Rapid_Payout_Txns']
    df1 = df_pay.loc[:,['Send_Customer','Pay_Customer','time_diff']]
    df1 =df1[df1['time_diff']<=600]
    df3 = df1.groupby('Pay_Customer').agg({'time_diff':'size'})
    df3.index.names = ['Customer']
    df3.columns = ['Pay_Rapid_Payout_Txns']
    df4 = pd.merge(df2,df3,left_index=True,right_index=True,how='outer')
    Final = pd.merge(Final,df4,left_index=True,right_index=True,how='outer')
    df1 =df_send.loc[:,['Send_Customer','Send_Amount','Send_Time']]
    df1 = df1.sort_values(by=['Send_Time'])
    dfa = df1.groupby('Send_Customer').agg({'Send_Amount':repeat_count})
    dfa.index.names = ['Customer']
    dfa.columns = ['Send_Repeat_Count']
    df2 =df_pay.loc[:,['Pay_Customer','Pay_Amount','Pay_Time']]
    df2 = df2.sort_values(by=['Pay_Time'])
    dfb = df2.groupby('Pay_Customer').agg({'Pay_Amount':repeat_count})
    dfb.index.names = ['Customer']
    dfb.columns = ['Pay_Repeat_Count']
    dfc = pd.merge(dfa,dfb,left_index=True,right_index=True,how='outer')
    Final = pd.merge(Final,dfc,left_index=True,right_index=True,how='outer')
    df = df_send.loc[:,['Send_Customer','Pay_Customer','Send_Time']]
    df = df.sort_values(by=['Send_Time'])
    df['unique'] = df.groupby('Send_Customer')['Pay_Customer'].apply(lambda x:x.shift(-1)!=x)
    df = df[df['unique']==True]
    df['time_diff'] = df.groupby('Send_Customer')['Send_Time'].apply(lambda x : x.diff().abs()<=dt.timedelta(seconds=36000))
    def func1(x):
        return (x==True).sum()
    def func2(x):
        return ((x==False)&(x.shift(-1)==True)).sum()
    dfa = df.groupby('Send_Customer').agg({'time_diff':[func1,func2]})
    dfa.columns = ['count1','count2']
    dfa['Total_Count'] = dfa['count1'] + dfa['count2']
    dfa = dfa[dfa['Total_Count']>0]
    dfa = dfa.drop(labels=['count1','count2'],axis=1)
    dfa.columns = ['One_To_Many_Send']
    dfa.index.names = ['Customer']
    Final = pd.merge(Final,dfa,left_index='True',right_index='True',how='outer')
    df = df_pay.loc[:,['Send_Customer','Pay_Customer','Pay_Time']]
    df = df.sort_values(by=['Pay_Time'])
    df['unique'] = df.groupby('Pay_Customer')['Send_Customer'].apply(lambda x:x.shift(-1)!=x)
    df['time_diff'] = df.groupby('Pay_Customer')['Pay_Time'].apply(lambda x : x.diff().abs()<=dt.timedelta(seconds=36000))
    dfa = df.groupby('Pay_Customer').agg({'time_diff':[func1,func2]})
    dfa.columns = ['count1','count2']
    dfa['Total_Count'] = dfa['count1'] + dfa['count2']
    dfa = dfa[dfa['Total_Count']>0]
    dfa = dfa.drop(labels=['count1','count2'] ,axis=1)
    dfa.columns = ['Many_To_One_Pay']
    dfa.index.names = ['Customer']
    Final = pd.merge(Final,dfa,left_index='True',right_index='True',how='outer')
    df1 = df_send.loc[:,['Send_Customer','Send_Time']]
    df2 = df_pay.loc[:,['Pay_Customer','Pay_Time']]
    df1 = df1.sort_values(by=['Send_Time'])
    df2 = df2.sort_values(by=['Pay_Time'])
    def  func1(s):
        return s.diff().abs().min()
    def  func2(s):
        return s.diff().abs().max()
    def  func3(s):
        return s.diff().abs().mean()
    def fn(x):
        if type(x)==pd.tslib.Timedelta:
            y=x/np.timedelta64(1,'s')
            return y
        else:
            y=x
            return y
    df3 = df1.groupby('Send_Customer').agg({'Send_Time':['min',func1,func2,func3]})
    df3.index.names = ['Customer']
    df4 = df2.groupby('Pay_Customer').agg({'Pay_Time':['min',func1,func2,func3]})
    df4.index.names = ['Customer']
    df4.columns = ['Min_Pay_Time','Min_Interval_Pay_Txns','Max_Interval_Pay_Txns','Avg_Interval_Pay_Txns']
    df3.columns = ['Min_Send_Time','Min_Interval_Send_Txns','Max_Interval_Send_Txns','Avg_Interval_Send_Txns']
    df5 = pd.merge(df3,df4,left_index=True,right_index=True,how='outer')
    df5['Min_Interval_Send_Txns'] = map(fn,df5.Min_Interval_Send_Txns)
    df5['Max_Interval_Send_Txns'] = map(fn,df5.Max_Interval_Send_Txns)
    df5['Avg_Interval_Send_Txns'] = map(fn,df5.Avg_Interval_Send_Txns)
    df5['Min_Interval_Pay_Txns'] = map(fn,df5.Min_Interval_Pay_Txns)
    df5['Max_Interval_Pay_Txns'] =  map(fn,df5.Max_Interval_Pay_Txns)
    df5['Avg_Interval_Pay_Txns']  = map(fn,df5.Avg_Interval_Pay_Txns)
    
    if df5.Avg_Interval_Send_Txns.dtype ==  '<M8[ns]':
        df5.Avg_Interval_Send_Txns = df5.Avg_Interval_Send_Txns.dt.second

    if df5.Min_Interval_Send_Txns.dtype  == '<M8[ns]':
        df5.Min_Interval_Send_Txns  = df5.Min_Interval_Send_Txns.dt.second

    if df5.Max_Interval_Send_Txns.dtype  == '<M8[ns]':
        df5.Max_Interval_Send_Txns  = df5.Max_Interval_Send_Txns.dt.second


    if df5.Avg_Interval_Pay_Txns.dtype  == '<M8[ns]':
        df5.Avg_Interval_Pay_Txns  = df5.Avg_Interval_Pay_Txns.dt.second

    if df5.Min_Interval_Pay_Txns.dtype  == '<M8[ns]':
        df5.Min_Interval_Pay_Txns  = df5.Min_Interval_Pay_Txns.dt.second

    if df5.Max_Interval_Pay_Txns.dtype == '<M8[ns]':
        df5.Max_Interval_Pay_Txns  = df5.Max_Interval_Pay_Txns.dt.second
    Final = pd.merge(Final,df5,left_index='True',right_index='True',how='outer')
    Final = Final.reset_index()
    a = df_send.Date.tolist()
    Final['Date']=a[0]
    Final = Final.set_index(['Customer','Date'])
    Final.index.names = ['ID','Date']
    #sqlquery = "select Time_Index from Model.Time_Index where t1 = '%s' and t2 = '%s'" % (strDate1,strDate2) */

    #df1 = pd.read_sql(sqlquery,cnx)
    #Final['Time_Index'] = df1['Time_Index'][0]
    sqlquery = "select  * from  Results.Outcomes where   CASE_CREATION_DATE >= '%s' and CASE_CREATION_DATE <= '%s'" % (strDate1,strDate2)
    df = pd.read_sql(sqlquery,cnx)
    df3 = df[df['SUBJECT_TYPE']=='Consumer by Galactic ID']
    df4 = df3.groupby('SUBJECT_ID').agg({'FIU_Risk_Level_1':'sum','FIU_Risk_Level_2':'sum','FIU_Risk_Level_3':'sum','FIU_Risk_Level_4':'sum','FIU_Risk_Level_5':'sum','High_Risk_GFO':'sum','Medium_Risk_GFO':'sum','No_Risk_GFO':'sum'})
    df4.index.names = ['ID']

    
    Final = pd.merge(Final,df4,left_index=True,right_index=True,how='outer')
    
    
    Final.columns = ['Pay_Customer_Count','Pay_Country_Count','Send_Txn_Count','Pay_Channel_Count','Avg_Send_Amount','Send_Banded_Count_Class1','Send_Banded_Count_Class2','Send_Banded_Count_Class3','Send_Banded_Count_Class4','Send_Banded_Count_Class5','Send_Phone_Count','Send_ID_Count','Send_Agent_Count','Min_Send_Payout_Speed','Max_Send_Payout_Speed','Avg_Send_Payout_Speed','Send_Product_Count','Send_Customer_Count','Pay_Phone_Count','Pay_Txn_Count','Send_Country_Count','Avg_Pay_Amount','Pay_Banded_Count_Class1','Pay_Banded_Count_Class2','Pay_Banded_Count_Class3','Pay_Banded_Count_Class4','Pay_Banded_Count_Class5','Pay_Agent_Count','Min_Pay_Payout_Speed','Max_Pay_Payout_Speed','Avg_Pay_Payout_Speed','Pay_ID_Count','Send_Channel_Count','Pay_Product_Count','Total_Customer_Count','Total_Agent_Count','Total_Phone_Count','Total_Channel_Count','Total_ID_Count','Primary_Send_Country','Primary_Pay_Country','Primary_Country','HRJ_Send_Amount','HRJ_Send_Country_Count','HRJ_Pay_Amount','HRJ_Pay_Country_Count','Flipped_Txns_Count','Send_Rapid_Pay_Out_Txns','Pay_Rapid_Pay_Out_Txns','Send_Repeat_Amount_Txns','Pay_Repeat_Amount_Txns','One_To_Many_Send_Count','Many_To_One_Pay_Count','Min_Send_Time','Min_Interval_Send_Txns','Max_Interval_Send_Txns','Avg_Interval_Send_Txns','Min_Pay_Time','Min_Interval_Pay_Txns','Max_Interval_Pay_Txns','Avg_Interval_Pay_Txns','FIU_Risk_Level_4','FIU_Risk_Level_5','FIU_Risk_Level_1','FIU_Risk_Level_2','FIU_Risk_Level_3','No_Risk_GFO','High_Risk_GFO','Medium_Risk_GFO']
    return Final   
   #Final.to_sql("Factor_Data_Test",cnx,if_exists="append",index_label=Final.index.name,dtype={Final.index.name: sqlalchemy.sql.sqltypes.VARCHAR(50)})

#if __name__=='__main__':
    #func1(dt.datetime(2016,1,1),dt.datetime(2016,1,2))

if __name__=='__main__':
    load_table(dt.datetime(2017,3,7))
    pid=make_pid(50000)
    dict=data(pid)
    #dict=bind(dict)
    full_set=run(dict,dt.datetime(2017,3,7))
    dict=chop_time(61,dict)
    sixty_set=run(dict,dt.datetime(2017,3,7),60)
    sixty_set.rename(columns=lambda x: '60_'+str(x),inplace=True)
    dict=chop_time(31,dict)
    thirty_set=run(dict,dt.datetime(2017,3,7),30)
    thirty_set.rename(columns=lambda x: '30_'+str(x),inplace=True)
    dict=chop_time(8,dict)
    seven_set=run(dict,dt.datetime(2017,3,7),7)
    seven_set.rename(columns=lambda x: '7_'+str(x),inplace=True)
    def mg(x,y):
        return x.merge(y,left_index=True,right_index=True,how='outer')
    out=reduce(mg,[full_set,sixty_set,thirty_set,seven_set])
    out.fillna(0,inplace=True)
    out.index=pd.MultiIndex.from_tuples(out.index,names=('ID','Date'))
    
    out.to_sql("Customer_Batch_test",cnx,if_exists="append",index=True,dtype={'ID':sqlalchemy.types.VARCHAR(50),'Date':sqlalchemy.types.Date})
    delete_queue(pid)






