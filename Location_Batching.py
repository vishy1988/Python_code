import datetime as dt
import pandas as pd
import pyodbc
import sqlalchemy
from pandas import Series,DataFrame
import numpy as np
from multiprocessing import Process
from multiprocessing import Pool, TimeoutError


cnx=sqlalchemy.create_engine("mssql+pyodbc://bhatt:DoubleBubble@172.31.163.135:1433/Basis?driver=/opt/microsoft/sqlncli/lib64/libsqlncli-11.0.so.1790.0")

cnx1 = pyodbc.connect('driver=/opt/microsoft/msodbcsql/lib64/libmsodbcsql-11.0.so.2270.0;server=SRVWUDEN0835;database=basis;uid=bhatt; pwd=DoubleBubble')


def load_table(date1,date2):
    strDate1 = date1.strftime('%m/%d/%Y')
    strDate2 = date2.strftime('%m/%d/%Y')
    stmt = "insert into Agent_Queue (ID)  select distinct Send_Location_ID from Pretty_Txns where Send_Date >= '%s' and Send_Date <= '%s' and Send_Location_ID is not null union select distinct Pay_Location_ID  from Pretty_Txns where Pay_Date >= '%s' and Pay_Date <= '%s' and Pay_Location_ID is not null" % (strDate1,strDate2,strDate1,strDate2)
    curs=cnx1.cursor()
    curs.execute(stmt)
    curs.commit()

def make_pid(n):
    stmt="execute mark_agent_queue "+str(n)
    curs=cnx1.cursor()
    curs.execute(stmt)
    pid=curs.fetchone()[0]
    curs.commit()
    return pid

def data(pid,date1,date2):
    strpid = str(pid)
    strDate1 = date1.strftime('%m/%d/%Y')
    strDate2 = date2.strftime('%m/%d/%Y')
    sql1 ="select T.* from Pretty_Txns T with (nolock) join Agent_Queue as id with (nolock)  on id.ID=T.Send_Location_ID where Send_Location_ID is not null and Send_Date >= '%s' and Send_Date <= '%s'and  id.pid =%s " %(strDate1,strDate2,strpid)
    sql2 ="select T.* from Pretty_Txns T with (nolock) join Agent_Queue as id with (nolock) on id.ID=T.Pay_Location_ID where Pay_Location_ID is not null and Pay_Date >= '%s' and Pay_Date <= '%s'and  id.pid = %s "%(strDate1,strDate2,strpid)
    df_send = pd.read_sql(sql1,cnx)
    df_pay = pd.read_sql(sql2,cnx)
    return {"df_send":df_send,"df_pay":df_pay}







def run(dict,date1,date2):
    strDate1 = date1.strftime('%m/%d/%Y')
    strDate2 = date2.strftime('%m/%d/%Y')
    df_send = dict["df_send"]
    df_pay = dict["df_pay"]
    df_send['time_diff'] = pd.to_numeric(df_send['Pay_Time']-df_send['Send_Time'])
    df_send['time_diff'] = df_send['time_diff']/1000000000
    
    df_pay['time_diff'] = pd.to_numeric(df_pay['Pay_Time']-df_pay['Send_Time'])
    df_pay['time_diff'] = df_pay['time_diff']/1000000000
    df1 = df_send.loc[:,['Send_Location_ID','Pay_Agent','Pay_Country','Send_Customer','Send_Time','Pay_Time','Send_Amount','Pay_Amount','time_diff','MTCN','Send_Phone','Pay_Channel','Send_ID_Number','Product']]
    df2 = df_pay.loc[:,['Pay_Location_ID','Send_Agent','Send_Country','Pay_Customer','Send_Time','Pay_Time','Send_Amount','Pay_Amount','time_diff','MTCN','Pay_Phone','Send_Channel','Pay_ID_Number','Product']]
    grouped = df1.groupby('Send_Location_ID')
    def repeat_count(s):
        return (s.shift(-1) == s).sum()
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
    result = grouped.agg({'Pay_Agent':'nunique','Pay_Country':'nunique','Send_Customer':'nunique','time_diff':['min','max','mean'],'MTCN':'size','Send_Phone':'nunique','Pay_Channel':'nunique','Send_ID_Number':'nunique','Product':'nunique','Send_Amount':['mean',banded_txn1,banded_txn2,banded_txn3,banded_txn4,banded_txn5]})
    grouped = df2.groupby('Pay_Location_ID')
    result1 = grouped.agg({'Send_Agent':'nunique','Send_Country':'nunique','Pay_Customer':'nunique','time_diff':['min','max','mean'],'MTCN':'size','Pay_Phone':'nunique','Send_Channel':'nunique','Pay_ID_Number':'nunique','Product':'nunique','Pay_Amount':['mean',banded_txn1,banded_txn2,banded_txn3,banded_txn4,banded_txn5]})
    Result = result.merge(result1,left_index=True,right_index=True,how='outer')
    Result.index.names = ['Location_ID']
    del df1,df2,grouped,result,result1
    df1 = df_send.loc[:,['Send_Location_ID','Pay_Location_ID']]
    df1.columns = ['id1','id2']
    df2 = df_pay.loc[:,['Pay_Location_ID','Send_Location_ID']]
    df2.columns = ['id1','id2']
    df4 = pd.concat([df1,df2],axis=0)
    df4 = df4.drop_duplicates()
    df4 = df4['id2'].groupby(df4['id1']).nunique()
    df4 = df4.to_frame()
    df4.columns = ['Agent_Count']
    df4.index.names = ['Location_ID']
    Final = Result.merge(df4,left_index=True,right_index=True,how='outer')
    del Result,df1,df2,df4
    df1 = df_send.loc[:,['Send_Location_ID','Send_Customer','Send_Phone','Send_Channel','Send_ID_Number']]
    df2 = df_pay.loc[:,['Pay_Location_ID','Pay_Customer','Pay_Phone','Pay_Channel','Pay_ID_Number']]
    df1.columns = ['Location_ID','Customer','Phone','Channel','ID_Number']
    df2.columns = ['Location_ID','Customer','Phone','Channel','ID_Number']
    df3 = pd.concat([df1,df2],axis=0)
    df3 = df3.drop_duplicates()
    df3 = df3.groupby('Location_ID').agg({'Customer':'nunique','Phone':'nunique','Channel':'nunique','ID_Number':'nunique'})
    df3.columns = ['Total_Customer_Count','Total_Phone_Count','Total_Channel_Count','Total_ID_Count']
    Final = Final.merge(df3,left_index=True,right_index=True,how='outer')
    del df1,df2,df3
    df = df_send.loc[:,['Send_Location_ID','Send_Customer','Pay_Customer','Send_Time']]
    df = df.sort_values(by=['Send_Time'])
    df['unique'] = df.groupby(['Send_Location_ID','Send_Customer'])['Pay_Customer'].apply(lambda x :x.shift(-1)!=x)
    df =  df[df['unique']==True]
    df['time_diff'] =   df.groupby(['Send_Location_ID','Send_Customer'])['Send_Time'].apply(lambda x : x.diff().abs()<=dt.timedelta(seconds=36000))
    def func1(x):
        return (x==True).sum()
    def func2(x):
        return ((x==False)&(x.shift(-1)==True)).sum()
    dfa = df.groupby(['Send_Location_ID','Send_Customer']).agg({'time_diff':[func1,func2]})
    dfa.columns = ['count1','count2']
    dfa['One_To_Many_Send'] = dfa['count1'] + dfa['count2']
    dfa = dfa[dfa['One_To_Many_Send']>0]
    dfa = dfa.reset_index()
    dfb = dfa.groupby('Send_Location_ID').agg({'One_To_Many_Send':'sum'})
    Final = Final.merge(dfb,left_index=True,right_index=True,how='outer')
    del df,dfa,dfb
    df = df_pay.loc[:,['Pay_Location_ID','Send_Customer','Pay_Customer','Pay_Time']]
    df['unique'] = df.groupby(['Pay_Location_ID','Pay_Customer'])['Send_Customer'].apply(lambda x :x.shift(-1)!=x)
    df = df[df['unique']==True]
    df['time_diff'] =   df.groupby(['Pay_Location_ID','Pay_Customer'])['Pay_Time'].apply(lambda x : x.diff().abs()<=dt.timedelta(seconds=36000))
    dfa = df.groupby(['Pay_Location_ID','Pay_Customer']).agg({'time_diff':[func1,func2]})
    dfa.columns = ['count1','count2']
    dfa['Many_To_One_Pay'] = dfa['count1'] + dfa['count2']
    dfa = dfa[dfa['Many_To_One_Pay']>0]
    dfa = dfa.reset_index()
    dfb = dfa.groupby('Pay_Location_ID').agg({'Many_To_One_Pay':'sum'})
    Final = Final.merge(dfb,left_index=True,right_index=True,how='outer')
    del df,dfa,dfb
    df3 = df_send.loc[:,['Send_Location_ID','Send_Amount']]
    df4 = df_pay.loc[:,['Pay_Location_ID','Pay_Amount']]
    Benford1 = df3.assign(Leading_Digit = df3['Send_Amount'].astype(str).str[0]).groupby('Send_Location_ID')['Leading_Digit'].value_counts(sort=False, normalize=True)
    Benford1 = Benford1.unstack(level=0).subtract(np.log10(1+(1/np.arange(0,11))), axis=0).abs().sum()
    Benford1 = Benford1.to_frame()
    Benford1.columns = ['Send_Benford']
    Benford1.index.names = ['Location_ID']
    Benford2 = df4.assign(Leading_Digit = df4['Pay_Amount'].astype(str).str[0]).groupby('Pay_Location_ID')['Leading_Digit'].value_counts(sort=False, normalize=True)
    Benford2 = Benford2.unstack(level=0).subtract(np.log10(1+(1/np.arange(0, 10))), axis=0).abs().sum()
    Benford2 = Benford2.to_frame()
    Benford2.columns = ['Pay_Benford']
    Benford2.index.names = ['Location_ID']
    Benford = pd.merge(Benford1,Benford2,left_index=True,right_index=True,how='outer')
    Final = Final.merge(Benford,left_index=True,right_index=True,how='outer')
    del df3,df4,Benford1,Benford2,Benford
    df5 = df_send.loc[:,['Send_Location_ID','Send_Customer','Send_Time']]
    df5.columns = ['Location_ID','Customer','Time']
    df6 = df_pay.loc[:,['Pay_Location_ID','Pay_Customer','Pay_Time']]
    df6.columns = ['Location_ID','Customer','Time']
    df7 = pd.merge(df5,df6,on=['Location_ID','Customer'])
    df7['Time_x'] = pd.to_numeric(df7['Time_x'])
    df7['Time_y'] = pd.to_numeric(df7['Time_y'])
    df7['time_diff'] = (df7['Time_x']-df7['Time_y']).abs()
    df7['time_diff'] = df7['time_diff']/1000000000
    df7 = df7[df7['time_diff'] <= 36000]
    df8 = df7['Customer'].groupby(df7['Location_ID']).nunique()
    df8 = df8.to_frame()
    df8.columns = ['Flipped_Customer_count']
    Final = pd.merge(df8,Final,left_index=True,right_index=True,how='outer')

    dfa = df7.groupby('Location_ID').agg({'time_diff':'size'})
    dfa.columns = ['Flipped_Txns_Count']
    Final = pd.merge(Final,dfa,left_index=True,right_index=True,how='outer')
    del df5,df6,df7,df8,dfa
    df1 = df_send.loc[:,['Send_Location_ID','Send_Customer','Pay_Customer','time_diff']]
    df1 = df1[df1['time_diff'] <= 600]
    df2 = df1.groupby('Send_Location_ID').agg({'time_diff':'size'})
    df2.index.names = ['Location_ID']
    df2.columns = ['Send_Rapid_Pay_out_txns']
    df1 = df_pay.loc[:,['Pay_Location_ID','Send_Customer','Pay_Customer','time_diff']]
    df1 = df1[df1['time_diff'] <= 600]
    df3 = df1.groupby('Pay_Location_ID').agg({'time_diff':'size'})
    df3.index.names = ['Location_ID']
    df3.columns = ['Pay_Rapid_Pay_out_txns']
    df4 = pd.merge(df2,df3,left_index='True',right_index='True',how='outer')
    Final = pd.merge(df4,Final,left_index=True,right_index=True,how='outer')
    del df1,df2,df3,df4
    df1 = df_send.loc[:,['Send_Location_ID','Send_Terminal','Send_Time']]
    df2 = df_pay.loc[:,['Pay_Location_ID','Pay_Terminal','Pay_Time']]
    df1.columns = ['Location_ID','Terminal','Time']
    df2.columns = ['Location_ID','Terminal','Time']
    df4 = pd.concat([df1,df2],axis=0)
    df4 = df4.drop_duplicates()
    df4 = df4.sort_values(by=['Time'])
    df4 = df4.reset_index()
    df4['time_diff'] = df4.groupby(['Location_ID','Terminal'])['Time'].apply(lambda x : x.diff().abs()<=dt.timedelta(seconds=60))
    dfa = df4.groupby(['Location_ID','Terminal']).agg({'time_diff':[func1,func2]})
    dfa.columns = ['count1','count2']
    dfa['Rapid_Fire_Terminal_Count'] = dfa['count1'] + dfa['count2']
    dfa = dfa[dfa['Rapid_Fire_Terminal_Count']>0]
    dfa = dfa.reset_index()
    dfa = dfa.groupby('Location_ID').agg({'Rapid_Fire_Terminal_Count':'sum'})
    Final = pd.merge(Final,dfa,left_index=True,right_index=True,how='outer')
    del df1,df2,df4,dfa
    df1 = df_send.loc[:,['Send_Location_ID','Send_Terminal','Send_Amount','Send_Time']]
    df1 = df1.sort_values(by=['Send_Time'])
    df2 = df1.groupby(['Send_Location_ID','Send_Terminal']).agg({'Send_Amount':repeat_count})
    df2 = df2[df2['Send_Amount']>=1]
    df2 = df2.reset_index(level=['Send_Location_ID','Send_Terminal'])
    df3 = df2.groupby('Send_Location_ID').agg({'Send_Amount':'sum'})
    df3.index.names = ['Location_ID']
    df3.columns = ['Send_Repeat_Amount_Txns']
    df4 = df_pay.loc[:,['Pay_Location_ID','Pay_Terminal','Pay_Amount','Pay_Time']]
    df4 = df4.sort_values(by=['Pay_Time'])
    df5 = df4.groupby(['Pay_Location_ID','Pay_Terminal']).agg({'Pay_Amount':repeat_count})
    df5 = df5[df5['Pay_Amount']>=1]
    df5 = df5.reset_index(level=['Pay_Location_ID','Pay_Terminal'])
    df6 = df5.groupby('Pay_Location_ID').agg({'Pay_Amount':'sum'})
    df6.index.names = ['Location_ID']
    df6.columns = ['Pay_Repeat_Amt_Txns']
    df7 = pd.merge(df3,df6,left_index='True',right_index='True',how='outer')
    Final = pd.merge(Final,df7,left_index=True,right_index=True,how='outer')
    del df1,df2,df3,df4,df5,df6,df7
    df1 = df_send.loc[:,['Send_Location_ID','Send_Amount','Pay_Country']]
    df2 = df_pay.loc[:,['Pay_Location_ID','Pay_Amount','Send_Country']]
    sql2 = 'select Country_Code, Rating from GeoRisk'
    df3 = pd.read_sql(sql2,cnx)
    df1.columns = ['Location_ID','Amount','Country_Code']
    df2.columns = ['Location_ID','Amount','Country_Code']
    df4 = pd.merge(df1,df3,how='left')
    df4 = df4[df4['Rating']=='Higher']
    dfa = df4.groupby('Location_ID').agg({'Country_Code':'nunique','Amount':'sum'})
    dfa.columns = ['HRJ_Send_Amount','HRJ_Send_Country_Count']
    df5 = pd.merge(df2,df3,how='left')
    df5 = df5[df5['Rating']=='Higher']
    dfb = df5.groupby('Location_ID').agg({'Country_Code':'nunique','Amount':'sum'})
    dfb.columns = ['HRJ_Pay_Amount','HRJ_Pay_Country_Count']
    dfc = pd.merge(dfa,dfb,left_index='True',right_index='True',how='outer')
    Final = pd.merge(Final,dfc,left_index=True,right_index=True,how='outer')
    Final.index.names = ['ID']
    del df1,df2,df3,df4,df5,dfa,dfb,dfc
    sqlquery = "select  * from  Results.Outcomes where  CASE_CREATION_DATE >= '%s' and CASE_CREATION_DATE <= '%s'"  %  (strDate1,strDate2)
    df = pd.read_sql(sqlquery,cnx)
    df1 = df[df['SUBJECT_TYPE']=='AgentLocation']
    df2 = df1.groupby('SUBJECT_ID').agg({'FIU_Risk_Level_1':'sum','FIU_Risk_Level_2':'sum','FIU_Risk_Level_3':'sum','FIU_Risk_Level_4':'sum','FIU_Risk_Level_5':'sum','High_Risk_GFO'    :'sum','Medium_Risk_GFO':'sum','No_Risk_GFO':'sum'})
    df2.index.names = ['ID']
    Final = pd.merge(Final,df2,left_index=True,right_index=True,how='left')
    Final['ID_Type'] = 'Location_ID'
    del df,df1,df2
    sqlquery = "select Time_Index from Model.Time_Index where t1 = '%s' and t2 = '%s'" % (strDate1,strDate2)
    df1 = pd.read_sql(sqlquery,cnx)
    Final['Time_Index'] = df1['Time_Index'][0]
    Final = Final.fillna(0)
    del df1
    Final = Final.drop_duplicates()
    Final.columns = ['Send_Rapid_Pay_Out_Txns','Pay_Rapid_Pay_Out_Txns','Flipped_Customer_Count','Send_Customer_Count','Send_Phone_Count','Pay_Country_Count','Send_Txn_Count','Pay_Channel_Count','Avg_Send_Amount','Send_Banded_Count_Class1','Send_Banded_Count_Class2','Send_Banded_Count_Class3','Send_Banded_Count_Class4','Send_Banded_Count_Class5','Send_ID_Count','Pay_Agent_Count','Min_Send_Payout_Speed','Max_Send_Payout_Speed','Avg_Send_Payout_Speed','Send_Product_Count','Pay_Customer_Count','Pay_Phone_Count','Pay_Txn_Count','Send_Country_Count','Avg_Pay_Amount','Pay_Banded_Count_Class1','Pay_Banded_Count_Class2','Pay_Banded_Count_Class3','Pay_Banded_Count_Class4','Pay_Banded_Count_Class5','Send_Agent_Count','Pay_ID_Count','Min_Pay_Payout_Speed','Max_Pay_Payout_Speed','Avg_Pay_Payout_Speed','Send_Channel_Count','Pay_Product_Count','Total_Agent_Count','Total_Customer_Count','Total_Phone_Count','Total_Channel_Count','Total_ID_Count','One_To_Many_Send_Count','Many_To_One_Pay_Count','First_Digit_Send','First_Digit_Pay','Flipped_Txns_Count','Rapid_Fire_Count','Send_Repeat_Amount_Txns','Pay_Repeat_Amount_Txns','HRJ_Send_Amount','HRJ_Send_Country_Count','HRJ_Pay_Amount','HRJ_Pay_Country_Count','FIU_Risk_Level_4','FIU_Risk_Level_5','FIU_Risk_Level_1','FIU_Risk_Level_2','FIU_Risk_Level_3','No_Risk_GFO','High_Risk_GFO','Medium_Risk_GFO','ID_type','Time_Index']
    Final.to_sql("Location_Batch_Test",cnx,if_exists="append",index_label=Final.index.name,dtype={Final.index.name: sqlalchemy.sql.sqltypes.VARCHAR(50)})


if __name__=='__main__':
    load_table(dt.datetime(2016,10,30),dt.datetime(2016,11,29))
    pid=make_pid(20000)
    dict = data(pid,dt.datetime(2016,10,30),dt.datetime(2016,11,29))
    run(dict,dt.datetime(2016,10,30),dt.datetime(2016,11,29))







    


    




