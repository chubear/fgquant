# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# import quantlib
import logging as log
import os
from datetime import datetime
from time import perf_counter
import marketrawdb_importer
import marketstddb_importer
import indistockdb_importer
import measure_monitor
import strategy_output_gsheet
import pandas as pd
from sqlalchemy import create_engine


#configuration
engine = create_engine("mysql+pymysql://{user}:{pw}@home.dottdot.com:{port}/{db}".format(user='dataimporter',pw='dataimporter',port='13306'))
measure_data_path=os.path.join(os.path.dirname(os.path.abspath(__file__)),'measure_data')


def output_cm_tmp(QueryDate, engine):
    db_connection = engine.connect() 
    db_cursor = db_connection.cursor()
    
    period_day = 10
    #日資料
    tablelist=['md_cm_fd_brokertrading'
              ,'md_cm_fd_foreigninsttraing'
              ,'md_cm_fd_investmenttrusttrading'
              ,'md_cm_fd_stockholderstructure'
              ,'md_cm_ta_dailyquotes_adj'
              ,'md_cm_ta_dailyquotes'
              ,'md_cm_ta_dailystatistics']
    for tbl in tablelist:
        time_start = perf_counter()
        SQL= "Delete from marketrawdb_cm." + tbl + "_tmp  WHERE 日期 <=DATE_SUB(" + QueryDate.strftime('%Y%m%d') + ",INTERVAL 4 MONTH)"
        db_cursor.execute(SQL)
        SQL= "REPLACE INTO marketrawdb_cm." + tbl + "_tmp SELECT * FROM marketrawdb_cm." + tbl + " WHERE 日期 BETWEEN DATE_SUB(" + QueryDate.strftime('%Y%m%d') + ",INTERVAL " + str(period_day) + " DAY) AND CURDATE();"
        #print(SQL)
        db_cursor.execute(SQL)
        db_connection.commit()
        log.debug('output_cm_tmp: %s: %s' % (tbl,str(perf_counter()-time_start)))
def output_yeswin(csv_file_path):
    import csv

    #PB+ROE+MS
    df=pd.read_csv(csv_file_path)
    df.set_index('Ticker',inplace=True)
    df['Score']=df['PB_Rank']+df['ROE_Rank']+df['MonthSale_YoY_Rank']
    tickerlist=list(df.sort_values(['Score'],ascending = False).head(80).index)
    datas=[[1,'$TWT.TW']]+ [[1 , str(ticker)+'.TW'] for ticker in tickerlist]
    #PB+ROE+MS+MV
    df['Score_1']=df['PB_Rank']+df['ROE_Rank']+df['MonthSale_YoY_Rank']+df['市值_Rank']
    tickerlist=list(df.sort_values(['Score_1'],ascending = False).head(80).index)
    datas=datas + [[2,'$TWT.TW']]+ [[2 , str(ticker)+'.TW'] for ticker in tickerlist]
    #PB+ROE+MS+67D
    df['Score_2']=df['PB_Rank']+df['ROE_Rank']+df['MonthSale_YoY_Rank']+df['Price_Return_67D_Rank']
    tickerlist=list(df.sort_values(['Score_2'],ascending = False).head(80).index)
    datas=datas + [[3,'$TWT.TW']]+ [[3 , str(ticker)+'.TW'] for ticker in tickerlist]

    output_csv_file_path = os.path.join(os.path.dirname(os.path.dirname(csv_file_path)),'YUS-1.csv')
    with open(output_csv_file_path, 'w', newline='') as csvfile:
        writer  = csv.writer(csvfile)
        for row in datas:
            writer.writerow(row)
    
    #print(df['Ticker'].head(100))
def is_data_update(querydate,dbname,engine):
    db_connection = engine.connect() 

    db_cursor = db_connection.cursor()
       
    if dbname.upper() == 'MARKETRAWDB':
        SQL= "select count(*) as num from marketrawdb_cm.md_cm_ta_dailyquotes  WHERE 日期 ='" + querydate.strftime('%Y%m%d') + "'"
        db_cursor.execute(SQL)
        record = db_cursor.fetchone()
        return record[0]>0 
    if dbname.upper() == 'MARKETSTDDB':
        SQL= "select count(*) as num from marketstddb.`9962`  WHERE Date ='" + querydate.strftime('%Y%m%d') + "'"
            
    if dbname.upper() == 'INDISTOCKDB':
        SQL= "select count(*) as num from indistockdb.9962  WHERE Date ='" + querydate.strftime('%Y%m%d') + "'"

    db_cursor.execute(SQL)
    record = db_cursor.fetchone()
    return record[0]>0

if __name__=='__main__':
    QueryDate = (datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
    #QueryDate=datetime(2022,4,25)
    # 將csv匯入marketrawdb並產生部分tmp表
    is_run = 1 
    if not is_data_update(QueryDate,'marketrawdb'):
        marketrawdb_importer.main()
        output_cm_tmp(QueryDate,engine)
        
        print('marketrawdb finished')
    #將資料處理為marketstddb
    if is_data_update(QueryDate,'marketrawdb'):
        if not is_data_update(QueryDate,'marketstddb'):
            marketstddb_importer.main(QueryDate)
            print('marketstddb finished')
            is_run = 0
    
    if is_run == 1 and is_data_update(QueryDate,'marketstddb') and not is_data_update(QueryDate,'indistockdb'):    
        #將資料處理為indistockdb
        indistockdb_importer.main(QueryDate)
        is_run = 0
   
    if is_run == 1 and is_data_update(QueryDate,'indistockdb'):  
        #產生measure_moniter
        if not os.path.isfile(measure_data_path,'measure_monitor_' + QueryDate.strftime('%Y%m%d') +'.csv')):       
            measure_monitor.main(QueryDate)
                
    #measure_monitor若有資料
    if os.path.isfile(os.path.join(measure_monitor_folder_path,'data','measure_monitor_' + QueryDate.strftime('%Y%m%d') +'.csv')):       
        #output_gsheet
        from measure_monitor import strategy_output_gsheet
        strategy_output_gsheet.main(QueryDate)
        #output yeswin
        #output_yeswin(os.path.join(measure_monitor_folder_path,'data','measure_monitor_' + QueryDate.strftime('%Y%m%d') +'.csv'))

    # #=====exec output_yeswin
    # measure_monitor_folder_path=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'measure_monitor')
    # output_yeswin(os.path.join(measure_monitor_folder_path,'data','measure_monitor_' + QueryDate.strftime('%Y%m%d') +'.csv'))
