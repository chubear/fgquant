from copy import deepcopy
import os,sys

from pandas.core.indexes.datetimes import date_range
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np
import threading 
import sys
from datetime import date,datetime
import quantlib
from dateutil.relativedelta import relativedelta
import pandas as pd
import time
from sqlalchemy import create_engine
# import logging
#logging.basicConfig(level=logging.info, format='%(asctime)s - %(levelname)s : %(message)s')
# db_conn_insert =quanlib.get_db_conn(engine='sqlalchemy',user='dataimporter',db='indistockdb')
# db_conn_query = quanlib.get_db_conn(engine='mysql',user='dataimporter',db='marketstddb') 
def cutfin(finName,fintype):
    temp=finName
    cutTemp=0
    for i in range(len(temp)-1,0,-1):
        if (temp['年季'].iloc[i]-temp['年季'].iloc[i-1]==2) | (temp['年季'].iloc[i]-temp['年季'].iloc[i-1]==98) :
            
            if fintype=='IS':
                temp.iloc[i,1:]=temp.iloc[i,1:]/2
                
            temp=temp.append(temp.iloc[i])
            temp['年季'].iloc[-1]=temp['年季'].iloc[i]-1
            cutTemp=1
        elif temp['年季'].iloc[i]-temp['年季'].iloc[i-1]==100:
            
            if fintype=='IS':
                temp.iloc[i,1:]=temp.iloc[i,1:]/4
                
            temp=temp.append(temp.iloc[i])
            temp['年季'].iloc[-1]=temp['年季'].iloc[i]-1 
            temp=temp.append(temp.iloc[i])
            temp['年季'].iloc[-1]=temp['年季'].iloc[i]-2
            temp=temp.append(temp.iloc[i])
            temp['年季'].iloc[-1]=temp['年季'].iloc[i]-3
            cutTemp=2
         
    if  cutTemp==1 :
        if fintype=='IS':
            temp.iloc[0,1:]=temp.iloc[0,1:]/2
                
        temp=temp.append(temp.iloc[0])
        temp['年季'].iloc[-1]=temp['年季'].iloc[0]-1
    elif cutTemp==2 :
        if fintype=='IS':
            temp.iloc[0,1:]=temp.iloc[0,1:]/4
            
        temp=temp.append(temp.iloc[0])
        temp['年季'].iloc[-1]=temp['年季'].iloc[0]-1
        temp=temp.append(temp.iloc[0])
        temp['年季'].iloc[-1]=temp['年季'].iloc[0]-2
        temp=temp.append(temp.iloc[0])
        temp['年季'].iloc[-1]=temp['年季'].iloc[0]-3
        
    temp=temp.sort_values(by=['年季'])
    return temp

def setSeason(StartDateFi,EndDateFi):
    temp=pd.DataFrame([int(StartDateFi)],columns=(['年季']))
    tempD=int(StartDateFi)
    k=1
    i=5
    while tempD< int(EndDateFi):
    
        if i % 4 == 0:
            tempD=tempD+97
        else:
            tempD=tempD+1
        
        temp.loc[k]=tempD
        k+=1
        i+=1
        
    temp=temp.astype(str)
    return temp
class INDISTOCK_IMPORTER():
    def __init__(self,Ticker: str, StartDate: datetime, EndDate: datetime, indistockdb_name, db_conn_insert, db_conn_query) -> None:
        self.Ticker = Ticker
        self.StartDate = StartDate
        self.EndDate = EndDate
        self.indistockdb_name = indistockdb_name
        self.db_conn_query = db_conn_query
        self.db_conn_insert = db_conn_insert

        self.StartDateFi=str(EndDate.year-11)+ '01'
        self.EndDateFi=str(EndDate.year)+ '04'
        self.SeasonArray=setSeason(self.StartDateFi,self.EndDateFi)  
        #為了可以補上週月年資料
        self.StartDate_Data = StartDate - relativedelta(months=+1)

                
        ETF_list = ['0050','0056']
        Index_list = ['TWA00', 'TWA02', 'TWA04', 'TWA05', 'TWA06', 'TWA64', 'TWA6N', 'TWA6S', 'TWB11', 'TWB12', 'TWB13', 'TWB14', 'TWB15', 'TWB16', 'TWB18', 'TWB19', 'TWB20', 'TWB21', 'TWB22', 'TWB25', 'TWB26', 'TWB27', 'TWB28', 'TWB29', 'TWB30', 'TWB31', 'TWB32', 'TWB33', 'TWB34', 'TWB35', 'TWB36', 'TWB37', 'TWB38', 'TWB39', 'TWB40', 'TWB99', 'TWC37', 'TWC38', 'TWC39']
        if Ticker in ETF_list:
            self.securitytype = 'ETF'
        elif Ticker in Index_list:
            self.securitytype = 'INDEX'
        else:        
            self.securitytype = 'STOCK'
        
    def _create_table(self):
        try:
            db_cursor = self.db_conn_query.cursor()
            db_cursor.execute ('Create Table %s.`%s` select * from %s._template where 1=0;' % (self.indistockdb_name,self.Ticker,self.indistockdb_name))
            db_cursor.execute ('Alter table %s.`%s` add primary key (`id`);' % (self.indistockdb_name,self.Ticker))
            db_cursor.execute ('Alter table %s.`%s` MODIFY id INTEGER  AUTO_INCREMENT;' % (self.indistockdb_name,self.Ticker))
            db_cursor.execute ('Alter table %s.`%s`  add  UNIQUE index `UniqueIndex` (`Date`);'% (self.indistockdb_name,self.Ticker))
            self.db_conn_query.commit()
            print('%s.`%s` created.' % (self.indistockdb_name,self.Ticker))
        except Exception as Err:
            print("create_table Error(%s): %s" % (self.Ticker,Err))
        finally:
            db_cursor.close()
    def _importdb(self):

        if self.securitytype == 'STOCK':
            self.df_dailydata = self._df_dailydata_stock()
            self.df_monthsales = self._df_monthsales()
            self.df_stockholderstructure = self._df_stockholderstructure()
            self.df_insiderholdingstructure = self._df_insiderholdingstructure()
            self.df_fi_quarterly = self._df_fi_quarterly()
            self.df_dividendpolicy = self._df_dividendpolicy()
        elif self.securitytype == 'ETF':
            self.df_dailydata = self._df_dailydata_etf_index()
            self.df_stockholderstructure = self._df_stockholderstructure()
            self.df_dividendpolicy = self._df_dividendpolicy()    
        elif  self.securitytype == 'INDEX':
            self.df_dailydata = self._df_dailydata_etf_index()                   
        self._df_combined()
        self._flag_nondailydata()
        self._calc_ratio()
        # self.df_result.to_csv('test.csv',encoding='utf-8-sig')
        self._insert_db()
    def _df_dailydata_stock(self):
        _SQL='Select `Date`, `Ticker`, `CorpName`,B.`產業名稱` as `industry`,B.`指數彙編分類` as `Sector` ,B.上市日期 ,B.`上櫃日期`,`Open`, `High`, `Low`, `Close`,`漲幅`,`Volume`, `Amount`,`市值比重`,`週轉率`,`成交值比重`,`Open_Adj`, `High_Adj`,`Low_Adj`,`Close_Adj`,`Beta係數21D`, `Beta係數65D`, `Beta係數250D`, `外資買張`, `外資賣張`, `外資買賣超`, `外資持股異動`, `外資持股張數`, `外資及陸資買張`, `外資及陸資賣張`, `外資及陸資買賣超`, `外資自營商買張`, `外資自營商賣張`, `外資自營商買賣超`, `外資買金額`, `外資賣金額`, `外資買賣超金額`, `外資買均價`, `外資賣均價`, `外資持股比率`, `外資持股市值`, `外資持股成本`, `外資尚可投資張數`, `外資尚可投資比率`, `外資投資上限比率`, `陸資投資上限比率`, `與前日異動原因`,自營商買張, 自營商賣張, 自營商買賣超, 自營商買張_自行買賣, 自營商賣張_自行買賣, 自營商買賣超_自行買賣, 自營商買張_避險, 自營商賣張_避險, 自營商買賣超_避險, 自營商庫存, 自營商買金額, 自營商賣金額, 自營商買賣超金額, 自營商買均價, 自營商賣均價, 自營商持股比率, 自營商持股市值, 自營商持股成本, 投信買張, 投信賣張, 投信買賣超, 投信庫存, 投信買金額, 投信賣金額, 投信買賣超金額, 投信買均價, 投信賣均價, 投信持股比率, 投信持股市值, 投信持股成本  from `marketstddb`.`' + self.Ticker + '` as A left join `marketrawdb_cm`.`bd_cm_companyprofile` as B on (A.Ticker=B.`股票代號`) where  `Date` between "'+ self.StartDate_Data.strftime('%Y%m%d') +'" and "'+ self.EndDate.strftime('%Y%m%d') +'" and B.`年度`='+ str(self.EndDate.year)
        df_dailydata = pd.read_sql_query(_SQL, self.db_conn_query)
        df_dailydata['Date']=pd.to_datetime(df_dailydata['Date'])
        df_dailydata=df_dailydata.sort_values(by=['Date'])        
        return df_dailydata
    def _df_dailydata_etf_index(self):
        _SQL='Select `Date`, `Ticker`, `CorpName`,"" as `industry`,"" as `Sector` , (null) as `上市日期`, (null) as `上櫃日期`,`Open`, `High`, `Low`, `Close`,`漲幅`,`Volume`, `Amount`,`市值比重`,`週轉率`,`成交值比重`,`Open_Adj`, `High_Adj`,`Low_Adj`,`Close_Adj`,`Beta係數21D`, `Beta係數65D`, `Beta係數250D`, `外資買張`, `外資賣張`, `外資買賣超`, `外資持股異動`, `外資持股張數`, `外資及陸資買張`, `外資及陸資賣張`, `外資及陸資買賣超`, `外資自營商買張`, `外資自營商賣張`, `外資自營商買賣超`, `外資買金額`, `外資賣金額`, `外資買賣超金額`, `外資買均價`, `外資賣均價`, `外資持股比率`, `外資持股市值`, `外資持股成本`, `外資尚可投資張數`, `外資尚可投資比率`, `外資投資上限比率`, `陸資投資上限比率`, `與前日異動原因`,自營商買張, 自營商賣張, 自營商買賣超, 自營商買張_自行買賣, 自營商賣張_自行買賣, 自營商買賣超_自行買賣, 自營商買張_避險, 自營商賣張_避險, 自營商買賣超_避險, 自營商庫存, 自營商買金額, 自營商賣金額, 自營商買賣超金額, 自營商買均價, 自營商賣均價, 自營商持股比率, 自營商持股市值, 自營商持股成本, 投信買張, 投信賣張, 投信買賣超, 投信庫存, 投信買金額, 投信賣金額, 投信買賣超金額, 投信買均價, 投信賣均價, 投信持股比率, 投信持股市值, 投信持股成本  from `marketstddb`.`' + self.Ticker + '` where  `Date` between "'+ self.StartDate_Data.strftime('%Y%m%d') +'" and "'+ self.EndDate.strftime('%Y%m%d') +'"'
        df_dailydata = pd.read_sql_query(_SQL, self.db_conn_query)
        df_dailydata['Date']=pd.to_datetime(df_dailydata['Date'])
        df_dailydata=df_dailydata.sort_values(by=['Date'])        
        return df_dailydata
    def _df_monthsales(self):
        _SQL='Select `公告日` as `日期_月營收_公布日`,concat(`年月`,"01") as `日期_月營收_資料日`,0 as Flag_月營收, COALESCE(`單月合併營收`,`單月營收`) AS `單月合併月營收`, COALESCE(`累計合併營收`,`累計營收`) AS `累計合併月營收`  from `marketrawdb_cm`.`md_cm_fi_monthsales` where  `公告日` between "'+ self.StartDate_Data.strftime('%Y%m%d') +'" and "'+ self.EndDate.strftime('%Y%m%d') +'"  and  `股票代號` = "' + self.Ticker + '"'
        df_monthsales = pd.read_sql_query(_SQL,self.db_conn_query)
        df_monthsales['日期_月營收_公布日']=pd.to_datetime(df_monthsales['日期_月營收_公布日'])
        df_monthsales=df_monthsales.sort_values(by=['日期_月營收_公布日'])    
        return df_monthsales
    def _df_stockholderstructure(self):   
        #取得集保庫存資料
        _SQL='Select  DATE_ADD(`日期`, INTERVAL 1 DAY) as `日期_集保庫存_公布日`,`日期` as `日期_集保庫存_資料日` ,0 as Flag_集保庫存 ,  近1週1000張以上集保比率變動, 1張以下_人, 5張以下_人, 10張以下_人, 15張以下_人, 20張以下_人, 30張以下_人, 40張以下_人, 50張以下_人, 100張以下_人, 200張以下_人, 400張以下_人, 600張以下_人, 800張以下_人, 1張以上_人, 5張以上_人, 10張以上_人, 15張以上_人, 20張以上_人, 30張以上_人, 40張以上_人, 50張以上_人, 100張以上_人, 200張以上_人, 400張以上_人, 600張以上_人, 800張以上_人, 1000張以上_人, 1張以下_張, 5張以下_張, 10張以下_張, 15張以下_張, 20張以下_張, 30張以下_張, 40張以下_張, 50張以下_張, 100張以下_張, 200張以下_張, 400張以下_張, 600張以下_張, 800張以下_張, 1張以上_張, 5張以上_張, 10張以上_張, 15張以上_張, 20張以上_張, 30張以上_張, 40張以上_張, 50張以上_張, 100張以上_張, 200張以上_張, 400張以上_張, 600張以上_張, 800張以上_張, 1000張以上_張, 1張以下佔集保比率, 5張以下佔集保比率, 10張以下佔集保比率, 15張以下佔集保比率, 20張以下佔集保比率, 30張以下佔集保比率, 40張以下佔集保比率, 50張以下佔集保比率, 100張以下佔集保比率, 200張以下佔集保比率, 400張以下佔集保比率, 600張以下佔集保比率, 800張以下佔集保比率, 1張以上佔集保比率, 5張以上佔集保比率, 10張以上佔集保比率, 15張以上佔集保比率, 20張以上佔集保比率, 30張以上佔集保比率, 40張以上佔集保比率, 50張以上佔集保比率, 100張以上佔集保比率, 200張以上佔集保比率, 400張以上佔集保比率, 600張以上佔集保比率, 800張以上佔集保比率, 1000張以上佔集保比率 from `marketrawdb_cm`.`md_cm_fd_stockholderstructure` where  `日期` between "'+ self.StartDate_Data.strftime('%Y%m%d') +'" and "'+ self.EndDate.strftime('%Y%m%d') +'"  and  `股票代號` = "' + self.Ticker + '"'
        df_stockholderstructure=pd.read_sql_query(_SQL, self.db_conn_query)
        df_stockholderstructure['日期_集保庫存_公布日']=pd.to_datetime(df_stockholderstructure['日期_集保庫存_公布日'])
        df_stockholderstructure=df_stockholderstructure.sort_values(by=['日期_集保庫存_公布日'])
        return df_stockholderstructure
    def _df_insiderholdingstructure(self):
        _SQL='Select Date_Add(cast(concat(`年月`,"15") as date),INTERVAL 1 MONTH) as `日期_董事持股_公布日`,concat(`年月`,"01") as `日期_董事持股_資料日`,0 as Flag_董事持股, 董事持股張數, 董事持股張數增減, 董事持股比例, 董事持股比例增減, 董事關係人持股張數, 董事關係人持股張數增減, 董事關係人持股比例, 董事關係人持股比例增減, 董事及其關係人持股張數, 董事及其關係人持股張數增減, 董事及其關係人持股比例, 董事及其關係人持股比例增減, 監察人持股張數, 監察人持股張數增減, 監察人持股比例, 監察人持股比例增減, 監察人關係人持股張數, 監察人關係人持股張數增減, 監察人關係人持股比例, 監察人關係人持股比例增減, 監察人及其關係人持股張數, 監察人及其關係人持股張數增減, 監察人及其關係人持股比例, 監察人及其關係人持股比例增減, 董監持股張數, 董監持股張數增減, 董監持股比例, 董監持股比例增減, 董監及其關係人持股張數, 董監及其關係人持股張數增減, 董監及其關係人持股比例, 董監及其關係人持股比例增減, 經理人持股張數, 經理人持股張數增減, 經理人持股比例, 經理人持股比例增減, 經理人關係人持股張數, 經理人關係人持股張數增減, 經理人關係人持股比例, 經理人關係人持股比例增減, 經理人及其關係人持股張數, 經理人及其關係人持股張數增減, 經理人及其關係人持股比例, 經理人及其關係人持股比例增減, 大股東持股張數, 大股東持股張數增減, 大股東持股比例, 大股東持股比例增減, 大股東關係人持股張數, 大股東關係人持股張數增減, 大股東關係人持股比例, 大股東關係人持股比例增減, 大股東及其關係人持股張數, 大股東及其關係人持股張數增減, 大股東及其關係人持股比例, 大股東及其關係人持股比例增減, 大股東經理人持股張數, 大股東經理人持股張數增減, 大股東經理人持股比例, 大股東經理人持股比例增減, 大股東經理人及其關係人持股張數, 大股東經理人及其關係人持股張數增減, 大股東經理人及其關係人持股比例, 大股東經理人及其關係人持股比例增減, 內部人持股張數, 內部人持股張數增減, 內部人持股比例, 內部人持股比例增減, 內部人關係人持股張數, 內部人關係人持股張數增減, 內部人關係人持股比例, 內部人關係人持股比例增減, 內部人及其關係人持股張數, 內部人及其關係人持股張數增減, 內部人及其關係人持股比例, 內部人及其關係人持股比例增減, 全體持股張數, 全體持股張數增減, 全體持股比例, 全體持股比例增減, 全體關係人持股張數, 全體關係人持股張數增減, 全體關係人持股比例, 全體關係人持股比例增減, 全體及其關係人持股張數, 全體及其關係人持股張數增減, 全體及其關係人持股比例, 全體及其關係人持股比例增減, 董事設質張數, 董事設質張數增減, 董事設質比例, 董事設質比例增減, 董事關係人設質張數, 董事關係人設質張數增減, 董事關係人設質比例, 董事關係人設質比例增減, 董事及其關係人設質張數, 董事及其關係人設質張數增減, 董事及其關係人設質比例, 董事及其關係人設質比例增減, 監察人設質張數, 監察人設質張數增減, 監察人設質比例, 監察人設質比例增減, 監察人關係人設質張數, 監察人關係人設質張數增減, 監察人關係人設質比例, 監察人關係人設質比例增減, 監察人及其關係人設質張數, 監察人及其關係人設質張數增減, 監察人及其關係人設質比例, 監察人及其關係人設質比例增減, 董監設質張數, 董監設質張數增減, 董監設質比例, 董監設質比例增減, 董監及其關係人設質張數, 董監及其關係人設質張數增減, 董監及其關係人設質比例, 董監及其關係人設質比例增減, 經理人設質張數, 經理人設質張數增減, 經理人設質比例, 經理人設質比例增減, 經理人關係人設質張數, 經理人關係人設質張數增減, 經理人關係人設質比例, 經理人關係人設質比例增減, 經理人及其關係人設質張數, 經理人及其關係人設質張數增減, 經理人及其關係人設質比例, 經理人及其關係人設質比例增減, 大股東設質張數, 大股東設質張數增減, 大股東設質比例, 大股東設質比例增減, 大股東關係人設質張數, 大股東關係人設質張數增減, 大股東關係人設質比例, 大股東關係人設質比例增減, 大股東及其關係人設質張數, 大股東及其關係人設質張數增減, 大股東及其關係人設質比例, 大股東及其關係人設質比例增減, 大股東經理人設質張數, 大股東經理人設質張數增減, 大股東經理人設質比例, 大股東經理人設質比例增減, 大股東經理人及其關係人設質張數, 大股東經理人及其關係人設質張數增減, 大股東經理人及其關係人設質比例, 大股東經理人及其關係人設質比例增減, 內部人設質張數, 內部人設質張數增減, 內部人設質比例, 內部人設質比例增減, 內部人關係人設質張數, 內部人關係人設質張數增減, 內部人關係人設質比例, 內部人關係人設質比例增減, 內部人及其關係人設質張數, 內部人及其關係人設質張數增減, 內部人及其關係人設質比例, 內部人及其關係人設質比例增減, 全體設質張數, 全體設質張數增減, 全體設質比例, 全體設質比例增減, 全體關係人設質張數, 全體關係人設質張數增減, 全體關係人設質比例, 全體關係人設質比例增減, 全體及其關係人設質張數, 全體及其關係人設質張數增減, 全體及其關係人設質比例, 全體及其關係人設質比例增減, 董事私募張數, 董事私募張數增減, 董事私募比例, 董事私募比例增減, 董事關係人私募張數, 董事關係人私募張數增減, 董事關係人私募比例, 董事關係人私募比例增減, 董事及其關係人私募張數, 董事及其關係人私募張數增減, 董事及其關係人私募比例, 董事及其關係人私募比例增減, 監察人私募張數, 監察人私募張數增減, 監察人私募比例, 監察人私募比例增減, 監察人關係人私募張數, 監察人關係人私募張數增減, 監察人關係人私募比例, 監察人關係人私募比例增減, 監察人及其關係人私募張數, 監察人及其關係人私募張數增減, 監察人及其關係人私募比例, 監察人及其關係人私募比例增減, 董監私募張數, 董監私募張數增減, 董監私募比例, 董監私募比例增減, 董監及其關係人私募張數, 董監及其關係人私募張數增減, 董監及其關係人私募比例, 董監及其關係人私募比例增減, 經理人私募張數, 經理人私募張數增減, 經理人私募比例, 經理人私募比例增減, 經理人關係人私募張數, 經理人關係人私募張數增減, 經理人關係人私募比例, 經理人關係人私募比例增減, 經理人及其關係人私募張數, 經理人及其關係人私募張數增減, 經理人及其關係人私募比例, 經理人及其關係人私募比例增減, 大股東私募張數, 大股東私募張數增減, 大股東私募比例, 大股東私募比例增減, 大股東關係人私募張數, 大股東關係人私募張數增減, 大股東關係人私募比例, 大股東關係人私募比例增減, 大股東及其關係人私募張數, 大股東及其關係人私募張數增減, 大股東及其關係人私募比例, 大股東及其關係人私募比例增減, 大股東經理人私募張數, 大股東經理人私募張數增減, 大股東經理人私募比例, 大股東經理人私募比例增減, 大股東經理人及其關係人私募張數, 大股東經理人及其關係人私募張數增減, 大股東經理人及其關係人私募比例, 大股東經理人及其關係人私募比例增減, 內部人私募張數, 內部人私募張數增減, 內部人私募比例, 內部人私募比例增減, 內部人關係人私募張數, 內部人關係人私募張數增減, 內部人關係人私募比例, 內部人關係人私募比例增減, 內部人及其關係人私募張數, 內部人及其關係人私募張數增減, 內部人及其關係人私募比例, 內部人及其關係人私募比例增減, 全體私募張數, 全體私募張數增減, 全體私募比例, 全體私募比例增減, 全體關係人私募張數, 全體關係人私募張數增減, 全體關係人私募比例, 全體關係人私募比例增減, 全體及其關係人私募張數, 全體及其關係人私募張數增減, 全體及其關係人私募比例, 全體及其關係人私募比例增減, 獨立董事人數, 常董人數, 一般董事人數, 董事總人數, 獨立監察人數, 常監人數, 一般監察人數, 監察總人數 from `marketrawdb_cm`.`md_cm_fd_insiderholdingstructure` where  Date_Add(cast(concat(`年月`,"15") as date),INTERVAL 1 MONTH) between "'+ self.StartDate_Data.strftime('%Y%m%d') +'" and "'+ self.EndDate.strftime('%Y%m%d') +'" and `股票代號` = "' + self.Ticker + '"'
        df_insiderholdingstructure = pd.read_sql_query(_SQL, self.db_conn_query)
        df_insiderholdingstructure['日期_董事持股_公布日']=pd.to_datetime(df_insiderholdingstructure['日期_董事持股_公布日'])
        df_insiderholdingstructure = df_insiderholdingstructure.sort_values(by=['日期_董事持股_公布日'])
        return df_insiderholdingstructure
    def _df_fi_quarterly(self):
        # CM財報BS==================================================
        _SQL='Select 公告日期 as 日期_財報_公告日,年季,case(right(`年季`,2)) when "01" then cast(concat(left(`年季`,4),"0331") as date) when "02" then cast(concat(left(年季,4),"0630") as date) when "03" then cast(concat(left(年季,4),"0930") as date) when "04" then cast(concat(left(年季,4),"1231") as date) end  as 日期_財報_資料日,0 as Flag_財報,流動資產,現金及約當現金,短期投資合計,應收帳款與票據合計,存貨,非流動資產,採用權益法之投資,長期投資合計,不動產、廠房及設備,無形資產,資產總計,流動負債,非流動負債,負債總計,股本,普通股股本,特別股股本,資本公積,保留盈餘,其他權益,庫藏股票,共同控制下前手權益,合併前非屬共同控制股權,母公司業主權益,非控制權益,權益總計,原始每股淨值,公告每股淨值,負債及權益總計,預收股款之約當發行股數,母公司暨子公司所持有之母公司庫藏股,(ifnull(普通股股本,0)+ifnull(特別股股本,0)+0.01* ifnull(預收股款之約當發行股數,0) - 0.01*ifnull(母公司暨子公司所持有之母公司庫藏股,0)) as 流通在外股本 from `marketrawdb_cm`.`md_cm_fi_bs_quarterly` where `股票代號` = "' + self.Ticker + '" and 年季 between "'+ self.StartDateFi +'" and "'+ self.EndDateFi +'"  order by 年季 asc'
        df_md_cm_fi_bs_quarterly = pd.read_sql_query(_SQL, self.db_conn_query)
        df_md_cm_fi_bs_quarterly=df_md_cm_fi_bs_quarterly.fillna(0)
        df_md_cm_fi_bs_quarterly=cutfin(df_md_cm_fi_bs_quarterly,'BS')
        df_md_cm_fi_bs_quarterly=df_md_cm_fi_bs_quarterly.loc[df_md_cm_fi_bs_quarterly['日期_財報_公告日']!=0].copy()
        df_md_cm_fi_bs_quarterly['日期_財報_公告日']=pd.to_datetime(df_md_cm_fi_bs_quarterly['日期_財報_公告日'])
        
        df_md_cm_fi_bs_quarterly4=pd.DataFrame(df_md_cm_fi_bs_quarterly[['流動資產','現金及約當現金','短期投資合計','應收帳款與票據合計','存貨','非流動資產','採用權益法之投資','長期投資合計','不動產、廠房及設備','無形資產','資產總計','流動負債','非流動負債','負債總計','股本','普通股股本','特別股股本','資本公積','保留盈餘','其他權益','庫藏股票','共同控制下前手權益','合併前非屬共同控制股權','母公司業主權益','非控制權益','權益總計','原始每股淨值','公告每股淨值','負債及權益總計','預收股款之約當發行股數','母公司暨子公司所持有之母公司庫藏股']].rolling(4).mean()).set_axis(['流動資產4','現金及約當現金4','短期投資合計4','應收帳款與票據合計4','存貨4','非流動資產4','採用權益法之投資4','長期投資合計4','不動產、廠房及設備4','無形資產4','資產總計4','流動負債4','非流動負債4','負債總計4','股本4','普通股股本4','特別股股本4','資本公積4','保留盈餘4','其他權益4','庫藏股票4','共同控制下前手權益4','合併前非屬共同控制股權4','母公司業主權益4','非控制權益4','權益總計4','原始每股淨值4','公告每股淨值4','負債及權益總計4','預收股款之約當發行股數4','母公司暨子公司所持有之母公司庫藏股4'],axis=1,inplace=False)
        # df_md_cm_fi_bs_quarterly4=df_md_cm_fi_bs_quarterly4.fillna(0)
        df_md_cm_fi_bs_quarterly4['流通在外股本4']=df_md_cm_fi_bs_quarterly4['普通股股本4']+df_md_cm_fi_bs_quarterly4['特別股股本4']+0.01*df_md_cm_fi_bs_quarterly4['預收股款之約當發行股數4']-0.01*+df_md_cm_fi_bs_quarterly4['母公司暨子公司所持有之母公司庫藏股4']
            
        if df_md_cm_fi_bs_quarterly.shape[0]==df_md_cm_fi_bs_quarterly4.shape[0] :
            df_md_cm_fi_quarterly=pd.concat([df_md_cm_fi_bs_quarterly,df_md_cm_fi_bs_quarterly4],axis=1)
            print('df_md_cm_fi_quarterly yes 完成4')
        else:
            print('df_md_cm_fi_quarterly no')
            
        # CM財報IS==================================================
        _SQL='Select 年季,銷貨收入淨額,營業收入淨額,營業成本,營業毛利,營業毛利淨額,營業費用,推銷費用,管理費用,研發費用,預期信用減損損益_營業費用,其他收益及費損,營業利益,營業外收入及支出,其他收入,利息收入,租金收入,權利金收入,股利收入,財務成本,利息費用,預期信用減損損益,採用權益法之關聯企業及合資損益之份額,除列按攤銷後成本衡量金融資產淨損益,金融資產重分類淨損益,營業外收入,營業外支出,稅前純益,所得稅,繼續營業單位損益,停業部門損益,合併前非屬共同控制股權損益,稅後純益,其他綜合損益,綜合損益,母公司業主–稅後純益 as 母公司業主_稅後純益,非控制權益–稅後純益 as 非控制權益_稅後純益,共同控制下前手權益–稅後純益 as 共同控制下前手權益_稅後純益,母公司業主–綜合損益 as 母公司業主_綜合損益,非控制權益–綜合損益 as 非控制權益_綜合損益,共同控制下前手權益–綜合損益 as 共同控制下前手權益_綜合損益,EBITDA,公告基本每股盈餘,公告稀釋每股盈餘,原始每股稅前盈餘,原始每股稅後盈餘,原始每股綜合盈餘 from `marketrawdb_cm`.`md_cm_fi_is_quarterly` where `股票代號` = "' + self.Ticker + '" and 年季 between "'+ self.StartDateFi +'" and "'+ self.EndDateFi +'" order by 年季 asc'
        df_md_cm_fi_is_quarterly = pd.read_sql_query(_SQL, self.db_conn_query)
        df_md_cm_fi_is_quarterly=df_md_cm_fi_is_quarterly.fillna(0)
        df_md_cm_fi_is_quarterly=cutfin(df_md_cm_fi_is_quarterly,'IS')
        # df_md_cm_fi_bs_quarterly['日期_財報_公布日']=pd.to_datetime(df_md_cm_fi_bs_quarterly['日期_財報_公布日'])
        
        df_md_cm_fi_is_quarterly4=pd.DataFrame(df_md_cm_fi_is_quarterly[['銷貨收入淨額','營業收入淨額','營業成本','營業毛利','營業毛利淨額','營業費用','推銷費用','管理費用','研發費用','預期信用減損損益_營業費用','其他收益及費損','營業利益','營業外收入及支出','其他收入','利息收入','租金收入','權利金收入','股利收入','財務成本','利息費用','預期信用減損損益','採用權益法之關聯企業及合資損益之份額','除列按攤銷後成本衡量金融資產淨損益','金融資產重分類淨損益','營業外收入','營業外支出','稅前純益','所得稅','繼續營業單位損益','停業部門損益','合併前非屬共同控制股權損益','稅後純益','其他綜合損益','綜合損益','母公司業主_稅後純益','非控制權益_稅後純益','共同控制下前手權益_稅後純益','母公司業主_綜合損益','非控制權益_綜合損益','共同控制下前手權益_綜合損益','EBITDA','公告基本每股盈餘','公告稀釋每股盈餘','原始每股稅前盈餘','原始每股稅後盈餘','原始每股綜合盈餘']].rolling(4).sum()).set_axis(['銷貨收入淨額4','營業收入淨額4','營業成本4','營業毛利4','營業毛利淨額4','營業費用4','推銷費用4','管理費用4','研發費用4','預期信用減損損益_營業費用4','其他收益及費損4','營業利益4','營業外收入及支出4','其他收入4','利息收入4','租金收入4','權利金收入4','股利收入4','財務成本4','利息費用4','預期信用減損損益4','採用權益法之關聯企業及合資損益之份額4','除列按攤銷後成本衡量金融資產淨損益4','金融資產重分類淨損益4','營業外收入4','營業外支出4','稅前純益4','所得稅4','繼續營業單位損益4','停業部門損益4','合併前非屬共同控制股權損益4','稅後純益4','其他綜合損益4','綜合損益4','母公司業主_稅後純益4','非控制權益_稅後純益4','共同控制下前手權益_稅後純益4','母公司業主_綜合損益4','非控制權益_綜合損益4','共同控制下前手權益_綜合損益4','EBITDA4','公告基本每股盈餘4','公告稀釋每股盈餘4','原始每股稅前盈餘4','原始每股稅後盈餘4','原始每股綜合盈餘4'],axis=1,inplace=False)

        
        if df_md_cm_fi_is_quarterly.shape[0]==df_md_cm_fi_is_quarterly4.shape[0] :
            df_md_cm_fi_is_quarterly=pd.concat([df_md_cm_fi_is_quarterly,df_md_cm_fi_is_quarterly4],axis=1)
            print('df_md_cm_fi_is_quarterly yes 完成4')
            if df_md_cm_fi_quarterly.shape[0]==df_md_cm_fi_is_quarterly.shape[0] :
                print('df_md_cm_fi_is_quarterly 與 BS表同長度')
            else:
                print('df_md_cm_fi_is_quarterly 與 BS表不同長度')
            df_md_cm_fi_quarterly = pd.merge(df_md_cm_fi_quarterly, df_md_cm_fi_is_quarterly, how='left', on=['年季'])
            
            df_md_cm_fi_quarterly['原始每股稅前盈餘4']=pd.DataFrame(df_md_cm_fi_quarterly['稅前純益4']/df_md_cm_fi_quarterly['流通在外股本4']*10)
            df_md_cm_fi_quarterly['原始每股稅後盈餘4']=pd.DataFrame(df_md_cm_fi_quarterly['稅後純益4']/df_md_cm_fi_quarterly['流通在外股本4']*10)
            df_md_cm_fi_quarterly['原始每股綜合盈餘4']=pd.DataFrame(df_md_cm_fi_quarterly['綜合損益4']/df_md_cm_fi_quarterly['流通在外股本4']*10)

        else:
            print('df_md_cm_fi_is_quarterly no')
        # CM財報CF==================================================
        _SQL='Select  年季 ,營業活動現金流量,不影響現金流量之收益費損項目,折舊費用,攤銷費用,與營業活動相關資產之淨變動,與營業活動相關負債之淨變動,營運產生之現金流動,營業活動之收現類別,營業活動之付現類別,投資活動現金流量,籌資活動現金流量,匯率變動對現金及約當現金之影響,本期現金及約當現金增減數,自由現金流量 from `marketrawdb_cm`.`md_cm_fi_cf_quarterly` where `股票代號` = "' + self.Ticker + '" and 年季 between "'+ self.StartDateFi +'" and "'+ self.EndDateFi +'" order by 年季 asc'
        df_md_cm_fi_cf_quarterly = pd.read_sql_query(_SQL, self.db_conn_query)
        df_md_cm_fi_cf_quarterly=df_md_cm_fi_cf_quarterly.fillna(0)
        # df_md_cm_fi_cf_quarterly['日期_財報_建立日_cf']=pd.to_datetime(df_md_cm_fi_cf_quarterly['日期_財報_建立日_cf'])
        df_md_cm_fi_cf_quarterly=cutfin(df_md_cm_fi_cf_quarterly,'IS')
        
        df_md_cm_fi_cf_quarterly4=pd.DataFrame(df_md_cm_fi_cf_quarterly[['營業活動現金流量','不影響現金流量之收益費損項目','折舊費用','攤銷費用','與營業活動相關資產之淨變動','與營業活動相關負債之淨變動','營運產生之現金流動','營業活動之收現類別','營業活動之付現類別','投資活動現金流量','籌資活動現金流量','匯率變動對現金及約當現金之影響','本期現金及約當現金增減數','自由現金流量']].rolling(4).sum()).set_axis(['營業活動現金流量4','不影響現金流量之收益費損項目4','折舊費用4','攤銷費用4','與營業活動相關資產之淨變動4','與營業活動相關負債之淨變動4','營運產生之現金流動4','營業活動之收現類別4','營業活動之付現類別4','投資活動現金流量4','籌資活動現金流量4','匯率變動對現金及約當現金之影響4','本期現金及約當現金增減數4','自由現金流量4'],axis=1,inplace=False)
        # df_md_cm_fi_cf_quarterly4=df_md_cm_fi_cf_quarterly4.fillna(0)
        
        if df_md_cm_fi_cf_quarterly.shape[0]==df_md_cm_fi_cf_quarterly4.shape[0] :
            df_md_cm_fi_cf_quarterly=pd.concat([df_md_cm_fi_cf_quarterly,df_md_cm_fi_cf_quarterly4],axis=1)
            print('df_md_cm_fi_cf_quarterly yes 完成4')
            if df_md_cm_fi_quarterly.shape[0]==df_md_cm_fi_cf_quarterly.shape[0] :
                print('df_md_cm_fi_cf_quarterly 與 BS表同長度 ')
            else:
                print('df_md_cm_fi_cf_quarterly 與 BS表不同長度')
            df_md_cm_fi_quarterly = pd.merge(df_md_cm_fi_quarterly, df_md_cm_fi_cf_quarterly, how='left', on=['年季'])
            # sys.exit()
        else:
            print('df_md_cm_fi_cf_quarterly no')
        #df_md_cm_fi_quarterly=df_md_cm_fi_quarterly.drop(columns=['年季'])
        df_md_cm_fi_quarterly=df_md_cm_fi_quarterly.sort_values(by=['日期_財報_公告日'])
        return  df_md_cm_fi_quarterly    
    def _df_dividendpolicy(self):
        _SQL='Select 年季,盈餘分派頻率,除權日,除息日,領股日期,領息日期,盈餘配股,公積配股,股票股利合計,盈餘配息,公積配息,現金股利合計,股利合計 from `marketrawdb_cm`.`md_cm_ot_dividendpolicy` where `股票代號` = "' + self.Ticker + '" and 年季 between "'+ self.StartDateFi +'" and "'+ self.EndDateFi +'"  and (股利合計 <>0)  order by 年季 asc'
        df_md_cm_ot_dividendpolicy = pd.read_sql_query(_SQL, self.db_conn_query).fillna(0)
        
        df=pd.DataFrame(df_md_cm_ot_dividendpolicy[['年季','盈餘分派頻率','盈餘配股','公積配股','股票股利合計','盈餘配息','公積配息','現金股利合計','股利合計']]).set_axis(['年季','盈餘分派頻率','盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t'],axis=1,inplace=False).copy()
        
        for i in  range(0,len(df),1):

            if df['盈餘分派頻率'].iloc[i]=='半年配':
                df.iloc[i,2:]=df.iloc[i,2:]/2
                df=df.append(df.iloc[i])
                df.iloc[-1,0]=str(int(df.iloc[i,0])-1)
            elif df['盈餘分派頻率'].iloc[i]=='年配':
                df.iloc[i,2:]=df.iloc[i,2:]/4
                df=df.append(df.iloc[i])
                df.iloc[-1,0]=str(int(df.iloc[i,0])-1)
                df=df.append(df.iloc[i])
                df.iloc[-1,0]=str(int(df.iloc[i,0])-2)
                df=df.append(df.iloc[i])
                df.iloc[-1,0]=str(int(df.iloc[i,0])-3)
        df=df.sort_values(by=['年季'])
        
        temp= self.SeasonArray.copy()
        temp=temp.merge(df, how='left', on=['年季'])   
        temp[['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t']]=temp[['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t']].fillna(0)  
        temp[['盈餘配股4','公積配股4','股票股利合計4','盈餘配息4','公積配息4','現金股利合計4','股利合計4']]=pd.DataFrame(temp[['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t']].rolling(4).sum()).copy()
        
        temp[['盈餘配股_平均_3yr','公積配股_平均_3yr','股票股利合計_平均_3yr','盈餘配息_平均_3yr','公積配息_平均_3yr','現金股利合計_平均_3yr','股利合計_平均_3yr']]=pd.DataFrame(temp[['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t']].rolling(12).sum()/3).copy()
        temp[['盈餘配股_平均_5yr','公積配股_平均_5yr','股票股利合計_平均_5yr','盈餘配息_平均_5yr','公積配息_平均_5yr','現金股利合計_平均_5yr','股利合計_平均_5yr']]=pd.DataFrame(temp[['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t']].rolling(20).sum()/5).copy()
        temp[['盈餘配股_平均_10yr','公積配股_平均_10yr','股票股利合計_平均_10yr','盈餘配息_平均_10yr','公積配息_平均_10yr','現金股利合計_平均_10yr','股利合計_平均_10yr']]=pd.DataFrame(temp[['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t']].rolling(40).sum()/10).copy()
        
        
        
        _SQL='Select 年季,case(right(`年季`,2)) when "01" then cast(concat(left(`年季`,4),"0331") as date) when "02" then cast(concat(left(年季,4),"0630") as date) when "03" then cast(concat(left(年季,4),"0930") as date) when "04" then cast(concat(left(年季,4),"1231") as date) end  as 日期_股利_資料日,0 as Flag_股利,盈餘分派頻率,除權日,除息日,領股日期,領息日期,盈餘配股,公積配股,股票股利合計,盈餘配息,公積配息,現金股利合計,股利合計,股票股利發放率,現金股利發放率,股利發放率,董事會決議通過股利分派日,股東會日期,公告日期 from `marketrawdb_cm`.`md_cm_ot_dividendpolicy` where `股票代號` = "' + self.Ticker + '" and 年季 between "'+ self.StartDateFi +'" and "'+ self.EndDateFi +'"   order by 年季 asc'
        df_md_cm_ot_dividendpolicy1 = pd.read_sql_query(_SQL, self.db_conn_query)
        
        df_md_cm_ot_dividendpolicy1['日期_股利_公告日']=df_md_cm_ot_dividendpolicy1[['董事會決議通過股利分派日','股東會日期','公告日期']].fillna(date(2121,1,1)).replace(date(1900,1,1), date(2121,1,1)).min(axis=1)
        df_md_cm_ot_dividendpolicy1=df_md_cm_ot_dividendpolicy1.merge( temp[['年季','盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t','盈餘配股4','公積配股4','股票股利合計4','盈餘配息4','公積配息4','現金股利合計4','股利合計4','盈餘配股_平均_3yr','公積配股_平均_3yr','股票股利合計_平均_3yr','盈餘配息_平均_3yr','公積配息_平均_3yr','現金股利合計_平均_3yr','股利合計_平均_3yr','盈餘配股_平均_5yr','公積配股_平均_5yr','股票股利合計_平均_5yr','盈餘配息_平均_5yr','公積配息_平均_5yr','現金股利合計_平均_5yr','股利合計_平均_5yr','盈餘配股_平均_10yr','公積配股_平均_10yr','股票股利合計_平均_10yr','盈餘配息_平均_10yr','公積配息_平均_10yr','現金股利合計_平均_10yr','股利合計_平均_10yr']],how='left', on=['年季'])
        
        for  i in  range(0,len(df_md_cm_ot_dividendpolicy1),1):
            # if df_md_cm_ot_dividendpolicy1['除權日'].loc[i]==None and df_md_cm_ot_dividendpolicy1['除息日'].loc[i]==None:
            if df_md_cm_ot_dividendpolicy1['股利合計'].loc[i]==0:
                
                df_md_cm_ot_dividendpolicy1.loc[i,['盈餘配股4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['盈餘配股4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['盈餘配股t']].squeeze()).copy()
                df_md_cm_ot_dividendpolicy1.loc[i,['公積配股4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['公積配股4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['公積配股t']].squeeze()).copy()
                df_md_cm_ot_dividendpolicy1.loc[i,['股票股利合計4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['股票股利合計4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['股票股利合計t']].squeeze()).copy()
                df_md_cm_ot_dividendpolicy1.loc[i,['盈餘配息4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['盈餘配息4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['盈餘配息t']].squeeze()).copy()
                df_md_cm_ot_dividendpolicy1.loc[i,['公積配息4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['公積配息4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['公積配息t']].squeeze()).copy()
                df_md_cm_ot_dividendpolicy1.loc[i,['現金股利合計4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['現金股利合計4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['現金股利合計t']].squeeze()).copy()
                df_md_cm_ot_dividendpolicy1.loc[i,['股利合計4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['股利合計4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['股利合計t']].squeeze()).copy()
        
        df_md_cm_ot_dividendpolicy1=df_md_cm_ot_dividendpolicy1.drop(columns=['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t'])

        # df_md_cm_fi_quarterly['年季']=self.df_fi_quarterly['年季'].astype(float).copy()
        # df_md_cm_ot_dividendpolicy1['年季']=df_md_cm_ot_dividendpolicy1['年季'].astype(float).copy()
        df_md_cm_ot_dividendpolicy1['年季'] = df_md_cm_ot_dividendpolicy1['年季'].astype(float)
        df_md_cm_ot_dividendpolicy1=df_md_cm_ot_dividendpolicy1.merge(self.df_fi_quarterly[['年季','原始每股稅後盈餘4']],how='left', on=['年季'])
        df_md_cm_ot_dividendpolicy1['股票股利發放率4']=df_md_cm_ot_dividendpolicy1['股票股利合計4']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
        df_md_cm_ot_dividendpolicy1['現金股利發放率4']=df_md_cm_ot_dividendpolicy1['現金股利合計4']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
        df_md_cm_ot_dividendpolicy1['股利發放率4']=df_md_cm_ot_dividendpolicy1['股利合計4']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
        
        df_md_cm_ot_dividendpolicy1['股票股利發放率_平均_3yr']=df_md_cm_ot_dividendpolicy1['股票股利合計_平均_3yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
        df_md_cm_ot_dividendpolicy1['現金股利發放率_平均_3yr']=df_md_cm_ot_dividendpolicy1['現金股利合計_平均_3yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
        df_md_cm_ot_dividendpolicy1['股利發放率_平均_3yr']=df_md_cm_ot_dividendpolicy1['股利合計_平均_3yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
        df_md_cm_ot_dividendpolicy1['股票股利發放率_平均_5yr']=df_md_cm_ot_dividendpolicy1['股票股利合計_平均_5yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
        df_md_cm_ot_dividendpolicy1['現金股利發放率_平均_5yr']=df_md_cm_ot_dividendpolicy1['現金股利合計_平均_5yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
        df_md_cm_ot_dividendpolicy1['股利發放率_平均_5yr']=df_md_cm_ot_dividendpolicy1['股利合計_平均_5yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
        df_md_cm_ot_dividendpolicy1['股票股利發放率_平均_10yr']=df_md_cm_ot_dividendpolicy1['股票股利合計_平均_10yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
        df_md_cm_ot_dividendpolicy1['現金股利發放率_平均_10yr']=df_md_cm_ot_dividendpolicy1['現金股利合計_平均_10yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
        df_md_cm_ot_dividendpolicy1['股利發放率_平均_10yr']=df_md_cm_ot_dividendpolicy1['股利合計_平均_10yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
        
        df_md_cm_ot_dividendpolicy1=df_md_cm_ot_dividendpolicy1.drop(columns=['年季','原始每股稅後盈餘4'])
        df_md_cm_ot_dividendpolicy1['日期_股利_公告日']=pd.to_datetime(df_md_cm_ot_dividendpolicy1['日期_股利_公告日'])
        df_md_cm_ot_dividendpolicy1=df_md_cm_ot_dividendpolicy1.sort_values(by=['日期_股利_公告日'])
        
        return df_md_cm_ot_dividendpolicy1
    def _df_combined(self):
        self.df_result=pd.merge_asof(self.df_dailydata,self.df_monthsales,left_on='Date',right_on='日期_月營收_公布日')
        self.df_result=pd.merge_asof(self.df_result,self.df_stockholderstructure,left_on='Date',right_on='日期_集保庫存_公布日')
        self.df_result=pd.merge_asof(self.df_result,self.df_insiderholdingstructure,left_on='Date',right_on='日期_董事持股_公布日')
        self.df_result=pd.merge_asof(self.df_result,self.df_fi_quarterly,left_on='Date',right_on='日期_財報_公告日')
        self.df_result=pd.merge_asof(self.df_result,self.df_dividendpolicy,left_on='Date',right_on='日期_股利_公告日')
    def _flag_nondailydata(self):
        dict_flag_col_mapping = {
                                '日期_月營收_公布日':'Flag_月營收',
                                '日期_集保庫存_公布日':'Flag_集保庫存',
                                '日期_董事持股_公布日':'Flag_董事持股',
                                '日期_財報_公告日':'Flag_財報',
                                '日期_股利_公告日':'Flag_股利'
                                }

        for key, value in dict_flag_col_mapping.items():
            mask = ~self.df_result[key].diff().astype(str).str.strip().str[0].isin(['0','N'])
            self.df_result.loc[mask,[value]]= 1    
 
    def _calc_ratio(self):
        self.df_result['毛利率'] = self.df_result['營業毛利'] / self.df_result['營業收入淨額']*100
        self.df_result['淨利率'] = self.df_result['稅後純益'] / self.df_result['營業收入淨額']*100
        self.df_result['毛利率4'] = self.df_result['營業毛利4'] / self.df_result['營業收入淨額4']*100
        self.df_result['淨利率4'] = self.df_result['稅後純益4'] / self.df_result['營業收入淨額4']*100
        self.df_result['市值'] = self.df_result['Close'] * self.df_result['普通股股本']/10
        self.df_result['ROE4'] = self.df_result['稅後純益4'] / self.df_result['權益總計4']*100
        self.df_result['PB'] = self.df_result['Close'] / self.df_result['公告每股淨值']
        # self.df_result['PB'] = 0
        self.df_result['PE4'] = self.df_result['Close'] / self.df_result['原始每股稅後盈餘4']
        
        self.df_result['股票股利合計殖利率'] = self.df_result['股票股利合計'] / self.df_result['Close']*100
        self.df_result['現金股利合計殖利率'] = self.df_result['現金股利合計'] / self.df_result['Close']*100
        self.df_result['股利合計殖利率'] = self.df_result['股利合計'] / self.df_result['Close']*100
                
        self.df_result['股票股利合計殖利率4'] = self.df_result['股票股利合計4'] / self.df_result['Close']*100
        self.df_result['現金股利合計殖利率4'] = self.df_result['現金股利合計4'] / self.df_result['Close']*100
        self.df_result['股利合計殖利率4'] = self.df_result['股利合計4'] / self.df_result['Close']*100
        
        self.df_result['股票股利合計殖利率_平均_3yr'] = self.df_result['股票股利合計_平均_3yr'] / self.df_result['Close']*100
        self.df_result['現金股利合計殖利率_平均_3yr'] = self.df_result['現金股利合計_平均_3yr'] / self.df_result['Close']*100
        self.df_result['股利合計殖利率_平均_3yr'] = self.df_result['股利合計_平均_3yr'] / self.df_result['Close']*100
        self.df_result['股票股利合計殖利率_平均_5yr'] = self.df_result['股票股利合計_平均_5yr'] / self.df_result['Close']*100
        self.df_result['現金股利合計殖利率_平均_5yr'] = self.df_result['現金股利合計_平均_5yr'] / self.df_result['Close']*100
        self.df_result['股利合計殖利率_平均_5yr'] = self.df_result['股利合計_平均_5yr'] / self.df_result['Close']*100
        self.df_result['股票股利合計殖利率_平均_10yr'] = self.df_result['股票股利合計_平均_10yr'] / self.df_result['Close']*100
        self.df_result['現金股利合計殖利率_平均_10yr'] = self.df_result['現金股利合計_平均_10yr'] / self.df_result['Close']*100
        self.df_result['股利合計殖利率_平均_10yr'] = self.df_result['股利合計_平均_10yr'] / self.df_result['Close']*100
        
        
        self.df_result['負債比率'] = self.df_result['負債總計'] / self.df_result['資產總計']*100
        self.df_result['非流動負債比率'] = self.df_result['非流動負債'] / self.df_result['資產總計']*100
    def _insert_db(self):

        #產生db table
        self._create_table()
        
        self.df_result=self.df_result.replace([np.inf, -np.inf], np.nan).copy()
        self.df_result.insert(0,'id',0)
        self.df_result['CreateTime']=datetime.now()
        self.df_result['Creator']='datetime'
        self.df_result['ModifiedTime']=datetime.now()
        self.df_result['Modified_User']='datetime'
        #篩選所需日期資料
        self.df_result=self.df_result[self.df_result['Date']>=self.StartDate]

        self.db_conn_insert.execute('delete from  %s.`%s` where date>="%s" and date<="%s"' % (self.indistockdb_name,self.Ticker,self.StartDate.strftime('%Y%m%d'),self.EndDate.strftime('%Y%m%d')))

        self.df_result.to_sql(con=self.db_conn_insert, 
                          name=self.Ticker, 
                          if_exists='append',
                          index=False)
def indistock_importer(Ticker: str, StartDate: datetime, EndDate: datetime,StartDateFi: str, EndDateFi: str,SeasonArray,db_conn_insert,db_conn_query):
    try:
        time_start = time.time()
        time_start1 = time.time()
        #為了可以補上週月年資料
        StartDate_Data = StartDate - relativedelta(months=+1)
        ETF_list = ['0050','0056']
        Index_list = ['TWA00', 'TWA02', 'TWA04', 'TWA05', 'TWA06', 'TWA64', 'TWA6N', 'TWA6S', 'TWB11', 'TWB12', 'TWB13', 'TWB14', 'TWB15', 'TWB16', 'TWB18', 'TWB19', 'TWB20', 'TWB21', 'TWB22', 'TWB25', 'TWB26', 'TWB27', 'TWB28', 'TWB29', 'TWB30', 'TWB31', 'TWB32', 'TWB33', 'TWB34', 'TWB35', 'TWB36', 'TWB37', 'TWB38', 'TWB39', 'TWB40', 'TWB99', 'TWC37', 'TWC38', 'TWC39']
        is_Index = True if Ticker in Index_list+ETF_list else False
        #取得dailydata資料
        if Ticker in ETF_list + Index_list :
            #_SQL='Select `Date`, `Ticker`, `CorpName`,"" as `industry`,"" as `Sector` , (null) as `上市日期`, (null) as `上櫃日期`,`開盤價` as `Open`, `最高價` as `High`, `最低價` as `Low`, `收盤價` as `Close`,`漲幅`,`成交量` as `Volume`, `成交金額` as `Amount`,市值比重,週轉率,成交值比重   from `marketstddb`.`' + Ticker + '`   where  `Date` between "'+ StartDate_Data.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'"'
            _SQL='Select `Date`, `Ticker`, `CorpName`,"" as `industry`,"" as `Sector` , (null) as `上市日期`, (null) as `上櫃日期`,`Open`, `High`, `Low`, `Close`,`漲幅`,`Volume`, `Amount`,`市值比重`,`週轉率`,`成交值比重`,`Open_Adj`, `High_Adj`,`Low_Adj`,`Close_Adj`,`Beta係數21D`, `Beta係數65D`, `Beta係數250D`, `外資買張`, `外資賣張`, `外資買賣超`, `外資持股異動`, `外資持股張數`, `外資及陸資買張`, `外資及陸資賣張`, `外資及陸資買賣超`, `外資自營商買張`, `外資自營商賣張`, `外資自營商買賣超`, `外資買金額`, `外資賣金額`, `外資買賣超金額`, `外資買均價`, `外資賣均價`, `外資持股比率`, `外資持股市值`, `外資持股成本`, `外資尚可投資張數`, `外資尚可投資比率`, `外資投資上限比率`, `陸資投資上限比率`, `與前日異動原因`,自營商買張, 自營商賣張, 自營商買賣超, 自營商買張_自行買賣, 自營商賣張_自行買賣, 自營商買賣超_自行買賣, 自營商買張_避險, 自營商賣張_避險, 自營商買賣超_避險, 自營商庫存, 自營商買金額, 自營商賣金額, 自營商買賣超金額, 自營商買均價, 自營商賣均價, 自營商持股比率, 自營商持股市值, 自營商持股成本, 投信買張, 投信賣張, 投信買賣超, 投信庫存, 投信買金額, 投信賣金額, 投信買賣超金額, 投信買均價, 投信賣均價, 投信持股比率, 投信持股市值, 投信持股成本  from `marketstddb`.`' + Ticker + '` where  `Date` between "'+ StartDate_Data.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'"'
        else:
            _SQL='Select `Date`, `Ticker`, `CorpName`,B.`產業名稱` as `industry`,B.`指數彙編分類` as `Sector` ,B.上市日期 ,B.`上櫃日期`,`Open`, `High`, `Low`, `Close`,`漲幅`,`Volume`, `Amount`,`市值比重`,`週轉率`,`成交值比重`,`Open_Adj`, `High_Adj`,`Low_Adj`,`Close_Adj`,`Beta係數21D`, `Beta係數65D`, `Beta係數250D`, `外資買張`, `外資賣張`, `外資買賣超`, `外資持股異動`, `外資持股張數`, `外資及陸資買張`, `外資及陸資賣張`, `外資及陸資買賣超`, `外資自營商買張`, `外資自營商賣張`, `外資自營商買賣超`, `外資買金額`, `外資賣金額`, `外資買賣超金額`, `外資買均價`, `外資賣均價`, `外資持股比率`, `外資持股市值`, `外資持股成本`, `外資尚可投資張數`, `外資尚可投資比率`, `外資投資上限比率`, `陸資投資上限比率`, `與前日異動原因`,自營商買張, 自營商賣張, 自營商買賣超, 自營商買張_自行買賣, 自營商賣張_自行買賣, 自營商買賣超_自行買賣, 自營商買張_避險, 自營商賣張_避險, 自營商買賣超_避險, 自營商庫存, 自營商買金額, 自營商賣金額, 自營商買賣超金額, 自營商買均價, 自營商賣均價, 自營商持股比率, 自營商持股市值, 自營商持股成本, 投信買張, 投信賣張, 投信買賣超, 投信庫存, 投信買金額, 投信賣金額, 投信買賣超金額, 投信買均價, 投信賣均價, 投信持股比率, 投信持股市值, 投信持股成本  from `marketstddb`.`' + Ticker + '` as A left join `marketrawdb_cm`.`bd_cm_companyprofile` as B on (A.Ticker=B.`股票代號`) where  `Date` between "'+ StartDate_Data.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'" and B.`年度`='+ str(EndDate.year)
        df_dailydata = pd.read_sql_query(_SQL, db_conn_query)
        df_dailydata['Date']=pd.to_datetime(df_dailydata['Date'])
        df_dailydata=df_dailydata.sort_values(by=['Date'])
        print('取得日資料.',(time.time() - time_start1))
       
        time_start1 = time.time()
        if is_Index:
            df_result = df_dailydata
            dairlyT=time.time()-time_start1
        else:
            #取得月營收資料
            _SQL='Select `公告日` as `日期_月營收_公布日`,concat(`年月`,"01") as `日期_月營收_資料日`,0 as Flag_月營收, COALESCE(`單月合併營收`,`單月營收`) AS `單月合併月營收`, COALESCE(`累計合併營收`,`累計營收`) AS `累計合併月營收`  from `marketrawdb_cm`.`md_cm_fi_monthsales` where  `公告日` between "'+ StartDate_Data.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'"  and  `股票代號` = "' + Ticker + '"'
            df_monthsales = pd.read_sql_query(_SQL, db_conn_query)
            df_monthsales['日期_月營收_公布日']=pd.to_datetime(df_monthsales['日期_月營收_公布日'])
            df_monthsales=df_monthsales.sort_values(by=['日期_月營收_公布日'])
            print('取得月營收資料.',(time.time() - time_start1))
            
            time_start1 = time.time()
            #取得集保庫存資料
            _SQL='Select  DATE_ADD(`日期`, INTERVAL 1 DAY) as `日期_集保庫存_公布日`,`日期` as `日期_集保庫存_資料日` ,0 as Flag_集保庫存 ,  近1週1000張以上集保比率變動, 1張以下_人, 5張以下_人, 10張以下_人, 15張以下_人, 20張以下_人, 30張以下_人, 40張以下_人, 50張以下_人, 100張以下_人, 200張以下_人, 400張以下_人, 600張以下_人, 800張以下_人, 1張以上_人, 5張以上_人, 10張以上_人, 15張以上_人, 20張以上_人, 30張以上_人, 40張以上_人, 50張以上_人, 100張以上_人, 200張以上_人, 400張以上_人, 600張以上_人, 800張以上_人, 1000張以上_人, 1張以下_張, 5張以下_張, 10張以下_張, 15張以下_張, 20張以下_張, 30張以下_張, 40張以下_張, 50張以下_張, 100張以下_張, 200張以下_張, 400張以下_張, 600張以下_張, 800張以下_張, 1張以上_張, 5張以上_張, 10張以上_張, 15張以上_張, 20張以上_張, 30張以上_張, 40張以上_張, 50張以上_張, 100張以上_張, 200張以上_張, 400張以上_張, 600張以上_張, 800張以上_張, 1000張以上_張, 1張以下佔集保比率, 5張以下佔集保比率, 10張以下佔集保比率, 15張以下佔集保比率, 20張以下佔集保比率, 30張以下佔集保比率, 40張以下佔集保比率, 50張以下佔集保比率, 100張以下佔集保比率, 200張以下佔集保比率, 400張以下佔集保比率, 600張以下佔集保比率, 800張以下佔集保比率, 1張以上佔集保比率, 5張以上佔集保比率, 10張以上佔集保比率, 15張以上佔集保比率, 20張以上佔集保比率, 30張以上佔集保比率, 40張以上佔集保比率, 50張以上佔集保比率, 100張以上佔集保比率, 200張以上佔集保比率, 400張以上佔集保比率, 600張以上佔集保比率, 800張以上佔集保比率, 1000張以上佔集保比率 from `marketrawdb_cm`.`md_cm_fd_stockholderstructure` where  `日期` between "'+ StartDate_Data.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'"  and  `股票代號` = "' + Ticker + '"'
            df_HolderStruc=pd.read_sql_query(_SQL, db_conn_query)
            df_HolderStruc['日期_集保庫存_公布日']=pd.to_datetime(df_HolderStruc['日期_集保庫存_公布日'])
            df_HolderStruc=df_HolderStruc.sort_values(by=['日期_集保庫存_公布日'])
            print('取得集保庫存資料.',(time.time() - time_start1))
                
            time_start1 = time.time()
            #取得月董監股權與設質統計表
            _SQL='Select Date_Add(cast(concat(`年月`,"15") as date),INTERVAL 1 MONTH) as `日期_董事持股_公布日`,concat(`年月`,"01") as `日期_董事持股_資料日`,0 as Flag_董事持股, 董事持股張數, 董事持股張數增減, 董事持股比例, 董事持股比例增減, 董事關係人持股張數, 董事關係人持股張數增減, 董事關係人持股比例, 董事關係人持股比例增減, 董事及其關係人持股張數, 董事及其關係人持股張數增減, 董事及其關係人持股比例, 董事及其關係人持股比例增減, 監察人持股張數, 監察人持股張數增減, 監察人持股比例, 監察人持股比例增減, 監察人關係人持股張數, 監察人關係人持股張數增減, 監察人關係人持股比例, 監察人關係人持股比例增減, 監察人及其關係人持股張數, 監察人及其關係人持股張數增減, 監察人及其關係人持股比例, 監察人及其關係人持股比例增減, 董監持股張數, 董監持股張數增減, 董監持股比例, 董監持股比例增減, 董監及其關係人持股張數, 董監及其關係人持股張數增減, 董監及其關係人持股比例, 董監及其關係人持股比例增減, 經理人持股張數, 經理人持股張數增減, 經理人持股比例, 經理人持股比例增減, 經理人關係人持股張數, 經理人關係人持股張數增減, 經理人關係人持股比例, 經理人關係人持股比例增減, 經理人及其關係人持股張數, 經理人及其關係人持股張數增減, 經理人及其關係人持股比例, 經理人及其關係人持股比例增減, 大股東持股張數, 大股東持股張數增減, 大股東持股比例, 大股東持股比例增減, 大股東關係人持股張數, 大股東關係人持股張數增減, 大股東關係人持股比例, 大股東關係人持股比例增減, 大股東及其關係人持股張數, 大股東及其關係人持股張數增減, 大股東及其關係人持股比例, 大股東及其關係人持股比例增減, 大股東經理人持股張數, 大股東經理人持股張數增減, 大股東經理人持股比例, 大股東經理人持股比例增減, 大股東經理人及其關係人持股張數, 大股東經理人及其關係人持股張數增減, 大股東經理人及其關係人持股比例, 大股東經理人及其關係人持股比例增減, 內部人持股張數, 內部人持股張數增減, 內部人持股比例, 內部人持股比例增減, 內部人關係人持股張數, 內部人關係人持股張數增減, 內部人關係人持股比例, 內部人關係人持股比例增減, 內部人及其關係人持股張數, 內部人及其關係人持股張數增減, 內部人及其關係人持股比例, 內部人及其關係人持股比例增減, 全體持股張數, 全體持股張數增減, 全體持股比例, 全體持股比例增減, 全體關係人持股張數, 全體關係人持股張數增減, 全體關係人持股比例, 全體關係人持股比例增減, 全體及其關係人持股張數, 全體及其關係人持股張數增減, 全體及其關係人持股比例, 全體及其關係人持股比例增減, 董事設質張數, 董事設質張數增減, 董事設質比例, 董事設質比例增減, 董事關係人設質張數, 董事關係人設質張數增減, 董事關係人設質比例, 董事關係人設質比例增減, 董事及其關係人設質張數, 董事及其關係人設質張數增減, 董事及其關係人設質比例, 董事及其關係人設質比例增減, 監察人設質張數, 監察人設質張數增減, 監察人設質比例, 監察人設質比例增減, 監察人關係人設質張數, 監察人關係人設質張數增減, 監察人關係人設質比例, 監察人關係人設質比例增減, 監察人及其關係人設質張數, 監察人及其關係人設質張數增減, 監察人及其關係人設質比例, 監察人及其關係人設質比例增減, 董監設質張數, 董監設質張數增減, 董監設質比例, 董監設質比例增減, 董監及其關係人設質張數, 董監及其關係人設質張數增減, 董監及其關係人設質比例, 董監及其關係人設質比例增減, 經理人設質張數, 經理人設質張數增減, 經理人設質比例, 經理人設質比例增減, 經理人關係人設質張數, 經理人關係人設質張數增減, 經理人關係人設質比例, 經理人關係人設質比例增減, 經理人及其關係人設質張數, 經理人及其關係人設質張數增減, 經理人及其關係人設質比例, 經理人及其關係人設質比例增減, 大股東設質張數, 大股東設質張數增減, 大股東設質比例, 大股東設質比例增減, 大股東關係人設質張數, 大股東關係人設質張數增減, 大股東關係人設質比例, 大股東關係人設質比例增減, 大股東及其關係人設質張數, 大股東及其關係人設質張數增減, 大股東及其關係人設質比例, 大股東及其關係人設質比例增減, 大股東經理人設質張數, 大股東經理人設質張數增減, 大股東經理人設質比例, 大股東經理人設質比例增減, 大股東經理人及其關係人設質張數, 大股東經理人及其關係人設質張數增減, 大股東經理人及其關係人設質比例, 大股東經理人及其關係人設質比例增減, 內部人設質張數, 內部人設質張數增減, 內部人設質比例, 內部人設質比例增減, 內部人關係人設質張數, 內部人關係人設質張數增減, 內部人關係人設質比例, 內部人關係人設質比例增減, 內部人及其關係人設質張數, 內部人及其關係人設質張數增減, 內部人及其關係人設質比例, 內部人及其關係人設質比例增減, 全體設質張數, 全體設質張數增減, 全體設質比例, 全體設質比例增減, 全體關係人設質張數, 全體關係人設質張數增減, 全體關係人設質比例, 全體關係人設質比例增減, 全體及其關係人設質張數, 全體及其關係人設質張數增減, 全體及其關係人設質比例, 全體及其關係人設質比例增減, 董事私募張數, 董事私募張數增減, 董事私募比例, 董事私募比例增減, 董事關係人私募張數, 董事關係人私募張數增減, 董事關係人私募比例, 董事關係人私募比例增減, 董事及其關係人私募張數, 董事及其關係人私募張數增減, 董事及其關係人私募比例, 董事及其關係人私募比例增減, 監察人私募張數, 監察人私募張數增減, 監察人私募比例, 監察人私募比例增減, 監察人關係人私募張數, 監察人關係人私募張數增減, 監察人關係人私募比例, 監察人關係人私募比例增減, 監察人及其關係人私募張數, 監察人及其關係人私募張數增減, 監察人及其關係人私募比例, 監察人及其關係人私募比例增減, 董監私募張數, 董監私募張數增減, 董監私募比例, 董監私募比例增減, 董監及其關係人私募張數, 董監及其關係人私募張數增減, 董監及其關係人私募比例, 董監及其關係人私募比例增減, 經理人私募張數, 經理人私募張數增減, 經理人私募比例, 經理人私募比例增減, 經理人關係人私募張數, 經理人關係人私募張數增減, 經理人關係人私募比例, 經理人關係人私募比例增減, 經理人及其關係人私募張數, 經理人及其關係人私募張數增減, 經理人及其關係人私募比例, 經理人及其關係人私募比例增減, 大股東私募張數, 大股東私募張數增減, 大股東私募比例, 大股東私募比例增減, 大股東關係人私募張數, 大股東關係人私募張數增減, 大股東關係人私募比例, 大股東關係人私募比例增減, 大股東及其關係人私募張數, 大股東及其關係人私募張數增減, 大股東及其關係人私募比例, 大股東及其關係人私募比例增減, 大股東經理人私募張數, 大股東經理人私募張數增減, 大股東經理人私募比例, 大股東經理人私募比例增減, 大股東經理人及其關係人私募張數, 大股東經理人及其關係人私募張數增減, 大股東經理人及其關係人私募比例, 大股東經理人及其關係人私募比例增減, 內部人私募張數, 內部人私募張數增減, 內部人私募比例, 內部人私募比例增減, 內部人關係人私募張數, 內部人關係人私募張數增減, 內部人關係人私募比例, 內部人關係人私募比例增減, 內部人及其關係人私募張數, 內部人及其關係人私募張數增減, 內部人及其關係人私募比例, 內部人及其關係人私募比例增減, 全體私募張數, 全體私募張數增減, 全體私募比例, 全體私募比例增減, 全體關係人私募張數, 全體關係人私募張數增減, 全體關係人私募比例, 全體關係人私募比例增減, 全體及其關係人私募張數, 全體及其關係人私募張數增減, 全體及其關係人私募比例, 全體及其關係人私募比例增減, 獨立董事人數, 常董人數, 一般董事人數, 董事總人數, 獨立監察人數, 常監人數, 一般監察人數, 監察總人數 from `marketrawdb_cm`.`md_cm_fd_insiderholdingstructure` where  Date_Add(cast(concat(`年月`,"15") as date),INTERVAL 1 MONTH) between "'+ StartDate_Data.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'" and `股票代號` = "' + Ticker + '"'
            df_insiderholdingstructure = pd.read_sql_query(_SQL, db_conn_query)
            df_insiderholdingstructure['日期_董事持股_公布日']=pd.to_datetime(df_insiderholdingstructure['日期_董事持股_公布日'])
            df_insiderholdingstructure=df_insiderholdingstructure.sort_values(by=['日期_董事持股_公布日'])
            print('取得月董監股權與設質統計表.',( time.time() - time_start1))
        
            dairlyT=time.time()-time_start
            print('取得財報之前花費.============',dairlyT)
            
            time_start1 = time.time()
            #取得財報BS
            # CM財報BS----------------------------------------
            _SQL='Select 公告日期 as 日期_財報_公告日,年季,case(right(`年季`,2)) when "01" then cast(concat(left(`年季`,4),"0331") as date) when "02" then cast(concat(left(年季,4),"0630") as date) when "03" then cast(concat(left(年季,4),"0930") as date) when "04" then cast(concat(left(年季,4),"1231") as date) end  as 日期_財報_資料日,0 as Flag_財報,流動資產,現金及約當現金,短期投資合計,應收帳款與票據合計,存貨,非流動資產,採用權益法之投資,長期投資合計,不動產、廠房及設備,無形資產,資產總計,流動負債,非流動負債,負債總計,股本,普通股股本,特別股股本,資本公積,保留盈餘,其他權益,庫藏股票,共同控制下前手權益,合併前非屬共同控制股權,母公司業主權益,非控制權益,權益總計,原始每股淨值,公告每股淨值,負債及權益總計,預收股款之約當發行股數,母公司暨子公司所持有之母公司庫藏股,(ifnull(普通股股本,0)+ifnull(特別股股本,0)+0.01* ifnull(預收股款之約當發行股數,0) - 0.01*ifnull(母公司暨子公司所持有之母公司庫藏股,0)) as 流通在外股本 from `marketrawdb_cm`.`md_cm_fi_bs_quarterly` where `股票代號` = "' + Ticker + '" and 年季 between "'+ StartDateFi +'" and "'+ EndDateFi +'"  order by 年季 asc'
            df_md_cm_fi_bs_quarterly = pd.read_sql_query(_SQL, db_conn_query)
            df_md_cm_fi_bs_quarterly=df_md_cm_fi_bs_quarterly.fillna(0)
            df_md_cm_fi_bs_quarterly=cutfin(df_md_cm_fi_bs_quarterly,'BS')
            df_md_cm_fi_bs_quarterly=df_md_cm_fi_bs_quarterly.loc[df_md_cm_fi_bs_quarterly.日期_財報_公告日!=0].copy()
            df_md_cm_fi_bs_quarterly['日期_財報_公告日']=pd.to_datetime(df_md_cm_fi_bs_quarterly['日期_財報_公告日'])
            
            
            df_md_cm_fi_bs_quarterly4=pd.DataFrame(df_md_cm_fi_bs_quarterly[['流動資產','現金及約當現金','短期投資合計','應收帳款與票據合計','存貨','非流動資產','採用權益法之投資','長期投資合計','不動產、廠房及設備','無形資產','資產總計','流動負債','非流動負債','負債總計','股本','普通股股本','特別股股本','資本公積','保留盈餘','其他權益','庫藏股票','共同控制下前手權益','合併前非屬共同控制股權','母公司業主權益','非控制權益','權益總計','原始每股淨值','公告每股淨值','負債及權益總計','預收股款之約當發行股數','母公司暨子公司所持有之母公司庫藏股']].rolling(4).mean()).set_axis(['流動資產4','現金及約當現金4','短期投資合計4','應收帳款與票據合計4','存貨4','非流動資產4','採用權益法之投資4','長期投資合計4','不動產、廠房及設備4','無形資產4','資產總計4','流動負債4','非流動負債4','負債總計4','股本4','普通股股本4','特別股股本4','資本公積4','保留盈餘4','其他權益4','庫藏股票4','共同控制下前手權益4','合併前非屬共同控制股權4','母公司業主權益4','非控制權益4','權益總計4','原始每股淨值4','公告每股淨值4','負債及權益總計4','預收股款之約當發行股數4','母公司暨子公司所持有之母公司庫藏股4'],axis=1,inplace=False)
            # df_md_cm_fi_bs_quarterly4=df_md_cm_fi_bs_quarterly4.fillna(0)
            流通在外股本4=pd.DataFrame(df_md_cm_fi_bs_quarterly4['普通股股本4']+df_md_cm_fi_bs_quarterly4['特別股股本4']+0.01*df_md_cm_fi_bs_quarterly4['預收股款之約當發行股數4']-0.01*+df_md_cm_fi_bs_quarterly4['母公司暨子公司所持有之母公司庫藏股4']).set_axis(['流通在外股本4'],axis=1,inplace=False)
                
            if df_md_cm_fi_bs_quarterly.shape[0]==df_md_cm_fi_bs_quarterly4.shape[0] :
                df_md_cm_fi_quarterly=pd.concat([df_md_cm_fi_bs_quarterly,df_md_cm_fi_bs_quarterly4,流通在外股本4],axis=1)
                print('df_md_cm_fi_quarterly yes 完成4')
            else:
                print('df_md_cm_fi_quarterly no')
                
            # CM財報IS==================================================
            _SQL='Select 年季,銷貨收入淨額,營業收入淨額,營業成本,營業毛利,營業毛利淨額,營業費用,推銷費用,管理費用,研發費用,預期信用減損損益_營業費用,其他收益及費損,營業利益,營業外收入及支出,其他收入,利息收入,租金收入,權利金收入,股利收入,財務成本,利息費用,預期信用減損損益,採用權益法之關聯企業及合資損益之份額,除列按攤銷後成本衡量金融資產淨損益,金融資產重分類淨損益,營業外收入,營業外支出,稅前純益,所得稅,繼續營業單位損益,停業部門損益,合併前非屬共同控制股權損益,稅後純益,其他綜合損益,綜合損益,母公司業主–稅後純益 as 母公司業主_稅後純益,非控制權益–稅後純益 as 非控制權益_稅後純益,共同控制下前手權益–稅後純益 as 共同控制下前手權益_稅後純益,母公司業主–綜合損益 as 母公司業主_綜合損益,非控制權益–綜合損益 as 非控制權益_綜合損益,共同控制下前手權益–綜合損益 as 共同控制下前手權益_綜合損益,EBITDA,公告基本每股盈餘,公告稀釋每股盈餘,原始每股稅前盈餘,原始每股稅後盈餘,原始每股綜合盈餘 from `marketrawdb_cm`.`md_cm_fi_is_quarterly` where `股票代號` = "' + Ticker + '" and 年季 between "'+ StartDateFi +'" and "'+ EndDateFi +'" order by 年季 asc'
            df_md_cm_fi_is_quarterly = pd.read_sql_query(_SQL, db_conn_query)
            df_md_cm_fi_is_quarterly=df_md_cm_fi_is_quarterly.fillna(0)
            df_md_cm_fi_is_quarterly=cutfin(df_md_cm_fi_is_quarterly,'IS')
            # df_md_cm_fi_bs_quarterly['日期_財報_公布日']=pd.to_datetime(df_md_cm_fi_bs_quarterly['日期_財報_公布日'])
            
            df_md_cm_fi_is_quarterly4=pd.DataFrame(df_md_cm_fi_is_quarterly[['銷貨收入淨額','營業收入淨額','營業成本','營業毛利','營業毛利淨額','營業費用','推銷費用','管理費用','研發費用','預期信用減損損益_營業費用','其他收益及費損','營業利益','營業外收入及支出','其他收入','利息收入','租金收入','權利金收入','股利收入','財務成本','利息費用','預期信用減損損益','採用權益法之關聯企業及合資損益之份額','除列按攤銷後成本衡量金融資產淨損益','金融資產重分類淨損益','營業外收入','營業外支出','稅前純益','所得稅','繼續營業單位損益','停業部門損益','合併前非屬共同控制股權損益','稅後純益','其他綜合損益','綜合損益','母公司業主_稅後純益','非控制權益_稅後純益','共同控制下前手權益_稅後純益','母公司業主_綜合損益','非控制權益_綜合損益','共同控制下前手權益_綜合損益','EBITDA','公告基本每股盈餘','公告稀釋每股盈餘','原始每股稅前盈餘','原始每股稅後盈餘','原始每股綜合盈餘']].rolling(4).sum()).set_axis(['銷貨收入淨額4','營業收入淨額4','營業成本4','營業毛利4','營業毛利淨額4','營業費用4','推銷費用4','管理費用4','研發費用4','預期信用減損損益_營業費用4','其他收益及費損4','營業利益4','營業外收入及支出4','其他收入4','利息收入4','租金收入4','權利金收入4','股利收入4','財務成本4','利息費用4','預期信用減損損益4','採用權益法之關聯企業及合資損益之份額4','除列按攤銷後成本衡量金融資產淨損益4','金融資產重分類淨損益4','營業外收入4','營業外支出4','稅前純益4','所得稅4','繼續營業單位損益4','停業部門損益4','合併前非屬共同控制股權損益4','稅後純益4','其他綜合損益4','綜合損益4','母公司業主_稅後純益4','非控制權益_稅後純益4','共同控制下前手權益_稅後純益4','母公司業主_綜合損益4','非控制權益_綜合損益4','共同控制下前手權益_綜合損益4','EBITDA4','公告基本每股盈餘4','公告稀釋每股盈餘4','原始每股稅前盈餘4','原始每股稅後盈餘4','原始每股綜合盈餘4'],axis=1,inplace=False)

            
            if df_md_cm_fi_is_quarterly.shape[0]==df_md_cm_fi_is_quarterly4.shape[0] :
                df_md_cm_fi_is_quarterly=pd.concat([df_md_cm_fi_is_quarterly,df_md_cm_fi_is_quarterly4],axis=1)
                print('df_md_cm_fi_is_quarterly yes 完成4')
                if df_md_cm_fi_quarterly.shape[0]==df_md_cm_fi_is_quarterly.shape[0] :
                    print('df_md_cm_fi_is_quarterly 與 BS表同長度')
                else:
                    print('df_md_cm_fi_is_quarterly 與 BS表不同長度')
                df_md_cm_fi_quarterly = pd.merge(df_md_cm_fi_quarterly, df_md_cm_fi_is_quarterly, how='left', on=['年季'])
                
                df_md_cm_fi_quarterly['原始每股稅前盈餘4']=pd.DataFrame(df_md_cm_fi_quarterly['稅前純益4']/df_md_cm_fi_quarterly['流通在外股本4']*10)
                df_md_cm_fi_quarterly['原始每股稅後盈餘4']=pd.DataFrame(df_md_cm_fi_quarterly['稅後純益4']/df_md_cm_fi_quarterly['流通在外股本4']*10)
                df_md_cm_fi_quarterly['原始每股綜合盈餘4']=pd.DataFrame(df_md_cm_fi_quarterly['綜合損益4']/df_md_cm_fi_quarterly['流通在外股本4']*10)
                

            else:
                print('df_md_cm_fi_is_quarterly no')
            # CM財報CF==================================================
            _SQL='Select  年季 ,營業活動現金流量,不影響現金流量之收益費損項目,折舊費用,攤銷費用,與營業活動相關資產之淨變動,與營業活動相關負債之淨變動,營運產生之現金流動,營業活動之收現類別,營業活動之付現類別,投資活動現金流量,籌資活動現金流量,匯率變動對現金及約當現金之影響,本期現金及約當現金增減數,自由現金流量 from `marketrawdb_cm`.`md_cm_fi_cf_quarterly` where `股票代號` = "' + Ticker + '" and 年季 between "'+ StartDateFi +'" and "'+ EndDateFi +'" order by 年季 asc'
            df_md_cm_fi_cf_quarterly = pd.read_sql_query(_SQL, db_conn_query)
            df_md_cm_fi_cf_quarterly=df_md_cm_fi_cf_quarterly.fillna(0)
            # df_md_cm_fi_cf_quarterly['日期_財報_建立日_cf']=pd.to_datetime(df_md_cm_fi_cf_quarterly['日期_財報_建立日_cf'])
            df_md_cm_fi_cf_quarterly=cutfin(df_md_cm_fi_cf_quarterly,'IS')
            
            df_md_cm_fi_cf_quarterly4=pd.DataFrame(df_md_cm_fi_cf_quarterly[['營業活動現金流量','不影響現金流量之收益費損項目','折舊費用','攤銷費用','與營業活動相關資產之淨變動','與營業活動相關負債之淨變動','營運產生之現金流動','營業活動之收現類別','營業活動之付現類別','投資活動現金流量','籌資活動現金流量','匯率變動對現金及約當現金之影響','本期現金及約當現金增減數','自由現金流量']].rolling(4).sum()).set_axis(['營業活動現金流量4','不影響現金流量之收益費損項目4','折舊費用4','攤銷費用4','與營業活動相關資產之淨變動4','與營業活動相關負債之淨變動4','營運產生之現金流動4','營業活動之收現類別4','營業活動之付現類別4','投資活動現金流量4','籌資活動現金流量4','匯率變動對現金及約當現金之影響4','本期現金及約當現金增減數4','自由現金流量4'],axis=1,inplace=False)
            # df_md_cm_fi_cf_quarterly4=df_md_cm_fi_cf_quarterly4.fillna(0)
            
            if df_md_cm_fi_cf_quarterly.shape[0]==df_md_cm_fi_cf_quarterly4.shape[0] :
                df_md_cm_fi_cf_quarterly=pd.concat([df_md_cm_fi_cf_quarterly,df_md_cm_fi_cf_quarterly4],axis=1)
                print('df_md_cm_fi_cf_quarterly yes 完成4')
                if df_md_cm_fi_quarterly.shape[0]==df_md_cm_fi_cf_quarterly.shape[0] :
                    print('df_md_cm_fi_cf_quarterly 與 BS表同長度 ')
                else:
                    print('df_md_cm_fi_cf_quarterly 與 BS表不同長度')
                df_md_cm_fi_quarterly = pd.merge(df_md_cm_fi_quarterly, df_md_cm_fi_cf_quarterly, how='left', on=['年季'])
                # sys.exit()
            else:
                print('df_md_cm_fi_cf_quarterly no')
            
            
            # df_md_cm_fi_quarterly=df_md_cm_fi_quarterly.drop(columns=['年季'])
            df_md_cm_fi_quarterly=df_md_cm_fi_quarterly.sort_values(by=['日期_財報_公告日'])
            print('取得BS IS CF .',( time.time() - time_start1))
            
            time_start1 = time.time()
        # ====股利========================================
            # _SQL='Select 年季,盈餘分派頻率,除權日,除息日,領股日期,領息日期,盈餘配股,公積配股,股票股利合計,盈餘配息,公積配息,現金股利合計,股利合計 from `marketrawdb_cm`.`md_cm_ot_dividendpolicy` where `股票代號` = "' + Ticker + '" and 年季 between "'+ StartDateFi +'" and "'+ EndDateFi +'"  and (除權日 or 除息日 ) is not null order by 年季 asc'
            _SQL='Select 年季,盈餘分派頻率,除權日,除息日,領股日期,領息日期,盈餘配股,公積配股,股票股利合計,盈餘配息,公積配息,現金股利合計,股利合計 from `marketrawdb_cm`.`md_cm_ot_dividendpolicy` where `股票代號` = "' + Ticker + '" and 年季 between "'+ StartDateFi +'" and "'+ EndDateFi +'"  and (股利合計 <>0)  order by 年季 asc'
            
            df_md_cm_ot_dividendpolicy = pd.read_sql_query(_SQL, db_conn_query)
            
            df=pd.DataFrame(df_md_cm_ot_dividendpolicy[['年季','盈餘分派頻率','盈餘配股','公積配股','股票股利合計','盈餘配息','公積配息','現金股利合計','股利合計']]).set_axis(['年季','盈餘分派頻率','盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t'],axis=1,inplace=False).copy()
            
            for i in  range(0,len(df),1):

                if df['盈餘分派頻率'].iloc[i]=='半年配':
                    df.iloc[i,2:]=df.iloc[i,2:]/2
                    df=df.append(df.iloc[i])
                    df.iloc[-1,0]=str(int(df.iloc[i,0])-1)
                elif df['盈餘分派頻率'].iloc[i]=='年配':
                    df.iloc[i,2:]=df.iloc[i,2:]/4
                    df=df.append(df.iloc[i])
                    df.iloc[-1,0]=str(int(df.iloc[i,0])-1)
                    df=df.append(df.iloc[i])
                    df.iloc[-1,0]=str(int(df.iloc[i,0])-2)
                    df=df.append(df.iloc[i])
                    df.iloc[-1,0]=str(int(df.iloc[i,0])-3)
            df=df.sort_values(by=['年季'])
            
            temp=SeasonArray.copy()
            
            temp=temp.merge( df,how='left', on=['年季'])   
            temp[['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t']]=temp[['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t']].fillna(0)  
            temp[['盈餘配股4','公積配股4','股票股利合計4','盈餘配息4','公積配息4','現金股利合計4','股利合計4']]=pd.DataFrame(temp[['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t']].rolling(4).sum()).copy()
            
            temp[['盈餘配股_平均_3yr','公積配股_平均_3yr','股票股利合計_平均_3yr','盈餘配息_平均_3yr','公積配息_平均_3yr','現金股利合計_平均_3yr','股利合計_平均_3yr']]=pd.DataFrame(temp[['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t']].rolling(12).sum()/3).copy()
            temp[['盈餘配股_平均_5yr','公積配股_平均_5yr','股票股利合計_平均_5yr','盈餘配息_平均_5yr','公積配息_平均_5yr','現金股利合計_平均_5yr','股利合計_平均_5yr']]=pd.DataFrame(temp[['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t']].rolling(20).sum()/5).copy()
            temp[['盈餘配股_平均_10yr','公積配股_平均_10yr','股票股利合計_平均_10yr','盈餘配息_平均_10yr','公積配息_平均_10yr','現金股利合計_平均_10yr','股利合計_平均_10yr']]=pd.DataFrame(temp[['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t']].rolling(40).sum()/10).copy()
            
            
            
            _SQL='Select 年季,case(right(`年季`,2)) when "01" then cast(concat(left(`年季`,4),"0331") as date) when "02" then cast(concat(left(年季,4),"0630") as date) when "03" then cast(concat(left(年季,4),"0930") as date) when "04" then cast(concat(left(年季,4),"1231") as date) end  as 日期_股利_資料日,0 as Flag_股利,盈餘分派頻率,除權日,除息日,領股日期,領息日期,盈餘配股,公積配股,股票股利合計,盈餘配息,公積配息,現金股利合計,股利合計,股票股利發放率,現金股利發放率,股利發放率,董事會決議通過股利分派日,股東會日期,公告日期 from `marketrawdb_cm`.`md_cm_ot_dividendpolicy` where `股票代號` = "' + Ticker + '" and 年季 between "'+ StartDateFi +'" and "'+ EndDateFi +'"   order by 年季 asc'
            df_md_cm_ot_dividendpolicy1 = pd.read_sql_query(_SQL, db_conn_query)
            
            df_md_cm_ot_dividendpolicy1['日期_股利_公告日']=df_md_cm_ot_dividendpolicy1[['董事會決議通過股利分派日','股東會日期','公告日期']].fillna(date(2121,1,1)).replace(date(1900,1,1), date(2121,1,1)).min(axis=1)
            df_md_cm_ot_dividendpolicy1=df_md_cm_ot_dividendpolicy1.merge( temp[['年季','盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t','盈餘配股4','公積配股4','股票股利合計4','盈餘配息4','公積配息4','現金股利合計4','股利合計4','盈餘配股_平均_3yr','公積配股_平均_3yr','股票股利合計_平均_3yr','盈餘配息_平均_3yr','公積配息_平均_3yr','現金股利合計_平均_3yr','股利合計_平均_3yr','盈餘配股_平均_5yr','公積配股_平均_5yr','股票股利合計_平均_5yr','盈餘配息_平均_5yr','公積配息_平均_5yr','現金股利合計_平均_5yr','股利合計_平均_5yr','盈餘配股_平均_10yr','公積配股_平均_10yr','股票股利合計_平均_10yr','盈餘配息_平均_10yr','公積配息_平均_10yr','現金股利合計_平均_10yr','股利合計_平均_10yr']],how='left', on=['年季'])
            
            for  i in  range(0,len(df_md_cm_ot_dividendpolicy1),1):
                # if df_md_cm_ot_dividendpolicy1['除權日'].loc[i]==None and df_md_cm_ot_dividendpolicy1['除息日'].loc[i]==None:
                if df_md_cm_ot_dividendpolicy1['股利合計'].loc[i]==0:
                    
                    df_md_cm_ot_dividendpolicy1.loc[i,['盈餘配股4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['盈餘配股4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['盈餘配股t']].squeeze()).copy()
                    df_md_cm_ot_dividendpolicy1.loc[i,['公積配股4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['公積配股4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['公積配股t']].squeeze()).copy()
                    df_md_cm_ot_dividendpolicy1.loc[i,['股票股利合計4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['股票股利合計4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['股票股利合計t']].squeeze()).copy()
                    df_md_cm_ot_dividendpolicy1.loc[i,['盈餘配息4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['盈餘配息4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['盈餘配息t']].squeeze()).copy()
                    df_md_cm_ot_dividendpolicy1.loc[i,['公積配息4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['公積配息4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['公積配息t']].squeeze()).copy()
                    df_md_cm_ot_dividendpolicy1.loc[i,['現金股利合計4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['現金股利合計4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['現金股利合計t']].squeeze()).copy()
                    df_md_cm_ot_dividendpolicy1.loc[i,['股利合計4']]=(df_md_cm_ot_dividendpolicy1.loc[i,['股利合計4']].squeeze()-df_md_cm_ot_dividendpolicy1.loc[i,['股利合計t']].squeeze()).copy()
            
            df_md_cm_ot_dividendpolicy1=df_md_cm_ot_dividendpolicy1.drop(columns=['盈餘配股t','公積配股t','股票股利合計t','盈餘配息t','公積配息t','現金股利合計t','股利合計t'])

            df_md_cm_fi_quarterly['年季']=df_md_cm_fi_quarterly['年季'].astype(float).copy()
            df_md_cm_ot_dividendpolicy1['年季']=df_md_cm_ot_dividendpolicy1['年季'].astype(float).copy()
            df_md_cm_ot_dividendpolicy1=df_md_cm_ot_dividendpolicy1.merge(df_md_cm_fi_quarterly[['年季','原始每股稅後盈餘4']],how='left', on=['年季'])
            df_md_cm_ot_dividendpolicy1['股票股利發放率4']=df_md_cm_ot_dividendpolicy1['股票股利合計4']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
            df_md_cm_ot_dividendpolicy1['現金股利發放率4']=df_md_cm_ot_dividendpolicy1['現金股利合計4']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
            df_md_cm_ot_dividendpolicy1['股利發放率4']=df_md_cm_ot_dividendpolicy1['股利合計4']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
            
            df_md_cm_ot_dividendpolicy1['股票股利發放率_平均_3yr']=df_md_cm_ot_dividendpolicy1['股票股利合計_平均_3yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
            df_md_cm_ot_dividendpolicy1['現金股利發放率_平均_3yr']=df_md_cm_ot_dividendpolicy1['現金股利合計_平均_3yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
            df_md_cm_ot_dividendpolicy1['股利發放率_平均_3yr']=df_md_cm_ot_dividendpolicy1['股利合計_平均_3yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
            df_md_cm_ot_dividendpolicy1['股票股利發放率_平均_5yr']=df_md_cm_ot_dividendpolicy1['股票股利合計_平均_5yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
            df_md_cm_ot_dividendpolicy1['現金股利發放率_平均_5yr']=df_md_cm_ot_dividendpolicy1['現金股利合計_平均_5yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
            df_md_cm_ot_dividendpolicy1['股利發放率_平均_5yr']=df_md_cm_ot_dividendpolicy1['股利合計_平均_5yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
            df_md_cm_ot_dividendpolicy1['股票股利發放率_平均_10yr']=df_md_cm_ot_dividendpolicy1['股票股利合計_平均_10yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
            df_md_cm_ot_dividendpolicy1['現金股利發放率_平均_10yr']=df_md_cm_ot_dividendpolicy1['現金股利合計_平均_10yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
            df_md_cm_ot_dividendpolicy1['股利發放率_平均_10yr']=df_md_cm_ot_dividendpolicy1['股利合計_平均_10yr']/df_md_cm_ot_dividendpolicy1['原始每股稅後盈餘4']*100
            
            
            df_md_cm_ot_dividendpolicy1=df_md_cm_ot_dividendpolicy1.drop(columns=['年季','原始每股稅後盈餘4'])
            df_md_cm_ot_dividendpolicy1['日期_股利_公告日']=pd.to_datetime(df_md_cm_ot_dividendpolicy1['日期_股利_公告日'])
            df_md_cm_ot_dividendpolicy1=df_md_cm_ot_dividendpolicy1.sort_values(by=['日期_股利_公告日'])
            
            df_md_cm_fi_quarterly=df_md_cm_fi_quarterly.drop(columns=['年季'])

            df_result=pd.merge_asof(df_dailydata,df_monthsales,left_on='Date',right_on='日期_月營收_公布日')
            df_result=pd.merge_asof(df_result,df_HolderStruc,left_on='Date',right_on='日期_集保庫存_公布日')
            df_result=pd.merge_asof(df_result,df_insiderholdingstructure,left_on='Date',right_on='日期_董事持股_公布日')
            df_result=pd.merge_asof(df_result,df_md_cm_fi_quarterly,left_on='Date',right_on='日期_財報_公告日')
            df_result=pd.merge_asof(df_result,df_md_cm_ot_dividendpolicy1,left_on='Date',right_on='日期_股利_公告日')

            #Flag_MonthSales
            mask = (df_result['日期_月營收_公布日'].diff().astype(str).str.strip().str[0] != '0' )
            df_result.loc[mask,['Flag_月營收']]= 1    
            #Flag_HolderStruc
            mask = (df_result['日期_集保庫存_公布日'].diff().astype(str).str.strip().str[0] != '0' )
            df_result.loc[mask,['Flag_集保庫存']]= 1   
            #Flag_insiderholdingstructure
            mask = (df_result['日期_董事持股_公布日'].diff().astype(str).str.strip().str[0] != '0' )
            df_result.loc[mask,['Flag_董事持股']]= 1   
            #Flag_insiderholdingstructure
            mask = (df_result['日期_財報_公告日'].diff().astype(str).str.strip().str[0] != '0' )
            df_result.loc[mask,['Flag_財報']]= 1   
            mask = (df_result['日期_股利_公告日'].diff().astype(str).str.strip().str[0] != '0' )
            df_result.loc[mask,['Flag_股利']]= 1  
            print('add OK!!')


            

            df_result['毛利率'] = df_result['營業毛利'] / df_result['營業收入淨額']*100
            df_result['淨利率'] = df_result['稅後純益'] / df_result['營業收入淨額']*100
            df_result['毛利率4'] = df_result['營業毛利4'] / df_result['營業收入淨額4']*100
            df_result['淨利率4'] = df_result['稅後純益4'] / df_result['營業收入淨額4']*100
            df_result['市值'] = df_result['Close'] * df_result['普通股股本']/10
            df_result['ROE4'] = df_result['稅後純益4'] / df_result['權益總計4']*100
            df_result['PB'] = df_result['Close'] / df_result['公告每股淨值']
            # df_result['PB'] = 0
            df_result['PE4'] = df_result['Close'] / df_result['原始每股稅後盈餘4']
            
            df_result['股票股利合計殖利率'] = df_result['股票股利合計'] / df_result['Close']*100
            df_result['現金股利合計殖利率'] = df_result['現金股利合計'] / df_result['Close']*100
            df_result['股利合計殖利率'] = df_result['股利合計'] / df_result['Close']*100
                    
            df_result['股票股利合計殖利率4'] = df_result['股票股利合計4'] / df_result['Close']*100
            df_result['現金股利合計殖利率4'] = df_result['現金股利合計4'] / df_result['Close']*100
            df_result['股利合計殖利率4'] = df_result['股利合計4'] / df_result['Close']*100
            
            df_result['股票股利合計殖利率_平均_3yr'] = df_result['股票股利合計_平均_3yr'] / df_result['Close']*100
            df_result['現金股利合計殖利率_平均_3yr'] = df_result['現金股利合計_平均_3yr'] / df_result['Close']*100
            df_result['股利合計殖利率_平均_3yr'] = df_result['股利合計_平均_3yr'] / df_result['Close']*100
            df_result['股票股利合計殖利率_平均_5yr'] = df_result['股票股利合計_平均_5yr'] / df_result['Close']*100
            df_result['現金股利合計殖利率_平均_5yr'] = df_result['現金股利合計_平均_5yr'] / df_result['Close']*100
            df_result['股利合計殖利率_平均_5yr'] = df_result['股利合計_平均_5yr'] / df_result['Close']*100
            df_result['股票股利合計殖利率_平均_10yr'] = df_result['股票股利合計_平均_10yr'] / df_result['Close']*100
            df_result['現金股利合計殖利率_平均_10yr'] = df_result['現金股利合計_平均_10yr'] / df_result['Close']*100
            df_result['股利合計殖利率_平均_10yr'] = df_result['股利合計_平均_10yr'] / df_result['Close']*100
            
            
            df_result['負債比率'] = df_result['負債總計'] / df_result['資產總計']*100
            df_result['非流動負債比率'] = df_result['非流動負債'] / df_result['資產總計']*100


        df_result=df_result.replace([np.inf, -np.inf], np.nan).copy()
        df_result.insert(0,'id',0)
        df_result['CreateTime']=datetime.now()
        df_result['Creator']='datetime'
        df_result['ModifiedTime']=datetime.now()
        df_result['Modified_User']='datetime'
        #篩選所需日期資料
        df_result=df_result[df_result.Date>=StartDate]
        #print(df_result[['Date','Flag_MonthSales','Flag_HolderStruc']])
        #輸出CSV
        #df_result.to_csv("C:\\Users\\chube\\Dropbox\\InvestQuant\\test.csv",index = False, header=True)
        #存到資料庫
        # StartDate_Data=StartDate - relativedelta(months=+1)
        db_conn_insert.execute('delete from  indistockdb.`' + Ticker + '` where date>="' + StartDate.strftime('%Y%m%d') + '" and date<="' + EndDate.strftime('%Y%m%d')+'"')
        
        print('股利與最後相關資料合併刪除 .',( time.time() - time_start1))
        print('財報與股利與合併資料.============',(time.time() - time_start-dairlyT))
          
        time_start1 = time.time()
        df_result.to_sql(con=db_conn_insert, 
                          name=Ticker, 
                          if_exists='append',
                          index=False)
        time_end1 = time.time()
        print('indistockdb_importer(%s):%s',(Ticker,str(round(time_end1 - time_start,3))))
    except Exception as err:
        print(err)
    finally:
        pass
def create_table(targetlist):
    db_conn_query=quantlib.get_db_conn(engine='mysql',user='dataimporter',db='marketstddb')
    _SQL1="(SELECT TABLE_NAME as Ticker FROM `INFORMATION_SCHEMA`.`TABLES` WHERE Table_Schema='indistockdb')"
    df=pd.read_sql_query(_SQL1, db_conn_query)
    _a = [str(x) for x in df['Ticker']]
    for Ticker in [x for x in targetlist if x not in _a]:
        try:
            db_cursor = db_conn_query.cursor()
            db_cursor.execute ('Create Table indistockdb.`' + Ticker + '` select * from indistockdb._template where 1=0;')
            db_cursor.execute ('Alter table indistockdb.`' + Ticker + '` add primary key (`id`);')
            db_cursor.execute ('Alter table indistockdb.`' + Ticker + '` MODIFY id INTEGER  AUTO_INCREMENT;')
            db_cursor.execute ('Alter table indistockdb.`' + Ticker + '`  add  UNIQUE index `UniqueIndex` (`Date`);')
            db_conn_query.commit()
            print('indistockdb.`' + Ticker + '` created.')
        except Exception as Err:
            print("def create_table Error(%s): %s" % (Ticker,Err))
        finally:
            db_cursor.close()

def get_latest_targetlist_indistock(querydate,targetlist):
    targetlist=sorted(targetlist)
    db_connection =quantlib.get_db_conn('mysql','dataimporter')
    db_cursor = db_connection.cursor()
    def check_is_exist_data(ticker):
        SQL= "select count(*) as num from indistockdb.`" + ticker + "`  WHERE Date ='" + querydate.strftime('%Y%m%d') + "'"
        db_cursor.execute(SQL)
        record = db_cursor.fetchone()
        print(record)
        return record[0]>0

    if not check_is_exist_data(targetlist[0]) and not check_is_exist_data('0050'):
        return targetlist
    if check_is_exist_data(targetlist[-1]) and check_is_exist_data('9962'):
        return []
    step = 90
    for i in range(0,len(targetlist),step):
        lower_ticker = targetlist[i]
        upper_ticker = targetlist[i+step] if i+step < len(targetlist) else targetlist[-1]
        print(check_is_exist_data(lower_ticker),check_is_exist_data(upper_ticker))
        if check_is_exist_data(lower_ticker) and not check_is_exist_data(upper_ticker):
            return targetlist[i:]

def get_ms_targetlist(latest_ms_date,measure_monitor_path):
    df = pd.read_csv(measure_monitor_path,index_col = 'Ticker')
    df['日期_月營收_資料日'] = pd.to_datetime(df['日期_月營收_資料日'])
    tickerlist = df[(df['日期_月營收_資料日'] != latest_ms_date)].index
    return [str(x) for x in tickerlist]
def main(QueryDate):
    time_start = time.time()
    # 每日自動---------------
    #QueryDate=(datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
    StartDate=QueryDate - relativedelta(months=+2)
    EndDate=QueryDate
    StartDateFi=str(QueryDate.year-11)+ '01'
    EndDateFi=str(QueryDate.year)+ '04'
    SeasonArray=setSeason(StartDateFi,EndDateFi)
    # 每日自動---------------
    
    # 手動---------------
    # StartDate=datetime(2004,12,31)
    # EndDate=datetime(2021,10,8)
    # StartDateFi=str(StartDate.year-11)+ '01'
    # EndDateFi=str(EndDate.year)+ '04'
    # SeasonArray=setSeason(StartDateFi,EndDateFi)
    # 手動---------------
    
    #取得更新名單
    targetlist=quantlib.get_dailyupdatelist(EndDate)
    targetlist=get_latest_targetlist_indistock(EndDate, targetlist)
    #更新月營收用--找出前一次貝兒讚尚未更新月營收之標的
    # latest_ms_date = datetime(2022,3,1)
    # folder_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'measure_monitor')
    # measure_monitor_path =  os.path.join(folder_path,'measure_20220411.csv')
    # targetlist = get_ms_targetlist(latest_ms_date,measure_monitor_path)
    #檢查是否要新增table
    create_table(targetlist)

    is_thread=True

    if is_thread:
        Num_thread=20
        Threadlist=[]
        #targetlist= targetlist[1200:2000]
        targetlist_copy=deepcopy(targetlist)
        Num_group=int(len(targetlist)/Num_thread + (0 if len(targetlist) % Num_thread ==0 else 1))
        k=0
        #create conn list
        db_conn_insert_list = [quantlib.get_db_conn(engine='sqlalchemy',user='dataimporter',db='indistockdb') for l in range(Num_thread)]
        db_conn_query_list= [quantlib.get_db_conn(engine='mysql',user='dataimporter',db='marketstddb') for l in range(Num_thread)]
        while len(targetlist_copy) >0:
            time_start1=time.time() 
            k=k+1
            Threadlist=[]
            l=0
            for Ticker in targetlist_copy[:Num_thread]:
                aThread=threading.Thread(target=indistock_importer,name=Ticker,args = (Ticker,StartDate,EndDate,StartDateFi,EndDateFi,SeasonArray,db_conn_insert_list[l],db_conn_query_list[l]))
                l+=1
                aThread.start()
                Threadlist.append(aThread)
                
            [x.join() for x in Threadlist]
            print('[%s~%s](%s/%s):%s' % (str(targetlist_copy[0]),str(targetlist_copy[min(Num_thread,len(targetlist_copy))-1]),str(k),str(Num_group), str(time.time() - time_start1)))
            del targetlist_copy[:Num_thread]
    else:
        db_conn_insert = quantlib.get_db_conn(engine='sqlalchemy',user='dataimporter',db='indistockdb')     
        db_conn_query = quantlib.get_db_conn(engine='mysql',user='dataimporter',db='marketstddb') 

        #for Ticker in ['1101','1102','1103']: #平均一次約2.6秒
        for Ticker in  targetlist:
            indistock_importer(Ticker,StartDate,EndDate,StartDateFi,EndDateFi,SeasonArray,db_conn_insert,db_conn_query) 

    print('Total time cost:' ,(time.time() - time_start))

def main_20220427(QueryDate):
    time_start = time.time()
    # 每日自動---------------
    #QueryDate=(datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
    StartDate=QueryDate - relativedelta(months=+2)
    EndDate=QueryDate
    indistockdb_name='indistockdb'
    # 每日自動---------------
    
    # 手動---------------
    # StartDate=datetime(2004,12,31)
    # EndDate=datetime(2021,10,8)
    # StartDateFi=str(StartDate.year-11)+ '01'
    # EndDateFi=str(EndDate.year)+ '04'
    # SeasonArray=setSeason(StartDateFi,EndDateFi)
    # 手動---------------
    
    #取得更新名單
    # targetlist=quantlib.get_dailyupdatelist(EndDate)
    # targetlist=get_latest_targetlist_indistock(EndDate, targetlist)
    targetlist=['1101']
    #更新月營收用--找出前一次貝兒讚尚未更新月營收之標的
    # latest_ms_date = datetime(2022,3,1)
    # folder_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),'measure_monitor')
    # measure_monitor_path =  os.path.join(folder_path,'measure_20220411.csv')
    # targetlist = get_ms_targetlist(latest_ms_date,measure_monitor_path)
    #檢查是否要新增table
    create_table(targetlist)

    is_thread=False

    if is_thread:
        Num_thread=20
        Threadlist=[]
        #targetlist= targetlist[1200:2000]
        targetlist_copy=deepcopy(targetlist)
        Num_group=int(len(targetlist)/Num_thread + (0 if len(targetlist) % Num_thread ==0 else 1))
        k=0
        #create conn list
        db_conn_insert_list = [quantlib.get_db_conn(engine='sqlalchemy',user='dataimporter',db='indistockdb') for l in range(Num_thread)]
        db_conn_query_list= [quantlib.get_db_conn(engine='mysql',user='dataimporter',db='marketstddb') for l in range(Num_thread)]
        while len(targetlist_copy) >0:
            time_start1=time.time() 
            k=k+1
            Threadlist=[]
            l=0
            for Ticker in targetlist_copy[:Num_thread]:
                aThread=threading.Thread(target=indistock_importer,name=Ticker,args = (Ticker,StartDate,EndDate,StartDateFi,EndDateFi,SeasonArray,db_conn_insert_list[l],db_conn_query_list[l]))
                l+=1
                aThread.start()
                Threadlist.append(aThread)
                
            [x.join() for x in Threadlist]
            print('[%s~%s](%s/%s):%s' % (str(targetlist_copy[0]),str(targetlist_copy[min(Num_thread,len(targetlist_copy))-1]),str(k),str(Num_group), str(time.time() - time_start1)))
            del targetlist_copy[:Num_thread]
    else:
        db_conn_insert = quantlib.get_db_conn(engine='sqlalchemy',user='dataimporter',db=indistockdb_name)     
        db_conn_query = quantlib.get_db_conn(engine='mysql',user='dataimporter',db='marketstddb') 

        #for Ticker in ['1101','1102','1103']: #平均一次約2.6秒
        for Ticker in  targetlist:
            db_conn_insert = quantlib.get_db_conn(engine='sqlalchemy',user='dataimporter',db=indistockdb_name)     
            db_conn_query = quantlib.get_db_conn(engine='mysql',user='dataimporter',db='marketstddb')
            obj=INDISTOCK_IMPORTER(Ticker,StartDate,EndDate,indistockdb_name,db_conn_insert,db_conn_query)
            obj._importdb()


    print('Total time cost:' ,(time.time() - time_start))        
    # cn.execute('replace INTO indistockdb.DataUpdateRecord values(0,"'+StartDate.strftime('%Y%m%d')+'","'+EndDate.strftime('%Y%m%d')+'","'+STime+'" ,"'+ str(datetime.now())+'");')
if __name__ == "__main__":
    QueryDate=datetime(2022,4,11)
    main(QueryDate)
    # db_conn_insert = quantlib.get_db_conn(engine='sqlalchemy',user='dataimporter',db='indistockdb_20220427')     
    # db_conn_query = quantlib.get_db_conn(engine='mysql',user='dataimporter',db='marketstddb')
    # obj=INDISTOCK_IMPORTER('1101',datetime(2022,1,11),datetime(2022,4,11),'indistockdb_20220427',db_conn_insert,db_conn_query)
    # obj._importdb()