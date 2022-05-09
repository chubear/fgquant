import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import quantlib
from datetime import date,datetime,timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import time
import logging
db_conn_insert =quantlib.get_db_conn(engine='sqlalchemy',user='dataimporter',db='marketstddb')
db_conn_query = quantlib.get_db_conn(engine='mysql',user='dataimporter',db='marketrawdb_cm') 
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
class MD_FI_QUARTERLY():
    def __init__(self, Ticker: str, StartDate_Data: datetime, StartDate: datetime, EndDate: datetime , db_conn_insert, db_conn_query) -> None:
        self.Ticker = Ticker
        self.StartDate = StartDate
        self.EndDate = EndDate
        self.StartDate_Data = StartDate_Data

        StartDateFi=str(QueryDate.year-11)+ '01'
        EndDateFi=str(QueryDate.year)+ '04'
        SeasonArray=setSeason(StartDateFi,EndDateFi)

        self.target_table = '`marketstddb`.`md_fi_quarterly'
        self.db_conn_insert = db_conn_insert
        self.db_conn_query = db_conn_query
    
    def _df_fi_quarterly(self):
        def default_fi_announce_date(df):
            for i in range(len(df)):
                if df.iloc[i]['日期_財報_公告日']==datetime(1900,1,1):
                    year_quarter=df.iloc[i]['年季']
                    year = int(year_quarter[:4])
                    qrt = int(year_quarter[-2:])
                    if year >=2013: #2013/1/2修正條文
                        if qrt==1: date1 = datetime(year,5,15)
                        elif qrt==2: date1 = datetime(year,8,14)
                        elif qrt==3: date1 = datetime(year,11,14)
                        else: date1 = datetime(year+1,3,31)
                    elif year>=2012:#2012/3/12修正條文 
                        if qrt==1: date1 = datetime(year,4,30)
                        elif qrt==2: date1 = datetime(year,8,31)
                        elif qrt==3: date1 = datetime(year,10,31)
                        else: date1 = datetime(year+1,4,30)               
                    else: 
                        if qrt==1: date1 = datetime(year,4,30)
                        elif qrt==2: date1 = datetime(year,8,31)
                        elif qrt==3: date1 = datetime(year,10,31)
                        else: date1 = datetime(year+1,4,30)                
                    df.at[i,'日期_財報_公告日'] = date1
            return df
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
        # CM財報BS==================================================
        _SQL='Select 公告日期 as 日期_財報_公告日,年季,case(right(`年季`,2)) when "01" then cast(concat(left(`年季`,4),"0331") as date) when "02" then cast(concat(left(年季,4),"0630") as date) when "03" then cast(concat(left(年季,4),"0930") as date) when "04" then cast(concat(left(年季,4),"1231") as date) end  as 日期_財報_資料日,0 as Flag_財報,流動資產,現金及約當現金,短期投資合計,應收帳款與票據合計,存貨,非流動資產,採用權益法之投資,長期投資合計,不動產、廠房及設備,無形資產,資產總計,流動負債,非流動負債,負債總計,股本,普通股股本,特別股股本,資本公積,保留盈餘,其他權益,庫藏股票,共同控制下前手權益,合併前非屬共同控制股權,母公司業主權益,非控制權益,權益總計,原始每股淨值,公告每股淨值,負債及權益總計,預收股款之約當發行股數,母公司暨子公司所持有之母公司庫藏股,(ifnull(普通股股本,0)+ifnull(特別股股本,0)+0.01* ifnull(預收股款之約當發行股數,0) - 0.01*ifnull(母公司暨子公司所持有之母公司庫藏股,0)) as 流通在外股本 from `marketrawdb_cm`.`md_cm_fi_bs_quarterly` where `股票代號` = "' + self.Ticker + '" and 年季 between "'+ self.StartDateFi +'" and "'+ self.EndDateFi +'"  order by 年季 asc'
        self.df_md_cm_fi_bs_quarterly = pd.read_sql_query(_SQL, self.db_conn_query)
        #===修正資料程序===
        df['日期_財報_公告日'] = df['日期_財報_公告日'].fillna(datetime(1900,1,1)) 
        df['日期_財報_公告日'] = pd.to_datetime(df['日期_財報_公告日'])
        df['年季'] = df['年季'].astype(int).astype(str)
         #補齊公告日
        self.df_md_cm_fi_bs_quarterly = default_fi_announce_date(self.df_md_cm_fi_bs_quarterly)
        self.df_md_cm_fi_bs_quarterly = self.df_md_cm_fi_bs_quarterly.fillna(method='ffill')
       
        self.df_md_cm_fi_bs_quarterly = cutfin(self.df_md_cm_fi_bs_quarterly,'BS')
        
        self.df_md_cm_fi_bs_quarterly4 = self.df_md_cm_fi_bs_quarterly[['流動資產','現金及約當現金','短期投資合計','應收帳款與票據合計','存貨','非流動資產','採用權益法之投資','長期投資合計','不動產、廠房及設備','無形資產','資產總計','流動負債','非流動負債','負債總計','股本','普通股股本','特別股股本','資本公積','保留盈餘','其他權益','庫藏股票','共同控制下前手權益','合併前非屬共同控制股權','母公司業主權益','非控制權益','權益總計','原始每股淨值','公告每股淨值','負債及權益總計','預收股款之約當發行股數','母公司暨子公司所持有之母公司庫藏股']].rolling(4).mean()
        self.df_md_cm_fi_bs_quarterly4.columns = ['流動資產4','現金及約當現金4','短期投資合計4','應收帳款與票據合計4','存貨4','非流動資產4','採用權益法之投資4','長期投資合計4','不動產、廠房及設備4','無形資產4','資產總計4','流動負債4','非流動負債4','負債總計4','股本4','普通股股本4','特別股股本4','資本公積4','保留盈餘4','其他權益4','庫藏股票4','共同控制下前手權益4','合併前非屬共同控制股權4','母公司業主權益4','非控制權益4','權益總計4','原始每股淨值4','公告每股淨值4','負債及權益總計4','預收股款之約當發行股數4','母公司暨子公司所持有之母公司庫藏股4']
        # df_md_cm_fi_bs_quarterly4=df_md_cm_fi_bs_quarterly4.fillna(0)
        self.df_md_cm_fi_bs_quarterly4['流通在外股本4']= self.df_md_cm_fi_bs_quarterly4['普通股股本4']+self.df_md_cm_fi_bs_quarterly4['特別股股本4']+0.01*self.df_md_cm_fi_bs_quarterly4['預收股款之約當發行股數4']-0.01*+self.df_md_cm_fi_bs_quarterly4['母公司暨子公司所持有之母公司庫藏股4']
            
        if self.df_md_cm_fi_bs_quarterly.shape[0]==self.df_md_cm_fi_bs_quarterly4.shape[0] :
            df_md_cm_fi_quarterly=pd.concat([self.df_md_cm_fi_bs_quarterly,self.df_md_cm_fi_bs_quarterly4],axis=1)
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
    def get_df_source(self):
        #BS
        _SQL='Select 公告日期 as 日期_財報_公告日,年季,case(right(`年季`,2)) when "01" then cast(concat(left(`年季`,4),"0331") as date) when "02" then cast(concat(left(年季,4),"0630") as date) when "03" then cast(concat(left(年季,4),"0930") as date) when "04" then cast(concat(left(年季,4),"1231") as date) end  as 日期_財報_資料日,0 as Flag_財報,流動資產,現金及約當現金,短期投資合計,應收帳款與票據合計,存貨,非流動資產,採用權益法之投資,長期投資合計,不動產、廠房及設備,無形資產,資產總計,流動負債,非流動負債,負債總計,股本,普通股股本,特別股股本,資本公積,保留盈餘,其他權益,庫藏股票,共同控制下前手權益,合併前非屬共同控制股權,母公司業主權益,非控制權益,權益總計,原始每股淨值,公告每股淨值,負債及權益總計,預收股款之約當發行股數,母公司暨子公司所持有之母公司庫藏股,(ifnull(普通股股本,0)+ifnull(特別股股本,0)+0.01* ifnull(預收股款之約當發行股數,0) - 0.01*ifnull(母公司暨子公司所持有之母公司庫藏股,0)) as 流通在外股本 from `marketrawdb_cm`.`md_cm_fi_bs_quarterly` where `股票代號` = "' + self.Ticker + '" and 年季 between "'+ self.StartDateFi +'" and "'+ self.EndDateFi +'"  order by 年季 asc'
        df_md_cm_fi_bs_quarterly = pd.read_sql_query(_SQL, self.db_conn_query)
        df_md_cm_fi_bs_quarterly=df_md_cm_fi_bs_quarterly.fillna(0)
        df_md_cm_fi_bs_quarterly=cutfin(df_md_cm_fi_bs_quarterly,'BS')
        #IS
        _SQL='Select 年季,銷貨收入淨額,營業收入淨額,營業成本,營業毛利,營業毛利淨額,營業費用,推銷費用,管理費用,研發費用,預期信用減損損益_營業費用,其他收益及費損,營業利益,營業外收入及支出,其他收入,利息收入,租金收入,權利金收入,股利收入,財務成本,利息費用,預期信用減損損益,採用權益法之關聯企業及合資損益之份額,除列按攤銷後成本衡量金融資產淨損益,金融資產重分類淨損益,營業外收入,營業外支出,稅前純益,所得稅,繼續營業單位損益,停業部門損益,合併前非屬共同控制股權損益,稅後純益,其他綜合損益,綜合損益,母公司業主–稅後純益 as 母公司業主_稅後純益,非控制權益–稅後純益 as 非控制權益_稅後純益,共同控制下前手權益–稅後純益 as 共同控制下前手權益_稅後純益,母公司業主–綜合損益 as 母公司業主_綜合損益,非控制權益–綜合損益 as 非控制權益_綜合損益,共同控制下前手權益–綜合損益 as 共同控制下前手權益_綜合損益,EBITDA,公告基本每股盈餘,公告稀釋每股盈餘,原始每股稅前盈餘,原始每股稅後盈餘,原始每股綜合盈餘 from `marketrawdb_cm`.`md_cm_fi_is_quarterly` where `股票代號` = "' + self.Ticker + '" and 年季 between "'+ self.StartDateFi +'" and "'+ self.EndDateFi +'" order by 年季 asc'
        df_md_cm_fi_is_quarterly = pd.read_sql_query(_SQL, self.db_conn_query)
        df_md_cm_fi_is_quarterly=df_md_cm_fi_is_quarterly.fillna(0)
        df_md_cm_fi_is_quarterly=cutfin(df_md_cm_fi_is_quarterly,'IS')
        #CS

        return pd.read_sql_query(_SQL, db_conn_query,index_col='Date')
    def calc_costum_col(self):
        #取得資料庫最新累計漲幅
        _SQL = 'Select `累計漲幅`,`Open_adj1`,`High_adj1`,`Low_adj1`,`Close_adj1` from `marketstddb`.`md_ta_dailyquotes`  where  `日期` <'+ self.StartDate_Data.strftime('%Y%m%d') +'"  and  `股票代號` = "' + self.Ticker + '" order by `Date` Desc'
        df = pd.read_sql_query(_SQL, db_conn_query,index_col='Date')
        if df.empty:
            Latest_chg_acc = 1
            Latest_Close_adj1 = self.df_result.iloc[0,['Close']]
        else:
            Latest_chg_acc = df['累計漲幅']
            Latest_Close_adj1 = df['Close_adj1']
        self.df_result['累計漲幅'] = (1+df['累計漲幅']/100).cumprod() * Latest_chg_acc
        self.df_result['Close_adj1'] = (1+df['累計漲幅']/100).cumprod() * Latest_Close_adj1
        self.df_result['Open_adj1'] = self.df_result['Open']/self.df_result['Close'] * self.df_result['Close_adj1'] 
        self.df_result['High_adj1'] = self.df_result['High']/self.df_result['Close'] * self.df_result['Close_adj1'] 
        self.df_result['Low_adj1'] = self.df_result['Low']/self.df_result['Close'] * self.df_result['Close_adj1'] 
    def insert_new_col_data(self):
        pass
    def create_table(self):
        try:
            db_cursor = self.db_conn_query.cursor()
            db_cursor.execute ('Create Table marketstddb.`%s` select * from marketstddb._template where 1=0;' % (self.Ticker))
            db_cursor.execute ('Alter table %s.`%s` add primary key (`id`);' % (self.indistockdb_name,self.Ticker))
            db_cursor.execute ('Alter table %s.`%s` MODIFY id INTEGER  AUTO_INCREMENT;' % (self.indistockdb_name,self.Ticker))
            db_cursor.execute ('Alter table %s.`%s`  add  UNIQUE index `UniqueIndex` (`Date`);'% (self.indistockdb_name,self.Ticker))
            self.db_conn_query.commit()
            print('%s.`%s` created.' % (self.indistockdb_name,self.Ticker))
        except Exception as Err:
            print("create_table Error(%s): %s" % (self.Ticker,Err))
        finally:
            db_cursor.close()
    def insert_db(self):
        
        df_result= self.df_result.replace([np.inf, -np.inf], np.nan)
        df_result.insert(0,'id',0)
        df_result.insert(1,'Ticker',self.Ticker)
        df_result['CreateTime']=datetime.now()
        df_result['Creator']='datetime'
        df_result['ModifiedTime']=datetime.now()
        df_result['Modified_User']='datetime'
        #篩選所需日期資料
        df_result=df_result[df_result['Date']>=self.StartDate]
        #產生table
        self. create_table()
        
        self.db_conn_insert.execute('delete from ' + self.target_table + ' where Ticker = "' + self.Ticker + '" `Date`>="' + self.StartDate.strftime('%Y%m%d') + '" and `Date`<="' + self.EndDate.strftime('%Y%m%d')+'"')
        df_result.to_sql(con=self.db_conn_insert, 
                          name= self.target_table, 
                          if_exists='append',
                          index=False)

class MD_DAILYQUOTES():
    def __init__(self, Ticker: str, StartDate_Data: datetime, StartDate: datetime, EndDate: datetime , db_conn_insert, db_conn_query) -> None:
        self.Ticker = Ticker
        self.StartDate = StartDate
        self.EndDate = EndDate
        self.StartDate_Data = StartDate_Data

        self.target_table = '`marketstddb`.`' + self.Ticker +'`'
        self.db_conn_insert = db_conn_insert
        self.db_conn_query = db_conn_query
    def get_df_source(self):
        _SQL = 'Select 日期 as `Date`, 股票代號 as `Ticker`, 股票名稱 as `CorpName`, 開盤價 as `Open`, 最高價 as `High`, 最低價 as `Low`, 收盤價 as `Close`, 漲跌, 漲幅, 振幅, 成交量 as `Volume`, 成交筆數, 成交金額 as `Amount`, 均張, 成交量變動, 均張變動, 股本, 總市值, 市值比重, 本益比, 股價淨值比, 本益比4, 週轉率, 成交值比重, 漲跌停, 均價, 成交量_股    from `marketrawdb_cm`.`md_cm_ta_dailyquotes`  where  `日期` between "'+ self.StartDate_Data.strftime('%Y%m%d') +'" and "'+ self.EndDate.strftime('%Y%m%d') +'"  and  `股票代號` = "' + self.Ticker + '"'
        return pd.read_sql_query(_SQL, db_conn_query,index_col='Date')
    def calc_costum_col(self):
        #取得資料庫最新累計漲幅
        _SQL = 'Select `累計漲幅`,`Open_adj1`,`High_adj1`,`Low_adj1`,`Close_adj1` from `marketstddb`.`md_ta_dailyquotes`  where  `日期` <'+ self.StartDate_Data.strftime('%Y%m%d') +'"  and  `股票代號` = "' + self.Ticker + '" order by `Date` Desc'
        df = pd.read_sql_query(_SQL, db_conn_query,index_col='Date')
        if df.empty:
            Latest_chg_acc = 1
            Latest_Close_adj1 = self.df_result.iloc[0,['Close']]
        else:
            Latest_chg_acc = df['累計漲幅']
            Latest_Close_adj1 = df['Close_adj1']
        self.df_result['累計漲幅'] = (1+df['累計漲幅']/100).cumprod() * Latest_chg_acc
        self.df_result['Close_adj1'] = (1+df['累計漲幅']/100).cumprod() * Latest_Close_adj1
        self.df_result['Open_adj1'] = self.df_result['Open']/self.df_result['Close'] * self.df_result['Close_adj1'] 
        self.df_result['High_adj1'] = self.df_result['High']/self.df_result['Close'] * self.df_result['Close_adj1'] 
        self.df_result['Low_adj1'] = self.df_result['Low']/self.df_result['Close'] * self.df_result['Close_adj1'] 
    def insert_new_col_data(self):
        pass
    def create_table(self):
        try:
            db_cursor = self.db_conn_query.cursor()
            db_cursor.execute ('Create Table marketstddb.`%s` select * from marketstddb._template where 1=0;' % (self.Ticker))
            db_cursor.execute ('Alter table %s.`%s` add primary key (`id`);' % (self.indistockdb_name,self.Ticker))
            db_cursor.execute ('Alter table %s.`%s` MODIFY id INTEGER  AUTO_INCREMENT;' % (self.indistockdb_name,self.Ticker))
            db_cursor.execute ('Alter table %s.`%s`  add  UNIQUE index `UniqueIndex` (`Date`);'% (self.indistockdb_name,self.Ticker))
            self.db_conn_query.commit()
            print('%s.`%s` created.' % (self.indistockdb_name,self.Ticker))
        except Exception as Err:
            print("create_table Error(%s): %s" % (self.Ticker,Err))
        finally:
            db_cursor.close()
    def insert_db(self):
        
        df_result= self.df_result.replace([np.inf, -np.inf], np.nan)
        df_result.insert(0,'id',0)
        df_result.insert(1,'Ticker',self.Ticker)
        df_result['CreateTime']=datetime.now()
        df_result['Creator']='datetime'
        df_result['ModifiedTime']=datetime.now()
        df_result['Modified_User']='datetime'
        #篩選所需日期資料
        df_result=df_result[df_result['Date']>=self.StartDate]
        #產生table
        self. create_table()
        
        self.db_conn_insert.execute('delete from ' + self.target_table + ' where Ticker = "' + self.Ticker + '" `Date`>="' + self.StartDate.strftime('%Y%m%d') + '" and `Date`<="' + self.EndDate.strftime('%Y%m%d')+'"')
        df_result.to_sql(con=self.db_conn_insert, 
                          name= self.target_table, 
                          if_exists='append',
                          index=False)
class MARKETSTDDB_IMPORTER():
    def __init__(self, Ticker: str, StartDate: datetime, EndDate: datetime,db_conn_insert,db_conn_query) -> None:
        self.Ticker = Ticker
        self.StartDate = StartDate
        self.EndDate = EndDate
    
        self.StartDateFi=str(EndDate.year-11)+ '01'
        self.EndDateFi=str(EndDate.year)+ '04'
        self.SeasonArray=setSeason(self.StartDateFi,self.EndDateFi)
        #為了可以補上週月年資料
        self.StartDate_Data = StartDate - relativedelta(months=+1)
        self.db_conn_query = db_conn_query
        self.db_conn_insert = db_conn_insert
    def import_md_fi_quarterly(self):
        df_result = self._df_fi_quarterly()
        df_result=df_result.replace([np.inf, -np.inf], np.nan)
        df_result.insert(0,'id',0)
        df_result.insert(1,'Ticker',self.Ticker)
        df_result['CreateTime']=datetime.now()
        df_result['Creator']='datetime'
        df_result['ModifiedTime']=datetime.now()
        df_result['Modified_User']='datetime'
        #篩選所需日期資料
        df_result=df_result[self.df_result['日期_財報_資料日']>=self.StartDate]
        self.db_conn_insert.execute('delete from  marketstddb.`md_fi_quarterly` where Ticker = "' + self.Ticker + '" `日期_財報_資料日`>="' + self.StartDate.strftime('%Y%m%d') + '" and `日期_財報_資料日`<="' + self.EndDate.strftime('%Y%m%d')+'"')
        df_result.to_sql(con=self.db_conn_insert, 
                          name=self.Ticker, 
                          if_exists='append',
                          index=False)
    def import_md_dividendpolicy(self):
        df_result = self._df_dividendpolicy()
        df_result=df_result.replace([np.inf, -np.inf], np.nan)
        df_result.insert(0,'id',0)
        df_result.insert(1,'Ticker',self.Ticker)        
        df_result['CreateTime']=datetime.now()
        df_result['Creator']='datetime'
        df_result['ModifiedTime']=datetime.now()
        df_result['Modified_User']='datetime'
        #篩選所需日期資料
        df_result=df_result[self.df_result['日期_股利_公告日']>=self.StartDate]
        self.db_conn_insert.execute('delete from  marketstddb.`md_dividendpolicy` where Ticker = "' + self.Ticker + '" `日期_股利_公告日`>="' + self.StartDate.strftime('%Y%m%d') + '" and `日期_股利_公告日`<="' + self.EndDate.strftime('%Y%m%d')+'"')
        df_result.to_sql(con=self.db_conn_insert, 
                          name=self.Ticker, 
                          if_exists='append',
                          index=False,
                          method='multi')
    def import_md_fi_quarterly(self):
        df_result = self._df_fi_quarterly()
        df_result=df_result.replace([np.inf, -np.inf], np.nan)
        df_result.insert(0,'id',0)
        df_result['CreateTime']=datetime.now()
        df_result['Creator']='datetime'
        df_result['ModifiedTime']=datetime.now()
        df_result['Modified_User']='datetime'
        #篩選所需日期資料
        df_result=df_result[self.df_result['日期_財報_資料日']>=self.StartDate]
        self.db_conn_insert.execute('delete from  marketstddb.`md_fi_quarterly` where `日期_財報_資料日`>="' + self.StartDate.strftime('%Y%m%d') + '" and `日期_財報_資料日`<="' + self.EndDate.strftime('%Y%m%d')+'"')
        df_result.to_sql(con=self.db_conn_insert, 
                          name=self.Ticker, 
                          if_exists='append',
                          index=False,
                          method='multi')
    def _df_fi_quarterly(self):
        # CM財報BS==================================================
        _SQL='Select 公告日期 as 日期_財報_公告日,年季,case(right(`年季`,2)) when "01" then cast(concat(left(`年季`,4),"0331") as date) when "02" then cast(concat(left(年季,4),"0630") as date) when "03" then cast(concat(left(年季,4),"0930") as date) when "04" then cast(concat(left(年季,4),"1231") as date) end  as 日期_財報_資料日,0 as Flag_財報,流動資產,現金及約當現金,短期投資合計,應收帳款與票據合計,存貨,非流動資產,採用權益法之投資,長期投資合計,不動產、廠房及設備,無形資產,資產總計,流動負債,非流動負債,負債總計,股本,普通股股本,特別股股本,資本公積,保留盈餘,其他權益,庫藏股票,共同控制下前手權益,合併前非屬共同控制股權,母公司業主權益,非控制權益,權益總計,原始每股淨值,公告每股淨值,負債及權益總計,預收股款之約當發行股數,母公司暨子公司所持有之母公司庫藏股,(ifnull(普通股股本,0)+ifnull(特別股股本,0)+0.01* ifnull(預收股款之約當發行股數,0) - 0.01*ifnull(母公司暨子公司所持有之母公司庫藏股,0)) as 流通在外股本 from `marketrawdb_cm`.`md_cm_fi_bs_quarterly` where `股票代號` = "' + self.Ticker + '" and 年季 between "'+ self.StartDateFi +'" and "'+ self.EndDateFi +'"  order by 年季 asc'
        df_md_cm_fi_bs_quarterly = pd.read_sql_query(_SQL, self.db_conn_query,index_col='日期_財報_資料日')
        df_md_cm_fi_bs_quarterly=df_md_cm_fi_bs_quarterly.fillna(0)
        df_md_cm_fi_bs_quarterly=cutfin(df_md_cm_fi_bs_quarterly,'BS')
        df_md_cm_fi_bs_quarterly=df_md_cm_fi_bs_quarterly.loc[df_md_cm_fi_bs_quarterly['日期_財報_公告日']!=0].copy()
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
        _SQL='Select  年季 ,營業活動現金流量,不影響現金流量之收益費損項目,折舊費用,攤銷費用,與營業活動相關資產之淨變動,與營業活動相關負債之淨變動,營運產生之現金流動,營業活動之收現類別,營業活動之付現類別,投資活動現金流量,籌資活動現金流量,匯率變動對現金及約當現金之影響,本期現金及約當現金增減數,自由現金流量 from `marketrawdb_cm`.`md_cm_fi_cf_quarterly` where `股票代號` = "' + Ticker + '" and 年季 between "'+ StartDateFi +'" and "'+ EndDateFi +'" order by 年季 asc'
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
        # df_md_cm_fi_quarterly=df_md_cm_fi_quarterly.drop(columns=['年季'])
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
def marketstddb_importer(Ticker: str, StartDate: datetime, EndDate: datetime,tbl_suffix = ''):
    try:
        time_start1 = time.time()
        StartDate_Data = StartDate 
        errRec = pd.DataFrame(columns=['Ticker','rowNum','errMsg'])
        exec_db_dict ={
        'md_cm_ta_dailyquotes':'Select 日期 as `Date`, 股票代號 as `Ticker`, 股票名稱 as `CorpName`, 開盤價 as `Open`, 最高價 as `High`, 最低價 as `Low`, 收盤價 as `Close`, 漲跌, 漲幅, 振幅, 成交量 as `Volume`, 成交筆數, 成交金額 as `Amount`, 均張, 成交量變動, 均張變動, 股本, 總市值, 市值比重, 本益比, 股價淨值比, 本益比4, 週轉率, 成交值比重, 漲跌停, 均價, 成交量_股    from `marketrawdb_cm`.`md_cm_ta_dailyquotes' + tbl_suffix +'`  where  `日期` between "'+ StartDate_Data.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'"  and  `股票代號` = "' + Ticker + '"'
        ,'md_cm_ta_dailyquotes_adj':'Select 日期 as `Date`,  開盤價 as `Open_Adj`, 最高價 as `High_Adj`, 最低價 as `Low_Adj`, 收盤價 as `Close_Adj`   from `marketrawdb_cm`.`md_cm_ta_dailyquotes_adj' + tbl_suffix + '`  where  `日期` between "'+ StartDate_Data.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'" and `股票代號` = "' + Ticker + '"'
        ,'md_cm_ta_dailystatistics':'Select `日期` as `Date`, K9, D9, RSI5, RSI10, DIF, MACD, DIF-MACD, WR5, WR10, 正DI14, 負DI14, ADX14, 週K9, 週D9, 週RSI5, 週RSI10, 週DIF, 週MACD, 週DIF減週MACD, 週正DI14, 週負DI14, 週ADX14, 月K9, 月D9, 月RSI5, 月RSI10, 月DIF, 月MACD, 月DIF減月MACD, 月正DI14, 月負DI14, 月ADX14, 季K9, 季D9, 季RSI5, 季RSI10, 季DIF, 季MACD, 季DIF-季MACD, 季正DI14, 季負DI14, 季ADX14, Alpha250D, Beta係數21D, Beta係數65D, Beta係數250D, 年化波動度21D, 年化波動度250D, 乖離率20日, 乖離率60日, 乖離率250日, 相對強弱比日, 相對強弱比週, 近一月歷史波動率, 近二月歷史波動率, 近三月歷史波動率, 近六月歷史波動率, 近九月歷史波動率, 近一年歷史波動率, EWMA波動率   from `marketrawdb_cm`.`md_cm_ta_dailystatistics' + tbl_suffix +'`  where  `日期` between "'+ StartDate_Data.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'"  and  `股票代號` = "' + Ticker + '"'
        ,'md_cm_fd_foreigninsttraing':'Select 日期  as `Date`, 外資買張, 外資賣張, 外資買賣超, 外資持股異動, 外資持股張數, 外資及陸資買張, 外資及陸資賣張, 外資及陸資買賣超, 外資自營商買張, 外資自營商賣張, 外資自營商買賣超, 外資買金額, 外資賣金額, 外資買賣超金額, 外資買均價, 外資賣均價, 外資持股比率, 外資持股市值, 外資持股成本, 外資尚可投資張數, 外資尚可投資比率, 外資投資上限比率, 陸資投資上限比率, 與前日異動原因   from `marketrawdb_cm`.`md_cm_fd_foreigninsttraing' + tbl_suffix +'`  where  `日期` between "'+ StartDate_Data.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'" and  `股票代號` = "' + Ticker + '"'
        ,'md_cm_fd_brokertrading':'Select `日期` as `Date`,自營商買張, 自營商賣張, 自營商買賣超, 自營商買張_自行買賣, 自營商賣張_自行買賣, 自營商買賣超_自行買賣, 自營商買張_避險, 自營商賣張_避險, 自營商買賣超_避險, 自營商庫存, 自營商買金額, 自營商賣金額, 自營商買賣超金額, 自營商買均價, 自營商賣均價, 自營商持股比率, 自營商持股市值, 自營商持股成本   from `marketrawdb_cm`.`md_cm_fd_brokertrading' + tbl_suffix +'`  where  `日期` between "'+ StartDate_Data.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'" and `股票代號` = "' + Ticker + '"'
        ,'md_cm_fd_investmenttrusttrading':'Select `日期` as `Date`, 投信買張, 投信賣張, 投信買賣超, 投信庫存, 投信買金額, 投信賣金額, 投信買賣超金額, 投信買均價, 投信賣均價, 投信持股比率, 投信持股市值, 投信持股成本  from `marketrawdb_cm`.`md_cm_fd_investmenttrusttrading' + tbl_suffix +'`  where  `日期` between "'+ StartDate_Data.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'" and `股票代號` = "' + Ticker + '"'

        }

        df_result=pd.DataFrame()
        exectimestr=''
        for tbl in list(exec_db_dict.keys()):
            starttime = time.time()
            _SQL = exec_db_dict[tbl]
            #df= df_from_sql(_SQL)
            df = pd.read_sql_query(_SQL, db_conn_query)
            df['Date']=pd.to_datetime(df['Date'])
            df.set_index('Date',inplace=True)
            df.sort_index(ascending=True,inplace=True)
            df_result = df if df_result.index.empty else df_result.join(df, how='left').fillna(method = 'ffill') 
           
            logging.debug('query time:' + tbl + "(" + '{:.2f}'.format(time.time() - starttime) + ");")
        
        df_result.insert(0,'id',0)
        df_result['DataSource']='cmoney'
        df_result['IsCheck']= 0
        df_result['CheckDataSource']=None
        df_result['CheckTime']=None
        df_result['CreateTime']=datetime.now()
        df_result['Creator']='datateam'
        df_result['ModifiedTime']=datetime.now()
        df_result['Modified_User']='datateam'
        
        #刪除資料庫重複資料
        db_conn_insert.execute('delete from  `' + Ticker + '` where date between "' + df_result.index[0].to_pydatetime().strftime('%Y%m%d')  + '" and "' + df_result.index[-1].to_pydatetime().strftime('%Y%m%d')  +'"')
        
        time_start1 = time.time()
        df_result.to_sql(con=db_conn_insert, 
                          name=Ticker, 
                          if_exists='append',
                          index=True)
        time_end1 = time.time()
        logging.debug('insert time:%s' % '{:.2f}'.format(time_end1 - time_start1))

    except Exception as err:
        logging.error(err)
        errRec = pd.DataFrame([[str(Ticker),err]], columns=['Ticker','errMsg'])
    finally:
        pass
    return errRec
def get_latest_targetlist_marketstddb(querydate,targetlist):
    targetlist=sorted(targetlist)
    db_connection =quantlib.get_db_conn('mysql','dataimporter')
    db_cursor = db_connection.cursor()
    def check_is_exist_data(ticker):
        SQL= "select count(*) as num from marketstddb.`" + ticker + "`  WHERE Date ='" + querydate.strftime('%Y%m%d') + "'"
        db_cursor.execute(SQL)
        record = db_cursor.fetchone()
        return record[0]>0

    if not check_is_exist_data(targetlist[0]) and not check_is_exist_data('0050'):
        return targetlist
    if check_is_exist_data(targetlist[-1]) and check_is_exist_data('9962'):
        return []
    step = 100
    for i in range(0,len(targetlist),step):
        lower_ticker = targetlist[i]
        upper_ticker = targetlist[i+step-1] if i+step-1 < len(targetlist) - 1 else targetlist[-1]
        if check_is_exist_data(lower_ticker) and not check_is_exist_data(upper_ticker):
            return targetlist[i:]
def create_table(targetlist):
    _SQL1="(SELECT TABLE_NAME as Ticker FROM `INFORMATION_SCHEMA`.`TABLES` WHERE Table_Schema='marketstddb')"
    df=pd.read_sql_query(_SQL1, db_conn_query)
    _a = [str(x) for x in df['Ticker']]
    for Ticker in [x for x in targetlist if x not in _a]:
        try:
            db_cursor = db_conn_query.cursor()
            db_cursor.execute ('Create Table marketstddb.`' + Ticker + '` select * from marketstddb._sample1 where 1=0;')
            db_cursor.execute ('Alter table marketstddb.`' + Ticker + '` add primary key (`id`);')
            db_cursor.execute ('Alter table marketstddb.`' + Ticker + '` MODIFY id INTEGER  AUTO_INCREMENT;')
            db_cursor.execute ('Alter table marketstddb.`' + Ticker + '`  add  UNIQUE index `UniqueIndex` (`Date`);')
            db_conn_query.commit()
            logging.debug('marketstddb.`' + Ticker + '` created.')
        except Exception as Err:
            logging.error("def create_table Error: %s" % Err)
        finally:
            db_cursor.close()

def main(QueryDate):
    start_time1 =time.perf_counter()
    # 每日自動---------------
    #QueryDate=(datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
    StartDate = QueryDate - timedelta(days=10)
    EndDate = QueryDate
    tbl_suffix='' #'_tmp'
    # 每日自動---------------
    
    # # 手動---------------
    # StartDate=datetime(2004,12,31)
    # EndDate=datetime(2021,12,31)
    # tbl_suffix='_2015'
    #取得更新名單
    targetlist=quantlib.get_dailyupdatelist(EndDate)
    targetlist=get_latest_targetlist_marketstddb(EndDate, targetlist)
    # industry = {
    #         "TWA00":"加權指數",	
    #         "TWA02":"加權報酬指數",	
    #         "TWA04":"不含金融",	
    #         "TWA05":"不含電子",	
    #         "TWA06":"不含金融電子",	
    #         "TWA64":"台灣高股息報酬指數",	
    #         "TWA6N":"低波動股利精選30報酬指數",	
    #         "TWA6S":"特選高股息低波動報酬",	
    #         "TWB11":"水泥類",	
    #         "TWB12":"食品類",	
    #         "TWB13":"塑膠類",	
    #         "TWB14":"紡織纖維",	
    #         "TWB15":"電機類",	
    #         "TWB16":"電器電纜",	
    #         "TWB18":"玻璃陶瓷",	
    #         "TWB19":"造紙類",	
    #         "TWB20":"鋼鐵類",	
    #         "TWB21":"橡膠類",	
    #         "TWB22":"汽車類",	
    #         "TWB25":"營建類",	
    #         "TWB26":"運輸類",	
    #         "TWB27":"觀光類",	
    #         "TWB28":"金融保險",	
    #         "TWB29":"百貨類",	
    #         "TWB30":"化學工業",	
    #         "TWB31":"生技醫療",	
    #         "TWB32":"油電燃氣",	
    #         "TWB33":"半導體",	
    #         "TWB34":"電腦及週邊設備",	
    #         "TWB35":"光電",	
    #         "TWB36":"通信網路業",	
    #         "TWB37":"電子零組件",	
    #         "TWB38":"電子通路",	
    #         "TWB39":"資訊服務",	
    #         "TWB40":"其他電子",	
    #         "TWB99":"其他類",	
    #         "TWC37":"OTC文化創意",	
    #         "TWC38":"OTC農業科技",	
    #         "TWC39":"OTC電子商務"}
    #targetlist = list(industry.keys())
    #targetlist=['1101','1103','1110','1201','1203','1210','1216','1217','1219','1229','1258','1268','1315','1316','1323','1324','1339','1409','1416','1417','1432','1439','1440','1460','1465','1468','1475','1504','1512','1514','1515','1517','1519','1521','1528','1532','1538','1589','1598','1605','1609','1617','1702','1712','1720','1725','1726','1730','1732','1737','1742','1762','1789','1795','1806','1810','1817','1905','1907','2002','2012','2014','2017','2024','2064','2066','2104','2106','2201','2206','2207','2236','2243','2247','2302','2312','2316','2323','2331','2349','2371','2373','2377','2388','2399','2401','2404','2406','2412','2420','2424','2427','2431','2433','2442','2454','2461','2462','2466','2468','2474','2485','2491','2501','2514','2534','2535','2540','2543','2596','2601','2606','2607','2608','2609','2610','2611','2615','2616','2618','2702','2705','2722','2726','2729','2734','2740','2743','2745','2748','2754','2832','2838','2850','2851','2867','2880','2881','2882','2883','2885','2888','2903','2904','2905','2912','2913','2915','2923','2926','2937','2939','3003','3018','3024','3028','3036','3045','3051','3052','3057','3062','3064','3086','3202','3205','3218','3219','3288','3289','3294','3313','3338','3354','3356','3373','3376','3383','3413','3434','3479','3490','3494','3504','3540','3577','3593','3605','3607','3615','3632','3645','3646','3672','3679','3685','3691','3701','3703','3704','3709','4104','4106','4113','4141','4152','4154','4155','4164','4190','4198','4304','4401','4414','4426','4502','4523','4526','4530','4536','4545','4554','4560','4572','4581','4609','4714','4728','4729','4741','4803','4807','4904','4906','4909','4916','4924','4943','4973','4977','4989','4991','5009','5013','5016','5209','5210','5212','5215','5220','5258','5278','5281','5283','5314','5324','5328','5351','5356','5371','5434','5438','5439','5457','5474','5475','5484','5515','5521','5529','5530','5531','5534','5543','5546','5876','5903','5905','5907','6104','6108','6111','6118','6133','6134','6136','6140','6141','6144','6148','6163','6165','6168','6169','6179','6203','6219','6222','6225','6231','6236','6241','6246','6264','6275','6276','6283','6290','6292','6411','6414','6419','6425','6431','6443','6461','6464','6465','6499','6527','6533','6542','6547','6558','6560','6570','6573','6578','6589','6593','6598','6605','6616','6666','6674','6697','6706','6741','6743','6803','8032','8044','8047','8059','8064','8066','8067','8068','8071','8072','8077','8084','8088','8096','8101','8110','8201','8249','8403','8418','8427','8429','8431','8444','8450','8454','8472','8476','8482','8489','8921','8923','8927','8933','8936','912000','912398','9904','9919','9921','9924','9925','9933','9935','9950']
    #檢查table是否存在,若不存在則新增
    create_table(targetlist)
    
    TotalerrRec=pd.DataFrame(columns=['Ticker','errMsg'])
    i=0
    #for Ticker in targetlist[1758:]:
    for Ticker in  targetlist:
        i=i+1
        time_start = time.perf_counter()
        importerErr=marketstddb_importer(Ticker,StartDate,EndDate,tbl_suffix)
        TotalerrRec=TotalerrRec.append(importerErr,ignore_index=True)
        print('marketstddb-' + Ticker +'(' + str(i)+ '/' +str(len(targetlist)) + '):' + str(round(time.perf_counter() - time_start,2)))
    print("marketstddb-Total Exec time: %s" % str(round(time.perf_counter()-start_time1,2)))
    print(TotalerrRec)
    # cn.execute('replace INTO indistockdb.DataUpdateRecord values(0,"'+StartDate.strftime('%Y%m%d')+'","'+EndDate.strftime('%Y%m%d')+'","'+STime+'" ,"'+ str(datetime.now())+'");')
def default_fi_announce_date(df):
    df['日期_財報_公告日'] = df['日期_財報_公告日'].fillna(datetime(1900,1,1)) 
    df['日期_財報_公告日'] = pd.to_datetime(df['日期_財報_公告日'])
    #df1 = df[(df['日期_財報_公告日'].isna())|(df['日期_財報_公告日']==datetime(1900,1,1))]
    for i in range(len(df)):
        if df.iloc[i]['日期_財報_公告日']==datetime(1900,1,1):
            year_quarter=str(int(df.iloc[i]['年季']))
            year = int(year_quarter[:4])
            qrt = int(year_quarter[-2:])
            if year >=2013: #2013/1/2修正條文
                if qrt==1: date1 = datetime(year,5,15)
                elif qrt==2: date1 = datetime(year,8,14)
                elif qrt==3: date1 = datetime(year,11,14)
                else: date1 = datetime(year+1,3,31)
            elif year>=2012:#2012/3/12修正條文 
                if qrt==1: date1 = datetime(year,4,30)
                elif qrt==2: date1 = datetime(year,8,31)
                elif qrt==3: date1 = datetime(year,10,31)
                else: date1 = datetime(year+1,4,30)               
            else: 
                if qrt==1: date1 = datetime(year,4,30)
                elif qrt==2: date1 = datetime(year,8,31)
                elif qrt==3: date1 = datetime(year,10,31)
                else: date1 = datetime(year+1,4,30)                
            df.at[i,'日期_財報_公告日'] = date1
    return df
if __name__ == "__main__":
    # QueryDate=datetime(2021,7,9)
    # main(QueryDate)
    # CM財報BS==================================================
    _SQL='Select 公告日期 as 日期_財報_公告日,年季,case(right(`年季`,2)) when "01" then cast(concat(left(`年季`,4),"0331") as date) when "02" then cast(concat(left(年季,4),"0630") as date) when "03" then cast(concat(left(年季,4),"0930") as date) when "04" then cast(concat(left(年季,4),"1231") as date) end  as 日期_財報_資料日 from `marketrawdb_cm`.`md_cm_fi_bs_quarterly` where `股票代號` = "1101" and 年季 between "199001" and "202204"  order by 年季 asc'
    df = pd.read_sql_query(_SQL, db_conn_query)
    df['日期_財報_公告日'] = pd.to_datetime(df['日期_財報_公告日'])
    df['年季'] = df['年季'].astype(int).astype(str)
    print(df) 