import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# from measureretriever import *
from dataretriever import *
from datetime import datetime
from quantlib import *
import pandas  as pd
from time import perf_counter
db_conn=quantlib.get_db_conn()
industry_info={
            "全市場":"TWA00",
            "全市場(不含金融)":"TWA04",
            "水泥工業":"TWB11",
            "食品工業":"TWB12",
            "塑膠工業":"TWB13",
            "紡織纖維":"TWB14",
            "電機機械":"TWB15",
            "電器電纜":"TWB16",
            "玻璃陶瓷":"TWB18",
            "造紙工業":"TWB19",
            "鋼鐵工業":"TWB20",
            "橡膠工業":"TWB21",
            "汽車工業":"TWB22",
            "建材營建":"TWB25",
            "航運業":"TWB26",
            "觀光事業":"TWB27",
            "金融保險":"TWB28",
            "貿易百貨":"TWB29",
            "化學工業":"TWB30",
            "生技醫療":"TWB31",
            "油電燃氣":"TWB32",
            "電子–半導體":"TWB33",
            "電子–電腦及週邊設備":"TWB34",
            "電子–光電":"TWB35",
            "電子–通信網路":"TWB36",
            "電子–電子零組件":"TWB37",
            "電子–電子通路":"TWB38",
            "電子–資訊服務":"TWB39",
            "電子–其他電子":"TWB40",
            "其他":"TWB99",
            "文化創意":"TWC37",
            "農業科技":"TWC38",
            "電子商務":"TWC39"
            }

class INDISTOCK_STATISTICS():
    def __init__(self, sourcedata_folder_path) -> None:
        self.sourcedata_folder_path = sourcedata_folder_path
        self.db_conn = quantlib.get_db_conn(user='datateam')
        self.df_corp= self.get_corp_industry()

        self.startdate_fr = '200501'
        self.enddate_fr = '202204'

        self.startdate_ms = '201201'
        self.enddate_ms = '202212'
    def get_corp_industry(self):
        _SQL="SELECT `股票代號` as Ticker, `產業名稱` as industry FROM marketrawdb_cm.`bd_cm_companyprofile` WHERE 年度='2021'"
        df_corp = pandas.read_sql_query(_SQL, self.db_conn,index_col='Ticker')
        return df_corp
    def monthsales_yoy(self):
        _SQL="SELECT `股票代號` as Ticker,年月 as YearMonth,單月合併營收年成長 as field1 FROM `marketrawdb_cm`.`md_cm_fi_monthsales` where 年月 between '" + self.startdate_ms + "' and '" + self.enddate_ms + "'" 
        df_raw= pandas.read_sql_query(_SQL,self.db_conn)
        #修正日期格式標準化為yyyy-mm以利與股價df結合
        df_raw['YearMonth'] = pd.to_datetime(df_raw['YearMonth'].astype(str), format='%Y%m').dt.to_period('M')
        df_data = df_raw.pivot(index='Ticker', columns='YearMonth', values='field1')
        # df_data.to_csv('test0.csv',encoding='big5')

        df_data = self.df_corp.join(df_data,how='left')
        df_data.to_csv('test.csv',encoding='big5')
        #=====取的benchmark價格資料=======
        # df_result = self.merger_sectordata_bmprice(df_sectordata,'M')

class SECTOR_STATISTICS():
    def __init__(self, request_sector_list,sourcedata_folder_path) -> None:
        self.sourcedata_folder_path = sourcedata_folder_path
        self.db_conn = quantlib.get_db_conn(user='datateam')
        self.df_corp= self.get_corp_industry(request_sector_list)
        self.request_sector_list = request_sector_list

        self.startdate_fr = '200501'
        self.enddate_fr = self.get_latest_fr_date()

        self.startdate_ms = '201201'
        self.enddate_ms = self.get_latest_ms_date()
    
    def get_latest_fr_date(self):
        _SQL="SELECT 年季,Count(年季) as Num FROM  marketrawdb_cm.`md_cm_fi_is_quarterly` where 年季 > '202001' Group by 年季 order by 年季 desc" 
        df= pandas.read_sql_query(_SQL, self.db_conn)
        print("近期財報發布家數/上期財報發布家數:%s/%s" % (str(df.iloc[0]['Num']),str(df.iloc[1]['Num'])))
        if df.iloc[0]['Num']/df.iloc[1]['Num'] < 0.95:
            return str(df.iloc[1]['年季'])
        else:
            return str(df.iloc[0]['年季'])
    def get_fr_unpublished_list(self):
        _SQL="SELECT Distinct 年季 FROM  marketrawdb_cm.`md_cm_fi_is_quarterly` order by 年季 desc" 
        df= pandas.read_sql_query(_SQL, self.db_conn)
        prev_1 = str(int(df.iloc[0]['年季']))
        curr = str(int(df.iloc[1]['年季']))
        _SQL="SELECT 股票代號 as Ticker,年季 as 'YearQuarter' FROM  marketrawdb_cm.`md_cm_fi_is_quarterly` where 年季 in ('" + prev_1 + "','" + curr + "')"
        df= pandas.read_sql_query(_SQL, self.db_conn)
        df_data = df.pivot(index='Ticker', columns='YearQuarter', values='YearQuarter')
        df_data.columns=['Prev_1','Current']
        print(df_data[df_data['Current'].isna()])
        # return str(df.iloc[0]['年季'])        
    def get_latest_ms_date(self):
        _SQL="SELECT 年月,Count(年月) as Num FROM  marketrawdb_cm.`md_cm_fi_monthsales` where 年月 > '202001' Group by 年月 order by 年月 desc" 
        df= pandas.read_sql_query(_SQL, self.db_conn)
        print("近期月營收發布家數/上期月營收發布家數:%s/%s" % (str(df.iloc[0]['Num']),str(df.iloc[1]['Num'])))
        if df.iloc[0]['Num']/df.iloc[1]['Num'] < 0.98:
            return str(df.iloc[1]['年月'])
        else:
            return str(df.iloc[0]['年月'])
    def get_corp_industry(self, request_sector_list = None):
        _SQL="SELECT `股票代號` as Ticker,`產業名稱` as industry FROM marketrawdb_cm.`bd_cm_companyprofile` WHERE 年度='2021'"
        df_corp = pandas.read_sql_query(_SQL, self.db_conn,index_col='Ticker')
        if request_sector_list != None:
            df_corp = df_corp.loc[df_corp['industry'].isin(request_sector_list)]
        return df_corp
    def get_benchmark_aveprice_from_csv(self, StartDate, EndDate, Ticker_bm_list, Ticker_bm_name_list, DataPeriod = 'Q'):
        df_sector_price = pd.read_csv(os.path.join(self.sourcedata_folder_path,'sector_index.csv'),index_col='Date')
        df_sector_price.index = pd.to_datetime(df_sector_price.index)
        if DataPeriod == 'D':
            df_sector_price.index.name = 'DataDate'
        else:
            df_sector_price['DataDate']=df_sector_price.index.to_period(DataPeriod.upper())
            df_sector_price.set_index('DataDate',inplace= True)
            df_sector_price = df_sector_price.groupby('DataDate').mean()
        df_sector_price = df_sector_price.loc[StartDate:EndDate,Ticker_bm_list]
        df_sector_price.columns = Ticker_bm_name_list
        return df_sector_price
    def merger_sectordata_bmprice(self,df_sectordata,Dataperiod):
        s=str(df_sectordata.index[0])
        startdate = datetime(int(s[:4]),1,1)
        enddate = datetime.now()
        sectorname_list = [x for x in self.request_sector_list if x in df_sectordata.columns] #依照self.request_sector_list排序
        sector_bmticker_list = [industry_info[x] for x in sectorname_list]
        period_mapping = {'Q':'季均價','M':'月均價','W':'週均價','D':'收盤價'}
        sector_price_name_list = [ x + period_mapping[Dataperiod.upper()] for x in sectorname_list]
        if Dataperiod =='D':
            df_sector_price= self.get_benchmark_aveprice_from_csv(startdate,enddate,sector_bmticker_list,sector_price_name_list,Dataperiod)
        else:
            df_sector_price= df_sector_price= self.get_benchmark_aveprice_from_csv(startdate,enddate,sector_bmticker_list,sector_price_name_list,Dataperiod)
        #=====結果=======
        df_result = df_sectordata.join(df_sector_price, how='left')
        #篩選輸出資料
        df_result = df_result.loc[~df_result['全市場'].isnull()]
        
        col_list = []
        for i in range(len(sectorname_list)):
            col_list.append(sector_price_name_list[i])
            col_list.append(sectorname_list[i])
        return   df_result[col_list] 
    def calc_1FR_Field(self, dict_frfield, shift1, operator1):
        '''計算單一財報數值成長率 
        frfield_info:資料欄位為dict('field':xx,'table':yy)
        shift1:移動期數
        operator1:可用+,-,*,/,%(成長率)'''

        #=====取得原始資料=======
        # startQ = str(StartDate.year) + '0' + str((StartDate.month-1)//3+1)
        # endQ = str(EndDate.year) + '0' + str((EndDate.month-1)//3+1)
        _SQL="SELECT `股票代號` as Ticker,年季 as YearQuarter," + dict_frfield['field'] + " as field1 FROM " + dict_frfield['table'] + " where 年季 between '" + self.startdate_fr + "' and '" + self.enddate_fr + "' order by 年季 Asc"# where 年季 between '" + startQ +"' and '" + endQ +"'"
        df_raw= pandas.read_sql_query(_SQL, self.db_conn)
        #修正日期格式標準化為yyyy-Q以利與股價df結合
        df_raw['YearQuarter']=df_raw['YearQuarter'].astype(int).astype(str).apply(lambda x: x[:4] + '-' + x[4:]).str.replace('-0','-Q')
        df_raw['YearQuarter']=pd.to_datetime(df_raw['YearQuarter']).dt.to_period('Q')
        #轉化為Ticker x YearQuarter矩陣
        df_data1 = df_raw.pivot(index='Ticker', columns='YearQuarter', values='field1')
        df_data1 = self.df_corp.join(df_data1,how='left')
        df_data1 = df_data1.groupby('industry').sum()
        if "全市場" in self.request_sector_list:
            df_data1.loc["全市場"] = df_data1.sum()
        if "全市場(不含金融)" in self.request_sector_list:    
            df_data1.loc["全市場(不含金融)"] = df_data1.loc[~df_data1.index.isin(['金融保險','全市場'])].sum()          
        
        #計算shift期數資料
        df_data2 = df_data1.shift(shift1,axis=1)

        import operator
        ops = {'+': operator.add,
            '/': operator.truediv,
            '*': operator.mul,
            '-': operator.sub}
        
        if operator1 in ['+','-','*','/']:
            df_sectordata = ops[operator1](df_data1 ,df_data2)
        elif operator1 in ['%']:
            df_sectordata = (df_data1 - df_data2) / df_data1.abs()
        #=====轉置為 YearQuarter x Industry矩陣=======
        df_sectordata = df_sectordata.transpose()
        df_sectordata.index.name = 'DataDate'
        #=====取的benchmark價格資料=======
        df_result = self.merger_sectordata_bmprice(df_sectordata,'Q')
        return df_result
    def calc_2FRFields(self, dict_frfield1, dict_frfield2, operator1):
        '''處理兩個財報欄位計算'''
        #=====取得原始資料=======
        _SQL="SELECT A.`股票代號` as Ticker,A.年季 as YearQuarter,A." + dict_frfield1['field'] + " as field1,B." + dict_frfield2['field'] + " as field2 FROM (" + dict_frfield1['table'] + " A inner join " + dict_frfield2['table'] + " B on A.`股票代號`=B.`股票代號` and  A.年季=B.年季) where A.年季 between '" + self.startdate_fr + "' and '" + self.enddate_fr + "'"# where 年季 between '" + startQ +"' and '" + endQ +"'"
        df_raw= pandas.read_sql_query(_SQL,self.db_conn)
        #修正日期格式標準化為yyyy-Q以利與股價df結合
        df_raw['YearQuarter']=df_raw['YearQuarter'].astype(int).astype(str).apply(lambda x: x[:4] + '-' + x[4:]).str.replace('-0','-Q')
        df_raw['YearQuarter']=pd.to_datetime(df_raw['YearQuarter']).dt.to_period('Q')
        #=====計算2FR Field Operand=======
        #轉化為Ticker x YearQuarter矩陣
        df_data1 = df_raw.pivot(index='Ticker', columns='YearQuarter', values='field1')
        df_data1 = self.df_corp.join(df_data1,how='left')
        df_data1 = df_data1.groupby('industry').sum()

        if "全市場" in self.request_sector_list:
            df_data1.loc["全市場"] = df_data1.sum()
        if "全市場(不含金融)" in self.request_sector_list:    
            df_data1.loc["全市場(不含金融)"] = df_data1.loc[~df_data1.index.isin(['金融保險','全市場'])].sum()  

        df_data2 = df_raw.pivot(index='Ticker', columns='YearQuarter', values='field2')
        df_data2 = self.df_corp.join(df_data2,how='left')
        df_data2 = df_data2.groupby('industry').sum()

        if "全市場" in self.request_sector_list:
            df_data2.loc["全市場"] = df_data2.sum()
        if "全市場(不含金融)" in self.request_sector_list:    
            df_data2.loc["全市場(不含金融)"] = df_data2.loc[~df_data2.index.isin(['金融保險','全市場'])].sum()  
        #處理運算子
        import operator
        ops = {'+': operator.add,
            '/': operator.truediv,
            '*': operator.mul,
            '-': operator.sub}
        df_sectordata = ops[operator1](df_data1 ,df_data2)
        #=====轉置為 YearQuarter x Industry矩陣=======
        df_sectordata = df_sectordata.transpose()
        df_sectordata.index.name = 'DataDate'
        #=====取的benchmark價格資料=======
        df_result = self.merger_sectordata_bmprice(df_sectordata,'Q')
        return df_result
    def profit_margin(self):
        dict_frfield1 = {'field':'稅後純益','table':'`marketrawdb_cm`.`md_cm_fi_is_quarterly`'}
        dict_frfield2 = {'field':'營業收入淨額','table':'`marketrawdb_cm`.`md_cm_fi_is_quarterly`'}
        return self.calc_2FRFields(dict_frfield1,dict_frfield2,'/')
    def profit_margin_yoy(self):
        dict_frfield = {'field':'稅後純益','table':'`marketrawdb_cm`.`md_cm_fi_is_quarterly`'}
        return self.calc_1FR_Field(dict_frfield,4,'%')
    def gross_margin(self):
        dict_frfield1 = {'field':'營業毛利','table':'`marketrawdb_cm`.`md_cm_fi_is_quarterly`'}
        dict_frfield2 = {'field':'營業收入淨額','table':'`marketrawdb_cm`.`md_cm_fi_is_quarterly`'}
        return self.calc_2FRFields(dict_frfield1,dict_frfield2,'/')
    def gross_margin_yoy(self):
        dict_frfield = {'field':'營業毛利','table':'`marketrawdb_cm`.`md_cm_fi_is_quarterly`'}
        return self.calc_1FR_Field(dict_frfield,4,'%')

    def cash_ratio(self):
        dict_frfield1 = {'field':'現金及約當現金','table':'`marketrawdb_cm`.`md_cm_fi_bs_quarterly`'}
        dict_frfield2 = {'field':'資產總計','table':'`marketrawdb_cm`.`md_cm_fi_bs_quarterly`'}
        return self.calc_2FRFields(dict_frfield1 ,dict_frfield2,'/')
    def debt_ratio(self):
        dict_frfield1 = {'field':'長期負債合計','table':'`marketrawdb_cm`.`md_cm_fi_bs_quarterly`'}
        dict_frfield2 = {'field':'資產總計','table':'`marketrawdb_cm`.`md_cm_fi_bs_quarterly`'}
        return self.calc_2FRFields(dict_frfield1,dict_frfield2,'/')
    def roe(self):
        dict_frfield1 = {'field':'稅後純益*4','table':'`marketrawdb_cm`.`md_cm_fi_is_quarterly`'}
        dict_frfield2 = {'field':'權益總計','table':'`marketrawdb_cm`.`md_cm_fi_bs_quarterly`'}
        return self.calc_2FRFields(dict_frfield1,dict_frfield2,'/')
    def roe_ychg(self):
        dict_frfield1 = {'field':'稅後純益*4','table':'`marketrawdb_cm`.`md_cm_fi_is_quarterly`'}
        dict_frfield2 = {'field':'權益總計','table':'`marketrawdb_cm`.`md_cm_fi_bs_quarterly`'}
        df = self.calc_2FRFields(dict_frfield1,dict_frfield2,'/')
        return df-df.shift(4)
    def monthsales_yoy(self):
        _SQL="SELECT `股票代號` as Ticker,年月 as YearMonth,單月合併營收 as field1 FROM `marketrawdb_cm`.`md_cm_fi_monthsales` where 年月 between '" + self.startdate_ms + "' and '" + self.enddate_ms + "'"  #between '" + StartDate.strftime('%Y%m') +"' and '" + EndDate.strftime('%Y%m') +"'"
        df_raw= pandas.read_sql_query(_SQL,self.db_conn)
        #修正日期格式標準化為yyyy-mm以利與股價df結合
        df_raw['YearMonth'] = pd.to_datetime(df_raw['YearMonth'].astype(str), format='%Y%m').dt.to_period('M')
        df_data = df_raw.pivot(index='Ticker', columns='YearMonth', values='field1')
        # df_data.to_csv('test0.csv',encoding='big5')
        # col = df_data.columns
        # df_data = (df_data-df_data.shift(12,axis=1))/df_data.shift(12,axis=1).abs()
        df_data = self.df_corp.join(df_data,how='left')
        # df_data.to_csv('test.csv',encoding='big5')
        df_sectordata = df_data.groupby('industry').sum()
        if "全市場" in self.request_sector_list:
            df_sectordata.loc["全市場"] = df_sectordata.sum()
        if "全市場(不含金融)" in self.request_sector_list:         
            df_sectordata.loc["全市場(不含金融)"] = df_sectordata.loc[~df_sectordata.index.isin(['金融保險','全市場'])].sum()  
        #=====轉置為 YearQuarter x Industry矩陣=======
        df_sectordata = df_sectordata.transpose()
        df_sectordata = df_sectordata/df_sectordata.shift(12)-1
        df_sectordata.index.name = 'DataDate'
        #=====取的benchmark價格資料=======
        df_result = self.merger_sectordata_bmprice(df_sectordata,'M')
        return df_result
    def stockholder_volume_chg(self, sharesheld = 400, trailing_period = 1):
        _SQL="SELECT `股票代號` as Ticker,日期 as Date, " +str(sharesheld) + "張以上_張 as field1 FROM `marketrawdb_cm`.`md_cm_fd_stockholderstructure` order by 日期 Asc"  #between '" + StartDate.strftime('%Y%m') +"' and '" + EndDate.strftime('%Y%m') +"'"
        df_raw= pandas.read_sql_query(_SQL, self.db_conn)
        df_raw['Date'] = pd.to_datetime(df_raw['Date'].astype(str))
        df_data = df_raw.pivot(index='Ticker', columns='Date', values='field1')
        df_data = self.df_corp.join(df_data,how='left')

        df_sectordata = df_data.groupby('industry').sum()
        if "全市場" in self.request_sector_list:
            df_sectordata.loc["全市場"] = df_sectordata.sum()
        if "全市場(不含金融)" in self.request_sector_list: 
            df_sectordata.loc["全市場(不含金融)"] = df_sectordata.loc[~df_sectordata.index.isin(['金融保險','全市場'])].sum()    
        #=====轉置為 YearQuarter x Industry矩陣=======
        df_sectordata = df_sectordata.transpose()
        df_sectordata = df_sectordata -df_sectordata.shift(trailing_period)
        df_sectordata.index.name = 'DataDate'
        #確保日期格式是yyyy/mm/dd 否則與價格合併後會出現hh:mm:sss
        df_sectordata.index = pd.to_datetime(df_sectordata.index)
        #=====取的benchmark價格資料=======
        df_result = self.merger_sectordata_bmprice(df_sectordata,'D')
        return df_result
    def stockholder_largesmall_ratio(self,sharesheld = 400):
        '''大小股東持股比例'''
        #大股東持有張數
        _SQL="SELECT `股票代號` as Ticker,日期 as Date, " +str(sharesheld) + "張以上_張 as field1 FROM `marketrawdb_cm`.`md_cm_fd_stockholderstructure` order by 日期 Asc"  #between '" + StartDate.strftime('%Y%m') +"' and '" + EndDate.strftime('%Y%m') +"'"
        df_raw= pandas.read_sql_query(_SQL,self.db_conn)
        df_raw['Date'] = pd.to_datetime(df_raw['Date'].astype(str))
        df_data = df_raw.pivot(index='Ticker', columns='Date', values='field1')
        df_data = self.df_corp.join(df_data,how='left')

        df_sectordata = df_data.groupby('industry').sum()
        if "全市場" in self.request_sector_list:
            df_sectordata.loc["全市場"] = df_sectordata.sum()
        if "全市場(不含金融)" in self.request_sector_list: 
            df_sectordata.loc["全市場(不含金融)"] = df_sectordata.loc[~df_sectordata.index.isin(['金融保險','全市場'])].sum()    
        #小股東持有張數
        _SQL="SELECT `股票代號` as Ticker,日期 as Date, " +str(sharesheld) + "張以下_張 as field1 FROM `marketrawdb_cm`.`md_cm_fd_stockholderstructure` order by 日期 Asc"  #between '" + StartDate.strftime('%Y%m') +"' and '" + EndDate.strftime('%Y%m') +"'"
        df_raw= pandas.read_sql_query(_SQL,self.db_conn)
        df_raw['Date'] = pd.to_datetime(df_raw['Date'].astype(str))
        df_data = df_raw.pivot(index='Ticker', columns='Date', values='field1')
        df_data = self.df_corp.join(df_data,how='left')
        df_sectordata2 = df_data.groupby('industry').sum()
        if "全市場" in self.request_sector_list:
            df_sectordata2.loc["全市場"] = df_sectordata2.sum()
        if "全市場(不含金融)" in self.request_sector_list: 
            df_sectordata2.loc["全市場(不含金融)"] = df_sectordata2.loc[~df_sectordata2.index.isin(['金融保險','全市場'])].sum()

        df_sectordata3 = df_sectordata / df_sectordata2
        
        df_sectordata3 = df_sectordata3.transpose()
        df_sectordata3.index.name = 'DataDate'
        #確保日期格式是yyyy/mm/dd 否則與價格合併後會出現hh:mm:sss
        df_sectordata3.index = pd.to_datetime(df_sectordata3.index)
        #=====取的benchmark價格資料=======
        df_result = self.merger_sectordata_bmprice(df_sectordata3,'D')
        return df_result

# if __name__=='__main__':
#     sourcedata_folder_path= os.path.join(os.path.dirname(os.path.abspath(__file__)),'sourcedata')
#     obj = INDISTOCK_STATISTICS(sourcedata_folder_path)
#     obj.monthsales_yoy()
if __name__=='__main__':
    request_sector_list=['全市場','全市場(不含金融)','水泥工業','食品工業','農業科技','觀光事業','塑膠工業','建材營建','汽車工業','紡織纖維','貿易百貨','電機機械','生技醫療','電器電纜','化學工業','玻璃陶瓷','造紙工業','鋼鐵工業','橡膠工業','航運業','電子–電腦及週邊設備','電子–電子零組件','電子–半導體','電子–通信網路','電子–光電','電子–電子通路','電子–資訊服務','電子–其他電子','油電燃氣','金融保險','文化創意','電子商務','其他']
    #request_sector_list=['農業科技']
    sourcedata_folder_path= os.path.join(os.path.dirname(os.path.abspath(__file__)),'sourcedata')
    #取得產業指數價格
    if True:
        tickerlist=industry_info.values()
        df_result = pd.DataFrame()
        for ticker in tickerlist:
            obj = DataRetriever(ticker)
            df =  obj.query(datetime(2005,1,1),datetime.now(),['Close'],[ticker],2,'ffill')
            df_result = df_result.join(df) if not df_result.empty  else df
        df_result.to_csv(os.path.join(sourcedata_folder_path,'sector_index.csv'))


    obj = SECTOR_STATISTICS(request_sector_list, sourcedata_folder_path)
    obj.get_fr_unpublished_list()
    exec_info = {
                #  '淨利率':{'func':'obj.profit_margin()','filename':'淨利率'},
                #  '淨利成長率YoY':{'func':'obj.profit_margin_yoy()','filename':'淨利成長率YoY'},
                #  '毛利率':{'func':'obj.gross_margin()','filename':'毛利率'},
                #  '毛利成長率YoY':{'func':'obj.gross_margin_yoy()','filename':'毛利成長率YoY'},
                #  '股東權益報酬率':{'func':'obj.roe()','filename':'股東權益報酬率'},
                #  '股東權益報酬率_ychg':{'func':'obj.roe_ychg()','filename':'股東權益報酬率_ychg'},                
                #  '現金比例':{'func':'obj.cash_ratio()','filename':'現金比例'},
                #  '負債比例':{'func':'obj.debt_ratio()','filename':'負債比例'},
                 '月營收YoY':{'func':'obj.monthsales_yoy()','filename':'月營收YoY'},
                 '大小股東持股比率_800張':{'func':'obj.stockholder_largesmall_ratio(800)','filename':'大小股東持股比率_800張'},
                 '大小股東持股比率_400張':{'func':'obj.stockholder_largesmall_ratio(400)','filename':'大小股東持股比率_400張'},
                 '持股400張以上週變動':{'func':'obj.stockholder_volume_chg(400,1)','filename':'持股400張以上週變動'},
                 '持股400張以上週變動_T4':{'func':'obj.stockholder_volume_chg(400,4)','filename':'持股400張以上週變動_T4'}
                }
    for item in exec_info:
        func = exec_info[item]['func']
        filename = exec_info[item]['filename']
        df = eval(func)
        df.round(5).to_csv(os.path.join(sourcedata_folder_path,filename + '.csv'),encoding='utf-8-sig')