from datetime import datetime, timedelta
import pandas 
import numpy 
import mysql.connector as sql
import json,os,logging as log,re
from sqlalchemy import create_engine
from time import perf_counter
log.basicConfig(level=log.ERROR, format='%(asctime)s - %(levelname)s : %(message)s')

class Setting:
    def __init__(self) -> None:
        self.folderpath_setting = os.path.dirname(__file__)
    def upper_measure_profile(self, measure_profile):   
        '''為了要讓measure不分大小寫，因此須確保與measure name相關的部分都轉為大寫,
        func-measure中的dependency須為大寫,以避免找不到DBDataProfile
        func-measure中的func須為大寫,以避免找不到TABLIB'''
        result = {}
        for k,v in measure_profile.items():
            if 'dependency' in v:
                v['dependency'] = [c.upper() for c in v['dependency']]
            if 'func' in v:
                v['func'] =  v['func'].upper()
            v['flag_colname'] =  v['flag_colname'].upper()
            result.update({k.upper():v})
        return result   
    def get_setting_db_measure(self):
        return self.upper_measure_profile(json.load(open(self.folderpath_setting + '\\setting_db_measure.json')))
    def get_setting_func_measure(self):
        return self.upper_measure_profile(json.load(open(self.folderpath_setting + '\\setting_func_measure.json')))
    def get_setting_all_measure(self):
        db_measure=self.upper_measure_profile(json.load(open(self.folderpath_setting + '\\setting_db_measure.json')))
        func_measure=self.upper_measure_profile(json.load(open(self.folderpath_setting + '\\setting_func_measure.json')))
        return  dict(**db_measure,**func_measure)
    def get_setting_rule(self) -> None:
        _ruleprofile = json.load(open(self.folderpath_setting + '\\setting_rule.json'))
        rule_setting = {}
        #將名稱轉為大寫
        for k,v in _ruleprofile.items():
            rule_setting.update({k.upper():v})
        return rule_setting
    def get_setting_rule_group(self) -> None:
        _rule_group = json.load(open(self.folderpath_setting + '\\setting_rule_group.json'))
        rule_group_setting = {}
        #將名稱轉為大寫
        for k,v in _rule_group.items():
            rule_group_setting.update({k.upper():v})
        return rule_group_setting
    def get_setting_strategy(self) -> None:
        _strategy = json.load(open(self.folderpath_setting + '\\setting_strategy.json'))
        strategy_setting = {}
        #將名稱轉為大寫
        for k,v in _strategy.items():
            strategy_setting.update({k.upper():v})
        return strategy_setting
    def get_setting_port_member(self) -> None:
        _port_member = json.load(open(self.folderpath_setting + '\\setting_port_member.json'))
        port_member_setting = {}
        #將名稱轉為大寫
        for k,v in _port_member.items():
            port_member_setting.update({k.upper():v})
        return port_member_setting

def get_db_conn(engine = 'mysql', user = 'dataretriever',db ='indistockdb'):
    conninfo = {'dataretriever':{'db':db, 'password':'dataretriever','port':'13306'},
            'dataimporter':{'db':db, 'password':'dataimporter','port':'13306'},
            'datateam':{'db':db, 'password':'datateam','port':'13306'},
             }
    pw=conninfo[user]['password']
    port=conninfo[user]['port']
    db=conninfo[user]['db']
    if engine == 'sqlalchemy':
        return create_engine("mysql+pymysql://{user}:{pw}@home.dottdot.com:{port}/{db}".format(user=user,pw=pw,port=port,db=db)).connect() 
    else:
        return sql.connect(host='home.dottdot.com', db=db, user=user, password=pw,port=port)
# def get_db_conn_dataretriever():
#     return sql.connect(host='home.dottdot.com', db='indistockdb', user='dataretriever', password='dataretriever',port='13306')
# def get_db_conn_dataimporter(db = 'indistockdb'):
#     return sql.connect(host='home.dottdot.com', db=db, user='dataimporter', password='dataimporter',port='13306')

#context manager寫法
from contextlib import contextmanager
@contextmanager
def get_db_conn_1():
    db_conn = sql.connect(host='home.dottdot.com', db='indistockdb', user='dataretriever', password='dataretriever',port='13306')
    yield db_conn
    db_conn.close() 
 
def get_db_conn_datateam():
    return sql.connect(host='home.dottdot.com', db='indistockdb', user='datateam', password='datateam',port='13306')
def get_rule_group_member(rule_group_id_list,is_drop_duplicate = True):
    '''
    Return the list of the rules by list of rule_group_id 
    rule_group_id_list 可傳入單一或是[多個]rule_group_id
    
    '''
    rule_group_profile = Setting().get_setting_rule_group()
    rule_group_member=[]
    if type(rule_group_id_list) == str:
        rule_group_member = rule_group_profile[rule_group_id_list.upper()]
    elif type(rule_group_id_list) == list:
        for rg in rule_group_id_list:
            rule_group_member= rule_group_member + rule_group_profile[rg.upper()]
    if is_drop_duplicate:
        tmp =list()
        [tmp.append(x) for x in rule_group_member if x not in tmp]
        rule_group_member = tmp
        #rule_group_member = list(dict.fromkeys(rule_group_member)) 不能用這個,因為會有nest of list 
    return rule_group_member
def get_rule_group_member_and_weighting(rule_group_id_list, rule_group_weighting =[]):
    '''
    Return the rule list and rule weighting by rule_group_name 
    rule_group_list 可傳入單一或是[多個]rule_group_name
    '''
    rule_group_profile = Setting().get_setting_rule_group()
    rule_group_member=[]
    rule_weighting=[]
    if type(rule_group_id_list) == str:
        rule_group_member = rule_group_profile[rule_group_id_list.upper()]
    elif type(rule_group_id_list) == list:
        for st in rule_group_id_list:
            rule_group_member= rule_group_member + rule_group_profile[st.upper()]
            if rule_group_weighting != []:
                num_rule =len(rule_group_profile[st.upper()])
                rule_weighting = rule_weighting + [rule_group_weighting[rule_group_id_list.index(st)]/num_rule] * num_rule
    if rule_group_weighting == []: 
        rule_weighting = [1] * len(rule_group_member)
    return rule_group_member,rule_weighting
def get_key_path():
    return "d:\key"
def get_corpname(ticker):
    db_connection = get_db_conn(user='datateam')
    db_cursor = db_connection.cursor()
    _sql ="Select 股票名稱 from `marketrawdb_cm`.`bd_cm_companyprofile` where 股票代號='" + str(ticker) +"' limit 1"
    db_cursor.execute(_sql)
    a = db_cursor.fetchone()
    return a[0]
def get_indistockdb_stock():
    #取得indistockdb有的標的
    _SQL1="(SELECT TABLE_NAME as Ticker FROM `INFORMATION_SCHEMA`.`TABLES` WHERE Table_Schema='indistockdb')"
    with get_db_conn_1() as db_connection:
        df_indi=pandas.read_sql_query(_SQL1, db_connection)
    #僅取得Ticker為數字的股票(排除基金類(開頭0)與指數類(TW..)與日期格式)
    df_indi=df_indi[(df_indi['Ticker'].apply(lambda x: list(x)[0]) != '0')  & (df_indi['Ticker'].apply(lambda x: len(x)<8)) & (df_indi['Ticker'].str.isnumeric())]
    df_indi.set_index('Ticker',inplace=True)
    return df_indi


def get_dailyupdatelist(QueryDate:datetime):
    df = pandas.read_sql_query('Select 股票代號 as Ticker from `marketrawdb_cm`.`bd_cm_companyprofile` where 年度=' + str(QueryDate.year) +' order by `股票代號` asc', get_db_conn(user='datateam'))
    industry = {
            "TWA00":"加權指數",	
            "TWA02":"加權報酬指數",	
            "TWA04":"不含金融",	
            "TWA05":"不含電子",	
            "TWA06":"不含金融電子",	
            "TWA64":"台灣高股息報酬指數",	
            "TWA6N":"低波動股利精選30報酬指數",	
            "TWA6S":"特選高股息低波動報酬",	
            "TWB11":"水泥類",	
            "TWB12":"食品類",	
            "TWB13":"塑膠類",	
            "TWB14":"紡織纖維",	
            "TWB15":"電機類",	
            "TWB16":"電器電纜",	
            "TWB18":"玻璃陶瓷",	
            "TWB19":"造紙類",	
            "TWB20":"鋼鐵類",	
            "TWB21":"橡膠類",	
            "TWB22":"汽車類",	
            "TWB25":"營建類",	
            "TWB26":"運輸類",	
            "TWB27":"觀光類",	
            "TWB28":"金融保險",	
            "TWB29":"百貨類",	
            "TWB30":"化學工業",	
            "TWB31":"生技醫療",	
            "TWB32":"油電燃氣",	
            "TWB33":"半導體",	
            "TWB34":"電腦及週邊設備",	
            "TWB35":"光電",	
            "TWB36":"通信網路業",	
            "TWB37":"電子零組件",	
            "TWB38":"電子通路",	
            "TWB39":"資訊服務",	
            "TWB40":"其他電子",	
            "TWB99":"其他類",	
            "TWC37":"OTC文化創意",	
            "TWC38":"OTC農業科技",	
            "TWC39":"OTC電子商務"}
    return sorted((['0050','0056'] + [str(x) for x in df['Ticker']] + list(industry.keys())))
class STOCK_SCREENER():
    '''用來篩選資產池物件(未來替代get_watchlist())'''
    def __init__(self, screener_name = ['ALL'],screener_date =[], **screener_para) -> None:
        self.db_connection = get_db_conn()
        #screener name
        if type(screener_name) == str:
            self.screener_name_list = [screener_name]    
        else:
            self.screener_name_list =  screener_name
        #screener date
        if type(screener_date) == datetime:
            self.screener_date_list =  [screener_date] 
        else:
            self.screener_date_list =  screener_date if screener_date!=[] else [datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)]
        #screener sector
        self.sector = []    
        if 'SECTOR' in screener_para.keys():
            self.sector =  screener_para['SECTOR'] 
        self.is_excl_sector = 0
        if 'IS_EXCL_SECTOR' in screener_para.keys():
            self.is_excl_sector = screener_para['IS_EXCL_SECTOR'] 
        

    def get_sn_type(self, sn):
        #FG
        if sn.upper() == 'FG':
            return 'FG'
        #ALL
        if sn.upper() == 'ALL':
            return 'ALL' 
        if sn.upper() == 'FA':
            return 'FA'
        #市值
        _r = re.search(r"(?P<MV>MV)_(?P<LB>.+)_(?P<UB>\d+)", sn.upper())
        if _r:
            return 'MV'
        #股本
        _r = re.search(r"(?P<CAP>CAP)_(?P<LB>.+)_(?P<UB>\d+)", sn.upper())
        if _r:
            return 'CAP'
    def screener_MV(self, sn, screener_date):
        tbl_suffix = '_2015' if screener_date.year <=2015 else ''
        #檢查LB是否為TOP
        _r = re.search(r"(?P<MV>MV)_(?P<LB>.+)_(?P<UB>\d+)", sn.upper())
        LB = 0 if _r.group('LB').upper() == 'TOP' else  int(_r.group('LB'))-1
        UB= int(_r.group('UB'))-1
        Num = UB-LB +1
        _SQL= "SELECT `股票代號` as Ticker,`總市值` as MV FROM view_md_cm_ta_dailyquotes" + tbl_suffix + " WHERE `日期` = (SELECT MAX(DATE) FROM view_calendardate WHERE `Date`<=" + screener_date.strftime('%Y%m%d') + ") And `股票代號` >'1000' and left(`股票代號`,1) REGEXP '^[0-9]+$' Order By MV Desc Limit " + str(LB) + "," + str(Num) 
        df= pandas.read_sql_query(_SQL, self.db_connection,index_col='Ticker')

        return list(df.index)
    def screener_CAP(self, sn, screener_date):
        tbl_suffix = '_2015' if screener_date.year <=2015 else ''
        #檢查LB是否為TOP
        _r = re.search(r"(?P<MV>CAP)_(?P<LB>.+)_(?P<UB>\d+)", sn.upper())
        LB = 0 if _r.group('LB').upper() == 'TOP' else  int(_r.group('LB'))-1
        UB= int(_r.group('UB')) -1
        Num = UB-LB +1
            
        _SQL= "SELECT `股票代號` as Ticker,`股本` as `CAP` FROM view_md_cm_ta_dailyquotes" + tbl_suffix + " WHERE `日期` = (SELECT MAX(DATE) FROM view_calendardate WHERE `Date`<=" + screener_date.strftime('%Y%m%d') + ") And `股票代號` >'1000' and left(`股票代號`,1) REGEXP '^[0-9]+$' Order By CAP Desc Limit " + str(LB) + "," + str(Num)
        df= pandas.read_sql_query(_SQL, self.db_connection,index_col='Ticker')
        return list(df.index)    
    def screener_FA_Ratio(self,criteria, yearquarter):
        _SQL= "SELECT `股票代號` as Ticker,`不動產、廠房及設備`/`資產總計` as Ratio FROM view_md_cm_fi_bs_quarterly WHERE `年季` = '" + str(yearquarter)  + "'"
        df= pandas.read_sql_query(_SQL, self.db_connection ,index_col='Ticker')
        df=df[df['Ratio']>=criteria]
        return list(df.index)
    def screener_FG(self):
        _SQL= "SELECT `Ticker` as Ticker FROM bd_stocklist資產池" 
        df= pandas.read_sql_query(_SQL, self.db_connection,index_col='Ticker')
        return list(df.index)
    def screener_ALL(self, screener_date):
        tbl_suffix = '_2015' if screener_date.year <=2015 else ''
        _SQL= "SELECT `股票代號` as Ticker FROM view_md_cm_ta_dailyquotes" + tbl_suffix + " WHERE `日期` = (SELECT MAX(DATE) FROM view_calendardate WHERE `Date`<=" + screener_date.strftime('%Y%m%d') + ") And `股票代號` >'1000' and left(`股票代號`,1) REGEXP '^[0-9]+$'" 
        df= pandas.read_sql_query(_SQL, self.db_connection ,index_col='Ticker')
        return list(df.index)
    def sector_filter(self, stock_list, sector_list, is_excl_sector):
        _SQL="SELECT Distinct `股票代號` as Ticker, `產業名稱` as industry FROM marketrawdb_cm.`bd_cm_companyprofile`" 
        df_corp = pandas.read_sql_query(_SQL,  self.db_connection,index_col='Ticker')
        if sector_list != []:
            if is_excl_sector:
                df_corp= df_corp.loc[~df_corp['industry'].isin(sector_list)]
            else:
                df_corp= df_corp.loc[df_corp['industry'].isin(sector_list)]
        return list(set(stock_list) & set(df_corp.index.astype(str)))
        
    def get_stock_list(self):
        stock_list_all=[]
        for sn in self.screener_name_list:
            for screener_date in self.screener_date_list:
                sn_type = self.get_sn_type(sn)
                if sn_type == 'MV':
                    stock_list = self.screener_MV(sn, screener_date)
                elif sn_type == 'CAP':
                    stock_list = self.screener_CAP(sn, screener_date)
                elif sn_type == 'FG':
                    stock_list = self.screener_FG()
                elif sn_type == 'FA':
                    stock_list = self.screener_FA_Ratio(0.2,'202103')
                else:#ALL
                    stock_list = self.screener_ALL(screener_date)
               
                #轉成str 
                stock_list_all = stock_list_all + [str(ticker) for ticker in stock_list]

        #去除重複
        stock_list_all = list(dict.fromkeys(stock_list_all))
        if '' in stock_list_all:stock_list_all.remove('')
        #去除沒在indistockdb的部分
        df_indi = get_indistockdb_stock()
        indi_list = list(df_indi.index.astype(str))
        stock_list_all=list(set(indi_list) & set(stock_list_all))
        #選取產業別
        stock_list_all = self.sector_filter(stock_list_all,self.sector,self.is_excl_sector)
        return sorted(stock_list_all)
        
def get_watchlist(screener_name, screen_date, is_excl_dr = True):
    '''給定screener_name與screen_date後，取得符合screener_name條件的標的
    screener_name:可以是單一或是list，目前可支援的是MV_TOP_標的數 or MV_LB_UB LB從1開始,含UB。或CAP_TOP_標的數 or CAP_LB_UB LB從1開始,含UB
    screen_date:可以是單一日期或是list'''
    watchlist = []
    screener_name_list = []
    if type(screener_name) == str:
        screener_name_list.append(screener_name)
    else:
        screener_name_list = screener_name    

    for sn in screener_name_list:
        sort_col = ''
        Num = 0
        #取得區間市值
        _r = re.search(r"(?P<MV>MV)_(?P<LB>.+)_(?P<UB>\d+)", sn.upper())
        is_MV = 1 if _r else 0
        #取得區間股本
        if is_MV ==0:
            _r = re.search(r"(?P<CAP>CAP)_(?P<LB>.+)_(?P<UB>\d+)", sn.upper())
            is_CAP = 1 if _r else 0        

        #看是否為FG
        is_FG = 1 if sn.upper() == 'FG' else 0
        is_All = 1 if sn.upper() == 'ALL' else 0

        screen_date_list = []
        if type(screen_date) == datetime:
            screen_date_list.append(screen_date)
        else:
            screen_date_list = screen_date
        for date1 in screen_date_list:
            tbl_suffix = '_2015' if date1.year <=2015 else ''
            if is_MV:
                #檢查LB是否為TOP
                LB = 1 if  _r.group('LB').upper() == 'TOP' else  int(_r.group('LB'))
                UB= int(_r.group('UB')) 
                Num = UB-LB +1              
                _SQL= "SELECT `股票代號` as Ticker,`總市值` as MV FROM view_md_cm_ta_dailyquotes" + tbl_suffix + " WHERE `日期` = (SELECT MAX(DATE) FROM view_calendardate WHERE `Date`<=" + date1.strftime('%Y%m%d') + ")"
                
                sort_col='MV'
            elif is_CAP:
                #檢查LB是否為TOP
                LB = 1 if  _r.group('LB').upper() == 'TOP' else  int(_r.group('LB'))
                UB= int(_r.group('UB')) 
                Num = UB-LB +1
                _SQL= "SELECT `股票代號` as Ticker,`股本` as CAP FROM view_md_cm_ta_dailyquotes" + tbl_suffix + " WHERE `日期` = (SELECT MAX(DATE) FROM view_calendardate WHERE `Date`<=" + date1.strftime('%Y%m%d') + ")"
                sort_col='CAP'

            elif is_FG:
                _SQL= "SELECT `Ticker` as Ticker FROM bd_stocklist資產池"
            else:#全樣本
                _SQL= "SELECT `股票代號` as Ticker FROM view_md_cm_ta_dailyquotes" + tbl_suffix + " WHERE `日期` = (SELECT MAX(DATE) FROM view_calendardate WHERE `Date`<=" + date1.strftime('%Y%m%d') + ")"
            with get_db_conn_1() as db_connection:
                df= pandas.read_sql_query(_SQL, db_connection,index_col='Ticker')
            
            #僅取得indistockdb有的標的
            df_indi = get_indistockdb_stock()
            df_result = pandas.concat([df_indi, df], axis=1, join="inner")
            if sort_col != '':
                df_result = df_result.sort_values(sort_col,ascending = False)
            df_result.reset_index(inplace=True)#取消Index
            if Num > 0:
                if UB >= len(df_result.index):
                    UB=len(df_result.index)-1
                df_result = df_result.iloc[LB-1:UB]
            #轉成str          
            watchlist = watchlist + [str(ticker) for ticker in list(df_result['Ticker'])]

    #是否排除DR
    if is_excl_dr:
        watchlist = [ticker for ticker in watchlist if ticker[:2] !='91']
    #remove duplicates required_dbfields
    watchlist = list(dict.fromkeys(watchlist))
    if '' in watchlist:watchlist.remove('')
    #移除特殊標的
    # if '2015' in watchlist:#找不出錯誤原因的標的,只要回測執行到這個標的,就會出錯，但看資料都沒問題
    #     watchlist.remove('2015')    
    return sorted(watchlist)
def get_request_rule_alias(request_rule):
    return ['_&_'.join(rd) if type(rd) == list else rd for rd in request_rule]

def get_measure_list(measure_type):
    if 'DB' in measure_type.upper():
        measure_profile = Setting().get_setting_db_measure()
    elif 'FUNC' in measure_type.upper():
        measure_profile = Setting().get_setting_func_measure()
    else:
        db_measure_profile = Setting().get_setting_db_measure()
        func_measure_profile = Setting().get_setting_func_measure()
        measure_profile = dict(**func_measure_profile,**db_measure_profile)
    return list(measure_profile.keys())
def get_port_member(portid):
    '''portid 可傳入單一或是[多個]'''
    port_member_profile = Setting().get_setting_port_member()
    port_member=[]
    if type(portid) == str:
        port_member = port_member_profile[portid.upper()]
    else:
        for pm in portid:
            port_member= port_member + port_member_profile[pm.upper()]
    #remove duplicates required_dbfields
    port_member = list(dict.fromkeys(port_member))
    if '' in port_member:port_member.remove('')
    #轉成str並排序
    port_member =sorted([str(ticker) for ticker in port_member])

    return port_member
def get_basedate(startdate: datetime, enddate: datetime, countrycode: str ='TW', intbasedatetype: int = 1):
    df = pandas.DataFrame()
    if intbasedatetype == 0: 
        return df
    if intbasedatetype == 1: # 'TradingDate
        # _SQL='Select `Date` from `indistockdb`.`view_calendardate`  where `Date` between "'+ startdate.strftime('%Y%m%d') +'" and "'+ enddate.strftime('%Y%m%d') +'" And CountryCode="' + countrycode + '" order by `Date` asc'
        # df= pandas.read_sql_query(_SQL, get_db_conn())

        df = pandas.read_csv('setting_tradingdate.csv')
        df['Date'] = pandas.to_datetime(df['Date'])
        # df.to_csv('setting_tradingdate.csv',index=False)
        df = df[(df['Date']>=startdate) & (df['Date']<=enddate)]
    elif intbasedatetype == 2:# Calendard date
        df['Date'] = pandas.date_range(start= startdate, end=enddate,freq='D')
    df.set_index('Date', inplace=True)
    return df
def df_adj_basedate(df: pandas.DataFrame, startdate: datetime, enddate: datetime, countrycode: str ='TW', intbasedatetype: int = 1, fillna_method = 'ffill'):
    df_basedate= get_basedate(startdate, enddate, countrycode , intbasedatetype)
    if df_basedate.index.empty:
        return df
    elif fillna_method is None or fillna_method == 0:
        return df_basedate.join(df, how='left')
    else:
        return df_basedate.join(df, how='left').fillna(method = fillna_method)        
    #df = pandas.merge_asof(df_basedate,df,left_index = True,right_on='Date',)
def get_periodic_date(startdate: datetime, enddate: datetime, periodic_day, periodic_month: int, intbasedatetype: int = 1):
    '''periodic_day : 每月幾號
        periodic_month : 幾個月重複一次
    '''
    periodic_day = str(periodic_day)
    if periodic_day == '1': # first day of month
        periodic_date = pandas.date_range(start= startdate.replace(day=1), end=enddate,freq=str(periodic_month) + ' MS')
    elif periodic_day == '-1': # enddate of month
        periodic_date = pandas.date_range(start= startdate.replace(day=1), end=enddate,freq=str(periodic_month) + ' M')
    elif periodic_day.upper() == 'FA': # TW Financial Report Announce Q1:5/15 Q2:8/14 Q3:11/14 Q4(Annual):3/31
        Q1_date = pandas.date_range(start= startdate.replace(month = 5,day=1), end=enddate,freq='12 MS').shift(15, freq='D')
        Q2_date = pandas.date_range(start= startdate.replace(month = 8,day=1), end=enddate,freq='12 MS').shift(14, freq='D')
        Q3_date = pandas.date_range(start= startdate.replace(month = 11,day=1), end=enddate,freq='12 MS').shift(14, freq='D')
        Q4_date = pandas.date_range(start= startdate.replace(month = 4,day=1), end=enddate,freq='12 MS')
    
        periodic_date = Q1_date.union(Q2_date).union(Q3_date).union(Q4_date) 
    else:
        periodic_date = pandas.date_range(start= startdate.replace(day=1), end=enddate,freq=str(periodic_month) + ' MS').shift(int(periodic_day) -1, freq='D')
    #處理非日曆日,預設是TW,bfill
    if intbasedatetype != 2:
        startdate1= startdate
        enddate1= enddate + timedelta(days= 30) #因為要bfill所以要多取日期
        df_left=get_basedate(startdate1,enddate1, 'TW' , 2)
        df_left['calendarddate']=df_left.index
        df_left.set_index(['calendarddate'],inplace=True)
        df_right= get_basedate(startdate1,enddate1, 'TW' , intbasedatetype)
        df_right['querydate']=df_right.index
        df=df_left.join(df_right, how='left').fillna(method ='bfill')  
        periodic_date = df.reindex(periodic_date)['querydate']
    return [d.to_pydatetime() for d in periodic_date if (d >= startdate) & (d<=enddate)]
def flatten_nested_list(nested_list: list):
    #將nest of list 變為 flat list
    flat_list = []
    for sublist in nested_list:
        if type(sublist) == list:
            for rl in sublist:
                flat_list.append(rl)
        else:
            flat_list.append(sublist)
    #remove duplicates 
    flat_list_r = list(dict.fromkeys(flat_list))
    if '' in flat_list_r:flat_list_r.remove('')
    return flat_list_r

def test():
    user="test_speed"
    pw="test_speed"
    port="13306"
    db="indistockdb"
    db_conn = create_engine("mysql+pymysql://{user}:{pw}@home.dottdot.com:{port}/{db}".format(user=user,pw=pw,port=port,db=db)).connect() 

    #db_conn =sql.connect(host='home.dottdot.com', db=db, user=user, password=pw,port=port)
    _SQL='Select Date,Close FROM `0050` where `Date` between "2015/1/1" And "2020/12/31"'
    df_resultdata= pandas.read_sql_query(_SQL, db_conn)
    print(df_resultdata)
def test_speed_df():
    #結果:
    #測試資料
    # ['1101','1102','2330','2303','2891']
    # 'Select * FROM `'+ ticker + '` where `Date` between "2015/1/1" And "2020/12/31"'
    # connect速度:用mysql比用sqlalchemy快0.3秒左右,但這個比較不重要，因為可以開啟一次做大量運算
    #pandas.read_sql_query速度(不同engine下):用mysql(24秒)比用sqlalchemy(40秒),平均一個table差了3秒，差異非常的大
    #比較pandas.read_sql_query和cursor.fetchall(在mysql engine):用pandas.read_sql_query(24秒)、cursor.fetchall(25秒),因此使用read_sql_query即可
    user="test_speed"
    pw="test_speed"
    port="13306"
    db="indistockdb"
    
    def get_db_conn_sqlalchemy():
        return create_engine("mysql+pymysql://{user}:{pw}@home.dottdot.com:{port}/{db}".format(user=user,pw=pw,port=port,db=db)).connect() 

    def get_db_conn_mysql():
        return sql.connect(host='home.dottdot.com', db=db, user=user, password=pw,port=port)
    testlist=['0050']
    a=perf_counter()
    db_connection = get_db_conn_mysql()
    print("connect: %s" % str(perf_counter()-a))
    for ticker in testlist:
        b=perf_counter()
        _SQL='Select `Date`,`Close` FROM `'+ ticker + '` where `Date` between "2015/1/1" And "2020/12/31"'
        print(_SQL)
        # method 1
        db_cursor = db_connection.cursor()
        db_cursor.execute(_SQL)
        df = pandas.DataFrame(db_cursor.fetchall())
        df_resultdata= pandas.read_sql_query(_SQL, db_connection)
        print(ticker +": %s" % str(perf_counter()-b))
       
    print("pandas.read_sql_query: %s" % str(perf_counter()-a))
    #method 2
    # _SQL='Select * FROM `1101` where `Date` between "2015/1/1" And "2020/12/31"'
    # a=perf_counter()
    # db_cursor = db_connection.cursor()
    # db_cursor.execute(_SQL)
    # df = pandas.DataFrame(db_cursor.fetchall())
    # db_cursor.close()
    # # df= pandas.DataFrame(db_connection.execute(_SQL).fetchall())
    # print("fetchall(): %s" % str(perf_counter()-a))
def test_speed_db_connect():
   
    #比較不同的package的連線速度 ==> mysql比sqlalchemy連線速度快
    user="test_speed"
    pw="test_speed"
    port="13306"
    db="indistockdb"
    def get_db_conn_sqlalchemy():
        return create_engine("mysql+pymysql://{user}:{pw}@home.dottdot.com:{port}/{db}".format(user=user,pw=pw,port=port,db=db)).connect() 
    import mariadb
    def get_db_conn_mariadb():
        return mariadb.connect(host='home.dottdot.com', db=db, user=user, password=pw,port=int(port))

    def get_db_conn_mysql():
        return sql.connect(host='home.dottdot.com', db=db, user=user, password=pw,port=port)
    #sqlalchemy
    a=perf_counter()
    db_cnn=get_db_conn_sqlalchemy()
    print("sqlalchemy: %s" % str(perf_counter()-a))
    #mariadb
    a=perf_counter()
    db_cnn1=get_db_conn_mariadb()
    print("maria: %s" % str(perf_counter()-a))    
    #mysql
    a=perf_counter()
    db_cnn2=get_db_conn_mysql()
    print("mysql: %s" % str(perf_counter()-a))
if __name__ == "__main__":
    #print(get_dailyupdatelist(datetime(2021,6,1)))
    #====== test_speed ===========
    #test_speed_db_connect()
    #test_speed_df()
    #print(get_port_member(['TEST']))
    #Example Setting
    #obj = Setting()
    #print(list(obj.get_setting_db_measure().keys())[:5])
    # print(list(obj.get_setting_func_measure().keys())[:5])
    # print(list(obj.get_setting_rule().keys())[:5])
    # print(list(obj.get_setting_rule_group().keys())[:5])
    # print(list(obj.get_setting_port_member().keys())[:5])
    #Example get_port_member
    # print(len(get_port_member(['TwSE'])))
    # print(len(get_port_member(['MV_TOP_100'])))
    # print(len(get_port_member(['TWSE','MV_TOP_100'])))
    #Example get_rule_group
    # print(get_rule_group(['rule_group_MS']))
    # print(get_rule_group(['rule_group_MS','rule_group_F']))
    #Example get_rebalance_date
    # print(get_periodic_date(datetime(2020,1,1),datetime(2020,12,31),0,1))
    #print(get_periodic_date(datetime(2020,1,31),datetime(2020,12,31),-1,1,1))
    # print(get_periodic_date(datetime(2014,12,31),datetime(2021,4,17),-1,12))
    # print(get_periodic_date(datetime(2018,12,31),datetime(2020,12,31),-1,12))
    # a = get_periodic_date(datetime(2019,12,31),datetime(2020,12,31),'FA',12)
    
    # b=get_basedate(datetime(2020,12,31),datetime(2022,12,31))
    # print(b)
    # blist=b.index.to_pydatetime()
    # print([elem in blist  for elem in a])
    #pandas.set_option('display.max_rows', None)
    #print(get_periodic_date(datetime(2015,12,31),datetime(2020,12,31),'FA',12))
    #print(get_periodic_date(datetime(2018,12,31),datetime(2020,12,31),15,1))
    #Example df_adj_basedate
    #print(get_basedate(datetime(2020,1,1),datetime(2020,1,10),2))
    # data1 = numpy.ones((20,5))
    # df1 = pandas.DataFrame(data1,index=pandas.date_range(datetime(2020,1,1), periods=20),columns=list('abcde'))
    # print(df_adj_basedate(df1,datetime(2020,1,1),datetime(2020,1,10),'TW',1,'bfill'))
    #Example get_watchlist
    # a= get_indistockdb_stock()
    # print(a.loc[a.index>='9100'])
    # watchlist = get_watchlist(['FG'],datetime(2014,12,31))
    #watchlist = get_watchlist('MV_TOP_300',datetime(2021,6,1))
    # watchlist = get_watchlist('MV_0_10',datetime(2020,12,31))
    # print(watchlist)
    # watchlist = get_watchlist('MV_10_20',datetime(2020,12,31))
    # print(watchlist)
    #print(get_measure_list('db_measure'))
    #print(get_watchlist(['MV_1_50'],[datetime(2008,12,31),datetime(2019,12,31),datetime(2020,12,31)]))
    #test()
    #class STOCK_SCREENER
    # sector_list = ['水泥工業','食品工業','農業科技','觀光事業','塑膠工業','建材營建','汽車工業','紡織纖維','貿易百貨','電機機械','生技醫療','電器電纜','化學工業','玻璃陶瓷','造紙工業','鋼鐵工業','橡膠工業','航運業','電子–電腦及週邊設備','電子–電子零組件','電子–半導體','電子–通信網路','電子–光電','電子–電子通路','電子–資訊服務','電子–其他電子','油電燃氣','金融保險','文化創意','電子商務','其他']
    # sector_excl_list = ['管理股票','存託憑證']
    # obj = STOCK_SCREENER(['CAP_TOP_30'],[datetime(2020,12,31)],**{'SECTOR':sector_excl_list,'IS_EXCL_SECTOR':1})
    # print(obj.get_stock_list())
    obj = STOCK_SCREENER(['FA'])
    print(obj.get_stock_list())