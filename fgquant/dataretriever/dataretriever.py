from datetime import datetime
import os
from pandas.core.frame import DataFrame
import quantlib
#from numpy.lib.function_base import append 
import pandas 
import numpy as NUMPY #若可能是setting_db_measure.json or setting_func_measure.json設定用到的libary需要用大寫
import talib as TALIB #若可能是setting_db_measure.json or setting_func_measure.json設定用到的libary需要用大寫
import copy,re
import logging
from time import perf_counter
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s : %(message)s')



class CTALIB():
    def RELATIVELEVEL(x1,timeperiod):
        x=pandas.Series(x1)
        return NUMPY.array((x-x.rolling(timeperiod).min())/(x.rolling(timeperiod).max()-x.rolling(timeperiod).min()))
    def SKEW(x1,timeperiod):
        from scipy.stats import skew
        return pandas.Series(x1).rolling(timeperiod).apply(skew,raw=True).values
    def SLOPE(x1,timeperiod):
        def calc_slope(x):
            slope = NUMPY.polyfit(range(len(x)), x, 1)[0]
            return slope
        return pandas.Series(x1).rolling(timeperiod).apply(calc_slope,raw=True).values
    def CV(x1,timeperiod):
        time_start = perf_counter()
        x=pandas.Series(x1)
        #log.debug("CTALIB.CV: %s" % (str(perf_counter()-time_start)))
        return NUMPY.array(x.rolling(timeperiod).mean()/x.rolling(timeperiod).std())
    def NI_REINVEST(NI,FA,LT_Invest,timeperiod):
        '''盈再率'''    
        df=pandas.concat([pandas.Series(NI), pandas.Series(FA),pandas.Series(LT_Invest)], axis=1)
        df.columns=['NI','FI','LT_Invest']
        y = TALIB.MOM(df['FI']+df['LT_Invest'],timeperiod)/TALIB.SUM(df['NI'],timeperiod)
        return y.values
    def CAPEX(FA, IA, depreciation,timeperiod):
        '''FA:Fixed Assets , IA:Intangible Assets'''
        df=pandas.concat([pandas.Series(FA), pandas.Series(IA), pandas.Series(depreciation)], axis=1)
        df.columns=['FA','IA','depreciation']
        y = TALIB.MOM(df['FA']+df['IA'],timeperiod) + TALIB.SUM(df['depreciation'],timeperiod)
        return y.values
    def DIST_TO_PERIODHIGH(close,timeperiod):
        x=pandas.Series(close)
        y=1-x/x.rolling(timeperiod).max()
        return y.values        
    def PERCENTILE(x1,timeperiod):
        #大約0.05秒,可與下方PERCENTILE_OLD舊版比較
        time_start = perf_counter()
        pct=True
        ascending = True #若x1值越大分數要越大則選True
        def to_rank(x):
            comp=NUMPY.less if ascending else NUMPY.greater
            if pct:
                return (NUMPY.sum(comp(x, x[-1]))+1)/len(x)
            else:
                return NUMPY.sum(comp(x, x[-1]))+1
        y= pandas.Series(x1).rolling(timeperiod).apply(to_rank,raw=True).values
        #logging.debug("CTALIB.PERCENTILE: %s" % (str(perf_counter()-time_start)))
        return y
    def PERCENTILE_OLD(x1,timeperiod):
        #此方式需耗費1.572秒
        #試圖pip install rollingrank套件,嘗試安裝MS C++ Build tool 2015多次皆失敗
        ascending = False
        time_start = perf_counter()
        x=pandas.Series(x1)
        pctrank = lambda x: pandas.Series(x).rank(method='min', pct=True,ascending=ascending).iloc[-1]
        y=x.rolling(timeperiod).apply(pctrank,raw=True)
        logging.debug("CTALIB.PERCENTILE_OLD: %s" % (str(perf_counter()-time_start)))
        return y
class PortDataRetriver():
    def __init__(self) -> None:
        pass
    def generator_dict_measure_matrix_from_indistockdb(self,
                                                    tickerlist: list, 
                                                    startdate: datetime, 
                                                    enddate:datetime, 
                                                    request_measure: list,
                                                    intbasedatetype: int = 2,
                                                    fillna_method = 'ffill'): 
        '''1.給watchlist與request_measure，傳回measure matrix(date-ticker)，若有多個request_meaure則傳回格式為dict，格式為{rd:measurematrix}
            2.以generator型式,是避免若資料太大會導致記憶體不足
        '''
        
        time_start = perf_counter()
        db_conn = quantlib.get_db_conn()
        ticker_measure_data ={}
        #取得watchlist中各tickr的measure，存入dict中{ticker:{measure1:series of measure1,measure2:series of measure2}}
        for ticker in tickerlist:
            time_start1 = perf_counter()
            obj = DataRetriever(ticker,db_conn)
            df =  obj.query(startdate, enddate,request_measure,request_measure,intbasedatetype,fillna_method)
            ticker_measure_data[ticker] = df.to_dict('series') 
            print("generator_dict_measure_matrix_from_indistockdb(%s): %s" % (ticker,str(round(perf_counter()-time_start1,2))))
        logging.debug("generator_dict_measure_matrix_from_indistockdb取得所有ticker資料時間:%s" % str(perf_counter()-time_start))
        for rd in request_measure:
            time_start1 = perf_counter()
            df_measure = pandas.DataFrame()
            for ticker in tickerlist:
                ticker_measure_data[ticker][rd].name=ticker
                df_measure_data=ticker_measure_data[ticker][rd].to_frame()
                if df_measure.empty:
                    df_measure = df_measure_data
                else:
                    df_measure= df_measure.join(df_measure_data,how='left')
            df_measure.reset_index(inplace = True) 
            df_measure['Date'] = pandas.to_datetime(df_measure['Date']).dt.date
            df_measure=df_measure.replace([NUMPY.inf, -NUMPY.inf], NUMPY.nan)
            #df_measure.index.name = rd
            logging.debug("generator_dict_measure_matrix_from_indistockdb整理df_measure資料時間(%s):%s" % (rd,str(perf_counter()-time_start1)))
            #print(df_measure)
            yield (rd,df_measure)
    def get_dict_measure_data (self,
                        tickerlist: list, 
                        startdate: datetime, 
                        enddate:datetime, 
                        request_measure: list, 
                        intbasedatetype: int = 2,
                        fillna_method = 'ffill'):   
        dict_measure_data={} 
        generator_measure_data = self.generator_dict_measure_matrix_from_indistockdb(tickerlist,startdate, enddate,request_measure,intbasedatetype,fillna_method)
        for (measure_name,df_measure) in generator_measure_data:
            dict_measure_data[measure_name] = df_measure
        return dict_measure_data 
    def port_price_matrix_for_bt(self,
                        tickerlist: list,
                        startdate: datetime, 
                        enddate: datetime,
                        intbasedatetype:int = 2):
        '''此為用來回測使用之價格資訊,必須特別處理過,
            1.產生N*M matrix (N: calandardate between startdate ~ endate , M: ticker list) 
           2. index = date    
           3.若有NaN則由後面往前補齊 (bfill)
        '''
        time_start = perf_counter()
        
        df_px = pandas.DataFrame()
        df_px_all = pandas.DataFrame()
        db_conn = quantlib.get_db_conn()
        for ticker in tickerlist:
            obj = DataRetriever(ticker,db_conn)
            df_px = obj.query(startdate,enddate,['Close','Volume'],[ticker,'Volume'],2,'bfill')
            #由於CM資料庫價格資料為ffill所以在此處理volume為0資料
            df_px[df_px['Volume'] < 0.001] = NUMPY.NaN

            series_px = df_px['Close']
            series_px.name = ticker
            if df_px_all.empty:
                df_px_all = series_px.to_frame()
            else:
                df_px_all=df_px_all.join(df_px[ticker],how='left')
            
        df_px_all.index.name = 'Date'
        df_px_all = df_px_all[tickerlist]
        df_px_all = quantlib.df_adj_basedate(df_px_all,startdate,enddate,'TW',intbasedatetype,'bfill')

        logging.debug("port_pricematrix: %s" % str(perf_counter()-time_start))
        return df_px_all.fillna(method = 'bfill')
    # def port_measure_matrix(self,
    #                     tickerlist: list, 
    #                     startdate: datetime, 
    #                     enddate:datetime, 
    #                     request_measure: list, 
    #                     intbasedatetype: int = 2,
    #                     fillna_method = 'ffill',
    #                     is_from_csv = True): 
    #     '''給watchlist與request_measure，傳回measurematrix，若有多個request_meaure則傳回格式為dict，格式為{rd:measurematrix}'''
    #     time_start = perf_counter()
    #     dict_measure_data={}
    #     for rd in request_measure:
    #         df_measure = pandas.read_csv(os.path.join(self.measure_data_folder_path, rd + '.csv'),index_col = 'Date')
    #         df_measure.index = map(pandas.to_datetime,df_measure.index)
    #         df_measure.index.name='Date'      
    #         df_measure = df_measure[tickerlist]
    #         dict_measure_data[rd] = df_measure
    #     logging.debug("PortDataRetriver.port_measure_matrix: %s" % str(perf_counter()-time_start))
    #     return dict_measure_data 

    def port_strategy_matrix(self,
                    tickerlist: list, 
                    startdate: datetime, 
                    enddate:datetime, 
                    strategy_list: list):
        '''取得port 的strategymatrix'''
        time_start = perf_counter()
        dict_strategy_df = {}
        para_strategy = {}
        for stg in strategy_list:
            dict_strategy_df[stg] = pandas.DataFrame()
            para_strategy[stg] = quantlib.get_strategy_rule_weighting(stg, [])
        all_rule,_ = quantlib.get_strategy_rule_weighting(strategy_list, [])
        for ticker in tickerlist:
            #取得所有rule
            df = corp_rulematrix(ticker, startdate, enddate,all_rule,2)
            #分strategy資料
            for stg in para_strategy:
                rule_list =para_strategy[stg][0]
                score = df[rule_list].sum(axis = 1)
                score.name=ticker
                #以下程式可能是python有bug,在跑到2014,2015的標的會出現很奇怪的錯誤，完全找不到問題,因此改用其他方式來結合
                #df_rule_rawdata[ticker] = a #df.multiply(rule_weighting,axis=1).sum(axis = 1)
                if dict_strategy_df[stg].empty:
                    dict_strategy_df[stg] = score.to_frame()
                else:
                    dict_strategy_df[stg]= dict_strategy_df[stg].join(score,how='left')
        logging.info("PortDataRetriver.port_strategy_matrix: %s" % str(perf_counter()-time_start))
        return dict_strategy_df    
class RULERETRIEVER():
    def __init__(self,Ticker: str,db_conn = None) -> None:
        self.dfname='df'
        self.Ticker = Ticker
        self.iserror = False
        self.errormsg = ''
        #取得rule_setting
        self.RuleProfile = quantlib.Setting().get_setting_rule()
        if db_conn == None:
            self.db_conn = quantlib.get_db_conn()
        else:
            self.db_conn = db_conn
    def sign(self,vgreaterless):
        #判別方向
        if vgreaterless.upper() == 'GREATER':
            return '>'
        elif vgreaterless.upper() == 'LESS':
            return '<'
        elif vgreaterless.upper() == 'EQUAL_GREATER':
            return '>='
        elif vgreaterless.upper() == 'EQUAL_LESS':
            return '<='
        elif vgreaterless.upper() == 'EQUAL':
            return '=='
        
    def greaterless(self,vgreaterless, measure, criteria):
        required_measure = [re.search(r"(?P<Measure>^.*?(?=_RETCOL_)|^.*)", measure).group('Measure')]
        #判別方向
        sign = self.sign(vgreaterless)
        if type(criteria) == str:
            required_measure.append(re.search(r"(?P<Measure>^.*?(?=_RETCOL_)|^.*)", criteria).group('Measure')) 
            rule = '(' + self.dfname + '["' + measure + '"]-' + self.dfname + '["' + criteria + '"]).apply(lambda x: 1 if x ' + sign + ' 0 else 0)'      
        else:
            rule = self.dfname + '["' + measure + '"].apply(lambda x: 1 if x '+ sign + str(criteria)+ ' else 0)'
        return  required_measure, rule
    def crossover(self,updown, measure, criteria):
        required_measure = [re.search(r"(?P<Measure>^.*?(?=_RETCOL_)|^.*)", measure).group('Measure')]
        if type(criteria) == str:
            required_measure.append(re.search(r"(?P<Measure>^.*?(?=_RETCOL_)|^.*)", criteria).group('Measure')) 
            if updown.upper() == 'UP':       
                rule = '((' + self.dfname + '["' + measure + '"]>' + self.dfname + '["' + criteria + '"]) & (' + self.dfname + '["' + measure + '"].shift(1) <' + self.dfname + '["' + criteria + '"])).apply(lambda x: 1 if x == True else 0)'        
            else:
                rule = '((' + self.dfname + '["' + measure + '"]<' + self.dfname + '["' + criteria + '"]) & (' + self.dfname + '["' + measure + '"].shift(1) >' + self.dfname + '["' + criteria + '"])).apply(lambda x: 1 if x == True else 0)'        
        else:
            if updown.upper() == 'UP':       
                rule = '((' + self.dfname + '["' + measure + '"]>' + str(criteria) + ') & (' + self.dfname + '["' + measure + '"].shift(1) <' + str(criteria) + ')).apply(lambda x: 1 if x == True else 0)'        
            else:
                rule = '((' + self.dfname + '["' + measure + '"]<' + str(criteria) + ') & (' + self.dfname + '["' + measure + '"].shift(1) >' + str(criteria) + ')).apply(lambda x: 1 if x == True else 0)'        

        return required_measure, rule
    def z(self,vgreaterless,  para_funcpara, measure , criteria):
        #判別方向
        sign = self.sign(vgreaterless)
        required_measure = [re.search(r"(?P<Measure>^.*?(?=_RETCOL_)|^.*)", measure).group('Measure')]
        rule = '((' + self.dfname + '["' + measure + '"] -' + self.dfname + '["' + measure + '"].rolling(' + para_funcpara + ').mean()) / ' + self.dfname + '["' + measure + '"].rolling(' + para_funcpara + ').std()).apply(lambda x: 1 if x ' + sign + str(criteria) +' else 0)'        
        return required_measure, rule

    def request_rule_parsing(self,Request_Rule: list):
        #將nest of list 變為 flat list & 去除重複
        Request_Rule_Adj = quantlib.flatten_nested_list(Request_Rule)
        result_required_measure = []
        result_exec_rule=[]
        for rl in Request_Rule_Adj:
            ruleid = rl
            measure = self.RuleProfile[rl.upper()]['measure']
            func = self.RuleProfile[rl.upper()]['func'] 
            criteria = self.RuleProfile[rl.upper()]['criteria']
            if func.upper() in ['GREATER','LESS','EQUAL_GREATER','EQUAL_LESS']:
                required_measure, exec_rule = self.greaterless(func.upper(), measure, criteria)
            elif func.upper() in ['CROSSOVER_UP','CROSSOVER_DOWN']:
                updown = 'UP' if func.upper() == 'CROSSOVER_UP' else 'DOWN'
                required_measure, exec_rule = self.crossover(updown, measure, criteria)
            elif func.upper().startswith('Z'): #in ['Z_GREATER','Z_LESS','Z_EQUAL_GREATER','Z_EQUAL_LESS']: 
                _r = re.search(r"[Zz]{1}_(?P<greaterless>.*)[(]{1}(?P<Para>.*)[)]{1}", func.upper())
                vgreaterless = _r.group('greaterless')
                para = _r.group('Para')
                required_measure, exec_rule =  self.z(vgreaterless.upper(),  para, measure ,criteria)
            #增加required_measure
            for rd in required_measure:
                result_required_measure.append(rd)
            #新增exec_rule
            result_exec_rule.append({'ruleid':ruleid,'exec_rule':exec_rule})
        return result_required_measure, result_exec_rule
    def query(self,
            StartDate: datetime,
            EndDate: datetime,
            Request_Rule: list, 
            Request_Rule_Alias: list = [], 
            is_incl_rawdata = False, 
            intbasedatetype:int = 0):
        try:
            time_start = perf_counter()
            df_result = pandas.DataFrame()
            #取得required_measure,exec_rule
            required_measure,exec_rule = self.request_rule_parsing(Request_Rule)
            #remove duplicates 
            required_measure_adj = list(dict.fromkeys(required_measure))

            #取得資料
            obj=DataRetriever(self.Ticker,self.db_conn)
            df = obj.query(StartDate,EndDate,required_measure_adj,[],0) # 此df名稱是根據 self.dfname,若要修改名稱同時也須修改  self.dfname
            result_measure = list(df.columns)
            if obj.iserror:
                raise Exception(obj.errormsg)
            if df.empty:
                raise Exception("DataRetriever returns empty dataframe.") 
            rulelist = []
            for exec_rl in exec_rule:
                rulelist.append(exec_rl['ruleid'])
                df[exec_rl['ruleid']] = eval(exec_rl['exec_rule'])   
            #處理combined-rule
            #i=1
            result_rl_colname = []
            for rd in Request_Rule:
                if type(rd) == list:
                    aliasname = '_&_'.join(rd)
                    df[aliasname] = df[rd].sum(axis=1).apply(lambda x: 1 if x == len(rd)  else 0)
                    result_rl_colname.append(aliasname)
                    #i =i + 1
                else:
                    result_rl_colname.append(rd)
            
            result_Aliasname = result_rl_colname if Request_Rule_Alias == [] else Request_Rule_Alias
            if is_incl_rawdata:
                result_rl_colname = result_measure + result_rl_colname
                result_Aliasname = result_measure + result_Aliasname
        #將欄位名稱按Request_Rule排序,並且改為DataAlias顯示
            if df.empty:
                df_result = pandas.DataFrame(columns=result_Aliasname)
            else:
                df_result = df[result_rl_colname]
                df_result.columns = result_Aliasname
            #檢查是否需要對齊calendarDate
            if intbasedatetype > 0:
                df_basedate= quantlib.get_basedate(StartDate, EndDate,intbasedatetype = intbasedatetype)
                df_result = pandas.merge_asof(df_basedate,df_result,left_index= True, right_index= True)
        except Exception as Err:
            self.iserror = True
            self.errormsg = Err
            log.error("RuleRetriever_query(%s): %s" % (self.Ticker,Err))
        finally:
            logging.info("RuleRetriever_query(%s): %s" % (self.Ticker,str(perf_counter()-time_start)))
            return df_result
class DataRetriever():
    def __init__(self, Ticker: str,db_conn = None):
        self.Ticker = Ticker
        self.TableName = str(Ticker)
        self.errormsg = ''
        self.iserror = False
        self.DBDataProfile = quantlib.Setting().get_setting_db_measure()
        self.FuncDataProfile = quantlib.Setting().get_setting_func_measure()
        self.AllDataProfile = quantlib.Setting().get_setting_all_measure()
        self.RuleDataProfile = quantlib.Setting().get_setting_rule()
        self.RuleGroupDataProfile = quantlib.Setting().get_setting_rule_group()

        
        if db_conn == None:
            self.db_conn = quantlib.get_db_conn(user='dataretriever')
        else:
            self.db_conn = db_conn

    def DBDataRetriever(self, StartDate: datetime, EndDate:datetime, Request_DBData: list = []):
        '''
        Return the dataframe indexed by date   
        1:僅根據Request_DBData負責取資料庫資料
        2.Request_DBData格式為list of db-colname 或 dict
        其中dict格式如下，其中必要之key為db_colname,flag_colname,measuretype,freq。
         {measure_name : {'db_colname': [],
                          'flag_colname': '',
                          'measuretype':'db',
                          'freq':d},...}    
        '''
        time_start = perf_counter()
        df_resultdata = pandas.DataFrame()
        #產生包含db colname的list
        list_Request_DBData =[]
        for rf in Request_DBData:
            if type(rf) == dict:
                list_Request_DBData.append(list(rf.keys())[0])
            else:
                list_Request_DBData.append(rf)
        #確保有DATE作為Index
        list_Request_DBData = list_Request_DBData + (['DATE'] if 'DATE' not in list_Request_DBData else []) 
        if list_Request_DBData != []:
            db_col=','.join([self.DBDataProfile[c]['db_colname'] + ' as ' + c for c in list_Request_DBData])
            _SQL='Select '+ db_col +' from `' + self.TableName  + '` where `Date` between "'+ StartDate.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'" order by `Date` asc'
            df_resultdata=  pandas.read_sql_query(_SQL,self.db_conn)
            df_resultdata['DATE'] = pandas.to_datetime(df_resultdata['DATE'])
            df_resultdata.set_index('DATE',inplace=True)
        logging.debug("DBDataRetriever(%s): %s" % (self.Ticker,str(perf_counter()-time_start)))
        return df_resultdata
    def FuncDataRetriever(self, StartDate: datetime, EndDate:datetime, Request_FuncData: list = []):
        '''Request_FuncData為list of dict,其中dict格式為
        func-measure:
         {measure_name : {'dependency': [],
                                'func': '',
                                'parameters': '',
                                'flag_colname': '',
                                'shift':0}}        
        '''
        time_start = perf_counter()
        result = pandas.DataFrame() 
        listdict_Request_FuncData = Request_FuncData
        #==========================================================================
        #整理要抓資料庫的欄位資料(為了只要query一次)
        required_dbfields=[]
        for fm in listdict_Request_FuncData:
            measure_name=list(fm.keys())[0]
            dep = fm[measure_name]['dependency']
            #取得dependency和他的flag_colname ex: MonthSales ,Flag_MonthSales
            for m in dep:
                required_dbfields.append(m) 
                m1=self.AllDataProfile[m].get('flag_colname','') #理論上只會有一個Flag_col
                if m1 != '':
                    required_dbfields.append(m1) 
        #remove duplicates required_dbfields
        required_dbfields = list(dict.fromkeys(required_dbfields))
        if '' in required_dbfields:required_dbfields.remove('')
        df_db_data = self.query(StartDate,EndDate,required_dbfields)
        #==========================================================================
        for rd_dict in listdict_Request_FuncData:
            try:  
                measure_name=list(rd_dict.keys())[0]
                if rd_dict[measure_name].get('flag_colname','') == '':#daily data
                    db_dependency = rd_dict[measure_name]['dependency']
                    func = rd_dict[measure_name]['func']
                    para_funcpara = rd_dict[measure_name]['parameters']
                    para_shift = rd_dict[measure_name].get('shift',0)
                    convert_data_dependency=','.join(['NUMPY.array(df_db_data["' + c + '"].fillna(method="ffill"))' for c in db_dependency])
                    #string example: talib.SMA(data_dependency['Close'], timeperiod = 5)
                    if func != '':
                        funcresult = eval(func + '('+ convert_data_dependency + ',' + para_funcpara + ')')
                    else:
                        funcresult = NUMPY.array(df_db_data[db_dependency[0]])
                    #若是talib傳回多個結果則是用tuple型態,ex:macd
                    #以下兩行是避免func of func 時確保有Date index
                    df_result = pandas.DataFrame(pandas.to_datetime(df_db_data.index),columns=['DATE'])
                    df_result.set_index('DATE',inplace=True)
                    if type(funcresult) is tuple: #表示有多個結果
                        colname=[measure_name + '_RETCOL_' + str(x+1) for x in range(len(funcresult))]
                        for i in range(len(colname)):
                            df_result[colname[i]] =NUMPY.transpose(funcresult[i])
                    else:
                        df_result[measure_name] = NUMPY.transpose(funcresult)
                    result = pandas.concat([result, df_result.shift(para_shift, axis = 0)], axis=1)
                else:
                    db_dependency = rd_dict[measure_name]['dependency']
                    flag_colname =rd_dict[measure_name]['flag_colname']
                    db_dependency_all = rd_dict[measure_name]['dependency'] + [flag_colname] 
                    func = rd_dict[measure_name]['func']
                    para_funcpara = rd_dict[measure_name]['parameters']
                    para_shift = rd_dict[measure_name].get('shift',0)
                    df_left = pandas.DataFrame(pandas.to_datetime(df_db_data.index),columns=['DATE'])
                    df_left.set_index('DATE',inplace=True)
                    df_right = df_db_data.loc[df_db_data[flag_colname] == 1][db_dependency_all]
                    #去除nan資料:因為TALIB運算若有nan則會全部傳回nan
                    df_right = df_right[~df_right.isnull().any(axis=1)]
                    if  df_right.size == 0:
                        raise Exception('df of ' + measure_name + ' is empty.')                    
                    else:
                        convert_data_dependency=','.join(['df_right["' + c + '"].to_numpy()' for c in db_dependency])
                        if func != '':
                            funcresult = eval(func + '('+ convert_data_dependency + ',' + para_funcpara + ')')
                        else:
                            funcresult = df_right[db_dependency].to_numpy()    
                        if  funcresult.size == 0:
                            raise Exception('df of ' + measure_name + ' is empty.')
                        else:
                            df_right[measure_name] = funcresult
                            #left join
                            df_result = df_left.join(df_right.shift(para_shift, axis = 0), how='left').fillna(method = 'ffill')   
                            #以下寫法是防止沒有result時,會出現date index格式錯誤  
                            result = df_result[measure_name].to_frame() if result.index.empty else pandas.concat([result, df_result[measure_name]], axis=1)
            except Exception as Err:
                #此錯誤是為了要處理新掛牌標的資料可能全為Nan，此時傳入talib會出現inputs are all Nan的錯誤，為了繼續計算其他func-measure因此多加了這段錯誤處理
                print("FuncDataRetriever Special Error(%s): %s" % (self.Ticker,Err))
                if df_db_data.index.empty == False: 
                    
                    if result.index.empty:
                        df_db_data[measure_name]=NUMPY.ones((len(df_db_data.index)))*NUMPY.nan
                        result = df_db_data[measure_name].to_frame() 
                    else:
                        result[measure_name] = NUMPY.ones((len(result.index)))*NUMPY.nan
                    continue
                else:
                    if result.index.empty:
                        result=pandas.DataFrame(columns=[measure_name])
                    else:
                        result[measure_name] = NUMPY.ones((len(result.index)))*NUMPY.nan
                    
        logging.debug("FuncDataRetriever(%s): %s" % (self.Ticker,str(perf_counter()-time_start)))
        return result
    def RuleDataRetriever(self, StartDate: datetime, EndDate:datetime, Request_RULEData: list = []):
        time_start = perf_counter()
        obj_rule = RULERETRIEVER(self.Ticker)
        Request_RULE = [list(x.keys())[0] for x in Request_RULEData]
        result = obj_rule.query(StartDate, EndDate,Request_RULE,[],False,0)
        logging.debug("RuleDataRetriever(%s): %s" % (self.Ticker,str(perf_counter()-time_start)))
        return result
    def RuleGroupDataRetriever(self, StartDate: datetime, EndDate:datetime, Request_RULEGROUP: list = []):
        time_start = perf_counter()
        obj_rule = RULERETRIEVER(self.Ticker)
        result = pandas.DataFrame()
        for rg in Request_RULEGROUP:
            rule_group_id = list(rg.keys())[0]
            Request_RULE = quantlib.get_rule_group_member(rule_group_id)
            df_result_rule = obj_rule.query(StartDate, EndDate,Request_RULE,[],False,0)
            result[rule_group_id] = df_result_rule.fillna(0).sum(axis=1)
        logging.debug("RuleGroupDataRetriever(%s): %s" % (self.Ticker,str(perf_counter()-time_start))) 
        return result       
    def transform_requestdata(self, request_data: list):
        '''將傳入request_data中每個measure轉成包含更多資訊的dict形式:有以下三類
         db-measure:完全是db-measure，且沒有額外func或shift資料
         {measure_name : {'db_colname': [],
                          'flag_colname': '',
                        'measuretype':'db',
                        'freq':d}}          
         func-measure:完全是func-measure，或是以db-measure利用func運算或shift資料
         {measure_name : {'dependency': [],
                                'func': '',
                                'parameters': '',
                                'flag_colname': '',
                                'measuretype':'func',
                                'shift':0}}
        err-measure:非以上兩類，表示此measure name是不合格式
         {measure_name : {'measuretype':'err'}}
            
        '''
        list_rd = []
        para_flag = ''
        #處理operator measure
        request_data_from_Operator= []
        for rd in request_data:
            _r = re.search(r"(?P<measure1>.*)(?P<Operator>[\*\/\+\-]{1})(?P<measure2>.*)", rd)
            if _r:
                measure1=_r.group('measure1').upper()
                measure2=_r.group('measure2').upper()
                if not measure1.isnumeric():
                    request_data_from_Operator.append(measure1)
                if not measure2.isnumeric():
                    request_data_from_Operator.append(measure2)

        #remove duplicates Request_Measure
        request_data = list(dict.fromkeys(request_data+request_data_from_Operator))
        if '' in request_data:request_data.remove('')
        for rd in request_data:
            #全部以大寫來判斷
            rd = rd.upper()
            para_operator = ''
            para_prev = 0
            para_func=''
            para_funcpara=''
            #判斷是否為operator_measure
            _r = re.search(r"(?P<measure1>.*)(?P<Operator>[\*\/\+\-]{1})(?P<measure2>.*)", rd)
            if _r: #operator_measure
                para_operator = _r.group('Operator') if _r else ''
                measure1=_r.group('measure1').upper()
                measure2=_r.group('measure2').upper()
            else:
                if '[' in rd: #使用公式產生func-measure
                    para_measurename = re.search(r"(?P<Measure>^.*?(?=\[)|^.*)", rd).group('Measure').upper() 
                    _r = re.search(r"\[.*[Pp]{1}:(?P<Prev>\d+).*\]", rd)
                    para_prev = int(_r.group('Prev'))  if _r else 0
                    _r = re.search(r"\[.*[Ff]{1}:(?P<Func>.*)[(]{1}(?P<FuncPara>.*)[)]{1}.*\]", rd) # 
                    para_func = _r.group('Func').upper()  if _r else ''
                    para_funcpara = _r.group('FuncPara')  if _r else ''
                    _r = re.search(r"(?P<Operator>[\*\/\+\-]{1})", rd)
                else:
                     para_measurename = rd

            if para_operator != '':
                measuresetting = {rd : {'dependency': [measure1, measure2],
                            'operator': para_operator,
                            'measuretype':'operator',
                            'shift':0}}
            elif para_measurename in self.DBDataProfile.keys():
                if rd in self.DBDataProfile.keys():#完全是DB-Measure
                    measuresetting = { rd : copy.deepcopy(self.DBDataProfile[para_measurename]) }
                    measuresetting[rd]['measuretype'] = 'db'
                else:#有func或shift
                    para_flag = self.DBDataProfile[para_measurename].get('flag_colname','')
                    measuresetting = {rd : {'dependency': [para_measurename],
                                'func': para_func,
                                'parameters': para_funcpara,
                                'flag_colname': para_flag,
                                'measuretype':'func',
                                'shift':para_prev}}
            #若para_measure是FuncData則使用FuncDataProfile的設定，僅能改變函數參數和prev days
            elif para_measurename in self.FuncDataProfile.keys():
                measuresetting = { rd :copy.deepcopy( self.FuncDataProfile[para_measurename]) }
                measuresetting[rd]['measuretype'] = 'func'
                if '[' in rd:
                    measuresetting[rd]['shift'] = para_prev 
                    measuresetting[rd]['parameters'] = para_funcpara if para_funcpara != '' else self.FuncDataProfile[para_measurename]['parameters']
            elif para_measurename in self.RuleDataProfile.keys():
                measuresetting = { rd :copy.deepcopy( self.RuleDataProfile[para_measurename]) }
                measuresetting[rd]['measuretype'] = 'rule'
            elif para_measurename in self.RuleGroupDataProfile.keys():
                measuresetting = { rd :{'ruleid':copy.deepcopy(self.RuleGroupDataProfile[para_measurename])} }
                measuresetting[rd]['measuretype'] = 'rulegroup'                
            else:
                measuresetting = {rd : {'measuretype':'err'}}  
                self.iserror = True
                self.errormsg =  self.errormsg + ('\n' if self.errormsg != '' else '') + 'request_measuer("' + para_measurename + '") is not in db-measure setting and func-measure setting'

            list_rd.append(copy.deepcopy(measuresetting)) 
        return list_rd  
    def output_std_unit_format(self,df_result):
        measure_profile = dict(**self.DBDataProfile,**self.FuncDataProfile)
        measure_profile_keys =  list(measure_profile.keys())
        for col in df_result.columns:
            if col.upper() in measure_profile_keys:
                col_type = measure_profile[col.upper()]['col_type']
                col_unit = measure_profile[col.upper()]['col_unit']
                if col_type in ('double','int'):
                    unit_number , unit = ('','') if col_unit == '' else col_unit.split("_") 
                    unit_number = 1 if unit_number == '' else float(unit_number)
                    df_result[col] = df_result[col] * unit_number
                    if unit == '%':
                        df_result[col] = df_result[col] / 100 
        return df_result
    def query(self, StartDate: datetime, EndDate:datetime, Request_Measure: list ,DataAlias : list =[], intbasedatetype:int = 0, fillna_method = None, is_std_unit_format:int = 0 ):
        '''
        intbasedatetype: 0:none 1:tradingdate 2:calendardate 若遇到nan則依據fillna_method設定
        '''
        try:
            time_start = perf_counter()
            #初始化
            requestmeasure_db = []
            requestmeasure_func = []
            requestmeasure_rule = []
            requestmeasure_rulegroup = []
            requestmeasure_operator = []
            requestmeasure_err = []
            db_result_df = pandas.DataFrame()
            func_result_df = pandas.DataFrame()
            rule_result_df = pandas.DataFrame()
            rulegroup_result_df = pandas.DataFrame()
            err_result_df = pandas.DataFrame()   
            df_result=pandas.DataFrame()  
            #with quanlib.get_db_conn_1() as db_connection:
            # if self.df_left.index.empty:
            #     a = perf_counter()
            #     db_cursor = self.db_conn.cursor()
            #     _SQL='Select DATE from `' + self.TableName  + '` where `Date` between "'+ StartDate.strftime('%Y%m%d') +'" and "'+ EndDate.strftime('%Y%m%d') +'" order by `Date` asc'
            #     db_cursor.execute(_SQL)
            #     self.df_left=pandas.DataFrame(db_cursor.fetchall(),columns=['DATE'])
            #     print("open cursor: %s" % str(perf_counter()-a))
            #     a = perf_counter()
            #     self.df_left['DATE'] = pandas.to_datetime(self.df_left['DATE'])
            #     self.df_left.set_index('DATE',inplace=True)
            #     print("to_datetime: %s" % str(perf_counter()-a))
            if DataAlias == []:
                DataAlias = Request_Measure
            #轉為大寫
            Request_Measure_UPPER = [ele.upper() for ele in Request_Measure]
            #初始化:放這此處當發生error時df_result才有欄位
            result_colname = Request_Measure_UPPER
            result_Aliasname = DataAlias
            #強制補上Date
            Request_Measure_Adj = Request_Measure_UPPER #+ (['DATE'] if 'DATE' not in Request_Measure_UPPER else []) 
            #remove duplicates Request_Measure
            Request_Measure_Adj = list(dict.fromkeys(Request_Measure_Adj))
            if '' in Request_Measure_Adj:Request_Measure_Adj.remove('')
            #transform Request_Measure to list of dict
            requestmeasure_detail = self.transform_requestdata(Request_Measure_Adj)
            #區分是db-measure、func-measure或err-measure
            for rd in requestmeasure_detail:
                measure_name = list(rd.keys())[0]
                if rd[measure_name]['measuretype'] == 'db':  
                    requestmeasure_db.append(rd)
                elif rd[measure_name]['measuretype'] == 'func':           
                    requestmeasure_func.append(rd)
                elif rd[measure_name]['measuretype'] == 'rule':           
                    requestmeasure_rule.append(rd)
                elif rd[measure_name]['measuretype'] == 'rulegroup':           
                    requestmeasure_rulegroup.append(rd)
                elif rd[measure_name]['measuretype'] == 'operator':
                    requestmeasure_operator.append(rd)
                else:
                    requestmeasure_err.append(rd)
            #取得資料:
            if requestmeasure_db!= []:
                db_result_df = self.DBDataRetriever(StartDate,EndDate,requestmeasure_db)
            if requestmeasure_func!= []:
                func_result_df = self.FuncDataRetriever(StartDate,EndDate,requestmeasure_func)
            if requestmeasure_rule!= []:
                rule_result_df = self.RuleDataRetriever(StartDate,EndDate,requestmeasure_rule)
            if requestmeasure_rulegroup!= []:
                rulegroup_result_df = self.RuleGroupDataRetriever(StartDate,EndDate,requestmeasure_rulegroup)
            #結合db_result_df, func_result_df:以下寫法是防止沒有db_result_df時,會出現date index格式錯誤   
            for df in [db_result_df,func_result_df,rule_result_df,rulegroup_result_df]:
                if not df.index.empty:     
                    df_result = pandas.concat([df_result, df], axis=1)
            #處理err_measure
            if requestmeasure_err!= []:
                for c in  requestmeasure_err:
                    measure_name=list(c.keys())[0]
                    df_result[measure_name] = NUMPY.zeros([df_result.index.size])*NUMPY.nan
            #計算operator measure
            for rd in requestmeasure_operator:
                measure_name = list(rd.keys())[0]
                measure1 = rd[measure_name]['dependency'][0] 
                measure2 = rd[measure_name]['dependency'][1]
                operator = rd[measure_name]['operator']
                if measure1.isnumeric():
                    df_result[measure_name] = eval( measure1 + operator + "df_result['" + measure2 + "']")                        
                elif measure2.isnumeric():
                    df_result[measure_name] = eval("df_result['" + measure1 + "']" + operator +  measure2)
                else:
                    df_result[measure_name] = eval("df_result['" + measure1 + "']" + operator + "df_result['" + measure2 + "']")
            #將Date設定為Index
            # df_result['DATE'] = pandas.to_datetime(df_result['DATE'])
            # df_result.set_index('DATE', drop= False, inplace=True)
            # df_result.index.name = 'DATE'
            #檢查是否有傳回多欄位:ex:macd會回傳三個欄位

            if  len(df_result.columns) > len(Request_Measure_Adj):
                Request_Measure_Alias_Dict = {a[0]:a[1] for a in zip(Request_Measure_UPPER,DataAlias)}
                result_colname = []
                result_Aliasname = []
                for rd in Request_Measure_UPPER:
                    if rd in df_result.columns:
                        result_colname.append(rd)
                        if DataAlias != []:
                            result_Aliasname.append(Request_Measure_Alias_Dict[rd])
                    else:
                        matching = [s for s in df_result.columns if rd + "_RETCOL_" in s]
                        for ele in matching:
                            result_colname.append(ele)
                            if DataAlias != []:
                                ext_col = re.search(r"(?P<SN>_RETCOL_\d+)", ele).group('SN') 
                                result_Aliasname.append(Request_Measure_Alias_Dict[rd]+ ext_col)

        except Exception as Err:
            logging.error("DataRetriever_query(%s): %s" % (self.Ticker,Err))
            self.iserror = True
            self.errormsg =  self.errormsg + ('\n' if self.errormsg != '' else '') + "DataRetriever_query(%s): %s" % (self.Ticker,Err)
        finally:
            #若request_measure有DATE欄位則回復Date成為column
            if 'DATE' in result_colname:
                df_result['DATE'] = df_result.index
            #將欄位名稱按Request_Measure排序,並且改為DataAlias顯示
            if df_result.empty:
                df_result = pandas.DataFrame(columns=result_Aliasname)
            else:
                df_result = df_result[result_colname]
                df_result.columns = result_Aliasname
            #檢查是否需要對齊basedate
            if intbasedatetype > 0:
                df_result = quantlib.df_adj_basedate(df_result,StartDate, EndDate,'TW',intbasedatetype = intbasedatetype, fillna_method = fillna_method)
            
            if is_std_unit_format:
                df_result = self.output_std_unit_format(df_result)
            logging.debug("DataRetriever_query:%s" % (perf_counter() - time_start))
            return df_result

if __name__ == "__main__":

    #========範例 1:===========================
    # 取得已定義之measure:包含db-measure、func-measure、rule-measure、rulegroup-measure , 若有回傳多欄則會以'FacotrName + _RETCOL_數字'作為欄位名稱(請試Price_MACD_12D_26D_9D) 
    #measure = ['Close','Price_MA_5D','單月合併月營收','Rule_MS_1','Rule_MS_2','rg_TEST_1','Price_MACD_12D_26D_9D']
    #========範例 2:===========================
    #取得已定義measure的前N期 :運算型式[P:前N期] ex:[P:1]
    #measure = ['Date','Close','Close[P:1]','Price_MA_5D','Price_MA_5D[P:1]','單月合併月營收','單月合併月營收[P:1]']
    #========範例 3:===========================
    #支援db-measure的函式運算(尚不支援func-measure) :運算型式[F:函式名稱(參數)] ex:[F:talib.SMA(5)]
    #可同時支援函式運算與前N期 ex: [P:Prev,F:talib.SMA(5)] 
    #measure = ['CLose','Price_MA_5D','Close[F:talib.SMA(5),P:1]','ClOSe[F:taLIb.BBANDS(20,2,2)]']  
    #========範例 4:===========================
    #若欲想改變Func-measure的參數，可先建立一個Func-measure的template,例如Price_MA
    #measure = ['Date','Close','Price_MA_5D','Price_MA[F:(5)]','Price_MA[F:(5),P:1]'] 
    #========範例 5:===========================
    #若可利用operator來取得資料
    #measure = ['Date','MonthSale_YoY','現金及約當現金/普通股股本'] 
    #========範例 6:===========================
    #可給欄為別名
    #measure = ['Date','Close','Price_MA_5D','Price_MA_5D[P:5]'] 
    #alias = ['日期','收盤','外資_五日','外資_五日_P5']
    #========範例 其他功能===========================
    #其他_1:會自動設定Date為Index
    #measure = ['Close'] 
    #measure = ['Date','Close'] 
    #其他_2:不分大小寫 
    #measure = ['Date','Close','CLOSE','Price_MA_5D','PriCE_MA_5D'] 
    #其他_3:若無資料則傳回NaN
    #measure = ['Date','CLOSE','CLOSE123']
    #其他_4:可利用obj.iserror or obj.errormsg來取得錯誤訊息
    
    #其他_5:以可變更日期，交易日或日曆日，要如何補資料則透過參數fillna_method來設定，預設是NONE-->表示NaN,'ffill'用前值補,'bfill'用後值補
        #交易日:intbasedatetype = 1 
        #日曆日:intbasedatetype = 2 
    #其他_6:可以調整輸出單位
        #若要輸出標準單位:is_std_unit_format = 1
    Ticker = '1101'
    alias = []
    intbasedatetype = 1
    fillna_method = 'ffill'
    is_std_unit_format = 1
    obj=DataRetriever(Ticker,db_conn=quantlib.get_db_conn()) 
    StartDate = datetime(2008,1,1)
    EndDate = datetime(2021,9,20)
    
    start_price = perf_counter()
    #measure = ['Ticker','CorpName','Close','Volume','Amount','日期_月營收_資料日','單月合併月營收','單月合併月營收/1000',]#'累計合併月營收','近1週1000張以上集保比率變動','10張以下佔集保比率','1000張以上佔集保比率','外資買賣超','外資買賣超金額','外資持股比率','外資持股成本','自營商買賣超_自行買賣','自營商持股比率','投信持股比率','日期_董監持股_資料日','董監及其關係人持股張數增減','董監及其關係人持股比例','董監及其關係人持股比例增減','大股東及其關係人持股張數增減','大股東及其關係人持股比例','大股東及其關係人持股比例增減','內部人及其關係人持股張數增減','內部人及其關係人持股比例','內部人及其關係人持股比例增減','全體及其關係人持股張數增減','全體及其關係人持股比例','全體及其關係人持股比例增減','董監及其關係人設質張數增減','董監及其關係人設質比例','董監及其關係人設質比例增減','內部人及其關係人設質張數增減','內部人及其關係人設質比例','內部人及其關係人設質比例增減','全體及其關係人設質張數增減','全體及其關係人設質比例','全體及其關係人設質比例增減','日期_財報_資料日','流動資產','現金及約當現金','短期投資合計','應收帳款與票據合計','存貨','採用權益法之投資','長期投資合計','不動產廠房及設備','資產總計','流動負債','非流動負債','負債總計','普通股股本','母公司業主權益','權益總計','原始每股淨值','公告每股淨值','銷貨收入淨額','營業毛利','營業外收入','營業外支出','稅前純益','稅後純益','綜合損益','母公司業主_稅後純益','母公司業主_綜合損益','EBITDA','公告基本每股盈餘','公告稀釋每股盈餘','原始每股稅前盈餘','原始每股稅後盈餘','原始每股綜合盈餘'] 
    #measure=['市值','權益總計','PB','市值/權益總計','普通股股本','流通在外股本','公告每股淨值',]#'Price_KD_9D_3D_3D', 'Price_MACD_12D_26D_9D']#, 'Price_MACD_12D_26D_9D[P:1]', 'Price_MACD_12D_26D_9D[P:2]', 'Price_MACD_12D_26D_9D[P:3]', 'Price_RSI_6D', 'Price_RSI_12D']
    #result= obj.query(StartDate, EndDate,['Close','Volume'],['2015','Volume'],2,'bfill')
    #measure = ['Ticker','CorpName','Close','Close_Prev_1','ROE4_AVG_3yr','ROE4_AVG_5yr','PB_RelativeLevel_3yr','PB_RelativeLevel_5yr','PB_RelativeLevel_10yr']
    #measure = ['ROE','ROE4','ROE4_AVG_5yr','Rule_MS_1','Rule_MS_2','rg_TEST_1']
    #measure = ['rg_MS_FG']+ quantlib.get_rule_group_member('rg_MS_FG')
    measure = ['日期_財報_公告日','GrossMargin_YoY','GrossMargin_YoY_next_1']
    result=obj.query(StartDate, EndDate, measure, alias, intbasedatetype,fillna_method,is_std_unit_format)
    #显示所有行
    #pandas.set_option('display.max_rows', None)
    #设置value的显示长度为100，默认为50
    #pandas.set_option('display.max_columns', None)
    #pandas.set_option('max_colwidth',100)
    #print(result.tail(100))
    print(result)
    result.to_csv("test.csv",encoding='utf-8-sig') 
    print("執行時間:%.2f" % (perf_counter()-start_price))
    print("錯誤訊息:%d(%s)" % (obj.iserror,obj.errormsg))
    
# def output_verified_data():
#     #=======DataRetriever檢核程式 配合InvestQuant\dataretriever驗證程式.xlsm============================
#     #取得db-measure作為後續驗證資料用
#     measure_db = {'Close':'收盤價',
#                  'Flag_月營收':'Flag_月營收',
#                  '單月合併月營收':'月營收',
#                  '市值':'市值',
#                  '普通股股本':'普通股股本'}
#     #取得已定義之func-measure:利用talib或ctalib
#     measure_func = {'Price_MA_5D':'驗證日資料Talib功能',
#                     'Price_Percentile_1M_test':'驗證日資料CTalib功能',
#                     'Price_BBANDS_20D_2std':'驗證Talib回傳多個欄位',
#                     'MonthSale_MoM':'驗證非日資料Talib',
#                     'MonthSale_CV_1yr_test':'驗證非日資料CTalib'}
#     #驗證函式運算公式
#     measure_func1 = {'Close[F:talib.SMA(5)]':'驗證Price_AVG_5D之函數運算',
#                'Price_MA[F:(5),P:1]':'驗證Price_AVG_5D之函數運算,利用template func',
#                'ClOSe[F:taLIb.BBANDS(20,2,2)]':'驗證Price_MACD_12D_26D_9D之函數運算'
#                }
#     #驗證func of func
#     measure_func2 = {'MonthSale_MoM_SMA5_test':'驗證非日資料之func of func',
#                      'Price_Return_5D_SMA5_test':'驗證日資料之func of func'}
#     #驗證operator
#     measure_operator = {'close*普通股股本':'驗證operator',
#                         '市值/單月合併月營收':'驗證operator'}

#     #驗證shiftP功能   
#     measure_shift = {'Price_MA_5D[P:1]':'驗證日資料Talib+shift',
#                     'Price_Percentile_1M_test[P:1]':'驗證日資料CTalib+shift',
#                     'MonthSale_MoM[P:1]':'驗證非日資料Talib+shift',
#                     'MonthSale_CV_1yr_test[P:1]':'驗證非日資料CTalib+shift'}  
#     #測試measure-err
#     measure_err = {'CLOSE123':'驗證error'}

#     measurelist =list(measure_db.keys())+list(measure_func.keys())+list(measure_func1.keys())+list(measure_func2.keys())+list(measure_operator.keys())+list(measure_shift.keys())+list(measure_err.keys())
#     #measurelist=list(measure_func2.keys()) + list(measure_shift.keys())
#     #-----run--------------
#     pandas.set_option('display.max_rows', None)
#     Ticker = '1101'
#     alias = []
#     intbasedatetype = 0
#     fillna_method = 'ffill'
#     obj=DataRetriever(Ticker) 
#     StartDate = datetime(2015,1,1)
#     EndDate = datetime(2021,10,31)
#     result=obj.query(StartDate, EndDate, measurelist, alias, intbasedatetype,fillna_method)
#     print("錯誤訊息:%d(%s)" % (obj.iserror,obj.errormsg))
    
#     print(result.head(100))
#     import os
#     result.to_csv(os.path.join(os.path.dirname(__file__), "dataretriever_verified_data.csv"),sep=';')
# if __name__ == "__main__":
#     output_verified_data()