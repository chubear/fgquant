import os,sys
from pydoc import describe
from xmlrpc.client import DateTime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dataretriever import *
import quantlib
from datetime import datetime,timedelta
import numpy
import pandas as pd
from time import perf_counter
import json
from dataretriever import *
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import random
import logging as log
import empyrical as ep
import seaborn as sns
import matplotlib.ticker as mtick
import matplotlib.pyplot as plt 
from matplotlib import cm
import pyfolio.timeseries
import pyfolio as pf
from sklearn.linear_model import LinearRegression
import scipy.stats

plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']  #中文字型 (前置需電腦設定https://medium.com/marketingdatascience/%E8%A7%A3%E6%B1%BApython-3-matplotlib%E8%88%87seaborn%E8%A6%96%E8%A6%BA%E5%8C%96%E5%A5%97%E4%BB%B6%E4%B8%AD%E6%96%87%E9%A1%AF%E7%A4%BA%E5%95%8F%E9%A1%8C-f7b3773a889b)
sns.set_style("whitegrid")
log.basicConfig(level=log.INFO, format='%(asctime)s - %(levelname)s : %(message)s')
class PORT_DATA():
    #預設measure data資料夾路徑
    def __init__(self,startdate, enddate, stockpool_name,strategyid) -> None:
        self.startdate =startdate
        self.enddate = enddate
        self.stockpool_name = stockpool_name
        self.strategyid= strategyid
        #預設路徑
        self.bt_result_data_folder_path = os.path.join(os.getcwd(),'port_backtest','bt_result_data')
        self.measure_folder_path = os.path.join(self.bt_result_data_folder_path ,'port_measure_data')
        self.rule_folder_path = os.path.join(self.bt_result_data_folder_path ,'port_rule_data')
        self.price_matrix_for_bt_path = os.path.join(self.bt_result_data_folder_path,'port_price_matrix_for_bt.csv')
        #取得資料
        self.strategy_profile = quantlib.Setting().get_setting_strategy()[strategyid.upper()]
        self.strategy_list = self.strategy_profile['measure']
        self.strategy_direction_list = self.strategy_profile['measure_dir']
        self.strategy_weighting_list = self.strategy_profile['weighting']
        #資產池預設預設參數
        self.stockpool_rebalancedate_list = quantlib.get_periodic_date(startdate-timedelta(days=1),enddate,-1,12)
        self.sector_excl_list = ['管理股票','存託憑證']
        self.stockpool_tickerlist = self.get_stockpool_list(stockpool_name)
    def get_stockpool_list(self, stockpool_name):
        obj = quantlib.STOCK_SCREENER(stockpool_name,self.stockpool_rebalancedate_list,**{'SECTOR':self.sector_excl_list,'IS_EXCL_SECTOR':1})
        return obj.get_stock_list()
    def get_bt_date_list():
        pass
    def port_price_matrix_from_csv(self,
                        tickerlist: list,
                        startdate: datetime, 
                        enddate: datetime,
                        intbasedatetype:int = 2):
        '''產生回測使用之價格資訊,必須特別處理過(calendar date、bfill),
           1.產生N*M matrix (N: calandardate between startdate ~ endate , M: ticker list) 
           2. index = date    
           3.若有NaN則由後面往前補齊 (bfill)
        '''
        time_start = perf_counter()
        df_px_all = pandas.read_csv(self.price_matrix_for_bt_path,index_col = 'Date')   
        df_px_all.index = map(pandas.to_datetime,df_px_all.index)
        df_px_all.index.name = 'Date'

        # tickerlist = set(tickerlist)-set(['6790', '6768', '4770', '4440', '3714', '3357', '2753', '6770', '2250', '6747', '5222', '6792', '2945'])
        # tickerlist=list(tickerlist)
        df_px_all = df_px_all[tickerlist]
        df_px_all = quantlib.df_adj_basedate(df_px_all,startdate,enddate,'TW',intbasedatetype,'bfill')

        logging.debug("port_pricematrix: %s" % str(perf_counter()-time_start))
        return df_px_all.fillna(method = 'bfill')
    def get_measure_matrix_from_csv(self,
                        tickerlist , 
                        startdate , 
                        enddate:datetime, 
                        request_measure: list, 
                        intbasedatetype: int = 2,
                        fillna_method = 'ffill'): 
        '''由已經產生好的csv檔，傳回measurematrix，若有多個request_meaure則傳回格式為dict，格式為{rd:measurematrix}'''
        time_start = perf_counter()
        dict_measure_data={}
        for rd in request_measure:
            df_measure = pandas.read_csv(os.path.join(self.measure_folder_path, rd + '.csv'),index_col = 'Date')
            df_measure.index = map(pandas.to_datetime,df_measure.index)
            df_measure.index.name='Date'      
            df_measure = df_measure[tickerlist]
            dict_measure_data[rd] =  quantlib.df_adj_basedate(df_measure,startdate,enddate,'TW',intbasedatetype,fillna_method)
        logging.debug("PORT_DATA.get_measure_matrix: %s" % str(perf_counter()-time_start))
        return dict_measure_data 
    def port_scorematrix_by_measure(self,
                                tickerlist = None, 
                                startdate = None, 
                                enddate = None, 
                                request_measure = None,
                                measure_rank_direction = None,
                                measure_weighting = None):
        '''給定tickerlist、measure、measure_rank_direction，將各measure按measure_rank_direction排序，然後將每個measure的rank轉成0~100後加總得到總分,分數越高代表越好。
        measure_rank_direction:list數量需與request_measure相同。若是數值越小越好則設為0,越大越好則設為1。預設為1
        measure_weighting:list數量需與request_measure相同。預設為equal-weight
        rank設定:若measure為nan則該rank設為100(na_option='bottom')'''
        tickerlist = self.stockpool_tickerlist if tickerlist == None else tickerlist
        startdate = self.startdate if startdate== None else startdate
        enddate = self.enddate if enddate== None else enddate
        request_measure = self.strategy_list if request_measure== None else request_measure
        measure_rank_direction = self.strategy_direction_list if measure_rank_direction== None else measure_rank_direction
        measure_weighting =self.strategy_weighting_list if measure_weighting== None else measure_weighting


        time_start = perf_counter()
        #按各measure組成NxM矩陣(Date x Ticker)
        dict_measure_rawdata={}
        dict_measure_rawdata = self.get_measure_matrix_from_csv(tickerlist,startdate, enddate,request_measure)
        #然後measure大小排序，存成{measure:df of measure}並計算成0~100之分數
        dict_measure_rankpct = {}
        for rd,dir in zip(request_measure,measure_rank_direction):
            df = dict_measure_rawdata[rd]
            dict_measure_rankpct[rd] = df.rank(axis=1, method='max',na_option='keep',pct=True,ascending= dir) *100
        #按照measure weighting計算weighted-ranking score
        df_result_measure_rankpct = pandas.DataFrame()
        for rd in list(dict_measure_rankpct.keys()):
            #取得weighting,預設為equal-weighted
            rd_weighting = 1/len(request_measure) if measure_weighting ==[] else measure_weighting[request_measure.index(rd)]
            if df_result_measure_rankpct.empty:
                df_result_measure_rankpct = dict_measure_rankpct[rd] * rd_weighting
            else:
                df_result_measure_rankpct = df_result_measure_rankpct.add(dict_measure_rankpct[rd] * rd_weighting)
        log.info("PORT_DATA.port_scorematrix_by_measure: %s" % str(perf_counter()-time_start))
        return df_result_measure_rankpct,dict_measure_rawdata #傳回最終分數結果與中間計算所使用的原始資料

    def get_rule_group_matrix_from_csv(self,
                    tickerlist: list, 
                    startdate: datetime, 
                    enddate:datetime, 
                    request_rule_group: list):
        '''與port_measure_matrix相同功能。給tickerlist與request_rule_group，傳回rule_group_matrix，傳回格式為dict，格式為{rule_group_name:rule_group_matrix}'''
        def corp_rule_matrix_from_csv(ticker, 
                        startdate: datetime, 
                        enddate:datetime, 
                        request_rule: list): 
            '''取得單一標的之rule matrix,會取得csv資料'''
            #處理combined rule名稱
            request_rule_alias = quantlib.get_request_rule_alias(request_rule) 
            df = pandas.read_csv(os.path.join(self.rule_folder_path,ticker +'csv') ,index_col = 'Date')
            df.index = map(pandas.to_datetime,df.index)
            df.index.name='Date'     
            df = df[request_rule_alias]
            df = quantlib.df_adj_basedate(df,startdate,enddate,'TW',2,'ffill')
            return df
        time_start = perf_counter()
        dict_rule_group_df = {}
        para_rule_group = {}
        for rg in request_rule_group:
            dict_rule_group_df[rg] = pandas.DataFrame()
            para_rule_group[rg] = quantlib.get_rule_group_rule_member_and_weighting(rg, [])
        all_rule,_ = quantlib.get_rule_group_rule_member_and_weighting(request_rule_group, [])
        for ticker in tickerlist:
            #取得所有rule
            df_rule = corp_rule_matrix_from_csv(ticker, startdate, enddate, all_rule)
            #分rule_group資料
            for rg in para_rule_group:
                rule_list =para_rule_group[rg][0]
                score = df_rule[rule_list].sum(axis = 1)
                score.name=ticker
                #以下程式可能是python有bug,在跑到2014,2015的標的會出現很奇怪的錯誤，完全找不到問題,因此改用其他方式來結合
                #df_rule_rawdata[ticker] = a #df.multiply(rule_weighting,axis=1).sum(axis = 1)
                if dict_rule_group_df[rg].empty:
                    dict_rule_group_df[rg] = score.to_frame()
                else:
                    dict_rule_group_df[rg]= dict_rule_group_df[rg].join(score,how='left')
        log.info("PORT_DATA.get_rule_group_matrix_by_rule: %s" % str(perf_counter()-time_start))
        return dict_rule_group_df
    def port_scorematrix_by_rule_group(self,
                        tickerlist: list, 
                        startdate: datetime, 
                        enddate:datetime, 
                        rule_group_list: list,
                        rule_group_list_weighting = []):
        time_start = perf_counter()
        dict_rg_rawdata = {}
        df_rule_rankpct=pandas.DataFrame()
        df_total = pandas.DataFrame()
        if rule_group_list_weighting == []:
            rule_group_list_weighting = [1/len(rule_group_list)] * len(rule_group_list)
        #分rule_group資料
        dict_rule_group_df = self.get_rule_group_matrix_from_csv(tickerlist,startdate,enddate,rule_group_list)
        for rg in rule_group_list:
            df =dict_rule_group_df[rg]
            dict_rg_rawdata[rg] = df.loc[startdate:enddate,tickerlist]
            rg_weighting = rule_group_list_weighting[rule_group_list.index(rg)]
            df_rg=df[tickerlist] * rg_weighting
            df_total = df_total.add(df_rg,fill_value=0)
        dict_rg_rawdata['total'] = df_total
        df_rule_rankpct = df_total.rank(axis=1, method='max',na_option='keep',pct=True,ascending= True) *100
        df_rule_rankpct.index.name='Date'
        log.info("port_scorematrix_by_rule: %s" % str(perf_counter()-time_start))
        return df_rule_rankpct,dict_rg_rawdata 
    def port_signalmatrix(self,
                          port_scorematrix_rankpct_entry: pandas.DataFrame,
                          bt_ranking_para_entry : tuple,
                          port_scorematrix_rankpct_exit :pandas.DataFrame ,
                          bt_ranking_para_exit : tuple):
        #買入訊號
        bt_ranking_start_entry = int(bt_ranking_para_entry[0])
        bt_ranking_end_entry = int(bt_ranking_para_entry[1])
        
        df_ranking_entry = port_scorematrix_rankpct_entry.rank(axis=1, method='average',na_option='keep',pct=False, ascending = False)#分數越高,排序越小
        mask=(df_ranking_entry>=bt_ranking_start_entry) & (df_ranking_entry<=bt_ranking_end_entry)
        df_signal_entry = df_ranking_entry.where(mask,0).mask(mask,1)
        #賣出訊號
        bt_ranking_start_exit = int(bt_ranking_para_exit[0])
        bt_ranking_end_exit = int(bt_ranking_para_exit[1])

        df_ranking_exit = port_scorematrix_rankpct_exit.rank(axis=1, method='average',na_option='keep',pct=False, ascending = False)#分數越高,排序越小
        mask=(df_ranking_exit>=bt_ranking_start_exit) & (df_ranking_exit<=bt_ranking_end_exit)
        df_signal_exit = df_ranking_exit.where(mask,0).mask(mask,-1)
        #df_signal_exit.to_csv('test.csv')
        df_signal_result = df_signal_entry + df_signal_exit
        #df_signal_result = df_signal_result.mask(df_signal_result==-1,0)
        return df_signal_result
    def port_scorerankingmatrix(self,
                                port_scorematrix, 
                                selective_date = None,
                                extra_measurematrix_rankpct = None):
        '''利用scorematrix，(可考慮亂數或measure調整)傳回標的排名，若有給selective_date則僅傳回selective_date結果，若沒有就針對整個port_scorematrix計算
        extra_measurematrix_rankpct:用來作為當有相同分數時的第二排序的measure，其數值為0~100,越大表示越好。若給None則利用亂數排序。
        '''
        
        time_start = perf_counter()
        if selective_date ==None:
            selective_date = port_scorematrix.index
 
        numpy.random.seed(10)
        df_score_adjust = pandas.DataFrame(columns=port_scorematrix.columns)
        df_score_final= pandas.DataFrame(columns=port_scorematrix.columns)
        df_score_rank= pandas.DataFrame(columns=port_scorematrix.columns)
        #計算各rebalance date的投資標的
        for date1 in selective_date:
            if extra_measurematrix_rankpct == None:
                score_adjust= numpy.random.rand(len(port_scorematrix.columns))*0.01  
            else:
                score_adjust = extra_measurematrix_rankpct.loc[(extra_measurematrix_rankpct.index<=date1)].iloc[-1]*0.0001
            #此處要注意:Series1+Series2會依照index相加,若Series1+np.array因為np.array沒有index,所以不會有問題,但是若Series1+Series(np.array),會有問題，Series(np.array)的index為0,1,2..,因此會與Series1的index變成聯集
            score_final = port_scorematrix.loc[date1] + score_adjust
            score_final.name=date1

            df_score_final = df_score_final.append(score_final)
            df_score_adjust = df_score_adjust.append(pandas.Series(score_adjust,name=date1,index=df_score_adjust.columns) if type(score_adjust) == numpy.ndarray else score_adjust)

        df_score_final.index.name='Date'
        
        df_score_adjust.index.name='Date'
        df_score_original = port_scorematrix.loc[selective_date]
        df_score_rank = df_score_final.rank(axis=1, method='average',na_option='keep',pct=False, ascending = False)#分數越高,排序越小
        log.info("PORT_DATA.port_scoreranking_matrix: %s" % str(perf_counter()-time_start))
        return df_score_original,df_score_adjust,df_score_final,df_score_rank

    def get_effective_ticker(self, port_scorematrix1, stockpool_name, stockpool_rebalance_date_list):
        #處理讓不在該期的watchlist的分數為nan
        enddate = list(port_scorematrix1.index)[-1]
        for i in range(len(stockpool_rebalance_date_list)):
            date1 = stockpool_rebalance_date_list[i]
            date2 = stockpool_rebalance_date_list[i+1]  if i < (len(stockpool_rebalance_date_list)-1) else enddate
            obj = quantlib.STOCK_SCREENER(stockpool_name, date1, **{'SECTOR':self.sector_excl_list,'IS_EXCL_SECTOR':1})
            watchlist1 = obj.get_stock_list()
            #watchlist1 = quantlib.get_watchlist(stockpool_name, date1)
            not_int_watchlist = [ticker for ticker in port_scorematrix1.columns if ticker not in watchlist1]
            port_scorematrix1.loc[date1 + timedelta(days = 1):date2,not_int_watchlist] = numpy.nan
        return port_scorematrix1
class PORT_GT_ANALYSIS():
    def __init__(self, strategyname, df_indistock_detail: pandas.DataFrame,df_grouped_detail: pandas.DataFrame,df_grouped_final: pandas.DataFrame):
        self.strategyname = strategyname 
        #確保index
        self.df_indistock_detail = df_indistock_detail
        self.df_indistock_detail.reset_index(inplace=True)
        self.df_indistock_detail.set_index('date',inplace=True)
        self.df_indistock_detail.index=pandas.to_datetime(df_indistock_detail.index)
        
        self.df_grouped_detail = df_grouped_detail
        self.df_grouped_detail.reset_index(inplace=True)
        self.df_grouped_detail.set_index('date',inplace=True)
        self.df_grouped_detail.index=pandas.to_datetime(df_grouped_detail.index)
        
        self.df_grouped_final = df_grouped_final
        self.df_grouped_final.reset_index(inplace=True)
        self.df_grouped_final.set_index('group',inplace=True)

        self.datelist = list(self.df_indistock_detail.index.drop_duplicates())
        self.num_group = len(self.df_grouped_final.index)
        self.group_order = ['group_' + str(k) for k in range(1,self.num_group+1)]
    def calc_slope_excess_return_vs_score_all_data(self, return_period_list=[1,3,6,12],score_filter = (0,100)):
        '''根據gt_indistock_detail,計算所有資料(不分date)與return_period_list計算slope'''
        #計算score vs return
        df_result = pd.DataFrame()
        dict_result={}
        for k in return_period_list:
            col_name = 'excess_return_' + str(k) + 'm'
            df= self.df_indistock_detail[['score','group',col_name]].dropna(axis=0,how='any')
            df=df[(df['score']>=score_filter[0]) & (df['score']<=score_filter[1])]
            regressor = LinearRegression()
            x=df['score'].values.reshape(-1,1) 
            y=df[col_name].values.reshape(-1,1) 
            try:
                regressor.fit(x, y)
                b = regressor.coef_[0][0] * 12/k
            except:
                b = numpy.nan

            dict_result[ str(k) + 'm'] = b
        s = pd.Series(dict_result)
        return s
    def get_num_group_1_excess_return_greater_0(self, datelist = [], return_period_list=[1,3,6,12]):
        '''計算根據df_indistock_detail中 group 1 超額報酬大於0的比例'''
        pass
    def get_df_detail_r_squared_excess_return_vs_score(self, datelist = [], return_period_list=[1,3,6,12],score_filter = (0,100)):
        '''根據gt_indistock_detail,給定datelist與return_period_list計算slope
        datelist:[]表示全部日期'''
        if datelist==[]:
            datelist = self.datelist
        #計算score vs return
        df_result = pd.DataFrame()
        for date1 in datelist:
            dict_result={}
            for k in return_period_list:
                col_name = 'excess_return_' + str(k) + 'm'
                df= self.df_indistock_detail.loc[date1,['score','group',col_name]]
                df=df[(df['score']>=score_filter[0]) & (df['score']<=score_filter[1])]
                regressor = LinearRegression()
                x=df['score'].values.reshape(-1,1) 
                y=df[col_name].values.reshape(-1,1) 
                try:
                    regressor.fit(x, y)
                    # mse = numpy.mean((regressor.predict(x) - y) ** 2)
                    r_squared = regressor.score(x, y)
                    #adj_r_squared = r_squared - (1 - r_squared) * (x.shape[1] / (x.shape[0] - x.shape[1] - 1))
                except:
                    r_squared = numpy.nan
                dict_result[ str(k) + 'm'] = r_squared
            s = pd.Series(dict_result, name = date1)
            df_result= df_result.append(s)
        return df_result
    def get_f_anova(self, datelist = [], return_period_list=[1,3,6,12]):
        if datelist==[]:
            datelist = self.datelist
        #計算score vs return
        df_result = pd.DataFrame()
        for date1 in datelist:
            dict_result={}
            for k in return_period_list:
                col_name = 'excess_return_' + str(k) + 'm'
                df= self.df_indistock_detail.loc[date1,['score','group',col_name]]
                #df= self.df_indistock_detail[['score','group',col_name]]
                data_1=df[df['group']=='group_10'][col_name].values#.reshape(-1,1) [df['group']=='group_2']
                data_1=numpy.random.normal(10,20,200)
                #print(scipy.stats.shapiro(data_1))
                print(scipy.stats.kstest(data_1,'norm'))
        #             b=regressor.coef_[0][0] * 12/k
        #             # mse = numpy.mean((regressor.predict(x) - y) ** 2)
        #             # r_squared = regressor.score(x, y)
        #             #adj_r_squared = r_squared - (1 - r_squared) * (x.shape[1] / (x.shape[0] - x.shape[1] - 1))
        #         except:
        #             b = numpy.nan
        #         dict_result[ str(k) + 'm'] = b
        #     s = pd.Series(dict_result, name = date1)
        #     df_result= df_result.append(s)
        # return df_result
    def get_df_detail_slope_excess_return_vs_score(self, datelist = [], return_period_list=[1,3,6,12],score_filter = (0,100)):
        '''根據gt_indistock_detail,給定datelist與return_period_list計算slope
        datelist:[]表示全部日期'''
        if datelist==[]:
            datelist = self.datelist
        #計算score vs return
        df_result = pd.DataFrame()
        for date1 in datelist:
            dict_result={}
            for k in return_period_list:
                col_name = 'excess_return_' + str(k) + 'm'
                df= self.df_indistock_detail.loc[date1,['score','group',col_name]]
                df=df[(df['score']>=score_filter[0]) & (df['score']<=score_filter[1])]
                regressor = LinearRegression()
                x=df['score'].values.reshape(-1,1) 
                y=df[col_name].values.reshape(-1,1) 
                try:
                    regressor.fit(x, y)
                    b=regressor.coef_[0][0] * 12/k
                    # mse = numpy.mean((regressor.predict(x) - y) ** 2)
                    # r_squared = regressor.score(x, y)
                    #adj_r_squared = r_squared - (1 - r_squared) * (x.shape[1] / (x.shape[0] - x.shape[1] - 1))
                except:
                    b = numpy.nan
                dict_result[ str(k) + 'm'] = b
            s = pd.Series(dict_result, name = date1)
            df_result= df_result.append(s)
        return df_result
    def gt_perf_stats_level_1(self):
        return_period_list=[1,3,6,12]
        df_result = DataFrame()
        #======ALL sample====================
        df = self.get_df_detail_slope_excess_return_vs_score([],return_period_list)
        #avg_slope_all
        s=pd.Series(self.calc_slope_excess_return_vs_score_all_data(return_period_list), name = 'avg_slope_all')
        df_result = df_result.append(s)
        #count
        s = pd.Series(df.count(axis=0), name = 'num_sampling_date')
        df_result = df_result.append(s)
        #avg_r-square
        s = pd.Series(self.get_df_detail_r_squared_excess_return_vs_score([],return_period_list).mean(axis=0), name ='avg_r_square')
        df_result = df_result.append(s)
        #avg_slope        
        s1 = pd.Series(df.mean(axis=0), name ='avg_slope')
        df_result = df_result.append(s1)
        #std_slope
        s2 =pd.Series( df.std(axis=0), name ='std_slope')
        df_result = df_result.append(s2)
        #cv
        s=pd.Series(s1/s2, name ='cv_slope')
        df_result = df_result.append(s)
        #prob_postive_slope
        s = pd.Series(df.where(df>0).count(axis=0) /df.count(axis=0), name = 'prob_positive_slope')
        df_result = df_result.append(s)

        index1 = [['all_level_1']*len(df_result.index),list(df_result.index)]
        df_result.set_index(index1,inplace=True)
        #======score>30 sample====================
        df_result1 = DataFrame()
        df = self.get_df_detail_slope_excess_return_vs_score([],return_period_list,(30,100))
        #avg_slope_all
        s=pd.Series(self.calc_slope_excess_return_vs_score_all_data(return_period_list,(30,100)), name = 'avg_slope_all')
        df_result1 = df_result1.append(s)
        #avg_slope        
        s = pd.Series(df.mean(axis=0),name = 'avg_slope')
        df_result1 = df_result1.append(s)
        #std_slope
        s =pd.Series( df.std(axis=0), name = 'std_slope')
        df_result1 = df_result1.append(s)
        #prob_postive_slope
        s = pd.Series(df.where(df>0).count(axis=0) /df.count(axis=0),name = 'prob_positive_slope')
        df_result1 = df_result1.append(s)
        index1 = [['Score>30_level_1']*len(df_result1.index),list(df_result1.index)]
        df_result1.set_index(index1,inplace=True)


        return pd.concat([df_result,df_result1])
    def get_df_detail_r_square_group_avg_excess_return_vs_group(self, datelist = [], return_period_list=[1,3,6,12]):
        '''根據df_grouped_detail,給定datelist與return_period_list計算slope
        datelist:[]表示全部日期'''
        if datelist==[]:
            datelist = self.datelist
        #計算score vs return
        df_result = pd.DataFrame()
        for date1 in datelist:
            dict_result={}
            for k in return_period_list:
                col_name = 'excess_return_' + str(k) + 'm_mean'
                df= self.df_grouped_detail.loc[date1,['group',col_name]]
                df=df.reset_index(drop=True)
                df.set_index('group',inplace=True)
                s = df.loc[['group_' + str(k) for k in range(self.num_group,0,-1)]]
                regressor = LinearRegression()
                x=numpy.arange(self.num_group).reshape(-1,1)               
                y=s[col_name].values.reshape(-1,1) 
                try:
                    regressor.fit(x, y)
                    r_squared = regressor.score(x, y)
                except:
                    r_squared = numpy.nan
                dict_result[ str(k) + 'm'] = r_squared
            s = pd.Series(dict_result, name = date1)
            df_result= df_result.append(s)
        return df_result     
    def get_df_detail_slope_group_avg_excess_return_vs_group(self, datelist = [], return_period_list=[1,3,6,12],group_filter = (1,10)):
        '''根據df_grouped_detail,給定datelist與return_period_list計算slope
        datelist:[]表示全部日期'''
        if datelist==[]:
            datelist = self.datelist
        #計算score vs return
        df_result = pd.DataFrame()
        for date1 in datelist:
            dict_result={}
            for k in return_period_list:
                col_name = 'excess_return_' + str(k) + 'm_mean'
                df= self.df_grouped_detail.loc[date1,['group',col_name]]
                df=df.reset_index(drop=True)
                df.set_index('group',inplace=True)
                s = df.loc[['group_' + str(k) for k in range(self.num_group,0,-1) if (k >= group_filter[0]) &  (k <= group_filter[1])]]
                num_data = group_filter[1] - group_filter[0] + 1
                regressor = LinearRegression()
                x=numpy.arange(num_data).reshape(-1,1)               
                y=s[col_name].values.reshape(-1,1) 
                try:
                    regressor.fit(x, y)
                    b=regressor.coef_[0][0] * 12/k
                except:
                    b = numpy.nan
                dict_result[ str(k) + 'm'] = b
            s = pd.Series(dict_result, name = date1)
            df_result= df_result.append(s)
        return df_result        
    def gt_perf_stats_level_2(self):
        return_period_list=[1,3,6,12]
        df_result = DataFrame()
        #======ALL sample====================
        df = self.get_df_detail_slope_group_avg_excess_return_vs_group([],return_period_list,(1,self.num_group))
        #count
        s = pd.Series(df.count(axis=0), name = 'num_sampling_date')
        df_result = df_result.append(s)
        #avg_r-square
        s = pd.Series(self.get_df_detail_r_square_group_avg_excess_return_vs_group([],return_period_list).mean(axis=0), name ='avg_r_square')
        df_result = df_result.append(s)
        #avg_slope        
        s1 = pd.Series(df.mean(axis=0), name ='avg_slope_group')
        df_result = df_result.append(s1)
        #std_slope
        s2 =pd.Series( df.std(axis=0), name ='std_slope_group')
        df_result = df_result.append(s2)
        #cv
        s=pd.Series(s1/s2, name ='cv_slope')
        df_result = df_result.append(s)
        #prob_postive_slope
        s = pd.Series(df.where(df>0).count(axis=0) /df.count(axis=0), name = 'prob_positive_slope_group')
        df_result = df_result.append(s)

        index1 = [['all_level_2']*len(df_result.index),list(df_result.index)]
        df_result.set_index(index1,inplace=True)
        #======group>3 sample====================
        df_result1 = DataFrame()
        df = self.get_df_detail_slope_group_avg_excess_return_vs_group([],return_period_list,(1,self.num_group-3))
        #avg_slope        
        s = pd.Series(df.mean(axis=0),name = 'avg_slope_group')
        df_result1 = df_result1.append(s)
        #std_slope
        s =pd.Series( df.std(axis=0), name = 'std_slope_group')
        df_result1 = df_result1.append(s)
        #prob_postive_slope
        s = pd.Series(df.where(df>0).count(axis=0) /df.count(axis=0),name = 'prob_positive_slope_group')
        df_result1 = df_result1.append(s)
        index1 = [['group_1~7_level_2']*len(df_result1.index),list(df_result1.index)]
        df_result1.set_index(index1,inplace=True)
        return pd.concat([df_result,df_result1])
    def get_slope_final_group_avg_excess_return_vs_group(self, return_period_list=[1,3,6,12],group_filter = (1,10)):
        '''根據df_grouped_final,return_period_list計算slope'''
        #計算score vs return
        dict_result={}
        for k in return_period_list:
            col_name = 'excess_return_' + str(k) + 'm_mean_mean'
            df= self.df_grouped_final[[col_name]]
            s = df.loc[['group_' + str(k) for k in range(self.num_group,0,-1) if (k >= group_filter[0]) &  (k <= group_filter[1])]]
            num_data = group_filter[1] - group_filter[0] + 1
            regressor = LinearRegression()
            x=numpy.arange(num_data).reshape(-1,1)               
            y=s[col_name].values.reshape(-1,1) 
            try:
                regressor.fit(x, y)
                b=regressor.coef_[0][0] * 12/k
            except:
                b = numpy.nan
            dict_result[ str(k) + 'm'] = b
        s = pd.Series(dict_result)
        return s  
    def gt_perf_stats_level_3(self):
        return_period_list=[1,3,6,12]
        df_result = DataFrame()
        #======ALL sample====================
        s = self.get_slope_final_group_avg_excess_return_vs_group(return_period_list)
        #final slope
        s.name='avg_slope_final_group'
        df_result = df_result.append(s)
        index1 = [['all_level_3']*len(df_result.index),list(df_result.index)]
        df_result.set_index(index1,inplace=True)      
        #======group>3 sample====================
        df_result1 = DataFrame()
        s = self.get_slope_final_group_avg_excess_return_vs_group(return_period_list,(1,self.num_group-3))
        #final slope
        s.name='avg_slope_final_group'
        df_result1 = df_result1.append(s)
        index1 = [['group_1~7_level_3']*len(df_result1.index),list(df_result1.index)]
        df_result1.set_index(index1,inplace=True) 

        return pd.concat([df_result,df_result1])          
    def gt_perf_stats_all(self):  
        df_result_1 = self.gt_perf_stats_level_1()
        df_result_2 = self.gt_perf_stats_level_2()
        df_result_3 = self.gt_perf_stats_level_3()

        df_result = pd.concat([df_result_1,df_result_2,df_result_3])
        df_result.name = self.strategyname
        return df_result

    def plot_scatter_return_score_by_date(self, querydate: datetime, return_period_list  =[1], is_plot_regline = True):
        '''根據df_indistock_detail繪製return vs score的散佈圖'''
        #fig = plt.figure()
        fig,axs=plt.subplots(1,len(return_period_list))
        i=0
        for k in return_period_list:
            ax1 = axs[i] if len(return_period_list) > 1 else axs
            col_name = 'excess_return_' + str(k) + 'm'
            df= self.df_indistock_detail.loc[querydate,['score','group',col_name]]
            #df[col_name] =df[col_name] * 12/k
            #plot scatter
            ax = sns.scatterplot(ax=ax1,x="score", y=col_name, data=df, hue="group",legend=False)
            #sns.barplot(ax=ax1[1],x="group", y=col_name, data=df, ci=0, order = self.group_order[::-1])
            #plot regress line
            if is_plot_regline:
                regressor = LinearRegression()
                x=df['score'].values.reshape(-1,1) 
                y=df[col_name].values.reshape(-1,1) 
                regressor.fit(x, y)
                b = regressor.coef_[0][0]
                x_pred = numpy.arange(1,101,1)
                y_pred = regressor.predict(x_pred.reshape(-1,1))
                ax.plot(x_pred,y_pred)
                ax.text(100,1,"slope: -" + str(round(b,6)),fontsize=10)
            # ax1.legend(prop={'size': 6},mode = "expand", ncol = self.num_group, loc = 'best')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1)) #設定為%格式
            # ax.set_ylim([-1,1.2])
            i=i+1
        
        # ax.invert_xaxis() 
        fig.suptitle('Grouping-Test : ' + self.strategyname + '_' + querydate.strftime('%Y%m%d') ,fontsize=15)
        # fig.suptitle('Grouping-Test : ' + self.strategyname,fontsize=15)
        return fig
    def plot_bar_avg_return_group_by_date(self, querydate: datetime, return_period_list = [1,3,6,12], is_plot_regline = True):
        '''根據df_grouped_detail繪製return vs group的散佈圖'''
        fig = plt.figure()
        axs=plt.subplot(1,len(return_period_list),1)
        i=0
        for k in return_period_list:
            ax1 = axs[i] if len(return_period_list) > 1 else axs
            col_name = 'excess_return_' + str(k) + 'm_mean'
            df= self.df_grouped_detail.loc[querydate,['group',col_name]]
            df.reset_index(drop=True,inplace=True)
            df.set_index('group',inplace=True)
            #index reorder
            df = df.reindex(index=self.group_order[::-1])
            # df[col_name] =df[col_name] * 12/k
            # print(df)
            #plot bar
            ax = ax1.bar(df.index,df[col_name])
            #plot regress line
            if is_plot_regline:
                regressor = LinearRegression()
                x=numpy.arange(self.num_group).reshape(-1,1) 
                y=df[col_name].values.reshape(-1,1) 
                regressor.fit(x, y)
                b = regressor.coef_[0][0]
                y_pred = regressor.predict(x)
                ax.plot(x,y_pred)
                ax.text(100,1,"slope: -" + str(round(b,6)),fontsize=10)
            # ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1)) #設定為%格式
            # ax.set_ylim([-1,1.2])
            i=i+1
        
        # ax.invert_xaxis() 
        fig.suptitle('Grouping-Test : ' + self.strategyname + '_' + querydate.strftime('%Y%m%d') ,fontsize=15)
        # fig.suptitle('Grouping-Test : ' + self.strategyname,fontsize=15)
        return fig  
    # def plot_bar_avg_return_score_by_date(self, querydate: datetime,return_type):
    #     df= self.df_grouped_detail.loc[querydate, ['group',return_type]]#,'excess_return_3m_mean','excess_return_6m_mean','excess_return_12m_mean']]
    #     df.set_index('group',inplace=True)
    #     #annualized
    #     df[return_type] =df[return_type] * 12
    #     # df['excess_return_3m_mean'] =df['excess_return_3m_mean'] * 4
    #     # df['excess_return_6m_mean'] =df['excess_return_6m_mean'] * 2 
    #     #index reorder
    #     df = df.reindex(index=self.group_order[::-1])

    #     #fig,axs = plt.subplots(nrows=1,ncols=len(df.columns), sharex=True,figsize=(18,8),sharey=True)
    #     fig = plt.figure()
    #     ax1=plt.subplot(1,1,1)
    #     ax = ax1.bar(df.index,df[return_type])
    #     #取得colormap
    #     # cmap = cm.get_cmap('tab20c')
    #     # i=0
    #     # for data_colname,ax in zip(df.columns, axs.flatten()):
    #     #     if i % 16 == 0: 
    #     #         i = 0
    #     #     colors=cmap([i]*10) #設定10根bar顏色
    #     #     df.plot(y= data_colname , kind="bar", color = colors, ax=ax ,legend=False)
    #     # ax.set_title(data_colname,fontsize=9)
    #     # ax.set_xlabel('')
    #     # ax.set_ylabel(querydate.strftime("%Y%m%d"))
    #     ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1)) #設定為%格式
    #         # i=i+4
        
    #     fig.suptitle('Grouping-Test : ' + self.strategyname + '_' + querydate.strftime('%Y%m%d') ,fontsize=15)
    #     return fig
    def plot_gt_grouped_final(self):
        df= self.df_grouped_final[['excess_return_1m_mean_mean','excess_return_3m_mean_mean','excess_return_6m_mean_mean','excess_return_12m_mean_mean']]
        #annualized
        df['excess_return_1m_mean_mean'] = df['excess_return_1m_mean_mean'] * 12
        df['excess_return_3m_mean_mean'] = df['excess_return_3m_mean_mean'] * 4
        df['excess_return_6m_mean_mean'] = df['excess_return_6m_mean_mean'] * 2 
        #index reorder
        df = df.reindex(index=self.group_order[::-1])
        
        fig,axs = plt.subplots(nrows=1,ncols=len(df.columns),sharex=True,sharey=True,figsize=(18,8))
        
        #取得colormap
        cmap = cm.get_cmap('tab20c')
        i=0
        for data_colname,ax in zip(df.columns, axs.flatten()):
            i = 0 if i % 16 == 0 else i
            colors=cmap([i]*10) #設定10根bar顏色
            df.plot(y= data_colname , kind="bar", color=colors, ax=ax, legend=False)
            ax.set_title(data_colname,fontsize=9)
            ax.set_xlabel('')
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1)) #設定為%格式
            i=i+4
        fig.suptitle('Grouping-Test : ' + self.strategyname ,fontsize=14)
        return fig
class PORT_GT():
    def __init__(self,port_scorematrix_rankpct = None,port_price_matrix = None ):# strategyid, startdate, enddate, stockpool_name, gt_date_sampling_para:tuple):
        '''port_scorematrix_rankpct : 格式:Date x Ticker ，0~100分數越高表示越好
           port_price_matrix:格式:Date x Ticker '''
        self.port_scorematrix_rankpct = port_scorematrix_rankpct 
        self.port_price_matrix = port_price_matrix  
    
    def calc_gt_indistock_detail(self, tickerlist, gt_date_sampling_para:tuple, num_group = 10, delay_buy_days= 1, benchmark = pandas.Series()):
        '''tickerlist :若為[]則取得scorematrix全部ticker
           gt_date_sampling_para:
           case 1:('RD',50, 1):RD表示random date(RD)隨機抽樣50筆,random seed = 1(若不要固定seed則傳入0)
           case 2:('SD',11):SD表示specific date(SD)指定每月11號
           case 3:('FA',3):FA表示財報日期,參數表示財報公告日後3天
           benchmark:價格資料,日期須與port_price_matrix相同
        '''
            
        #決定date_list
        gt_date_type = gt_date_sampling_para[0]
           
        startdate_sampling = self.port_price_matrix.index[0]
        enddate_sampling = self.port_price_matrix.index[-100]
        enddate  = self.port_price_matrix.index[-1]
        
        if gt_date_type == 'SD':
            gt_date_para = gt_date_sampling_para[1]
            date_list = quantlib.get_periodic_date(startdate_sampling,enddate_sampling,gt_date_para,1)
        elif gt_date_type == 'FA':
            delay_days = gt_date_sampling_para[1]
            date_list = [x + timedelta(days = delay_days) for x in  quantlib.get_periodic_date(startdate_sampling,enddate_sampling,'FA',1)]
        elif gt_date_type == 'RD':
            if gt_date_sampling_para[2] > 0:
                random.seed(gt_date_sampling_para[2])#固定抽樣樣本
            gt_date_para = gt_date_sampling_para[1]    
            date_list = list(self.port_price_matrix.index[0:len(self.port_price_matrix)-100])
            random.shuffle(date_list)
            date_list = date_list[:gt_date_para]

        df_score = pandas.DataFrame()
        df_indistock_detail = pandas.DataFrame()

        for date1 in date_list:
            df_return = pandas.DataFrame()
            df_score = pandas.DataFrame()
            if tickerlist == []:
                df_score['score'] = self.port_scorematrix_rankpct.loc[date1]
            else:
                df_score['score'] = self.port_scorematrix_rankpct.loc[date1,tickerlist]
            df_score.index.name='ticker'
            #去除na後重新計算rankpct
            df_score.dropna(axis=0,inplace=True)
            df_score['score'] = df_score['score'].rank(method='average',na_option='keep',pct=True, ascending = True)*100

            bins = numpy.arange(0,101,int(100/num_group)) #分num_group組
            #統計Score是屬於哪一組,並加上label。分數最低是Group_num_group,分數最高為Group_1
            df_score['group'] = pandas.cut(df_score['score'], bins,labels=['group_' + str(i) for i in range(num_group,0,-1)]) 
            #買入日期
            date_buy = date1 + timedelta(days = delay_buy_days)
            # df_return['Close'] = self.port_price_matrix.loc[date_buy]
            #計算區間報酬率
            for k in [1,3,6,12]:
               
                period_date = date_buy + relativedelta(months= k)
                if period_date <= enddate:
                    benchmark_return = 0 if benchmark.empty else (benchmark.loc[period_date] / benchmark.loc[date_buy] - 1)
                    df_return['return_'+ str(k) + 'm']= (self.port_price_matrix.loc[period_date] / self.port_price_matrix.loc[date_buy] - 1) 
                    df_return['excess_return_'+ str(k) + 'm']= (self.port_price_matrix.loc[period_date] / self.port_price_matrix.loc[date_buy] - 1) - benchmark_return
                else:
                    df_return['return_'+ str(k) + 'm'] = numpy.nan
                    df_return['excess_return_'+ str(k) + 'm'] = numpy.nan
            df_return.index.name='ticker'
            df_combine = df_score.join(df_return,how='left')
            df_combine.reset_index(inplace=True)
            df_combine.insert(0,'date',date_buy)
            df_indistock_detail = df_indistock_detail.append(df_combine,ignore_index=True)
        return df_indistock_detail
    def calc_gt_grouped_detail(self, df_indistock_detail):
        df_grouped_detail = pandas.DataFrame()
        date_buy_list = list(df_indistock_detail['date'].drop_duplicates())
        for date1 in date_buy_list:
            df_detail = df_indistock_detail[df_indistock_detail['date']==date1]
                      
            #=========分群統計=======================
            #Groupby統計欄位設定
            #stats = ['size','max','min','mean','median' 'std', 'skew','describe']
            df = pandas.DataFrame()
            stats=['mean', 'std', 'skew']
            col_setting={'return_1m':stats,
                        'return_3m': stats,
                        'return_6m': stats,
                        'return_12m': stats,
                        'excess_return_1m':stats,
                        'excess_return_3m': stats,
                        'excess_return_6m': stats,
                        'excess_return_12m': stats}
            df = df_detail.groupby(['group']).agg(col_setting)
            df.columns=['_'.join(x) for x in df.columns.to_flat_index()]
            df['date']=date1
            df.reset_index(inplace=True) #remove index:為了之後concat
            if df_grouped_detail.empty:
                df_grouped_detail = df
            else:
                df_grouped_detail = pandas.concat([df_grouped_detail,df],axis=0, ignore_index=True,sort=True)
        return df_grouped_detail
    def calc_gt_grouped_detail_ver2(self, df_indistock_detail):
        df_grouped_detail = pandas.DataFrame()
        df_slope = pandas.DataFrame()
        date_buy_list = list(df_indistock_detail['date'].drop_duplicates())
        for date1 in date_buy_list:
            df_detail = df_indistock_detail[df_indistock_detail['date']==date1]
            #calc slope_return_score
            dict_b={}
            try:#當有nan時忽略不記

                for k in [1,3,6,12]:
                    return_type = 'excess_return_'+ str(k) + 'm'
                    df= df_detail[['score','group',return_type]]
                    #計算score vs return
                    regressor = LinearRegression()
                    x=df['score'].values.reshape(-1,1) 
                    y=df[return_type].values.reshape(-1,1) 
                    
                    regressor.fit(x, y)
                    b = regressor.coef_[0][0]
                    dict_b['slope_'+ str(k) + 'm'] = b
                s = pd.Series(dict_b)
                s.name = date1
                df_slope= df_slope.append(s)
                
            except:
                pass
           
            #=========分群統計=======================
            #Groupby統計欄位設定
            #stats = ['size','max','min','mean','median' 'std', 'skew','describe']
            df = pandas.DataFrame()
            stats=['mean', 'std', 'skew']
            col_setting={'return_1m':stats,
                        'return_3m': stats,
                        'return_6m': stats,
                        'return_12m': stats,
                        'excess_return_1m':stats,
                        'excess_return_3m': stats,
                        'excess_return_6m': stats,
                        'excess_return_12m': stats}
            df = df_detail.groupby(['group']).agg(col_setting)
            s = df.stack([0,1])
            s.name = date1
            df_grouped_detail = df_grouped_detail.append(s)

        df_grouped_detail.columns=['_'.join(x) for x in df_grouped_detail.columns.to_flat_index()]
        
        df_result = df_slope.join(df_grouped_detail,how='left')
        df_result.index.name='date'
        df_result.reset_index(inplace=True)
        return df_result
    def calc_gt_grouped_final(self, df_grouped_detail):
        Num_group = len(list(df_grouped_detail['group'].drop_duplicates()))
        df_grouped_detail.set_index(keys=['date','group'],inplace=True)
        
        #Groupby統計欄位設定
        stats=['mean', 'std', 'skew']
        col_setting={'return_1m_mean': stats,
                     'return_3m_mean': stats,
                     'return_6m_mean': stats,
                     'return_12m_mean': stats,
                     'excess_return_1m_mean': stats,
                     'excess_return_3m_mean': stats,
                     'excess_return_6m_mean': stats,
                     'excess_return_12m_mean': stats
                     }

        df_grouped_final = df_grouped_detail.groupby(['group']).agg(col_setting)
        df_grouped_final.columns=[ '_'.join(x) for x in df_grouped_final.columns.to_flat_index()]
        df_grouped_final = df_grouped_final.reindex(['group_'+ str(x) for x in range(1,Num_group+1)])
        log.info("port_grouping_test: %s" % str(perf_counter()-time_start))
        return df_grouped_final

 
    def gt_run(self, startdate, enddate,  stockpool_name, strategyid, gt_sampling_para,num_group=10, sector_excl_list='',folder_suffix =''):
        '''
        folder_suffix:有時會測試不同參數時,可以讓結果輸出在另一個增加suffix的folder
        '''
        time_start= perf_counter()
        is_out_scorematrix= False
        is_out_pricematrix = False
        is_out_measure_rawdata =False
        obj_data = PORT_DATA(startdate, enddate, stockpool_name, strategyid)

        obj_data.sector_excl_list = obj_data.sector_excl_list + sector_excl_list
        gt_folder_path = os.path.join(obj_data.bt_result_data_folder_path,'group_testing')
        strategy_profile = obj_data.strategy_profile
        tickerlist = obj_data.stockpool_tickerlist
        strategyname = strategy_profile['strategyname']
        gt_para = {'gt_sampling_para':gt_sampling_para,'folder_suffix':folder_suffix}
        obj_output = PORT_OUTPUT(startdate, enddate, stockpool_name, strategyid,gt_folder_path,**gt_para)
        # date_list_all = quantlib.get_periodic_date(startdate- timedelta(days=1),enddate,-1,12)
        gt_result_2_folder_path= obj_output.bt_result_2_folder_path
         #para_setting.json
        para_setting={
            #data
            'startdate':startdate.strftime("%Y%m%d"),
            'enddate':enddate.strftime("%Y%m%d"),
            'stockpool':stockpool_name,
            'sector_excl_list' : obj_data.sector_excl_list,
            #strategy
            'strategyid':strategyid,
            'strategyname':strategyname,
            'strategy_list':strategy_profile['measure'],
            'strategy_direction_list':strategy_profile['measure_dir'],
            'strategy_weighting_list':strategy_profile['weighting'],
            #gt para
            'gt_sampling_para':gt_sampling_para,
            #time
            'gt_time': datetime.now().strftime('%Y%m%d')   
            }
        json.dump(para_setting, open(os.path.join(gt_result_2_folder_path,'0_para_setting.json'), "w",encoding='utf-8-sig'), ensure_ascii=False) 

        # #step 1:score_matrix
        # port_scorematrix_rankpct, dict_measure_rawdata = obj_data.port_scorematrix_by_measure()
        # #step 1-2 : effective ticker
        # self.port_scorematrix_rankpct =  obj_data.get_effective_ticker(port_scorematrix_rankpct, stockpool_name, obj_data.stockpool_rebalancedate_list)
        # if is_out_scorematrix:
        #     obj_output.df_to_csv(port_scorematrix_rankpct,'port_scorematrix_rankpct.csv')
        # #dict_measure_rawdata
        # if is_out_measure_rawdata:
        #     for datatype in self.dict_measure_rawdata:
        #         obj_output.df_to_csv(dict_measure_rawdata[datatype],'port_scorematrix_rawdata_' + datatype + '.csv')
        # #step 2:price_matrix
        # port_price_matrix = obj_data.port_price_matrix_from_csv(tickerlist,startdate,enddate,2)
        # if is_out_pricematrix:
        #     obj_output.df_to_csv(port_price_matrix,'port_price_matrix.csv')
        # self.port_price_matrix = port_price_matrix
        # #step 3: df_indistock_detail
        # Ticker_bm = '0050'
        # StartDate = list(self.port_price_matrix.index)[0]
        # EndDate = list(self.port_price_matrix.index)[-1]
        # obj_bm=DataRetriever(Ticker_bm)
        # df_benchmark= obj_bm.query(StartDate,EndDate,['Close'],[],2,'ffill')
        # df_indistock_detail = self.calc_gt_indistock_detail(tickerlist,gt_sampling_para, num_group,1,df_benchmark.squeeze('columns'))
        # df_indistock_detail.set_index('date',inplace=True)
        # obj_output.df_to_csv(df_indistock_detail,'gt_indistock_detail.csv')
        # #step 4:df_grouped_detail
        # # df_indistock_detail = pandas.read_csv(os.path.join(gt_result_2_folder_path,'gt_indistock_detail.csv'))
        # # df_indistock_detail.index = pandas.to_datetime(df_indistock_detail.index)
        # df_indistock_detail.reset_index(inplace=True)
        # df_grouped_detail = self.calc_gt_grouped_detail(df_indistock_detail)
        # df_grouped_detail.set_index('date',inplace=True)
        # obj_output.df_to_csv(df_grouped_detail,'gt_grouped_detail.csv')

        # # df_grouped_detail_2 = self.calc_gt_grouped_detail_ver2(df_indistock_detail)
        # # df_grouped_detail_2.set_index('date',inplace=True)
        # # obj_output.df_to_csv(df_grouped_detail_2,'gt_grouped_detail_2.csv')
        # # #step 5:df_grouped_final
        # df_grouped_detail.reset_index(inplace=True)
        # df_grouped_final = self.calc_gt_grouped_final(df_grouped_detail)
        # obj_output.df_to_csv(df_grouped_final,'gt_grouped_final.csv')
        #step 6: gt_perf_stats_all
        df_indistock_detail = pandas.read_csv(os.path.join(gt_result_2_folder_path,'gt_indistock_detail.csv'))
        df_indistock_detail.index = pandas.to_datetime(df_indistock_detail.index)
        df_grouped_detail = pandas.read_csv(os.path.join(gt_result_2_folder_path,'gt_grouped_detail.csv'))
        df_grouped_detail.index = pandas.to_datetime(df_grouped_detail.index)
        df_grouped_final = pandas.read_csv(os.path.join(gt_result_2_folder_path,'gt_grouped_final.csv'))
        obj_result_1 = PORT_GT_ANALYSIS(strategyname,df_indistock_detail,df_grouped_detail,df_grouped_final)
        obj_result_1.get_f_anova([datetime(2013,1,12)],[1])
        #df=obj_result_1.gt_perf_stats_all()
        #obj_output.df_to_csv(df,'gt_perf_stats.csv')
        # step 6_1 : plot      
        if False:
            for date1 in obj_result_1.datelist[:5]:
                fig = obj_result_1.plot_scatter_return_score_by_date(date1,[1],False)
                fig.savefig(os.path.join(gt_result_2_folder_path, strategyname +'_scatter_' + date1.strftime('%Y%m%d') + '.png'))
                fig = obj_result_1.plot_bar_avg_return_group_by_date(date1,[1],False)
                fig.savefig(os.path.join(gt_result_2_folder_path, strategyname +'_bar_' + date1.strftime('%Y%m%d') + '.png'))
        #plot_gt_grouped_final
        # fig = obj_result_1.plot_gt_grouped_final()
        # fig.savefig(os.path.join(gt_result_2_folder_path, strategyname +'_final.png'))
        logging.debug("gt_run time: %s" % (str(round(perf_counter()-time_start,2))))

class PORT_BT_ANALYSIS():
    def __init__(self,port_bt_result,Ticker_bm ='0050') -> None:
        self.Ticker_bm = Ticker_bm
        self.port_bt_result = port_bt_result
        #StartDate為第一個進場的那一天
        StartDate = list(port_bt_result[(port_bt_result['New']>0)].index)[0]
        EndDate = list(port_bt_result.index)[-1]
        obj_bm=DataRetriever(Ticker_bm)
        _tmp= obj_bm.query(StartDate,EndDate,['Price_Return_1D'],[],1)
        _tmp = _tmp.join(port_bt_result['Equity'], how='left')
        #轉成series
        self.bm_daily_return = _tmp['Price_Return_1D']
        self.bm_daily_return.name = 'bm_daily_return'
        self.port_daily_return = _tmp['Equity']/_tmp['Equity'].shift(1)-1
        self.port_daily_return.name = 'port_daily_return'
    def get_tearsheet(self):
        #產生pyfolio.tearsheet
        return pf.create_returns_tear_sheet(self.port_daily_return.tz_localize('UTC'), benchmark_rets=self.bm_daily_return.tz_localize('UTC'),return_fig=True)
    def calc_perf_stats(self):
        #產生pyfolio.perf_state
        perf_stats_port = pyfolio.timeseries.perf_stats(self.port_daily_return.tz_localize('UTC'))
        perf_stats_benchmark  = pyfolio.timeseries.perf_stats(self.bm_daily_return.tz_localize('UTC'))
        perf_stats = pandas.DataFrame(perf_stats_port,columns=['Port'])
        perf_stats[self.Ticker_bm] = perf_stats_benchmark.values
        return perf_stats
    def calc_rebalance_period_return(self):
        '''Return rebalance period return based on self.port_bt_result'''
        def cumulate_returns(x):
            return ep.cum_returns(x).iloc[-1]
        rebalance_date_list = list(self.port_bt_result.loc[~self.port_bt_result['Hold'].isnull()].index)
        df1 = DataFrame(rebalance_date_list,index = rebalance_date_list ,columns=['rebalance_date'])
        df2 = self.port_bt_result.join(df1,how='left').fillna(method = 'bfill')['rebalance_date']  
        if df2.iloc[-1] is pd.NaT:#假如最後rebalance_date是Nat則填入最後日期
            df2.fillna(df2.index[-1],inplace=True) 
        #處理第一個評估日
        df2.iloc[0] = df2.iloc[1]

        #結合port_daily_return
        df3 = self.port_daily_return.to_frame().join(df2.to_frame(),how='left')
        #結合bm_daily_return
        df3 = df3.join(self.bm_daily_return.to_frame(),how='left')
        df3 = df3.set_index('rebalance_date')
        #df3.to_csv('test3.csv',encoding ='utf-8-sig')
        port_period_return = df3[['port_daily_return','bm_daily_return']].groupby(['rebalance_date']).apply(cumulate_returns).round(5)
        return port_period_return['port_daily_return'],port_period_return['bm_daily_return']

    def calc_periodical_return(self, period_list = ['weekly','monthly','quarterly','yearly','rebalance_date']):
        result_dict={}
        for pr in period_list:
            if pr=='rebalance_date':
                port_period_return,bm_period_return = self.calc_rebalance_period_return()
            else:
                port_period_return = ep.aggregate_returns(self.port_daily_return, pr).round(5)
                bm_period_return = ep.aggregate_returns(self.bm_daily_return, pr).round(5)
            df_result = bm_period_return.to_frame('bm_'+ pr + '_return').join(port_period_return.to_frame('port_'+ pr + '_return'))
            df_result['excess_return'] = df_result['port_'+ pr + '_return']-df_result['bm_'+ pr + '_return']
            result_dict[pr] = df_result
        return result_dict
class PORT_OUTPUT():
    def __init__(self,startdate, enddate, stockpool_name, strategyid, parent_folder_path , **kwargs):
        # bt_ranking_para = None, rebalance_day = None, rebalace_period_m = 1):
        strategy_profile = quantlib.Setting().get_setting_strategy()
        strategyname =strategy_profile[strategyid.upper()]['strategyname']
        #結果第一層 ALL_201501_202106
        self.bt_result_1_folder_path = os.path.join(parent_folder_path,'_'.join(list(stockpool_name) + [startdate.strftime("%Y%m"),enddate.strftime("%Y%m")]))
        if not os.path.exists(self.bt_result_1_folder_path): os.mkdir(self.bt_result_1_folder_path)
        #結果第2層 格式 [bt_method_para]_[bt_rebalance_para]_strategy_list  ex:[1_50_50]_[11]_MonthSale_YoY
        #處理kwargs :
        folder_str=[]

        if 'gt_sampling_para' in kwargs.keys():
            gt_sampling_para_str = '[' + '_'.join([str(x) for x in kwargs['gt_sampling_para']]) +']' if kwargs['gt_sampling_para']!='' else ''
            folder_str.append(gt_sampling_para_str)
        if 'bt_method_para'in kwargs.keys():
            bt_method_para_str =  '[' + '_'.join([str(x) for x in kwargs['bt_method_para']]) +']' if kwargs['bt_method_para']!='' else ''
            folder_str.append(bt_method_para_str)
        if 'bt_rebalance_para' in kwargs.keys():
            bt_rebalance_para_str = '[' + '_'.join([str(x) for x in kwargs['bt_rebalance_para']]) +']' if kwargs['bt_rebalance_para']!='' else ''
            folder_str.append(bt_rebalance_para_str)
        
        folder_str.append(strategyname)
        if 'folder_suffix' in kwargs.keys():
            folder_suffix =  str(kwargs['folder_suffix']) 
            if  folder_suffix !='':
                folder_str.append(folder_suffix)
        self.bt_result_2_folder_path = os.path.join(self.bt_result_1_folder_path , '_'.join(folder_str))

        if not os.path.exists(self.bt_result_2_folder_path): os.mkdir(self.bt_result_2_folder_path)
    def bt_result_1_folder_path(self):
        return self.bt_result_1_folder_path
    def bt_result_2_folder_path(self):
        return self.bt_result_2_folder_path
    def df_to_csv(self, df, file_name):
            df.round(6).to_csv(os.path.join(self.bt_result_2_folder_path,file_name),encoding ='utf-8-sig')


class PORT_BT():
    def __init__(self,port_scorematrix_rankpct = None,port_price_matrix = None , TaxRate =0.003, FeeRate = 0.001425):
        '''port_scorematrix_rankpct : 格式:Date x Ticker ，0~100分數越高表示越好
           port_price_matrix:格式:Date x Ticker '''
        self.port_scorematrix_rankpct = port_scorematrix_rankpct 
        self.port_price_matrix = port_price_matrix  
        self.TaxRate = TaxRate
        self.FeeRate = FeeRate
    def calc_port_bt_result(self,
                            port_scorematrix_ranking: pandas.DataFrame,
                            port_price_matrix: pandas.DataFrame,
                            bt_ranking_para : tuple,
                            bt_ranking_random_sampling = 0,
                            is_equal_weight = 0,
                            holding_level_pct = pandas.Series()):
        '''較port_bt_rebalance_scoreranking_new多考慮出場排名部分
        bt_ranking_para :選擇標的之參數，格式為tuple(start,end ,exit)
        bt_ranking_random_sampling:表示是否要隨機選股,預設是在start ~ start + 2*(end-start)中隨機抽取所需要的標的數量
        回測假設:
        1.rebalance date隔日收盤價買入
        2.在rebalance date出清 
        '''
        bt_ranking_start = int(bt_ranking_para[0])
        bt_ranking_end = int(bt_ranking_para[1])
        bt_ranking_exit = int(bt_ranking_para[2])

        no_chg_list=[]
        rdate1_sold_list=[]
        rdate2_buy_list=[]
        def get_holding_level_pct(querydate):
            a = holding_level_pct.at[querydate.to_pydatetime(),'PB_Percentile'] if not holding_level_pct.empty else 1
            return a
        def get_rebalance_target_info(rdate2,port_inventory):

            if port_inventory == []:
                Port_Num_Avail = bt_ranking_end - bt_ranking_start + 1
                rdate1_sold_list=[]
                no_chg_list=[]
            else:
                #判別出場條件:若大於bt_ranking_exit出場
                port_inventory_Rank = port_scorematrix_ranking.loc[rdate2,port_inventory].fillna(2000)
                rdate1_sold_list = list(port_inventory_Rank[lambda x: x > bt_ranking_exit].index)
                no_chg_list = list(set(port_inventory) - set(rdate1_sold_list))
                Port_Num_Avail = len(rdate1_sold_list)
            
            if Port_Num_Avail>0:
                candidate_buy_list=list(set(port_scorematrix_ranking.columns) - set(port_inventory))
                rdate2_buy_list =list(port_scorematrix_ranking.loc[rdate2,candidate_buy_list][lambda x: x >= bt_ranking_start].sort_values(ascending=True).head(Port_Num_Avail).index)
                if bt_ranking_random_sampling:
                    s2=port_scorematrix_ranking.loc[rdate2,candidate_buy_list][lambda x: (x >= bt_ranking_start) & (x <= bt_ranking_end + 2*(bt_ranking_end-bt_ranking_start+1))]
                    rdate2_buy_list =list(s2.sample(n = Port_Num_Avail).index)
            else:
                rdate2_buy_list=[]
            return no_chg_list,rdate1_sold_list,rdate2_buy_list
        time_start = perf_counter()
        rebalance_info = ['Cash','Equity','Hold','Hold_ticker','Sold','Sold_ticker','New','New_ticker','TaxFee_Acc']
        df_dailyinventory_detail = pandas.DataFrame(columns=list(port_scorematrix_ranking.columns) + rebalance_info)
        df_port_shares = pandas.DataFrame(columns=list(port_scorematrix_ranking.columns))
        df_mtm = pandas.DataFrame(columns=list(port_scorematrix_ranking.columns))
        TaxFee_Acc=0
        rebalance_date = list(port_scorematrix_ranking.index)
        EndDate = list(port_price_matrix.index)[-1]
        if EndDate in rebalance_date:
            rebalance_date.remove(EndDate) #當rebalanceDate為最後一筆資料時,之後的計算會出現錯誤,因此將它移除
        cash = 100
        TaxRate = self.TaxRate #0.003
        FeeRate = self.FeeRate #0.001425
        TaxFee_Acc = 0
        port_inventory =[]
        port_shares =pandas.Series(dtype=float)
        port_shares_old_series =pandas.Series(dtype=float)
        for i in range(len(rebalance_date)):
            rdate1 = rebalance_date[i]
            invest_startdate = rdate1 + timedelta(days = 1)
            invest_enddate = rebalance_date[i+1] if i < (len(rebalance_date)-1) else EndDate
            #因子評估日
            no_chg_list,rdate1_sold_list,rdate2_buy_list = get_rebalance_target_info(rdate1,port_inventory)
            #rebalance date
            if is_equal_weight:
                #計算可重新配置之金額
                port_shares_old = port_shares
                if  port_shares_old.empty:
                    MV_before_rebalance = cash
                    cash = 0
                else:
                    MV_before_rebalance = (port_shares_old*port_price_matrix.loc[invest_startdate]).sum()  
                equity = MV_before_rebalance + cash
                investment_avail = equity * (1 - TaxRate  -2 * FeeRate) * get_holding_level_pct(rdate1)  #預估可用之投資金額(扣除最大的交易成本) 
                port_inventory =  no_chg_list + rdate2_buy_list 
                if port_inventory !=[]:
                    port_shares= (investment_avail/len(port_inventory)) / port_price_matrix.loc[invest_startdate][port_inventory]
                    #計算調整計算實際稅費
                    port_shares_adj = port_shares.subtract(port_shares_old,fill_value=0)
                    #買入稅費
                    Fee_buy = (port_shares_adj[port_shares_adj>0] * port_price_matrix.loc[invest_startdate]).sum() * FeeRate
                    TaxFee_sell = -1 * (port_shares_adj[port_shares_adj<0] * port_price_matrix.loc[invest_startdate]).sum() * (FeeRate+TaxRate)
                    TaxFee_Acc = TaxFee_Acc + Fee_buy + TaxFee_sell
                    cash = equity - (Fee_buy + TaxFee_sell) - (port_shares*port_price_matrix.loc[invest_startdate]).sum()  
                else:
                    cash = equity

            else:#會有錯誤訊息,需要修正
                #賣出流程
                if rdate1_sold_list!=[]: 
                    MV_rdate1_sold = df_mtm.loc[invest_startdate][rdate1_sold_list].values.sum()
                    TaxFee_Acc = TaxFee_Acc + MV_rdate1_sold * (TaxRate + FeeRate) 
                    cash = MV_rdate1_sold * (1 - TaxRate - FeeRate) 
                    port_shares_old_series = port_shares.loc[no_chg_list] if no_chg_list != [] else pandas.Series()
                #買入流程
                if rdate2_buy_list!=[]: 
                    TaxFee_Acc = TaxFee_Acc + cash * FeeRate
                    investment_avail = cash * (1 - FeeRate) * get_holding_level_pct(rdate1)
                    port_shares_buy_series= (investment_avail/len(rdate2_buy_list)) / port_price_matrix.loc[invest_startdate][rdate2_buy_list]
                    port_shares = port_shares_old_series.append(port_shares_buy_series)  
                    port_inventory = list(port_shares.index) 
                    cash = 0
            #以下兩行為驗證股數用
            df_mtm = port_shares*port_price_matrix.loc[invest_startdate:invest_enddate]
            df_mtm['Cash']=cash
            df_mtm['Equity'] = df_mtm.sum(axis = 1) 
            df_mtm = df_mtm.fillna(0)
            df_dailyinventory_detail = pandas.concat([df_dailyinventory_detail,df_mtm],sort=True)
            
            port_shares.name = invest_startdate 
            df_port_shares_tmp=df_port_shares.append([port_shares])
            df_left = pandas.DataFrame(df_mtm.index,columns=['Date'])
            df_left.set_index('Date',inplace=True)
            df_port_shares_tmp=df_port_shares_tmp.fillna(0)
            df_port_shares_tmp = df_left.join(df_port_shares_tmp, how='left').fillna(method = 'ffill')
            df_port_shares = pandas.concat([df_port_shares,df_port_shares_tmp])
            df_dailyinventory_detail.loc[[rdate1 + timedelta(days = 1 if i==0 else 0)],['Hold','Hold_ticker','Sold','Sold_ticker','New','New_ticker','TaxFee_Acc']]=[len(no_chg_list),
                ','.join(["'" + ele + "'" for ele in no_chg_list]),
                len(rdate1_sold_list),
                ','.join(["'" + ele + "'" for ele in rdate1_sold_list]),
                len(rdate2_buy_list),
                ','.join(["'" + ele + "'" for ele in rdate2_buy_list]),
                    TaxFee_Acc]

        df_dailyinventory_detail.index.name='Date'
        df_port_shares.index.name='Date'
        log.info("port_bt_rebalance_scoreranking: %s" % str(perf_counter()-time_start))
        return df_dailyinventory_detail,df_port_shares
    def calc_port_bt_result_ver2(self,
                            port_signalmatrix: pandas.DataFrame,
                            port_price_matrix: pandas.DataFrame,
                            is_equal_weight = 0):
        '''修正calc_port_bt_result,傳入參數變為signalmatrix
        2022/2/22發生問題:無法處理(1,30,60)問題,需要修正
        '''
        no_chg_list=[]
        rdate1_sold_list=[]
        rdate2_buy_list=[]
        def get_rebalance_target_info(rdate2,port_inventory):
            s = port_signalmatrix.loc[rdate2]
            candidate_buy_list = list(s[s==1].index)
            #candidate_sold_list = list(s[s==-1].index)
            rdate2_buy_list = list(set(candidate_buy_list) - set(port_inventory))
            rdate1_sold_list = list(set(port_inventory) - set(candidate_buy_list))
            no_chg_list = [x for x in candidate_buy_list if x in port_inventory]
            return no_chg_list,rdate1_sold_list,rdate2_buy_list
        time_start = perf_counter()
        rebalance_info = ['Cash','Equity','Hold','Hold_ticker','Sold','Sold_ticker','New','New_ticker','TaxFee_Acc']
        df_dailyinventory_detail = pandas.DataFrame(columns=list(port_signalmatrix.columns) + rebalance_info)
        df_port_shares = pandas.DataFrame(columns=list(port_signalmatrix.columns))
        df_mtm = pandas.DataFrame(columns=list(port_signalmatrix.columns))
        TaxFee_Acc=0
        rebalance_date = list(port_signalmatrix.index)
        EndDate = list(port_price_matrix.index)[-1]
        if EndDate in rebalance_date:
            rebalance_date.remove(EndDate) #當rebalanceDate為最後一筆資料時,之後的計算會出現錯誤,因此將它移除
        cash = 100
        TaxRate = self.TaxRate #0.003
        FeeRate = self.FeeRate #0.001425
        TaxFee_Acc = 0
        port_inventory =[]
        port_shares =pandas.Series(dtype=float)
        port_shares_old_series =pandas.Series(dtype=float)
        for i in range(len(rebalance_date)):
            rdate1 = rebalance_date[i]
            invest_startdate = rdate1 + timedelta(days = 1)
            invest_enddate = rebalance_date[i+1] if i < (len(rebalance_date)-1) else EndDate
            #因子評估日
            no_chg_list,rdate1_sold_list,rdate2_buy_list = get_rebalance_target_info(rdate1,port_inventory)
            #rebalance date
            if is_equal_weight:
                #計算可重新配置之金額
                port_shares_old = port_shares
                if  port_shares_old.empty:
                    MV_before_rebalance = cash
                    cash = 0
                else:
                    MV_before_rebalance = (port_shares_old*port_price_matrix.loc[invest_startdate]).sum()  
                equity = MV_before_rebalance + cash
                investment_avail = equity * (1 - TaxRate  -2 * FeeRate)  #預估可用之投資金額(扣除最大的交易成本) 
                port_inventory =  no_chg_list + rdate2_buy_list 
                if port_inventory !=[]:
                    port_shares= (investment_avail/len(port_inventory)) / port_price_matrix.loc[invest_startdate][port_inventory]
                    #計算調整計算實際稅費
                    port_shares_adj = port_shares.subtract(port_shares_old,fill_value=0)
                    #買入稅費
                    Fee_buy = (port_shares_adj[port_shares_adj>0] * port_price_matrix.loc[invest_startdate]).sum() * FeeRate
                    TaxFee_sell = -1 * (port_shares_adj[port_shares_adj<0] * port_price_matrix.loc[invest_startdate]).sum() * (FeeRate+TaxRate)
                    TaxFee_Acc = TaxFee_Acc + Fee_buy + TaxFee_sell
                    cash = equity - (Fee_buy + TaxFee_sell) - (port_shares*port_price_matrix.loc[invest_startdate]).sum()  
                else:
                    cash = equity

            else:#會有錯誤訊息,需要修正
                #賣出流程
                if rdate1_sold_list!=[]: 
                    MV_rdate1_sold = df_mtm.loc[invest_startdate][rdate1_sold_list].values.sum()
                    TaxFee_Acc = TaxFee_Acc + MV_rdate1_sold * (TaxRate + FeeRate) 
                    cash = MV_rdate1_sold * (1 - TaxRate - FeeRate) 
                    port_shares_old_series = port_shares.loc[no_chg_list] if no_chg_list != [] else pandas.Series()
                #買入流程
                if rdate2_buy_list!=[]: 
                    TaxFee_Acc = TaxFee_Acc + cash * FeeRate
                    investment_avail = cash * (1 - FeeRate)
                    port_shares_buy_series= (investment_avail/len(rdate2_buy_list)) / port_price_matrix.loc[invest_startdate][rdate2_buy_list]
                    port_shares = port_shares_old_series.append(port_shares_buy_series)  
                    port_inventory = list(port_shares.index) 
                    cash = 0
            #以下兩行為驗證股數用
            df_mtm = port_shares*port_price_matrix.loc[invest_startdate:invest_enddate]
            df_mtm['Cash']=cash
            df_mtm['Equity'] = df_mtm.sum(axis = 1) 
            df_mtm = df_mtm.fillna(0)
            df_dailyinventory_detail = pandas.concat([df_dailyinventory_detail,df_mtm],sort=True)
            
            port_shares.name = invest_startdate 
            df_port_shares_tmp=df_port_shares.append([port_shares])
            df_left = pandas.DataFrame(df_mtm.index,columns=['Date'])
            df_left.set_index('Date',inplace=True)
            df_port_shares_tmp=df_port_shares_tmp.fillna(0)
            df_port_shares_tmp = df_left.join(df_port_shares_tmp, how='left').fillna(method = 'ffill')
            df_port_shares = pandas.concat([df_port_shares,df_port_shares_tmp])
            df_dailyinventory_detail.loc[[rdate1 + timedelta(days = 1 if i==0 else 0)],['Hold','Hold_ticker','Sold','Sold_ticker','New','New_ticker','TaxFee_Acc']]=[len(no_chg_list),
                ','.join(["'" + ele + "'" for ele in no_chg_list]),
                len(rdate1_sold_list),
                ','.join(["'" + ele + "'" for ele in rdate1_sold_list]),
                len(rdate2_buy_list),
                ','.join(["'" + ele + "'" for ele in rdate2_buy_list]),
                    TaxFee_Acc]

        df_dailyinventory_detail.index.name='Date'
        df_port_shares.index.name='Date'
        log.info("port_bt_rebalance_scoreranking: %s" % str(perf_counter()-time_start))
        return df_dailyinventory_detail,df_port_shares
     
    
    def calc_port_bt_analysis(self,port_bt_result, is_create_tearsheet=False):
        obj_result = PORT_BT_ANALYSIS(port_bt_result)
        perf_stats = obj_result.calc_perf_stats()
        fig = None
        if is_create_tearsheet:
            fig=obj_result.get_tearsheet()
        #calc_periodical return
        period_perf=obj_result.calc_periodical_return()
        return perf_stats,fig, period_perf
    def bt_run(self, startdate, enddate, stockpool_name, strategyid, bt_method_para, bt_rebalance_para,sector_excl_list=[],folder_suffix=''):
        time_start= perf_counter()
        is_output_all_csv = False
        obj_data = PORT_DATA(startdate, enddate, stockpool_name, strategyid)
        obj_data.sector_excl_list = obj_data.sector_excl_list + sector_excl_list
        bt_folder_path = os.path.join(obj_data.bt_result_data_folder_path,'back_testing')
        strategy_profile = obj_data.strategy_profile
        tickerlist = obj_data.stockpool_tickerlist
        bt_rebalance_type = bt_rebalance_para[0]
        if bt_rebalance_type == 'SD':
            bt_rebalance_day =  bt_rebalance_para[1]
            bt_rebalance_period_m  =  bt_rebalance_para[2]
            bt_rebalancedate_list = quantlib.get_periodic_date(startdate,enddate,bt_rebalance_day,bt_rebalance_period_m)
        elif bt_rebalance_type == 'FA':
            delay_days = bt_rebalance_para[1]
            bt_rebalancedate_list = [x + timedelta(days = delay_days) for x in  quantlib.get_periodic_date(startdate,enddate,'FA',1)]
        elif bt_rebalance_type == 'RD':
            pass

        strategyname = strategy_profile['strategyname']

        bt_para = { 'bt_method_para':bt_method_para,'bt_rebalance_para':bt_rebalance_para,'folder_suffix':folder_suffix}
        obj_output = PORT_OUTPUT(startdate, enddate, stockpool_name, strategyid, bt_folder_path, **bt_para)
        # date_list_all = quantlib.get_periodic_date(startdate- timedelta(days=1),enddate,-1,12)
        bt_result_2_folder_path= obj_output.bt_result_2_folder_path
 
        #para_setting.json
        para_setting={
            #data
            'startdate':startdate.strftime("%Y%m%d"),
            'enddate':enddate.strftime("%Y%m%d"),
            'stockpool':stockpool_name,
            'sector_excl_list' : obj_data.sector_excl_list,
            #strategy
            'strategyid':strategyid,
            'strategyname':strategy_profile['strategyname'],
            'strategy_list':strategy_profile['measure'],
            'strategy_direction_list':strategy_profile['measure_dir'],
            'strategy_weighting_list':strategy_profile['weighting'],
            #bt
            'bt_method_para':bt_method_para,
            'bt_rebalance_para': bt_rebalance_para,
            #time
            'bt_time': datetime.now().strftime('%Y%m%d')   
            }
        json.dump(para_setting, open(os.path.join(bt_result_2_folder_path,'0_para_setting.json'), "w",encoding='utf-8-sig')) 

        #step 1-1:score_matrix 
        port_scorematrix_rankpct, dict_measure_rawdata = obj_data.port_scorematrix_by_measure()
        self.port_scorematrix_rankpct = port_scorematrix_rankpct
        if is_output_all_csv:
            obj_output.df_to_csv(port_scorematrix_rankpct,'port_scorematrix_rankpct.csv')
            # port_scorematrix_rankpct.round(6).to_csv(os.path.join(bt_result_2_folder_path,'port_scorematrix_rankpct.csv'))
            for datatype in dict_measure_rawdata:
                obj_output.df_to_csv(dict_measure_rawdata[datatype],'port_measurematrix_rawdata_' + datatype + '.csv')
        #step 1-2 : effective ticker
        port_scorematrix_rankpct =  obj_data.get_effective_ticker(port_scorematrix_rankpct, stockpool_name, obj_data.stockpool_rebalancedate_list)
        #step 1-3: scoreranking_matrix
        extra_measurematrix_rankpct = None
        df_score_original,df_score_adjust,df_score_final,df_score_rank = obj_data.port_scorerankingmatrix(self.port_scorematrix_rankpct, bt_rebalancedate_list, extra_measurematrix_rankpct)
        port_scorerankingmatrix = df_score_rank
        obj_output.df_to_csv(df_score_rank,'port_scorematrix_rank.csv')
        #df_score_rank.round(6).to_csv(os.path.join(bt_result_2_folder_path,'port_scorerankingmatrix_rank.csv'))
        if is_output_all_csv:
            obj_output.df_to_csv(df_score_original,'port_scorematrix_rankpct_original.csv')
            obj_output.df_to_csv(df_score_adjust,'port_scorermatrix_rankpct_adjust.csv')
            obj_output.df_to_csv(df_score_final,'port_scorematrix_rankpct_final.csv')
             

        #step 2:price_matrix
        port_price_matrix = obj_data.port_price_matrix_from_csv(tickerlist,startdate,enddate,2)
        self.port_price_matrix = port_price_matrix
        if is_output_all_csv:
            port_price_matrix.to_csv(os.path.join(bt_result_2_folder_path,'port_price_matrix.csv'),encoding ='utf-8-sig')
        #step 3: bt
        bt_ranking_random_sampling = 0 #隨機抽樣
        is_equal_weight = 1 #是否rebalance都equal-weighted
        bt_ranking_para = bt_method_para
        port_signalmatrix = obj_data.port_signalmatrix(df_score_final,bt_ranking_para,df_score_final,(bt_ranking_para[2]+1,2000))
        port_signalmatrix.to_csv(os.path.join(bt_result_2_folder_path,'port_signalmatrix.csv'),encoding ='utf-8-sig')
        port_bt_result,port_bt_shares= self.calc_port_bt_result_ver2(port_signalmatrix , port_price_matrix, is_equal_weight)
        # holding_level_pct = pandas.Series()
        # bt_ranking_para = bt_method_para
        # port_bt_result,port_bt_shares= self.calc_port_bt_result(port_scorerankingmatrix, port_price_matrix, bt_ranking_para , bt_ranking_random_sampling, is_equal_weight, holding_level_pct)
        obj_output.df_to_csv(port_bt_result,'port_bt_result.csv')
        if is_output_all_csv:
            obj_output.df_to_csv(port_bt_shares,'port_bt_shares.csv')
        # port_bt_result = pandas.read_csv(os.path.join(bt_result_2_folder_path,'port_bt_result.csv'),index_col = 'Date')
        # port_bt_result.index =map(pandas.to_datetime,port_bt_result.index)
        # port_bt_result.index.name='Date'
        #step 4:analysis
        perf_stats, fig, dict_period_perf = self.calc_port_bt_analysis(port_bt_result,False)
        perf_stats.index.name = strategyid 
        obj_output.df_to_csv(perf_stats,'perf_stats.csv')
        if fig != None:
            fig.savefig(os.path.join(bt_result_2_folder_path,'returns_tear_sheet.pdf'))
        for period_name in dict_period_perf.keys():
            obj_output.df_to_csv(dict_period_perf[period_name],'port_return_' + period_name + '.csv')
                
        print(perf_stats)
        logging.info("bt run:%s(%s):%s" % (strategyname,strategyid,str(round(perf_counter()-time_start,2))))
    def bt_run_test(self, startdate, enddate, stockpool_name, strategyid, bt_ranking_para, bt_rebalance_day, bt_rebalance_period_m):
        #用來產生對應基金持股之用
        time_start= perf_counter()
        is_output_all_csv = False
        obj_data = PORT_DATA(startdate, enddate, stockpool_name, strategyid)
        bt_folder_path = os.path.join(obj_data.bt_result_data_folder_path,'back_testing')
        strategy_profile = obj_data.strategy_profile
        tickerlist = obj_data.stockpool_tickerlist
        bt_rebalancedate_list = quantlib.get_periodic_date(startdate,enddate,bt_rebalance_day,bt_rebalance_period_m,2)
        strategyname = strategy_profile['strategyname']

        bt_para = { 'bt_ranking_para':bt_ranking_para,'bt_rebalance_day':bt_rebalance_day,'bt_rebalance_period_m':bt_rebalance_period_m}
        obj_output = PORT_OUTPUT(startdate, enddate, stockpool_name, strategyid, bt_folder_path, **bt_para)
        # date_list_all = quantlib.get_periodic_date(startdate- timedelta(days=1),enddate,-1,12)
        bt_result_2_folder_path= obj_output.bt_result_2_folder_path
 
        #para_setting.json
        para_setting={
            #data
            'startdate':startdate.strftime("%Y%m%d"),
            'enddate':enddate.strftime("%Y%m%d"),
            'stockpool':stockpool_name,
            #strategy
            'strategyid':strategyid,
            'strategyname':strategy_profile['strategyname'],
            'strategy_list':strategy_profile['measure'],
            'strategy_direction_list':strategy_profile['measure_dir'],
            'strategy_weighting_list':strategy_profile['weighting'],
            #bt
            'bt_ranking_para':'_'.join([str(x) for x in  bt_ranking_para]),
            'bt_rebalance_period_m': bt_rebalance_period_m,
            'bt_rebalance_day':bt_rebalance_day,
            #time
            'bt_time': datetime.now().strftime('%Y%m%d')         
            }
        json.dump(para_setting, open(os.path.join(bt_result_2_folder_path,'0_para_setting.json'), "w",encoding='utf-8-sig')) 

        #step 1-1:score_matrix 
        port_scorematrix_rankpct, dict_measure_rawdata = obj_data.port_scorematrix_by_measure()
        self.port_scorematrix_rankpct = port_scorematrix_rankpct
        for datatype in dict_measure_rawdata:
            obj_output.df_to_csv(dict_measure_rawdata[datatype],'port_scorematrix_rawdata_' + datatype + '.csv')

         #step 1-3: scoreranking_matrix
        extra_measurematrix_rankpct = None
        df_score_original,df_score_adjust,df_score_final,df_score_rank = obj_data.port_scorerankingmatrix(self.port_scorematrix_rankpct, bt_rebalancedate_list, extra_measurematrix_rankpct)
        port_scorerankingmatrix = df_score_rank
        obj_output.df_to_csv(df_score_rank,'port_scorerankingmatrix_rank.csv')
        obj_output.df_to_csv(df_score_original,'port_scorerankingmatrix_original.csv')


if __name__ == '__main__':  
    bt_result_data_folder_path = os.path.join(os.getcwd(),'port_backtest','bt_result_data')
    # df=pd.read_csv(os.path.join(bt_result_data_folder_path,'back_testing','ALL_201301_202107','[1_30_60]_[1_15]_25_MV_MS_12M_25_PB_20_MonthSale_YoY_15_ROE_15_Price_Return_67D','port_bt_result.csv'),index_col='Date')
    # df.index = pd.to_datetime(df.index)
    # obj=PORT_BT_ANALYSIS(df)
    # p=obj.calc_rebalance_period_return()
    # print(p)
    if True:
        #產生要分群測試之策略參數
        whole_strategy_profile = quantlib.Setting().get_setting_strategy()
        #=====參數設定 parameters=======
        bt_period_list = [(datetime(2013,1,1),datetime(2021,7,31))]
        #rebalace_date = quantlib.get_periodic_date(startdate,enddate,'FA',1)
        #screener_name_list =[['ALL'],['MV_1_300'],['MV_1_600'],['MV_301_600'],['MV_601_2000']]
        screener_name_list = [['ALL']]
        sector_excl_list = []# ['金融保險','建材營建','生技醫療']
        strategyid_list =['str_bt_v_v_g_g_q_m_1']
        # strategyid_list =['gt_quality_1','gt_momentum_1','gt_holding_1','gt_holding_2','gt_holding_3','gt_holding_4','str_bt_size_1','str_bt_v_1','str_bt_v_2','str_bt_v_3','str_bt_v_4','str_bt_q_1','str_bt_q_2','str_bt_q_3','str_bt_g_1','str_bt_g_2','str_bt_g_3','str_bt_g_4','str_bt_g_5','str_bt_g_6','str_bt_g_7','str_bt_g_8','str_bt_g_9','str_bt_g_10','str_bt_g_11','str_bt_m_1','str_bt_m_2','str_bt_m_3','str_bt_m_4','str_bt_r_1','str_bt_r_2','str_bt_r_3','str_bt_s_1','str_bt_s_2','str_bt_s_3','str_bt_size_q_1','str_bt_size_q_2','str_bt_v_v_1','str_bt_v_v_2','str_bt_v_v_3','str_bt_v_g_1','str_bt_v_g_2','str_bt_v_g_3','str_bt_v_g_4','str_bt_v_q_1','str_bt_v_q_2','str_bt_v_m_1','str_bt_v_m_2','str_bt_v_m_3','str_bt_v_m_4','str_bt_v_r_1','str_bt_v_r_2','str_bt_v_s_1','str_bt_v_s_2','str_bt_g_v_1','str_bt_g_v_2','str_bt_g_q_1','str_bt_g_m_1','str_bt_g_m_2','str_bt_g_r_1','str_bt_g_s_1','str_bt_g_g_1','str_bt_g_g_2','str_bt_g_g_3','str_bt_size_q_m_1','str_bt_v_g_q_1','str_bt_v_g_q_2','str_bt_v_g_m_1','str_bt_v_g_m_2','str_bt_v_g_m_3','str_bt_v_g_m_4','str_bt_v_g_r_1','str_bt_v_g_r_2','str_bt_v_g_s_1','str_bt_v_g_s_2','str_bt_g_v_q_1','str_bt_g_v_q_2','str_bt_g_v_m_1','str_bt_g_v_m_2','str_bt_g_v_m_3','str_bt_g_v_m_4','str_bt_g_v_m_5','str_bt_g_v_r_1','str_bt_g_v_r_2','str_bt_g_v_s_1','str_bt_g_v_s_2','str_bt_g_v_q_m_1','str_bt_v_g_q_m_1','str_bt_v_v_g_q_m_1','str_bt_g_v_v_q_m_1','str_bt_v_v_g_g_q_m_1','str_bt_v_v_g_g_q_m_2']
        #strategyid_list  = ['gt_quality_1','str_bt_size_1','str_bt_v_1','str_bt_v_2','str_bt_v_3','str_bt_v_4','str_bt_q_1','str_bt_q_2','str_bt_q_3','str_bt_g_1','str_bt_g_6','str_bt_m_1','str_bt_m_2','str_bt_m_4','str_bt_r_1','str_bt_r_2','str_bt_r_3','str_bt_s_1','str_bt_s_2','str_bt_s_3']
        gt_sampling_para_list=[('SD',11)]#[('SD',11),('FA',3),('RD',300, 4)]
        folder_suffix = ''# 'ex_sector'
        measure_data_folder_path = os.path.join(bt_result_data_folder_path,'port_measure_data')
        scenario_list = []
        for bt_period in bt_period_list:
            for screener_name in screener_name_list:
                for strategyid in strategyid_list:
                    for gt_sampling_para in gt_sampling_para_list:
                            scenario_list.append({
                                'strategyid':strategyid,
                                'startdate': bt_period[0],
                                'enddate': bt_period[1],
                                'stockpool_name': screener_name,
                                'sector_excl_list': sector_excl_list,
                                'gt_sampling_para': gt_sampling_para,
                                'folder_suffix' : folder_suffix
                            })
        print(pd.DataFrame(scenario_list))
        i=0
        for scenario in scenario_list:
            i+=1
            time_start = perf_counter()
            obj_gt = PORT_GT()
            obj_gt.gt_run(**scenario)
            print("gt scenario(%s/%s):%s" % (str(i),str(len(scenario_list)),str(round(perf_counter()-time_start,2))))
    if False: 
        #產生要回測之策略參數
        whole_strategy_profile = quantlib.Setting().get_setting_strategy()
        #=====參數設定 parameters=======
        bt_period_list = [(datetime(2013,1,1),datetime(2021,7,31))]
        #rebalace_date = quantlib.get_periodic_date(startdate,enddate,'FA',1)
        #screener_name_list =[['ALL'],['MV_1_300'],['MV_1_600'],['MV_301_600'],['MV_601_2000']]
        screener_name_list = [['MV_1_600']]
        sector_excl_list = ['金融保險','建材營建','生技醫療']
        strategyid_list = ['str_bt_g_11']#'str_rank_g_1','str_score_g_2',
        bt_method_para_list = [(1,30,30)] 
        bt_rebalance_para_list=[('SD',5,1)]#('SD',11,1),('FA',1)
        folder_suffix = 'ex_sector'
        measure_data_folder_path = os.path.join(bt_result_data_folder_path,'port_measure_data')
        scenario_list = []
        for bt_period in bt_period_list:
            for screener_name in screener_name_list:
                for strategyid in strategyid_list:
                    for bt_method_para in bt_method_para_list:
                        for bt_rebalance_para in bt_rebalance_para_list:
                                scenario_list.append({
                                    'strategyid':strategyid,
                                    'startdate': bt_period[0],
                                    'enddate': bt_period[1],
                                    'stockpool_name': screener_name,
                                    'sector_excl_list': sector_excl_list,
                                    'bt_method_para':bt_method_para,
                                    'bt_rebalance_para':bt_rebalance_para,
                                    'folder_suffix':folder_suffix
                                })
        print(pd.DataFrame(scenario_list))
        #執行回測
        i=0
        for scenario in scenario_list:
            i+=1
            time_start = perf_counter()
            obj_bt = PORT_BT()
            obj_bt.bt_run(**scenario)
            print("bt scenario(%s/%s):%s" % (str(i),str(len(scenario_list)),str(round(perf_counter()-time_start,2))))
    if False: #新功能套用所有資料夾
        bt_folder_path = os.path.join(os.getcwd(),'port_backtest','bt_result_data','back_testing')

        stockpool_list = ['ALL','MV_1_300','MV_1_600','MV_301_600','MV_601_2000']
        bt_period_list = ['201301_202107']
        for stockpool in stockpool_list:
            for bt_period in bt_period_list:
                stockpool_period_folder_path = os.path.join(bt_folder_path,stockpool + '_' + bt_period)
            
                for fd in os.listdir(stockpool_period_folder_path): 
                    strategy_folder_path = os.path.join(stockpool_period_folder_path,fd)
                    if os.path.isfile( os.path.join(strategy_folder_path,'port_bt_result.csv')):  
                        df=pd.read_csv(os.path.join(strategy_folder_path,'port_bt_result.csv'),index_col='Date')
                        df.index = pd.to_datetime(df.index)
                        obj=PORT_BT_ANALYSIS(df)
                        dict_period_perf = obj.calc_periodical_return()
                        period_name='rebalance_date'
                        dict_period_perf[period_name].to_csv(os.path.join(strategy_folder_path,'port_return_' + period_name + '.csv'),encoding ='utf-8-sig')
                      


