import sys,os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pygsheets
import quantlib
import pandas
import os
import collections
from datetime import datetime
#此路徑使用絕對路徑,因為在批次執行時才能找到路徑,用os.path.getcwd()找不到路徑
measure_monitor_folder_path = os.path.join("G:\\我的雲端硬碟\\投資_Qadris\\InvestQuant",'measure_monitor')
class strategy_monitor():
    def __init__(self,querydate) -> None:
        
        self.df_rawdata = pandas.read_csv(os.path.join(measure_monitor_folder_path,'data','measure_monitor_' + datetime.strftime(querydate,'%Y%m%d') + '.csv'))
        #單位w
        self.df_rawdata['市值_AVG_20D'] =self.df_rawdata['市值_AVG_20D'] / 100000000 
        self.df_rawdata['市值'] =self.df_rawdata['市值'] / 100000000 
        #基本顯示欄位
        self.dict_cols_basis_1= collections.OrderedDict()
        self.dict_cols_basis_1={
            'Ticker':'股票代號'
            ,'CorpName':'股票名稱'
            ,'Industry':'產業'
            ,'Sector':'子產業'
            ,'Date':'日期'
            ,'Close':'價格'
            ,'市值_AVG_20D':'市值(億_20日平均)'
            ,'Price_Return_22D':'月漲幅'
            ,'Price_Return_67D':'季漲幅'
            ,'MonthSale_YoY':'月營收YoY'
            ,'EPS4':'EPS(T4)'
            ,'毛利率4':'毛利率(T4)'
            ,'淨利率4':'淨利率(T4)'
            ,'ROE4':'ROE(T4)'
            ,'股利合計4':'股利(T4)'
            ,'股利殖利率':'股利殖利率'
            ,'全體及其關係人持股比例':'關係人持股'
            }       
    def beartzen(self,num = 2000):
        return self.df_rawdata.round(4)
    def MS_PB_ROE(self,num =100):
        req_cols=['Ticker','CorpName','Industry','Sector','市值_AVG_20D','市值_AVG_20D_Rank','Close','Price_Return_22D','Price_Return_22D_Rank','Price_Return_67D','Price_Return_67D_Rank','股利合計4','現金股利殖利率','股利殖利率','現金股利_平均_5YR','股利_平均_5YR','股利殖利率_平均_5YR','全體及其關係人持股比例','日期_月營收_資料日','ROE','ROE_Rank','PB','PB_Rank','MonthSale_YoY','MonthSale_YoY_Rank']       
        df_result=self.df_rawdata[req_cols]
        #新增
        df_result['MS+PB+ROE Score'] = df_result['ROE_Rank'] + df_result['PB_Rank'] + df_result['MonthSale_YoY_Rank']
        df_result['MS+PB+ROE Rank'] = df_result['MS+PB+ROE Score'].rank( method='max',na_option='keep',pct=False,ascending = False) 
        df_result.sort_values('MS+PB+ROE Rank',ascending=True,inplace=True)
        
        #單位
        df_result = df_result.round(4)
        #欄位
        df_result.columns=['代碼','公司','產業_1','產業_2','市值_近20日平均(億元)','市值排名','股價','22天股價變化','22天股價排名','67天股價變化','67天股價排名','總股利4','現金股利殖利率','股利殖利率','現金股利_平均_5YR','股利_平均_5YR','股利殖利率_平均_5YR','內部人持股比例','日期_月營收_資料日','ROE','ROE_Score','PB','PB_Score','MonthSale_YoY','MonthSale_YoY_Score','MS+PB+ROE Score','MS+PB+ROE Rank']
       
        return df_result.iloc[:num]
    def str_rank_g_v_q_1(self,num = 2000):
        '''50_MonthSale_YoY_30_PB_20_ROE'''
        #策略所需欄位設定
        dict_cols_strategy = {
             'MonthSale_YoY_rankscore':'MonthSale_YoY_rankscore'
             ,'PB_rankscore':'PB_rankscore'      
             ,'ROE_rankscore':'ROE_rankscore'  
        }
        dict_cols_req=collections.OrderedDict()
        dict_cols_req.update(self.dict_cols_basis_1)
        dict_cols_req.update(dict_cols_strategy)
        df_result=self.df_rawdata[list(dict_cols_req.keys())]
        #策略分數
        df_result['Strategy RankScore'] = 0.5*df_result['MonthSale_YoY_rankscore'] + 0.3*df_result['PB_rankscore'] + 0.2*df_result['ROE_rankscore']
        df_result['Strategy Rank'] = df_result['Strategy RankScore'].rank( method='max',na_option='keep',pct=False,ascending = False) 

        #顯示欄位名稱 
        df_result.columns = [dict_cols_req[x]  for x in list(dict_cols_req.keys()) if x not in ['Strategy RankScore','Strategy Rank']] + ['Strategy RankScore','Strategy Rank']

        df_result.sort_values('Strategy Rank',ascending=True,inplace=True)
        return df_result.iloc[:num].round(4)
    def str_rank_v_v_g_q_m_1(self,num = 2000):
        '''25_MV_MS_12M_25_PB_20_MonthSale_YoY_15_ROE_15_Price_Return_67D'''
        #策略所需欄位設定
        dict_cols_strategy = {
             'MV_MS_12M_rankscore':'MV_MS_12M_RankScore'
             ,'PB_rankscore':'PB_rankscore'
             ,'MonthSale_YoY_rankscore':'MonthSale_YoY_rankscore'
             ,'ROE_rankscore':'ROE_rankscore'
             ,'Price_Return_67D_rankscore':'Price_Return_67D_rankscore'        
        }
        dict_cols_req=collections.OrderedDict()
        dict_cols_req.update(self.dict_cols_basis_1)
        dict_cols_req.update(dict_cols_strategy)
        df_result=self.df_rawdata[list(dict_cols_req.keys())]
        #策略分數
        df_result['Strategy RankScore'] = 0.25*df_result['MV_MS_12M_rankscore'] + 0.25*df_result['PB_rankscore'] + 0.2*df_result['MonthSale_YoY_rankscore'] + 0.15*df_result['ROE_rankscore']+0.15*df_result['Price_Return_67D_rankscore']
        df_result['Strategy Rank'] = df_result['Strategy RankScore'].rank( method='max',na_option='keep',pct=False,ascending = False) 

        #顯示欄位名稱 
        df_result.columns = [dict_cols_req[x]  for x in list(dict_cols_req.keys()) if x not in ['Strategy RankScore','Strategy Rank']] + ['Strategy RankScore','Strategy Rank']

        df_result.sort_values('Strategy Rank',ascending=True,inplace=True)
        return df_result.iloc[:num].round(4)
    def str_rank_v_g_r_2(self,num = 2000):
        '''50_MV_MS_12M_30_MonthSale_YoY_20_Beta係數250D'''
        #策略所需欄位設定
        dict_cols_strategy = {
             'MV_MS_12M_rankscore':'MV_MS_12M_RankScore'
             ,'MonthSale_YoY_rankscore':'MonthSale_YoY_RankScore'
             ,'Beta係數250D_rankscore':'Beta係數250D_RankScore'        
        }
        dict_cols_req=collections.OrderedDict()
        dict_cols_req.update(self.dict_cols_basis_1)
        dict_cols_req.update(dict_cols_strategy)
        df_result=self.df_rawdata[list(dict_cols_req.keys())]
        #策略分數
        df_result['Strategy RankScore'] = 0.5*df_result['MV_MS_12M_rankscore'] + 0.3*df_result['MonthSale_YoY_rankscore'] + 0.2*df_result['Beta係數250D_rankscore']
        df_result['Strategy Rank'] = df_result['Strategy RankScore'].rank( method='max',na_option='keep',pct=False,ascending = False) 

        #顯示欄位名稱 
        df_result.columns = [dict_cols_req[x]  for x in list(dict_cols_req.keys()) if x not in ['Strategy RankScore','Strategy Rank']] + ['Strategy RankScore','Strategy Rank']

        df_result.sort_values('Strategy Rank',ascending=True,inplace=True)
        return df_result.iloc[:num].round(4)
    def str_value_invest_1(self,num = 2000):
        '''自訂篩選標準'''
        #策略所需欄位設定
        dict_cols_strategy={
            'Ticker':'股票代號'
            ,'CorpName':'股票名稱'
            ,'Industry':'產業'
            ,'Sector':'子產業'
            ,'Date':'日期'
            ,'Close':'價格'
            ,'市值_AVG_20D':'市值(億_20日平均)'
            ,'Price_Return_22D':'月漲幅'
            ,'Price_Return_67D':'季漲幅'
            ,'MonthSale_YoY':'月營收YoY'
            ,'股利合計4':'總股利'
            ,'現金股利殖利率':'現金股利殖利率'
            ,'股利殖利率':'股利殖利率'
            ,'股利殖利率_平均_5YR':'股利殖利率_平均_5YR'
            ,'ROE4':'ROE(T4)'
            ,'ROE4_AVG_5yr':'ROE4_AVG_5yr'
            ,'ROE4_CV_5yr':'ROE4_CV_5yr'
            ,'PB':'PB'
            ,'PB_Percentile_5yr':'PB_Percentile_5yr'
            ,'負債比率':'負債比率'
            ,'負債比率_AVG_3yr':'負債比率_AVG_3yr'      
            ,'1000張以上佔集保比率':'1000張以上佔集保比率'
            ,'全體及其關係人持股比例':'關係人持股'
            }  
        dict_cols_req=collections.OrderedDict()
        dict_cols_req.update(dict_cols_strategy)
        df_result=self.df_rawdata[list(dict_cols_req.keys())]
        #篩選
        mask_1 = (df_result['股利殖利率_平均_5YR'] >= 0.05) & (df_result['股利殖利率']>=0.05) & (df_result['ROE4_AVG_5yr']>=0.1) & (df_result['ROE4_CV_5yr']>=8)
        
        df_result = df_result[mask_1]
        #顯示欄位名稱 
        df_result.columns = [dict_cols_req[x]  for x in list(dict_cols_req.keys())] 
        df_result.sort_values('股利殖利率',ascending=False,inplace=True)
        return df_result.iloc[:num].round(4)
    def str_rank_size_q_1(self,num = 2000):
        '''80_MV_20_ROE'''

        #策略所需欄位設定
        dict_cols_strategy = {
             '市值_rankscore':'MV_RankScore'
             ,'ROE_rankscore':'ROE_RankScore'

        }
        dict_cols_req=collections.OrderedDict()
        dict_cols_req.update(self.dict_cols_basis_1)
        dict_cols_req.update(dict_cols_strategy)
        df_result=self.df_rawdata[list(dict_cols_req.keys())]
        #策略分數
        df_result['Strategy RankScore'] = 0.8*df_result['市值_rankscore'] + 0.2*df_result['ROE_rankscore'] 
        df_result['Strategy Rank'] = df_result['Strategy RankScore'].rank( method='max',na_option='keep',pct=False,ascending = False) 

        #顯示欄位名稱 
        df_result.columns = [dict_cols_req[x]  for x in list(dict_cols_req.keys()) if x not in ['Strategy RankScore','Strategy Rank']] + ['Strategy RankScore','Strategy Rank']
        df_result.sort_values('Strategy Rank',ascending=True,inplace=True)
        return df_result.iloc[:num].round(4)
def output_gsheet(api_key_json_path, gsheet_key, wks_name,df_data, sht_num_limit=30):
    '''sht_num_limit:由於gsheet的儲存個上限為1000萬個,對於貝兒讚欄位較多,若工作表又太多的話,很容易超過限制'''
    gc = pygsheets.authorize(service_file = api_key_json_path)
    gsht = gc.open_by_key(gsheet_key)
    #刪除index大於sht_num_limit
    shts=gsht.worksheets()
    for sht in shts[sht_num_limit:]:
        gsht.del_worksheet(gsht.worksheet_by_title(sht.title))
    #新增或更新wks_name資料表
    try:
        sht = gsht.worksheet_by_title(wks_name)
        gsht.del_worksheet(sht)
    except pygsheets.WorksheetNotFound as error:
        pass
    new_sht = gsht.add_worksheet(wks_name,index=1,rows=len(df_data),cols=len(df_data.columns))
    #去除Nan,避免NaN appsheet無法讀取
    df_data = df_data.fillna('')
    new_sht.set_dataframe(df_data,start="A1")

def main(QueryDate):
    obj=strategy_monitor(QueryDate)
    chubear_api_key_json_path = os.path.join(quantlib.get_key_path(), 'client_secret_chubear.json')
    qadris_api_key_json_path = os.path.join(quantlib.get_key_path(), 'client_secret_qadris.json')    
    fg_quant_api_key_json_path = os.path.join(quantlib.get_key_path(), 'client_secret_fgquant.json') 
    exec_info = {
                'beartzen_qadris':{'api_key_json_path':qadris_api_key_json_path,'gsht_key':'1tZzn0fNK0WWLmkzB4izjyxvaBJllYkEiemcLeIT_hr0','func':'obj.beartzen()','sht_num_limit':3},   
                'beartzen_fg':{'api_key_json_path':fg_quant_api_key_json_path,'gsht_key':'1LaMy3Y1f50ewVsqRX15RnGVUoeYB1xBeA_sAOSi-cu8','func':'obj.beartzen()','sht_num_limit':3},   
               # 'beartze_global_fg':{'api_key_json_path':fg_quant_api_key_json_path,'gsht_key':'1LaMy3Y1f50ewVsqRX15RnGVUoeYB1xBeA_sAOSi-cu8','func':'obj.beartzen_global()','sht_num_limit':3},   

                'str_rank_g_v_q_1':{'api_key_json_path':chubear_api_key_json_path,'gsht_key':'1lvUnfZkQA-0uGYK8fCVzX6LNPyPFAztk8ZCVxAKKOLg','func':'obj.str_rank_g_v_q_1()','sht_num_limit':30},
                'str_rank_v_v_g_q_m_1':{'api_key_json_path':chubear_api_key_json_path,'gsht_key':'1fYxBM-L2nmeMwfVgPCBsKYEcru10vGKUtpIOwT19Sgw','func':'obj.str_rank_v_v_g_q_m_1()','sht_num_limit':30},
                'str_rank_v_g_r_2':{'api_key_json_path':chubear_api_key_json_path,'gsht_key':'1iiSBhvBmoBJ567Ll8u-0QhdfDV96zxfXHaIHZzSy1io','func':'obj.str_rank_v_g_r_2()','sht_num_limit':30},   
                'str_rank_size_q_1':{'api_key_json_path':chubear_api_key_json_path,'gsht_key':'14ZgUXgUasovqpy-yOFC4qAPlz5vBoaUcNaDbzad94g0','func':'obj.str_rank_size_q_1()','sht_num_limit':30},
                'str_value_invest_1':{'api_key_json_path':chubear_api_key_json_path,'gsht_key':'1gBPM5lcYRmUmvUbsiE4B5uxh5fzc-Ix16H__fvIEwR8','func':'obj.str_value_invest_1()','sht_num_limit':30}       
                }    
       
    for item in exec_info:
        wks_name = datetime.strftime(QueryDate,'%Y%m%d')
        func = exec_info[item]['func']
        api_key_json_path = exec_info[item]['api_key_json_path']
        gsht_key = exec_info[item]['gsht_key']
        sht_num_limit = exec_info[item]['sht_num_limit']
        df_data = eval(func)
        output_gsheet(api_key_json_path,gsht_key,wks_name,df_data,sht_num_limit)
        output_gsheet(api_key_json_path,gsht_key,'Latest',df_data,sht_num_limit)
if __name__=='__main__':
    QueryDate=datetime(2022,4,25)
    main(QueryDate)

    