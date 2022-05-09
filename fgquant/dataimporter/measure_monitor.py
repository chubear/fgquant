import os,sys

from numpy.core.fromnumeric import sort
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dataretriever import *
from measureretriever import *
from datetime import datetime
import os,csv
import pandas 
import quantlib
from dateutil.relativedelta import relativedelta
import logging as log
from time import perf_counter
log.basicConfig(level=log.ERROR, format='%(asctime)s - %(levelname)s : %(message)s')
parent_path = os.path.dirname(__file__)
def output_measure_csv_from_db(QueryDate,
                             watchlist,
                             measure_list):
    time_start = perf_counter()
    #csv_path=os.path.join(parent_path,'measure_' + QueryDate.strftime('%Y%m%d') + '.csv')
    obj = MeasureRetriever()
    dict_result=obj.query(QueryDate, QueryDate, measure_list, watchlist,'date')
    print(dict_result)
    #dict_result[QueryDate].to_csv(csv_path,index=True,header=True)

    print("output_measure_csv: %s" % str(perf_counter()-time_start))
    return None

def output_measure_csv(QueryDate,
                watchlist,
                measure_list,
                is_replace_data = False):
    '''將回傳標準的資料格式，例:
        若單位為元:則傳回千元,
        若單位為%,則預設傳回0.xx'''

    time_start = perf_counter()
    StartDate = QueryDate - relativedelta(years=12)
    EndDate = QueryDate
    #建立CSV
    csv_path=os.path.join(parent_path,'measure_' + QueryDate.strftime('%Y%m%d') + '.csv')
    
    if os.path.exists(csv_path):#若檔案存在則取得最後Ticker
        df=pandas.read_csv(csv_path)
        if len(df[df['Ticker'].isna()].index): #清除na資料,因為有時會執行到一半斷線後出現很多,,,,,,
            df = df[~df['Ticker'].isna()]
            df.to_csv(csv_path, mode='w',index=False,header=True,encoding='utf-8-sig')
        if is_replace_data:
            watchlist=[int(x) for x in watchlist]
            df = df[~df['Ticker'].isin(watchlist)]
            df.to_csv(csv_path, mode='w',index=False,header=True,encoding='utf-8-sig')
        else:
            latest_Ticker = df.iloc[-1]['Ticker']
            watchlist = [x for x in watchlist if x>str(latest_Ticker)]
    else:
        with open(csv_path, 'w', newline='',encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(measure_list)

    #取的measure資料
    i=1
    for ticker in watchlist:
        if i % 100 == 1: #每執行100次重新連結
            db_conn =quantlib.get_db_conn()
        time_start1=perf_counter()
        obj=DataRetriever(ticker,db_conn) 
        result=obj.query(StartDate, EndDate, measure_list, [], 2,fillna_method='ffill')
        if not result.empty:
            result.loc[result.index<=QueryDate].loc[result.index[-1]].to_frame().transpose().to_csv(csv_path, mode='a',index=False,header=False)
        print("%s: %s" % (ticker,str(round(perf_counter()-time_start1,4))))
    print("output_measure_csv: %s" % str(perf_counter()-time_start))
    return None
def get_measure_monitor(csv_path,extra_measure_list):
    time_start = perf_counter()
    #取得csv資料
    df_measure = pandas.read_csv(csv_path,encoding='utf8')

    #Modified to standard format
    db_measure_profile = quantlib.Setting().get_setting_db_measure()
    func_measure_profile = quantlib.Setting().get_setting_func_measure()
    measure_profile = dict(**func_measure_profile,**db_measure_profile)
    for col in df_measure.columns:
        if col.upper() in list(measure_profile.keys()):
            col_type = measure_profile[col.upper()]['col_type']
            col_unit = measure_profile[col.upper()]['col_unit']
            if col_type in ('double','int'):
                unit_number , unit = ('','')if col_unit == '' else col_unit.split("_") 
                unit_number = 1 if unit_number == '' else float(unit_number)
                if unit == '%':
                    df_measure[col] = df_measure[col] * unit_number /100 
                else:
                    df_measure[col] = df_measure[col] * unit_number 
    #Scoring
    if 'Price_Return_67D_rankscore' in extra_measure_list:
        df_measure['Price_Return_67D_rankscore'] = df_measure['Price_Return_67D'].rank( method='max',na_option='keep',pct=True,ascending = True) *100
    if 'Price_Return_22D_rankscore' in extra_measure_list:
        df_measure['Price_Return_22D_rankscore'] = df_measure['Price_Return_22D'].rank( method='max',na_option='keep',pct=True,ascending = True) *100
    if 'ROE_rankscore' in extra_measure_list:
        df_measure['ROE_rankscore'] = df_measure['ROE'].rank( method='max',na_option='keep',pct=True,ascending = True) *100
    if 'MonthSale_YoY_rankscore' in extra_measure_list:
        df_measure['MonthSale_YoY_rankscore'] = df_measure['MonthSale_YoY'].rank( method='max',na_option='keep',pct=True,ascending = True) *100
    if 'PB_rankscore' in extra_measure_list:
        df_measure['PB_rankscore'] = df_measure['PB'].rank( method='max',na_option='keep',pct=True,ascending = False) *100
    if '市值_rankscore' in extra_measure_list:
        df_measure['市值_rankscore'] = df_measure['市值'].rank( method='max',na_option='keep',pct=True,ascending = False) *100
    if '市值_AVG_20D_rankscore' in extra_measure_list:
        df_measure['市值_AVG_20D_rankscore'] = df_measure['市值_AVG_20D'].rank( method='max',na_option='keep',pct=True,ascending = False) *100
    if '成交值比重_MA_5D_rankscore' in extra_measure_list:
        df_measure['成交值比重_MA_5D_rankscore'] = df_measure['成交值比重_MA_5D'].rank( method='max',na_option='keep',pct=True,ascending = True) *100
    if 'MV_MS_12M_rankscore' in extra_measure_list:
        df_measure['MV_MS_12M_rankscore'] = df_measure['MV_MS_12M'].rank( method='max',na_option='keep',pct=True,ascending = False) *100
    if 'Beta係數250D_rankscore' in extra_measure_list:
        df_measure['Beta係數250D_rankscore'] = df_measure['Beta係數250D'].rank( method='max',na_option='keep',pct=True,ascending = False) *100
    if 'PB_Percentile_5yr_rankscore' in extra_measure_list:
        df_measure['PB_Percentile_5yr_rankscore'] = df_measure['PB_Percentile_5yr'].rank( method='max',na_option='keep',pct=True,ascending = True) *100
    if 'QII_HighValue_Growth_rankscore' in extra_measure_list:
        df_measure['QII_HighValue_Growth_rankscore'] = 0.25*df_measure['MV_MS_12M_rankscore'] + 0.25*df_measure['PB_rankscore']+0.2*df_measure['MonthSale_YoY_rankscore']+0.15*df_measure['ROE_rankscore']+0.15*df_measure['Price_Return_67D_rankscore']

    print("output_measure_csv: %s" % str(perf_counter()-time_start))
    return df_measure

def main(QueryDate):
    time_start = perf_counter()
    #QueryDate = (datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
    #QueryDate = datetime(2021,7,8)
    output_date_list = [QueryDate]    
    watchlist = quantlib.get_watchlist('all',QueryDate)
    # watchlist =['6227','6228','6229','6230','6231','6242','6243','6251','6257','6259','6261','6263','6276','6277','6284','6285','6287','6288','6289','6412','6414','6425','6426','6431','6432','6435','6457','6461','6472','6477','6482','6485','6486','6506','6508','6516','6523','6525','6527','6530','6548','6552','6569','6570','6573','6574','6576','6591','6592','6605','6609','6612','6613','6615','6649','6651','6666','6667','6668','6669','6670','6691','6693','6715','6716','6719','6727','6728','6756','6761','6776','6781','6788','6790','6792','8027','8028','8040','8042','8043','8044','8046','8067','8068','8076','8077','8081','8083','8093','8096','8105','8107','8109','8110','8111','8171','8176','8215','8222','8234','8240','8249','8299','8341','8374','8383','8390','8401','8403','8420','8421','8429','8431','8432','8433','8435','8450','8454','8467','8472','8473','8476','8477','8906','8908','8926','8927','8928','8929','8930','8941','8942','9908','9910','9919','9921','9924','9925','9926','9937','9938','9944','9945','9946','9949','9950','1103','1110','1236','1316','1402','1416','1432','1507','1517','1519','1521','1580','1605','1609','1702','1712','1776','1806','1810','1905','2024','2025','2062','2206','2207','2312','2316','2401','2404','2406','2412','2424','2442','2443','2454','2462','2466','2468','2491','2542','2543','2607','2608','2609','2615','2617','2618','2724','2726','2739','2838','2867','2880','2881','2882','2885','2890','2891','2905','2906','2912','2923','2924','2926','2929','2939','2945','3045','3147','3162','3205','3288','3313','3356','3379','3434','3504','3540','3557','3593','3605','3607','3632','3702','3704','3710','4164','4190','4198','4304','4401','4414','4510','4523','4714','4728','4745','4804','4807','4904','4930','5287','5371','5443','5530','5546','5820','5871','5903','6108','6111','6122','6141','6196','6220','6419','6464','6499','6616','6640','6654','8085','8354','8482','8489','8499','8917','9904','9934']

    # #watchlist =['1101','2303']
    measure_basic =  ['Ticker','CorpName','Industry','Sector','上市日期','上櫃日期']
    measure_ta = ['Date','市值','市值_AVG_20D','Close','Price_Return_1D','Price_Return_5D','Price_Return_22D','Price_Return_67D','Volume','Volume_MA_5D','Amount','Amount_MA_5D','成交值比重_MA_5D','Beta係數21D','Beta係數65D','Beta係數250D']
    measure_ms = ['日期_月營收_公布日','日期_月營收_資料日','單月合併月營收','累計合併月營收','MonthSale_YoY','MonthSale_Acc_YoY']
    measure_structure = ['集保比例變化_400張以上_4W','集保比例變化_1000張以上_4W','集保比例變化_400張以上_12W','集保比例變化_1000張以上_12W','集保張數變化_1000張以上_4W','集保張數變化_1000張以上_12W','10張以下佔集保比率','1000張以上佔集保比率']
    measure_fd = ['外資買賣超','外資買賣超金額','外資持股比率','外資持股成本','自營商買賣超_自行買賣','自營商持股比率','投信持股比率']
    measure_insider = ['日期_董監持股_資料日','董監及其關係人持股張數增減','董監及其關係人持股比例','董監及其關係人持股比例增減','大股東及其關係人持股張數增減','大股東及其關係人持股比例','大股東及其關係人持股比例增減','內部人及其關係人持股張數增減','內部人及其關係人持股比例','內部人及其關係人持股比例增減','全體及其關係人持股張數增減','全體及其關係人持股比例','全體及其關係人持股比例增減','董監及其關係人設質張數增減','董監及其關係人設質比例','董監及其關係人設質比例增減','內部人及其關係人設質張數增減','內部人及其關係人設質比例','內部人及其關係人設質比例增減','全體及其關係人設質張數增減','全體及其關係人設質比例','全體及其關係人設質比例增減']
    measure_fa = ['日期_財報_公告日','日期_財報_資料日','毛利率','淨利率','毛利率4','淨利率4','營業收入淨額4','營業毛利4','稅後純益4','原始每股稅後盈餘4','公告每股淨值','普通股股本'] 
    measure_fa_ratio=['ROE','ROE4','ROE4_AVG_3yr','ROE4_AVG_5yr','ROE4_AVG_10yr','ROE4_CV_5yr','ROE4_CV_10yr','EPS','EPS4','EPS4_AVG_3yr','EPS4_AVG_5yr','EPS4_AVG_10yr','PB','PB_Percentile_3yr','PB_Percentile_5yr','PB_Percentile_10yr','PE','PE4','PE4_Percentile_3yr','PE4_Percentile_5yr','PE4_Percentile_10yr','盈再率4','盈再率_3yr','盈再率_5yr','盈再率_10yr','負債比率','負債比率_AVG_3yr','負債比率_AVG_5yr','負債比率_AVG_10yr']
    measure_dividend=['日期_股利_資料日','除權日','除息日','領股日期','領息日期','董事會決議通過股利分派日','股東會日期','公告日期','日期_股利_公告日','盈餘分派頻率','現金股利合計4','股利合計4','現金股利殖利率','股利殖利率','現金股利_平均_3YR','現金股利_平均_5YR','現金股利_平均_10YR','股票股利_平均_3YR','股票股利_平均_5YR','股票股利_平均_10YR','現金股利發放率_平均_3yr','現金股利發放率_平均_5yr','現金股利發放率_平均_10yr','股利_平均_3YR','股利_平均_5YR','股利_平均_10YR','股利殖利率_平均_5YR','股利殖利率_平均_5YR','股利殖利率_平均_10YR','Dividend_CV_5yr']
    
    measure_fg_weekly_report = ['Price_MA_5D','普通股股本','市值','母公司業主_稅後純益4','權益總計','外資買賣超金額_Sum_5D','自營商買賣超金額_Sum_5D','投信買賣超金額_Sum_5D','集保張數變化_400張以上_1W','集保張數變化_1000張以上_1W','Amount_MA_5D']

    measure_quant = ['市值','MonthSale_YoY','ROE','MV_MS_12M','PB','Price_Return_67D','Beta係數250D','PB_Percentile_5yr']
    #以下在get_measure_monitor程式中計算,也要列出需要計算欄位
    measure_list = measure_basic + measure_ta+measure_ms+measure_structure+measure_fd+measure_insider+measure_fa+measure_fa_ratio + measure_dividend +measure_fg_weekly_report + measure_quant
    measure_list = list(dict.fromkeys(measure_list))
    if '' in measure_list:measure_list.remove('')
    # #measure_list =  ['Ticker','ROE','ROE4','ROE4_AVG_3yr','ROE4_AVG_5yr']
    # #measure_list =measure_basic + measure_ta + measure_dividend + measure_fa_ratio 
    output_measure_csv(QueryDate,watchlist,measure_list,False)

    #Score/Rank
    measure_rankscore = ['Price_Return_22D_rankscore','Price_Return_67D_rankscore','ROE_rankscore','MonthSale_YoY_rankscore','成交值比重_MA_5D_rankscore','PB_rankscore','市值_rankscore','市值_AVG_20D_rankscore','MV_MS_12M_rankscore','Beta係數250D_rankscore','PB_Percentile_5yr_rankscore']
    measure_strategy_rankscore = ['QII_HighValue_Growth_rankscore']
    csv_path=os.path.join(parent_path,'measure_' + QueryDate.strftime('%Y%m%d') + '.csv')
    df_measure = get_measure_monitor(csv_path,measure_rankscore+measure_strategy_rankscore)
    
    errlist=list(set(watchlist) - set(list(df_measure['Ticker'].apply(lambda x:str(x)))))
    if errlist !=[]:
        log.debug("Ticker inccured error: %s" % ','.join(errlist))
    #去除重複Ticker
    df_measure =df_measure.drop_duplicates('Ticker')    
    #print(df_measure)
    #貝兒讚
    folder_path = os.path.join(parent_path,'data')
    df_measure.to_csv(os.path.join(folder_path,'measure_monitor_' + QueryDate.strftime('%Y%m%d') +'.csv'),index= False,encoding='utf-8-sig')
    from pathlib import Path
    folder_path = os.path.join(Path(parent_path).parent.parent,'貝兒讚','data')
    df_measure.to_csv(os.path.join(folder_path,'measure_monitor_' + QueryDate.strftime('%Y%m%d') +'.csv'),index= False,encoding='utf-8-sig')
    print("exec duration: %s" % (str(perf_counter()-time_start)))
if __name__=='__main__': 
    QueryDate=datetime(2022,4,25)
    # watchlist = quantlib.ge
    # t_watchlist('MV_1_100',QueryDate)
    # output_measure_csv_from_db(QueryDate,watchlist,['市值','Close'])
    main(QueryDate)

