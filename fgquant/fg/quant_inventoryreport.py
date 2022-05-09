import os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  #InvestQuant
from dataretriever import *
from datetime import datetime,timedelta
from quantlib import *
import pygsheets
db_conn=quantlib.get_db_conn()


def get_inventoryreport(QueryDate,Date_lastweek,portfolioid,df_scorematrix,df_traderecord):
    '''透過日損益報表資料計算,會取得資料庫資料'''
    df_traderecord =df_traderecord.loc[(df_traderecord['交易方向']=='B')]
    df_traderecord.drop_duplicates(['股票代號'], keep='last',inplace=True)
    df_traderecord.set_index('股票代號',inplace=True) 
    #print(df_traderecord)  
    #df_traderecord.to_csv(os.path.join(data_folder_path,portfolioid + 'test.csv'))
    #計算庫存資訊
    db_conn = quantlib.get_db_conn(user = 'datateam',db='equityreportdb')
    _SQL="SELECT A.`Date`,A.`SecurityID`,A.`SecurityName`,A.`Unit_Inv`,A.`UnitCost_Original_LC`,A.`MarketPrice`,A.`MarketValue_LC`,A.`UnRePL_MTM_Original_TWD`,A.`RePL_Dividend_Original` as DVD, A.`UnRePL_MTM_Original_TWD` - coalesce(B.`UnRePL_MTM_Original_TWD`,0) as PL_Chg_W,A.`RePL_Dividend_Original`-coalesce(B.`RePL_Dividend_Original`,0) as DVD_W FROM (SELECT * FROM  `eq_dailyinventory` WHERE DATE='" + QueryDate.strftime('%Y%m%d') + "' AND Unit_Inv>0) A LEFT JOIN (SELECT * FROM  `eq_dailyinventory` WHERE DATE='" + Date_lastweek.strftime('%Y%m%d') + "' AND Unit_Inv>0) B ON A.SecurityID=B.SecurityID and A.PortfolioId = B.PortfolioId Where A.PortfolioId = '" + portfolioid + "' order by A.SecurityID Asc"
    df_inventory =  pandas.read_sql_query(_SQL,db_conn,index_col='SecurityID')
    print(df_inventory)
    df_inventory['Date'] = df_inventory['Date'].apply(lambda x : datetime.strptime(str(x), "%Y-%m-%d"))
    df_inventory.index = map(int, df_inventory.index)
    df = df_inventory.join(df_traderecord,how='left')
    df.index.name = 'Ticker'

    df['持有天數'] = (df['Date']-df['交易日期']).apply(lambda x : x.days)
    df['Return_BTD'] = df['MarketPrice' ]/df['UnitCost_Original_LC'] -1
    #df=df[['Date','SecurityName','交易日期','持有天數','Unit_Inv','UnitCost_Reset_LC','MarketPrice','MarketValue_LC','UnRePL_MTM_Reset_TWD','PL_Chg_W']]
    df['PL_Chg_W'].fillna(df['UnRePL_MTM_Original_TWD'], inplace=True)
    df['DVD_W'].fillna(0, inplace=True)
    df['Return_W'] = (df['PL_Chg_W']+df['DVD_W'])/(df['MarketValue_LC'] - df['PL_Chg_W'])

    
    df['initial_score'] = numpy.zeros(len(df.index))
    df['Current_score'] = numpy.zeros(len(df.index))
    #取得Score
    for i in range(len(df.index)):
        ticker = df.index[i]
        tradedate=df.iloc[i]['交易日期']
        try:
            a=df_scorematrix.loc[tradedate.to_pydatetime()-timedelta(days=1),str(ticker)]
        except:
            a=0
        df.at[ticker,'initial_score'] = a
        b=df_scorematrix.loc[QueryDate,str(ticker)]
        df.at[ticker,'Current_score'] = b
        df.iloc[i]['initial_score'] = a
    #,'交易日期','持有天數','Unit_Inv','UnitCost_Reset_LC','MarketPrice','MarketValue_LC','UnRePL_MTM_Reset_TWD','PL_Chg_W']])
    df = df[['SecurityName','交易日期','持有天數','Unit_Inv','UnitCost_Original_LC','MarketPrice','UnRePL_MTM_Original_TWD','DVD','Return_BTD','PL_Chg_W','Return_W','initial_score','Current_score']]
    df.columns = ['股票名稱','買進日期','持有天數','股數(千)','成本','市價','未實現損益(千)','股利(千)','報酬率_BTD','未實現損益_週變動(千)','週報酬率','買進分數','目前分數']
    #格式
    df['股數(千)'] = df['股數(千)']/1000 #將股數轉為張數
    df['未實現損益(千)'] = df['未實現損益(千)']/1000 #改變單位
    df['未實現損益_週變動(千)'] = df['未實現損益_週變動(千)']/1000 #改變單位
    return df

def get_trade_on_rebalancedate(RebalanceDate,df_scorematrix,df_traderecord,df_inventory):
    df_traderecord.drop_duplicates(['股票代號'], keep='last',inplace=True)
    df_traderecord.set_index('股票代號',inplace=True)   
    #RebalanceDate 前後三天之交易單
    df_traderecord= df_traderecord.loc[(df_traderecord['交易日期']>= RebalanceDate - timedelta(days=3)) & (df_traderecord['交易日期']<= RebalanceDate + timedelta(days=3))]
    
    # #賣出交易單資訊
    df_traderecord_Sold= df_traderecord.loc[(df_traderecord['交易方向'] =='S')]
    df_traderecord_Sold.columns = df_traderecord_Sold.columns.str.replace('交易日期', '賣出日期')
    df_traderecord_Sold.columns = df_traderecord_Sold.columns.str.replace('交易價位', '賣出價格')
    #取得RebalanceDate前庫存資訊

    df_inventory.columns = df_inventory.columns.str.replace('成本', '買進價格')    
    df_traderecord_Sold = df_traderecord_Sold.join(df_inventory[['股票名稱','買進日期','買進價格','股數(千)','股利(千)','買進分數']],how='left')
    df_traderecord_Sold.fillna(0,inplace=True)
    df_traderecord_Sold['交易數量(張)'] = df_traderecord_Sold['股數(千)']
    df_traderecord_Sold['持有天數'] = (df_traderecord_Sold['賣出日期']-df_traderecord_Sold['買進日期']).apply(lambda x : x.days) 
    df_traderecord_Sold['出售損益'] =  df_traderecord_Sold['交易數量'] *  (df_traderecord_Sold['賣出價格'] - df_traderecord_Sold['買進價格'])
    df_traderecord_Sold['報酬率'] =  (df_traderecord_Sold['出售損益'] + df_traderecord_Sold['股利(千)']) / (df_traderecord_Sold['買進價格'] * df_traderecord_Sold['股數(千)'])/1000
    df_traderecord_Sold['賣出分數'] = numpy.zeros(len(df_traderecord_Sold.index))   

    for i in range(len(df_traderecord_Sold.index)):
        ticker = df_traderecord_Sold.index[i]
        tradedate=df_traderecord_Sold.iloc[i]['賣出日期']
        a=df_scorematrix.loc[tradedate.to_pydatetime() ,str(ticker)]
        df_traderecord_Sold.set_value(ticker,'賣出分數',a)
    
    df_traderecord_Sold = df_traderecord_Sold[['股票名稱','買進日期','賣出日期','持有天數','買進價格','賣出價格','交易數量(張)','出售損益','股利(千)','報酬率','買進分數','賣出分數']]
    df_traderecord_Sold.sort_index(inplace=True)
    
    # print(df_traderecord_Sold)

    #買入交易單資訊
    df_traderecord_Buy= df_traderecord.loc[(df_traderecord['交易方向'] =='B')]
    df_traderecord_Buy.columns = df_traderecord_Buy.columns.str.replace('交易日期', '買進日期')
    df_traderecord_Buy.columns = df_traderecord_Buy.columns.str.replace('交易價位', '買進價格')

    df_traderecord_Buy['交易數量(張)'] = df_traderecord_Buy['交易數量']/1000
    df_traderecord_Buy['買進分數'] = numpy.zeros(len(df_traderecord_Buy.index)) 
    df_traderecord_Buy['股票名稱'] = ""
    for i in range(len(df_traderecord_Buy.index)):
        ticker = df_traderecord_Buy.index[i]
        corpname = get_corpname(ticker)
        df_traderecord_Buy.set_value(ticker,'股票名稱',corpname)
        tradedate=df_traderecord_Buy.iloc[i]['買進日期']
        a=df_scorematrix.loc[tradedate.to_pydatetime() - timedelta(days =1),str(ticker)]
        df_traderecord_Buy.set_value(ticker,'買進分數',a)
    #print(df_traderecord_Buy)
    df_traderecord_Buy = df_traderecord_Buy[['股票名稱','買進日期','買進價格','交易數量(張)','買進分數']]
    df_traderecord_Buy.sort_index(inplace=True)
    return df_traderecord_Buy,df_traderecord_Sold
def get_traderecord_sold_on_rebalancedate(df_scorematrix,df_traderecord,RebalanceDate = None):
    if RebalanceDate is None:
        Latest_RebalanceDate_Sold = df_traderecord[df_traderecord['交易方向'] =='S']['交易日期'].max()
    def get_corpname(ticker):
        db_connection = get_db_conn(user='datateam')
        db_cursor = db_connection.cursor()
        _sql ="Select 股票名稱 from `marketrawdb_cm`.`bd_cm_companyprofile` where 股票代號='" + str(ticker) +"' limit 1"
        db_cursor.execute(_sql)
        a = db_cursor.fetchone()
        return a[0]
    def get_info_Buy(SoldDate,ticker):
        df = df_traderecord.loc[(df_traderecord['交易日期']<SoldDate) & (df_traderecord['交易方向'] =='B') & (df_traderecord['股票代號'] ==ticker)]
        return df[df['交易日期'] == df['交易日期'].max()].iloc[0]

    # #賣出交易單資訊
    df_traderecord_Sold= df_traderecord.loc[(df_traderecord['交易日期']==Latest_RebalanceDate_Sold) & (df_traderecord['交易方向'] =='S')]
    df_traderecord_Sold.set_index('股票代號',inplace=True) 
    df_traderecord_Sold.columns = df_traderecord_Sold.columns.str.replace('交易日期', '賣出日期')
    df_traderecord_Sold.columns = df_traderecord_Sold.columns.str.replace('交易價位', '賣出價格')

    df_traderecord_Sold['買進日期'] = ''
    df_traderecord_Sold['持有天數'] = 0
    df_traderecord_Sold['買進價格'] = 0.0
    df_traderecord_Sold['出售損益'] = 0.0
    df_traderecord_Sold['股利'] = 0.0
    df_traderecord_Sold['報酬率'] = 0.0
    df_traderecord_Sold['買進分數'] = 0.0 
    df_traderecord_Sold['賣出分數'] = 0.0 

    for i in range(len(df_traderecord_Sold.index)):
        ticker = df_traderecord_Sold.index[i]
        s= get_info_Buy(Latest_RebalanceDate_Sold,ticker)
        df_traderecord_Sold.at[ticker,'股票名稱'] = get_corpname(ticker)
        df_traderecord_Sold.at[ticker,'買進日期'] = s['交易日期'].to_pydatetime().date()
        df_traderecord_Sold.at[ticker,'持有天數'] =  (df_traderecord_Sold.iloc[i]['賣出日期'] - s['交易日期']).days
        df_traderecord_Sold.at[ticker,'買進價格'] = s['交易價位']
        df_traderecord_Sold.at[ticker,'出售損益'] =  df_traderecord_Sold.at[ticker,'交易數量'] *  (df_traderecord_Sold.at[ticker,'賣出價格'] - df_traderecord_Sold.at[ticker,'買進價格'])
        df_traderecord_Sold.at[ticker,'股利'] =  0
        df_traderecord_Sold.at[ticker,'報酬率'] = (df_traderecord_Sold.at[ticker,'出售損益'] +df_traderecord_Sold.at[ticker,'股利']) / (df_traderecord_Sold.at[ticker,'買進價格'] * df_traderecord_Sold.at[ticker,'交易數量'])

        df_traderecord_Sold.at[ticker,'買進分數'] = df_scorematrix.loc[Latest_RebalanceDate_Sold.to_pydatetime() ,str(ticker)]
        df_traderecord_Sold.at[ticker,'賣出分數'] = df_scorematrix.loc[Latest_RebalanceDate_Sold.to_pydatetime() ,str(ticker)]
    
    #格式設定
    
    df_traderecord_Sold['交易數量(張)'] = (df_traderecord_Sold['交易數量']/1000).astype(int) 
    df_traderecord_Sold['股利(千)'] = (df_traderecord_Sold['股利']/1000).round(2) 
    df_traderecord_Sold['出售損益(千)'] = (df_traderecord_Sold['出售損益']/1000).round(2)    
    df_traderecord_Sold['報酬率'] = df_traderecord_Sold['報酬率'].round(4)    
    df_traderecord_Sold['買進分數'] = df_traderecord_Sold['買進分數'].round(2) 
    df_traderecord_Sold['賣出分數'] = df_traderecord_Sold['賣出分數'].round(2) 
    
    #輸出欄位設定
    df_traderecord_Sold = df_traderecord_Sold[['股票名稱','買進日期','賣出日期','持有天數','買進價格','賣出價格','交易數量(張)','出售損益(千)','股利(千)','報酬率','買進分數','賣出分數']]
    df_traderecord_Sold.sort_index(inplace=True)
    return df_traderecord_Sold
def get_traderecord_buy_on_rebalancedate(df_scorematrix,df_traderecord,RebalanceDate = None):
    if RebalanceDate is None:
        Latest_RebalanceDate_Buy = df_traderecord[df_traderecord['交易方向'] =='B']['交易日期'].max()
    def get_corpname(ticker):
        db_connection = get_db_conn(user='datateam')
        db_cursor = db_connection.cursor()
        _sql ="Select 股票名稱 from `marketrawdb_cm`.`bd_cm_companyprofile` where 股票代號='" + str(ticker) +"' limit 1"
        db_cursor.execute(_sql)
        a = db_cursor.fetchone()
        return a[0]
    def get_info_Buy(SoldDate,ticker):
        df = df_traderecord.loc[(df_traderecord['交易日期']<SoldDate) & (df_traderecord['交易方向'] =='B') & (df_traderecord['股票代號'] ==ticker)]
        return df[df['交易日期'] == df['交易日期'].max()].iloc[0]

    #買入交易單資訊
    df_traderecord_Buy= df_traderecord[(df_traderecord['交易日期']==Latest_RebalanceDate_Buy) & (df_traderecord['交易方向'] =='B')]
    df_traderecord_Buy.set_index('股票代號',inplace=True) 
    df_traderecord_Buy.columns = df_traderecord_Buy.columns.str.replace('交易日期', '買進日期')
    df_traderecord_Buy.columns = df_traderecord_Buy.columns.str.replace('交易價位', '買進價格')

    df_traderecord_Buy['買進分數'] = 0.0
    df_traderecord_Buy['股票名稱'] = ""
    df_traderecord_Buy['近期資訊'] = ""
    for i in range(len(df_traderecord_Buy.index)):
        ticker = df_traderecord_Buy.index[i]
        df_traderecord_Buy.at[ticker,'股票名稱'] = get_corpname(ticker)
        df_traderecord_Buy.at[ticker,'買進分數'] = df_scorematrix.loc[Latest_RebalanceDate_Buy.to_pydatetime() - timedelta(days =1),str(ticker)]
        obj = DataRetriever(ticker)
        df = obj.query(Latest_RebalanceDate_Buy  - timedelta(days=500),Latest_RebalanceDate_Buy,['日期_月營收_資料日','MonthSale_YoY','EPS','單月合併月營收'])
        s = df.iloc[-1]
        ss = "%s月份營收為%s億元，年增%s％，近一季度EPS為%s元。" % (s['日期_月營收_資料日'].month,round(s['單月合併月營收']/100000,2),round(s['MonthSale_YoY']*100,2),round(s['EPS'],2))
        df_traderecord_Buy.at[ticker,'近期資訊']= ss
    
    #格式設定
    df_traderecord_Buy['交易數量(張)'] = (df_traderecord_Buy['交易數量']/1000).astype(int) 
    df_traderecord_Buy['買進分數'] = df_traderecord_Buy['買進分數'].round(2) 
 

    df_traderecord_Buy = df_traderecord_Buy[['股票名稱','買進日期','買進價格','交易數量(張)','買進分數','近期資訊']]
    df_traderecord_Buy.sort_index(inplace=True)

    return df_traderecord_Buy
def output_gsheet(api_key_json_path, gsheet_key, data_folder_path, portfolioid):
    gc = pygsheets.authorize(service_file = api_key_json_path)
    gsht = gc.open_by_key(gsheet_key)
    
    gsheet_data_mapping = {'perf_stats': portfolioid + '_perf_stats.csv',
                           'mtd_pl_to_equity':portfolioid + '_mtd_pl_to_equity.csv',
                           'equity_curve':portfolioid + '_equity_curve.csv',
                           'latest_inventory':portfolioid + '_latest_inventory2.csv',
                           'latest_buy_list':portfolioid + '_traderecord_buy.csv',
                           'latest_sold_list':portfolioid + '_traderecord_sold.csv',
                           'monthly_return':portfolioid + '_monthly_return.csv',
                           'monthly_excess_return':portfolioid + '_monthly_excess_return.csv'
                           }
    for wks_name in gsheet_data_mapping.keys():
        data_csv = gsheet_data_mapping[wks_name]
        df_data=pandas.read_csv(os.path.join(data_folder_path,  data_csv),encoding=charset)  
        try:
            sht = gsht.worksheet_by_title(wks_name)
            # gsht.del_worksheet(sht)
        except pygsheets.WorksheetNotFound as error:
            sht = gsht.add_worksheet(wks_name,index=1,rows=len(df_data),cols=len(df_data.columns))
        #去除Nan,避免NaN appsheet無法讀取
        df_data = df_data.fillna('')
        sht.clear()
        sht.set_dataframe(df_data,start="A1")

    
if __name__=='__main__':
    QueryDate = datetime(2022,4,26)
    fg_quant_api_key_json_path = os.path.join(quantlib.get_key_path(), 'client_secret_fgquant.json') 
    # Date_Lastweek = datetime(2022,3,15)   
    charset='utf-8-sig'
    
    gsheet_id_mapping={
        'E6':'1CKeT6uQmYP3JOUlirVeknOcd3qfmC1UY74BYe0Os7qk',#Eason
        'T1013':'1sRzhcUCOQJEhLT17ESJ_ca4p2wWmg-y6c-p0R6TZaWM',#殖利率
        'T1016':'1hoRIQT6OcvfoGq6nUSzdRxmLJeME8A4c1uLJwAm7MCI',#區間操作
        }
    portfolioidlist=list(gsheet_id_mapping.keys())
    #portfolioidlist=['E5','T964','T971','T985','T987','T988','T989']
    data_folder_path =os.path.join( os.path.dirname(os.path.abspath(__file__)),'來源資料')
    #整理策略分析結果
    df_result = pandas.read_csv(os.path.join(data_folder_path,'(格式化)策略分析.csv'),index_col='項目')
    df_result_F14 = pandas.read_csv(os.path.join(data_folder_path,'F14(格式化)策略分析.csv'),index_col='項目')   
    F14_strategy_list = list(set(df_result_F14.columns) - {'0050'})#排除0050
    df= df_result.join(df_result_F14[F14_strategy_list], how = 'left')
    df.to_csv(os.path.join(data_folder_path, '策略分析_整理後.csv'),encoding= charset)
    #整理權益數:將F14與非F14結合
    df_result = pandas.read_csv(os.path.join(data_folder_path,'(整理)權益數.csv'),index_col='Date')
    df_result_F14 = pandas.read_csv(os.path.join(data_folder_path,'F14(整理)權益數.csv'),index_col='Date')  
    F14_strategy_list = list(set(df_result_F14.columns) - {'0050'})#排除0050
    df= df_result.join(df_result_F14[F14_strategy_list], how = 'left')
    df.round(2).to_csv(os.path.join(data_folder_path, '權益數_整理後.csv'),encoding=charset)
    for  portfolioid in portfolioidlist:
        gsheet_id =gsheet_id_mapping[portfolioid]
        #取得score matrix
        df_scorematrix = pandas.read_csv(os.path.join(data_folder_path,portfolioid,portfolioid + '_ScoreMatrix_Original.csv'),index_col='Date')
        df_scorematrix.index = map(pandas.to_datetime,df_scorematrix.index)   

        #取得traderecord:
        df_traderecord = pandas.read_csv(os.path.join(data_folder_path, portfolioid, portfolioid + '_(回測報表)交易單ForBear.csv'))
        df_traderecord['交易日期'] = df_traderecord['交易日期'].apply(lambda x : datetime.strptime(str(x), "%Y%m%d"))

        #=====交易單資料================
        if True:
            df_traderecord_Buy =get_traderecord_buy_on_rebalancedate(df_scorematrix,df_traderecord)
            df_traderecord_Buy.to_csv(os.path.join(data_folder_path,portfolioid, portfolioid + '_traderecord_buy.csv'),encoding=charset)
            df_traderecord_Sold =get_traderecord_sold_on_rebalancedate(df_scorematrix,df_traderecord)
            df_traderecord_Sold.to_csv(os.path.join(data_folder_path,portfolioid, portfolioid + '_traderecord_sold.csv'),encoding=charset)
        #=====庫存================
        #庫存(日損益報表格式,會用到資料庫)(家綸產生)
        if False:
            df = get_inventoryreport(QueryDate,Date_Lastweek,portfolioid,df_scorematrix,df_traderecord)
            df.to_csv(os.path.join(data_folder_path,portfolioid, portfolioid + '_latest_inventory.csv'),encoding=charset)
        
        df_inventory = pandas.read_csv(os.path.join(data_folder_path, portfolioid, portfolioid + '_持有部位_有標籤_最新部位.csv'),encoding=charset)        
        df_inventory['評價日期'] = QueryDate 
        # df_inventory['initial_score'] = numpy.zeros(len(df_inventory.index))
        # df_inventory['Current_score'] = numpy.zeros(len(df_inventory.index))
        # df_inventory['建倉日期'] = pandas.to_datetime(df_inventory['建倉日期']) 
        # #取得Score
        # for i in range(len(df_inventory.index)):
        #     ticker = df_inventory.iloc[i]['股票代號']
        #     tradedate= df_inventory.iloc[i]['建倉日期']
        #     print(tradedate.to_pydatetime())
        #     try:
        #         s = df_scorematrix[str(ticker)]
        #         s=s[s.index<=tradedate.to_pydatetime()-timedelta(days=1)] #df_scorematrix.loc[tradedate.to_pydatetime()-timedelta(days=1),str(ticker)]
        #         a=s.iloc[-1]
        #     except:
        #         a=0
        #     df_inventory.at[i,'initial_score'] = a
        #     try:
        #         b=df_scorematrix.loc[QueryDate,str(ticker)]
        #     except:
        #         b=0            
        #     df_inventory.at[i,'Current_score'] = b
        df_inventory = df_inventory[['股票代號','股票名稱','建倉日期','建倉價格','評價日期','最新價格','持有天數','累積報酬率','年報酬率','月報酬率','週報酬率']]
        df_inventory.to_csv(os.path.join(data_folder_path,portfolioid, portfolioid + '_latest_inventory2.csv'), index=False , encoding=charset)
        #=====表格資料-perf_stats================
        df_perf = pandas.read_csv(os.path.join(data_folder_path, '策略分析_整理後.csv'),index_col='項目',encoding=charset)
        df = df_perf.loc[['YTD Return','MTD Return','Latest Weekly Return','Annual return','Cumulative returns','Annual volatility','Sharpe ratio','持有天數_平均','勝率','賺賠比'],['0050',portfolioid]]
        df.index.name = 'ITEM'
        #變更index名稱        
        df.rename({'YTD Return':'YTD Return(%)','MTD Return':'MTD Return(%)','Latest Weekly Return':'WTD Return(%)','Annual return':'Annual Return(%)','Cumulative returns':'Cumulative Returns(%)','Annual volatility':'Annual Volatility(%)','勝率':'勝率(%)'},inplace=True)
        #變更columns名稱
        df.columns = df.columns.str.replace('0050', '0050 ETF')
        df = df.fillna('')
        df.replace('%','',regex=True,inplace=True)
        df.to_csv(os.path.join(data_folder_path,portfolioid, portfolioid + '_perf_stats.csv'),encoding=charset)
        #=====繪圖資料-權益數================
        df_equity = pandas.read_csv(os.path.join(data_folder_path, '權益數_整理後.csv'),index_col='Date',encoding=charset)
        df_equity = df_equity[['0050',portfolioid]].dropna()
        df_equity.columns =df_equity.columns.str.replace('0050', '0050 ETF')
        df_equity = df_equity / df_equity.iloc[0] * 100
        df_equity.to_csv(os.path.join(data_folder_path,portfolioid, portfolioid + '_equity_curve.csv'),encoding=charset)     
        #=====繪圖資料-每月已實現損益佔比================
        df_monthlyPL = pandas.read_csv(os.path.join(data_folder_path, portfolioid, portfolioid + '_(原始)時序分析.csv'),index_col='日期',encoding=charset)
        df_monthlyPL = df_monthlyPL[['MTD處份權益變動比率']]
        df_monthlyPL = df_equity.join(df_monthlyPL,how = 'left')
        df_monthlyPL= df_monthlyPL.dropna()#篩選有MTD處份權益變動比率的資料
        new_row = pandas.Series(data={'0050 ETF':100, str(portfolioid):100, 'MTD處份權益變動比率':0}, name=df_equity.index[0])
        df_monthlyPL = df_monthlyPL.append(new_row, ignore_index=False)
        df_monthlyPL.sort_index(inplace=True)
        df_monthlyPL.to_csv(os.path.join(data_folder_path,portfolioid, portfolioid + '_mtd_pl_to_equity.csv'),encoding=charset)  
        #=====表格資料-Monthly Return================
        df_monthly_return = pandas.read_csv(os.path.join(data_folder_path, portfolioid ,portfolioid +'_策略年月報酬率比較.csv'),index_col='Date',encoding=charset)
        df_monthly_return = df_monthly_return.mask(df_monthly_return=='nan%',numpy.nan) #轉為np.nan後續才能將string 轉為 float
        df_monthly_return.replace('\w%','',regex=True,inplace=True) #去除%
        df_monthly_return=df_monthly_return.astype(float)/100 
        #計算勝率
        s_odds=df_monthly_return.mask(df_monthly_return<=0).count(axis=0,numeric_only=True)/df_monthly_return.count(axis=0)
        s_odds.name = '勝率'
        s_odds = s_odds.to_frame().T
        df_monthly_return = pandas.concat([df_monthly_return,s_odds]).fillna('')
        df_monthly_return.index.name='Year'
        df_monthly_return.to_csv(os.path.join(data_folder_path,portfolioid, portfolioid + '_monthly_return.csv'),encoding=charset)
        #=====#表格資料-Monthly Excess Return================
        df_monthly_excess_return = pandas.read_csv(os.path.join(data_folder_path, portfolioid ,portfolioid +'_策略超越指標年月報酬率比較.csv'),index_col='Date',encoding=charset)
        df_monthly_excess_return = df_monthly_excess_return.mask(df_monthly_excess_return=='nan%',numpy.nan) #轉為np.nan後續才能將string 轉為 float
        df_monthly_excess_return.replace('\w%','',regex=True,inplace=True) #去除%
        df_monthly_excess_return=df_monthly_excess_return.astype(float)/100 
        #計算勝率================
        s_odds=df_monthly_excess_return.mask(df_monthly_excess_return<=0).count(axis=0,numeric_only=True)/df_monthly_excess_return.count(axis=0)
        s_odds.name = '勝率'
        s_odds = s_odds.to_frame().T
        df_monthly_excess_return = pandas.concat([df_monthly_excess_return,s_odds]).fillna('')
        df_monthly_excess_return.index.name='Year'
        df_monthly_excess_return.to_csv(os.path.join(data_folder_path,portfolioid, portfolioid + '_monthly_excess_return.csv'),encoding=charset)
        #upload gsheet
        output_gsheet(fg_quant_api_key_json_path,gsheet_id,os.path.join(data_folder_path,portfolioid),portfolioid)
