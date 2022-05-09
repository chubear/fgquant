# -*- coding: utf-8 -*-
import sys,os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pygsheets
import quantlib
import pandas
import json
from pandas.core.indexing import check_bool_indexer
import seaborn as sns
import numpy as np
import matplotlib
import matplotlib.ticker as mtick
import matplotlib.pyplot as plt 
from matplotlib import cm
from pandas.core.reshape.reshape import unstack
import matplotlib.pyplot as plt
import pygsheets
from PyPDF2 import PdfFileMerger
plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei'] 
plt.rcParams['axes.unicode_minus'] = False

def get_num_measure(measure_name):
    #為了排序用
    measure_list= ['市值','PB','PE','ROE','MonthSale_YoY','Price_Return_22D','Price_Return_67D',
            '股利殖利率','Volme_MA_20D','成交值比重_MA_5D',
            '集保比例變化_400張以上_4W','集保比例變化_400張以上_12W','集保比例變化_1000張以上_4W','集保比例變化_1000張以上_12W']
    num=0
    for ms in measure_list:
        if ms.upper() in measure_name.upper():
            num=num+1
    return num
def rank_measure_list(measure_list):
    sort_result = sorted([(get_num_measure(ms),ms) for ms in measure_list], key=lambda x: x[0])
    return [x[1] for x in sort_result]
def get_bt_perf_stat(folder_path):
    if os.path.isfile(os.path.join(folder_path,'perf_stats.csv')):
        df = pandas.read_csv(os.path.join(folder_path,'perf_stats.csv'))
        df.set_index(df.columns[0],inplace=True)
        df.index.name='items'
        return df
    return None
def get_gt_perf_stat(folder_path):
    if os.path.isfile(os.path.join(folder_path,'gt_perf_stats.csv')):
        df = pandas.read_csv(os.path.join(folder_path,'gt_perf_stats.csv'))
        df.set_index(df.columns[0:2],inplace=True)
        df=df.T
        df.set
        return df
    return None
def combine_result_by_meausre_in_watchlist(watchlist_period_folder_name,para_filter):
    #統計同一資料夾，不同measure之比較
    df_result =pandas.DataFrame()
    data_folder_path = os.path.join(result_data_folder_path,watchlist_period_folder_name)
    if os.listdir(data_folder_path)==[]:
        print("NO New data!!!")
    else:
        #符合條件之dir
        dir = sorted([x for x in os.listdir(data_folder_path) if x.startswith(para_filter)], key=len)
        for fd in dir:   
            measure_name = fd[len(para_filter)+1:]   
            df =  get_bt_perf_stat(os.path.join(data_folder_path,fd))
            if df !=None:
            # if os.path.isfile(os.path.join(data_folder_path,fd,'perf_stats.csv')):
            #     df = pandas.read_csv(os.path.join(data_folder_path,fd,'perf_stats.csv'))
            #     df.set_index('Unnamed: 0',inplace=True)
                if df_result.empty:
                    df.columns = [measure_name,'bench_0050']
                    df_result= df[['bench_0050',measure_name]]
                else:
                    df_result[measure_name] = df['Port']
        #將因子依個數重新排名
        bm_ticker=df_result.columns[0]
        measure_list = df_result.columns[1:]
        sort_measure_list = rank_measure_list(measure_list)
        # df_result.columns = [bm_ticker] + sort_measure_list    
        df_result[[bm_ticker] + sort_measure_list].to_csv(os.path.join(data_folder_path,'comparision'+ '_' + watchlist_period_folder_name + '_' + para_filter + '.csv'),float_format = '%.4f',encoding='big5')
        print('finished: comparision'+ '_' + watchlist_period_folder_name + '_' + para_filter + '.csv')
def combine_result_by_watchlist_period(watchlist_period_fold_list,para_filter,perf_item_list,result_xlsx_path):
    #選定比較項目，比如:annual return，統計不同watchlist_period
    
    writer = pandas.ExcelWriter(result_xlsx_path, engine='openpyxl')
    for perf_item in perf_item_list:
        df_result=pandas.DataFrame()
        for rs in watchlist_period_fold_list:
            result_file_name = 'comparision'+ '_' + rs + '_' + para_filter + '.csv'
            if os.path.isfile(os.path.join(result_data_folder_path,rs,result_file_name)):
                df = pandas.read_csv(os.path.join(result_data_folder_path,rs,result_file_name),encoding='big5')
                df.set_index('Unnamed: 0',drop=True, inplace=True)
                df.to_excel(writer,sheet_name= rs)
                data_series= df.loc[perf_item]
                data_series.name = rs
                if df_result.empty:#為了維持df_result欄位的順序
                    df_result = data_series.to_frame().transpose()
                else:
                    df_result = df_result.append(data_series)
        df_result.index.name=perf_item
        #將因子依個數重新排名
        bm_ticker=df_result.columns[0]
        measure_list = df_result.columns[1:]
        sort_measure_list = rank_measure_list(measure_list)
        # df_result.columns = [bm_ticker] + sort_measure_list
        df_result[[bm_ticker] + sort_measure_list].to_excel(writer,sheet_name=perf_item)
        writer.save()
    writer.close()
                #df_result.to_csv(os.path.join(result_data_folder_path,'comparision'+ '_' + perf_item + '.csv'),encoding='big5')
def get_strategy_display_order(stockpool,bt_period,bt_folder_path):
    stockpool_period_folder_path = os.path.join(bt_folder_path,stockpool + '_' + bt_period)
    strategyname_list=[] 
    display_order_list = []
    for fd in os.listdir(stockpool_period_folder_path): 
        strategy_folder_path = os.path.join(stockpool_period_folder_path,fd)
        if os.path.isfile( os.path.join(strategy_folder_path,'0_para_setting.json')):  
            #讀取json
            para_setting = json.load(open(os.path.join(stockpool_period_folder_path,fd,'0_para_setting.json')))
            strategyname_list.append(para_setting['strategyname'])
            display_order_list.append(para_setting['display_order'])
    return pandas.DataFrame.from_dict({'strategyname':strategyname_list,'display_order':display_order_list})

def get_gt_perf_stat_comparision(stockpool_list,gt_period_list,gt_folder_path):
    '''給定stockpool與gt_period，取得該資料夾下全部策略之分群測試的結果,欄位為stockpool,bt_period,stategyname,gt_perf_stats.csv的欄位 '''
    def get_gt_perf_stats(folder_path):
        if os.path.isfile(os.path.join(folder_path,'gt_perf_stats.csv')):
            df = pandas.read_csv(os.path.join(folder_path,'gt_perf_stats.csv'))
            df.set_index(list(df.columns[0:2]),inplace=True)
            return df.T
        return None
    df_result = pandas.DataFrame()
    for stockpool in stockpool_list:
        for gt_period in gt_period_list:    
            stockpool_period_folder_path = os.path.join(gt_folder_path,stockpool + '_' + gt_period)
            for fd in os.listdir(stockpool_period_folder_path): 
                strategy_folder_path = os.path.join(stockpool_period_folder_path,fd)
                if os.path.isfile( os.path.join(strategy_folder_path,'gt_perf_stats.csv')):  
                    #讀取json
                    para_setting = json.load(open(os.path.join(stockpool_period_folder_path,fd,'0_para_setting.json'),encoding='utf-8-sig'))
                    strategyname= para_setting['strategyname']
                    df = get_gt_perf_stats(strategy_folder_path)
                    df = df.reset_index()
                    df.rename(columns = {'index':'holding_period'}, inplace = True)
                    df.insert(0,'stockpool',stockpool)
                    df.insert(1,'gt_period',gt_period)
                    df.insert(2,'strategyname',strategyname)
                    df.insert(3,'num_factor',len(para_setting['strategy_list']))
                    df.insert(4,'gt_time',para_setting['gt_time'])
                    df.insert(5,'gt_sampling_para', '_'.join([str(x) for x in para_setting['gt_sampling_para']]))
                    df_result=df_result.append(df,ignore_index=True)
    return df_result
def get_bt_perf_stat_comparision(stockpool_list,bt_period_list,bt_folder_path):
    '''給定stockpool與bt_period，取得該資料夾下全部策略之perf_stat的結果'''
    df_result = pandas.DataFrame()
    for stockpool in stockpool_list:
        for bt_period in bt_period_list:
            stockpool_period_folder_path = os.path.join(bt_folder_path,stockpool + '_' + bt_period)
            
            for fd in os.listdir(stockpool_period_folder_path): 
                strategy_folder_path = os.path.join(stockpool_period_folder_path,fd)
                if os.path.isfile( os.path.join(strategy_folder_path,'0_para_setting.json')):  
                    #讀取json
                    para_setting = json.load(open(os.path.join(stockpool_period_folder_path,fd,'0_para_setting.json')))
                    strategyid= para_setting['strategyid']
                    strategyname= para_setting['strategyname']
                    bt_para =  para_setting['bt_ranking_para'] + '_' + str(para_setting['bt_rebalance_period_m']) + '_' + str(para_setting['bt_rebalance_day'])        
                    df = get_bt_perf_stat(strategy_folder_path)
                    df=df.reset_index()
                    
                    df.insert(0,'stockpool',stockpool)
                    df.insert(1,'bt_period',bt_period)
                    df.insert(2,'bt_para',bt_para)
                    df.insert(3,'strategyid',strategyid)
                    df.insert(4,'strategyname',strategyname)
                    #第一筆資料取得0050 perf_stat
                    if df_result.empty:
                        df_bm=df.copy()
                        df_bm['Port'] = df_bm['0050']
                        df_bm['strategyid'] = '0050'
                        df_bm['strategyname'] = 'Benchmark_0050'
                        df_bm.drop('0050',axis = 1,inplace=True)
                        df_result=df_result.append(df_bm,ignore_index=True)
                    df.drop('0050',axis = 1,inplace=True)
                    df_result=df_result.append(df,ignore_index=True)
    return df_result
# def get_gt_perf_stat_comparision_result(stockpool_list,bt_period_list,bt_folder_path):
#     '''給定stockpool與bt_period，取得該資料夾下全部策略之gt_perf_stat的結果'''
#     df_result = pandas.DataFrame()
#     for stockpool in stockpool_list:
#         for bt_period in bt_period_list:
#             stockpool_period_folder_path = os.path.join(bt_folder_path,stockpool + '_' + bt_period)
            
#             for fd in os.listdir(stockpool_period_folder_path): 
#                 strategy_folder_path = os.path.join(stockpool_period_folder_path,fd)
#                 if os.path.isfile( os.path.join(strategy_folder_path,'0_para_setting.json')):  
#                     #讀取json
#                     para_setting = json.load(open(os.path.join(stockpool_period_folder_path,fd,'0_para_setting.json')))
#                     strategyid= para_setting['strategyid']
#                     strategyname= para_setting['strategyname']
#                     #gt_time = para_setting['gt_time']
#                     #bt_para =  para_setting['bt_ranking_para'] + '_' + str(para_setting['bt_rebalance_period_m']) + '_' + str(para_setting['bt_rebalance_day'])        
#                     df = get_gt_perf_stat(strategy_folder_path)
#                     df=df.reset_index()
                    
#                     df.insert(0,'stockpool',stockpool)
#                     df.insert(1,'bt_period',bt_period)
#                     df.insert(3,'strategyid',strategyid)
#                     df.insert(4,'strategyname',strategyname)
#                     df_result=df_result.append(df,ignore_index=True)
#     return df_result
def plot_groupingtest_comparison_fig(df , title1, col_lable_x,col_lable_y,x_list_order = None , y_list_order = None):
    '''繪製bt測試比較圖'''
    x_list = list(df[col_lable_x].unique()) if x_list_order == None else x_list_order
    y_list = list(df[col_lable_y].unique()) if y_list_order == None else y_list_order
    sns.set_style("whitegrid")
    #為了顯示中文
    plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']  #中文字型 (前置需電腦設定https://medium.com/marketingdatascience/%E8%A7%A3%E6%B1%BApython-3-matplotlib%E8%88%87seaborn%E8%A6%96%E8%A6%BA%E5%8C%96%E5%A5%97%E4%BB%B6%E4%B8%AD%E6%96%87%E9%A1%AF%E7%A4%BA%E5%95%8F%E9%A1%8C-f7b3773a889b)
    fig,axs = plt.subplots(nrows=len(y_list),ncols=len(x_list),sharex=False,sharey=True)
    
    #取得colormap
    cmap = cm.get_cmap('tab20c')
    k=0
    for y in y_list:
        i=0
        for x,ax in zip(x_list, [a for a in axs[k]]):
            print(y,x)
            df1 = df.loc[(df[col_lable_x] == x) & (df[col_lable_y] == y)]
            colors=cmap([i]*12) #設定12根bar顏色
            df1.plot(x ='Group', y = 'return' , kind="bar", color = colors, ax=ax ,legend=False)
            if k % 4 == 0:
                ax.set_title(x,fontsize=8)
            ax.set_xlabel('')
            ax.set_xticklabels(df1['Group'],fontsize=6)
            ax.set_ylabel(y,fontsize=8)
            ax.set_yticklabels(df1['return'],fontsize=6)
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1)) #設定為%格式
            i=i+4
        k=k+1

    fig.set_size_inches(8.27,len(y_list)*2) 
    
    fig.tight_layout()
    fig.subplots_adjust(top=0.90)
    fig.suptitle(title1,fontsize=12,y=0.98)
    
    
    return fig
def plot_bt_comparison_fig(df , title1, col_lable_x,col_lable_y,x_list_order = None , y_list_order = None):
    '''繪製分群測試比較圖,每個圖為Group1~Group10+T_B,T2_B2,col_lable_x表示'''
    x_list = list(df[col_lable_x].unique()) if x_list_order == None else x_list_order
    y_list = list(df[col_lable_y].unique()) if y_list_order == None else y_list_order
    sns.set_style("whitegrid")
    #為了顯示中文
    plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei']  #中文字型 (前置需電腦設定https://medium.com/marketingdatascience/%E8%A7%A3%E6%B1%BApython-3-matplotlib%E8%88%87seaborn%E8%A6%96%E8%A6%BA%E5%8C%96%E5%A5%97%E4%BB%B6%E4%B8%AD%E6%96%87%E9%A1%AF%E7%A4%BA%E5%95%8F%E9%A1%8C-f7b3773a889b)
    fig,axs = plt.subplots(nrows=len(y_list),ncols=len(x_list),sharex=False,sharey=False)
    #fig = plt.figure()
    #取得colormap
    cmap = cm.get_cmap('tab20')
    k=0
    for y in y_list:
        i=0
        for x,ax in zip(x_list, [a for a in axs[k]]):
            x_col_lable = 'strategyname'
            y_col_lable = 'Port'
            print(y,x)
            df1 = df.loc[(df[col_lable_x] == x) & (df[col_lable_y] == y)]
            Num_strategy = len(df1['strategyname'].drop_duplicates())
            colors=cmap([i] * Num_strategy) 
            df1.plot(x =x_col_lable, y = y_col_lable , kind="bar", ax = ax, color = colors ,legend=False)
            ax.set_title(x,fontsize=8)
            ax.set_xlabel('')
            ax.set_xticklabels(df1[x_col_lable],fontsize=6)
            if i==0:
                ax.set_ylabel(y,fontsize=8)
            ax.set_yticklabels(df1[y_col_lable],fontsize=6)
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1)) #設定為%格式
            i+=1
        k+=1

    fig.set_size_inches(12,len(y_list)*2) 
    fig.tight_layout()
    fig.subplots_adjust(top=0.90)
    fig.suptitle(title1,fontsize=12,y=0.98)
    
    return fig
def grouping_test_quant_score_sum_steps(group_return_list):
    score=0
    for i in range(len(group_return_list)):
        score = score + sum([group_return_list[i] - group_return_list[k] for k in range(len(group_return_list)) if k>i])
    return score
def grouping_test_quant_score_slope(group_return_list):
    slope, intercept = np.polyfit(np.arange(1,11),group_return_list,1)
    return slope
def output_gsheet( gsheet_id, wks_name,df_data):
    gc = pygsheets.authorize(service_file= os.path.join(quantlib.get_key_path(), 'client_secret.json'))
    gsht = gc.open_by_key(gsheet_id)
    try:
        sht = gsht.worksheet_by_title(wks_name)
        gsht.del_worksheet(sht)
    except pygsheets.WorksheetNotFound as error:
        pass
    new_sht = gsht.add_worksheet(wks_name,index=1)
    new_sht.set_dataframe(df_data,start="A1")
if __name__=='__main__':
    result_data_folder_path = os.path.join(os.path.dirname(__file__),'bt_result_data')#port_backtest 資料夾
    #===bt_perf_stat=======================
    if False:
        stockpool_list = ['ALL','MV_1_300','MV_1_600','MV_301_600','MV_601_2000']
        bt_period_list = ['201301_202107']
        #=====Perf_stat==========
        #取得排序list
        # df_display_order = get_strategy_display_order(stockpool,bt_period)
        # display_order_list = df_display_order.sort_values('display_order')['strategyname'].to_list()
        bt_folder_path = os.path.join(result_data_folder_path,'back_testing')
        df = get_bt_perf_stat_comparision_result(stockpool_list,bt_period_list,bt_folder_path)
        #df .to_csv(os.path.join(result_data_folder_path,'bt_result_statistics','test.csv'),encoding = 'big5') 
        #輸出檔案
        #輸出Excel
        if True:
            writer = pandas.ExcelWriter(os.path.join(result_data_folder_path,'bt_result_statistics','bt_perf_stat_comparision_result.xlsx'), engine='openpyxl')
            for stockpool in stockpool_list:
                for bt_period in bt_period_list:
                    df1= df [(df ['stockpool']==stockpool) & (df ['bt_period']==bt_period)].pivot_table(index=['stockpool','bt_period','bt_para','strategyid','strategyname'],columns='items',values='Port')
                    df1.to_excel(writer,sheet_name=stockpool,float_format='%.4f',merge_cells = False)   
            writer.save()
            writer.close()
        #輸出gsheet
        if False:
                for stockpool in stockpool_list:
                    for bt_period in bt_period_list:
                        df1= df [(df ['stockpool']==stockpool) & (df ['bt_period']==bt_period)].pivot_table(index=['stockpool','bt_period','bt_para','strategyid','strategyname'],columns='items',values='Port')
                        df1 = df1[['Annual return','Annual volatility','Sharpe ratio','Max drawdown','Sortino ratio','Calmar ratio','Omega ratio']]
                        df1 = df1[df1.index.get_level_values('bt_para').isin(['1_30_60_1_15'])]
                        df1.reset_index(inplace=True)
                        output_gsheet('1t-ya_XTW3KUuxVrroQLL5gLZqO2D7SlYN2ca-_3tBMw', stockpool,df1)
                    
        #產生pdf
        if False:
            strategy_order_list = list(get_strategy_display_order('ALL','200801_202107').sort_values('display_order')['strategyname'])
            #items_list = ['Annual return','Annual volatility','Calmar ratio','Cumulative returns','Daily value at risk','Max drawdown','Omega ratio','Sharpe ratio','Sortino ratio','Stability','Tail ratio']
            items_list = ['Annual return','Annual volatility','Max drawdown','Sharpe ratio','Stability']
            strategy_list =['Benchmark_0050','PB','MV_MS_12M','Beta係數250D','ROE','Price_Return_67D','MonthSale_YoY','Price_Skew_252D','PB_Percentile_5yr']
            fig_para_list= [
            #----bt by items------
            {'col_lable_file': 'bt_para','title_para' : '項目', 'col_lable_x' : 'stockpool', 'x_list_order':None, 'col_lable_y' : 'items','y_list_order':None},
            # #----分群測試 by stockpool------
            # {'col_lable_file' : 'stockpool', 'title_para' : '資產池', 'col_lable_x' : 'return_period','x_list_order':None,  'col_lable_y' : 'strategyname','y_list_order':strategy_order_list},
            # #====分群測試 by strategy==========
            # {'col_lable_file' : 'strategyname', 'title_para' : 'factor-measure', 'col_lable_x' : 'return_period','x_list_order':None, 'col_lable_y' : 'stockpool','y_list_order':None}
            ]
            #===繪圖===
            for fig_para in fig_para_list:
                merger = PdfFileMerger()
                col_lable_file = fig_para['col_lable_file']
                title_para =  fig_para['title_para']
                col_lable_x =  fig_para['col_lable_x']
                col_lable_y =  fig_para['col_lable_y']
                x_list_order =  fig_para['x_list_order']
                y_list_order =  fig_para['y_list_order']
                pdf_list=[]
                for bt_period in bt_period_list:
                    for z in list(df[col_lable_file].unique()):
                    
                        print("ploting:回朔測試比較(%s:%s, 回測期間:%s)" % (title_para,z, bt_period))
                        df1 = df.loc[(df[col_lable_file] == z)&(df['bt_period'] == bt_period) & (df['items'].isin(items_list))& (df['strategyname'].isin(strategy_list))]
                        fig = plot_bt_comparison_fig(df1,'回溯測試比較\n(%s:%s, 回測期間:%s)' % (title_para,z, bt_period) ,col_lable_x,col_lable_y,x_list_order,y_list_order)
                        pdf_name = 'fig_bt_comparision_result_by_' + z + '_' + bt_period + '.pdf'
                        fig.savefig(os.path.join(result_data_folder_path,'bt_result_statistics',pdf_name))
                        pdf_list.append(pdf_name)
                #結合各個pdf
                # for pdf in pdf_list:
                #     merger.append(os.path.join(result_data_folder_path,'bt_result_statistics',pdf))
                # with open(os.path.join(result_data_folder_path,'bt_result_statistics','bt_by_' + col_lable_file + '_' + bt_period + '.pdf'), 'wb') as fout:
                #     merger.write(fout) 

                
    #====分群測試==========
    if True:
        gt_folder_path = os.path.join(result_data_folder_path,'group_testing')
        stockpool_list = ['ALL']#['ALL','MV_1_300','MV_1_600','MV_301_600','MV_601_2000']
        bt_period_list = ['201301_202107']
        #取得與整理繪圖資料
        # df_grouped_final = get_gt_perf_stats_comparision(stockpool_list,bt_period_list)
        # # df_grouped_final.to_csv(os.path.join(result_data_folder_path,'bt_result_statistics','groupingtest_comparision_result.csv'),index=False)
        # #df_grouped_final = pandas.read_csv(os.path.join(result_data_folder_path,'bt_result_statistics','groupingtest_comparision_result.csv'))
        # #========== 整理資料--->將報酬率年化==========
        # df_grouped_final['Group_Return_1M_annualized'] = df_grouped_final['Group_Return_1M_mean'] * 12
        # df_grouped_final['Group_Return_3M_annualized'] = df_grouped_final['Group_Return_3M_mean'] * 4
        # df_grouped_final['Group_Return_6M_annualized'] = df_grouped_final['Group_Return_6M_mean'] * 2
        # df_grouped_final['Group_Return_12M_annualized'] = df_grouped_final['Group_Return_12M_mean'] 
        # df=df_grouped_final[['stockpool','bt_period','strategyname','Group','Group_Return_1M_annualized','Group_Return_3M_annualized','Group_Return_6M_annualized','Group_Return_12M_annualized']]
        # df.columns.name = 'return_period'
        # df = df.set_index(['stockpool','bt_period','strategyname','Group'])
        # s = df.stack() #變為series
        # s.name = 'return'
        # df = s.reset_index() #取消index,因此變為df
        #=========================================
        #策略篩選
        # strategy_order_list = list(get_strategy_display_order('ALL','200801_202107').sort_values('display_order')['strategyname'])
        # strategy_list =[]
        # # strategy_list = ['MonthSale_YoY','PB','ROE']
        # if len(strategy_list) >0:
        #     df= df[df['strategyname'].isin(strategy_list)]
        #     strategy_order_list = [x for x in strategy_order_list if x in strategy_list]
        #計算分群量化分數
        if True:
            # df2=df.pivot_table(index=['stockpool','bt_period','strategyname','return_period'],columns='Group',values='return')
            # df2['Score_sumsteps'] = 0
            # df2['Score_slope'] = 0
            # for k in range(len(df2)):
            #     return_list=df2.iloc[k].loc[['Group_1','Group_2','Group_3','Group_4','Group_5','Group_6','Group_7','Group_8','Group_9','Group_10']].to_list()
            #     df2.ix[k,'Score_sumsteps'] = grouping_test_quant_score_sum_steps(return_list)
            #     df2.ix[k,'Score_slope'] = grouping_test_quant_score_slope(return_list)
            
            df2 = get_gt_perf_stat_comparision(stockpool_list,bt_period_list,gt_folder_path)
            df2.to_csv(os.path.join(result_data_folder_path,'bt_result_statistics','gt_perf_stats_comparision.csv'),index=False,encoding='utf-8-sig')


        #===繪圖 start===
        if False:
            fig_para_list= [
            #----分群測試 by return period------
            # {'col_lable_file': 'return_period','title_para' : '持有期間報酬', 'col_lable_x' : 'stockpool', 'x_list_order':None, 'col_lable_y' : 'strategyname','y_list_order':strategy_order_list},
            # #----分群測試 by stockpool------
            {'col_lable_file' : 'stockpool', 'title_para' : '資產池', 'col_lable_x' : 'return_period','x_list_order':None,  'col_lable_y' : 'strategyname','y_list_order':strategy_order_list},
            #====分群測試 by strategy==========
            #{'col_lable_file' : 'strategyname', 'title_para' : 'factor-measure', 'col_lable_x' : 'return_period','x_list_order':None, 'col_lable_y' : 'stockpool','y_list_order':None}
            ]

            for fig_para in fig_para_list:
                merger = PdfFileMerger()
                col_lable_file = fig_para['col_lable_file']
                title_para =  fig_para['title_para']
                col_lable_x =  fig_para['col_lable_x']
                col_lable_y =  fig_para['col_lable_y']
                x_list_order =  fig_para['x_list_order']
                y_list_order =  fig_para['y_list_order']
                pdf_list=[]
                for bt_period in bt_period_list:
                    for z in list(df[col_lable_file].unique()):
                    
                        print("ploting:分群測試比較(%s:%s, 回測期間:%s)" % (title_para,z, bt_period))
                        df1 = df.loc[(df[col_lable_file] == z)&(df['bt_period'] == bt_period)]
                        fig = plot_groupingtest_comparison_fig(df1,'分群測試比較\n(%s:%s, 回測期間:%s)' % (title_para,z, bt_period) ,col_lable_x,col_lable_y,x_list_order,y_list_order)
                        pdf_name = 'fig_groupingtest_comparision_result_by_' + z + '_' + bt_period + '.pdf'
                        fig.savefig(os.path.join(result_data_folder_path,'bt_result_statistics',pdf_name))
                        pdf_list.append(pdf_name)
                #結合各個pdf
                for pdf in pdf_list:
                    merger.append(os.path.join(result_data_folder_path,'bt_result_statistics',pdf))
                with open(os.path.join(result_data_folder_path,'bt_result_statistics','groupingtest_by_' + col_lable_file + '_' + bt_period + '.pdf'), 'wb') as fout:
                    merger.write(fout) 
                # for pdf in pdf_list:
                #     os.remove(os.path.join(result_data_folder_path,'bt_result_statistics',pdf))
                #plt.show()
        #===繪圖 End===
# if __name__=='__main__':
#     from PyPDF2 import PdfFileMerger
#     import webbrowser
#     import os
#     dir_path = os.path.join(result_data_folder_path,'bt_result_statistics')
#     def list_files(directory, extension):
#         return (f for f in os.listdir(directory) if f.endswith('.' + extension))

#     pdfs = list_files(dir_path, "pdf")
#     merger = PdfFileMerger()

#     for pdf in pdfs:
#         merger.append(open( os.path.join(dir_path,pdf), 'rb'))

#     with open(os.path.join(dir_path,'result.pdf'), 'wb') as fout:
#         merger.write(fout)
    #df_result.set_index(['stockpool','bt_period','strategyname','bt_para','items'],inplace=True)
    #df_result.unstack()
        # watchlist_period_fold_list=[
    #     'MV_1_600_200801_202106'
        # 'ALL_200801_202105','MV_1_300_200801_202105','MV_301_600_200801_202105','MV_601_2000_200801_202105',
        # 'ALL_201301_202105','MV_1_300_201301_202105','MV_301_600_201301_202105','MV_601_2000_201301_202105'
    #]#'MV_1_300_200801_202105',
    # watchlist_period_fold_list=['ALL_201301_202105']
 
    # para_filter='[1_30_30]_[11]'    
    # for watchlist_period_fold_name in watchlist_period_fold_list:
    #     combine_result_by_meausre_in_watchlist(watchlist_period_fold_name,para_filter)
    
    # perf_item_list=['Annual return','Sharpe ratio','Calmar ratio','Sortino ratio','Omega ratio','Max drawdown','Stability','Tail ratio']
    # para_filter='[1_30_30]_[11]' 
    # combine_result_by_watchlist_period(watchlist_period_fold_list,para_filter,perf_item_list,os.path.join(result_data_folder_path,'comparision_watchlist_period_' + para_filter + '.xlsx'))