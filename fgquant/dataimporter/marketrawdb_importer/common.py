import csv
import logging as log
# import os
import shutil
from datetime import date, datetime, timedelta

# import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import relativedelta
from sqlalchemy import create_engine

log.basicConfig(level=log.WARNING, format='%(asctime)s - %(levelname)s : %(message)s')
header = {'user-agent': 'Mozilla/5.0 (Macintosh Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'}
class OutputStdCsv:
    '''Output to self-defined standard format csv file.
    The format of csv file as followed
    line msg if display :error_code,exe_msg,querydate,datadate,exe_time
    line type:DataFieldType
    line header:DataFieldName''' 
    def __init__(self, array_fieldname: tuple, array_fielddatatype: tuple, path_stdcsv: str, is_show_msg: int =0)-> None:
        self.error_code = 0
        self.exe_msg = ''
        self.exe_time = 0
        self.is_show_msg = is_show_msg # if set to 1 then the first line of output csv file provides process msg.
        self.row_firstdata = 3 if self.is_show_msg == 0 else 4 
        self.querydate = datetime.today()
        self.datadate = datetime(1900,1,1)
        self.__ResultArray = []
        self.path_stdcsv = path_stdcsv
        self.fieldnamearray = []
        self.fielddatatypearray =[]
        self.fieldnamearray = array_fieldname
        self.fielddatatypearray = array_fielddatatype
        if len(array_fieldname) == 0 or len(array_fielddatatype) == 0 :
          self.error_code = 1
          self.exe_msg = 'array_fieldname or array_fielddatatype is empty'
        if len(array_fieldname) != len(array_fielddatatype):
          self.error_code = 1
          self.exe_msg = 'The number of columns in the array_fieldname and array_fielddatatype are not matched'
        if not path_stdcsv.upper().endswith('CSV'):
          self.error_code = 1
          self.exe_msg = 'path_stdcsv is not csv file'
    def Add(self, array_newdata: list) -> None:
        '''Add new data to result list.'''
        newdata = list(str(ele).strip().replace(',','').replace('"','') for ele in array_newdata)
        self.__ResultArray.append(newdata)
    def outputcsv(self) -> None:
        '''write elements of result list in the csv file.'''
        with open(self.path_stdcsv, 'w', newline='',encoding='utf-8') as tmpfile:
            csvwriter=csv.writer(tmpfile)
            if self.error_code == 0:
                if self.is_show_msg == 1:
                    #msg line:error_code,xe_msg,querydate,datadate,exe_time
                    csvwriter.writerow([self.error_code, self.exe_msg, self.row_firstdata, self.querydate.strftime('%Y%m%d'), self.datadate.strftime('%Y%m%d'), self.exe_time])
                #line:DataFieldType
                csvwriter.writerow(self.fielddatatypearray)
                #line:DataFieldName
                csvwriter.writerow(self.fieldnamearray)
                #below: data
                csvwriter.writerows(self.__ResultArray)
            else:
                csvwriter.writerow([self.error_code, self.exe_msg])
        dayfile = self.path_stdcsv[:-4] + '_on_' + self.querydate.strftime('%Y%m%d') + '.csv'
        #shutil.copy2(self.path_stdcsv, dayfile) # save anther copy
def convert_datadate(datadate: datetime, datetype: str ='') -> str:  #datetype:'S':%Y/%m/%d,'M' mingquo yyymmdd
  if datetype.upper() == 'S':
    return datadate.strftime('%Y/%m/%d')
  elif datetype.upper() == 'M':
    datearray=[str(datadate.year-1911), str(format(datadate.month, '02')), str(format(datadate.day, '02'))]
    return ''.join(datearray)
  elif datetype.upper() == 'MS':
    datearray=[str(datadate.year-1911), str(format(datadate.month, '02')), str(format(datadate.day, '02'))]
    return '/'.join(datearray)
  else:
    return datadate.strftime('%Y%m%d')
def convert_datatype(value,fieldtype: str):
  if fieldtype == 'S':
    return str(value).replace("'" , "\\'")     
  elif fieldtype == 'S1':
    return str(value).replace('=' , '').replace("'" , "\\'") .strip()      
  elif fieldtype == 'N':
    #str(value).replace(',' , '')避免數字中有逗號
    return (float(str(value).replace(',' , '')) if isfloat(str(value).replace(',' , '')) else 'Null')
  elif fieldtype == 'NU':
    return 'Null'     
  elif fieldtype == 'D':#this for YYYYMMDD
    if value == '0' or value=='': 
      return '19000101'
    else:
     return datetime.strptime(value, '%Y%m%d').strftime('%Y%m%d')
  elif fieldtype == 'D1':#YYYY/MM/DD
    if value == '0' or value=='':
      return '19000101'
    else:
     return datetime.strptime(value, '%Y/%m/%d').strftime('%Y%m%d')
  elif fieldtype == 'D2':#mingquo YYY/MM/DD
    if value == '0' or value=='' or value == '&nbsp'  or value == '不適用':#&nb 是md_mops_dividendpolicy有部分日期資料會是&nb 與'不適用'
      return '19000101'
    else:
      pos1=value.find('/')
      return datetime.strptime(value.replace(value[:pos1],str(int(value[:pos1])+1911)), '%Y/%m/%d').strftime('%Y%m%d')
  elif fieldtype == 'D3':#YYYYMMDD
    if value == '0' or value=='':
      return '19000101'
    else:
      return value
  elif fieldtype == 'D4':#ming quo YYYMMDD
    if value == '0' or value=='':
      return '19000101'
    else:
      pos1=2 if int(value[:3])>500 else 3
      return datetime.strptime(value.replace(value[:pos1],str(int(value[:pos1])+1911)), '%Y%m%d').strftime('%Y%m%d')
  elif fieldtype == 'D5':#ming quo YYY年MM月DD日 md_tse_exdividendcalculation
    if value == '0' or value=='':
      return '19000101'
    else:
      pos1=value.find('年')
      return datetime.strptime(value.replace(value[:pos1],str(int(value[:pos1])+1911)).replace('年','').replace('月','').replace('日',''), '%Y%m%d').strftime('%Y%m%d')
  elif fieldtype == 'X':
    return 'Null'
def find_line_start(dataset: list,dc_info: dict) -> int:
    line_start = 100000
    first_row_header = dc_info['first_row_header']
    path_rawcsv = dc_info['path_rawcsv']
    if str(first_row_header).isnumeric(): #if the type of value is number
        line_start = int(first_row_header)
    else:
        i = 0
        for row in dataset:
            i += 1
            if "".join(row).replace('\u3000','').startswith(first_row_header): #md_tse_dailyquotes_twse_returnindex 的日/u3000期
                line_start = i+1
                break
    return line_start
def isfloat(value)-> bool:
  try:
    float(value)
    return True
  except ValueError:
    return False
def crawlwebdata(dc_info: dict, datadate: datetime) -> None:
    log.info("Start-->crawlwebdata('%s')" % dc_info['dcname'])
    try:
        today = datetime.today().strftime("%Y%m%d")
        url = dc_info['url']
        parameters = dc_info['get_param']
        crawl_type=dc_info['flag_sourcetype']
        #以下為客製爬蟲網頁資訊
        if dc_info['dcname']  == 'md_ctbc_instholderstat_tw':
            get_webdata_ctbc_instholderstat_tw(datadate)
        elif dc_info['dcname']  == 'md_tdcc_stockholderstructure':
            get_webdata_tdcc_stockholderstructure(datadate)
        else:
            if crawl_type == 1:#CSV
              r = requests.get(url, params=parameters,verify=False,headers=header)
              r.encoding='big5'
              path_rawcsv = dc_info['path_rawcsv']
              dayfile = path_rawcsv[:-4] + '_on_' + today + '.csv'
              with open(path_rawcsv, 'wt',encoding='utf-8') as f :
                  f.write(r.text)
              try:
                  shutil.copy2(path_rawcsv, dayfile) # save anther copy
              except:
                log.warning('Error occurred --> crawlwebdata:count not make a copy of %s' % path_rawcsv)
                raise
            elif crawl_type == 2:#Web crawl
                if dc_info['dcname'] == 'md_otc_foreigninvestorhold':
                    r = requests.get(url, params=parameters,verify=False,headers=header)
                    r.encoding='big5'
                    path_rawcsv = dc_info['path_rawcsv']
                    Array_SourceFieldDataName = dc_info['Array_SourceFieldDataName']
                    Array_SourceFieldDataType = dc_info['Array_SourceFieldDataType']
                    is_show_msg=0
                    obj_OutputStdCsv = OutputStdCsv(Array_SourceFieldDataName, Array_SourceFieldDataType, path_rawcsv,is_show_msg)
                    soup = BeautifulSoup(r.text, 'html.parser')
                    try:
                        rows = soup.find('table', {'border':'1'}).find_all('tr')
                    except AttributeError as err1:
                        raise Exception('md_otc_foreigninvestorhold:Web has no data!')
                    i=0
                    for row in rows:
                        i = i + 1
                        data_array_source=[]
                        if i>=3: 
                            all_tds=row.find_all('td')
                            for td in all_tds:
                                data_array_source.append(td.text.strip())
                            obj_OutputStdCsv.Add(data_array_source)
                    obj_OutputStdCsv.outputcsv()
                elif dc_info['dcname'] == 'md_mops_monthsales':
                    path_rawcsv = dc_info['path_rawcsv']
                    Array_SourceFieldDataName = dc_info['Array_SourceFieldDataName']
                    Array_SourceFieldDataType = dc_info['Array_SourceFieldDataType']
                    is_show_msg=0
                    obj_OutputStdCsv = OutputStdCsv(Array_SourceFieldDataName, Array_SourceFieldDataType, path_rawcsv,is_show_msg)
                    for url1 in url:
                        r = requests.get(url1, params=parameters,verify=False,headers=header)
                        log.debug("crawlwebdata request headers: %s", r.headers)
                        r.encoding='big5'
                        soup = BeautifulSoup(r.text, 'html.parser')
                        tables_industry = soup.find_all('table', {'width':'100%','border':'5'})
                        for table_industry in tables_industry:
                            rows=table_industry.find_all('tr')
                            i=0
                            for row in rows:
                                i = i + 1
                                data_array_source=[]
                                if i>=3: 
                                    all_tds=row.find_all('td')
                                    for td in all_tds:
                                        data_array_source.append(td.text.strip())
                                    if len(data_array_source) == 11:#避開合計欄位
                                        obj_OutputStdCsv.Add(data_array_source)
                    obj_OutputStdCsv.outputcsv()  
                elif dc_info['dcname'] == 'md_mops_dividendpolicy':
                    path_rawcsv = dc_info['path_rawcsv']
                    Array_SourceFieldDataName = dc_info['Array_SourceFieldDataName']
                    Array_SourceFieldDataType = dc_info['Array_SourceFieldDataType']
                    is_show_msg=0
                    obj_OutputStdCsv = OutputStdCsv(Array_SourceFieldDataName, Array_SourceFieldDataType, path_rawcsv,is_show_msg)
                    for url1 in url:
                      r = requests.get(url1, params=parameters,verify=False,headers=header)
                      r.encoding='big5'
                      soup = BeautifulSoup(r.text, 'lxml')
                      rows=soup.find_all('tr', {'align':'center','class':['odd','even']})
                      for row in rows:
                          data_array_source=[]
                          all_tds=row.find_all('td')
                          for td in all_tds:
                              data_array_source.append(td.text.strip())
                          obj_OutputStdCsv.Add(data_array_source)
                    obj_OutputStdCsv.outputcsv()
    except Exception as err:
        log.warning("Error occurred --> crawlwebdata('%s'): %s" % (dc_info['dcname'],err))
        raise
    finally:
        log.info("End-->crawlwebdata('%s')" % dc_info['dcname'])
def rawcsv_to_stdcsv(dc_info:dict, datadate:datetime) -> None: 
    '''output a standard format csv file from the rawdata csv got from web source''' 
    log.info("Start-->rawcsv_to_stdcsv('%s')", dc_info['dcname'])
    try:
        querydate = datetime.today()
        time_start = datetime.today()        
        DCName = dc_info['dcname']
        flag_fieldmapping = dc_info['flag_fieldmapping']
        path_rawcsv = dc_info['path_rawcsv']
        path_stdcsv = dc_info['path_stdcsv']
        Array_TargetFieldDataType = dc_info['Array_TargetFieldDataType']
        Array_TargetFieldDataName = dc_info['Array_TargetFieldDataName']
        Array_SourceFieldDataType = dc_info['Array_SourceFieldDataType']
        is_show_msg=1
        msg_exe='conversion completed!'
        lineseq = 0
        IsHasData = 0
        obj_OutputStdCsv = OutputStdCsv(Array_TargetFieldDataName, Array_TargetFieldDataType, path_stdcsv,is_show_msg)
        if  obj_OutputStdCsv.error_code == 0:
          with open(path_rawcsv, newline='',encoding='ansi') as csvfile:              
              rows =csv.reader(csvfile)
              list_csv=list(rows)
              #find the the beginning row of data
              lineseq_start = find_line_start(list_csv,dc_info)
              log.debug('lineseq_start:%s',lineseq_start )
              for row in list_csv:
                  lineseq += 1
                  data_array_source=[]
                  if lineseq >= lineseq_start:
                      if row != []: 
                          log.info("Progessing-->rawcsv_to_stdcsv('%s'): %d / %d" % (dc_info['dcname'],lineseq,len(list_csv)))
                          if lineseq == lineseq_start:
                               #check if field num are matched
                              if len(row[0]) == len(Array_SourceFieldDataType):
                                  raise Exception('The nums of field and data source are matched!')
                              elif str(row[0]).strip() == '共0筆':#md_otc_exdividendcalculation與md_otc_capitalreduction若沒資料會出現'共0筆'
                                  raise Exception('No Data!')
                              elif str(row[0]).strip() == '"說明："':#md_tse_foreigninvestorhold若無資料第一行會出現'說明:'
                                  raise Exception('No Data!')
                          #check data
                          IsOutput = IsAllowedData(DCName, row,Array_SourceFieldDataType)
                          if IsOutput == -1:
                              pass
                          elif IsOutput == 1:
                              IsHasData = 1
                              for j in range(len(Array_SourceFieldDataType)):
                                  if Array_SourceFieldDataType[j].upper() != 'X':
                                      data_array_source.append(convert_datatype(row[j],Array_SourceFieldDataType[j]))
                              data_array_target = GetFieldMappingArray(DCName, datadate, flag_fieldmapping,  data_array_source)
                              obj_OutputStdCsv.Add(data_array_target)
          if IsHasData==0:
            raise Exception('rawcsv_to_stdcsv:No Data!')

    except Exception as err:
        msg_exe=err
        obj_OutputStdCsv.error_code=1
        log.warning('Error occurred --> rawstd_to_stdcsv("%s"):%s' % (DCName,err))
        raise
    finally:
        if is_show_msg == 1:
          obj_OutputStdCsv.exe_msg=msg_exe
          obj_OutputStdCsv.querydate=querydate
          obj_OutputStdCsv.datadate=datadate
          obj_OutputStdCsv.exe_time=(datetime.today()-time_start).total_seconds()
        obj_OutputStdCsv.outputcsv()
        log.info("End-->rawstd_to_stdcsv('%s')" % dc_info['dcname'])
def importdb_stdcsv(dc_info:dict,dbconfig:dict)->None:
    log.info("Start-->importdb_stdcsv('%s')", dc_info['dcname'])
    try:
        #conn = mysql.connector.connect(**dbconfig)
        conn = create_engine("mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(user=dbconfig['user'],pw=dbconfig['password'],host=dbconfig['host'],port=dbconfig['port'],db=dbconfig['database'])).connect()
        # conn = create_engine(**dbconfig).connect()
        cursor=conn.cursor()
        path_stdcsv=dc_info['path_stdcsv']
        sp_name=dc_info['sp_name']
        Array_TargetFieldDataType = dc_info['Array_TargetFieldDataType']
        tmplist=['%s' if ele == 'N' else "'%s'" for ele in Array_TargetFieldDataType]
        _SQL='call ' + sp_name + '(' + ','.join(tmplist) + ')'

        # 改成矩陣上傳==============
        tmplist.insert(0,'%s')
        tmplist.append("'%s'")
        tmplist.append("'%s'")
        tmplist.append("'%s'")
        tmplist.append("'%s'")
        _V1=''
        insertNum=1
        # 改成矩陣上傳==============
        
        with open(path_stdcsv,'r',encoding='utf-8') as csvfile:
            rows=list(csv.reader(csvfile))
            if int(rows[0][0]) == 0:#errorcode=0 means no error occures
              array_datatype=rows[1]
              row_firstdata=int(rows[0][2]) 
              i = 0
              
              for row in rows:
                i += 1
                if i >= row_firstdata:
                    
                    # 改成矩陣上傳==============
                     row.append(datetime.now())
                     row.append('DataTeam')
                     row.append(datetime.now())
                     row.append('DataTeam')
                     row.insert( 0, '0')
                     _V='(' + ','.join(tmplist) + ')'
                     _V1=_V1 + (_V %tuple(row)).replace("'Null'",'Null').replace("''",'Null') +',' 
                    
                     if i>=insertNum*1000:
                         SQL1='REPLACE  INTO ' + dc_info['dcname'] +' values'+  _V1[:-1]
                         cursor.execute(SQL1)
                         insertNum=insertNum+1
                         _V1=''
                         print(i,'/',len(rows),'迴圈內上傳!!!!!!',dc_info['path_stdcsv'])
                    # 改成矩陣上傳==============
                    
                    # _SQL_insert=(_SQL %tuple(row)).replace("'Null'",'Null')
                    # _SQL_insert=(_SQL %tuple(row)).replace("''",'Null')
                    # log.info("Progessing-->importdb_stdcsv('%s'): %d / %d" % (dc_info['dcname'],i,len(rows)))
                    # log.debug("importdb_stdcsv SQL: %s", _SQL_insert)
                    # cursor.execute(_SQL_insert)
                    # print(_SQL_insert,'/',i)
                    
              # 改成矩陣上傳==============   
              SQL1='REPLACE  INTO ' + dc_info['dcname'] +' values'+  _V1[:-1]
              cursor.execute(SQL1)
              print(i,'/',len(rows),'迴圈外上傳!!!!!',dc_info['path_stdcsv'])
              # 改成矩陣上傳==============
    
    except  Exception as err:
        log.warning('Error occurred --> importdb_stdcsv("%s"): %s' % (dc_info['dcname'],err))
        conn.rollback()
        raise
    finally:
        log.info("End-->importdb_stdcsv('%s')" % dc_info['dcname'])
        conn.commit()
        conn.close()
def insert_log_sys(dbconfig: dict, QueryDate: datetime,DataDate: datetime,TableName: str,ProcName: str,ErrorCode_Sys: int,ErrMsg_Sys: str,ErrMsg_User: str,ExecTime: float) -> None:
    try:
        #conn = mysql.connector.connect(**dbconfig,autocommit=True)
        conn = create_engine("mysql+pymysql://{user}:{pw}@{host}:{port}/{db}".format(user=dbconfig['user'],pw=dbconfig['password'],host=dbconfig['host'],port=dbconfig['port'],db=dbconfig['database'])).connect()
        # conn = create_engine(**dbconfig,autocommit=True).connect()
        cursor = conn.cursor(dictionary=True)
        _sql="insert log_sys Values(0,'%s','%s','%s','%s',%d,'%s','%s',%.2f,now(),%d,'',now());" % (QueryDate.strftime('%Y%m%d'),DataDate.strftime('%Y%m%d'),TableName,ProcName,ErrorCode_Sys,ErrMsg_Sys,ErrMsg_User,ExecTime,1 if ErrorCode_Sys ==0 else 0)
        cursor.execute(_sql)
    except Exception as err:
        log.warning('Error occurred --> insert_log_sys("%s"): %s' % (TableName,err))
#以下函數可能需要因為資料來源而客製
def get_datadate(dcname: str,querydate: datetime) -> datetime:
    '''將查詢日期轉換為資料日期'''
    try:
        if dcname == 'md_mops_monthsales':
            if querydate.day < 15:
                return (querydate+relativedelta.relativedelta(months=-1)).replace(day=1)
            else:
                return querydate.replace(day=1)
        elif dcname == 'md_mops_dividendpolicy':
            return querydate.replace(month=1,day=1)
        else:
            return querydate 
    except Exception as err: 
        log.warning('Error occurred --> get_datadate("%s"): %s' % (dcname,err))
def IsAllowedData(dcname: str,data_array_source: list, field_array_source: list)->int:
  if len(data_array_source) != len(field_array_source):
    return 0
  vIsAllowedData = 1
  if dcname == 'md_otc_dailyquotes':
    if str(data_array_source[0]).startswith(('7','代號')):#排除權證（開頭為7,除了7402邑錡),與管理股票之表頭
      if str(data_array_source[0]) != '7402':
        vIsAllowedData = 0
  elif dcname == 'md_rotc_dailyquotes':
    if str(data_array_source[1]).startswith('合計'):#排除合計
       vIsAllowedData = 0
  elif dcname == 'md_mops_monthsales' or dcname == 'md_mops_dividendpolicy' or dcname == 'md_otc_foreigninvestorhold' or dcname == 'md_mops_insiderhold' or dcname == 'md_mops_insiderpledge':
    if not str(data_array_source[0]).replace('""', '')[:4].isnumeric(): #第一欄非股票代碼就排除
       vIsAllowedData = 0
  elif dcname == 'md_ctbc_dividendpolicy' or dcname == 'md_cnyes_dividendpolicy':
    tmpStr = str(data_array_source[0]).replace('""', '')
    if tmpStr == '' or tmpStr.upper() == 'NULL':
       vIsAllowedData = 0
    else:
      if str(data_array_source[0]).replace('""', '')[:4].isnumeric(): #判別股利年度是否為數字'
        if int(tmpStr[:4]) < datetime.datetime.today().year - 1: #僅更新近兩年資料'
          vIsAllowedData = 0
      else:
        vIsAllowedData = 0
  return vIsAllowedData  
def GetFieldMappingArray(dcname: str, datadate: datetime,flag_fieldmapping: int,data_array_source: list)->list:
  if flag_fieldmapping == 1: #目標欄位第一欄為日期,其他欄位完全對應
    data_array_source.insert(0,datadate.strftime('%Y%m%d'))
    return data_array_source[:]
  elif flag_fieldmapping == 2: #完全對應
    return data_array_source
  elif flag_fieldmapping == 3: #自訂對應
    if dcname == 'md_rotc_dailyquotes':#來源資料第一欄為header
      data_array_source[0]=datadate.strftime('%Y%m%d')
    elif dcname == 'md_mops_dividendpolicy':#來源資料的第一欄位代碼名稱拆為兩欄代碼與名稱
      tmplist=data_array_source[0].split('-')
      data_array_source[0]=tmplist[0].strip()
      data_array_source.insert(1,tmplist[1].strip())
    return data_array_source
#為客製爬蟲網頁資訊
def get_webdata_tdcc_stockholderstructure(querydate:datetime) -> None:

    today = datetime.today().strftime("%Y%m%d")
    dcname='md_tdcc_stockholderstructure'
    dc_info = dc_settings(dcname,querydate)
    DCName = dc_info['dcname']
    path_rawcsv = dc_info['path_rawcsv']
    url = dc_info['url']
    parameters = dc_info['get_param']
    Array_SourceFieldDataName = dc_info['Array_SourceFieldDataName']
    Array_SourceFieldDataType = dc_info['Array_SourceFieldDataType']
    is_show_msg=0
    obj_OutputStdCsv = OutputStdCsv(Array_SourceFieldDataName, Array_SourceFieldDataType, path_rawcsv,is_show_msg)
    try:
        r = requests.get(url, params=parameters,verify=False,headers=header)
        r.encoding='utf-8'
        rows = csv.reader(r.text.splitlines())
        data_array_source=[None]*53
        for row in rows:
            if row[2] in ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17']:
                datadate = row[0]
                StockCode = row[1]
                LevelCode = int(row[2])
                Num_SharesHolder = float(row[3])
                Shares = float(row[4])
                Percent = float(row[5])
                data_array_source[0] = datadate
                data_array_source[1] = StockCode
                data_array_source[LevelCode * 3 - 1] = Num_SharesHolder
                data_array_source[LevelCode * 3 ] = Shares
                data_array_source[LevelCode * 3 + 1] = Percent
                if LevelCode == 17:
                  obj_OutputStdCsv.Add(data_array_source)
        obj_OutputStdCsv.outputcsv() 
    except Exception as err:
        log.warning('Error occurred --> get_webdata_tdcc_stockholderstructure):%s' % err)
        raise
def get_webdata_ctbc_instholderstat_tw(querydate:datetime) -> None:

    dcname='md_ctbc_instholderstat_tw'
    dc_info = dc_settings(dcname,querydate)
    DCName = dc_info['dcname']
    path_rawcsv = dc_info['path_rawcsv']
    url = dc_info['url']
    parameters = dc_info['get_param']
    Array_SourceFieldDataName = dc_info['Array_SourceFieldDataName']
    Array_SourceFieldDataType = dc_info['Array_SourceFieldDataType']
    is_show_msg=0
    obj_OutputStdCsv = OutputStdCsv(Array_SourceFieldDataName, Array_SourceFieldDataType, path_rawcsv,is_show_msg)
    try:
        stocklist=get_stocklist(querydate)
        i=0
        for stock in stocklist:
            i=i+1
            log.info("Progessing-->get_webdata_ctbc_instholderstat_tw: %d / %d" % (i,len(stocklist)))
            url1=url.replace('xxxx',stock)
            r = requests.get(url1, verify=False,headers=header)
            r.encoding ='Big5' #若無編碼會出現亂碼
            soup = BeautifulSoup(r.text, 'lxml')
            rows = soup.find('table', {'class':'t01','border':'0'}).find_all('td',{'class':['t3n1','t3r1']})
            #get datadate
            datadate_str=soup.find('div','t11').text.split('：')[-1].split('/') #ex: 08/08 -->['08','08']
            if datetime.today().month == 1 and int(datadate_str[0]) == 12:
                datadate=datetime(datetime.today().year-1,int(datadate_str[0]),int(datadate_str[1]))
            else:
                datadate=datetime(datetime.today().year,int(datadate_str[0]),int(datadate_str[1]))

            data_array_source=[]
            data_array_source.append(datadate.strftime('%Y%m%d'))
            data_array_source.append(stock)
            for row in rows:
                if row.text.strip() == '' or row.text.strip() == 'N/A':
                    value='null'
                elif row.text.find('%') > -1:
                    value=round(float(row.text.replace('%',''))/100,4)
                else:
                    value=float(row.text.replace(',',''))
                data_array_source.append(value)
            obj_OutputStdCsv.Add(data_array_source)
        obj_OutputStdCsv.outputcsv()
    except Exception as err:
        log.warning('Error occurred --> get_webdata_ctbc_instholderstat_tw):%s' % err)
        raise

