import os
import shutil
from datetime import datetime
from marketrawdb_cm_importer import MARKETRAWDB_CM_IMPORTER
import logging
logging.basicConfig(level=logging.ERROR)

#Configuration
path_datafolder = os.path.join(os.path.dirname(os.path.dirname(__file__)),'data')  #設定data資料夾
dbconfig={'host':'home.dottdot.com','port':'13306','user':'datateam','password':'datateam','database':'marketrawdb_cm'}

def main():
    updatedata_path = os.path.join(path_datafolder,'updatedata')
    if os.listdir(updatedata_path)==[]:
        logging.warning("No new data")
    else:
        for fd in os.listdir(updatedata_path):
          _,ext = os.path.splitext(os.path.join(updatedata_path,fd))
          if ext == '.csv':
            dcname = fd[4:-13]
            datadate =  fd[-12:-4]
            source_path=os.path.join(updatedata_path, fd)
            target_path=os.path.join(path_datafolder, dcname, fd)
            #為了解決虛擬硬碟amefileerror問題
            def my_same_file_diff_checker(*args, **kwargs):
                return False
            shutil._samefile = my_same_file_diff_checker
            shutil.copy(source_path, target_path)
            rawfile_path = os.path.join(path_datafolder, dcname,  'raw_' + dcname + '_' + datadate + 'csv')
            stdfile_path = os.path.join(path_datafolder, dcname,  'std_' + dcname + '_' + datadate + 'csv')
            obj = MARKETRAWDB_CM_IMPORTER(dcname, datetime.strptime(datadate,'%Y%m%d'), rawfile_path , stdfile_path, dbconfig)
            obj.data_import()

            os.remove(source_path)
        
if __name__=='__main__':
    main()