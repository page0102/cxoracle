
import fnmatch
import os
import re
import shutil
from time import sleep, ctime

import sys
sys.path.append('/bjrun/pybeacondb')
import oraclefunc as Oracle
import listdata as La
import cxoralcelog as Log

datapath = '/data/bcp/010.812.346.523/003/1438683000/wabasicfj1002/xml'
destpath = '/bjrun/pybeacondb/destpath'

datalist = []
typelist = ['WA_BASIC_FJ_1002.bcp','WA_BASIC_FJ_0003.bcp']
devinfosql = '''INSERT INTO DEVICEBASEINFO (netbarcode,        
      deviceid ,devicename ,deviceaddress ,devicetype , supplierorgcode ,  
      longitude  ,latitude ,uploadinterval ,radius ,vehiclecode ,lineinfo ,         
      vehicleinfo ,compartmentnumber ,datasource,ap_mac ,station_info ,     
      floor_info ,doc_version ,version  ,eventtime )
      VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21)
      '''

def exe_sql(oraDbA, cursorA):  

    cursorA.execute("select count(*) from user_tab_columns where table_name=upper('DEVICEBASEINFO')")
    max_column = cursorA.fetchone()
    max_column = max_column[0]
    M = []
    os.chdir(datapath)
    ruledir = os.getcwd()
    datalist = La.listdata(datapath, typelist)
    if datalist:
        while datalist:
            bcpname = datalist.pop(0)
            file = open(os.path.join(datapath,bcpname),'r',encoding='UTF-8')
            lines = file.readlines()
            for line in lines:
                devicebaseinfo = [''] * max_column
                if line:
                    tmpline = line.strip('\n')
                    tmpline = tmpline.split('\t')
                    for i in range(len(tmpline)):
                        devicebaseinfo[i] = tmpline[i]
                    M.append(tuple(devicebaseinfo))
            file.close()
            for db1 in M:
                try:
                    N = []
                    N.append(db1)
                    oraDbA.insertBatch(devinfosql,N)
                    cursorA.execute("SELECT COUNT(*) FROM DEVICEBASEINFO")
                    print(cursorA.fetchone())
                    print('insertbase ok.')
                except Exception as err:
                    Log.exe_log(bcpname,err,list(db1))
                    pass
            shutil.move(os.path.join(datapath,bcpname),os.path.join(destpath,bcpname))
    else:
        print("baseinfo dir is null\n")
        sleep(1)

