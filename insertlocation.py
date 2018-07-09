
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

datapath = '/data/bcp/010.812.346.523/003/1438683000/wabasicfj1003/xml'
destpath = '/bjrun/pybeacondb/destpath'

datalist = []
typelist = ['WA_BASIC_FJ_1003.bcp','WA_BASIC_FJ_0001.bcp','WA_BASIC_FJ_0006.bcp']

devinfosql = '''INSERT INTO LOCATION (netbarcode,netbarname,address,longitude,latitude,
        netsitetype,businessnature,principalname,principalcertype,principalcerid,link,
        starttime,endtime,supplierorgcode,datasource,access_type,operator_net,
        access_ip,doc_version,version,eventtime,charger,safer,saferid,
        chargerid,safertel,chargertel,email,orgname,software,
        terminalnum,department,fromip,toip,servicecode,acnum,height)
        VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,
        :18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37)
        '''

def exe_sql(oraDbB, cursorB):  

    cursorB.execute("select count(*) from user_tab_columns where table_name=upper('LOCATION')")
    max_column = cursorB.fetchone()
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
                LOCATION = [''] * max_column
                if line:
                    tmpline = line.strip('\n')
                    tmpline = tmpline.split('\t')
                    for i in range(len(tmpline)):
                        LOCATION[i] = tmpline[i]
                    M.append(tuple(LOCATION))
            file.close()
            for db1 in M:
                try:
                    N = []
                    N.append(db1)
                    oraDbB.insertBatch(devinfosql,N)
                    cursorB.execute("SELECT COUNT(*) FROM LOCATION")
                    print(cursorB.fetchone())
                    print('insertlocat ok.')
                except Exception as err:
                    Log.exe_log(bcpname,err,list(db1))
                    pass
            shutil.move(os.path.join(datapath,bcpname),os.path.join(destpath,bcpname))
    else:
        print("location dir is null\n")
        sleep(1)



