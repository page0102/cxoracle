
import sys
sys.path.append('/bjrun/pybeacondb')
import oraclefunc as Oracle
import listdata as La
import os
import re
import fnmatch
import shutil
from time import sleep, ctime

datapath = '/data/bcp/010.812.346.523/003/1438683000/status/xml'
logpath = '/bjrun/pybeacondb/log'
destpath = '/bjrun/pybeacondb/destpath'

datalist = []
typelist = ['Status.bcp']

sql = "INSERT INTO DEVICESTATUS \
             (DEVICEID, SERVERSTATUS, DATASTATUS, CPUUSED, MEMUSED, STORAGEUSED,SENDTIME, UPLOADINTERVAL) \
              VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"

def compile_regular():
    Status_regular = re.compile(r'^(.{21})-(0[1,2])-(0[1,2])-([0-9]{1,3})-([0-9]{1,3})-([0-9]{1,3})-([0-9]{1,8})-([0-9]{10})$')
    return Status_regular

def exe_sql(oraDbC, cursorC):
 
    M = []
    os.chdir(datapath)
    ruledir = os.getcwd()
    Status_regular = compile_regular()
    datalist = La.listdata(datapath, typelist)
    if datalist:
        while datalist:
            bcpname = datalist.pop(0)
            file = open(os.path.join(datapath,bcpname),'r')
            lines = file.readlines()
            for line in lines:
                if line:
                    try:
                        stc = line.replace('\t','-')
                        Sts = Status_regular.match(stc).groups()
                        cloumn1,cloumn2,cloumn3,cloumn4,cloumn5,cloumn6,cloumn7,cloumn8 = Sts#提取元组中的变量，否则回出错
                        M.append((cloumn1, cloumn2, cloumn3, cloumn4, cloumn5, cloumn6, cloumn8, cloumn7,))
             
                    except AttributeError:
                        f = open(os.path.join(logpath,bcpname),'a')
                        f.write(line)
                        f.close()
                        continue

            file.close()
            shutil.move(os.path.join(datapath,bcpname),os.path.join(destpath,bcpname))
        oraDbC.insertBatch(sql,M)
        cursorC.execute("SELECT COUNT(*) FROM DEVICESTATUS")
        print(cursorC.fetchone())
        print('insert status ok.')
    else:
        print("status dir is null\n")
        sleep(1)

