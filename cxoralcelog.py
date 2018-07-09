import os
import json
logpath = '/bjrun/pybeacondb/log'

devbasetable = ['NETBARCODE','DEVICEID','DEVICENAME','DEVICEADDRESS','DEVICETYPE','SUPPLIERORGCODE',
        'LONGITUDE','LATITUDE','UPLOADINTERVAL','RADIUS','VEHICLECODE','LINEINFO','VEHICLEINFO',
        'COMPARTMENTNUMBER','DATASOURCE','AP_MAC','STATION_INFO','FLOOR_INFO','DOC_VERSION',
        'VERSION' ,'EVENTTIME','COORDINATE_TYPE']

locationbable = ['NETBARCODE','NETBARNAME','ADDRESS','LONGITUDE','LATITUDE','NETSITETYPE','BUSINESSNATURE',
        'PRINCIPALNAME','PRINCIPALCERTYPE','PRINCIPALCERID','LINK','STARTTIME','ENDTIME',
        'SUPPLIERORGCODE','DATASOURCE','ACCESS_TYPE','OPERATOR_NET','ACCESS_IP',
        'DOC_VERSION','VERSION'  ,'EVENTTIME' ,'CHARGER','SAFER','SAFERID',
        'CHARGERID','SAFERTEL','CHARGERTEL','EMAIL','ORGNAME','SOFTWARE',
        'TERMINALNUM','DEPARTMENT','FROMIP','TOIP','SERVICECODE',
        'ACNUM','HEIGHT']
	
def exe_log(bcpname, errlog, format_line):
    if bcpname[-17:-4] in ['BASIC_FJ_1002','BASIC_FJ_0003']:
        logfile = bcpname[:-3]+'log'
        logcont = zip(devbasetable,format_line)
        logcont = list(logcont)
        logcont = dict(logcont)
        logcont = json.dumps(logcont, indent=4, ensure_ascii=False)
        try:
            fname = open(os.path.join(logpath,logfile),'a')
            fname.write(str(logcont))
            fname.write(" Error: " + str(errlog) + '\n')
            fname.close()
        except:
            ft = open(os.path.join(logpath,"Unknow_error.log"),'a')
            ft.write("unknow err was happend when write errfile")
            ft.close()
    elif bcpname[-17:-4] in ['BASIC_FJ_1003','BASIC_FJ_0001','BASIC_FJ_0006']:
        logfile = bcpname[:-3]+'log'
        logcont = zip(locationbable,format_line)
        logcont = list(logcont)
        logcont = dict(logcont)
        logcont = json.dumps(logcont, indent=4, ensure_ascii=False)
        try:
            fname = open(os.path.join(logpath,logfile),'a')
            fname.write(str(logcont))
            fname.write(" Error: " + str(errlog) + '\n')
            fname.close()
        except:
            ft = open(os.path.join(logpath,"Unknow_error.log"),'a')
            ft.write("unknow err was happend when write errfile")
            ft.close()

