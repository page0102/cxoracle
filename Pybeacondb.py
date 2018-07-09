import cx_Oracle
import sys
import threading
from time import sleep, ctime

sys.path.append('/bjrun/pybeacondb')
import oraclefunc as Oracle
import insertbasinfo as Insbase
import insertlocation as Insloca
import insertstat as Inssta

def insert_baseinfo():
    print("  Start insertbaseinfo", ctime())
    oraDbA = Oracle.Oracle('runvista','runco','172.16.0.218','orcl')
    cursorA = oraDbA.cursor
    while True:
        try:
            Insbase.exe_sql(oraDbA, cursorA)
        except Exception as merr:
            print("Unexcept err when insert_baseinfo:", merr)

def insert_location():
    print("  Start insertlocation", ctime())
    oraDbB = Oracle.Oracle('runvista','runco','172.16.0.218','orcl')
    cursorB = oraDbB.cursor
    while True:
        try:
            Insloca.exe_sql(oraDbB, cursorB)
        except Exception as merr:
            print("Unexcept err when insert_location:", merr)

def insert_status():
    print("  Start insertstatus", ctime())
    oraDbC = Oracle.Oracle('runvista','runco','172.16.0.218','orcl')
    cursorC = oraDbC.cursor
    while True:
        try:
            Inssta.exe_sql(oraDbC, cursorC)
        except Exception as merr:
            print("Unexcept err when insert_status:", merr)


funcs = [insert_baseinfo, insert_location, insert_status]


def main():

    nfuncs = range(len(funcs))
    threads = []

    for i in nfuncs:
        t = threading.Thread(target=funcs[i], args=())
        threads.append(t)

    for i in nfuncs:
        threads[i].start()

    for i in nfuncs:
        threads[i].join()


if __name__ == '__main__':
    main()

