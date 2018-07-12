''''' 

'''  
import os
import re
import fnmatch
  
def listdata(path, FileType = None):
    datalist = []
    if  FileType:
        datadir = path
        os.chdir(datadir)
        for ftype in FileType:
            for filename in os.listdir("."):
                if fnmatch.fnmatch(filename, "*" + ftype):
                    datalist.append(filename)
    else:
        datadir = path
        os.chdir(datadir)
        for filename in os.listdir("."):
            if fnmatch.fnmatch(filename, "*" + 'bcp'):
                datalist.append(filename)
            else:
                continue
    return datalist

