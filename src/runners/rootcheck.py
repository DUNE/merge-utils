''' open a root file and check contents '''
''' returns # of bad files '''
from ROOT import TFile

import os, sys


def rootcheck(names):
    bad = []
    gone = []
    for filename in names:
        print ("checking file",filename)
        file = filename.strip()

        try:
            f = TFile.Open(file,'readonly')
            f.ls()
            f.Close()
        except:
            bad.append(file)
    return bad


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print ("please provide a list of files")
        print ("rootcheck.py f1 f2 f3 f4")
        sys.exit(1)
    else:
        bad = rootcheck(sys.argv[1:])
        for x in bad:
            print ("BAD",x)
        
    sys.exit(len(bad))           
