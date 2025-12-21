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

        if not os.path.exists(file):
            print(" does not exist")
            gone.append(file)
        else:
            try:
                f = TFile.Open(file,'readonly')
                f.ls()
                f.Close()
            except:
                bad.append(file)
    return bad,gone


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print ("please provide a list of files")
        print ("rootcheck.py f1 f2 f3 f4")
        sys.exit(1)
    else:
        bad,gone = rootcheck(sys.argv[1:])
        for x in bad:
            print ("BAD",x)
        for x in gone:
            print ("MISSING",x)
    sys.exit(len(bad))           
