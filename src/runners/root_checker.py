''' open a root file and check contents '''
from ROOT import TFile

import os, sys


def rootcheck(names):
    for filename in names:
        print ("checking file",filename)
        file = filename.strip()
        if not os.path.exists(file):
            print(" does not exist")
        else:
            f = TFile.Open(file,'readonly')
            f.ls()


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print ("using local test sample, otherwise please provide a list of files")
        filelist = open('test.txt','r').readlines()
        rootcheck(filelist)
    else:
        rootcheck(sys.argv[1:])
