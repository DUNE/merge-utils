import os, sys
from ROOT import TFile, TTree

filename = sys.argv[1]

file = TFile.Open(filename,"READONLY")

n = file["triggerAna"]["event_summary"].GetEntries()
print (n)
print (n/50)

