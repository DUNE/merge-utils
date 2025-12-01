import os, sys
from metacat.webapi import MetaCatClient

import csv
import math

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

tasklist = os.path.join(os.getenv("MERGE_UTILS_DIR"),"config","trg_mc_2025a",'trg_mc_2025a_jobs.csv')
maxjob = 50
tasks = {}

with open(tasklist,encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)
    #print(reader.fieldnames)
    
    for row in reader:
        #print(row["TAG"], row['FCL'], row['QUERY'])
        tasks[row["TAG"]] = row 
    
if len(sys.argv)<2:
    task="HELP"
else:
    task = sys.argv[1]
if task not in tasks:
    
        print(f"Task {task} not found")
        print("Available tasks:", ', '.join(tasks.keys()))
        sys.exit(1)
nfiles = math.ceil(int(tasks[task]['NFILES'])/100)
#f = open(f'{task}.sh','w')
print ("nfiles",nfiles)

query = "files where merge.tag=%s and dune.output_status=confirmed "%sys.argv[1]
tag = sys.argv[1]

skip = 0
while skip < nfiles+maxjob:
    thequery = query + "ordered skip %d limit %d"%(skip,maxjob)
    print (thequery)
    info = mc_client.query(query=thequery,summary="count") 
    count = info['count']
    print (f"tag {tag} skip {skip} got {count} files")
    skip += maxjob
