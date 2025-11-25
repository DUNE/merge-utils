import os, sys
from metacat.webapi import MetaCatClient

import csv

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

tasklist = os.path.join(os.getenv("MERGE_UTILS_DIR"),"config","trg_mc_2025a",'trg_mc_2025a_jobs.csv')
maxjob = 2000
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
nfiles = int(tasks[task]['NFILES'])
f = open(f'{task}.sh','w')
print ("nfiles",nfiles)

query = "files where merge.tag=%s-pass2 and dune.output_status=confirmed"%sys.argv[1]

files = mc_client.query(query=query,with_metadata=True, with_provenance=True)

event_count = 0
fid = 0
filecount = 0

for file in files:
    filecount +=1
    #print ("a file",file)
    metadata = file["metadata"]
    count = metadata["core.event_count"]
    nfid = len(file["parents"])
    event_count += count
    fid += nfid
    print (nfid, count)


print ("this query had ",fid,"parents and ",event_count,"events, spread across ", filecount," pass2 files")

if fid != nfiles:
     print ("ERROR: final number of files %d is not = the input %d"%(fid,nfiles))
else:
     print ("This task is complete",task)