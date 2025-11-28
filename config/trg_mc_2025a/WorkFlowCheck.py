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
#f = open(f'{task}.sh','w')
print ("nfiles",nfiles)

for workflow in range(int(sys.argv[2]), int(sys.argv[3])+1):
    query = "files where merge.tag=%s and dune.output_status=confirmed and dune.workflow['workflow_id']=%d"%(sys.argv[1],workflow)

    files = mc_client.query(query=query,with_metadata=True, with_provenance=True)

    event_count = 0
    fid = 0
    filecount = 0
    skip = 0
    for file in files:
        filecount += 1
        #print ("a file",file)
        metadata = file["metadata"]
        count = metadata["core.event_count"]
        nfid = len(file["parents"])
        skip = metadata["merge.skip"]
        event_count += count
        fid += nfid

    #print ("pass1 this workflow", workflow, " had ",fid,"parents and ",event_count,"events, spread across",filecount,"pass1 files and has skip=",skip)

    if fid != maxjob:
        #print (fid,maxjob)
        print ("ERROR: final number of input files %d is not = the input %d for workflow %d skip %d"%(fid,maxjob,workflow,skip))
    else:
        print ("This pass1 %s workflow %d with skip %d is complete %d %d"%(task,workflow,skip,fid,filecount))

