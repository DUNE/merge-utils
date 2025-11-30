'''Script to check the status of workflows for a given task'''

import os, sys
from metacat.webapi import MetaCatClient

import csv

from get_tasks import get_tasks

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

tasklist = os.path.join(os.getenv("MERGE_UTILS_DIR"),os.getenv("CAMPAIGN_DIR"),os.getenv("CAMPAIGN")+"_jobs.csv")


chunk = 100

tasks = get_tasks(tasklist)

batch = tasks[task].get("BATCH",2000)
    
if len(sys.argv)<4:
    task="HELP"
    print ("Usage: python -m WorkFlowCheck.py <TASK> <start_workflow_id> <end_workflow_id>")
else:
    task = sys.argv[1]
if task not in tasks:   
        print(f"Task {task} not found")
        print("Available tasks:", ', '.join(tasks.keys()))
        sys.exit(1)

nfiles = int(tasks[task]['NFILES'])

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

    if fid != batch/chunk:
        #print (fid,batch)
        print ("ERROR: final number of input files %d is not = the input %d for workflow %d skip %d"%(fid,batch,workflow,skip))
    else:
        print ("This pass1 %s workflow %d with skip %d is complete %d %d"%(task,workflow,skip,fid,filecount))

