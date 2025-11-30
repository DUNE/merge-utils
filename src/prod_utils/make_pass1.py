import os,sys,csv

import csv
from metacat.webapi import MetaCatClient

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

from datetime import datetime, timezone

timestamp: str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

from get_tasks import get_tasks

if len(os.getenv("CAMPAIGN")) < 1:
    print ("Please set CAMPAIGN environment variable")
    sys.exit(1)

retry = " "
maxjob = 2000
local = " "
if len(sys.argv)>2:
    if sys.argv[2].lower() == "retry":
        retry = "--retry"


tasks = get_tasks(os.path.join(os.getenv("CAMPAIGN_DIR"),os.getenv("CAMPAIGN")+"_jobs.csv"))
    
if len(sys.argv)<2:
    print ("Need to specify a task")
    sys.exit(1)
else:
    task = sys.argv[1]
if task not in tasks:
        print(f"Task {task} not found")
        print("Available tasks:", ', '.join(tasks.keys()))
        sys.exit(1)
else:
    if "TEST" in task:
        # For testing purposes
        maxjob = 20
        #local = "--local"
        tasks[task]["NFILES"]="50"

config = tasks[task]['CONFIG']
campaign = tasks[task]["CAMPAIGN"]
campaign_dir = os.path.join(os.getenv("CAMPAIGN_DIR"))
nfiles = int(tasks[task]['NFILES'])
f = open(f'{task}.sh','w')
print ("nfiles",nfiles)
skip = 0
query = f"files where merge.tag={task} and dune.output_status=confirmed and namespace=%s"%(tasks[task]["NAMESPACE"]) 
check = mc_client.query(query=query,summary="count")
count = check["count"]

if count > 0:
    retry = "--retry"
if nfiles < maxjob:  
    if count > 0:
        retry = "--retry"
    command = f"merge {retry} {local} -vv -c {config} --tag=\"{task}\" dataset {tasks[task]['DATASET']} > {task}_{timestamp}_{skip}.log 2>&1 "
    print(command)
    f.write(command + '\n')
else:
    step = maxjob
    
    while skip < nfiles:
        saveretry = retry
        query = f"files where merge.tag={task} and dune.output_status=confirmed and namespace=%s and merge.skip={skip} and merge.limit={step}"%(tasks[task]["NAMESPACE"]) 
        check = mc_client.query(query=query,summary="count")
        count = check["count"]
        if count > 0:
            retry = "--retry"
        command = f"merge  {retry} {local} -vv -c {campaign_dir}/{config} --skip={skip} --limit={step}  --tag=\"{task}\" dataset {tasks[task]['DATASET']} > {task}_{timestamp}_{skip}.log 2>&1 "
        print(command)
        f.write(command + '\n')
        skip += step
        retry = saveretry
f.close()

