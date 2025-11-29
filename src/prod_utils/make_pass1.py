import os,sys,csv

import csv
from datetime import datetime, timezone

timestamp: str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

from get_tasks import get_tasks

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
    if task != "test":
        local = ""
        print(f"Task {task} not found")
        print("Available tasks:", ', '.join(tasks.keys()))
        sys.exit(1)
    else:
        # For testing purposes
        maxjob = 2
        local = "--local"
        basetask="TEST"
        tasks[task] = tasks[basetask]
        tasks[task]["NFILES"]="5"
        tasks[task]["TAG"]=task

config = tasks[task]['CONFIG']
campaign = tasks[task]["CAMPAIGN"]
campaign_dir = os.path.join(os.getenv("CAMPAIGN_DIR"))
nfiles = int(tasks[task]['NFILES'])
f = open(f'{task}.sh','w')
print ("nfiles",nfiles)
skip = 0
query = f"files where merge.tag={task} and dune.output_status=confirmed and namespace=%s"%(tasks[task]["NAMESPACE"]) 
check = mc_client.query(query=query,summary="count")
count = check.next()["count"]
if count > 0:
    retry = "--retry"
if nfiles < maxjob:  
    
    command = f"merge {retry} {local} -vv -c {config} --tag=\"{task}\" dataset {tasks[task]['DATASET']} > {task}_{timestamp}_{skip}.log 2>&1 "
    print(command)
    f.write(command + '\n')
else:
    step = maxjob
    
    while skip < nfiles:

        command = f"merge  {retry} {local} -vv -c {campaign_dir}/{config} --skip={skip} --limit={step}  --tag=\"{task}\" dataset {tasks[task]['DATASET']} > {task}_{timestamp}_{skip}.log 2>&1 "
        print(command)
        f.write(command + '\n')
        skip += step
f.close()

