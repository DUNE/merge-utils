import os,sys,csv

'''
Script to create pass1 merge commands for a given task'''

import csv
from metacat.webapi import MetaCatClient

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

from datetime import datetime, timezone

if __name__ == '__main__':
    
    timestamp: str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    from get_tasks import get_tasks

    if len(os.getenv("CAMPAIGN")) < 1:
        print ("Please set CAMPAIGN environment variable using the setup_campaign.sh script")
        sys.exit(1)

    
    retry = " "
    local = " "
    if len(sys.argv)>2:
        if sys.argv[2].lower() == "retry":
            retry = "--retry"


    tasks = get_tasks(os.path.join(os.getenv("CAMPAIGN_DIR"),os.getenv("CAMPAIGN")+"_jobs.csv"))
        
    if len(sys.argv)<2:
        print ("Need to specify a task")
        print("Available tasks:", ', '.join(tasks.keys()))
        sys.exit(1)
    else:
        task = sys.argv[1]


    if task not in tasks:
            print(f"Task {task} not found")
            print("Available tasks:", ', '.join(tasks.keys()))
            sys.exit(1)
    else:
        if "TEST" in task:
            local="-l"

    batch = int(tasks[task].get("BATCH",2000))
    print ("batch",batch)
    version = os.getenv("DUNE_VERSION", "unknown")
    dunesw = tasks[task].get("DUNESW", "unknown")
    total = tasks[task].get("TOTAL", 100000)
    if version == "unknown":
        print ("Quitting: DUNE_VERSION environment variable not set, you need to set DUNE_VERSION and then set up larsoft and merging again.")
        sys.exit(1)
    if version != dunesw:
        print (f"Quitting: DUNE_VERSION environment variable ({version}) does not match the required DUNESW version for the task ({dunesw})")
        print (f"Unfortunately this means you have to start a new session, set DUNE_VERSION and then rerun setup_fnal.sh")
        sys.exit(1)

    config = tasks[task]['CONFIG']
    campaign = tasks[task]["CAMPAIGN"]
    campaign_dir = os.path.join(os.getenv("CAMPAIGN_DIR"))
    nfiles = int(tasks[task]['NFILES'])
    f = open(f'{task}.sh','w')
    print ("nfiles",nfiles)
    skip = 0
    query = f"files where merge.tag={task} and dune.output_status=confirmed and namespace=%s"%(tasks[task]["NAMESPACE"]) 
    print ("query",query)
    try:
        check, = mc_client.query(query=query,summary="count")
    except:
        check = mc_client.query(query=query,summary="count")
    dupcount = check["count"]

    print ("dupcount",dupcount)
    if dupcount > 0:
        retry = "--retry"
        print (f"Found {dupcount} existing files for task {task}, will use {retry} option")

    if nfiles < batch:  
        print ("less than batch")
        if dupcount > 0:
            retry = "--retry"
        command = f"merge {retry} {local} -vv -c {config} --tag=\"{task}\" dataset {tasks[task]['DATASET']} > {task}_{timestamp}_{skip}.log 2>&1 "
        print(command)
        f.write(command + '\n')
    else:
        step = batch
        
        while skip < nfiles:
            print ("next",skip,step)
            saveretry = retry
            query = f"files where merge.tag={task} and dune.output_status=confirmed and namespace=%s and merge.skip={skip} and merge.limit={step}"%(tasks[task]["NAMESPACE"]) 
            try:
                check, = mc_client.query(query=query,summary="count")
            except:
                check = mc_client.query(query=query,summary="count")
            dupcount = check["count"]
            if dupcount > 0:
                retry = "--retry"
            command = f"merge  {retry} {local} -vv -c {campaign_dir}/{config} --skip={skip} --limit={step}  --tag=\"{task}\" dataset {tasks[task]['DATASET']} > {task}_{timestamp}_{skip}.log 2>&1 "
            print(command)
            f.write(command + '\n')
            skip += step
            retry = saveretry
    f.close()

