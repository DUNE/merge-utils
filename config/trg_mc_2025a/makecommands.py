import os,sys,csv

import csv
from datetime import datetime, timezone

timestamp: str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

retry = " "

if len(sys.argv)>2:
    if sys.argv[2].lower() == "retry":
        retry = "--retry"

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
    if task != "TEST":
        print(f"Task {task} not found")
        print("Available tasks:", ', '.join(tasks.keys()))
        sys.exit(1)
    else:
        # For testing purposes
        maxjob = 2
        basetask="FLAT-CC_prod"
        tasks[task] = tasks[basetask]
        tasks[task]["NFILES"]="5"
        tasks[task]["TAG"]=task

config = tasks[task]["FCL"].replace('.fcl','.json')
if "CENTRAL" in task or "LATERAL" in task:
    config = "triggerana_dune10kt_1x2x6_large.json"
    maxjob = 5000
nfiles = int(tasks[task]['NFILES'])
f = open(f'{task}.sh','w')
print ("nfiles",nfiles)
skip = 0
if nfiles < maxjob:    
    command = f"merge {retry} -vv -c trg_mc_2025a/{config} --tag=\"{task}\" dataset {tasks[task]['DATASET']} > {task}_{timestamp}_{skip}.log 2>&1 "
    print(command)
    f.write(command + '\n')
else:
    step = maxjob
    
    while skip < nfiles:
        command = f"merge  {retry} -vv -c trg_mc_2025a/{config} --skip={skip} --limit={step}  --tag=\"{task}\" dataset {tasks[task]['DATASET']} > {task}_{timestamp}_{skip}.log 2>&1 "
        print(command)
        f.write(command + '\n')
        skip += step
f.close()

