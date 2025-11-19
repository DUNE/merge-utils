import os,sys,csv

import csv
from datetime import datetime, timezone

timestamp: str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

tasklist = os.path.join(os.getenv("MERGE_UTILS_DIR"),"config","trg_mc_2025a",'trg_mc_2025a_jobs.csv')
maxjob = 2000
tasks = {}
with open(tasklist,encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)
    #print(reader.fieldnames)
    
    for row in reader:
        #print(row["TAG"], row['FCL'], row['QUERY'])
        tasks[row["TAG"]] = row 
    
task = sys.argv[1]
if task not in tasks:
    print(f"Task {task} not found")
    print("Available tasks:", ', '.join(tasks.keys()))
    sys.exit(1)

config = tasks[task]["FCL"].replace('.fcl','.json')
nfiles = int(tasks[task]['NFILES'])
f = open(f'{task}.sh','w')
print ("nfiles",nfiles)
if nfiles < maxjob:    
    command = f"merge -vv -c trg_mc_2025a/{config} --tag=\"{task}\" query \" files from {tasks[task]['QUERY']}\" > {task}_{timestamp}_{skip}.log 2>&1 "
    print(command)
    f.write(command + '\n')
else:
    step = maxjob
    skip = 0
    while skip < nfiles:
        command = f"merge -vv -c trg_mc_2025a/{config} --skip={skip} --limit={step}  --tag=\"{task}\" query \" files from {tasks[task]['QUERY']}\" > {task}_{timestamp}_{skip}.log 2>&1 "
        print(command)
        f.write(command + '\n')
        skip += step
f.close()

