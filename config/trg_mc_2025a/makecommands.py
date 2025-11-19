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
    if task != "TEST":
        print(f"Task {task} not found")
        print("Available tasks:", ', '.join(tasks.keys()))
        sys.exit(1)
    else:
        # For testing purposes
        maxjob = 2
        tasks[task] = {
            "FCL":"triggerana_dune10kt_1x2x2.fcl",
            "NFILES":"5",
            "QUERY":"fardet-hd:fardet-hd__trg_mc_2025a_tpg__trigger-primitives__v10_12_01d01__tpg_dune10kt_1x2x2__prodmarley_nue_flat_es_dune10kt_1x2x2__out1__v1_official"
        }

config = tasks[task]["FCL"].replace('.fcl','.json')
nfiles = int(tasks[task]['NFILES'])
f = open(f'{task}.sh','w')
print ("nfiles",nfiles)
skip = 0
if nfiles < maxjob:    
    command = f"merge -vv -c trg_mc_2025a/{config} --tag=\"{task}\" query \" files from {tasks[task]['QUERY']}\" > {task}_{timestamp}_{skip}.log 2>&1 "
    print(command)
    f.write(command + '\n')
else:
    step = maxjob
    
    while skip < nfiles:
        command = f"merge -vv -c trg_mc_2025a/{config} --skip={skip} --limit={step}  --tag=\"{task}\" query \" files from {tasks[task]['QUERY']}\" > {task}_{timestamp}_{skip}.log 2>&1 "
        print(command)
        f.write(command + '\n')
        skip += step
f.close()

