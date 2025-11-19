import os,sys,csv

import csv


tasklist = os.path.join(os.getenv("MERGE_UTILS_DIR"),"config","trg_mc_2025a",'trg_mc_2025a_jobs.csv')

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
if nfiles < 7500:    
    command = f"merge -vv -c trg_mc_2025a/{config} --tag=\"{task}\" query \" files from {tasks[task]['QUERY']}\""
    print(command)
    f.write(command + '\n')
else:
    step = 5000
    skip = 0
    while skip < nfiles:
        command = f"merge -vv -c trg_mc_2025a/{config} --skip {skip} --limit {step} --tag=\"{task}\" query \" files from {tasks[task]['QUERY']}\""
        print (command)
        f.write(command + '\n')
        skip += step
f.close()

