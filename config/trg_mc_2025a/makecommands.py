import os,sys,csv

import csv


tasklist = 'trg_mc_2025a.csv'

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
command = f"merge -v -c trg_mc_2025a/{config} --tag=\"{task}\" query \" files from {tasks[task]['QUERY']}\""
print(command)
