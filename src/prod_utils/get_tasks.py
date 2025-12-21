'''Script to get task definitions from a CSV file'''

import os, sys

import csv

def get_tasks(tasklist):
    if not os.path.exists(tasklist):
        print(f"Task list file {tasklist} not found")
        sys.exit(1)
    tasks = {}
    with open(tasklist,encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile) 
        for row in reader:
            tasks[row["TAG"]] = row        
    return tasks