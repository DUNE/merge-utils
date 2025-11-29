import os, sys

import csv

def get_tasks(listpath):
    tasklist = os.path.join(listpath)
    tasks = {}
    with open(tasklist,encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile) 
        for row in reader:
            tasks[row["TAG"]] = row        
    return tasks