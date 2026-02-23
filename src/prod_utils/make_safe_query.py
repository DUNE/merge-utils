''' Script to create list of unprocessed files for a given tag'''

# argument is tag

import os, sys
from metacat.webapi import MetaCatClient

from get_tasks import get_tasks
mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])


def make_safe_query(tasks,tag):
    campaign = tasks[tag]['CAMPAIGN']
    namespace = tasks[tag]['NAMESPACE']
    input_dataset = tasks[tag]['DATASET']
    fcl = tasks[tag]['FCL']
    
    children = f"files where merge.tag={tag} and dune.output_status=confirmed and dune.campaign={campaign} and namespace={namespace} and dune.config_file={fcl} ordered"
    parents = f"parents ({children})"
    newquery = f"files from {input_dataset} - ({parents})"
    print (newquery)
    return newquery
     


if __name__ == "__main__":
    tasklist = os.path.join(os.getenv("CAMPAIGN_DIR"),os.getenv("CAMPAIGN")+"_jobs.csv")

    tasks= get_tasks(tasklist)

    
    if len(sys.argv)<2:
        task="HELP"
    else:
        task = sys.argv[1]
    if task not in tasks:   
            print(f"Task {task} not found")
            print("Available tasks:", ', '.join(tasks.keys()))
            sys.exit(1)

    nfiles = int(tasks[task]['NFILES'])

    dataset = tasks[task]['DATASET']    
    initial = mc_client.query(query=f"files from {dataset}", summary="count")
                               

    safe = make_safe_query(tasks,task)

    final = mc_client.query(query = safe, summary="count")

    print (f"Initial files in {dataset}: {initial}['count']")
    print (f"Final files after safe query: {final}['count']")
