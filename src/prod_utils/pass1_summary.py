''' make a summary of pass1 for a tag '''

import os, sys
from metacat.webapi import MetaCatClient

import csv

from get_tasks import get_tasks

data_tier = 'root-tuple'

if len(sys.argv)<1:
    print ("Usage: python pass1_summary.py [data_tier]")
    sys.exit(1)


large = ["CENTRAL_prod","LATERAL_prod"]
tasklist = os.path.join(os.getenv("CAMPAIGN_DIR"),os.getenv("CAMPAIGN")+"_jobs.csv")
 

f = open(tasklist.replace("jobs.csv","Pass1Summary.csv"),"w")
tasks = get_tasks(tasklist)
print (tasks.keys())
f.write("TASK, NFILES, CHECK, NFILES_OUTPUT, QUERY\n")

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

for task in tasks.keys():

    nfiles = int(tasks[task]['NFILES'])
    print ("nfiles",nfiles)
    config = tasks[task]['FCL']
    parent_dataset = tasks[task]['DATASET']
    namespace = tasks[task]['NAMESPACE']
    campaign = os.getenv("CAMPAIGN")
    query = f"files where merge.tag={task} and namespace={namespace} and dune.output_status=confirmed and dune.campaign={campaign} and merge.dataset='{parent_dataset}' and merge.cfg='{config}'"
    print (query)
    files = mc_client.query(query=query,with_metadata=True, with_provenance=True)

    event_count = 0
    fid = 0
    filecount = 0
    avesize = 0
    for file in files:
        filecount +=1
        metadata = file["metadata"]
        count = metadata["core.event_count"]
        size = float(file['size']/1000000000)
        avesize += size
        nfid = len(file["parents"])
        event_count += count
        fid += nfid
        #print (nfid, count)

    avesize = float(avesize/filecount) if filecount>0 else 0
    print ("Average file size for pass1 files: ",avesize," GB")
    print ("this query had ",fid,"parents and ",event_count,"events, spread across ", filecount," pass1 files")

    if fid != nfiles:
        print ("ERROR: final number of files %d from %s is not = the input %d"%(fid,task,nfiles))
    else:
        print ("This task is complete",task)
    f.write("%s, %d, %d, %d, \"%s\" \n"%(task, nfiles, fid, filecount, query))
f.close()
