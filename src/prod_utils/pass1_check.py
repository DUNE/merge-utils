''' do a check for pass 1'''
import os, sys
from metacat.webapi import MetaCatClient

from get_tasks import get_tasks

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

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
batch = int(tasks[task]['BATCH'])
print ("nfiles",nfiles)

query = "files where merge.tag=%s and dune.output_status=confirmed  and namespace=%s"%(sys.argv[1],tasks[task]["NAMESPACE"])

files = mc_client.query(query=query,with_metadata=True, with_provenance=True)

event_count = 0
fid = 0
filecount = 0
for file in files:
    filecount += 1
    #print ("a file",file)
    metadata = file["metadata"]
    count = metadata["core.event_count"]
    nfid = len(file["parents"])
    event_count += count
    fid += nfid

print ("pass1 this tag had ",fid,"parents and ",event_count,"events, spread across",filecount,"pass1 files")

if fid != nfiles:
     print (fid,nfiles)
     print ("ERROR: final number of files %d is not = the input %d"%(fid,nfiles))
else:
     print ("This pass1 is complete",task)