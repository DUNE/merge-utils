import os, sys
from metacat.webapi import MetaCatClient

import csv

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

large = ["CENTRAL_prod","LATERAL_prod"]
tasklist = os.path.join(os.getenv("MERGE_UTILS_DIR"),"config","trg_mc_2025a",'trg_mc_2025a_jobs.csv')
maxjob = 5000
tasks = {}
 
with open(tasklist,encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)
    #print(reader.fieldnames)
    
    for row in reader:
        #print(row["TAG"], row['FCL'], row['QUERY'])
        tasks[row["TAG"]] = row 
    
    # if len(sys.argv)<2:
    #     task="HELP"
    # else:
    #     task = sys.argv[1]
    # if task not in tasks:
        
    #         print(f"Task {task} not found")
    #         print("Available tasks:", ', '.join(tasks.keys()))
    #         sys.exit(1)
f = open("trg_mc_2025a_Pass1Summary.txt","w")
f.write("TASK, NFILES, CHECK, NFILES_OUTPUT, QUERY\n")
for task in tasks.keys():
    if task not in large:
        continue
    nfiles = int(tasks[task]['NFILES'])
    print ("nfiles",nfiles)
    config = tasks[task]['FCL']
    parent_dataset = tasks[task]['DATASET']
    query = "files where merge.tag=%s and core.run_type='fardet-hd' and dune.output_status=confirmed and core.data_tier='root-tuple' and dune.campaign=trg_mc_2025a_tpg and merge.dataset='%s' and merge.cfg='%s'"%(task,parent_dataset,config)
    files = mc_client.query(query=query,with_metadata=True, with_provenance=True)

    event_count = 0
    fid = 0
    filecount = 0
    avesize = 0
    for file in files:
        filecount +=1
        #print ("a file",file)
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
