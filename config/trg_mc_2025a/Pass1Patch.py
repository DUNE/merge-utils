import os, sys, json
from metacat.webapi import MetaCatClient

import csv

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

tasklist = os.path.join(os.getenv("MERGE_UTILS_DIR"),"config","trg_mc_2025a",'trg_mc_2025a_jobs.csv')
maxjob = 2000
tasks = {}

with open(tasklist,encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)
    #print(reader.fieldnames)
    
    for row in reader:
        #print(row["TAG"], row['FCL'], row['QUERY'])
        tasks[row["TAG"]] = row 
    
if len(sys.argv)<2:
    task="HELP"
else:
    task = sys.argv[1]
if task not in tasks:
    
        print(f"Task {task} not found")
        print("Available tasks:", ', '.join(tasks.keys()))
        sys.exit(1)
nfiles = int(tasks[task]['NFILES'])

print ("nfiles",nfiles)

query = "files where merge.tag=%s  and core.data_tier='root-tuple-virtual'"%sys.argv[1]

#query += " and name =prodbackground_radiological_decay0_dune10kt_1x2x6_centralAPA__trg_mc_2025a_tpg__tpg_dune10kt_1x2x6__reco__v10_12_01d01__triggerana_dune10kt_1x2x6__v10_12_01d01__ntuple__CENTRAL_prod_s005000_l005000_20251126T011152_c39.root"

files = mc_client.query(query=query,with_metadata=True, with_provenance=True)

count = 0
filecount = 0

for file in files:
    filecount +=1
    #print ("a file",file)
    metadata = file["metadata"]
    # count = metadata["core.event_count"]
    # nfid = len(file["parents"])
    # event_count += count
    # fid += nfid
    # print (nfid, count)
    if metadata["core.data_tier"] == "root-tuple-virtual":
        did = file["namespace"] + ":" + file["name"]
        fix = {"core.data_tier":"root-tuple"}
        print ("Fixing data tier for ", did)
        f = open(file['name']+".json.backup",'w')
        json.dump(file,f)
        f.close()
        mc_client.update_file(did=did,replace=False,metadata=fix)
        count += 1

print ("fixed data tier for ",count," files out of ",filecount," pass1 files for this tag")

