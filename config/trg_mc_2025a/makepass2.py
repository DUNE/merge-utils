import os,sys,csv,math


from datetime import datetime, timezone

from metacat.webapi import MetaCatClient

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

timestamp: str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

tag = sys.argv[1]

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
    if task != "TEST":
        print(f"Task {task} not found")
        print("Available tasks:", ', '.join(tasks.keys()))
        sys.exit(1)
    else:
        # For testing purposes
        maxjob = 2
        basetask="FLAT-CC"
        tasks[task] = tasks[basetask]
        tasks[task]["NFILES"]="5"
        tasks[task]["TAG"]=task

nfiles = int(tasks[task]["NFILES"].strip())
nchunks = math.ceil(nfiles/100)
query = "files where merge.tag=%s and dune.output_status=confirmed"%tag
result  = mc_client.query(query=query,summary="count")
outfiles = result["count"]
                           
if nchunks != outfiles:
    print ("FAILURE: Expected %d chunks and got %d from pass1"%(nchunks,outfiles))
    sys.exit(1)
else:
    print ("Success: Expected %d chunks and got %d from pass1"%(nchunks,outfiles))

                    
 
command = "merge -c trg_mc_2025a/hadd_prod.json --tag=%s-pass2 query \"%s\""%(tag,query)
print ("if ok \n",command)