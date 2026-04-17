'''Script to build a job list with file counts and sizes from a base job list CSV file'''

import os,sys,csv,math

from metacat.webapi import MetaCatClient

import csv

DEBUG = True

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])


# if len(sys.argv) < 2:
#     print ("Usage: python build_jobs.py <joblist.csv>")
#     print ("assumes the csv file is in the $CAMPAIGN_DIR")
#     sys.exit(1)
if os.getenv("CAMPAIGN_DIR") is None or os.getenv("CAMPAIGN") is None:
    print ("Please set CAMPAIGN_DIR and CAMPAIGN environment variables")
    sys.exit(1)  

campaignfile  = os.getenv("CAMPAIGN")+".csv"
joblist = os.path.join(os.getenv("CAMPAIGN_DIR"),campaignfile)
print("Using joblist:",joblist)
newlist = joblist.replace('.csv','_jobs.csv')
checklist = joblist.replace('.csv','_checklist.csv')
with open(joblist,encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)
    if DEBUG: print(reader.fieldnames)
    newrows = []
    newflows = []
    for row in reader:
        basedataset = row['DATASET']
        config = row['CONFIG']
        fcl = row['FCL']
        campaign = row['CAMPAIGN']
        row['BATCH']= int(row['BATCH'])

        # check the config for fcl and lar, and if it has lar, make sure it also has fcl
        hasfcl = False
        haslar = False
        hascampaign = False
        lines = open(config,'r').readlines()
        for line in lines:
            if campaign in line:
                hascampaign = True
            if "lar " in line:
                haslar = True
            if fcl in line:
                hasfcl = True

        if haslar and not hasfcl:
            print (f"lar config file {config} does not contain {fcl} - please fix this" )
            sys.exit(1)
        if not hascampaign:
            print (f"config file {config} does not contain dune.campaign:{campaign} - please fix this" )
            sys.exit(1)
        if DEBUG: print(row["TAG"],  row['DATASET'])
        newrow = row.copy()
    
        query = "files from " + basedataset + " where dune.output_status=confirmed"
        if DEBUG: print(query)
        try:
            result, = mc_client.query(query=query,summary="count")
        except:
            result = mc_client.query(query=query,summary="count")
        info = result
        # for item in result:
        #     if DEBUG: print("   ",item)
        #     info = item
        print ("Result for tag",row["TAG"],":",info )
        newrow['NFILES'] = info['count']
        newrow['SIZE_GB'] = round(info['total_size']/1e9,3)

        
        newrows.append(newrow)
        nflows = math.ceil(newrow["NFILES"]/newrow["BATCH"])
        for flow in range(nflows):
            newflow = newrow.copy()
            newflow['SKIP']=flow*int(newrow["BATCH"])
            newflow['WORKFLOW iD']=""
            newflow['PASS']="pass1"
            newflow["TIMESTAMP"]=""
            newflow["# of jobs"]=""
            newflow["status"]=""
            newflow["#event/job"]=""
            newflow["#files"]=""
            newflow["success fraction"]=0.0
            newflow["total # of events"]=0
            newflow["volume (GB)"]=0.0
            newflow["comments"]="?"
            newflows.append(newflow)

        print(newrow)
    
with open(newlist,'w') as csvfile:
    fieldnames = ['TAG','DUNESW','NFILES','SIZE_GB','BATCH',
                  'FCL','CONFIG', 'CAMPAIGN','NAMESPACE', 'DATASET' ] 
    #fieldnames = reader.fieldnames + ['NFILES','SIZE_GB','NAMESPACE','CONFIG']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in newrows:
        writer.writerow(row)



with open(checklist,'w') as csvfile:
    fieldnames = ['TAG','SKIP','TIMESTAMP','PASS','WORKFLOW iD',
            "# of jobs","status","#event/job","#files","success fraction",
            "total # of events","volume (GB)",
            "comments",'DUNESW','NFILES','SIZE_GB','BATCH',
                  'FCL','CONFIG', 'CAMPAIGN','NAMESPACE', 'DATASET' ] 
    #fieldnames = reader.fieldnames + ['NFILES','SIZE_GB','NAMESPACE','CONFIG']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in newflows:
        writer.writerow(row)