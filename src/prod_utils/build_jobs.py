'''Script to build a job list with file counts and sizes from a base job list CSV file'''

import os,sys,csv

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
with open(joblist,encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)
    if DEBUG: print(reader.fieldnames)
    newrows = []
    for row in reader:
        basedataset = row['DATASET']
        config = row['CONFIG']
        fcl = row['FCL']
        # check the config for fcl and lar, and if it has lar, make sure it also has fcl
        hasfcl = False
        haslar = False
        lines = open(config,'r').readlines()
        for line in lines:
            if "lar " in line: 
                haslar = True
            if fcl in line:
                hasfcl = True
                break
        if haslar and not hasfcl:
            print (f"lar config file {config} does not contain {fcl} - please fix this" )
            sys.exit(1)
        if DEBUG: print(row["TAG"],  row['DATASET'])
        newrow = row.copy()
        query = "files from " + basedataset + " where dune.output_status=confirmed"
        if DEBUG: print(query)
        result, = mc_client.query(query=query,summary="count")
        info = result
        # for item in result:
        #     if DEBUG: print("   ",item)
        #     info = item
        print ("Result for tag",row["TAG"],":",info )
        newrow['NFILES'] = info['count']
        newrow['SIZE_GB'] = round(info['total_size']/1e9,3)

        
        newrows.append(newrow)
        print(newrow)
    
with open(newlist,'w') as csvfile:
    fieldnames = ['TAG','DUNESW','NFILES','SIZE_GB','BATCH',
                  'FCL','CONFIG', 'CAMPAIGN','NAMESPACE', 'DATASET' ] 
    #fieldnames = reader.fieldnames + ['NFILES','SIZE_GB','NAMESPACE','CONFIG']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in newrows:
        writer.writerow(row)