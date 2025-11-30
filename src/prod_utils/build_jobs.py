'''Script to build a job list with file counts and sizes from a base job list CSV file'''

import os,sys,csv

from metacat.webapi import MetaCatClient

import csv

DEBUG = True

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])


if len(sys.argv) < 2:
    print ("Usage: python build_jobs.py <joblist.csv>")
    print ("assumes the csv file is in the $CAMPAIGN_DIR")
    sys.exit(1)
if os.getenv("CAMPAIGN_DIR") is None:
    print ("Please set CAMPAIGN_DIR environment variable")
    sys.exit(1)  

joblist = os.path.join(os.getenv("CAMPAIGN_DIR"),sys.argv[1])
print("Using joblist:",joblist)
newlist = joblist.replace('.csv','_jobs.csv')
with open(joblist,encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)
    if DEBUG: print(reader.fieldnames)
    newrows = []
    for row in reader:
        basedataset = row['DATASET']
        if DEBUG: print(row["TAG"], row['FCL'], row['DATASET'])
        newrow = row.copy()
        query = "files from " + basedataset + " where dune.output_status=confirmed"
        if DEBUG: print(query)
        result = mc_client.query(query=query,summary="count")
        info = result
        # for item in result:
        #     if DEBUG: print("   ",item)
        #     info = item
        print ("Result for tag",row["TAG"],":",info )
        newrow['NFILES'] = info['count']
        newrow['SIZE_GB'] = round(info['total_size']/1e9,3)
        newrow["NAMESPACE"] = "usertests"
        newrow["CONFIG"] = row["FCL"].replace('.fcl','.json')
       
        newrows.append(newrow)
        print(newrow)
    
with open(newlist,'w') as csvfile:
    fieldnames = ['TAG', 'FCL','NFILES','SIZE_GB','FCL','CONFIG', 'CAMPAIGN','NAMESPACE', 'DATASET' ] 
    #fieldnames = reader.fieldnames + ['NFILES','SIZE_GB','NAMESPACE','CONFIG']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in newrows:
        writer.writerow(row)