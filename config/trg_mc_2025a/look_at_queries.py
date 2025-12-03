import os,sys,csv

from metacat.webapi import MetaCatClient

import csv

mc_client = MetaCatClient(os.environ["METACAT_SERVER_URL"])

joblist = 'trg_mc_2025a.csv'
newlist = 'trg_mc_2025a_jobs.csv'
with open(joblist,encoding='utf-8-sig') as csvfile:
    reader = csv.DictReader(csvfile)
    print(reader.fieldnames)
    newrows = []
    
    for row in reader:
        print(row["TAG"], row['FCL'], row['DATASET'])
        newrow = row.copy()
        dataset = row['DATASET']
        query = f"files from {dataset} where dune.output_status=confirmed"
        print (query)
        info = mc_client.query(query=query,summary="count")
        print (info)
        newrow['NFILES'] = info['count']
        newrow['SIZE_GB'] = round(info['total_size']/1e9,2)
        newrows.append(newrow)
        print(newrow)
    
with open(newlist,'w',newline='') as csvfile:
    fieldnames = reader.fieldnames + ['NFILES','SIZE_GB']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in newrows:
        writer.writerow(row)
