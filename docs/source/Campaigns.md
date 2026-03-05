## Campaigns

You can run campaigns with multiple datasets/configurations using the `campaign` folders

~~~
cd $MERGE_UTILS_DIR
cd campaigns
mkdir <campaign_name>
source setup_campaign.sh <campaign_name>
cd <campaign_name>
~~~

In that directory you need to make a csv file that stores:

TAG,FCL,CONFIG,CAMPAIGN,NAMESPACE,BATCH,DATASET

- TAG is a unique tag for this run 
- FCL is the fcl file you need to run with
- CONFIG is the merge yaml or json override of defaults
- CAMPAIGN is the campaign 
- NAMESPACE is the output namespace ('usertests)
- BATCH is how many files are sent to the merger at once - for production batches of 2000-5000 are good.  For testing set this to maybe 20. 
- DATASET the metacat dataset you want to run over. Generally should be official 

## production scripts

The production scripts are in `src/prod_utils`

- `build_jobs.py <base csv>` this takes the original csv file, figures out how many files you will be running over and produces <jobs csv>

- make_pass1.py <tag> makes a script that submits the pass1 jobs

### utilities

- get_tasks.py utility that scripts use to match tag with tasks

- pass1_check.py

- pass1_summary.py

- workflow_check.py

- make_safe_query.py