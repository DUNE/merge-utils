## Campaigns

You can set up and run campaigns with multiple datasets/configurations using the `campaign` folders

~~~
cd $MERGE_UTILS_DIR
cd campaigns
mkdir <campaign_name>
source setup_campaign.sh <campaign_name>
cd <campaign_name>
~~~

the directory name will be stored in `$CAMPAIGN`

In that directory you need to make a csv file `$CAMPAIGN.csv` that stores:

`TAG,CONFIG,CAMPAIGN,NAMESPACE,BATCH,DATASET`

- `TAG` is a unique tag for this run - if you put `TEST` in the tag, jobs will run interactively. 
- `CONFIG` is the merge yaml or json override of defaults. Normally has the same name as the fcl file. 
- `CAMPAIGN` is the campaign 
- `NAMESPACE` is the output namespace ('usertests')
- `BATCH` is how many files are sent to the merger at once - for production batches of 2000-5000 are good. 
- `DATASET` the metacat dataset you want to run over. Generally should be official 

## production scripts

The production scripts are in `src/prod_utils`

- `build_jobs.py <base csv>` this takes the original csv file, figures out how many files you will be running over and produces `<jobs csv>`

- make_pass1.py `<tag>` makes a script that submits the pass1 jobs for `<tag>`

### utilities

- `get_tasks.py` internal utility that scripts use to match tag with tasks

- `pass1_check.py`

- `pass1_summary.py`

- `workflow_check.py`

- `make_safe_query.py`

## sequence actions

0. Make your campaign directory

internally it does adds the production utils to your path

~~~
export PYTHONPATH=$MERGE_UTILS_DIR/src/prod_utils:$PYTHONPATH
~~~

1. Set up your base csv file <$CAMPAIGN.csv> based on examples.  Each row should reference a different yaml file which contains the correct fcl file. 

2. use it to build the <job csv> by doing metacat queries.

Each campaign directory should have a unique csv with the same suffix.

~~~
python -m build_jobs 
~~~

makes `$CAMPAIGN_jobs.csv`

3. you can then use this to generate sub-campaigns for each `fcl` file

~~~
python -m make_pass1 <tag>
~~~

will make a long list of 

