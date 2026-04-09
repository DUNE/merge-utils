# Campaigns

You can set up and run campaigns with multiple datasets/configurations using the `campaign` folders

## Setup

You need to set the `DUNE_VERSION` environmental to the code version you want to run.

Then get the merge-utils and set it up.

~~~
export `DUNE_VERSION`
git clone https://github.com/merge-utils
source setup_fnal.sh
~~~

Then you can set up a campaign

~~~
cd $MERGE_UTILS_DIR
cd campaigns
mkdir <campaign_name>
source setup_campaign.sh <campaign_name>
cd <campaign_name>
~~~

the directory name will be stored in `$CAMPAIGN` and the full directory path will be in `$CAMPAIGN_DIR`

In that directory you need to make a csv file with the same name as the directory. `$CAMPAIGN.csv` that stores tagged rows for each dataset you want to run over.

`TAG,FCL,CONFIG,CAMPAIGN,NAMESPACE,BATCH,DATASET`

- `TAG` is a unique tag for this run - if you put `TEST` in the tag, jobs will run interactively. 
- `FCL` is the name of the fcl file you want to use.  A single campaign can have differenc fcls.
- `CONFIG` is the merge yaml or json override of defaults. Should have the same name as the fcl file if using lar.
- `CAMPAIGN` is the campaign - same as directory and cvs vfile
- `NAMESPACE` is the output namespace (default is 'usertests')
- `BATCH` is how many input files are sent to the merger at once - for large production batches of 2000-5000 are good. 
- `DATASET` the metacat dataset you want to run over. Generally should be official 

## production scripts

The production scripts are in `src/prod_utils`

when you run the `setup_campaign.sh` script it should be added to your path.

- `build_jobs.py` this takes the original csv file, figures out how many files you will be running over and produces `<jobs csv>`

- `make_pass1.py <tag>` makes a script that submits the pass1 jobs for `<tag>`
  If you run it without a tag, it lists the available tags.

### utilities

- `get_tasks.py` internal utility that scripts use to match tag with tasks

- `pass1_check.py`

- `pass1_summary.py`

- `workflow_check.py`

- `make_safe_query.py`

## More detailed sequence of actions

0. Make your campaign directory

internally it does adds the production utils to your path

~~~
export PYTHONPATH=$MERGE_UTILS_DIR/src/prod_utils:$PYTHONPATH
~~~

1. Set up your base csv file `$CAMPAIGN.csv` based on examples.  Each row should reference a yaml file which contains the correct fcl file. Rows can share yaml files if you are running the same config on different datasets. 

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

will make a long list of potential merge submissions, each of which has length `--limit` and skips by `--skip`

those merge submission commands are stored in `<TAG.sh>` so you can use them later. 

This is what they look like:

~~~
merge  -l -vv -c /Users/schellma/Dropbox/merge-utils/campaigns/trigprim-2026-03/triggerana_tree_1x2x2_simpleThr_production.yaml --skip=0 --limit=2  --tag="TEST-TRGSIM_CC_v1" dataset fardet-hd:fardet-hd__trg_mc_2025a__detector-simulated__v10_06_00d01__detsim_dune10kt_1x2x2_notpcsigproc__prodmarley_nue_flat_cc_dune10kt_1x2x2__out1__v1_official > TEST-TRGSIM_CC_v1_20260321T161849_0.log 2>&1 
~~~

- `-l` means run locally (remove to run as batch)
- `--retry` this checks every single file to see if it is a duplicate.  Not done by default
- `-vv` means run verbose
- `-c` points to the config gile
- `--skip` skips n input files
- `--limit` limits the # of files
- `--tag` should be a unique tag for this workflow - you use this to check for duplicates and make the final merged file dataset
- `dataset` (could also be `query` or `list`) specifies what you will run over

Have set this up to write to log files.  

4. run some of those merge scripts.  

the merge scripts will check your metadata for validity and set up jobscripts to run locally or through justIn.

The last 2-3 lines of the merge scripts have the actual submission commands which look like:

- interactive
    '/exp/dune/data/users/schellma/prod/merge-utils/tmp/TEST_hd_atmos_l000002_20260408T235613/run.sh'

- batch
    '/exp/dune/data/users/schellma/prod/merge-utils/tmp/TEST_hd_atmos_20260409T015128/submit.sh'




