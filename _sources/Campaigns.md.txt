# Campaigns

You can set up and run campaigns with multiple datasets/configurations with one setup per campaign. 

Each campaign may have sub-campaigns that run different fcls over different datasets.

There is a shift operation procedure defined after the main setup and explanations. 

There is also an example of this procedure in

`/exp/dune/app/home/duneproshift/merge/hd_atmos_202604`

## Setup

We suggest that for each campaign you make a subdirectory:

for example in production when you first log in. 

Get into an apptainer:
~~~
/cvmfs/oasis.opensciencegrid.org/mis/apptainer/current/bin/apptainer shell --shell=/bin/bash \
-B /cvmfs,/exp,/nashome,/pnfs/dune,/opt,/run/user,/etc/hostname,/etc/krb5.conf --ipc --pid \
/cvmfs/singularity.opensciencegrid.org/fermilab/fnal-dev-sl7:latest
~~~


You need to set the `CAMPAIGN` environmentals to set up a unique campaign directory 

~~~
export $CAMPAIGN=<campaign_name>
mkdir /exp/dune/app/home/duneproshift/merge/$CAMPAIGN
cd /exp/dune/app/home/duneproshift/merge/$CAMPAIGN
git clone https://github.com/dune/merge-utils.git
~~~

Then make a script called `setup.sh`, make certain it has the code version you want and put it in 

`/exp/dune/app/home/duneproshift/merge/$CAMPAIGN`

Text of `setup.sh`

~~~
echo "this is setup.sh for $CAMPAIGN"
export RUCIO_ACCOUNT=justin_readonly # need this to access rucio
export CAMPAIGN=<campaign name>
export DUNE_VERSION=<version>
export DUNE_QUALIFIER=<qualifier>
cd /exp/dune/app/home/duneproshift/merge/$CAMPAIGN/merge-utils
source setup_fnal.sh
cd campaigns
mkdir -p $CAMPAIGN
source setup_campaign.sh $CAMPAIGN
cd $CAMPAIGN
~~~

You need to run this script every time you log in. 

ie, after the apptainer command:

source $HOME/merge/$CAMPAIGN/setup.sh

the full directory path to your $CAMPAIGN configuration will be in `$CAMPAIGN_DIR`

In that directory you need to make a csv file with the same name as the directory. `$CAMPAIGN.csv` that stores tagged rows for each dataset you want to run over.

`TAG,FCL,CONFIG,CAMPAIGN,NAMESPACE,BATCH,DATASET`

- `TAG` is a unique tag for this run of the merger - if you put `TEST` in the tag, jobs will be set up to run interactively. 
- `FCL` is the name of the fcl file you want to use.  A single campaign can have differenc fcls.
- `CONFIG` is the merge yaml or json override of defaults. Should have the same name as the fcl file if using lar. You may have to edit it to make certain that the `cfg` option points to  the correct fcl and the `metadata` field contains the correc `dune.campaign` field.  The `build_jobs` script will complain if these are not set consistently.
- `CAMPAIGN` is the campaign - same as directory and cvs vfile
- `NAMESPACE` is the output namespace (default is 'usertests', for production you need to change it to the right namespace.)
- `BATCH` is how many input files are sent to the merger at once - for large production batches of 2000-5000 are good. 
- `DATASET` the metacat dataset you want to run over. Generally should be official 

## production scripts

The production scripts are in `src/prod_utils`

when you run the `setup_campaign.sh` script (called in the overall setup script) it should be added to your path.

- `build_jobs.py` this takes the original csv file, figures out how many files you will be running over for each sub-campaign and produces `$CAMPAIGN_jobs.csv` and `$CAMPAIGN_checklist.csv`.  It checks that you are set up with the right larsoft version, campaign name and that your config files contain the right fcl file names. 

You generally only have to do this once but if you have to rerun a sub-campaign you may decided to redo it.  

You copy the `$CAMPAIGN_checklist.csv` into a google doc and use it to keep track of your submissions. 

When you are ready to run some jobs:

- `make_pass1.py <tag>` makes a script that submits the pass1 jobs for `<tag>`
  If you run it without a tag, it lists the available tags.


## Shifter instructions


0. Make your campaign directory 

Run the setup procedure described in detail above. 

Once that is done, every time you login, get an apptainer, got to `$HOME/merge/$CAMPAIGN` and run `setup.sh` and you should be ready to go. 

1. Set up your base csv file `$CAMPAIGN.csv`.  Each row represents a sub-campaign which can have different fcl, yaml and datasets but not different code versions.
Each sub-campaign needs a unique tag.  The yaml file needs to contain the correct fcl file. Rows can share yaml files if you are running the same config on different datasets. 

Each campaign directory should containe a unique `$CAMPAIGN.csv` with the same suffix as the directory name. 

Reminder from above - fields are:

`TAG,DUNESW,FCL,CONFIG,CAMPAIGN,NAMESPACE,BATCH,DATASET`

2. use `build_jobs`  to build the `$CAMPAIGN_jobs.csv` and `$CAMPAIGN_checklist.csv`


~~~
python -m build_jobs 
~~~

makes `$CAMPAIGN_jobs.csv` and `$CAMPAIGN_checklist.csv`

It will complain if you have not set up the right DUNESW version or if your yaml files are inconsistent with the fcl and campaign names. 

3. When you are ready to run jobs you can then use this to generate sub-campaigns for each `fcl` file/dataset

~~~
python -m make_pass1 <tag>
~~~

will make a long list of potential merge submissions, each of which has length `--limit` and skips by `--skip`

those merge submission commands are stored in `<TAG.sh>` so you can use them later. 

This is what they look like:

~~~
merge  -l -vv -c /Users/schellma/Dropbox/merge-utils/campaigns/trigprim-2026-03/triggerana_tree_1x2x2_simpleThr_production.yaml --skip=0 --limit=2  --tag="TEST-TRGSIM_CC_v1" dataset 
~~~
produces:
~~~
fardet-hd:fardet-hd__trg_mc_2025a__detector-simulated__v10_06_00d01__detsim_dune10kt_1x2x2_notpcsigproc__prodmarley_nue_flat_cc_dune10kt_1x2x2__out1__v1_official > TEST-TRGSIM_CC_v1_20260321T161849_0.log 2>&1 
~~~

- `-l` means run locally (remove to run as batch)
- `--retry` this checks every single file to see if it is a duplicate.  Not done by default
- `-vv` means run verbose
- `-c` points to the config gile
- `--skip` skips n input files
- `--limit` limits the # of files
- `--tag` should be a unique tag for this workflow - you use this to check for duplicates and make the final merged file dataset
- `dataset` (could also be `query` or `list`) specifies what you will run over

I have set this up to write to log files.  

4. run some of those merge scripts.  You likely have to get a new token

~~~
justin time
justin get-token
~~~

the merge scripts will check your metadata for validity and set up jobscripts to run locally or through justIn.

The last 2-3 lines of the merge scripts have the actual submission commands which look like:

- interactive
    '/exp/dune/data/users/schellma/prod/merge-utils/tmp/TEST_hd_atmos_l000002_20260408T235613/run.sh'

- batch
  `/exp/dune/data/users/schellma/prod/merge-utils/tmp/TEST_hd_atmos_v4_l000500_20260417T174112/submit.sh`

  `/exp/dune/data/users/schellma/prod/merge-utils/tmp/TEST_hd_atmos_v4_s000500_l000500_20260417T174145/submit.sh`

note the timestamp `20260417T174145`, you will need to use it.

You can pull them out by doing 

~~~
grep submit *<timestamp>*.log
~~~

and then issue those commands and record the workflow numbers that come back from each one. 

5. track your jobs.  Each campaign has a spreadsheet `$CAMPAIGN_checklist.csv$ which was generated when you run build_jobs

upload the partially filled spreadsheet into google sheets.  It should have a line for every workflow you submitted.

Fill in 

- 	TAG - the sub-campaign, already filled
- 	SKIP - already filled, index within the group of sub-campaigns - you can get it from the submit command:
`TEST_hd_atmos_v4_`*s000500*`_l000500_20260417T174145/submit.sh`
- 	TIMESTAMP - the timestamp for the sumbit commands - from the submit command:
TEST_hd_atmos_v4_s000500_l000500_*20260417T174145*/submit.sh
- 	PASS - pass1 or 2
- 	WORKFLOW iD - returned when you submit 
- 	# of jobs - look in justin workflow to see # of jobs
- 	status - once jobs runs
- 	#event/job 
- 	#files
- 	success fraction
- 	total # of events
- 	volume (GB)
- 	comments
- 	DUNESW - already filled
- 	NFILES - already filled
- 	SIZE_GB - already filled
- 	BATCH - already filled
- 	FCL - already filled
- 	CONFIG - already filled
- 	CAMPAIGN - already filled
- 	NAMESPACE - already filled
- 	DATASET - already filled

6. What if I made a mistake or things went very badly.

You can track your jobs at [the justin workflow page](https://dunejustin.fnal.gov/dashboard/?method=list-workflows)

You can check pass1 by using:

~~~
python -m pass1_check <tag>
~~~

You should see something like this if you check midway. 

~~~
python -m pass1_check TEST_hd_atmos_v3
nfiles 980
pass1 this tag had  600 parents and  63500 events, spread across 62 pass1 files
600 980
ERROR: final number of files 600 is not = the input 980
~~~

- if a workflow got paused, you can 

~~~
justin restart-workflow workflow-id=<workflow-id>
~~~

- if it still doesn't complete properly, wait a few hours and do a rerun of `make_pass1` for the workflows that did not finish. 

The scripts have a --retry option that will ignore files that are already processed.

- if all else fails, you can increment the tag version and rerun the whole sub-campaign again.  