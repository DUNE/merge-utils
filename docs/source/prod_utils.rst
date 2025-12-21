=================================
Production Utilities (prod_utils)
=================================

Setup 
-----

Production merging is done by setting up a directory under the `campaigns` directory. Each campaign directory contains configuration files and scripts to run the merging.  The code itself is in the `prod_utils` directory.

There is a bash script in `campaigns` which sets up environmentals. 

```
./setup_campaign.sh <campaign_name>
```
They are:

- `CAMPAIGN`` the name of the campaign - which is the name of the directory under `campaigns`

- `CAMPAIGN_DIR=$MERGE_UTILS_DIR/campaigns/$CAMPAIGN``
- `PYTHONPATH=$MERGE_UTILS_DIR/src/prod_utils:$PYTHONPATH`
- `CAMPAIGN_CONFIG=$CAMPAIGN_jobs.csv`

Within a campaign, you can specify multiple merges each of which is specified by line in the `<campaign>.csv` file in the campaign directory.

It specifies

- TAG - a unique ID you give to a merge project

- FCL - the fcl file you run over the merge

- CONFIG - the merge configuration file

- CAMPAIGN - the campaign name = directory name

- NAMESPACE	- the namespace the outputs will go to - `usertests` when testing

- BATCH - the # of input files that go into each output workflow

- DATASET - the input dataset - which will be chopped up into batches for processing. 




