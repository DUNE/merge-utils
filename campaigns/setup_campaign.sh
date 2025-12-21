#!/bin/bash 

# script to setup environment variables for a given campaign

export CAMPAIGN=$1

if [ -z "$CAMPAIGN" ]; then
    echo "Usage: source setup_campaign.sh <campaign_name>"
    return 1
fi

export CAMPAIGN_DIR=$MERGE_UTILS_DIR/campaigns/$CAMPAIGN
export PYTHONPATH=$MERGE_UTILS_DIR/src/prod_utils:$PYTHONPATH
export CAMPAIGN_CONFIG=$CAMPAIGN_jobs.csv
export PRODSRC=${MERGE_UTILS_DIR}/src/prod_utils

