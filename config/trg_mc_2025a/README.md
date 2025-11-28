## how to run this.

Overview 


- `trg_mc_2025a_jobs.csv` contains a list of datasets with tags assigned
- make certain the json config files look good. For example, set the namespace correctly (when testing it is usertests)

### logging in for production

~~~
ssh -Y duneproshift@dunegpvm05.fnal.gov
./apptainer.sh
source setup_merge.sh
~~~
this puts you in the 
`/exp/dune/data/users/duneproshift/merge` subdirectory and does some setup.  


### pass1
- use `makecommand.py` and a `<tag>` to make a `<tag>.sh` file 

~~~
python makecommand.py <tag>
~~~

the tag has to be one of the ones in the `csv` file. 
if a tag has already been used and you are reruning you need to edit the file to add a new tag. Outputs are merged later based on that tag so leftovers are bad. 
- run the `<tag>.sh` file interactively to set up `pass1`
- `cat` the log files from that run and grep for `pass1` to get a list of justin submissions. 
- source those scripts to submit to justin
- log the workflow #'s in the google sheet. 
- wait for justin workflows to complete
- check that all of the workflows complete, you should get `ceiling(N/100)` total output files. 
- do:


~~~
metacat query -s "files where merge.tag=<tag> and dune.output_status=confirmed"
~~~

and 

~~~
python Pass1Check.py <tag>
~~~
to check that all files actually were returned.

If they have not been returned first restart any paused workflows to see if they will complete

#### first restart workflows that paused 

run the workflow checker to identify the workflows that failed.
It will give you the `skip` index for the bad ones. 

~~~
WorkFlowCheck.py <tag> <minid> <maxid>
~~~

~~~
justin workflow-restart --workflow-id=<workflow>
~~~

#### then rerun the `skips` that are still missing files 

This resubmits with the `--retry` flag set.  This does a file by file check on the inputs for children - it takes some time so generally only do it for failed workflows. 

~~~
python makecommands.py <tag> retry
~~~

comment out any completed workflows in the `<tag>.sh` file and resubmit the rest.

### pass2

- make a query based on the `<tag>`, check that it has the right number `~N/100` of files in it and then set up a `pass2` merge. 

~~~
python makepass2.py < tag>
~~~

and then submit the justin job that comes out at the end. 

internaly this is doing this:

~~~
export QUERY="files where merge.tag=<tag> and dune.output_status=confirmed"
metacat query -s $QUERY
merge -v -c trg_mc_2025a/hadd.json --tag=<tag>_pass2 query "$QUERY"
~~~


- Once that job completes

~~~
python Pass2Check.py <tag>
~~~

This should check that the # of input files listed in the spreadsheet.

The sum of `core.event_count` across output files should also be consistent with `N input x events/file`.

You can check the # of events that are actually in a root file with the new script `CountEvents.py`

log checks on number of files and the event counts in the spreadsheet

- inform the production team of completion and discuss making a dataset with them. 

