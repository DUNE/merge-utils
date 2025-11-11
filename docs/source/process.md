## process for making trigger tuples

### setup

~~~
cd /exp/dune/data/users/$USER
git clone https://github.com/dune/merge-utils.git
cd merge-utils
git pull
git checkout skip
source setup.sh 
~~~
{: ..language-bash}

## Get your sample  

~~~
export QUERY="files from fardet-hd:fardet-hd__trg_mc_2025a__detector-simulated__v10_06_00d01__detsim_dune10kt_1x2x2_notpcsigproc__prodmarley_nue_flat_es_dune10kt_1x2x2__out1__v1_official"  

metacat query -s $QUERY
~~~
{: ..language-bash}

~~~
Files:        5549
Total size:   16795125427256 (16.795 TB)
~~~
{: ..output}

## do the first merge

~~~
merge  -v -c examples/hist_creation.json --skip 0 --limit 20000 --tag full query "$QUERY" >& big.log 
tail -n 10 big.log
~~~
{: ..language-bash}

~~~
Uploading configuration files to cvmfs...
Merging 5549 input files into 56 merged files:
  US_FNAL-FermiGrid: 	5549 -> 56
Files will be merged using lar
Execute the merge by running:
  /exp/dune/data/users/schellma/merge-utils/tmp/20251110T215154/submit.sh
~~~
{: ..output}

run the file it tells you to run at the end -> 56 justin jobs

You can look at the job at:

https://dunejustin.fnal.gov/dashboard/?method=show-workflow&workflow_id=9905

wait for the jobs to finish

## check the outputs using metacat

check the outputs  - claims they are all there. 

~~~
metacat  query  -s "files where merge.tag=full"
~~~
{: ..language-bash}

~~~
Files:        56
Total size:   3909248032 (3.909 GB){}
~~~
{: ..output}

## now merge them to make a single file

~~~
merge -l -c examples/hadd.json -v --tag=fullstage2 query "files where merge.tag=full"
~~~
{: ..language-bash}

this interactively `(-l)` makes a file in 
`/pnfs/dune/scratch/users/${USER}/merge_test/Full/stage2 `

which you can check by doing

~~~
grep -c fid /pnfs/dune/scratch/users/schellma/merge_test/Full/stage2/20251111T000638/*.json
~~~
{: ..language-bash}

~~~
5549
~~~
{: ..output}

So it thinks it has processed all the files.  How can this fail? The metadata is produced before the job is run and in principle there could be a failure in the hadd step that is not caught. 

We can look in metadata but metacat doesn't provide an event count in the summary (it could with some work) 


`grep event_count /pnfs/dune/scratch/users/schellma/merge_test/Full/stage2/20251111T000638/*.json`

~~~
"core.event_count": 277450
~~~
{: ..language-bash}

## try reading the output

`root://fndca1.fnal.gov:1094//pnfs/fnal.gov/usr/dune/scratch/users/schellma/merge_test/Full/stage2/20251111T000638/prodmarley_nue_flat_es_dune10kt_1x2x2.fcl_trg_mc_2025a_detsim_dune10kt_1x2x2_notpcsigproc.fcl_detsim_v10_06_00d01_triggerana_dune10kt_1x2x2.fcl_s0_l1000_fullstage2_hists_merged_20251111T000639.root`