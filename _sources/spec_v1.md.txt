## Design considerations ##

H. Schellman and E. Muldoon
January 7 2025 - updated January 21, 2025, March 24, 2025
 
Description of issues for merging files
 
These are things we have learned from merging.  We have a prototype merging package which https://github.com/DUNE/data-management-ops/utilities/merging. It has 3 parts, a data file merger, a metadata merged, and a batch submission script. The plan is to create a new github repo for the next iteration of the merging code.
 
The central requirement is that merged data have merged metadata that allows one to understand/reproduce the inputs and the merge step itself. 
 
This document describes

0)        motivation
1)        Input formats  
2)        Input delivery methods
3)        Merging methods  
4)        Requirements for merged metadata  
5)        Where this runs
 
## Motivation

Dune data operates on multiple scales, from 1 MB to 10 PB.  We need all of those data to be backed up to tape and catalogued which works optimally for files of size 1-10 GB.  This requires the ability to merge files and merge their metadata.  This document is about merging procedures for files below the size threshold for tape storage. 

## Inputs 
 
We  know of 4 different input formats:
 
1)        HDF5 
2)        Art-root
3)        Plain root
4)        Generic files (sam4users replacement)
 
Each of these likely requires a different method to read them.  We have successfully merged art-root using art, plain root using hadd and generic files using tar.  HDF5 merge still needs to be developed.
Utilities needed: data merging
scripts that perform the merge of data and generate a checksum for the output. 

1)        Root  → hadd or custom
2)        Art-root → art
3)        Hdf5 → ??? 
4)        Generic files → tar/gzip 
5)        Utilities needed: List generation

### At the list generation stage, we need methods that 

-         Can take a metacat query or list of input files
-         check for duplicates in that list
-         validate the metadata for those input files is ok
-         Check that the metadata of the input files is consistent within requirements (see below)

## Move from Jobsub to Justin for batch running

- the first iteration both ran interactively from a gpvm and via a jobsub script
- The suggestion is that future runs be done via justin and/or interactively

### Justin questions 2025-03-24

- needs to be able to take a list of file locations (if the inputs are not declated to metacat yet) or
- needs to be able to run over a dataset at a particular site that has the data available and enough capacity to run IO intensive activities
- need to be able to specify an output RSE for merge jobs - how do we do this?

### how do we get Justin to run over chunks of files in a list. 

- possibly - submit a job for each chunk.
- possibly - submit N parallel jobs which are smart enough to use jobid to choose a chunk. What is jobid. 

### Methods needed:

- justin jobscript that can take a list of files and loop over them
- justin jobscript that can take a dataset and loop over them
  
## Delivery methods  
 
1)        A rucio or metacat dataset with files declared to metacat
2)        A disk directory with data files and their metadata which have not been declared
3)        A disk directory with generic files that likely will need metadata creation.
 
As merging is I/O intensive, the inputs need to be quite local to the process being run.  Right now we achieve this by either 
 
a)         having rucio move the files to dcache at the merging site, or 
b)         having the original batch job write back to the merging site, or
c)         using xroot to move files from remote RSE’s to a local cache area at the merging site
 
Currently done at FNAL (and FZU) this needs to be generalized to run at other sites. 

We need methods:

-        Can take a list of did’s  and generate a list of valid locations given a merging site.
-        Can check that those valid locations are not on tape and otherwise request that they be recovered from tape
-        Can initiate a rucio move to the right location if needed. 
-        Can xrdcp to a local cache without using rucio if files are not cataloged yet. 
-        Need to be able to specify merging sites 
-        Need to be able to specify sites that are known to be unavailable. 

 
## Merging methods
 
The prototype system supports:
 
1)        Merging small art files into larger ones without modification
2)        Merging small root files into larger ones without modification
3)        Creating a tar file from a list of files
4)        Creating CAF’s from a dataset or list of art files. 
 
*We still need an HDF5 merging script*
 
The merges are done in “chunks” which are groups of files within a larger ordered list that produce an output file of appropriate size.  Jobs are submitted to batch as “chunks” while interactive processes can iterate over multiple chunks.  If anything fails for a chunk (data access, merge itself, metadata concatenation) the chunk is abandoned and will need to be redone. 
Metadata requirements
 
The program `mergeMetaCat.py` takes a list of metadata files that were used to make a merged data file, checks for consistency and then merges the metadata. 
 
The output merged metadata is placed in `<merged data file>.json` in the same directory.

### Note on Event counting and tracking

Some processes drop events and do not fill the event list.

The event list is not useful for multi-run files because it doesn’t link event `#` and run `#`.  But it is useful for raw data - do we need to do something about this - like mod the event list for derived data to be run.event? 
Do we want to read the root file to count the records in it so at least nevents is meaningful.

We need a method:  
·      root script that can count the events in an output file for inclusion in the metadata. 

## Consistency checks
Before a merge is done, one needs to check that the data being merged is consistent. This is currently done after the data merge as a final check but should probably be done before data are merged. 
 

`["
“core.run_type", “core.file_type", "namespace", "core.file_format", "core.data_tier", "core.data_stream", 'core.application.name', 'dune.campaign', 'DUNE.requestid', 'dune.requestid']`
 
Currently we do not require that `dune.config_file` or `core.application.version` be exactly consistent to allow patches, likely strict consistency for these fiels should be an option. 

Methods needed:  
·      script that can take a list of files or metadata files and check for consistency.  This exists as part of mergeMetaCar.py but should be pulled out for use before merging starts.  

## Metadata merge:

What gets combined:
 
The  metadata merging program takes a list of input metadata and data files.
 
*       Merges the runs and subruns lists
*       Calculates the number of events in the file (see above for alternative)
*       Calculates the first/last event range – should it merge the actual event lists for single run files?
*       Creates parentage from the input files.  If the inputs have not been declared yet, their parents are the parents for the merge file. 
*       Calculates a checksum and adds it and the merged data filesize to the metadata.
 
### Metadata fields added:
 
The output file `name` needs to be generated either at the data merging or metadata merging phase. This is currently generated from the generic fields from the metadata of the first file in the list during the initial data merge and passed into the merger. 
 
*         `dune.dataset_name` is an output dataset name that needs to be generated to give rucio/metacat a place to put the * ou tput files. This is currently generated from the merged metadata if not supplied on the command line
 
*       `dune.output_status` is set to “merged” - should be updated to “confirmed” once actually stored by declaD
 
*       `dune.merging_stage` is set to “final” if done, otherwise some intermediate status.  If not “final”, another merging stage will be run. 
 
*       `dune.merging_range` has the ranges for the files merged in the input list. 
 
Methods needed:  

*       Output filename generator

*       Output dataset name generator

*       Metadata checking
 
The output metadata is then checked with `TypeChecker.py` – it currently flags but does not patch incorrect or missing input metadata
 
### enhancements to `TypeChecker.py`

Possibly add a database of valid values for core metadata (data_tier, run_type …) and refuse to pass data that uses novel values until cleared by an admin) and abbreviations for important values to make auto-generated dataset and filenames shorter. 

Example: full-reconstructed → FReco, vd-protodune → PDVD … store them in the valid fields config file. 
 
### Sam4users
 
If we use this system to replace `sam4users`, what metadata should be stored for the tar file generated? Should it contain a directory listing for the tar file so that it is searchable? What files should be included? Which not? 
 
 
## Where this runs
 
The current setup is designed to run on Fermilab systems in either batch or interactive mode.   Rucio inputs work in batch, otherwise one needs to run interactively.  
 
Iterative merging needs to be done interactively as the intermediate files are not declared to metacat.

Methods needed:  
·      Neet do be able to specify merging sites more broadly. 
 
## Suggestions for future versions
 
Consistency and quality checks for metadata likely should be moved earlier in the sequence. In principle these can be done before the task is even submitted to batch.
 
The current version throws an error and refuses to merge if any file is not available in an input chunk (normally 1-4 GB of data).  Do we want to add the option of culling bad files from a chunk before sending it to the merge process? 
 
We don’t have a final check yet that can pick up files missed on the first round, except by checking by hand and resubmitting merges chunk by chunk. 
 
All of the scripts take similar arguments and pass them to each other.  Making a single arguments system would make the code a lot easier to maintain. 

Method needs: 
*         Need a method that can check a list of input files to check which have been merged and which have not.  Can use metadata - for example `dune.output_status=merged` if the parent files have been merged. 


## Design ideas:

How do we specify parameters for a merge?

·      Command line
·      YAML/json

What parameters have we used?  * indicates things that are used to make a query.  

~~~
                        fcl file to use with lar when making tuples, required with --uselar
                        
 -h, --help            show this help message and exit
 
  --detector DETECTOR   detector id [hd-protodune]
  
  --input_dataset INPUT_DATASET
                        metacat dataset as input
                        
  --chunk CHUNK         number of files/merge this step, should be < 100
  
  --nfiles NFILES       number of files to merge total
  
  --skip SKIP           number of files to skip before doing nfiles
  
  --run RUN             run number
  
  --destination DESTINATION
                        destination directory
                        
  --input_data_tier INPUT_DATA_TIER
                        input data tier [root-tuple-virtual]
                        
  --output_data_tier OUTPUT_DATA_TIER
                        output data tier
                        
  --output_file_format OUTPUT_FILE_FORMAT
                        output file_format [None]
                        
  --output_namespace OUTPUT_NAMESPACE
                        output namespace [None]
                        
  --file_type FILE_TYPE
                        input detector or mc, default=detector
                        
  --application APPLICATION
                        merge application name [inherits]
                        
  --input_version INPUT_VERSION
                        software version of files to merge (required)
                        
  --merge_version MERGE_VERSION
                        software version for merged file [inherits]
                        
  --debug               make very verbose
  
  --maketarball         make a tarball of source
  
  --usetarball USETARBALL
                        full path for existing tarball
                        
  --uselar              use lar instead of hadd or tar
  
  --lar_config LAR_CONFIG
                        fcl file to use with lar when making tuples, required with --uselar
                        
  --merge_stage MERGE_STAGE
                        stage of merging, final for last step
                        
  --project_tag PROJECT_TAG
                        tag to describe the project you are doing
                        
  --direct_parentage    parents are the files you are merging, not their parents
  
  --inherit_config      inherit config file - use for hadd stype merges
  
  --output_datasetName OUTPUT_DATASETNAME
                        optional name of output dataset this will go into
                        
  --campaign CAMPAIGN   campaign for the merge, default is campaign of the parents

 --merge_stage MERGE_STAGE
                        stage of merging, final for last step
~~~

Things to add 

~~~
–query METACAT query , overides many of the input metadata fields. 

–output_file_size_goal  - specify this to calculate chunk. 
–find_nevents - read the file to find the nevents
~~~
When making file/dataset names - abbreviations for common fields. (example full-reconstructed→ freco)




