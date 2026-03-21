merge
.....

.. code-block::
	
	python merge -h


	usage: merge [-h] [-c CFG] [-t TAG] [-r] [--comment COMMENT] [-v] [--log LOG] [-f FILE] [-d DIR] [--skip SKIP]
				[--limit LIMIT] [--validate] [--list OPT] [-n NAME] [-m MTD] [-l]
				[MODE] [inputs ...]

	Command line interface for merge_utils

	options:
	-h, --help         show this help message and exit
	-c, --config CFG   a configuration file
	-t, --tag TAG      tag to help identify this run
	-r, --retry        retry a failed workflow requires tag
	--comment COMMENT  a comment describing the workflow
	-v, --verbose      print more verbose output (e.g. -vvv for debug output)
	--log LOG          specify a custom log file path

	input arguments:
	MODE               input mode (query, dids, files, dir)
	-f, --file FILE    a text file with a list of input files
	-d, --dir DIR      a directory to add to search locations
	--skip SKIP        skip a number of files before processing the remainder
	--limit LIMIT      maximum number of files to process (after skip)
	inputs             remaining command line inputs

	output arguments:
	--validate         only validate metadata instead of merging
	--list OPT         list (dids, replicas, pfns) instead of merging
	-n, --name NAME    specify a name for the output files
	-m, --method MTD   explicitly specify the merge method to use
	-l, --local        run merge locally instead of submitting to JustIN



.. automodule:: merge_utils.__main__
	:members: