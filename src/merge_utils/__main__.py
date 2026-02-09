"""Command line interface for merge_utils."""

import argparse
import logging
import os
import sys
import json

from merge_utils import io_utils, config, scheduler, local, meta, naming
from merge_utils.retriever import QueryRetriever, DidRetriever

logger = logging.getLogger(__name__)

def get_parser() -> argparse.ArgumentParser:
    """Set up the command line argument parser."""
    parser = argparse.ArgumentParser(
        description='Command line interface for merge_utils')
    parser.add_argument('-c', '--config', action='append', metavar='CFG',
                        help='a configuration file')
    parser.add_argument('-t', '--tag', type=str, help='tag to help identify this run')
    parser.add_argument('--comment', type=str, help='a comment describing the workflow')
    parser.add_argument('-v', '--verbose', action='count', default=0,
                        help='print more verbose output (e.g. -vvv for debug output)')
    parser.add_argument('--log', help='specify a custom log file path')

    in_group = parser.add_argument_group('input arguments')
    in_group.add_argument('input_mode', nargs='?', default=None, metavar='MODE',
                          choices=['query', 'dataset', 'dids', 'files', 'resume'],
                          help='input mode (query, dataset, dids, files), or resume a partial job')
    in_group.add_argument('-f', '--file', action='append',
                          help='a text file with a list of input files')
    in_group.add_argument('-d', '--dir', action='append',
                          help='a directory to add to search locations')
    in_group.add_argument('--skip', type=int,
                          help='skip a number of files before processing the remainder')
    in_group.add_argument('--limit', type=int,
                          help='maximum number of files to process (after skip)')
    #in_group.add_argument('inputs', nargs=argparse.REMAINDER, help='remaining command line inputs')
    in_group.add_argument('inputs', nargs='*', help='remaining command line inputs')

    out_group = parser.add_argument_group('output arguments')
    out_group.add_argument('--merge', action='store_true',
                           help='generate a merge job (default)')
    out_group.add_argument('--validate', action='store_true',
                           help='only validate metadata instead of merging')
    out_group.add_argument('--list', choices=['dids', 'replicas', 'pfns'], metavar='OPT',
                           help='list (dids, replicas, pfns) instead of merging')
    out_group.add_argument('-l', '--local', action='store_true',
                           help='run merge locally instead of submitting to JustIN')
    out_group.add_argument('-n', '--name', type=str, help='override the base name for output files')
    out_group.add_argument('-s', '--namespace', type=str, metavar='NS',
                           help='override the namespace for output files')
    out_group.add_argument('-m', '--method', type=str, metavar='MTD',
                           help='explicitly specify the merging method')
    return parser

def get_metadata_retriever():
    """Create and return a metadata retriever based on input mode."""
    # Determine input mode and retrieve metadata
    inputs = config.input.inputs
    if config.input.mode == 'files':
        paths = local.get_local_files(inputs, config.input.search_dirs)
        return paths.meta, paths
    if config.input.mode == 'dids':
        return DidRetriever(dids=inputs), None
    if config.input.mode == 'query':
        if len(inputs) != 1:
            logger.critical("Query mode currently only supports a single MetaCat query.")
            sys.exit(1)
        query = str(inputs[0])
        if 'ordered' in query:
            logger.info("The 'ordered' keyword is automatically appeneded to queries.")
        if 'skip' in query or 'limit' in query:
            logger.warning("Consider using command line options for 'skip' and 'limit'.")
        query += " ordered"
        return QueryRetriever(query=query), None
    if config.input.mode == 'dataset':
        if len(inputs) != 1:
            logger.critical("Dataset mode currently only supports a single dataset name.")
            sys.exit(1)
        query = f"files from {inputs[0]} where dune.output_status=confirmed ordered"
        return QueryRetriever(query=query), None
    logger.critical("Unknown input mode: %s", config.input.mode)
    sys.exit(1)

def start_job(args):
    """Start a new merge job with the given command line arguments."""
    # Load configuration
    config.load(args)
    formatter = naming.Formatter()
    formatter.format(config.output.tmp_dir)
    job_uuid = config.uuid()
    job_dir = os.path.join(str(config.output.tmp_dir), job_uuid)
    config.job.dir = job_dir
    io_utils.setup_job_dir(job_dir)
    msg = [
        f"Starting merge job {job_uuid}",
        f"Dir: {config.job.dir}",
        f"Input mode: {config.input.mode}",
        f"Output mode: {config.output.mode}"
    ]
    io_utils.log_print("\n  ".join(msg))

    # Collect inputs
    inputs = config.input.inputs
    io_utils.log_nonzero("Found {n} input{s} from config files", len(inputs))
    if io_utils.log_nonzero("Found {n} input{s} from command line", len(args.inputs)):
        inputs.extend(args.inputs)
    inputs.extend(io_utils.get_inputs(args.file))
    if len(inputs) == 0:
        logger.critical("No input provided, exiting.")
        sys.exit(1)
    io_utils.log_list("Found {n} total input{s}:", inputs, logging.INFO)

    # Collect file search directories
    dirs = config.input.search_dirs
    io_utils.log_nonzero("Found {n} search location{s} from config files", len(dirs))
    if args.dir:
        io_utils.log_nonzero("Found {n} search location{s} from command line", len(args.dir))
        dirs.extend(args.dir)
    io_utils.log_list("Found {n} total search location{s}:", dirs, logging.INFO)
    config.input.search_dirs = dirs

    # Dump final configuration
    config.dump()

def resume_job(args):
    """Resume a previously started merge job with the given command line arguments."""
    # Load default configuration
    if len(args.inputs) == 0:
        logger.critical("Please provide a job directory to resume.")
        sys.exit(1)
    if len(args.inputs) > 1:
        logger.critical("Multiple job directories provided, please only provide one.")
        sys.exit(1)
    job_dir = args.inputs[0]
    config.load()
    formatter = naming.Formatter()
    formatter.format(config.output.tmp_dir)
    if not os.path.exists(job_dir):
        job_dir = os.path.join(str(config.output.tmp_dir), job_dir)
    if not os.path.exists(job_dir):
        logger.critical("Job directory '%s' does not exist.", job_dir)
        sys.exit(1)
    config.resume(job_dir, args)
    io_utils.setup_job_dir(job_dir)
    msg = [
        f"Restarting merge job {config.uuid()}",
        f"Dir: {config.job.dir}",
        f"Input mode: {config.input.mode}",
        f"Output mode: {config.output.mode}"
    ]
    io_utils.log_print("\n  ".join(msg))

def main():
    """Run a merge job"""
    parser = get_parser()
    args = parser.parse_args()
    print ("main arguments are: ",args)

    # Set up logging
    io_utils.setup_log(log_file=args.log, verbosity=args.verbose)

    if args.input_mode == 'resume':
        resume_job(args)
    else:
        start_job(args)

    # Set up metadata retriever
    metadata, paths = get_metadata_retriever()

    # If we're only listing DIDs, we can skip the rest of the setup
    if config.output.mode == 'validate':
        metadata.run()
        good_files = metadata.files.good_files
        ngood = len(good_files)
        nerrs = len(metadata.files.files) - ngood
        io_utils.log_print(f"{ngood} inputs passed validation")
        if nerrs:
            io_utils.log_print(f"{nerrs} inputs failed validation")
        else:
            meta.make_names(good_files)
            merged_metadata = meta.merged_keys(good_files, True)
            io_utils.log_print(f"Combined metadata:\n{json.dumps(merged_metadata, indent=2)}")
        return

    if config.output.mode == 'list_dids':
        metadata.run()
        good_files = metadata.files.good_files
        ngood = len(good_files)
        io_utils.log_print(f"Found {ngood} valid files:")
        for file in good_files:
            print(f"  {file.did}")
        nerrs = len(metadata.files.files) - ngood
        if nerrs:
            io_utils.log_print(f"An additional {nerrs} files failed validation!")
        return

    # Set up a retriever for physical file locations if needed
    if not paths:
        if config.input.search_dirs:
            logger.info("Searching for local data files in provided directories")
            paths = local.LocalPathFinder(metadata, dirs=config.input.search_dirs)
        else:
            logger.info("No local search directories provided, querying Rucio to find data files")
            from merge_utils.rucio_utils import RucioFinder #pylint: disable=import-outside-toplevel
            paths = RucioFinder(metadata)

    # Process the other list options
    if config.output.mode in ['list_replicas', 'list_pfns']:
        paths.run()
        if config.output.mode == 'list_replicas':
            if config.input.mode in ['files']:
                print("Local file paths:")
                for file in paths.files:
                    print(f"  {file.path}")
            else:
                for name, rse in paths.rses.items():
                    print(f"RSE {name}:")
                    for pfn in rse.pfns.values():
                        print(f"  {pfn}")
        elif config.output.mode == 'list_pfns':
            for file in paths.files:
                best_pfn = sorted(file.paths.values(), key=lambda p: p[1])[0]
                print(f"  {best_pfn[0]}")
        return

    # Process merging
    if config.output.mode == 'merge':
        if config.output.local:
            sched = scheduler.LocalScheduler(paths)
        else:
            sched = scheduler.JustinScheduler(paths)
        sched.run()
        return

    logger.critical("Unknown output mode: %s", config.output.mode)
    sys.exit(1)
