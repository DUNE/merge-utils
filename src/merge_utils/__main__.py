"""Command line interface for merge_utils."""

import argparse
import collections
import logging
import os
import sys
import json
import copy

from merge_utils import io_utils, config, meta, naming, retriever, replicas, scheduler

logger = logging.getLogger(__name__)

def get_parser() -> argparse.ArgumentParser:
    """Set up the command line argument parser."""
    parser = argparse.ArgumentParser(
        description='Command line interface for merge_utils')
    parser.add_argument('-c', '--config', action='append', metavar='CFG',
                        help='a configuration file')
    parser.add_argument('-t', '--tag', type=str, help='tag to help identify this run')
    parser.add_argument('-r', '--retry', action='store_true', help='retry a failed workflow requires tag')
    parser.add_argument('--comment', type=str, help='a comment describing the workflow')
    parser.add_argument('-v', '--verbose', action='count', default=0,
                        help='print more verbose output (e.g. -vvv for debug output)')
    parser.add_argument('--log', help='specify a custom log file path')
    parser.add_argument('--retry', action='store_true',
                        help='enable checking for already-merged files')

    in_group = parser.add_argument_group('input arguments')
    in_group.add_argument('input_mode', nargs='?', default=None, metavar='MODE',
                          choices=['query', 'dataset', 'dids', 'files', 'resume'],
                          help='input mode (query, dataset, dids, files), or resume a partial job')
    in_group.add_argument('-f', '--file', action='append', default=[],
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
    out_mode = out_group.add_mutually_exclusive_group()
    out_mode.add_argument('--merge', dest='output_mode', action='store_const', const='merge',
                          help='generate a merge job (default)')
    out_mode.add_argument('--validate', dest='output_mode', action='store_const', const='validate',
                          help='only validate metadata instead of merging')
    out_mode.add_argument('--list', dest='output_mode', metavar='OPT',
                          choices=['metadata', 'dids', 'replicas', 'pfns', 'rses'],
                          help='list (metadata, dids, replicas, pfns, rses) instead of merging')
    out_group.add_argument('-l', '--local', action='store_true',
                           help='run merge locally instead of submitting to JustIN')
    out_group.add_argument('-n', '--name', type=str, help='override the base name for output files')
    out_group.add_argument('-s', '--namespace', type=str, metavar='NS',
                           help='override the namespace for output files')
    out_group.add_argument('-m', '--method', type=str, metavar='MTD',
                           help='explicitly specify the merging method')
    return parser

def get_inputs(args: dict) -> None:
    """
    Collect the list of inputs from the command line, from standard input, or from files.
    If multiple sources are provided, this will give an error since it's likely a mistake.
    If a single source is provided, it will override the config.
    Inputs from the config file(s) will be used if no other sources are provided.
    
    :param args: command line arguments
    :return: None (updates config.input.inputs in place)
    """
    # Command line arguments
    cmd_inputs = args.pop("inputs", [])
    io_utils.log_nonzero("Found {n} input{s} from command line", len(cmd_inputs))
    # Inputs piped to standard input
    pipe_inputs = []
    if not sys.stdin.isatty():
        pipe_inputs = [x.strip() for x in sys.stdin.readlines()]
        io_utils.log_nonzero("Found {n} input{s} from standard input", len(pipe_inputs))
    # Inputs from file lists
    file_inputs = []
    filelists = args.pop("file", [])
    for filelist in filelists:
        if not os.path.isfile(filelist):
            logger.critical("Input file list '%s' does not exist.", filelist)
            sys.exit(1)
        with open(filelist, encoding="utf-8") as f:
            entries = f.readlines()
        io_utils.log_nonzero("Found {n} input{s} in file %s" % filelist, len(entries))
        file_inputs.extend([x.strip() for x in entries])
    io_utils.log_nonzero("Found {n} total input{s} from file lists", len(file_inputs))
    # Inputs from the merge config
    cfg_inputs = config.input.inputs
    # Give an error if we have mixed input sources, since this is likely a mistake
    sources = sum(bool(x) for x in [cmd_inputs, pipe_inputs, file_inputs])
    if sources > 1:
        logger.critical("Multiple input sources detected, please only provide one!")
        sys.exit(1)
    # If we have a single source, override the config inputs with that source
    if cmd_inputs:
        io_utils.log_nonzero("Overriding {n} default config input{s} with command line inputs",
                             len(cfg_inputs))
        config.input.inputs = cmd_inputs
    elif pipe_inputs:
        io_utils.log_nonzero("Overriding {n} default config input{s} with piped inputs",
                             len(cfg_inputs))
        config.input.inputs = pipe_inputs
    elif file_inputs:
        io_utils.log_nonzero("Overriding {n} default config input{s} with file list inputs",
                             len(cfg_inputs))
        config.input.inputs = file_inputs
    # If we have no other sources, use the config inputs (if any)
    elif cfg_inputs:
        io_utils.log_nonzero("Using {n} default input{s} from config", len(cfg_inputs))
    else:
        logger.critical("No input provided, exiting.")
        sys.exit(1)


def start_job(args: dict):
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
    get_inputs(args)

    # Collect file search directories
    dirs = config.input.search_dirs
    io_utils.log_nonzero("Found {n} search location{s} from config files", len(dirs))
    cmd_dirs = args.pop("dir", [])
    if cmd_dirs:
        io_utils.log_nonzero("Found {n} search location{s} from command line", len(cmd_dirs))
        dirs |= cmd_dirs
    io_utils.log_list("Found {n} total search location{s}:", dirs, logging.INFO)

    # Dump final configuration
    config.dump()

    # Quit if we have any unprocessed command line arguments
    if len(args) > 0:
        bad_args = ", ".join(args.keys())
        logger.critical("Some command line arguments were not processed: %s", bad_args)
        sys.exit(1)

def resume_job(args: dict):
    """Resume a previously started merge job with the given command line arguments."""
    # Load default configuration
    inputs = args.pop("inputs", [])
    if len(inputs) == 0:
        logger.critical("Please provide a job directory to resume.")
        sys.exit(1)
    if len(inputs) > 1:
        logger.critical("Multiple job directories provided, please only provide one.")
        sys.exit(1)
    job_dir = inputs[0]
    config.load()
    formatter = naming.Formatter()
    formatter.format(config.output.tmp_dir)
    if not os.path.exists(job_dir):
        job_dir = os.path.join(str(config.output.tmp_dir), job_dir)
    if not os.path.exists(job_dir):
        logger.critical("Job directory '%s' does not exist.", job_dir)
        sys.exit(1)
    config.resume(job_dir, args)
    # Quit if we have any unprocessed command line arguments
    if len(args) > 0:
        bad_args = ", ".join(args.keys())
        logger.critical("Invalid command line arguments for resuming a job: %s", bad_args)
        sys.exit(1)
    io_utils.setup_job_dir(job_dir)
    msg = [
        f"Restarting merge job {config.uuid()}",
        f"Dir: {config.job.dir}",
        f"Input mode: {config.input.mode}",
        f"Output mode: {config.output.mode}"
    ]
    io_utils.log_print("\n  ".join(msg))

def print_metadata(metadata, mode):
    """Print info about the metadata for a list of files."""
    metadata.run()
    good_files = metadata.files.good_files
    ngood = len(good_files)
    nerrs = len(metadata.files) - ngood
    # In DID mode, list all valid DIDs and exit
    if mode == 'dids':
        io_utils.log_print(f"Found {ngood} valid input files:")
        for file in good_files:
            print(f"  {file.did}")
        if nerrs:
            logger.error("An additional %d files failed validation!", nerrs)
        return
    # Otherwise, print summary of validation results
    if nerrs:
        logger.error("%d input files passed validation, but %d files failed!", ngood, nerrs)
        return
    io_utils.log_print(f"All {ngood} input files passed validation!", logging.INFO)
    # Check the metadata for the output files
    meta.make_names(good_files)
    if mode == 'validate':
        io_utils.log_print("All input and output metadata passed validation!")
        return
    # In metadata mode, also print the combined output metadata
    merged_metadata = meta.merged_keys(good_files, True)
    io_utils.log_print(f"Combined metadata:\n{json.dumps(merged_metadata, indent=2)}")
    for idx, output in enumerate(config.method.outputs):
        if not output.metadata:
            continue
        overrides = json.dumps({k: v.value for k, v in output.metadata.items()}, indent=2)
        io_utils.log_print(f"Additional metadata overrides for output {idx}:\n{overrides}")

def print_replicas(paths, mode):
    """Print info about the replicas for a list of files."""
    paths.run()
    if mode == 'replicas':
        print("File replicas:")
        for file in paths.files.good_files:
            print(f"{file.did}:")
            for replica in sorted(file.replicas):
                print(f"  {replica}")
    elif mode == 'pfns':
        print("File PFNs:")
        for file in paths.files.good_files:
            print(f"{file.did}:")
            for replica in sorted(file.replicas):
                if replica.status.good:
                    print(f"  {replica.path}")
    elif mode == 'rses':
        rses = {}
        good_files = paths.files.good_files
        for file in good_files:
            for replica in file.replicas:
                rse_name = replica.rse.name
                if rse_name not in rses:
                    rses[rse_name] = collections.defaultdict(int)
                rses[rse_name][replica.status] += 1
        print(f"Found {len(good_files)} valid input files with replicas at {len(rses)} RSEs:")
        for rse_name, statuses in rses.items():
            print(f"{rse_name}:")
            for status, count in sorted(statuses.items(), key=lambda x: x[1]):
                print(f"  {status.name} replicas: {count}")

def main():
    """Run a merge job"""
    parser = get_parser()
    arguments = parser.parse_args()

    # Convert command line arguments to a dict and remove any None values
    args = {k: copy.deepcopy(v) for k, v in vars(arguments).items() if v}
    print ("main arguments are:", args)

    # Set up logging
    io_utils.setup_log(log_file=args.pop("log", None), verbosity=args.pop("verbose", None))

    if arguments.input_mode == 'resume':
        args.pop("input_mode", None)
        resume_job(args)
    else:
        start_job(args)

    # Set up metadata retriever
    metadata = retriever.get()

    # If we're only validating or listing DIDs, we can skip the rest of the setup
    if config.output.mode in ['validate', 'metadata', 'dids']:
        print_metadata(metadata, config.output.mode)
        return

    # Set up physical path finder
    paths = replicas.get(metadata)

    # Process the other list options
    if config.output.mode in ['replicas', 'pfns', 'rses']:
        print_replicas(paths, config.output.mode)
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
