"""Command line interface for merge_utils."""

import argparse
import logging
import sys

from merge_utils import io_utils, config, scheduler, local

logger = logging.getLogger(__name__)

def main():
    """Test the command line interface for merge_utils."""

    parser = argparse.ArgumentParser(
        description='Command line interface for merge_utils')
    parser.add_argument('-c', '--config', action='append', metavar='CFG',
                        help='a configuration file')
    parser.add_argument('-v', '--verbose', action='count', default=0,
                        help='print more verbose output (e.g. -vvv for debug output)')
    parser.add_argument('--log', help='specify a custom log file path')

    in_group = parser.add_argument_group('input arguments')
    in_group.add_argument('input_mode', nargs='?', default=None, metavar='MODE',
                          choices=['query', 'dids', 'files'],
                          help='input mode (query, dids, files, dir)')
    in_group.add_argument('-f', '--file', action='append',
                          help='a text file with a list of input files')
    in_group.add_argument('-d', '--dir', action='append',
                          help='a directory to add to search locations')
    #in_group.add_argument('inputs', nargs=argparse.REMAINDER, help='remaining command line inputs')
    in_group.add_argument('inputs', nargs='*', help='remaining command line inputs')
    parser.add_argument("--tag",type=str, help="tag for output ",default="test")
    parser.add_argument("--limit",type=int, help="number of files to pull from query ",default=1000)
    parser.add_argument("--skip",type=int, help="number of files to skip before doing nfiles",default=0)
    out_group = parser.add_argument_group('output arguments')
    out_group.add_argument('--validate', action='store_true',
                           help='only validate metadata instead of merging')
    out_group.add_argument('--list', choices=['dids', 'replicas', 'pfns'], metavar='OPT',
                           help='list (dids, replicas, pfns) instead of merging')
    out_group.add_argument('-l', '--local', action='store_true',
                           help='run merge locally instead of submitting to JustIN')

    args = parser.parse_args()
    print ("main arguments are: ",args)

    # Set up logging and configuration
    name = "merge"
    if args.validate:
        name = "validate"
        args.list = 'dids'
    elif args.list:
        name = "list "+args.list
    io_utils.setup_log(name, log_file=args.log, verbosity=args.verbose)
    config.load(args.config)

    if args.local:
        config.output['mode'] = 'local'

    if args.input_mode:
        config.inputs['mode'] = args.input_mode
    input_mode = config.inputs['mode']
    logger.info("Input mode: %s", input_mode)

    # Collect inputs
    inputs = config.inputs['inputs'] or []
    io_utils.log_nonzero("Found {n} input{s} from config files", len(inputs))
    if io_utils.log_nonzero("Found {n} input{s} from command line", len(args.inputs)):
        inputs.extend(args.inputs)
    inputs.extend(io_utils.get_inputs(args.file))
    if len(inputs) == 0:
        logger.critical("No input provided, exiting.")
        sys.exit(1)
    io_utils.log_list("Found {n} total input{s}:", inputs, logging.INFO)

    # Collect file search directories
    dirs = config.inputs['search_dirs'] or []
    io_utils.log_nonzero("Found {n} search location{s} from config files", len(dirs))
    if args.dir:
        io_utils.log_nonzero("Found {n} search location{s} from command line", len(args.dir))
        dirs.extend(args.dir)
    io_utils.log_list("Found {n} total search location{s}:", dirs, logging.INFO)

    # Determine input mode and retrieve metadata
    paths = None
    metadata = None
    if args.tag:
        config.metadata['overrides']["merge.tag"] = args.tag
    if input_mode == 'files':
        paths = local.get_local_files(inputs, dirs)
        metadata = paths.meta
    elif input_mode == 'query':
        from merge_utils.metacat_utils import MetaCatRetriever #pylint: disable=import-outside-toplevel
        if len(inputs) != 1:
            logger.critical("Query mode currently only supports a single MetaCat query. Put quotes areount it? ")
            sys.exit(1)
        
        config.inputs['skip'] = args.skip
        config.inputs['limit'] = args.limit
        if "with" not in query:
            query = "%s with dune.output_status=confirmed ordered skip %d limit %d"%(inputs[0],args.skip,args.limit)
        else:
            query = "%s and dune.output_status=confirmed ordered skip %d limit %d"%(inputs[0],args.skip,args.limit)
        metadata = MetaCatRetriever(query=query)
        print ("query was:", query)
        config.metadata['overrides']["merge.skip"] = args.skip
        config.metadata['overrides']["merge.limit"] = args.limit
        
    elif input_mode == 'dids':
        from merge_utils.metacat_utils import MetaCatRetriever #pylint: disable=import-outside-toplevel
        metadata = MetaCatRetriever(dids=inputs)
    else:
        logger.critical("Unknown input mode: %s", input_mode)
        sys.exit(1)

    # If we're only listing DIDs, we can skip the rest of the setup
    if args.validate:
        metadata.run()
        io_utils.log_print(f"{len(metadata.files)} inputs passed validation")
        nerrs = metadata.files.count_errors()
        if nerrs:
            io_utils.log_print(f"{nerrs} inputs failed validation")
        return

    if args.list == 'dids':
        metadata.run()
        io_utils.log_print(f"Found {len(metadata.files)} valid inputs:")
        for file in metadata.files:
            print(file.did)
        return

    # Set up a retriever for physical file locations if needed
    if not paths:
        if dirs:
            logger.info("Searching for local data files in provided directories")
            paths = local.LocalPathFinder(metadata, dirs=dirs)
        else:
            logger.info("No local search directories provided, querying Rucio to find data files")
            from merge_utils.rucio_utils import RucioFinder #pylint: disable=import-outside-toplevel
            paths = RucioFinder(metadata)

    # Process the other list options
    if args.list:
        paths.run()
        if args.list == 'replicas':
            if input_mode in ['files']:
                print("Local file paths:")
                for file in paths.files:
                    print(f"  {file.path}")
            else:
                for name, rse in paths.rses.items():
                    print(f"RSE {name}:")
                    for pfn in rse.pfns.values():
                        print(f"  {pfn}")
        elif args.list == 'pfns':
            for chunk in paths.output_chunks():
                print(f"Output file {chunk.name} (site {chunk.site}):")
                for pfn in chunk.values():
                    print(f"  {pfn.path}")
        else:
            raise ValueError(f"Unknown list option: {args.list}")
        return

    # Process merging
    if args.local:
        sched = scheduler.LocalScheduler(paths)
    else:
        sched = scheduler.JustinScheduler(paths)
    sched.run()
