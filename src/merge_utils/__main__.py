"""Command line interface for merge_utils."""

import argparse
import logging

from merge_utils import io_utils, config, metacat_utils, rucio_utils, scheduler, retriever

logger = logging.getLogger(__name__)

def main():
    """Test the command line interface for merge_utils."""

    parser = argparse.ArgumentParser(
        description='Command line interface for merge_utils')
    parser.add_argument('-c', '--config', action='append', help='a configuration file')
    parser.add_argument('-v', '--verbose', action='count', default=0, help='print more verbose output')

    in_group = parser.add_argument_group('input arguments')
    in_group.add_argument('-l', '--local', action='store_true',
                          help='use local files instead of MetaCat')
    in_group.add_argument('-q', '--query', help='a MetaCat query')
    in_group.add_argument('-f', '--filelist', help='a text file with a list of input files')
    in_group.add_argument('files', nargs=argparse.REMAINDER, help='')

    out_group = parser.add_argument_group('output arguments')
    out_group.add_argument('--list', choices=['dids', 'replicas', 'pfns'],
                           help='list output instead of merging')
    

    args = parser.parse_args()
    print ("main arguments are: ",args)

    # Set up logging and configuration
    name = "merge"
    if args.list:
        name = "list "+args.list
    io_utils.setup_log(name)
    config.load(args.config)
    io_utils.set_log_level(args.verbose)

    # Set up the retriever based on the input arguments
    flist = io_utils.get_inputs(args.filelist, args.files)

    if args.filelist:
        ret = retriever.LocalRetriever(filelist=flist)
    elif args.list == 'dids':
        ret = metacat_utils.MetaCatRetriever(query=args.query, filelist=flist)
    else:
        ret = rucio_utils.RucioRetriever(
            metacat_utils.MetaCatRetriever(query=args.query, filelist=flist)
        )

    if args.list:
        ret.run()
        if args.list == 'dids':
            for file in ret.files:
                print(file.did)
        elif args.list == 'replicas':
            if args.local:
                print("Local file paths:")
                for file in ret.files:
                    print(f"  {file.path}")
            else:
                for name, rse in ret.rses.items():
                    print(f"RSE {name}:")
                    for pfn in rse.pfns.values():
                        print(f"  {pfn}")
        elif args.list == 'pfns':
            for chunk in ret.output_chunks():
                print(f"Output file {chunk.name} (site {chunk.site}):")
                for pfn in chunk.values():
                    print(f"  {pfn.path}")
        else:
            raise ValueError(f"Unknown list option: {args.list}")
    else:
        if args.local:
            sched = scheduler.LocalScheduler(ret)
        else:
            sched = scheduler.JustinScheduler(ret)
        sched.run()
