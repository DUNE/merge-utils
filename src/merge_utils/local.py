"""Class variants for managing local file retrieval and merging operations."""

import logging
import os
#import sys
import collections

from typing import AsyncGenerator, Generator

from merge_utils import io_utils, config
from merge_utils.retriever import MetaRetriever, PathFinder
from merge_utils.merge_set import MergeChunk

logger = logging.getLogger(__name__)

def search(file: str, dirs: list) -> str:
    """
    Search for a file in a list of directories.
    
    :param file: Name of the file to search for
    :param dirs: List of directories to search in
    :return: Full path to the file if found, otherwise None
    """
    for directory in dirs:
        path = os.path.join(directory, file)
        if os.path.exists(path):
            return path
    return None

def get_local_files(files: list, dirs: list) -> PathFinder:
    """
    Search local filesystem for pairs of data and metadata files.
    If both data and metadata files are found, return a LocalPathFinder using a LocalMetaRetriever.
    If only metadata files are found, return a RucioPathFinder using a LocalMetaRetriever.
    If only data files are found, return a LocalPathFinder using a MetaCatRetriever.
    
    :param files: List of input data or metadata file paths
    :param dirs: List of directories to search for corresponding data or metadata files
    :return: Appropriate PathFinder object
    """
    inputs = collections.defaultdict(lambda: [None, None])

    # Sort input files into data and metadata
    for file in files:
        name = os.path.basename(file)
        if os.path.splitext(name)[1] == '.json':
            name = os.path.splitext(name)[0]
            inputs[name][1] = file
        else:
            inputs[name][0] = file

    # Search for missing data and/or metadata files
    for name, paths in inputs.items():
        if not paths[0]:
            paths[0] = search(name, [os.path.dirname(paths[1])] + dirs)
        elif not paths[1]:
            paths[1] = search(name + '.json', [os.path.dirname(paths[0])] + dirs)

    has_data = any(paths[0] for paths in inputs.values())
    has_meta = any(paths[1] for paths in inputs.values())

    if has_meta:
        logger.info("Reading metadata from local files")
        meta = LocalMetaRetriever({name: paths[1] for name, paths in inputs.items()})
    else:
        logger.info("No metadata files found, requesting metadata from MetaCat")
        from merge_utils.metacat_utils import MetaCatRetriever #pylint: disable=import-outside-toplevel
        ns = config.inputs['namespace']
        meta = MetaCatRetriever(dids = [ns + ':' + name for name in inputs])

    if has_data:
        logger.info("Reading data from local files")
        data = LocalPathFinder(meta, {name: paths[0] for name, paths in inputs.items() if paths[0]})
    else:
        logger.info("No data files found, requesting physical file paths from Rucio")
        from merge_utils.rucio_utils import RucioFinder #pylint: disable=import-outside-toplevel
        data = RucioFinder(meta)

    return data

class LocalMetaRetriever(MetaRetriever):
    """MetaRetriever for local files"""

    def __init__(self, files: dict):
        """
        Initialize the LocalMetaRetriever with a list of json files.

        :param files: dictionary of metadata file names and paths
        """
        super().__init__()

        self.filelist = files

    async def input_batches(self) -> AsyncGenerator[dict, None]:
        """Retrieve metadata for local files in batches"""
        batch_id = 0
        batch = []
        while self.filelist:
            name, path = self.filelist.popitem()
            if path is None:
                self.missing[config.inputs['namespace'] + ':' + name] += 1
                continue
            metadata = io_utils.read_config_file(path)
            if metadata is None:
                self.missing[config.inputs['namespace'] + ':' + name] += 1
                continue
            batch.append(metadata)

            if len(batch) >= self.step:
                added = await self.add(batch)
                logger.debug("yielding file batch %d", batch_id)
                batch_id += 1
                yield added
                batch = []
        if batch:
            added = await self.add(batch)
            logger.debug("yielding last file batch")
            yield added


class LocalPathFinder(PathFinder):
    """PathFinder for local files"""

    def __init__(self, source: MetaRetriever, files: dict = None, dirs: list = None):
        """
        Initialize the LocalMetaRetriever with a list of json files.

        :param source: MetaRetriever object to use as the source of file metadata
        :param files: dictionary of metadata file names and paths
        :param dirs: list of directories to search for data files
        """
        super().__init__(source)

        self.filelist = files or {}
        self.dirs = dirs or []

    async def process(self, files: dict) -> None:
        """
        Process a batch of files to find their physical locations.
        
        :param files: dictionary of files to process
        """
        logger.debug("Retrieving physical file paths")
        unreachable = []
        for file in files.values():
            path = self.filelist.pop(file.name, None)
            if path is None:
                # If no path is found, try searching in the provided directories
                path = search(file.name, self.dirs)
                if path is None:
                    # If still not found, mark as unreachable
                    unreachable.append(file.did)
                    continue

            #TODO: generalize this for other sites
            if path.startswith('/pnfs/'):
                # convert pnfs paths to xroot paths
                fnal_prefix = "root://fndca1.fnal.gov:1094/pnfs/fnal.gov/usr"
                path = path.replace('/pnfs', fnal_prefix)
            file.path = path

        lvl = logging.ERROR if config.validation['skip']['unreachable'] else logging.CRITICAL
        io_utils.log_list("Failed to locate {n} file path{s}:", unreachable, lvl)
        self.files.set_unreachable(unreachable)

    def output_chunks(self) -> Generator[MergeChunk, None, None]:
        """
        Yield chunks of files for merging.
        
        :return: yields a series of MergeChunk objects
        """
        for chunk in super().output_chunks():
            chunk.site = config.sites['local']
            yield chunk
