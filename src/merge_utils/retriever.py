"""FileRetriever classes"""

import logging
import os
import sys
import math
import asyncio
from abc import ABC, abstractmethod

from typing import AsyncGenerator, Generator

from merge_utils import io_utils, config
from merge_utils.merge_set import MergeSet, MergeChunk

logger = logging.getLogger(__name__)

class FileRetriever(ABC):
    """Base class for retrieving metadata from a source"""

    def __init__(self):
        self.step = config.validation['batch_size']
        self.allow_missing = config.validation['skip']['missing']
        self._files = MergeSet()

    @property
    def files(self) -> MergeSet:
        """Return the set of files from the source"""
        return self._files

    @property
    def dupes(self) -> dict:
        """Return the set of duplicate files from the source"""
        return self._files.dupes

    @property
    def missing(self) -> dict:
        """Return the set of missing files from the source"""
        return self._files.missing

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the metadata source"""
        # connect to source

    async def add(self, files: list, dids: list = None) -> dict:
        """
        Add the metadata for a list of files to the set.
        
        :param files: list of dictionaries with file metadata
        :param dids: optional list of DIDs requested, used to check for missing files
        :return: dict of MergeFile objects that were added
        """
        # check for missing files
        if dids and len(files) < len(dids):
            res_set = {x['namespace'] + ':' + x['name'] for x in files}
            for did in set(dids) - res_set:
                self.missing[did] += 1

        # add files to merge set
        added = await asyncio.to_thread(self.files.add_files, files)
        return added

    @abstractmethod
    async def input_batches(self) -> AsyncGenerator[dict, None]:
        """
        Asynchronously retrieve metadata for the next batch of files.

        :return: dict of MergeFile objects that were added
        """
        # yield batch

    async def _loop(self) -> None:
        """Repeatedly get input_batches until all files are retrieved."""
        # connect to source
        await self.connect()
        # loop over batches
        async for _ in self.input_batches():
            if config.validation['fast_fail']:
                if self.files.check_errors():
                    raise ValueError("Input files failed validation, exiting early!")

    def run(self) -> None:
        """Retrieve metadata for all files."""
        try:
            asyncio.run(self._loop())
        except ValueError as err:
            logger.critical("%s", err)
            sys.exit(1)

        # do error checking
        if self.files.check_errors(final=True):
            logger.critical("Input files failed validation, exiting!")
            sys.exit(1)
        if len(self.files) == 0:
            logger.critical("Failed to retrieve any files, exiting!")
            sys.exit(1)

    def output_chunks(self) -> Generator[MergeChunk, None, None]:
        """
        Yield chunks of files for merging.
        
        :return: yields a series of MergeChunk objects
        """
        for group in self.files.groups():
            if len(group) > config.merging['chunk_max']:
                n_chunks = math.ceil(len(group) / config.merging['chunk_max'])
                target_size = len(group) / n_chunks
                chunk = group.chunk()
                for file in sorted(group.values(), key=lambda f: f.path):
                    chunk.add(file)
                    if len(chunk) >= target_size:
                        yield chunk
                        chunk = group.chunk()
                if chunk:
                    yield chunk
            yield group

class LocalRetriever(FileRetriever):
    """FileRetriever for local files"""

    def __init__(self, filelist: list, meta_dirs: list = None):
        """
        Initialize the LocalRetriever with a list of files and optional metadata directories.
        
        :param filelist: list of input data files
        :param meta_dirs: optional list of directories to search for metadata files
        """
        super().__init__()
        self.filelist = filelist or []
        self.meta_dirs = meta_dirs or []
        self.json_files = {}

    async def connect(self) -> None:
        """No need to connect to the local filesystem, but we can do some preprocessing."""
        # No connection needed for local files
        # We might have a mix of data and json files, so we need to separate them
        data_files = []
        for file in self.filelist:
            name = os.path.basename(file)
            if os.path.splitext(name)[1] == '.json':
                path = os.path.dirname(file)
                name = os.path.splitext(name)[0]
                self.json_files[name] = path
            else:
                if os.path.exists(file):
                    data_files.append(file)
                else:
                    self.missing[name] += 1
        self.filelist = data_files
        logger.debug("Found %d input data files", len(self.filelist))

    async def get_metadata(self, file: str) -> dict:
        """Retrieve metadata for a single file"""
        name = os.path.basename(file)
        path = os.path.dirname(file)
        metadata = None
        if name in self.json_files:
            # we already have a matching json file for this data file
            meta_path = os.path.join(self.json_files.pop(name), name + '.json')
            if os.path.exists(meta_path):
                metadata = io_utils.read_config_file(meta_path)
        if metadata is None:
            # try to find a matching json file in the same directory or in the meta_dirs
            dirs = [path] + self.meta_dirs
            for path in dirs:
                meta_path = os.path.join(path, name + '.json')
                if os.path.exists(meta_path):
                    metadata = io_utils.read_config_file(meta_path)
                    break
        if metadata is None:
            self.missing[name] += 1
            return None

        path = os.path.join(path, name)
        #TODO: generalize this for other sites
        if path.startswith('/pnfs/'):
            # convert pnfs paths to xroot paths
            fnal_prefix = "root://fndca1.fnal.gov:1094/pnfs/fnal.gov/usr"
            path = path.replace('/pnfs', fnal_prefix)
        metadata['path'] = path
        return metadata

    async def input_batches(self) -> AsyncGenerator[dict, None]:
        """Retrieve metadata for local files in batches"""
        batch_id = 0
        batch = []
        for file in self.filelist:
            metadata = await self.get_metadata(file)
            if metadata is None:
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
