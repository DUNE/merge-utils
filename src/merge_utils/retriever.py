"""FileRetriever classes"""

import logging
import sys
import math
import asyncio
from abc import ABC, abstractmethod

from typing import AsyncGenerator, Generator

from merge_utils import config
from merge_utils.merge_set import MergeSet, MergeChunk

logger = logging.getLogger(__name__)

class MetaRetriever(ABC):
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
        return self.files.dupes

    @property
    def missing(self) -> dict:
        """Return the set of missing files from the source"""
        return self.files.missing

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
            if len(group) <= config.merging['chunk_max']:
                yield group
                continue
            n_chunks = math.ceil(len(group) / config.merging['chunk_max'])
            target_size = len(group) / n_chunks
            chunk = group.chunk()
            for file in sorted(group.values(), key=lambda f: f.path):
                if len(chunk) >= target_size:
                    yield chunk
                    chunk = group.chunk()
                chunk.add(file)
            yield chunk
            yield from group.tier2_chunks()

class PathFinder(MetaRetriever):
    """Base class for finding paths to files"""

    def __init__(self, meta: MetaRetriever): #pylint: disable=super-init-not-called
        self.meta = meta

    @property
    def files(self) -> MergeSet:
        """Return the set of files from the source"""
        return self.meta.files

    async def connect(self) -> None:
        """Connect to the file source"""
        # connect to source
        await self.meta.connect()

    @abstractmethod
    async def process(self, files: dict) -> None:
        """
        Process a batch of files to find their physical locations.
        
        :param files: dictionary of files to process
        """
        # process files to find paths

    async def input_batches(self) -> AsyncGenerator[dict, None]:
        """
        Asynchronously retrieve paths for the next batch of files.

        :return: dict of MergeFile objects that were added
        """
        async for batch in self.meta.input_batches():
            await self.process(batch)
            yield batch
