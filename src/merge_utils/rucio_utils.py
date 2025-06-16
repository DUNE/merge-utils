"""Utility functions for interacting with the Rucio web API."""
from __future__ import annotations

import logging
import math
import asyncio
from typing import AsyncGenerator, Generator

from rucio.client.replicaclient import ReplicaClient

from merge_utils.merge_set import MergeFile, MergeSet, MergeChunk
from merge_utils.retriever import FileRetriever
from merge_utils.merge_rse import MergeRSEs
from merge_utils import io_utils, config

logger = logging.getLogger(__name__)

class RucioRetriever (FileRetriever):
    """Class for managing asynchronous queries to the Rucio web API."""

    def __init__(self, source: FileRetriever):
        """
        Initialize the RucioRetriever with a source of file metadata.
        
        :param source: FileRetriever object to use as the source of file metadata
        """
        super().__init__()

        self.checksums = config.validation['checksums']
        self.rses = MergeRSEs()
        self.source = source

        self.client = None

    @property
    def files(self) -> MergeSet:
        """Return the set of files from the source"""
        return self.source.files

    @property
    def missing(self) -> dict:
        """Return the set of missing files from the source"""
        return self.source.missing

    @property
    def dupes(self) -> dict:
        """Return the set of duplicate files from the source"""
        return self.source.dupes

    async def connect(self) -> None:
        """Connect to the Rucio web API"""
        if not self.client:
            logger.debug("Connecting to Rucio")
            src_connect = asyncio.create_task(self.source.connect())
            rse_connect = asyncio.create_task(self.rses.connect())
            self.client = ReplicaClient()
            await rse_connect
            await src_connect
        else:
            logger.debug("Already connected to Rucio")

    async def checksum(self, file: MergeFile, rucio: dict) -> bool:
        """
        Ensure file sizes and checksums from Rucio agree with the input metadata.
        
        :param file: MergeFile object to check
        :param rucio: Rucio replicas dictionary
        :return: True if files match, False otherwise
        """
        # Check the file size
        if file.size != rucio['bytes']:
            lvl = logging.ERROR if config.validation['skip']['unreachable'] else logging.CRITICAL
            logger.log(lvl, "Size mismatch for %s: %d != %d", file.did, file.size, rucio['bytes'])
            return False
        # See if we should skip the checksum check
        if len(self.checksums) == 0:
            return True
        # Check the checksum
        for algo in self.checksums:
            if algo in file.checksums and algo in rucio:
                csum1 = file.checksums[algo]
                csum2 = rucio[algo]
                if csum1 == csum2:
                    logger.debug("Found matching %s checksum for %s", algo, file.did)
                    return True
                lvl = logging.ERROR if config.validation['skip']['unreachable'] else logging.CRITICAL
                logger.log(lvl, "%s checksum mismatch for %s: %s != %s", algo, file.did, csum1, csum2)
                return False
            if algo not in file.checksums:
                logger.debug("MetaCat missing %s checksum for %s", algo, file.did)
            if algo not in rucio:
                logger.debug("Rucio missing %s checksum for %s", algo, file.did)
        # If we get here, we have no matching checksums
        lvl = logging.ERROR if config.validation['skip']['unreachable'] else logging.CRITICAL
        logger.log(lvl, "No matching checksums for %s", file.did)
        return False

    async def process(self, files: dict) -> None:
        """
        Process a batch of files to find their physical locations in Rucio.
        
        :param files: dictionary of files to process
        """
        logger.debug("Retrieving physical file paths from Rucio")
        found = set()
        unreachable = []
        query = [{'scope':file.namespace, 'name':file.name} for file in files.values()]
        res = await asyncio.to_thread(self.client.list_replicas, query, ignore_availability=False)
        for replicas in res:
            did = replicas['scope'] + ':' + replicas['name']
            logger.debug("Found %d replicas for %s", len(replicas['pfns']), did)
            found.add(did)
            file = files[did]

            added, csum = await asyncio.gather(
                self.rses.add_replicas(did, replicas),
                self.checksum(file, replicas)
            )
            if not csum:
                added = 0
            if not added:
                unreachable.append(did)

            logger.debug("Added %d replicas for %s", added, did)

        missing = [did for did in files if did not in found]
        lvl = logging.ERROR if config.validation['skip']['unreachable'] else logging.CRITICAL
        io_utils.log_list("Failed to find {n} file{s} in Rucio database:", missing, lvl)
        unreachable.extend(missing)
        io_utils.log_list("Failed to retrieve {n} file{s} from Rucio:", unreachable, lvl)
        self.files.set_unreachable(unreachable)

    async def input_batches(self) -> AsyncGenerator[dict, None]:
        """
        Asynchronously retrieve metadata for the next batch of files.

        :return: dict of MergeFile objects that were added
        """
        async for batch in self.source.input_batches():
            await self.process(batch)
            yield batch

    def run(self) -> None:
        """Retrieve metadata for all files."""
        super().run()
        self.rses.cleanup()

    def output_chunks(self) -> Generator[MergeChunk, None, None]:
        """
        Yield chunks of files for merging.
        
        :return: yeilds a series of MergeChunk objects
        """
        for group in self.files.groups():
            pfns = self.rses.get_pfns(group)
            for site in pfns:
                for did, pfn in pfns[site].items():
                    group[did].path = pfn[0]

            if len(pfns) == 1:
                site = next(iter(pfns))
                if len(pfns[site]) <= config.merging['chunk_max']:
                    group.site = site
                    yield group
                    continue

            for site in pfns:
                n_chunks = math.ceil(len(group) / config.merging['chunk_max'])
                target_size = len(group) / n_chunks
                chunk = group.chunk()
                chunk.site = site
                for did in pfns[site]:
                    chunk.add(group[did])
                    if len(chunk) >= target_size:
                        yield chunk
                        chunk = group.chunk()
                        chunk.site = site
                if chunk:
                    yield chunk
            yield group
