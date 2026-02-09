"""Utility functions for interacting with the Rucio web API."""
from __future__ import annotations

import logging
import math
import asyncio
from typing import Generator

from rucio.client import Client #type: ignore pylint: disable=import-error
from rucio.client.replicaclient import ReplicaClient #type: ignore pylint: disable=import-error

from merge_utils.merge_set import MergeFile, MergeChunk
from merge_utils.retriever import MetaRetriever, PathFinder
from merge_utils.merge_rse import MergeRSEs
from merge_utils import io_utils, config

logger = logging.getLogger(__name__)

class RucioFinder (PathFinder):
    """Class for managing asynchronous queries to the Rucio web API."""

    def __init__(self, source: MetaRetriever):
        """
        Initialize the RucioRetriever with a source of file metadata.
        
        :param source: FileRetriever object to use as the source of file metadata
        """
        super().__init__('rucio', source)

        self.rses = MergeRSEs()

        self.client = None

    async def connect(self) -> None:
        """Connect to the Rucio web API"""
        if not self.client:
            src_connect = asyncio.create_task(self.meta.connect())
            logger.debug("Connecting to Rucio")
            rucio_client = Client()
            rse_connect = asyncio.create_task(self.rses.connect(rucio_client))
            self.client = ReplicaClient(rucio_client)
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
            crit = config.validation.error_handling.unreachable == 'quit'
            lvl = logging.CRITICAL if crit else logging.ERROR
            logger.log(lvl, "Size mismatch for %s: %d != %d", file.did, file.size, rucio['bytes'])
            return False
        # See if we should skip the checksum check
        if len(config.validation.checksums) == 0:
            return True
        # Check the checksum
        for algo in config.validation.checksums:
            if algo in file.checksums and algo in rucio:
                csum1 = file.checksums[algo]
                csum2 = rucio[algo]
                if csum1 == csum2:
                    logger.debug("Found matching %s checksum for %s", algo, file.did)
                    return True
                crit = config.validation.error_handling.unreachable == 'quit'
                lvl = logging.CRITICAL if crit else logging.ERROR
                logger.log(lvl, "%s checksum err for %s: %s != %s", algo, file.did, csum1, csum2)
                return False
            if algo not in file.checksums:
                logger.debug("MetaCat missing %s checksum for %s", algo, file.did)
            if algo not in rucio:
                logger.debug("Rucio missing %s checksum for %s", algo, file.did)
        # If we get here, we have no matching checksums
        crit = config.validation.error_handling.unreachable == 'quit'
        lvl = logging.CRITICAL if crit else logging.ERROR
        logger.log(lvl, "No matching checksums for %s", file.did)
        return False

    async def get_paths(self, files: dict) -> list:
        """
        Asynchronously retrieve paths for a specific batch of files.

        :param files: dictionary of files to retrieve paths for
        :return: list of file path dictionaries
        """
        query = [{'scope':file.namespace, 'name':file.name} for file in files.values()]
        res = await asyncio.to_thread(self.client.list_replicas, query, ignore_availability=False)
        return list(res)

    async def process(self, files: dict, paths: list) -> None:
        """
        Process a batch of files to assign paths.
        
        :param files: dictionary of files to process
        :param paths: list of file path dictionaries from Rucio
        """
        found = set()
        unreachable = []
        for replicas in paths:
            did = replicas['scope'] + ':' + replicas['name']
            found.add(did)
            count = len(replicas['pfns'])
            logger.debug("Found %d replicas for %s", count, did)
            if count == 0:
                unreachable.append(did)
                continue

            file = files[did]
            pfns, csum = await asyncio.gather(
                self.rses.add_replicas(did, replicas),
                self.checksum(file, replicas)
            )
            if not csum:
                pfns = {}
            if pfns:
                file.paths = pfns
            else:
                unreachable.append(did)

            logger.debug("Added %d replicas for %s", len(pfns), did)

        missing = [did for did in files if did not in found]
        crit = config.validation.error_handling.unreachable == 'quit'
        lvl = logging.CRITICAL if crit else logging.ERROR
        io_utils.log_list("Failed to find {n} file{s} in Rucio database:", missing, lvl)
        unreachable.extend(missing)
        io_utils.log_list("Failed to retrieve {n} file{s} from Rucio:", unreachable, lvl)
        self.files.set_unreachable(unreachable)

    def run(self) -> None:
        """Retrieve metadata for all files."""
        super().run()
        self.rses.cleanup()

    def output_chunks_old(self) -> Generator[MergeChunk, None, None]:
        """
        Yield chunks of files for merging.
        
        :return: yeilds a series of MergeChunk objects
        """
        for group in self.files.groups():
            pfns = self.rses.get_pfns(group)
            for site in pfns:
                for did, pfn in pfns[site].items():
                    group[did].path = pfn[0]

            chunk_max = config.grouping.chunk_max
            if len(pfns) == 1:
                site = next(iter(pfns))
                group.site = site
                if len(pfns[site]) <= chunk_max:
                    yield group
                    continue

            for site in pfns:
                n_chunks = math.ceil(len(group) / chunk_max)
                target_size = math.ceil(len(group) / n_chunks)
                chunk = group.chunk()
                chunk.site = site
                for did in pfns[site]:
                    if len(chunk) >= target_size:
                        yield chunk
                        chunk = group.chunk()
                        chunk.site = site
                    chunk.add(group[did])
                yield chunk

            if not group.site:
                group.site = config.sites.default
            yield from group.tier2_chunks()
