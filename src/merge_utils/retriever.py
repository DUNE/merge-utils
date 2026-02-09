"""FileRetriever classes"""

import logging
import os
import sys
import math
import json
import asyncio
from abc import ABC, abstractmethod
import collections

from typing import AsyncGenerator, Generator

from merge_utils import config, io_utils
from merge_utils.merge_set import MergeSet, MergeChunk
from merge_utils.metacat_utils import MetaCatWrapper

logger = logging.getLogger(__name__)

class MetaRetriever(ABC):
    """Base class for retrieving metadata from a source"""

    def __init__(self, name, file_owner: bool = True):
        self.name = name
        if file_owner:
            self._files = MergeSet()
        self.dir = os.path.join(str(config.job.dir), 'cache', self.name)
        os.makedirs(self.dir, exist_ok=True)
        self.client = MetaCatWrapper()

    @property
    def files(self) -> MergeSet:
        """Return the set of files from the source"""
        return self._files

    async def connect(self) -> None:
        """Connect to the MetaCat web API"""
        await self.client.connect()

    @abstractmethod
    async def get_metadata(self, skip: int, limit: int) -> list:
        """
        Asynchronously retrieve metadata for a specific batch of files.

        :param skip: number of files to skip
        :param limit: maximum number of files to retrieve
        :return: list of file metadata dictionaries
        """
        # retrieve specific batch

    async def load_cache(self, idx: int) -> list:
        """
        Load a cached batch of file metadata from disk.

        :param idx: batch number to load
        :return: list of file metadata dictionaries if cache exists, else None
        """
        path = os.path.join(self.dir, f"batch_{idx}.json")
        if os.path.exists(path):
            logger.debug("Loading cached %s input batch %d", self.name, idx)
            json_dict = io_utils.read_config_file(path)
            files = json_dict.get('files', [])
            return files
        return None

    async def save_cache(self, idx: int, files: list) -> None:
        """
        Save a batch of file metadata to a cache file.

        :param idx: batch number to save
        :param files: list of file metadata dictionaries to save
        """
        logger.debug("Saving cached %s input batch %d", self.name, idx)
        path = os.path.join(self.dir, f"batch_{idx}.json")
        json_dict = {'files': files}
        with open(path, 'w', encoding="utf-8") as f:
            f.write(json.dumps(json_dict, indent=2))

    async def check_existence(self, files: list) -> None:
        """
        Check that MetaCat records exist for a batch of input files.

        :param files: list of file metadata dictionaries to check
        """
        logger.debug("Checking MetaCat to verify file records")
        indices = {}
        for idx, file in enumerate(files):
            if file.get('missing', False) or file.get('duplicate', False):
                continue
            if file.get('fid') and not config.validation.check_fids:
                continue
            indices[f"{file['namespace']}:{file['name']}"] = idx
        res = await self.client.files([{'did': did} for did in indices], False, False)
        for file in res:
            did = f"{file['namespace']}:{file['name']}"
            idx = indices.pop(did)
            files[idx].update(file)
        if indices:
            for idx in indices.values():
                files[idx]['missing'] = True
            io_utils.log_list("MetaCat missing {n} file record{s}:", indices, logging.ERROR)
            io_utils.log_print("Did you mean to enable the grandparents option?", logging.ERROR)

    async def check_parents(self, files: list) -> None:
        """
        Check that MetaCat records exist for the parents of a batch of input files.

        :param files: list of file metadata dictionaries to check
        """
        logger.debug("Checking MetaCat to verify parent records")
        check_fids = config.validation.check_fids
        # Collect list of parents to check
        parents = []
        for file in files:
            for parent in file['parents']:
                if 'fid' in parent:
                    if check_fids:
                        parents.append({'fid': parent['fid']})
                else:
                    parents.append({"did": f"{parent['namespace']}:{parent['name']}"})
        # Retrieve parent info from MetaCat
        res = []
        step = config.validation.batch_size
        for i in range(0, len(parents), step):
            res.extend(await self.client.files(parents[i:i+step], False, False))
        fids = {file['fid'] for file in res}
        dids = {f"{file['namespace']}:{file['name']}":file['fid'] for file in res}
        # Mark files with missing parents
        missing = set()
        for file in files:
            parents = file['parents']
            for idx, parent in enumerate(parents):
                fid = parent.get('fid', None)
                if not fid:
                    did = f"{parent['namespace']}:{parent['name']}"
                    fid = dids.get(did, None)
                    if not fid:
                        file['missing'] = True
                        missing.add(f"did: {did}")
                        continue
                elif check_fids and fid not in fids:
                    file['missing'] = True
                    missing.add(f"fid: {fid}")
                    continue
                parents[idx] = {'fid': fid}
        if missing:
            io_utils.log_list("MetaCat missing {n} grandparent record{s}:", missing, logging.ERROR)

    async def input_batches(self) -> AsyncGenerator[tuple[int, dict], None]:
        """
        Asynchronously retrieve metadata for the next batch of files.

        :return: tuple of (skip, dict of MergeFile objects that were added)
        """
        skip0 = int(config.input.skip or 0)
        skip = skip0
        step = int(config.validation.batch_size)
        while True:
            # Determine file limit for this batch
            limit = step
            if config.input.limit:
                limit = min(limit, config.input.limit + skip0 - skip)
                if limit <= 0:
                    break
            # Retrieve batch, using cache if available
            batch = await self.load_cache(skip)
            if batch is None:
                logger.debug("Retrieving %s input batch %d", self.name, skip)
                batch = await self.get_metadata(skip, limit)
                if config.output.grandparents:
                    await self.check_parents(batch)
                await self.save_cache(skip, batch)
            # If no files were retrieved, we're done
            if len(batch) == 0:
                break
            # Add files to merge set
            added = await asyncio.to_thread(self.files.add, batch)
            yield (skip, added)
            # If the last batch was a partial batch, we're done
            if len(batch) < step:
                break
            # Increment skip for next batch
            skip += step

    async def _loop(self) -> None:
        """Repeatedly get input_batches until all files are retrieved."""
        # connect to source
        await self.connect()
        # loop over batches
        async for _ in self.input_batches():
            self.files.check_errors()

    def run(self) -> None:
        """Retrieve metadata for all files."""
        try:
            asyncio.run(self._loop())
        except ValueError as err:
            logger.critical("%s", err)
            sys.exit(1)

        self.files.check_errors(final = True)

class QueryRetriever(MetaRetriever):
    """Class for retrieving metadata from MetaCat using an MQL query."""

    def __init__(self, query: str):
        """
        Initialize the QueryRetriever with an MQL query.

        :param query: MQL query to find files
        """
        super().__init__('mc_query')
        self.query = query

    async def get_metadata(self, skip: int, limit: int) -> list:
        """
        Asynchronously query MetaCat for a specific batch of files

        :param skip: number of files to skip
        :param limit: maximum number of files to retrieve
        :return: list of file metadata dictionaries
        """
        query_batch = self.query + f" skip {skip} limit {limit}"
        return await self.client.query(query_batch, metadata = True,
                                       provenance = config.output.grandparents)

class DidRetriever(MetaRetriever):
    """Class for retrieving metadata from MetaCat using a list of DIDs."""

    def __init__(self, dids: list, dupes: set = None):
        """
        Initialize the DidRetriever with a list of DIDs.

        :param dids: list of file DIDs to find
        :param dupes: set of indices of duplicate DIDs
        """
        super().__init__('mc_dids')
        self.dids = dids
        self.check_namespaces()
        self.dupes = dupes if dupes is not None else self.check_duplicates()

    def check_namespaces(self) -> None:
        """
        Check DID list for namespace issues.
        """
        # Check namespaces
        namespaces = collections.defaultdict(int)
        for did in self.dids:
            parts = did.split(':', 1)
            if len(parts) == 2:
                namespaces[parts[0]] += 1
        if len(namespaces) == 0:
            ns = config.input.namespace
            logger.info("DID list missing namespaces, using default namespace '%s'", ns)
            self.dids = [f"{ns}:{did}" for did in self.dids]
        elif len(namespaces) == 1:
            ns, count = next(iter(namespaces.items()))
            if count < len(self.dids):
                logger.warning("Some DIDs missing namespaces, assuming shared namespace '%s'", ns)
                self.dids = [f"{ns}:{did}" if ':' not in did else did for did in self.dids]
        elif config.validation.error_handling.inconsistent == 'quit':
            io_utils.log_list("DID list contains multiple namespaces:",
                              namespaces, logging.CRITICAL)
            sys.exit(1)
        else:
            count = sum(namespaces.values())
            ns = config.input.namespace
            if count < len(self.dids):
                logger.warning("Some DIDs missing namespaces, assuming default namespace '%s'", ns)
                self.dids = [f"{ns}:{did}" if ':' not in did else did for did in self.dids]

    def check_duplicates(self) -> set:
        """
        Check DID list for duplicate entries.

        :return: set of indices of duplicate DIDs
        """
        seen = set()
        dupes = set()
        for idx, did in enumerate(self.dids):
            if did in seen:
                dupes.add(idx)
            seen.add(did)
        if dupes and config.validation.error_handling.duplicate == 'quit':
            io_utils.log_list("DID list contains {n} duplicate file{s}:",
                              list(self.dupes), logging.CRITICAL)
            sys.exit(1)
        return dupes

    async def get_metadata(self, skip: int, limit: int) -> list:
        """
        Asynchronously request a batch of DIDs from MetaCat

        :param skip: number of files to skip
        :param limit: maximum number of files to retrieve
        :return: list of file metadata dictionaries
        """
        dids = self.dids[skip:skip+limit]
        if len(dids) == 0:
            logger.debug("No DIDs to request for skip=%d, limit=%d", skip, limit)
            return []
        # Build query and list of placeholder files
        query = []
        files = []
        indices = {}
        for idx, did in enumerate(dids):
            namespace, name = did.split(':')
            placeholder = {'namespace': namespace, 'name': name}
            if (skip + idx) in self.dupes:
                logger.debug("Skipping duplicate DID: %s", did)
                placeholder['duplicate'] = True
            else:
                placeholder['missing'] = True
                query.append({'did': did})
                indices[did] = idx
            files.append(placeholder)
        if len(query) == 0:
            logger.debug("All DIDs in batch are duplicates, skipping MetaCat request")
            return files
        # Request files from MetaCat
        res = await self.client.files(query, metadata = True,
                                      provenance = config.output.grandparents)
        # Add returned files to output list in correct order
        for file in res:
            files[indices[f"{file['namespace']}:{file['name']}"]] = file
        return files

class PathFinder(MetaRetriever):
    """Base class for finding paths to files"""

    def __init__(self, name: str, meta: MetaRetriever):
        super().__init__(name, file_owner=False)
        self.meta = meta
        self.client = None

    @property
    def files(self) -> MergeSet:
        """Return the set of files from the source"""
        return self.meta.files

    async def connect(self) -> None:
        """Connect to the file source"""
        # connect to source
        await self.meta.connect()

    async def get_metadata(self, skip: int, limit: int) -> list:
        raise NotImplementedError("PathFinder does not implement get_metadata")

    @abstractmethod
    async def get_paths(self, files: dict) -> list:
        """
        Asynchronously retrieve paths for a specific batch of files.

        :param files: dictionary of files to retrieve paths for
        :return: list of file path dictionaries
        """
        # retrieve paths for specific batch

    @abstractmethod
    async def process(self, files: dict, paths: list) -> None:
        """
        Process a batch of files to assign paths.
        
        :param files: dictionary of files to process
        """
        # process files to find paths

    async def input_batches(self) -> AsyncGenerator[tuple[int, dict], None]:
        """
        Asynchronously retrieve paths for the next batch of files.

        :return: tuple of (skip, dict of MergeFile objects that were processed)
        """
        async for skip, batch in self.meta.input_batches():
            # Retrieve paths, using cache if available
            paths = await self.load_cache(skip)
            if paths is None:
                logger.debug("Retrieving %s input batch %d", self.name, skip)
                good_files = {did: file for did, file in batch.items() if not file.errors}
                paths = await self.get_paths(good_files)
                await self.save_cache(skip, paths)
            await self.process(batch, paths)
            yield (skip, batch)
