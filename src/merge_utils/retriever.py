"""FileRetriever classes"""

import logging
import os
import sys
import math
import json
import asyncio
from abc import ABC, abstractmethod
import collections
from dataclasses import dataclass

from typing import AsyncGenerator, Generator

from merge_utils import config, io_utils
from merge_utils.merge_set import MergeSet, MergeChunk
from merge_utils.metacat_utils import MetaCatWrapper

logger = logging.getLogger(__name__)

@dataclass
class InputBatch:
    """Class representing a batch of input file data, starting at a specific skip index."""
    skip: int
    files: list = None

    def __post_init__(self):
        if self.files is None:
            self.files = []

    def __len__(self):
        """Return the number of files in the batch."""
        return len(self.files)

    def __iter__(self):
        """Iterate over the files in the batch."""
        return iter(self.files)

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
    async def get_metadata(self, batch: InputBatch, limit: int) -> list:
        """
        Asynchronously retrieve metadata for a specific batch of files.

        :param batch: empty InputBatch object with the skip index set
        :param limit: maximum number of files to retrieve
        :return: list of file metadata dictionaries
        """
        # retrieve specific batch

    async def get_batch(self, getter: callable, batch: InputBatch, **kwargs) -> InputBatch:
        """
        Asynchronously retrieve a batch of input data, with caching.

        :param getter: function to call to retrieve inputs
        :param batch: InputBatch object to retrieve data for
        :param kwargs: additional arguments to pass to getter
        :return: list of file dictionaries
        """
        skip = batch.skip
        cache = os.path.join(self.dir, f"batch_{skip}.json")
        if os.path.exists(cache):
            logger.debug("Loading cached %s input batch %d", self.name, skip)
            files = io_utils.read_config_file(cache).get('files', [])
        else:
            logger.debug("Retrieving new %s input batch %d", self.name, skip)
            files = await getter(batch=batch, **kwargs)
            with open(cache, 'w', encoding="utf-8") as f:
                f.write(json.dumps({'files': files}, indent=2))
        return InputBatch(skip=skip, files=files)

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

    async def input_batches(self) -> AsyncGenerator[InputBatch, None]:
        """
        Asynchronously retrieve input file metadata in batches.

        :return: InputBatch object containing skip index and list of MergeFile objects
        """
        skip0 = int(config.input.skip or 0)
        skip = skip0
        step = int(config.validation.batch_size)
        batch = None
        task = None
        while True:
            # Determine file limit for next batch
            limit = step
            if config.input.limit:
                limit = min(limit, config.input.limit + skip0 - skip)
            # Get previous batch to process, if we have a request in flight
            if task is not None:
                batch = await task
            task = None
            # Start request for next batch
            if limit > 0:
                req = InputBatch(skip=skip)
                task = asyncio.create_task(self.get_batch(self.get_metadata, req, limit=limit))
            # Increment skip for next batch
            skip += step
            # Process previous batch while we wait, if we have one
            if batch is None:
                continue
            logger.debug("Processing new %s input batch %d", self.name, batch.skip)
            # Add file to merge set, and yield if we added any
            added = await asyncio.to_thread(self.files.add, batch.skip, batch.files)
            if added:
                yield InputBatch(skip=batch.skip, files=added)
            # If the last batch was a partial batch, we're done
            if len(batch) < limit:
                # If we started a request for the next batch, wait for it to finish
                if task is not None:
                    await task
                break

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

    async def get_metadata(self, batch: InputBatch, limit: int) -> list:
        """
        Asynchronously query MetaCat for a specific batch of files

        :param batch: InputBatch object with skip index set
        :param limit: maximum number of files to retrieve
        :return: list of file metadata dictionaries
        """
        query_batch = self.query + f" skip {batch.skip} limit {limit}"
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

    async def get_metadata(self, batch: InputBatch, limit: int) -> list:
        """
        Asynchronously request a batch of DIDs from MetaCat

        :param batch: InputBatch object with skip index set
        :param limit: maximum number of files to retrieve
        :return: list of file metadata dictionaries
        """
        skip = batch.skip
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
            if skip+idx in self.dupes:
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

    async def get_metadata(self, batch: InputBatch, limit: int) -> list:
        raise NotImplementedError("PathFinder does not implement get_metadata")

    @abstractmethod
    async def get_paths(self, batch: InputBatch) -> list:
        """
        Asynchronously retrieve paths for a specific batch of files.

        :param batch: InputBatch object containing files to retrieve paths for
        :return: list of file path dictionaries
        """
        # retrieve paths for specific batch

    @abstractmethod
    async def set_paths(self, batch: InputBatch, paths: list) -> None:
        """
        Asynchronously set paths for a specific batch of files.
        
        :param batch: InputBatch object containing files to process
        :param paths: list of file path dictionaries to use for setting paths
        """
        # process files to find paths

    async def input_batches(self) -> AsyncGenerator[InputBatch, None]:
        """
        Asynchronously retrieve paths for the next batch of files.

        :return: InputBatch object containing skip index and list of MergeFile objects
        """
        batch = None
        task = None
        paths = None
        async for new_batch in self.meta.input_batches():
            # Get paths for previous batch, if we have a request in flight
            if task is not None:
                paths = await task
            # Start request for next batch
            task = asyncio.create_task(self.get_batch(self.get_paths, new_batch))
            # Process previous batch while we wait, if we have one
            if batch is not None:
                await self.set_paths(batch, paths)
                good_files = [f for f in batch if not f.errors]
                if good_files:
                    yield InputBatch(skip=batch.skip, files=good_files)
            # Save new batch for next iteration
            batch = new_batch
        # Process last batch
        if batch is not None:
            paths = await task
            await self.set_paths(batch, paths)
            good_files = [f for f in batch if not f.errors]
            if good_files:
                yield InputBatch(skip=batch.skip, files=good_files)
