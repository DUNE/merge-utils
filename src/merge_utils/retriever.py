"""FileRetriever classes"""

import logging
import os
import sys
import json
import asyncio
from abc import ABC, abstractmethod
import collections
from dataclasses import dataclass

from typing import AsyncGenerator, Callable

from merge_utils import config, io_utils
from merge_utils.merge_set import MergeSet, MergeFileError
from merge_utils.metacat_utils import MetaCatWrapper

logger = logging.getLogger(__name__)

@dataclass
class InputBatch:
    """Class representing a batch of input file data, starting at a specific skip index."""
    skip: int = -1
    files: list = None

    def __post_init__(self):
        if self.files is None:
            self.files = []

    def __bool__(self):
        """Return True if the batch contains any files."""
        return len(self.files) > 0

    def __len__(self):
        """Return the number of files in the batch."""
        return len(self.files)

    def __iter__(self):
        """Iterate over the files in the batch."""
        return iter(self.files)

def file_serializer(obj):
    """Custom JSON serializer for MergeFileError objects"""
    if isinstance(obj, MergeFileError):
        return obj.name
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

class MetaRetriever(ABC):
    """Base class for retrieving metadata from a source"""
    name: str = "metadata"
    file_owner: bool = True

    def __init__(self):
        if self.file_owner:
            self._files = MergeSet()
        self.dir = os.path.join(str(config.job.dir), 'cache', self.name)
        os.makedirs(self.dir, exist_ok=True)
        self.client = MetaCatWrapper()

    @property
    def files(self) -> MergeSet:
        """Return the set of files from the source"""
        return self._files

    @property
    def namespace(self) -> str:
        """
        Return the default namespace for files without an explicit namespace.
        Checks the config for input.namespace, then output.namespace, then defaults to 'usertests'.
        """
        if config.input.namespace:
            return str(config.input.namespace)
        if config.output.namespace:
            return str(config.output.namespace)
        return 'usertests'

    async def get_done(self) -> None:
        """Asynchronously query MetaCat for merged files with the same tag as this job"""
        if config.validation.handling.already_done == 'include':
            return
        if not config.input.tag:
            logger.critical("Already-done checking requires a job tag to be specified!")
            sys.exit(1)
        tag = str(config.input.tag)
        logger.info("Checking MetaCat for already merged files with tag '%s'", tag)
        query = f"files where merge.tag == '{tag}' and dune.output_status == confirmed"
        dids = []
        skip = 0
        while True:
            batch_query = query + f" skip {skip} limit {config.validation.batch_size}"
            files = await self.client.query(batch_query, metadata=False, provenance=False)
            self.files.children.update(f['fid'] for f in files)
            dids.extend(f"{f['namespace']}:{f['name']}" for f in files)
            if len(files) < config.validation.batch_size:
                break
            skip += config.validation.batch_size
        if not dids:
            logger.info("No already merged files found with tag '%s'", tag)
            return
        io_utils.log_list("Found {n} merged file{s} with tag '%s':" % tag, dids, logging.INFO)

    async def connect(self) -> None:
        """Connect to the MetaCat web API"""
        await self.client.connect()
        await self.get_done()

    async def disconnect(self) -> None:
        """Disconnect from the MetaCat web API"""
        await self.client.disconnect()

    @abstractmethod
    async def get_metadata(self, batch: InputBatch, limit: int) -> list:
        """
        Asynchronously retrieve metadata for a specific batch of files.

        :param batch: empty InputBatch object with the skip index set
        :param limit: maximum number of files to retrieve
        :return: list of file metadata dictionaries
        """
        # retrieve specific batch

    async def get_batch(self, getter: Callable, batch: InputBatch, **kwargs) -> InputBatch:
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
                f.write(json.dumps({'files': files}, indent=2, default=file_serializer))
        return InputBatch(skip=skip, files=files)

    async def check_existence(self, files: list) -> None:
        """
        Check that MetaCat records exist for a batch of input files.

        :param files: list of file metadata dictionaries to check
        """
        logger.debug("Checking MetaCat to verify file records")
        skip_fids = not config.validation.check_fids
        provenance = False
        # To check for already merged files, we need the children of all input files
        if config.validation.handling.already_done != 'include':
            skip_fids = False
            provenance = True
        # Build list of DIDs to check, skipping files with FIDs if we can
        indices = {}
        for idx, file in enumerate(files):
            if file.get('errors'):
                continue
            if skip_fids and 'fid' in file:
                continue
            indices[f"{file['namespace']}:{file['name']}"] = idx
        # Request file info from MetaCat, with provenance if necessary
        dids = [{'did': did} for did in indices]
        res = await self.client.files(dids, metadata=False, provenance=provenance)
        # Update file list with the returned records
        for file in res:
            did = f"{file['namespace']}:{file['name']}"
            idx = indices.pop(did)
            files[idx].update(file)
        # Mark any files that were missing from MetaCat with an error
        if indices:
            for idx in indices.values():
                files[idx]['errors'] = MergeFileError.NO_METADATA
            io_utils.log_list("MetaCat missing {n} file record{s}:", indices, logging.ERROR)
            io_utils.log_print("Did you mean to enable the grandparents option?", logging.ERROR)

    async def get_siblings(self, files: list) -> None:
        """
        We check for already merged files by looking at the children of the input files.
        But in grandparents mode, we actually want the children of the input file parents instead.
        We don't need the original children in that case, so just replace them with the siblings.

        :param files: list of file metadata dictionaries to check
        """
        # Collect list of all parent FIDs to check
        parent_fids = set()
        for file in files:
            for parent in file['parents']:
                parent_fids.add(parent['fid'])
        fid_list = [{'fid': fid} for fid in parent_fids]
        # Retrieve children of parents from MetaCat
        parents = await self.client.files(fid_list, metadata = False, provenance = True)
        children = collections.defaultdict(set)
        for parent in parents:
            child_fids = set(c['fid'] for c in parent.get('children', []))
            children[parent['fid']].update(child_fids)
        # Override the children of the input files
        for file in files:
            siblings = set()
            for parent in file['parents']:
                siblings.update(children[parent['fid']])
            siblings.discard(file.get('fid', None))
            file['children'] = [{'fid': f} for f in siblings]

    async def check_parents(self, files: list) -> None:
        """
        Check that MetaCat records exist for the parents of a batch of input files.
        Also get the siblings if we need them for the already merged check.

        :param files: list of file metadata dictionaries to check
        """
        logger.debug("Checking MetaCat to verify parent records")
        skip_fids = not config.validation.check_fids
        provenance = False
        # To check for already merged files, we need the children of all input files
        if config.validation.handling.already_done != 'include':
            skip_fids = False
            provenance = True
        # Collect list of all parent DIDs to check
        parent_dids = set()
        for file in [f for f in files if not f.get('errors')]:
            for parent in file.get('parents', []):
                if skip_fids and 'fid' in parent:
                    continue
                parent_dids.add(f"{parent['namespace']}:{parent['name']}")
        # Request parent info from MetaCat, with provenance if necessary
        dids = [{'did': did} for did in parent_dids]
        res = await self.client.files(dids, metadata=False, provenance=provenance)
        parents = {f"{file['namespace']}:{file['name']}": file for file in res}
        # Check for missing parents, and build child list if needed for already merged check
        missing = set()
        for file in [f for f in files if not f.get('errors')]:
            children = set()
            for parent in file.get('parents', []):
                if skip_fids and 'fid' in parent:
                    continue
                did = f"{parent['namespace']}:{parent['name']}"
                parent_info = parents.get(did)
                if not parent_info:
                    file['errors'] = MergeFileError.UNDECLARED
                    missing.add(f"did: {did}")
                    continue
                parent['fid'] = parent_info.get('fid')
                if not provenance:
                    continue
                child_fids = set(c['fid'] for c in parent.get('children', []))
                children.update(child_fids)
            if provenance:
                children.discard(file.get('fid', None))
                file['children'] = [{'fid': f} for f in children]
        # Log any missing parents
        if missing:
            io_utils.log_list("MetaCat missing {n} grandparent record{s}:", missing, logging.ERROR)

    async def get_files(self, query: list) -> list:
        """
        Asynchronously retrieve file metadata for a specific list of DIDs.
        Also gets the siblings if we need them for the already merged check.

        :param query: list of dictionaries with 'did' keys to retrieve
        :return: list of file metadata dictionaries
        """
        # In grandparents mode, we need the parents of the input files
        parents = bool(config.output.grandparents)
        # To check for already merged files, we need the children of the input files
        children = (config.validation.handling.already_done != 'include')
        # Request files from MetaCat, with provenance if necessary
        provenance = parents or children
        files = await self.client.files(query, metadata = True, provenance = provenance)
        # In grandparents mode, we actually need the children of the parents
        if parents and children:
            await self.get_siblings(files)
        return files

    async def input_batches(self) -> AsyncGenerator[InputBatch, None]:
        """
        Asynchronously retrieve input file metadata in batches.

        :return: InputBatch object containing skip index and list of MergeFile objects
        """
        skip0 = int(config.input.skip or 0)
        skip = skip0
        step = int(config.validation.batch_size)
        task = None
        while True:
            # Determine file limit for next batch
            limit = step
            if config.input.limit:
                limit = min(limit, config.input.limit + skip0 - skip)
            # Get previous batch to process, if we have a request in flight
            batch = await task if task is not None else None
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
            logger.info("Processing new %s input batch %d", self.name, batch.skip)
            # Add file to merge set, and yield if we added any
            added = await asyncio.to_thread(self.files.add, batch.skip, batch.files)
            if added:
                yield InputBatch(skip=batch.skip, files=added)
            # If there is no next task, we're done
            if task is None:
                break
            # If the last batch was a partial batch, we're done
            if len(batch) < step:
                # Need to wait for the the last task to finish
                await task
                break
        # Yield empty batch to signal completion
        yield InputBatch()

    async def _loop(self) -> None:
        """Repeatedly get input_batches until all files are retrieved."""
        # Connect to source
        await self.connect()
        # Loop over batches, checking for errors as we go
        async for _ in self.input_batches():
            self.files.check_errors()
        # Close connections and do final error checking
        await self.client.disconnect()
        self.files.check_errors(final = True)

    def run(self) -> None:
        """Retrieve metadata for all files."""
        try:
            asyncio.run(self._loop())
        except ValueError as err:
            logger.critical("%s", err)
            sys.exit(1)

class QueryRetriever(MetaRetriever):
    """Class for retrieving metadata from MetaCat using an MQL query."""
    name = "metacat_query"

    def __init__(self, query: str):
        """
        Initialize the QueryRetriever with an MQL query.

        :param query: MQL query to find files
        """
        super().__init__()
        if 'skip' in query or 'limit' in query:
            logger.warning("Consider using command line options for 'skip' and 'limit'!")
        elif query.endswith(' ordered'):
            logger.info("Merge-Utils will append the 'ordered' keyword to queries automatically.")
        else:
            query += ' ordered'
        self.query = query

    async def get_metadata(self, batch: InputBatch, limit: int) -> list:
        """
        Asynchronously query MetaCat for a specific batch of files

        :param batch: InputBatch object with skip index set
        :param limit: maximum number of files to retrieve
        :return: list of file metadata dictionaries
        """
        query_batch = self.query + f" skip {batch.skip} limit {limit}"
        # In grandparents mode, we need the parents of the input files
        parents = bool(config.output.grandparents)
        # To check for already merged files, we need the children of the input files
        children = (config.validation.handling.already_done != 'include')
        # Query MetaCat, with provenance if necessary
        provenance = parents or children
        files = await self.client.query(query_batch, metadata = True, provenance = provenance)
        # In grandparents mode, we actually need the children of the parents
        if parents and children:
            await self.get_siblings(files)
        return files

class DidRetriever(MetaRetriever):
    """Class for retrieving metadata from MetaCat using a list of DIDs."""
    name: str = "metacat_dids"

    def __init__(self, dids: list, dupes: set = None):
        """
        Initialize the DidRetriever with a list of DIDs.

        :param dids: list of file DIDs to find
        :param dupes: set of indices of duplicate DIDs
        """
        super().__init__()
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
            ns = self.namespace
            logger.info("DID list missing namespaces, using default namespace '%s'", ns)
            self.dids = [f"{ns}:{did}" for did in self.dids]
        elif len(namespaces) == 1:
            ns, count = next(iter(namespaces.items()))
            if count < len(self.dids):
                logger.warning("Some DIDs missing namespaces, assuming shared namespace '%s'", ns)
                self.dids = [f"{ns}:{did}" if ':' not in did else did for did in self.dids]
        elif config.validation.handling.inconsistent == 'quit':
            io_utils.log_list("DID list contains multiple namespaces:",
                              namespaces, logging.CRITICAL)
            sys.exit(1)
        else:
            count = sum(namespaces.values())
            ns = self.namespace
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
        if dupes and config.validation.handling.duplicate == 'quit':
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
            placeholder = {
                'namespace': namespace,
                'name': name,
                'errors': MergeFileError.NO_METADATA
            }
            if skip+idx in self.dupes:
                logger.debug("Skipping duplicate DID: %s", did)
                placeholder['errors'] = MergeFileError.DUPLICATE
            else:
                query.append({'did': did})
                indices[did] = idx
            files.append(placeholder)
        if len(query) == 0:
            logger.debug("All DIDs in batch are duplicates, skipping MetaCat request")
            return files
        # Request files from MetaCat
        res = await self.get_files(query)
        # Add returned files to output list in correct order
        for file in res:
            files[indices[f"{file['namespace']}:{file['name']}"]] = file
        return files

class LocalMetaRetriever(MetaRetriever):
    """MetaRetriever for local files"""
    name = "local_meta"

    def __init__(self, paths: list):
        """
        Initialize the LocalMetaRetriever with a list of json files.

        :param paths: list of metadata file paths
        """
        super().__init__()
        self.paths = paths

    async def get_metadata(self, batch: InputBatch, limit: int) -> list:
        """
        Asynchronously retrieve metadata for a specific batch of files.

        :param batch: InputBatch object containing skip index and list of file names
        :param limit: maximum number of files to retrieve
        :return: list of file metadata dictionaries
        """
        files = []
        skip = batch.skip
        end = min(skip + limit, len(self.paths))
        namespace = self.namespace
        # Read metadata from local files if possible
        missing = {}
        for path in self.paths[skip:end]:
            name = os.path.basename(path).rsplit('.', 1)[0]
            metadata = io_utils.read_json(path)
            # If file is missing or unreadable, create a placeholder
            if not metadata:
                metadata = {
                    'namespace': namespace,
                    'name': name,
                    'errors': MergeFileError.NO_METADATA
                }
                missing[name] = len(files)
            files.append(metadata)
        # Make sure files exist in MetaCat, for parent listing
        if config.output.grandparents:
            await self.check_parents(files)
        else:
            await self.check_existence(files)
        # If we were missing any local files, try to find them in MetaCat
        if missing:
            io_utils.log_list("Checking MetaCat for {n} missing metadata file{s}:",
                              missing, logging.INFO)
            res = await self.get_files([{'did': f"{namespace}:{name}"} for name in missing])
            for file in res:
                files[missing[file['name']]] = file
        return files


def get() -> MetaRetriever:
    """
    Create and return a metadata retriever based on input mode:
    files: LocalMetaRetriever if any metadata files were provided, otherwise DidRetriever
    dids: DidRetriever
    query: QueryRetriever
    dataset: QueryRetriever with query for files in the specified dataset
    
    :return: MetaRetriever object for retrieving file metadata
    """
    # Determine input mode and retrieve metadata
    inputs = [str(f) for f in config.input.inputs]
    if config.input.mode == 'files':
        # We need to sort the input files into data and metadata files
        # Start by getting the set of all metadata file names (without the .json suffix)
        seen = set(os.path.basename(f)[:-5] for f in inputs if os.path.splitext(f)[1] == '.json')
        # Then go through the input list in order
        json_files = []
        for path in inputs:
            # If we have a JSON file, just add it to the list and continue
            if os.path.splitext(path)[1] == '.json':
                json_files.append(path)
                continue
            # Skip data files if we already have a metadata file with that name
            name = os.path.basename(path)
            if name in seen:
                continue
            seen.add(name)
            # Otherwise, try to find a matching metadata file in the same directory
            meta_path = path + '.json'
            if os.path.isfile(meta_path):
                json_files.append(meta_path)
                continue
            # Or in the provided search directories
            for search_dir in config.input.search_dirs:
                meta_path = os.path.join(search_dir, name + '.json')
                if os.path.isfile(meta_path):
                    json_files.append(meta_path)
                    break
        # If we found any metadata files, return a LocalMetaRetriever
        if len(json_files) > 0:
            return LocalMetaRetriever(paths=json_files)
        # Otherwise, return a DidRetriever based on the input file names
        return DidRetriever(dids=[os.path.basename(f) for f in inputs])
    if config.input.mode == 'dids':
        return DidRetriever(dids=inputs)
    if config.input.mode == 'query':
        if len(inputs) != 1:
            logger.critical("Multiple query inputs detected, did you forget to quote the query?")
            sys.exit(1)
        query = str(inputs[0])
        return QueryRetriever(query=query)
    if config.input.mode == 'dataset':
        if len(inputs) != 1:
            logger.critical("Dataset input mode currently only supports a single dataset name.")
            sys.exit(1)
        query = f"files from {inputs[0]} where dune.output_status=confirmed"
        return QueryRetriever(query=query)
    logger.critical("Unknown input mode: %s", config.input.mode)
    sys.exit(1)
