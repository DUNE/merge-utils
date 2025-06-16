"""Utility functions for interacting with the MetaCat web API."""

import logging
import itertools
import asyncio
import time
from typing import AsyncGenerator

import metacat.webapi as metacat

from merge_utils import config
from merge_utils.retriever import FileRetriever

logger = logging.getLogger(__name__)

class MetaCatRetriever(FileRetriever):
    """Class for managing asynchronous queries to the MetaCat web API."""

    def __init__(self, query: str = None, filelist: list = None):
        """
        Initialize the MetaCatRetriever with a query or a list of files.

        :param query: MQL query to find files
        :param filelist: list of file DIDs to find
        """
        super().__init__()

        self.query = query
        self.filelist = filelist
        self.parents = config.output['grandparents']
        if query and filelist:
            logger.warning("Both query and file list provided, was this intended?")
        if not self.filelist:
            self.filelist = []

        self.client = None

    async def connect(self) -> None:
        """Connect to the MetaCat web API"""
        if not self.client:
            logger.debug("Connecting to MetaCat")
            self.client = await asyncio.to_thread(metacat.MetaCatClient)
        else:
            logger.debug("Already connected to MetaCat")

    async def _get_files(self, idx: int, dids: list) -> list:
        """
        Asynchronously request file data from MetaCat
        
        :param idx: batch number (for logging)
        :param dids: list of DIDs to request
        :return: list of results from the request
        """
        if len(dids) == 0:
            return []
        logger.info("Retrieving files from MetaCat for batch %d", idx)
        dictlist = [{'did':did} for did in dids]
        logger.debug("Requesting dids:\n  %s", "\n  ".join([f['did'] for f in dictlist]))
        try:
            res = await asyncio.to_thread(self.client.get_files, dictlist,
                                          with_metadata = True, with_provenance = self.parents)
        except (ValueError, metacat.webapi.BadRequestError) as err:
            logger.critical("%s", err)
            raise ValueError(f"Failed to retrieve files from MetaCat for batch {idx}: {err}") from err
        return list(res)

    async def _get_query(self, idx: int) -> list:
        """
        Asynchronously query MetaCat
        
        :param idx: batch number to query
        :return: list of results from the query
        """
        if not self.query:
            return []
        logger.info("Querying MetaCat for batch %d", idx)
        query_batch = self.query + f" skip {idx*self.step} limit {self.step}"
        logger.debug("Query: %s", query_batch)
        try:
            # async_query exists but does not seem to be compatible with asyncIO
            res = await asyncio.to_thread(self.client.query, query_batch,
                                          with_metadata = True, with_provenance = self.parents)
        except metacat.webapi.BadRequestError as err:
            logger.critical("Malformed MetaCat query:\n  %s\n%s", self.query, err)
            raise ValueError(f"Failed to query MetaCat for batch {idx}: {err}") from err
        return list(res)

    async def input_batches(self) -> AsyncGenerator[dict, None]:
        """
        Asynchronously retrieve metadata for the next batch of files.

        :return: dict of MergeFile objects that were added
        """
        # request first batch from filelist
        dids = self.filelist[0:self.step]
        task = asyncio.create_task(self._get_files(0, dids))
        # loop over batches from filelist
        for idx in range(1, len(self.filelist)//self.step + 1):
            old_dids = dids
            dids = self.filelist[idx*self.step:(idx+1)*self.step]
            res = await task
            task = asyncio.create_task(self._get_files(idx, dids))
            added = await self.add(res, old_dids)
            logger.debug("yielding file batch %d", idx-1)
            yield added
        res = await task
        # request first batch from query
        task = asyncio.create_task(self._get_query(0))
        # finish processing last batch from filelist
        if dids:
            added = await self.add(res, dids)
            logger.debug("yielding last file batch")
            yield added
        # loop over batches from query
        for idx in itertools.count(1):
            res = await task
            if len(res) < self.step:
                break
            task = asyncio.create_task(self._get_query(idx))
            added = await self.add(res)
            logger.debug("yielding query batch %d", idx-1)
            yield added
        # yield last partial batch from query
        if res:
            added = await self.add(res)
            logger.debug("yielding last query batch")
            yield added

def list_field_values(field: str) -> list:
    """
    Get a list of all values for a given field in the MetaCat database.

    :param field: field to query
    :return: list of values for the field
    """
    client = metacat.MetaCatClient()
    query = f"files where {field} present limit 1"
    values = []
    while True:
        res = client.query(query, with_metadata=True)
        data = next(res, None)
        if not data:
            break
        value = data['metadata'][field]
        print(value)
        values.append(value)
        query = f"""files where {field} present and {field} not in ('{"','".join(values)}') limit 1"""
        #time.sleep(1)
    return values

def list_extensions() -> list:
    """
    Get a list of all file extensions in the MetaCat database.

    :return: list of file extensions
    """
    client = metacat.MetaCatClient()
    query = "files where name ~ '\\.[a-z]' limit 1"
    and_name = "' and name !~ '\\."
    values = []
    while True:
        res = client.query(query, with_metadata=False)
        data = next(res, None)
        if not data:
            break
        ext = data['name'].split('.')[-1]
        print(data['namespace']+":"+data['name'])
        values.append(ext)
        query = f"files where name ~ '\\.[a-z]{and_name}{and_name.join(values)}' limit 1"
    return values
