"""Utility functions for interacting with the MetaCat web API."""

import logging
import sys
import asyncio

import metacat.webapi as metacat #pylint: disable=import-error

#from merge_utils import config

logger = logging.getLogger(__name__)

class MetaCatWrapper:
    """Class for sending asynchronous requests to the MetaCat web API."""

    def __init__(self):
        """Initialize the MetaCatWrapper."""
        self.client = None

    async def connect(self) -> None:
        """Connect to the MetaCat web API"""
        if not self.client:
            logger.debug("Connecting to MetaCat")
            self.client = await asyncio.to_thread(metacat.MetaCatClient)
        else:
            logger.debug("Already connected to MetaCat")

    async def query(self, query: str, metadata: bool = True, provenance: bool = True) -> list:
        """
        Asynchronously query MetaCat.

        :param query: MQL query to execute
        :param metadata: whether to include metadata in the results
        :param provenance: whether to include provenance in the results
        :return: list of file metadata dictionaries
        """
        try:
            res = await asyncio.to_thread(self.client.query, query,
                                          with_metadata = metadata,
                                          with_provenance = provenance)
        except metacat.webapi.BadRequestError as err:
            logger.critical("Malformed MetaCat query:\n  %s\n%s", query, err)
            sys.exit(1)
        return list(res)

    async def files(self, files: list, metadata: bool = True, provenance: bool = True) -> list:
        """
        Asynchronously request a list of DIDs from MetaCat

        :param files: list of files to find
        :param metadata: whether to include metadata in the results
        :param provenance: whether to include provenance in the results
        :return: list of file metadata dictionaries
        """
        if len(files) == 0:
            logger.debug("No files to request")
            return []
        try:
            res = await asyncio.to_thread(self.client.get_files, files,
                                          with_metadata = metadata,
                                          with_provenance = provenance)
        except (ValueError, metacat.webapi.BadRequestError) as err:
            logger.critical("%s", err)
            raise ValueError(f"MetaCat error: {err}") from err
        return list(res)

def list_field_values(field: str, date: str = None, vals: list = None) -> list:
    """
    Get a list of all values for a given field in the MetaCat database.

    :param field: field to query
    :param date: limit results to those created after this date (yyyy-mm-dd)
    :return: list of values for the field
    """
    client = metacat.MetaCatClient()
    vals = vals or []
    while True:
        query = f"files where {field} present"
        if date:
            query = f"{query} and created_timestamp > '{date}'"
        if vals:
            query = f"{query} and {field} not in ('{"','".join(vals)}')"
        res = client.query(f"{query} limit 1", with_metadata=True)
        data = next(res, None)
        if not data:
            break
        value = data['metadata'][field]
        print(value)
        vals.append(value)
        #time.sleep(1)
    return vals

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
