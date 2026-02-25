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

        :param files: list of file dicts, with either 'fid', 'did', or 'namespace' & 'name' keys
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
