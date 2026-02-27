"""Utility functions for interacting with the Rucio web API."""
from __future__ import annotations

import logging
import asyncio
from typing import AsyncGenerator

logger = logging.getLogger(__name__)

try:
    from rucio.client import Client #type: ignore pylint: disable=import-error
    #from rucio.client.replicaclient import ReplicaClient #type: ignore pylint: disable=import-error
    #from rucio.client.rseclient import RSEClient #type: ignore pylint: disable=import-error
    HAS_RUCIO = True
except ImportError:
    logger.warning("Failed to import Rucio client, Rucio functionality will be unavailable!")
    HAS_RUCIO = False

#from merge_utils import io_utils, config

class RucioWrapper:
    """Class for sending asynchronous requests to the Rucio web API."""

    def __init__(self):
        """Initialize the RucioWrapper."""
        self.client = None
        self.rses = {}

    def __bool__(self) -> bool:
        """Return True if the Rucio client is connected."""
        return self.client is not None

    async def connect(self) -> None:
        """Connect to the Rucio web API"""
        if not HAS_RUCIO:
            logger.warning("Rucio client is not available!")
        elif not self.client:
            logger.debug("Connecting to Rucio")
            try:
                self.client = await asyncio.to_thread(Client)
            except Exception as e:
                logger.warning("Failed to connect to Rucio: %s", e)
                self.client = None
        else:
            logger.debug("Already connected to Rucio")

    async def get_rse(self, name: str) -> dict:
        """
        Asynchronously retrieve information for a specific RSEfrom Rucio.

        :param name: name of the RSE to retrieve
        :return: dictionary of RSE attributes
        """
        # Check if we already have information about this RSE cached
        if name in self.rses:
            return self.rses[name]
        rse = await asyncio.to_thread(self.client.get_rse, name)
        rse['attrs'] = await asyncio.to_thread(self.client.list_rse_attributes, name)
        self.rses[name] = rse
        return rse

    async def get_rses(self, detailed: bool = False) -> AsyncGenerator[dict, None]:
        """
        Asynchronously retrieve information for all RSEs from Rucio.

        :param detailed: whether to include detailed RSE information
        :return: dictionary of RSE attributes for each RSE
        """
        for rse in await asyncio.to_thread(self.client.list_rses):
            name = rse['rse']
            if rse['deleted']:
                continue
            if detailed:
                details = await asyncio.to_thread(self.client.get_rse, name)
                for key, value in details.items():
                    if key in rse and rse[key] != value:
                        logger.warning("RSE %s has conflicting values for %s: %s != %s",
                                    name, key, rse[key], value)
                    rse[key] = value
            rse['attrs'] = await asyncio.to_thread(self.client.list_rse_attributes, name)
            self.rses[name] = rse
            yield rse

    async def get_replicas(self, files: list) -> list:
        """
        Asynchronously retrieve replicas for a specific batch of files.

        :param files: list of files to retrieve paths for
        :return: list of file path dictionaries
        """
        query = [{'scope':f.namespace, 'name':f.name} for f in files]
        res = await asyncio.to_thread(self.client.list_replicas, query,
                                      ignore_availability=False)
        return list(res)


# Example RSE info from FNAL_DCACHE, as of February 2026

FNAL_DCACHE = {
  "rse_type": "TAPE",
  "time_zone": None,
  "availability_write": True,
  "deterministic": False,
  "ISP": None,
  "availability_delete": False,
  "volatile": False,
  "ASN": None,
  "qos_class": None,
  "id": "a9780baae23e48359e2a84d3b19261ae",
  "staging_area": False,
  "longitude": None,
  "deleted": False,
  "city": None,
  "latitude": None,
  "deleted_at": None,
  "region_code": None,
  "availability": 6,
  "created_at": "2018-08-07 18:23:04",
  "vo": "def",
  "country_name": "US",
  "availability_read": True,
  "updated_at": "2025-05-08 12:59:30",
  "continent": None
}

FNAL_DCACHE_DETAILS = {
  "credentials": None,
  "domain": [
    "lan",
    "wan"
  ],
  "lfn2pfn_algorithm": "DUNE",
  "protocols": [
    {
      "hostname": "fndcadoor.fnal.gov",
      "scheme": "davs",
      "port": 2880,
      "prefix": "/dune/tape_backed/dunepro",
      "impl": "rucio.rse.protocols.gfal.Default",
      "domains": {
        "lan": {
          "read": 2,
          "write": 1,
          "delete": 1
        },
        "wan": {
          "read": 2,
          "write": 1,
          "delete": 1,
          "third_party_copy_read": 1,
          "third_party_copy_write": 1
        }
      },
      "extended_attributes": None
    },
    {
      "hostname": "fndcadoor.fnal.gov",
      "scheme": "root",
      "port": 1094,
      "prefix": "/pnfs/fnal.gov/usr/dune/tape_backed/dunepro",
      "impl": "rucio.rse.protocols.gfal.Default",
      "domains": {
        "lan": {
          "read": 1,
          "write": 2,
          "delete": 2
        },
        "wan": {
          "read": 1,
          "write": 2,
          "delete": 2,
          "third_party_copy_read": 0,
          "third_party_copy_write": 0
        }
      },
      "extended_attributes": None
    }
  ],
  "sign_url": None,
  "verify_checksum": True,
  "read_protocol": 1,
  "write_protocol": 1,
  "delete_protocol": 1,
  "third_party_copy_read_protocol": 1,
  "third_party_copy_write_protocol": 1,
}

FNAL_DCACHE_ATTRIBUTES = {
    'FNAL_DCACHE': True,
    'US_SITES': True,
    'country': 'US',
    'country_name': 'US',
    'fts': 'https://fts3-public.cern.ch:8446',
    'istape': True,
    'naming_convention': 'DUNE_metacat',
    'site': 'US_FNAL-FermiGrid',
    'srr_url': 'https://fndca.fnal.gov/FNAL-WLCG-tape-statistics.json',
    'staging_buffer': 'FNAL_DCACHE_STAGING'
}
