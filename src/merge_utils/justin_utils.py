"""Utility functions for interacting with the JustIN web API."""

import logging
import csv
import asyncio
import requests

from merge_utils import config

logger = logging.getLogger(__name__)

SITE_STORAGE_URL = "/api/info/sites_storages.csv"

async def get_site_rse_distances() -> dict:
    """
    Retrieve site-RSE distances from the JustIN web API.
    Adds site distance offsets from the config
    Does NOT add RSE distance offsets, since those are already accounted for by the PathFinder

    :return: dictionary of {rse: {site: distance}} for all reachable site-RSE pairs
    """
    # Query JustIN for site-RSE distances
    full_url = str(config.sites.justin_url) + SITE_STORAGE_URL
    try:
        res = await asyncio.to_thread(requests.get, full_url, verify=False, timeout=60)
        connected = res.ok
    except requests.ConnectionError as err:
        logger.error("JustIN connection error: %s", err)
        connected = False
    if not connected:
        return {}
    # Parse the CSV response
    distances = {}
    text = res.iter_lines(decode_unicode=True)
    fields = ['site', 'rse', 'dist', 'site_enabled', 'rse_read', 'rse_write']
    reader = csv.DictReader(text, fields)
    default_dist = config.sites.site_distances['default']
    for row in reader:
        # Skip disabled sites and RSEs with no read/write access
        if not row['site_enabled']:
            continue
        if not row['rse_read'] and not row['rse_write']:
            continue
        # Get site distance offset
        site = row['site']
        site_dist = config.sites.site_distances.get(site, default_dist)
        if site_dist > config.sites.max_distance:
            continue
        # Get total distance
        distance = 100*float(row['dist']) + site_dist
        rse = row['rse']
        if rse in distances:
            distances[rse][site] = distance
        else:
            distances[rse] = {site: distance}
    return distances
