"""Utility functions for interacting with the JustIN web API."""

import logging
import csv
import asyncio
import requests

from merge_utils import config

logger = logging.getLogger(__name__)

SITE_STORAGE_URL = "/api/info/sites_storages.csv"

async def get_site_rse_distances(valid_rses: set = None) -> dict:
    """
    Retrieve site-RSE distances from the JustIN web API.

    :param valid_rses: set of valid RSE names to consider
    :return: dictionary of distances for each site-rse pair
    """
    # Query JustIN for site-RSE distances
    try:
        res = await asyncio.to_thread(
            requests.get, config.sites.justin_url+SITE_STORAGE_URL, verify=False, timeout=10
        )
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
    default_site_dist = config.sites.site_distances['default']
    default_rse_dist = config.sites.rse_distances['default']
    for row in reader:
        if not row['site_enabled'] or not row['rse_read']:
            continue
        # Get site distance offset
        site = row['site']
        site_dist = config.sites.site_distances.get(site, default_site_dist)
        if site_dist > config.sites.max_distance:
            continue
        # Get RSE distance offset
        rse = row['rse']
        if valid_rses is not None and rse not in valid_rses:
            continue
        rse_dist = config.sites.rse_distances.get(rse, default_rse_dist)
        if rse_dist > config.sites.max_distance:
            continue
        # Compute total distance
        dist = 100*float(row['dist']) + site_dist + rse_dist
        distances[(site, rse)] = dist
    return distances
