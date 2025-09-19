"""Utilities for running justin jobs"""
from __future__ import annotations

import csv
import requests

SITES_STORAGES_URL = "https://justin-ui-fnal.dune.hep.ac.uk/api/info/sites_storages.csv"

def get_distances(sites: list, rses: list) -> dict:
    """Get the distances between sites and storage elements"""
    fields = ['site', 'rse', 'dist', 'site_enabled', 'rse_read', 'rse_write']
    distances = {}
    r = requests.get(SITES_STORAGES_URL, verify=False, timeout=10)
    text = r.iter_lines(decode_unicode=True)
    reader = csv.DictReader(text, fields)
    for row in reader:
        if not row['site_enabled'] or not row['rse_read']:
            continue
        if row['site'] not in sites or row['rse'] not in rses:
            continue
        distances[(row['site'], row['rse'])] = float(row['dist'])
    return distances
