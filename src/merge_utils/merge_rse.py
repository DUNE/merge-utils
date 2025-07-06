"""Keep track of RSEs where files are stored"""

import os
import logging
import collections
import itertools
import math
import subprocess
import csv
import asyncio
from typing import Iterable

import requests

from rucio.client.rseclient import RSEClient

from merge_utils import config

logger = logging.getLogger(__name__)

SITES_STORAGES_URL = "https://justin-ui-pro.dune.hep.ac.uk/api/info/sites_storages.csv"
LOCAL_PING_THRESHOLD = 5

def check_path(path: str) -> str:
    """
    Check the status of a file path.

    :param path: file path to check.  Can be a local file or a remote URL.
    :return: status of the file ('ONLINE', 'NEARLINE', 'NONEXISTENT', or 'UNKNOWN')
    """
    if "://" in path:
        # remote site
        cmd = ['gfal-xattr', path, 'user.status']
        ret = subprocess.run(cmd, capture_output=True, text=True, check=False)
        status = ret.stdout.strip()

        # special case for FNAL DCACHE
        fnal_prefix = "root://fndca1.fnal.gov:1094/pnfs/fnal.gov/usr"
        if status=='UNKNOWN' and path.startswith(fnal_prefix):
            local_path = path.replace(fnal_prefix, "/pnfs")
            if not os.path.exists(local_path):
                logger.info("Got UNKNOWN status for %s, assuming NEARLINE", path)
                return 'NEARLINE'
            directory, filename = os.path.split(local_path)
            stat_file=f"{directory}/.(get)({filename})(locality)"
            with open(stat_file, encoding="utf-8") as stats:
                status = stats.readline().strip()
    else:
        # local site
        if not os.path.exists(path):
            logger.warning("File %s not found!", path)
            return 'NONEXISTENT'

        # special case for FNAL DCACHE
        path = os.path.realpath(path)
        directory, filename = os.path.split(path)
        stat_file=f"{directory}/.(get)({filename})(locality)"
        if not os.path.exists(stat_file):
            # normal file not in DCACHE?
            return 'ONLINE'

        with open(stat_file, encoding="utf-8") as stats:
            status = stats.readline().strip()

    # status can be 'ONLINE AND NEARLINE', just return one or the other
    if 'ONLINE' in status:
        return 'ONLINE'
    if 'NEARLINE' in status:
        return 'NEARLINE'
    logger.warning("File %s status is %s", path, status)
    return status

class MergeRSE:
    """Class to store information about an RSE"""
    def __init__(self, valid: bool, nearline_dist: float = 0):
        self.valid = valid
        self.disk = {}
        self.tape = {}
        self.distances = {}
        self.nearline_dist = nearline_dist
        self.ping = float('inf')

    @property
    def pfns(self) -> dict:
        """Get the set of PFNs for this RSE"""
        return collections.ChainMap(self.disk, self.tape)

    def get_ping(self) -> float:
        """Get the ping time to the RSE in ms"""
        if len(self.disk) > 0:
            pfn = next(iter(self.disk.values()))
        elif len(self.tape) > 0:
            pfn = next(iter(self.tape.values()))
        else:
            return float('inf')
        url = pfn.split(':', 2)[1][2:] # extract host from 'protocol://host:port/path'
        cmd = ['ping', '-c', '1', url]
        ret = subprocess.run(cmd, capture_output=True, check=False)
        if ret.returncode == 0:
            self.ping = float(ret.stdout.split(b'/')[-2]) # average ping time
            logger.debug("Pinged %s, t = %d ms", url, self.ping)
        else:
            logger.warning("Failed to ping %s", url)
        return self.ping

    def distance(self, site: str) -> float:
        """Get the distance to a site"""
        return self.distances.get(site, float('inf'))

    def nearest_site(self, sites: list = None) -> tuple:
        """
        Get the nearest merging site to this RSE
        
        :param sites: list of merging sites to consider (default: all sites)
        :return: tuple of (site, distance)
        """
        if len(self.distances) == 0:
            return None, float('inf')
        if sites is None:
            sites = self.distances.keys()
        site = min(sites, key=self.distance)
        distance = self.distance(site)
        if distance == float('inf'):
            return None, float('inf')
        return site, distance

class MergeRSEs(collections.UserDict):
    """Class to keep track of a set of RSEs"""
    def __init__(self):
        super().__init__()
        self.sites = config.sites['allowed_sites']
        self.max_distance = config.sites['max_distance']
        self.nearline_distances = config.sites['nearline_distance']

        self.disk = set()
        self.tape = set()

    async def connect(self) -> None:
        """Download the RSE list from Rucio and determine their distances"""
        # Get the list of RSEs from Rucio
        rse_client = RSEClient()
        rses = await asyncio.to_thread(rse_client.list_rses)
        for rse in rses:
            valid = (
                rse['availability_read']
                and not rse['staging_area']
                and not rse['deleted']
            )
            if rse['rse'] in self.nearline_distances:
                nearline_dist = self.nearline_distances[rse['rse']]
            else:
                nearline_dist = self.nearline_distances['default']
            self[rse['rse']] = MergeRSE(valid, nearline_dist)

        # Get site distances from justIN web API
        fields = ['site', 'rse', 'dist', 'site_enabled', 'rse_read', 'rse_write']
        res = await asyncio.to_thread(requests.get, SITES_STORAGES_URL, verify=False, timeout=10)
        text = res.iter_lines(decode_unicode=True)
        reader = csv.DictReader(text, fields)
        for row in reader:
            if not row['site_enabled'] or not row['rse_read']:
                continue
            site = row['site']
            rse = row['rse']
            if site not in self.sites or rse not in self or not self[rse].valid:
                continue
            self[rse].distances[site] = 100*float(row['dist'])
        # hard wire obvious options for when justin is down
        self["DUNE_US_FNAL_DISK_STAGE"].distances["US_FNAL-FermiGrid"]=0
        n_accessible = sum(1 for rse in self.values() if rse.valid
                           and rse.nearest_site()[1] <= self.max_distance)
        logger.info("Found %d RSEs accessible from %d sites", n_accessible, len(self.sites))

    async def add_pfn(self, did: str, pfn: str, info: dict) -> float:
        """Add a file PFN to the corresponding RSE
        
        :param did: file DID
        :param pfn: physical file name
        :param info: dictionary with RSE information
        :return: distance from the RSE to the nearest site
        """
        # Check if the RSE is valid
        rse = info['rse']
        if rse not in self or not self[rse].valid:
            logger.warning("RSE %s does not exist?", rse)
            return float('inf')
        if not self[rse].valid:
            logger.warning("RSE %s is invalid", rse)
            return float('inf')
        _, dist = self[rse].nearest_site()
        if dist > self.max_distance:
            logger.warning("RSE %s is too far from merging sites (%s > %s)",
                           rse, dist, self.max_distance)
            return dist

        # Check file status
        if info['type'] == 'DISK':
            status = 'ONLINE'
        else:
            status = await asyncio.to_thread(check_path, pfn)

        # Actually add the file to the RSE
        if status == 'ONLINE':
            self.disk.add(did)
            self.data[rse].disk[did] = pfn
        elif status == 'NEARLINE':
            dist += self.data[rse].nearline_dist
            if dist > self.max_distance:
                logger.warning("RSE %s (tape) is too far from merging sites (%s > %s)",
                               rse, dist, self.max_distance)
            else:
                self.tape.add(did)
                self.data[rse].tape[did] = pfn
        else:
            # File is not accessible
            return float('inf')

        return dist

    async def add_replicas(self, did: str, replicas: dict) -> int:
        """Add a set of file replicas to the RSEs
        
        :param did: file DID
        :param replicas: Rucio replica dictionary
        :return: number of PFNs added
        """
        tasks = [self.add_pfn(did, pfn, info) for pfn, info in replicas['pfns'].items()]
        results = await asyncio.gather(*tasks)
        best_dist = min(results)
        if best_dist > self.max_distance:
            if best_dist == float('inf'):
                logger.error("Could not retrieve file %s from Rucio", did)
                raise ValueError("File not found")
            logger.error("File %s is too far from merging sites (%s > %s)",
                         did, best_dist, self.max_distance)
            raise ValueError("File exceeds max distance")
        return sum(1 for dist in results if dist <= self.max_distance)

    def set_rse_sites(self) -> None:
        """Query Rucio for the site associated with each RSE"""
        rse_client = RSEClient()
        for name, rse in self.items():
            attr = rse_client.list_rse_attributes(name)
            rse.site = attr['site']

    def cleanup(self) -> None:
        """Remove RSEs with no files"""
        # Remove RSEs with no files
        self.data = {k: v for k, v in self.items() if len(v.disk) > 0 or len(v.tape) > 0}

        # Log how many files we found
        msg = [""]
        for name, rse in sorted(self.items(), key=lambda x: len(x[1].disk), reverse=True):
            msg.append(f"\n  {name}: {len(rse.disk)} ({len(rse.tape)}) files")
        n_files = len(self.disk)
        n_tape = len(self.tape - self.disk)
        n_rses = len(self.items())
        s_files = "s" if n_files != 1 else ""
        s_rses = "s" if n_rses != 1 else ""
        msg[0] = (f"Found {n_files} ONLINE ({n_tape} NEARLINE) file{s_files} "
                  f"from {n_rses} RSE{s_rses}:")
        logger.info("".join(msg))

    def site_pfns(self, site: str, files: Iterable) -> dict:
        """
        Find the file replicas with the shortest distance to a given merging site.
        
        :param site: merging site name
        :param files: collection of files to process
        :return: dictionary of PFNs for the site
        """
        pfns = {}
        for did in files:
            pfns[did] = (None, float('inf'))

        for name, rse in self.items():
            distance = rse.distance(site)
            if distance > self.max_distance:
                logger.info("RSE %s is too far away from site %s (%s > %s)",
                            name, site, distance, self.max_distance)
                continue
            for did, pfn in rse.disk.items():
                if did not in files:
                    continue
                if distance < pfns[did][1]:
                    pfns[did] = (pfn, distance)

            distance += rse.nearline_dist
            if distance > self.max_distance:
                logger.info("RSE %s (tape) is too far away from site %s (%s > %s)",
                            name, site, distance, self.max_distance)
                continue
            for did, pfn in rse.tape.items():
                if did not in files:
                    continue
                if distance < pfns[did][1]:
                    pfns[did] = (pfn, distance)
        return pfns

    def optimize_pfns(self, pfns: dict, files: Iterable, sites: list) -> dict:
        """
        Find the best PFNs for a set of sites.
        
        :param pfns: dictionary of PFNs for all sites
        :param files: collection of files to process
        :param sites: list of sites to consider
        :return: dictionary of best PFNs for each site
        """
        if len(sites) == 1:
            return {sites[0]: pfns[sites[0]]}
        best_pfns = {site: {} for site in sites}

        chunk_max = config.merging['chunk_max']
        def chunk_err(counts):
            """Calculate how far we are from optimal chunk sizes"""
            total_err = 0
            for count in counts:
                if count == 0:
                    continue
                n_chunks = count / chunk_max
                err = (math.ceil(n_chunks) - n_chunks) / math.ceil(n_chunks)
                total_err += err**2
            return total_err

        def delta(did) -> float:
            """Calculate the difference between the best and second-best distances"""
            dists = [pfns[site][did][1] for site in sites]
            dists.sort()
            return dists[1] - dists[0]

        # Add files with largest distance difference first
        for did in sorted(files, key=delta, reverse=True):
            # Break ties by trying to optimize chunk sizes
            counts = [len(best_pfns[site]) for site in best_pfns]
            err = chunk_err(counts)
            best_site = None
            best_priority = float('inf')
            for i, site in enumerate(best_pfns):
                priority = pfns[site][did][1] \
                         + chunk_err(counts[:i] + [counts[i] + 1] + counts[i+1:]) - err
                if priority < best_priority:
                    best_priority = priority
                    best_site = site
            best_pfns[best_site][did] = pfns[best_site][did]

        return best_pfns

    def get_pfns(self, files: Iterable = None) -> dict:
        """Determine the best RSE for each file"""
        if files is None:
            files = self.disk | self.tape
        pfns = {site: self.site_pfns(site, files) for site in self.sites}
        if len(self.sites) == 1:
            return pfns

        # Find a minimal set of sites with access to all files
        for n_sites in range(1, len(self.sites) + 1):
            best_sites = (None,) * n_sites
            best_distance = float('inf')
            for sites in itertools.combinations(self.sites, n_sites):
                total_distance = 0
                for did in files:
                    total_distance += min(pfns[site][did][1] for site in sites)
                if total_distance < best_distance:
                    best_distance = total_distance
                    best_sites = sites
            if best_distance < float('inf'):
                break
        if n_sites == 1:
            logger.info("Site %s has the shortest distance to all files", best_sites[0])
        elif n_sites < len(self.sites):
            logger.debug("Sites %s have the shortest distance to all files", (best_sites,))
        else:
            logger.debug("All sites required to access all files")

        # Collect the PFNs for the best sites
        best_pfns = self.optimize_pfns(pfns, files, best_sites)
        #if max(len(best_pfns[site]) for site in best_pfns) <= config.merging['chunk_max']:
        #    return best_pfns

        # See if we can do better with more sites?
        #for n_sites in range(n_sites + 1, len(self.sites) + 1):
        return best_pfns
