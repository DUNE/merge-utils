"""Tools for working with paths and xrootd URLs."""

import os
import sys
import subprocess
import logging
import enum
import asyncio
import collections
from dataclasses import dataclass
from typing import AsyncGenerator
from abc import ABC, abstractmethod

from merge_utils import io_utils, config
from merge_utils.merge_set import MergeSet, MergeFile
from merge_utils.retriever import MetaRetriever, InputBatch
from merge_utils.rucio_utils import RucioWrapper

logger = logging.getLogger(__name__)

# Utility functions for extracting components from URLs

def get_protocol(url: str) -> str:
    """
    Extract the protocol from a URL or local file path
    
    :param url: URL of the form 'protocol://host:port/path', or a local file path
    :return: protocol from URL, or 'file' if it is a local file path
    """
    if "://" in url:
        return url.split("://", 1)[0]
    return "file"

def get_host(url: str) -> str:
    """
    Extract the host from a URL or local file path
    
    :param url: URL of the form 'protocol://host:port/path', or a local file path
    :return: host from URL, or 'local' if it is a local file path
    """
    if "://" in url:
        return url.split('/', 3)[2].split(':')[0]
    return "local"

def get_port(url: str) -> int:
    """
    Extract the port from a URL or local file path
    
    :param url: URL of the form 'protocol://host:port/path', or a local file path
    :return: port from URL, or None if it is a local file path or no port is specified
    """
    if "://" in url:
        parts = url.split('/', 3)[2].split(':')
        if len(parts) > 1:
            return int(parts[1])
    return None

def get_path(url: str) -> str:
    """
    Extract the path from a URL or local file path
    
    :param url: URL of the form 'protocol://host:port/path', or a local file path
    :return: path from URL, or the original string if it is a local file path
    """
    if "://" in url:
        return '/' + url.split('/', 3)[3]
    return url

# Utility functions for converting between local paths and xrootd URLs

def path_to_xrootd(path: str) -> str:
    """
    Convert a local file path to an xrootd URL

    :param path: local file path
    :return: xrootd URL corresponding to the local file path, or None if conversion fails
    """
    if not config.local.site:
        return None
    urls = config.local.xrootd[str(config.local.site)]
    for url_prefix, path_prefix in sorted(urls.items(), key=lambda x: len(x[0]), reverse=True):
        path_prefix = str(path_prefix)
        if path.startswith(path_prefix):
            return path.replace(path_prefix, url_prefix, 1)
    return None

def xrootd_to_path(url: str) -> str:
    """
    Convert an xrootd URL to a local file path

    :param url: xrootd URL
    :return: local file path corresponding to the xrootd URL, or None if conversion fails
    """
    if not config.local.site:
        return None
    urls = config.local.xrootd[str(config.local.site)]
    for url_prefix, path_prefix in sorted(urls.items(), key=lambda x: len(x[0]), reverse=True):
        path_prefix = str(path_prefix)
        if url.startswith(url_prefix):
            return url.replace(url_prefix, path_prefix, 1)
    return None

# Classes for representing file replicas and their statuses

class StatusMeta(enum.EnumMeta):
    """Metaclass for Status enum to allow special handling of 'ONLINE AND NEARLINE' status."""
    def __getitem__(cls, key):
        # Special case for ONLINE AND NEARLINE, which should be treated as ONLINE
        if 'ONLINE' in key:
            return cls.ONLINE
        # Otherwise, get the value normally
        try:
            return super().__getitem__(key)
        except KeyError as err:
            raise KeyError(f"Unknown file status: {key}") from err

class Status(enum.Enum, metaclass=StatusMeta):
    """Enumeration of possible file statuses"""
    ONLINE          = enum.auto()
    NEARLINE        = enum.auto()
    OFFLINE         = enum.auto()
    UNKNOWN         = enum.auto()
    MISSING         = enum.auto()
    BAD_SIZE        = enum.auto()
    BAD_CHECKSUM    = enum.auto()
    BAD_PROTOCOL    = enum.auto()
    UNREACHABLE     = enum.auto()

    @property
    def good(self) -> bool:
        """Return True if this status indicates a good file replica"""
        return self in {Status.ONLINE, Status.NEARLINE}

    @property
    def bad(self) -> bool:
        """Return True if this status indicates a bad file replica"""
        return not self.good

@dataclass
class Replica:
    """Class representing a file replica, including its path and status."""
    path: str
    rse: 'BaseRSE' = None
    status: Status = Status.UNREACHABLE # Assume unreachable until we can check otherwise
    distance: float = float('inf')

    @property
    def protocol(self) -> str:
        """Get the protocol of the replica's path"""
        return get_protocol(self.path)

# Classes for representing RSEs and checking the status of replicas on those RSEs

class BaseRSE(ABC):
    """Base class for an RSE, which can be used to check the status of file replicas"""

    def __init__(self):
        self.name = None
        self.urls = {}
        self.distance = float('inf')
        self.disk = None
        self.staging = None
        self.read = True
        self.write = True

    def ping(self) -> float:
        """Get the ping time to the RSE in ms"""
        if len(self.urls) == 0:
            logger.warning("No URLs found for RSE %s, cannot ping", self.name)
            return float('inf')
        if 'file' in self.urls:
            logger.debug("RSE %s is local, skipping ping", self.name)
            return 0.0
        # Try pinging the URLs
        best_ping = float('inf')
        hosts = set(get_host(url) for url in self.urls.values())
        for host in hosts:
            cmd = ['ping', '-c', '1', host]
            ret = subprocess.run(cmd, capture_output=True, check=False)
            if ret.returncode != 0:
                logger.debug("Failed to ping %s", host)
                continue
            ping = float(ret.stdout.split()[-2].split(b'/')[0]) # min ping time
            logger.debug("Pinged %s, t = %.1f ms", host, ping)
            best_ping = min(best_ping, ping)
        if best_ping != float('inf'):
            logger.info("Best ping to RSE %s is %.1f ms", self.name, best_ping)
            return best_ping
        # If we get here, all pings failed
        logger.warning("Failed to ping any URLs for RSE %s", self.name)
        return float('inf')

    async def xrdfs(self, replica: Replica, cmd: str, timeout: float = 1) -> str:
        """
        Run an xrdfs command on a replica and return the output
        In case of failure, set the replica status accordingly and return None

        :param replica: Replica object to check
        :param cmd: xrdfs command to run (e.g. 'ls -l')
        :param timeout: timeout for the xrdfs command in seconds
        :return: stdout of the xrdfs command, or None if it failed
        """
        protocol = replica.protocol
        if protocol != 'root':
            raise ValueError(f"Unsupported protocol for xrdfs: {protocol}")
        host = get_host(replica.path)
        port = get_port(replica.path)
        path = get_path(replica.path)
        url = f"{protocol}://{host}:{port}"
        full_cmd = ['xrdfs', url] + cmd.split() + [path]
        try:
            ret = await asyncio.to_thread(subprocess.run, full_cmd, capture_output=True,
                                          text=True, check=False, timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.debug("Timeout accessing xrootd server %s", host)
            replica.status = Status.UNREACHABLE
            return None
        if ret.returncode != 0:
            replica.status = Status.UNREACHABLE
            if ret.returncode == 51:
                logger.debug("Invalid xrootd server %s", host)
            elif ret.returncode == 52:
                logger.debug("Auth failed for xrootd server %s", host)
            elif ret.returncode == 54:
                logger.debug("No such file %s", replica.path)
                replica.status = Status.MISSING
            elif ret.returncode != 0:
                logger.debug("Failed to access %s\n  %s", replica.path, ret.stderr.strip())
            return None
        return ret.stdout.strip()

    async def checksum_xrootd(self, replica: Replica, cksums: dict) -> bool:
        """
        Check the checksums of a remote file against expected values
        
        :param replica: Replica object to check
        :param cksums: dict of {algorithm: expected_checksum} pairs to check against
        :return: True if any matching checksums are found, False otherwise
        """
        xrdfs_cksums = await self.xrdfs(replica, 'query checksum')
        if xrdfs_cksums is None:
            logger.warning("Failed to get checksums for file %s", replica.path)
            return False
        unknown_algos = set()
        for line in xrdfs_cksums.splitlines():
            algo, cksum = line.split()
            if algo not in cksums:
                unknown_algos.add(algo)
                continue
            if cksum == cksums[algo]:
                logger.debug("Checksum %s matches for file %s", algo, replica.path)
                return True
            logger.warning("File %s has bad %s checksum: %s != %s",
                            replica.path, algo, cksum, cksums[algo])
            return False
        if unknown_algos:
            logger.info("File %s has checksums for unknown algorithms: %s",
                        replica.path, unknown_algos)
        logger.warning("Checksum failed for file %s", replica.path)
        return False

    async def checksum_local(self, path: str, cksums: dict) -> bool:
        """
        Check the checksums of a local file against expected values
        
        :param path: path to the local file
        :param cksums: dict of {algorithm: expected_checksum} pairs to check against
        :return: True if any matching checksums are found, False otherwise
        """
        algorithms = [
            ('adler32', 'xrdadler32'),
            ('md5', 'md5sum'),
            ('sha256', 'sha256sum'),
            ('sha512', 'sha512sum')
        ]
        # Check known algorithms
        for algo, cmd in algorithms:
            if algo not in cksums:
                continue
            expected = cksums[algo]
            ret = await asyncio.to_thread(subprocess.run, [cmd, path],
                                            capture_output=True, text=True, check=False)
            if ret.returncode != 0:
                logger.debug("Failed to run %s for file %s: %s", cmd, path, ret.stderr.strip())
                continue
            actual = ret.stdout.strip().split()[0]
            if actual == expected:
                return True
            logger.warning("File %s has bad %s checksum: %s != %s",
                            path, algo, actual, expected)
            return False
        # Failed to verify any checksums
        checked = set(a[0] for a in algorithms)
        unchecked = set(cksums.keys()) - checked
        if unchecked:
            logger.debug("Missing commands for checksum algorithms: %s", ', '.join(unchecked))
        logger.warning("Checksum failed for file %s", path)
        return False

    async def cache_xrootd(self, replica: Replica, timeout: float = 1) -> None:
        """
        Check if a replica is online or nearline by using gfal-xattr to query user.status

        :param replica: Replica object to check
        """
        # Skip cache check if the distance is already too high
        if replica.distance > config.sites.max_distance:
            replica.status = Status.UNREACHABLE
            return
        # Assume nearline unless we can confirm it is online
        replica.status = Status.NEARLINE
        cmd = ['gfal-xattr', '-t', int(timeout), replica.path, 'user.status']
        try:
            ret = await asyncio.to_thread(subprocess.run, cmd, capture_output=True,
                                          text=True, check=False, timeout=timeout+1)
        except subprocess.TimeoutExpired:
            logger.debug("Timeout running gfal-xattr on %s", replica.path)
            return
        if ret.returncode != 0:
            # code 110 is timeout
            logger.debug("Failed to run gfal-xattr on %s\n  %s", replica.path, ret.stderr.strip())
            return
        status = ret.stdout.strip()
        if status == 'UNKNOWN':
            logger.info("Got UNKNOWN status for %s, assuming NEARLINE", replica.path)
            return
        # If we got a valid status, set it on the replica
        replica.status = Status[status]

    def cache_local(self, replica: Replica) -> None:
        """
        Check if a replica is online or nearline by reading the locality stat file

        :param replica: Replica object to check
        """
        directory, filename = os.path.split(replica.path)
        stat_file=f"{directory}/.(get)({filename})(locality)"
        if not os.path.exists(stat_file):
            # normal file not in DCACHE?
            replica.status = Status.ONLINE
            return
        with open(stat_file, encoding="utf-8") as stats:
            status = stats.readline().strip()
        replica.status = Status[status]

    async def check_cache(self, replica: Replica) -> None:
        """
        Check if a replica is online or nearline using the appropriate method

        :param replica: Replica object to check
        """
        # For non-dcache RSEs, set status to ONLINE or NEARLINE depending on tape vs disk type
        if self.staging is None:
            replica.status = Status.ONLINE if self.disk else Status.NEARLINE
            return
        # For dcache RSEs, skip the cache check if there is no staging penalty
        if self.staging <= 0:
            replica.status = Status.ONLINE
            return
        # Check the cache status of the file
        if replica.protocol == 'file':
            await asyncio.to_thread(self.cache_local, replica)
        else:
            await self.cache_xrootd(replica)
        # If the file is not online, add the staging penalty to the distance
        if replica.status != Status.ONLINE:
            replica.distance += self.staging

    async def check_local(self, replica: Replica, size: int = None, cksums: dict = None):
        """
        Check the status of a local file replica

        :param replica: Replica object to check
        :param size: optionally check the file size against an expected value
        :param cksums: optionally check the file checksums against a dict of {algorithm: checksum}
        """
        # Make sure the file exists and is readable
        if not await asyncio.to_thread(os.path.isfile, replica.path):
            replica.status = Status.MISSING
            return
        if not await asyncio.to_thread(os.access, replica.path, os.R_OK):
            replica.status = Status.OFFLINE
            return
        # Check file size and checksums if we have expected values
        if size and await asyncio.to_thread(os.path.getsize, replica.path) != size:
            replica.status = Status.BAD_SIZE
            return
        if cksums and not await self.checksum_local(replica.path, cksums):
            replica.status = Status.BAD_CHECKSUM
            return
        # Check the cache status of the file
        await self.check_cache(replica)

    async def check_xrootd(self, replica: Replica, size: int = None, cksums: dict = None):
        """
        Check the status of a remote file replica accessed via xrootd

        :param replica: Replica object to check
        :param size: optionally check the file size against an expected value
        :param cksums: optionally check the file checksums against a dict of {algorithm: checksum}
        """
        # If we have an expected size, make sure the file exists and matches that size
        if size:
            ls = await self.xrdfs(replica, 'ls -l')
            if ls is None:
                return
            ls_perm, _, _, ls_size, _ = ls.split() # permissions, date, time, size, name
            # Make sure the file is readable
            if ls_perm[1] != 'r':
                logger.debug("File %s is not readable", replica.path)
                return
            # Check the size
            if ls_size != size:
                replica.status = Status.BAD_SIZE
                return
            # Check the checksums, if we have expected values
            if cksums and not await self.checksum_xrootd(replica, cksums):
                replica.status = Status.BAD_CHECKSUM
                return
        # Check the cache status of the file
        await self.check_cache(replica)

    async def check(self, replica: Replica, size: int = None, cksums: dict = None):
        """
        Check the status of a file replica on the RSE

        :param replica: Replica object to check
        :param size: optionally check the file size against an expected value
        :param cksums: optionally check the file checksums against a dict of {algorithm: checksum}
        """
        replica.distance = self.distance
        # Don't bother checking bad RSEs
        if self.distance > config.sites.max_distance:
            replica.status = Status.UNREACHABLE
            return
        if self.read is False:
            replica.status = Status.OFFLINE
            return
        # Check replica using the appropriate method based on the protocol
        protocol = replica.protocol
        # For local files, check directly but try to convert to xrootd URL if possible
        if protocol == 'file':
            await self.check_local(replica, size=size, cksums=cksums)
            if 'xrootd' in self.urls:
                replica.path = replica.path.replace(self.urls['file'], self.urls['xrootd'], 1)
            return
        # For local RSEs, get local path and check directly
        if 'file' in self.urls:
            url = replica.path
            replica.path = url.replace(self.urls[protocol], self.urls['file'], 1)
            await self.check_local(replica, size=size, cksums=cksums)
            replica.path = url
            return
        # For xrootd files, check using xrdfs and gfal-xattr
        if protocol == 'root':
            await self.check_xrootd(replica, size=size, cksums=cksums)
            return
        # If we get here, we don't know how to check this replica
        logger.debug("Unsupported protocol %s for replica %s", protocol, replica.path)
        replica.status = Status.BAD_PROTOCOL
        return


class GenericRSE(BaseRSE):
    """Class to store information about an unknown RSE"""

    def __init__(self, url: str = None, name: str = None):
        super().__init__()
        # If we have a name, check config for info about the RSE
        if name:
            if name in config.sites.dcache:
                url = config.sites.dcache[name]['url']
                self.staging = config.sites.dcache[name]['staging']
            if not url:
                raise ValueError(f"No URL found for RSE {name} in config")
        elif url is None:
            raise ValueError("Must provide either a name or url for the RSE")
        # Otherwise, use URL host name for the RSE
        host = get_host(url)
        self.name = name or host
        # Set url
        protocol = get_protocol(url)
        self.urls[protocol] = url
        # Try to convert file paths to xrootd URLs and vice versa
        path = None
        if protocol == 'file':
            path = url
            url = path_to_xrootd(path)
            if url:
                self.urls['root'] = url
                host = get_host(url)
                if name is None:
                    self.name = host
        elif protocol == 'root':
            path = xrootd_to_path(url)
            if path:
                self.urls['file'] = path
        else:
            logger.debug("Creating RSE %s with unknown protocol %s", self.name, protocol)
        # Check for distance based on RSE name, full URL, then host name
        if name and name in config.sites.rse_distances:
            self.distance = config.sites.rse_distances[name]
            logger.debug("Found distance for RSE %s based on name: %s", self.name, self.distance)
        elif url in config.sites.rse_distances:
            self.distance = config.sites.rse_distances[url]
            logger.debug("Found distance for RSE %s based on URL: %s", self.name, self.distance)
        elif host in config.sites.rse_distances:
            self.distance = config.sites.rse_distances[host]
            logger.debug("Found distance for RSE %s based on host: %s", self.name, self.distance)
        # If we don't have a distance, using staging penalty to represent tape vs disk
        else:
            logger.debug("Using default distances for RSE %s", self.name)
            self.distance = config.sites.rse_distances['disk']
            if self.staging is None:
                self.staging = config.sites.rse_distances['tape'] - self.distance
        # Add ping time to the distance
        self.distance += self.ping()

class RucioRSE(BaseRSE):
    """Class to store information about a Rucio RSE"""

    def __init__(self, info: dict):
        super().__init__()
        self.name = info['rse']
        self.read = info['availability_read'] and not info['deleted']
        self.write = info['availability_write'] and not info['deleted']
        if info['rse_type'] == 'DISK':
            self.disk = True
        elif info['rse_type'] == 'TAPE':
            self.disk = False
        else:
            logger.error("RSE %s has unknown type %s", self.name, info['rse_type'])
            self.disk = None
        if self.name in config.sites.dcache:
            self.staging = config.sites.dcache[self.name]['staging']
        self.set_distance()
        self.set_urls(info['protocols'])

    def set_distance(self) -> None:
        """Get the distance offset for this RSE from the config"""
        if self.name in config.sites.rse_distances:
            self.distance = config.sites.rse_distances[self.name]
        elif self.disk is True:
            self.distance = config.sites.rse_distances['disk']
        elif self.disk is False:
            self.distance = config.sites.rse_distances['tape']
        else:
            self.distance = float('inf')

    def set_urls(self, protocols: list) -> None:
        """
        Get the URL prefixes for this RSE from the protocols list

        :param protocols: list of protocol dictionaries from Rucio
        """
        for proto in protocols:
            scheme = proto['scheme']
            url = f"{scheme}://{proto['hostname']}:{proto['port']}{proto['prefix']}"
            self.urls[scheme] = url
            # Try converting xrootd URLs to local paths
            if scheme == 'root':
                path = xrootd_to_path(url)
                if path:
                    self.urls['file'] = path

# PathFinder classes

class PathFinder(MetaRetriever):
    """Base class for finding paths to files"""
    name: str = "replicas"
    file_owner: bool = False

    def __init__(self, meta: MetaRetriever):
        super().__init__()
        self.meta = meta
        self.client = RucioWrapper()
        self.rses = {}
        self.replica_queue = None
        self.workers = []

    @property
    def files(self) -> MergeSet:
        """Return the set of files from the source"""
        return self.meta.files

    async def replica_checker(self) -> None:
        """Asynchronous worker method to check the status of replicas from the replica queue"""
        while True:
            # Wait for a replica to check from the queue
            job = await self.replica_queue.get()
            # If we get a None job, it means we should stop the checker
            if job is None:
                self.replica_queue.task_done()
                break
            # Check the replica and mark the job as done
            await job[0].rse.check_replica(job[0], size=job[1], cksums=job[2])
            self.replica_queue.task_done()

    async def check_replica(self, replica: Replica, size: int = None, cksums: dict = None) -> None:
        """
        Add a replica to the replica queue for asynchronous checking

        :param replica: Replica object to check
        :param size: optionally check the file size against an expected value
        :param cksums: optionally check the file checksums against a dict of {algorithm: checksum}
        """
        await self.replica_queue.put((replica, size, cksums))

    async def connect(self) -> None:
        """Connect to the file source and rucio"""
        await asyncio.gather(self.meta.connect(), self.client.connect())
        self.replica_queue = asyncio.Queue()
        for _ in range(int(config.validation.concurrency)):
            worker = asyncio.create_task(self.replica_checker())
            self.workers.append(worker)

    async def disconnect(self) -> None:
        """Disconnect from the file source and rucio, and stop the replica checkers"""
        await asyncio.gather(self.meta.disconnect(), self.client.disconnect())
        # Stop the replica checkers
        for _ in self.workers:
            await self.replica_queue.put(None)
        await asyncio.gather(*self.workers)

    async def get_metadata(self, batch: InputBatch, limit: int) -> list:
        raise NotImplementedError("PathFinder does not implement get_metadata")

    @abstractmethod
    async def add_replica(self, file: MergeFile, path: str, rse_name: str = None) -> None:
        """
        Add a replica to a MergeFile object, including its path and RSE information

        :param file: MergeFile object to add the replica to
        :param path: file path for the replica
        :param rse_name: optional RSE name; if not provided, it will be inferred from the path
        """

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
        async for new_batch in self.meta.input_batches():
            # Get paths for previous batch, if we have a request in flight
            paths = await task if task is not None else None
            task = None
            # Start request for next batch
            if new_batch:
                task = asyncio.create_task(self.get_batch(self.get_paths, new_batch))
            # Process previous batch while we wait, if we have one
            if batch:
                logger.debug("Processing new %s input batch %d", self.name, batch.skip)
                await self.set_paths(batch, paths)
                # Wait for any pending path checks to finish before yielding the batch
                for rse in self.rses.values():
                    if rse.queue:
                        await rse.queue.join()
                # Check for replica errors
                good_files = []
                no_replicas = []
                unreachable = []
                for file in batch:
                    if not file.paths:
                        no_replicas.append(file.did)
                    elif all(r.status.bad for r in file.paths):
                        unreachable.append(file.did)
                    elif not file.errors:
                        good_files.append(file)
                self.files.set_error(no_replicas, 'NO_REPLICAS')
                self.files.set_error(unreachable, 'UNREACHABLE')
                if good_files:
                    yield InputBatch(skip=batch.skip, files=good_files)
            # Save new batch for processing in the next iteration
            batch = new_batch
        # Yield empty batch to signal completion
        yield InputBatch()


class RucioFinder (PathFinder):
    """Class for managing asynchronous queries to the Rucio web API."""
    name: str = "rucio"

    async def connect(self) -> None:
        """Connect to the file source and rucio"""
        await super().connect()
        if not self.client:
            logger.critical("Failed to connect to Rucio client")
            sys.exit(1)

    async def add_replica(self, file: MergeFile, path: str, rse_name: str = None) -> None:
        """
        Add a replica to a MergeFile object

        :param file: MergeFile object to add the replica to
        :param path: file path for the replica
        :param rse_name: RSE name for the replica
        """
        if not rse_name:
            raise ValueError("RucioFinder requires an RSE name to add a replica")
        rse = self.rses.get(rse_name)
        if not rse:
            rse_info = await self.client.get_rse(rse_name)
            if not rse_info:
                logger.critical("RSE %s not found in Rucio, cannot add replica", rse_name)
                sys.exit(1)
            rse = RucioRSE(rse_info)
            self.rses[rse_name] = rse
        replica = Replica(path=path, rse=rse)
        file.paths.append(replica)
        # Assume Rucio has already validated replica size and checksums
        #await self.check_replica(replica, size=file.size, cksums=file.checksums)
        await self.check_replica(replica)

    async def checksum(self, file: MergeFile, rucio: dict) -> bool:
        """
        Ensure file sizes and checksums from Rucio agree with the input metadata.
        
        :param file: MergeFile object to check
        :param rucio: Rucio replicas dictionary
        :return: True if files match, False otherwise
        """
        # Check the file size
        if file.size != rucio['bytes']:
            crit = config.validation.error_handling.unreachable == 'quit'
            lvl = logging.CRITICAL if crit else logging.ERROR
            logger.log(lvl, "Size mismatch for %s: %d != %d", file.did, file.size, rucio['bytes'])
            return False
        # See if we should skip the checksum check
        if len(config.validation.checksums) == 0:
            return True
        # Check the checksums
        for algo in config.validation.checksums:
            algo = str(algo)
            if algo in file.checksums and algo in rucio:
                csum1 = file.checksums[algo]
                csum2 = rucio[algo]
                if csum1 == csum2:
                    logger.debug("Found matching %s checksum for %s", algo, file.did)
                    return True
                crit = config.validation.error_handling.unreachable == 'quit'
                lvl = logging.CRITICAL if crit else logging.ERROR
                logger.log(lvl, "%s checksum err for %s: %s != %s", algo, file.did, csum1, csum2)
                return False
            if algo not in file.checksums:
                logger.debug("MetaCat missing %s checksum for %s", algo, file.did)
            if algo not in rucio:
                logger.debug("Rucio missing %s checksum for %s", algo, file.did)
        # If we get here, we have no matching checksums
        crit = config.validation.error_handling.unreachable == 'quit'
        lvl = logging.CRITICAL if crit else logging.ERROR
        logger.log(lvl, "No matching checksums for %s", file.did)
        return False

    async def get_paths(self, batch: InputBatch) -> list:
        """
        Asynchronously retrieve paths for a specific batch of files.

        :param batch: InputBatch object containing files to retrieve paths for
        :return: list of file path dictionaries
        """
        return await self.client.get_replicas(batch.files)

    async def set_paths(self, batch: InputBatch, paths: list) -> None:
        """
        Asynchronously set paths for a specific batch of files.
        
        :param batch: InputBatch object containing files to process
        :param paths: list of file path dictionaries from Rucio
        """
        dids = {f.did: f for f in batch.files}
        for replicas in paths:
            did = replicas['scope'] + ':' + replicas['name']
            pfns = replicas.get('pfns', {})
            count = len(pfns)
            logger.debug("Found %d replicas for %s", count, did)
            if count == 0:
                continue
            file = dids[did]
            if not await self.checksum(file, replicas):
                continue
            for pfn, info in pfns.items():
                rse = info['rse']
                self.add_replica(file, pfn, rse_name=rse)


class PathListFinder(PathFinder):
    """Class for finding paths from a list of explicit file paths"""
    name: str = "replica_list"

    def __init__(self, source: MetaRetriever, paths: dict = None):
        super().__init__(source)
        self.paths = paths or {}

    def add_rse(self, rse: BaseRSE) -> None:
        """Add an RSE to the list of known RSEs"""
        for protocol, url in rse.urls.items():
            rses = self.rses.setdefault(protocol, {})
            rses[url] = rse

    async def connect(self) -> None:
        """Connect to the file source and rucio"""
        await super().connect()
        # If we have access to Rucio, get the list of RSEs
        if self.client:
            for rse_info in self.client.get_rses():
                self.add_rse(RucioRSE(rse_info))
            return
        # If this is a batch job, we need Rucio
        if not config.output.local:
            logger.critical("Failed to connect to Rucio client")
            sys.exit(1)
        # For local jobs, fall back to generic RSEs for DCACHE locations
        for name in config.sites.dcache.keys():
            self.add_rse(GenericRSE(name=name))
        # Also check for path-like keys in the distance config to create generic RSEs for those
        for url in config.sites.rse_distances.keys():
            if '/' not in url:
                continue
            self.add_rse(GenericRSE(url=url))

    async def add_replica(self, file: MergeFile, path: str, rse_name: str = None) -> None:
        """
        Add a replica to a MergeFile object

        :param file: MergeFile object to add the replica to
        :param path: file path for the replica
        :param rse_name: RSE name for the replica
        """
        protocol = get_protocol(path)
        # Make sure local paths are absolute and expanded
        if protocol == 'file':
            path = io_utils.expand_path(path)
        # Try to find an existing RSE that matches the path prefix
        rses = self.rses.setdefault(protocol, {})
        rse = None
        for prefix, candidate in sorted(rses.items(), key=lambda x: len(x[0]), reverse=True):
            if path.startswith(prefix):
                rse = candidate
                break
        # Add a new RSE for this path if we don't have a match
        if not rse:
            if protocol == 'file':
                prefix = "/{path.split('/',2)[1]}/"
            else:
                prefix = f"{protocol}://{get_host(path)}:{get_port(path)}/"
            rse = GenericRSE(url=prefix)
            self.add_rse(rse)
        # Add the replica to the file
        replica = Replica(path=path, rse=rse)
        file.paths.append(replica)
        await self.check_replica(replica, size=file.size, cksums=file.checksums)

    async def get_paths(self, batch: InputBatch) -> list:
        """
        Asynchronously retrieve paths for a specific batch of files.

        :param batch: InputBatch object containing files to retrieve paths for
        :return: list of file path dictionaries
        """
        paths = []
        for file in batch.files:
            name = file.name
            # Get any explicit paths provided for this file
            file_paths = list(self.paths.get(name, []))
            # If we have search directories, look for a matching file in those as well
            for search_dir in config.input.search_dirs:
                search_path = os.path.join(search_dir, name)
                if os.path.isfile(search_path):
                    file_paths.append(search_path)
            # If we have any paths for this file, add them to the list of paths to return
            if file_paths:
                paths.append({name: file_paths})
        return paths

    async def set_paths(self, batch: InputBatch, paths: list) -> None:
        """
        Asynchronously set paths for a specific batch of files.
        
        :param batch: InputBatch object containing files to process
        :param paths: list of file path dictionaries from Rucio
        """
        # Consolidate list of paths into single dictionary
        path_dict = {}
        for file in paths:
            path_dict.update(file)

        # Assign paths to files
        for file in batch.files:
            name = file.name
            replicas = path_dict.get(name, [])
            count = len(replicas)
            logger.debug("Found %d replicas for %s", count, file.did)
            if count == 0:
                continue
            for path in replicas:
                self.add_replica(file, path)


def get(metadata: MetaRetriever) -> PathFinder:
    """
    Create and return a physical path finder:
    PathListFinder if any data files were provided in files input mode
    PathListFinder if any search directories were provided in other input modes
    RucioFinder if no data files or search directories were provided
    
    :return: PathFinder object for finding file locations
    """
    # First check for explicit file paths in files input mode
    if config.input.mode == 'files':
        # Group data file paths by name
        paths = collections.defaultdict(set)
        for path in config.input.inputs:
            # If we have a JSON file, strip the extension and look for a matching data file
            if path.endswith('.json'):
                path = path[:-5]
                if not os.path.isfile(path):
                    continue
            name = os.path.basename(path)
            paths[name].add(path)
        if paths:
            return PathListFinder(metadata, paths)
    # Also return a PathListFinder if we have local search directories
    if config.input.search_dirs:
        return PathListFinder(metadata)
    # Otherwise, we need to query Rucio to find the file paths
    return RucioFinder(metadata)
