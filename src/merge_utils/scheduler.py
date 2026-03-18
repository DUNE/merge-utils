"""JobScheduler classes"""

import logging
import os
import sys
import shutil
import json
import tarfile
import subprocess
import collections
import math
import asyncio
from abc import ABC, abstractmethod
from typing import AsyncGenerator

from merge_utils import io_utils, config, justin_utils
from merge_utils.merge_set import MergeFileError, MergeSet, MergeFile, MergeChunk
from merge_utils.retriever import InputBatch
from merge_utils.replicas import Replica, PathFinder, GenericRSE, RucioRSE

logger = logging.getLogger(__name__)

STD_ENV_VARS = set([
    'MERGE_CONFIG',
    'CONFIG_DIR',
    'DUNE_VERSION',
    'DUNE_QUALIFIER',
    'EXTRA_PRODUCTS'
])

class JobScheduler(ABC):
    """Base class for scheduling a merge job"""

    def __init__(self, source: PathFinder):
        """
        Initialize the JobScheduler with a source of files to merge.
        
        :param source: PathFinder object to provide input files
        """
        self.source = source
        self.dir = os.path.join(str(config.job.dir), 'merge')
        self.distances = {} # Cache of RSE-site distances
        self.jobs = []

    @property
    def files(self) -> MergeSet:
        """Return the set of files from the source"""
        return self.source.meta.files

    async def connect(self) -> None:
        """Connect to the file source"""
        await self.source.connect()

    async def disconnect(self) -> None:
        """Disconnect from the file source"""
        await self.source.disconnect()

    async def replica_distances(self, replica: Replica) -> dict:
        """
        Get the distances from a replica to potential merging sites.

        :param replica: Replica object to get distances for
        :return: Dictionary mapping site names to distances
        """
        rse = replica.rse.name
        distances = self.distances.setdefault(rse, {})
        if not distances:
            distances[None] = float('inf')
        return distances

    def file_distances(self, file: MergeFile) -> dict:
        """
        Get the distances from a file to potential merging sites, based on its replicas.

        :param file: MergeFile object to get distances for
        :return: Dictionary mapping site names to distances
        """
        site_dists = {}
        for replica in file.replicas:
            if not replica.status.good:
                continue
            rse_dists = self.distances[replica.rse.name]
            for site, dist in rse_dists.items():
                site_dists[site] = min(site_dists.get(site, float('inf')), dist + replica.distance)
        if not site_dists:
            raise RuntimeError(f"File {file.did} has no available replicas")
        return site_dists

    def chunk_distances(self, chunk: MergeChunk) -> dict:
        """
        Get the distances from a chunk to potential merging sites, based on the file distances.
        If any file is unreachable from a site, the chunk is also unreachable from that site.

        :param chunk: MergeChunk object to get distances for
        :return: Dictionary mapping site names to distances
        """
        site_dists = self.file_distances(chunk.files[0])
        for file in chunk.files[1:]:
            file_dists = self.file_distances(file)
            for site, dist in file_dists.items():
                if site in site_dists:
                    site_dists[site] += dist
                else:
                    site_dists[site] = float('inf')
            for site in site_dists:
                if site not in file_dists:
                    site_dists[site] = float('inf')
        return site_dists

    async def input_batches(self) -> AsyncGenerator[InputBatch, None]:
        """
        Asynchronously check RSE-site distances for batches of input files

        :return: InputBatch object containing skip index and list of MergeFile objects
        """
        async for batch in self.source.input_batches():
            unreachable = []
            for file in batch:
                if file.errors:
                    continue
                # Get minimum distance from the file replicas to any merging site
                min_dist = float('inf')
                for replica in file.replicas:
                    if not replica.status.good:
                        continue
                    replica_dists = await self.replica_distances(replica)
                    dist = replica.distance + min(replica_dists.values())
                    min_dist = min(min_dist, dist)
                # File is unreachable no RSE-site distance was below threshold
                if min_dist > config.sites.max_distance:
                    logger.warning("File %s has no replicas within max distance", file.did)
                    unreachable.append(file.did)
            # Set unreachable flag for bad files
            self.files.set_error(unreachable, 'UNREACHABLE')
            # Output batch with reachable files
            good_files = [f for f in batch if not f.errors]
            yield InputBatch(skip=batch.skip, files=good_files)

    async def _loop(self) -> None:
        """Repeatedly get input_batches until all files are retrieved."""
        # Connect to source
        await self.connect()
        # Loop over batches
        async for _ in self.input_batches():
            self.files.check_errors()
        # Close connections
        await self.disconnect()

    def run_loop(self) -> None:
        """Retrieve metadata for all files."""
        try:
            asyncio.run(self._loop())
        except ValueError as err:
            logger.critical("%s", err)
            sys.exit(1)

        self.files.check_errors(final = True)

    def assign_site(self, chunk: MergeChunk, site: str = None) -> None:
        """
        Assign a merging site for a chunk of files, and select the best replica for each file

        :param chunk: MergeChunk object to assign a site for
        :param site: Site assignment
        """
        chunk.site = site   # Local jobs should not have a site assigned
        for file in chunk.files:
            best_replica = None
            min_dist = float('inf')
            for replica in file.replicas:
                if not replica.status.good:
                    continue
                dist = replica.distance + self.distances[replica.rse.name].get(site, float('inf'))
                if dist < min_dist:
                    min_dist = dist
                    best_replica = replica
            if best_replica is None:
                raise RuntimeError(f"File {file.did} has no good replicas for site {site}")
            file.replicas = [best_replica]

    def split_files(self, files: list) -> list[list]:
        """
        Split a list of files into groups for merging, based on the configured chunk size.
        
        :param files: List of MergeFile objects to split
        :return: List of lists of MergeFile objects, where each sublist is a group for merging
        """
        if not files:
            return []
        chunk_max = config.method.chunks.max_count
        if len(files) <= chunk_max:
            return [files]
        n_chunks = math.ceil(len(files) / chunk_max)
        target_size = len(files) / n_chunks
        return [files[int(i*target_size):int((i+1)*target_size)] for i in range(n_chunks)]

    @abstractmethod
    def schedule(self, chunk: MergeChunk) -> None:
        """
        Schedule a chunk for merging, subdividing and assigning to sites as necessary.
        
        :param chunk: MergeChunk object to schedule
        """

    def write_specs(self, chunk) -> None:
        """
        Write merge specs for a chunk to JSON dictionary files.
        
        :param chunk: MergeChunk object to write
        """
        # Recursively write specs for child chunks, if any
        for child in chunk.children:
            self.write_specs(child)
        # Get site job list for this tier, creating it if necessary
        tier = chunk.tier
        if tier >= len(self.jobs):
            self.jobs.append(collections.defaultdict(list))
        site_jobs = self.jobs[tier][chunk.site]
        prefix = f"pass{tier+1}"
        if chunk.site:
            prefix = f"{prefix}_{chunk.site}"
        # Write a JSON file for each output spec from the chunk
        for spec in chunk.specs:
            name = os.path.join(self.dir, f"{prefix}_{len(site_jobs)+1:>06}.json")
            with open(name, 'w', encoding="utf-8") as fjson:
                fjson.write(json.dumps(spec, indent=2))
            site_jobs.append((name, chunk))

    @abstractmethod
    def write_script(self) -> list:
        """
        Write the job script

        :return: List of the generated script file name(s)
        """

    def run(self) -> None:
        """
        Run the Job scheduler.
        
        :return: None
        """
        self.run_loop()
        os.makedirs(self.dir, exist_ok=True)

        for chunk in self.files.groups():
            self.schedule(chunk)
            self.write_specs(chunk)
        if not self.jobs:
            logger.critical("No files to merge")
            return
        io_utils.log_print(f"Writing job config files to {self.dir}")

        msg = ["Merge jobs:"] if len(self.jobs) == 1 else ["Pass 1 merge jobs:"]
        for site, site_jobs in self.jobs[0].items():
            if site is None:
                site = "local"
            msg.append(f"  {site}: {len(site_jobs)} merges")
        for tier, tier_jobs in enumerate(self.jobs[1:], start=2):
            msg.append(f"Pass {tier} merge jobs:")
            for site, site_jobs in tier_jobs.items():
                if site is None:
                    site = "local"
                msg.append(f"  {site}: {len(site_jobs)} merges")
        io_utils.log_print("\n".join(msg))

        script = self.write_script()

        io_utils.log_print(f"Files will be merged using {config.method.method_name}")
        msg = ["Execute the merge by running:"] + script
        io_utils.log_print("\n  ".join(msg))

class LocalScheduler(JobScheduler):
    """Job scheduler for local merge jobs"""

    def __init__(self, source):
        super().__init__(source)
        self.justin = False

    async def connect(self) -> None:
        """Connect to the file source"""
        # Connect to source
        await self.source.connect()
        # If we have a local site name, try to get distances from JustIN
        if config.local.site:
            local_site = str(config.local.site)
            justin_dists = await justin_utils.get_site_rse_distances()
            if justin_dists:
                self.justin = True
            for rse, dists in justin_dists.items():
                dist = dists.get(local_site, float('inf'))
                if dist < float('inf'):
                    self.distances[rse] = {None: dist}

    async def replica_distances(self, replica: Replica) -> dict:
        """
        Get the distances from a replica to potential merging sites.

        :param replica: Replica object to get distances for
        :return: Dictionary mapping site names to distances
        """
        rse = replica.rse.name
        distances = self.distances.setdefault(rse, {})
        if not distances:
            if self.justin:
                distances[None] = float('inf')
            elif isinstance(replica.rse, RucioRSE):
                distances[None] = replica.rse.ping()
            else:
                distances[None] = 0
        return distances

    def schedule(self, chunk: MergeChunk) -> None:
        """
        Schedule a chunk for merging, clearing any site assignment and subdividing as necessary.
        
        :param chunk: MergeChunk object to schedule
        """
        # Just set site to None for local jobs
        self.assign_site(chunk, site=None)
        # Split into subchunks if there are too many files
        if len(chunk.files) > config.method.chunks.max_count:
            for subchunk in self.split_files(chunk.files):
                chunk.make_child(subchunk)

    def write_script(self) -> list:
        """
        Write the job script for running local merge jobs.

        :return: Name of the generated script file
        """
        out_dir = io_utils.expand_path(config.output.out_dir)
        if not os.path.exists(out_dir):
            try:
                os.makedirs(out_dir, exist_ok=True)
                logger.info("Output directory '%s' created", out_dir)
            except OSError as error:
                logger.critical("Failed to create output directory '%s': %s", out_dir, error)
                sys.exit(1)

        for dep in config.method.dependencies:
            file_name = os.path.basename(dep)
            logger.debug("Adding %s to job directory", file_name)
            shutil.copyfile(dep, os.path.join(self.dir, file_name))

        script_name = os.path.join(str(config.job.dir), "run.sh")
        with open(script_name, 'w', encoding="utf-8") as f:
            f.write("#!/bin/bash\n")
            f.write("# This script will run the merge jobs locally\n")
            for tier, jobs in enumerate(self.jobs):
                if len(self.jobs) == 1:
                    f.write("echo 'Creating merged files'\n")
                else:
                    pass_msg = f"pass {tier+1}/{len(self.jobs)}"
                    if tier == len(self.jobs)-1:
                        f.write(f"echo 'Creating final merged files ({pass_msg})'\n")
                    else:
                        f.write(f"echo 'Creating intermediate merged files ({pass_msg})'\n")
                for job in jobs[None]:
                    #cmd = ["LD_PRELOAD=$XROOTD_LIB/libXrdPosixPreload.so", "python3",
                    #       io_utils.find_runner("do_merge.py"), job[0], out_dir]
                    cmd = ["python3", io_utils.find_runner("do_merge.py"), job[0], out_dir]
                    f.write(f"{' '.join(cmd)}\n")
        subprocess.run(['chmod', '+x', script_name], check=False)
        return [script_name]

class JustinScheduler(JobScheduler):
    """Job scheduler for JustIN merge jobs"""

    def __init__(self, source: PathFinder):
        """
        Initialize the JustinScheduler with a source of files to merge.
        
        :param source: PathFinder object to provide input files
        """
        super().__init__(source)
        self.cvmfs_dir = None

    async def connect(self) -> None:
        """Connect to the file source"""
        # Connect to source
        await self.source.connect()
        # Get site-rse distances from JustIN
        self.distances = await justin_utils.get_site_rse_distances()
        if not self.distances:
            logger.critical("Cannot run batch jobs without JustIN connection!")
            sys.exit(1)

    def schedule(self, chunk: MergeChunk) -> None:
        """
        Schedule a chunk for merging, subdividing and assigning to sites as necessary.
        
        :param chunk: MergeChunk object to schedule
        """
        # Try to do merge as one chunk if possible
        if len(chunk.files) < config.method.chunks.max_count:
            site, dist = sorted(self.chunk_distances(chunk).items(), key=lambda x: x[1])[0]
            if dist < float('inf'):
                self.assign_site(chunk, site=site)
                return
        # Oherwise, group files by the best merging site
        best_sites = collections.defaultdict(list)
        for file in chunk.files:
            dists = self.file_distances(file)
            best_site = sorted(dists.items(), key=lambda p: p[1])[0][0]
            best_sites[best_site].append(file)
        best_sites = sorted(best_sites.items(), key=lambda x: len(x[1]), reverse=True)
        # If all files are at the same site, just assign the chunk there and split if needed
        if len(best_sites) == 1:
            self.assign_site(chunk, site=best_sites[0][0])
            # Split into subchunks if there are too many files
            if len(chunk.files) > config.method.chunks.max_count:
                for subchunk in self.split_files(chunk.files):
                    chunk.make_child(subchunk)
            return
        # Try to remove sites with small groups of files
        for idx in range(len(best_sites)-1, 0, -1):
            files = best_sites[idx][1]
            if len(files) >= config.method.chunks.min_count:
                break
            # Find the next best site for each file in the small group
            new_sites = [-1] * len(files)
            new_dists = [float('inf')] * len(files)
            for f_idx, file in enumerate(files):
                dists = self.file_distances(file)
                for new_idx, (new_site, new_files) in enumerate(best_sites):
                    if new_idx == idx or len(new_files) == 0:
                        continue
                    dist = dists.get(new_site, float('inf'))
                    if dist < new_dists[f_idx]:
                        new_sites[f_idx] = new_idx
                        new_dists[f_idx] = dist
            # If every file has a valid new site, move them there and clear the small group
            if all(new_idx >= 0 for new_idx in new_sites):
                for f_idx, file in enumerate(files):
                    best_sites[new_sites[f_idx]][1].append(file)
                best_sites[idx][1] = []
        # Split by best site and schedule separately
        for site, site_files in best_sites:
            for subchunk in self.split_files(site_files):
                child = chunk.make_child(subchunk)
                self.assign_site(child, site=site)
        # Use default site for parent chunk
        chunk.site = config.sites.default

    def upload_cfg(self) -> None:
        """
        Make a tarball of the configuration files and upload them to cvmfs
        
        :return: Path to the uploaded configuration directory
        """
        def add_file(tar, file_path = None):
            if file_path is None:
                return
            file_name = os.path.basename(file_path)
            logger.debug("Adding %s to config tarball", file_name)
            tar.add(file_path, file_name)

        io_utils.log_print("Uploading configuration files to cvmfs...")
        cfg_base = os.path.join(str(config.job.dir), "config.tar")
        with tarfile.open(cfg_base,"w") as tar:
            add_file(tar, io_utils.find_runner("do_merge.py"))
            for dep in config.method.dependencies:
                add_file(tar, dep)

        cfg_pass1 = os.path.join(str(config.job.dir), "config_pass1.tar")
        shutil.copyfile(cfg_base, cfg_pass1)
        with tarfile.open(cfg_pass1, "a") as tar:
            for site_jobs in self.jobs[0].values():
                for job in site_jobs:
                    add_file(tar, job[0])

        proc = subprocess.run(['justin-cvmfs-upload', cfg_pass1], capture_output=True, check=False)
        if proc.returncode != 0:
            logger.error("Failed to upload configuration files: %s", proc.stderr.decode('utf-8'))
            raise RuntimeError("Failed to upload configuration files")
        self.cvmfs_dir = proc.stdout.decode('utf-8').strip()
        logger.info("Uploaded configuration files to %s", self.cvmfs_dir)

    def justin_cmd(self, tier: int, site: str) -> str:
        """
        Create the JustIN command for submitting a merge job.
        
        :param tier: Merge pass number (0-indexed!)
        :param site: Site to run the job
        :param cvmfs_dir: CVMFS directory where config files are located
        :return: JustIN command string
        """
        if site is None:
            logger.critical("No site for pass %d job!", tier+1)
            sys.exit(1)
        cvmfs_dir = "$cvmfs_dir" if tier > 0 else self.cvmfs_dir
        #TODO: Make sure this works if different outputs need different numbers of passes
        if tier == len(self.jobs)-1:
            logger.debug("Using output namespace and lifetime for pass %d jobs", tier+1)
            namespace = config.output.namespace
            lifetime = config.output.batch.lifetime
        else:
            logger.debug("Using scratch namespace and lifetime for pass %d jobs", tier+1)
            namespace = config.output.scratch.namespace
            lifetime = config.output.scratch.lifetime
        cmd = [
            'justin', 'simple-workflow',
            '--description', f'"Merge {config.uuid()} p{tier+1} {site}"',
            '--monte-carlo', str(len(self.jobs[tier][site])),
            '--jobscript', io_utils.find_runner("merge.jobscript"),
            '--site', site,
            '--scope', str(namespace),
            '--lifetime-days', str(lifetime),
            '--env', f'MERGE_CONFIG="pass{tier+1}_{site}"',
            '--env', f'CONFIG_DIR="{cvmfs_dir}"'
        ]
        if config.method.environment.dunesw_version:
            cmd += ['--env', f'DUNE_VERSION="{config.method.environment.dunesw_version}"']
        if config.method.environment.dunesw_qualifier:
            cmd += ['--env', f'DUNE_QUALIFIER="{config.method.environment.dunesw_qualifier}"']
        for var, val in config.method.environment.vars.items():
            if var in STD_ENV_VARS:
                logger.error("Cannot override reserved environment variable: %s", var)
                continue
            cmd += ['--env', f'{var}="{val}"']
        if config.method.environment.products:
            products = ':'.join([str(p) for p in config.method.environment.products])
            cmd += ['--env', f'EXTRA_PRODUCTS="{products}"']
        for output in config.method.outputs:
            name = str(output['name'])
            cmd += ['--output-pattern', name.format(UUID='*')]
        if config.output.batch.rse:
            cmd += ['--output-rse', str(config.output.batch.rse)]
        return f"{' '.join(cmd)}\n"

    def write_script(self) -> list:
        """
        Write the job scripts for submitting JustIN merge jobs.
        
        :return: Name of the generated script file(s)
        """
        self.upload_cfg()

        # Pass 1 submission script
        script_name = "submit.sh" if len(self.jobs) == 1 else "submit_pass1.sh"
        script_name = os.path.join(str(config.job.dir), script_name)
        with open(script_name, 'w', encoding="utf-8") as f:
            f.write("#!/bin/bash\n")
            pass_msg = " for pass 1" if len(self.jobs) > 1 else ""
            f.write(f"# This script will submit the JustIN jobs{pass_msg}\n")
            for site in self.jobs[0]:
                f.write(self.justin_cmd(0, site))
        subprocess.run(['chmod', '+x', script_name], check=False)
        scripts = [script_name]
        if len(self.jobs) == 1:
            return scripts

        # Pass 2+
        for tier in range(1, len(self.jobs)):
            justin = os.path.join(str(config.job.dir), f"pass{tier+1}_justin.sh")
            with open(justin, 'w', encoding="utf-8") as f:
                msg = [
                    "#!/bin/bash",
                    f"# This script will submit the JustIN jobs for pass {tier+1}",
                    f"# Use submit_pass{tier+1}.sh to generate the cvmfs directory first!",
                    "cvmfs_dir=$1",
                    "if [ -z \"$cvmfs_dir\" ]; then",
                    f"  echo 'Use submit_pass{tier+1}.sh instead of calling this script directly!'",
                    "  exit 1",
                    "fi"
                ]
                f.write("\n".join(msg) + "\n")
                for site in self.jobs[1]:
                    f.write(self.justin_cmd(tier, site))
            subprocess.run(['chmod', '+x', justin], check=False)

            # Pass 2 submission script
            script_name = os.path.join(str(config.job.dir), f"submit_pass{tier+1}.sh")
            pass_cfgs = []
            for site_jobs in self.jobs[tier].values():
                pass_cfgs.extend(os.path.basename(job[0]) for job in site_jobs)
            with open(script_name, 'w', encoding="utf-8") as f:
                f.write("#!/bin/bash\n")
                f.write(f"# This script will update the cfg files for pass {tier+1}\n")
                pass2_fix = os.path.join(io_utils.src_dir(), 'pass2_fix.py')
                f.write(f"python3 {pass2_fix} {self.dir} {' '.join(pass_cfgs)}\n")
            subprocess.run(['chmod', '+x', script_name], check=False)
            scripts.append(script_name)

        return scripts
