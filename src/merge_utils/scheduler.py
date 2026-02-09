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
from abc import ABC, abstractmethod

from merge_utils import io_utils, config
from merge_utils.retriever import PathFinder
from merge_utils.merge_set import MergeChunk

logger = logging.getLogger(__name__)

class JobScheduler(ABC):
    """Base class for scheduling a merge job"""

    def __init__(self, source: PathFinder):
        """
        Initialize the JobScheduler with a source of files to merge.
        
        :param source: PathFinder object to provide input files
        """
        self.source = source
        self.dir = os.path.join(str(config.job.dir), 'merge')
        self.jobs = []

    def split_files(self, files: list) -> list[list]:
        """
        Split a list of files into groups for merging, based on the configured chunk size.
        
        :param files: List of MergeFile objects to split
        :return: List of lists of MergeFile objects, where each sublist is a group for merging
        """
        if not files:
            return []
        chunk_max = config.grouping.chunk_max
        if len(files) <= chunk_max:
            return [files]
        n_chunks = math.ceil(len(files) / chunk_max)
        target_size = len(files) / n_chunks
        return [files[int(i*target_size):int((i+1)*target_size)] for i in range(n_chunks)]

    def schedule(self, chunk: MergeChunk) -> None:
        """
        Schedule a chunk for merging, subdividing and assigning to sites as necessary.
        
        :param chunk: MergeChunk object to schedule
        """
        # By default, just split into subchunks if too many files, without site assignment
        if len(chunk.files) > config.grouping.chunk_max:
            for subchunk in self.split_files(chunk.files):
                chunk.make_child(subchunk)

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
        self.source.run()
        os.makedirs(self.dir, exist_ok=True)

        for chunk in self.source.files.groups():
            self.schedule(chunk)
            self.write_specs(chunk)
        if not self.jobs:
            logger.critical("No files to merge")
            return
        io_utils.log_print(f"Writing job config files to {self.dir}")

        n_inputs = 0
        n_stage1 = 0
        n_stage2 = 0
        n_outputs = 0
        msg = [""]
        for site, site_jobs in self.jobs[0].items():
            site_inputs = sum(len(job[1]) for job in site_jobs)
            site_stage1 = len(site_jobs)
            site_outputs = sum(1 for job in site_jobs if job[1].chunk_id < 0)
            n_inputs += site_inputs
            n_stage1 += site_stage1
            n_outputs += site_outputs
            if site is None:
                site = "local"
            msg.append(f"{site}: \t{site_inputs} -> {site_stage1}")
        for site, site_jobs in self.jobs[1].items():
            n_stage2 += sum(1 for job in site_jobs if job[1].output_id == 0)
        n_outputs += n_stage2

        script = self.write_script()

        msg[0] = f"Merging {n_inputs} input files into {n_outputs} groups"
        io_utils.log_print("\n  ".join(msg))
        if len(script) > 1:
            if len(self.jobs[0]) > 1:
                msg = ["A second merging pass is required due to distributed inputs:"]
            else:
                msg = ["A second merging pass is required due to high multiplicity:"]
            for site, site_jobs in self.jobs[1].items():
                site_inputs = sum(len(job[1].inputs) for job in site_jobs)
                site_stage2 = len(site_jobs)
                msg.append(f"{site}: \t{site_inputs} -> {site_stage2}")
            io_utils.log_print("\n  ".join(msg))

        io_utils.log_print(f"Files will be merged using {config.method.method_name}")
        msg = ["Execute the merge by running:"] + script
        io_utils.log_print("\n  ".join(msg))

class LocalScheduler(JobScheduler):
    """Job scheduler for local merge jobs"""

    def schedule(self, chunk: MergeChunk) -> None:
        """
        Schedule a chunk for merging, clearing any site assignment and subdividing as necessary.
        
        :param chunk: MergeChunk object to schedule
        """
        chunk.site = None   # Local jobs should not have a site assigned
        super().schedule(chunk)

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

    def schedule(self, chunk: MergeChunk) -> None:
        """
        Schedule a chunk for merging, subdividing and assigning to sites as necessary.
        
        :param chunk: MergeChunk object to schedule
        """
        # Group files by the best merging site
        best_sites = collections.defaultdict(list)
        for file in chunk.files:
            best_site = sorted(file.paths.items(), key=lambda p: p[1][1])[0][0]
            best_sites[best_site].append(file)
        best_sites = sorted(best_sites.items(), key=lambda x: len(x[1]), reverse=True)
        # Try to do merge as one chunk if possible
        chunk_max = config.grouping.chunk_max
        if len(chunk.files) < chunk_max:
            # If all files have the same best site, assign the chunk to that site
            if len(best_sites) == 1:
                chunk.site = best_sites[0][0]
                return
            # Otherwise, see if we can find a site with access to all files
            site_dists = {site: dist for site, (_, dist) in chunk.files[0].paths.items()}
            for file in chunk.files[1:]:
                for site in site_dists:
                    if site_dists[site] == float('inf'):
                        continue
                    if site not in file.paths:
                        site_dists[site] = float('inf')
                    else:
                        site_dists[site] += file.paths[site][1]
            best_site, dist = sorted(site_dists.items(), key=lambda x: x[1])[0]
            if dist < float('inf'):
                chunk.site = best_site
                return
        # Try to remove sites with small groups of files
        for idx in range(len(best_sites)-1, 0, -1):
            files = best_sites[idx][1]
            if len(files) >= config.grouping.chunk_min:
                break
            # Find the next best site for each file in the small group
            new_sites = [-1] * len(files)
            new_dists = [float('inf')] * len(files)
            for f_idx, file in enumerate(files):
                for new_idx, (new_site, new_files) in enumerate(best_sites):
                    if new_idx == idx or len(new_files) == 0:
                        continue
                    dist = file.paths[new_site][1] if new_site in file.paths else float('inf')
                    if dist < new_dists[f_idx]:
                        new_sites[f_idx] = new_idx
                        new_dists[f_idx] = file.paths[new_site][1]
            # If every file has a valid new site, move them there and clear the small group
            if all(new_idx >= 0 for new_idx in new_sites):
                for f_idx, file in enumerate(files):
                    best_sites[new_sites[f_idx]][1].append(file)
                best_sites[idx][1] = []
        # Split by best site and schedule separately
        for site, site_files in best_sites:
            for subchunk in self.split_files(site_files):
                child = chunk.make_child(subchunk)
                child.site = site
        # If all files have the same best site use that, otherwise use default 
        if len(best_sites) == 1:
            chunk.site = best_sites[0][0]
        else:
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
        cfg = os.path.join(str(config.job.dir), "pass1.tar")
        with tarfile.open(cfg,"w") as tar:
            for site_jobs in self.jobs[0].values():
                for job in site_jobs:
                    add_file(tar, job[0])
            add_file(tar, io_utils.find_runner("do_merge.py"))
            for dep in config.method.dependencies:
                add_file(tar, dep)

        proc = subprocess.run(['justin-cvmfs-upload', cfg], capture_output=True, check=False)
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
        if tier == 0:
            cvmfs_dir = self.cvmfs_dir
            namespace = config.output.namespace
            lifetime = config.output.batch.lifetime
        else:
            cvmfs_dir = "$cvmfs_dir"
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
            if var in ['MERGE_CONFIG', 'CONFIG_DIR', 'DUNE_VERSION', 'DUNE_QUALIFIER', 'EXTRA_PRODUCTS']:
                logger.error("Cannot override reserved environment variable: %s", var)
                continue
            cmd += ['--env', f'{var}="{val}"']
        if config.method.environment.products:
            products = ':'.join([str(p) for p in config.method.environment.products])
            cmd += ['--env', f'EXTRA_PRODUCTS="{products}"']
        for output in config.method.outputs:
            name, ext = os.path.splitext(output['name'])
            cmd += ['--output-pattern', f"{name}*{ext}"]
        return f"{' '.join(cmd)}\n"

    def write_script(self) -> list:
        """
        Write the job scripts for submitting JustIN merge jobs.
        
        :return: Name of the generated script file(s)
        """
        self.upload_cfg()

        # Pass 1 submission script
        script_name = "submit.sh" if len(self.jobs) == 1 else "submit_pass1.sh"
        with open(os.path.join(str(config.job.dir), script_name), 'w', encoding="utf-8") as f:
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
                f.write(f"python3 {pass2_fix} {str(config.job.dir)} {' '.join(pass_cfgs)}\n")
            subprocess.run(['chmod', '+x', script_name], check=False)
            scripts.append(script_name)

        return scripts
