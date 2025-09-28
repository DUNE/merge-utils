"""JobScheduler classes"""

import logging
import os
import sys
import json
import tarfile
import subprocess
import collections
from abc import ABC, abstractmethod

from merge_utils import io_utils, config
from merge_utils.retriever import PathFinder

logger = logging.getLogger(__name__)

class JobScheduler(ABC):
    """Base class for scheduling a merge job"""

    def __init__(self, source: PathFinder):
        """
        Initialize the JobScheduler with a source of files to merge.
        
        :param source: PathFinder object to provide input files
        """
        self.source = source
        self.dir = os.path.expanduser(os.path.expandvars(os.path.join(
            config.output['scripts'], io_utils.get_timestamp()
        )))
        if not os.path.isabs(self.dir):
            self.dir = os.path.join(io_utils.pkg_dir(), self.dir)
        self.jobs = [collections.defaultdict(list), collections.defaultdict(list)]

    def write_json(self, chunk) -> str:
        """
        Write a JSON dictionary to a file and return the file name.
        
        :param chunk: MergeChunk object to write
        :return: Name of the written JSON file
        """
        json_dict = chunk.json
        site = chunk.site
        tier = chunk.tier

        site_jobs = self.jobs[tier-1][site]
        idx = len(site_jobs) + 1

        if site:
            name = f"pass{tier}_{site}_{idx:>06}.json"
        else:
            name = f"pass{tier}_{idx:>06}.json"
        name = os.path.join(self.dir, name)

        with open(name, 'w', encoding="utf-8") as fjson:
            fjson.write(json.dumps(json_dict, indent=2))
        site_jobs.append(name)
        return name

    @abstractmethod
    def write_script(self, tier: int) -> str:
        """
        Write the job script for a given tier.
        
        :param tier: Pass number (1 or 2)
        :return: Name of the generated script file
        """

    def run(self) -> None:
        """
        Run the Job scheduler.
        
        :return: None
        """
        self.source.run()
        os.makedirs(self.dir)

        for chunk in self.source.output_chunks():
            self.write_json(chunk)
        if not self.jobs[0]:
            logger.critical("No files to merge")
            return
        io_utils.log_print(f"Job config files written to {self.dir}")

        if not self.jobs[1]:
            io_utils.log_print("Only one merging pass is required")
        elif len(self.jobs[0]) > 1:
            io_utils.log_print("Two merging passes are required due to distributed inputs")
        else:
            io_utils.log_print("Two merging passes are required due to high multiplicity")

        msg = [
            "Execute the merge by running:",
            self.write_script(1)
        ]
        if self.jobs[1]:
            msg.append(self.write_script(2))

        io_utils.log_print("\n  ".join(msg))

class LocalScheduler(JobScheduler):
    """Job scheduler for local merge jobs"""

    def write_json(self, chunk) -> str:
        """
        Write a JSON dictionary to a file and return the file name.
        
        :param chunk: MergeChunk object to write
        :return: Name of the written JSON file
        """
        chunk.site = None  # Local jobs do not require a site
        return super().write_json(chunk)

    def write_script(self, tier: int) -> str:
        """
        Write the job script for a given tier.
        
        :param tier: Pass number (1 or 2)
        :return: Name of the generated script file
        """
        out_dir = os.path.expanduser(os.path.expandvars(config.output['dir']))
        if tier == 1 and not os.path.exists(out_dir):
            try:
                os.makedirs(out_dir, exist_ok=True)
                logger.info("Output directory '%s' created", out_dir)
            except OSError as error:
                logger.critical("Failed to create output directory '%s': %s", out_dir, error)
                sys.exit(1)

        script_name = os.path.join(self.dir, f"run_pass{tier}.sh")
        with open(script_name, 'w', encoding="utf-8") as f:
            f.write("#!/bin/bash\n")
            f.write(f"# This script will run local merge jobs for pass {tier}\n")
            for job in self.jobs[tier-1][None]:
                cmd = ["LD_PRELOAD=$XROOTD_LIB/libXrdPosixPreload.so",
                       "python3", os.path.join(io_utils.src_dir(), "do_merge.py"), job, out_dir]
                f.write(f"{' '.join(cmd)}\n")
        subprocess.run(['chmod', '+x', script_name], check=False)
        return script_name

class JustinScheduler(JobScheduler):
    """Job scheduler for JustIN merge jobs"""

    def __init__(self, source: PathFinder):
        """
        Initialize the JustinScheduler with a source of files to merge.
        
        :param source: PathFinder object to provide input files
        """
        super().__init__(source)
        self.cvmfs_dir = None

    def upload_cfg(self) -> None:
        """
        Make a tarball of the configuration files and upload them to cvmfs
        
        :return: Path to the uploaded configuration directory
        """
        cfg = os.path.join(self.dir, "config.tar")
        with tarfile.open(cfg,"w") as tar:
            for _, files in collections.ChainMap(*self.jobs).items():
                for file in files:
                    tar.add(file, os.path.basename(file))
            tar.add(os.path.join(io_utils.src_dir(), "do_merge.py"), "do_merge.py")

        io_utils.log_print("Uploading configuration files to cvmfs...")
        proc = subprocess.run(['justin-cvmfs-upload', cfg], capture_output=True, check=False)
        if proc.returncode != 0:
            logger.error("Failed to upload configuration files: %s", proc.stderr.decode('utf-8'))
            raise RuntimeError("Failed to upload configuration files")
        self.cvmfs_dir = proc.stdout.decode('utf-8').strip()
        logger.info("Uploaded configuration files to %s", self.cvmfs_dir)

    def write_script(self, tier: int) -> str:
        """
        Write the job script for a given tier.
        
        :param tier: Pass number (1 or 2)
        :return: Name of the generated script file
        """
        if tier == 1:
            self.upload_cfg()

        script_name = os.path.join(self.dir, f"submit_pass{tier}.sh")
        with open(script_name, 'w', encoding="utf-8") as f:
            f.write("#!/bin/bash\n")
            f.write(f"# This script will submit JustIN jobs for pass {tier}\n")
            for site, site_jobs in self.jobs[tier-1].items():
                cmd = [
                    'justin', 'simple-workflow',
                    '--monte-carlo', str(len(site_jobs)),
                    '--jobscript', os.path.join(io_utils.src_dir(), "merge.jobscript"),
                    '--env', f'MERGE_CONFIG="pass{tier}_{site}"',
                    '--env', f'CONFIG_DIR="{self.cvmfs_dir}"',
                    '--site', site,
                    '--scope', 'usertests',
                    '--output-pattern', '*_merged_*:merge-test', 
                    '--lifetime-days', '1'
                ]
                f.write(f"{' '.join(cmd)}\n")
        subprocess.run(['chmod', '+x', script_name], check=False)
        return script_name
