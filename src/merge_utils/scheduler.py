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
from merge_utils.retriever import FileRetriever

logger = logging.getLogger(__name__)

class JobScheduler(ABC):
    """Base class for scheduling a merge job"""

    def __init__(self, source: FileRetriever):
        """
        Initialize the JobScheduler with a source of files to merge.
        
        :param source: FileRetriever object to provide input files
        """
        self.source = source
        self.dir = os.path.join(io_utils.pkg_dir(), "tmp", io_utils.get_timestamp())
        self.pass1 = []
        self.pass2 = []

    def get_chunks(self) -> None:
        """Run the FileRetriever and get the list of chunks."""
        self.source.run()
        for chunk in self.source.output_chunks():
            json_dict = chunk.json
            if chunk.chunks: # if the chunk has sub-chunks it is pass 2
                json_name = os.path.join(self.dir, f"pass2_{len(self.pass2):>06}.json")
                self.pass2.append(json_name)
            else: # if the chunk has no sub-chunks it is pass 1
                json_name = os.path.join(self.dir, f"pass1_{len(self.pass1):>06}.json")
                self.pass1.append(json_name)
            with open(json_name, 'w', encoding="utf-8") as fjson:
                fjson.write(json.dumps(json_dict, indent=2))

    @abstractmethod
    def run(self) -> None:
        """Run the job scheduler."""
        raise NotImplementedError("Subclasses must implement this method")

class LocalScheduler():
    """Job scheduler for local merge jobs"""

    def __init__(self, source: FileRetriever):
        """
        Initialize the LocalScheduler with a source of files to merge.
        
        :param source: FileRetriever object to provide input files
        """
        self.source = source
        self.dir = os.path.join(io_utils.pkg_dir(), "tmp", io_utils.get_timestamp())
        self.pass1 = []
        self.pass2 = []

    def json_name(self, tier: int) -> str:
        """
        Get the name of the next JSON config file for a given pass and site.
        
        :param tier: Pass number (1 or 2)
        :param site: Optional site name
        :return: Name of the JSON file
        """
        if tier == 1:
            site_jobs = self.pass1
        elif tier == 2:
            site_jobs = self.pass2
        else:
            raise ValueError("Tier must be 1 or 2")

        idx = len(site_jobs) + 1
        name = f"pass{tier}_{idx:>06}.json"
        name = os.path.join(self.dir, name)
        site_jobs.append(name)
        return name

    def get_chunks(self) -> None:
        """Run the FileRetriever and get the list of chunks."""
        self.source.run()
        os.makedirs(self.dir)
        for chunk in self.source.output_chunks():
            json_dict = chunk.json
            name = self.json_name(chunk.tier)
            with open(name, 'w', encoding="utf-8") as fjson:
                fjson.write(json.dumps(json_dict, indent=2))

    def run(self) -> None:
        """Run the job scheduler."""
        out_dir = config.output['dir']
        if not os.path.exists(out_dir):
            try:
                os.makedirs(out_dir, exist_ok=True)
                logger.info("Output directory '%s' created", out_dir)
            except OSError as error:
                logger.critical("Failed to create output directory '%s': %s", out_dir, error)
                sys.exit(1)

        self.get_chunks()
        if not self.pass1:
            logger.warning("No files to merge")
            return

        script1 = os.path.join(self.dir, "local_pass1.sh")
        with open(script1, 'w', encoding="utf-8") as f:
            f.write("#!/bin/bash\n")
            f.write("# This script will run the merge jobs for pass 1\n")
            for chunk in self.pass1:
                cmd = ['python3', os.path.join(io_utils.src_dir(), "do_merge.py"), chunk, out_dir]
                f.write(f"{' '.join(cmd)}\n")
        subprocess.run(['chmod', '+x', script1], check=False)

        if self.pass2:
            script2 = os.path.join(self.dir, "submit_pass2.sh")
            with open(script2, 'w', encoding="utf-8") as f:
                f.write("#!/bin/bash\n")
                f.write("# This script will run the merge jobs for pass 2\n")
                for chunk in self.pass2:
                    cmd = ['python3', os.path.join(io_utils.src_dir(), "do_merge.py"), chunk, out_dir]
                    f.write(f"{' '.join(cmd)}\n")
            subprocess.run(['chmod', '+x', script2], check=False)

        logger.info("Local job scripts written to %s", self.dir)
        #subprocess.run(cmd, check=True)

class JustinScheduler():
    """Job scheduler for JustIN merge jobs"""

    def __init__(self, source: FileRetriever):
        """
        Initialize the JustinScheduler with a source of files to merge.
        
        :param source: FileRetriever object to provide input files
        """
        self.source = source
        self.dir = os.path.join(io_utils.pkg_dir(), "tmp", io_utils.get_timestamp())
        self.pass1 = collections.defaultdict(list)
        self.pass2 = collections.defaultdict(list)
        self.cvmfs_dir = None

    def json_name(self, tier: int, site: str = None) -> str:
        """
        Get the name of the next JSON config file for a given pass and site.
        
        :param tier: Pass number (1 or 2)
        :param site: Optional site name
        :return: Name of the JSON file
        """
        if tier == 1:
            site_jobs = self.pass1[site]
        elif tier == 2:
            site_jobs = self.pass2[site]
        else:
            raise ValueError("Tier must be 1 or 2")

        idx = len(site_jobs) + 1
        if site:
            name = f"pass{tier}_{site}_{idx:>06}.json"
        else:
            name = f"pass{tier}_{idx:>06}.json"
        name = os.path.join(self.dir, name)
        site_jobs.append(name)
        return name

    def get_chunks(self) -> None:
        """Run the FileRetriever and get the list of chunks."""
        self.source.run()
        os.makedirs(self.dir)
        for chunk in self.source.output_chunks():
            json_dict = chunk.json
            site = chunk.site
            name = self.json_name(chunk.tier, site)
            with open(name, 'w', encoding="utf-8") as fjson:
                fjson.write(json.dumps(json_dict, indent=2))

    def upload_cfg(self) -> None:
        """
        Make a tarball of the configuration files and upload them to cvmfs
        
        :return: Path to the uploaded configuration directory
        """
        cfg = os.path.join(self.dir, "config.tar")
        with tarfile.open(cfg,"w") as tar:
            for _, files in collections.ChainMap(self.pass1, self.pass2).items():
                for file in files:
                    tar.add(file, os.path.basename(file))
            tar.add(os.path.join(io_utils.src_dir(), "do_merge.py"), "do_merge.py")

        proc = subprocess.run(['justin-cvmfs-upload', cfg], capture_output=True, check=False)
        if proc.returncode != 0:
            logger.error("Failed to upload configuration files: %s", proc.stderr.decode('utf-8'))
            raise RuntimeError("Failed to upload configuration files")
        self.cvmfs_dir = proc.stdout.decode('utf-8').strip()
        logger.info("Uploaded configuration files to %s", self.cvmfs_dir)

    def get_cmd(self, tier: int, site: str = None) -> str:
        """
        Get the command to run for a given tier and site.
        
        :param tier: Pass number (1 or 2)
        :param site: Optional site name
        :return: Command string
        """
        if tier == 1:
            site_jobs = self.pass1[site]
        elif tier == 2:
            site_jobs = self.pass2[site]
        else:
            raise ValueError("Tier must be 1 or 2")

        if not site_jobs:
            raise ValueError(f"No jobs found for pass {tier} and site {site}")

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
        return cmd

    def run(self) -> None:
        """
        Run the JustIN job scheduler.
        
        :return: None
        """
        self.get_chunks()
        if not self.pass1:
            logger.warning("No files to merge")
            return
        self.upload_cfg()

        with open(os.path.join(self.dir, "pass1.sh"), 'w', encoding="utf-8") as f:
            f.write("#!/bin/bash\n")
            f.write("# This script will submit the JustIN jobs for pass 1\n")
            for site in self.pass1:
                cmd = self.get_cmd(1, site)
                f.write(f"{' '.join(cmd)}\n")

        if self.pass2:
            with open(os.path.join(self.dir, "pass2.sh"), 'w', encoding="utf-8") as f:
                f.write("#!/bin/bash\n")
                f.write("# This script will submit the JustIN jobs for pass 2\n")
                for site in self.pass2:
                    cmd = self.get_cmd(2, site)
                    f.write(f"{' '.join(cmd)}\n")

        logger.info("JustIN job scripts written to %s", self.dir)
        #subprocess.run(cmd, check=True)
