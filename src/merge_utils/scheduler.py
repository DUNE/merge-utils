"""JobScheduler classes"""

import logging
import os
import sys
import shutil
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
        self.dir = io_utils.expand_path(
            os.path.join(config.output['scripts'], config.timestamp),
            base_dir=io_utils.pkg_dir()
        )
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
        site_jobs.append((name, chunk))
        return name

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
        os.makedirs(self.dir)

        for chunk in self.source.output_chunks():
            self.write_json(chunk)
        if not self.jobs[0]:
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

        io_utils.log_print(f"Files will be merged using {config.merging['method']['name']}")
        msg = ["Execute the merge by running:"] + script
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

    def write_script(self) -> list:
        """
        Write the job script for running local merge jobs.

        :return: Name of the generated script file
        """
        out_dir = io_utils.expand_path(config.output['dir'])
        if not os.path.exists(out_dir):
            try:
                os.makedirs(out_dir, exist_ok=True)
                logger.info("Output directory '%s' created", out_dir)
            except OSError as error:
                logger.critical("Failed to create output directory '%s': %s", out_dir, error)
                sys.exit(1)

        for dep in config.merging['method']['dependencies']:
            file_name = os.path.basename(dep)
            logger.debug("Adding %s to job directory", file_name)
            shutil.copyfile(dep, os.path.join(self.dir, file_name))

        script_name = os.path.join(self.dir, "run.sh")
        pass_msg = [
            "echo 'Creating intermediate merged files'",
            "echo 'Creating final merged files'"
        ]
        with open(script_name, 'w', encoding="utf-8") as f:
            f.write("#!/bin/bash\n")
            f.write("# This script will run the merge jobs locally\n")
            for tier in range(2):
                if self.jobs[1]:
                    f.write(pass_msg[tier] + "\n")
                for job in self.jobs[tier][None]:
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
        cfg = os.path.join(self.dir, "config.tar")
        with tarfile.open(cfg,"w") as tar:
            for site_jobs in self.jobs[0].values():
                for job in site_jobs:
                    add_file(tar, job[0])
            add_file(tar, io_utils.find_runner("do_merge.py"))
            for dep in config.merging['method']['dependencies']:
                add_file(tar, dep)

        proc = subprocess.run(['justin-cvmfs-upload', cfg], capture_output=True, check=False)
        if proc.returncode != 0:
            logger.error("Failed to upload configuration files: %s", proc.stderr.decode('utf-8'))
            raise RuntimeError("Failed to upload configuration files")
        self.cvmfs_dir = proc.stdout.decode('utf-8').strip()
        logger.info("Uploaded configuration files to %s", self.cvmfs_dir)

    def justin_cmd(self, tier: int, site: str, cvmfs_dir: str = None) -> str:
        """
        Create the JustIN command for submitting a merge job.
        
        :param tier: Merge pass (1 or 2)
        :param site: Site to run the job
        :param cvmfs_dir: CVMFS directory where config files are located
        :return: JustIN command string
        """
        if site is None:
            logger.critical("No site for pass %d job!", tier)
            sys.exit(1)
        if cvmfs_dir is None:
            cvmfs_dir = self.cvmfs_dir
        cmd = [
            'justin', 'simple-workflow',
            '--description', f'"Merge {config.timestamp} p{tier} {site}"',
            '--monte-carlo', str(len(self.jobs[tier-1][site])),
            '--jobscript', io_utils.find_runner("merge.jobscript"),
            '--site', site,
            '--scope', config.output['namespace'],
            '--lifetime-days', str(config.output['lifetime']),
            '--env', f'MERGE_CONFIG="pass{tier}_{site}"',
            '--env', f'CONFIG_DIR="{cvmfs_dir}"'
        ]
        if config.merging['dune_version']:
            cmd += ['--env', f'DUNE_VERSION="{config.merging["dune_version"]}"']
        if config.merging['dune_qualifier']:
            cmd += ['--env', f'DUNE_QUALIFIER="{config.merging["dune_qualifier"]}"']
        for output in config.merging['method']['outputs']:
            name, ext = os.path.splitext(output['name'])
            cmd += ['--output-pattern', f"{name}*{ext}"]
        return f"{' '.join(cmd)}\n"

    def write_script(self) -> list:
        """
        Write the job scripts for submitting JustIN merge jobs.
        
        :return: Name of the generated script file(s)
        """
        self.upload_cfg()

        # If no second pass is needed, create a single submission script
        if len(self.jobs[1]) == 0:
            script_name = os.path.join(self.dir, "submit.sh")
            with open(script_name, 'w', encoding="utf-8") as f:
                f.write("#!/bin/bash\n")
                f.write("# This script will submit the JustIN jobs\n")
                for site in self.jobs[0]:
                    f.write(self.justin_cmd(1, site))
            subprocess.run(['chmod', '+x', script_name], check=False)
            return [script_name]

        # Pass 1 submission script
        script_pass1 = os.path.join(self.dir, "submit_pass1.sh")
        with open(script_pass1, 'w', encoding="utf-8") as f:
            f.write("#!/bin/bash\n")
            f.write("# This script will submit JustIN jobs for pass 1\n")
            for site in self.jobs[0]:
                f.write(self.justin_cmd(1, site))
        subprocess.run(['chmod', '+x', script_pass1], check=False)

        # Pass 2 JustIN submission commands
        pass2_justin = os.path.join(self.dir, "pass2_justin.sh")
        with open(pass2_justin, 'w', encoding="utf-8") as f:
            f.write("#!/bin/bash\n"
                    "# This script will submit JustIN jobs for pass 2\n"
                    "# Use submit_pass2.sh to generate the cvmfs directory first!\n"
                    "cvmfs_dir=$1\n"
                    "if [ -z \"$cvmfs_dir\" ]; then\n"
                    "  echo 'Use submit_pass2.sh instead of calling this script directly!'\n"
                    "  exit 1\n"
                    "fi\n")
            for site in self.jobs[1]:
                f.write(self.justin_cmd(2, site, "$cvmfs_dir"))
        subprocess.run(['chmod', '+x', pass2_justin], check=False)

        # Pass 2 submission script
        script_pass2 = os.path.join(self.dir, "submit_pass2.sh")
        pass2_cfgs = []
        for site_jobs in self.jobs[1].values():
            pass2_cfgs.extend(os.path.basename(job[0]) for job in site_jobs)
        with open(script_pass2, 'w', encoding="utf-8") as f:
            f.write("#!/bin/bash\n")
            f.write("# This script will update the cfg files for pass 2 before submission\n")
            pass2_fix = os.path.join(io_utils.src_dir(), 'pass2_fix.py')
            f.write(f"python3 {pass2_fix} {self.dir} {' '.join(pass2_cfgs)}\n")
        subprocess.run(['chmod', '+x', script_pass2], check=False)

        return [script_pass1, script_pass2]
