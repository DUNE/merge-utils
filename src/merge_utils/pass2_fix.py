"""Update pass 2 json files and add to cvmfs directory before submission"""

import sys
import os
import shutil
import json
import tarfile
import subprocess

from rucio.client.replicaclient import ReplicaClient

def get_cfgs(cfg_dir: str, files: list[str]) -> dict:
    """Get the configuration dictionaries from the list of files"""
    cfgs = {}
    for cfg in files:
        path = os.path.join(cfg_dir, cfg)
        if not os.path.isfile(path):
            print(f"ERROR: Configuration file {path} does not exist!")
            sys.exit(1)
        with open(path, encoding="utf-8") as f:
            cfgs[cfg] = json.load(f)
    return cfgs

def get_pfns(inputs: set) -> dict:
    """Get the physical file names from Rucio for the given input DIDs"""
    query = []
    for did in inputs:
        scope, name = did.split(':', 1)
        query.append({'scope': scope, 'name': name})
    found = {}
    unreachable = []

    client = ReplicaClient()
    res = client.list_replicas(query, ignore_availability=False)
    for replicas in res:
        did = replicas['scope'] + ':' + replicas['name']
        count = len(replicas['pfns'])
        if count == 0:
            unreachable.append(did)
            continue
        if count > 1:
            print(f"WARNING: Found {count} replicas for {did}, using the first one")

        pfn = next(iter(replicas['pfns']))
        found[did] = pfn

    missing = [did for did in inputs if did not in found]
    unreachable.extend(missing)
    if len(unreachable) > 0:
        print(f"ERROR: Failed to retrieve {len(unreachable)} file(s) from Rucio:")
        for did in unreachable:
            print(f"  {did}")
        print("Did the first merging pass complete successfully?")
        sys.exit(1)

    return found

def main():
    """Main function for command line execution"""
    cfg_dir = sys.argv[1]
    cfgs = get_cfgs(cfg_dir, sys.argv[2:])
    job_dir = os.path.dirname(cfg_dir)

    inputs = set()
    for cfg in cfgs.values():
        inputs.update(cfg['inputs'])
    print(f"Found {len(inputs)} unique inputs in {len(cfgs)} configuration files")

    print("Retrieving physical file paths from Rucio")
    pfns = get_pfns(inputs)

    cfg_pass2 = os.path.join(job_dir, "config_pass2.tar")
    if os.path.exists(cfg_pass2):
        os.remove(cfg_pass2)
    cfg_base = os.path.join(job_dir, "config.tar")
    if not os.path.isfile(cfg_base):
        print(f"ERROR: Base configuration file {cfg_base} does not exist!")
        sys.exit(1)
    shutil.copyfile(cfg_base, cfg_pass2)

    with tarfile.open(cfg_pass2, "a") as tar:
        for name, cfg in cfgs.items():
            cfg['inputs'] = [pfns[did] for did in cfg['inputs']]
            fix_name = os.path.join(cfg_dir, name.replace('.json', '_fixed.json'))
            if os.path.exists(fix_name):
                os.remove(fix_name)
            with open(fix_name, 'w', encoding="utf-8") as fjson:
                fjson.write(json.dumps(cfg, indent=2))
            tar.add(fix_name, name)

    print("Uploading corrected configuration files to cvmfs")
    proc = subprocess.run(['justin-cvmfs-upload', cfg_pass2], capture_output=True, check=False)
    if proc.returncode != 0:
        print(f"Failed to upload configuration files: {proc.stderr.decode('utf-8')}")
        sys.exit(1)
    cvmfs_dir = proc.stdout.decode('utf-8').strip()
    print(f"Uploaded configuration files to {cvmfs_dir}")

    print("Submitting pass 2 jobs to JustIN")
    subprocess.run([os.path.join(job_dir, "pass2_justin.sh"), cvmfs_dir], check=False)

if __name__ == '__main__':
    main()
