#!/usr/bin/env python3
"""Actually perform the merging"""

import sys
import os
import json
import subprocess
import tarfile

def checksums(filename: str) -> dict:
    """Calculate the checksum of a file"""
    proc = subprocess.run(['xrdadler32', filename], capture_output=True, check=False)
    if proc.returncode != 0:
        raise ValueError('xrdadler32 failed', proc.returncode, proc.stderr)
    checksum = proc.stdout.decode('utf-8').split()[0]
    results = {'adler32':checksum}

    return results

def merge_hadd(output: str, inputs: list) -> None:
    """Merge the input files using hadd"""
    cmd = ['hadd', '-v', '0', '-f', output] + inputs
    print(f"Running command:\n{' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def merge_lar(output: str, inputs: list[str], config: str) -> None:
    """Merge the input files using lar"""
    cmd = ['lar', '-c', config, '-o', output] + inputs
    print(f"Running command:\n{' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def merge_hdf5(output: str, inputs: list[str]) -> None:
    """Merge the input files into an HDF5 file"""
    raise NotImplementedError("HDF5 merging is not yet implemented")
    #TODO: investigate https://github.com/NU-CUCIS/ph5concat

def merge_tar(output: str, inputs: list[str]) -> None:
    """Merge the input files into a tar.gz archive"""
    with tarfile.open(output,"w:gz") as tar:
        for file in inputs:
            tar.add(file,os.path.basename(file))

def merge(config: dict, outdir: str) -> None:
    """Merge the input files into a single output file"""
    method = config['metadata']['merge.method']
    output = os.path.join(outdir, config['name'])
    inputs = config.pop('inputs')

    # Merge the input files based on the specified method
    if method == "hadd":
        merge_hadd(output, inputs)
    elif method == "lar":
        lar_config = config['metadata']['merge.fcl']
        merge_lar(output, inputs, lar_config)
    elif method == "hdf5":
        merge_hdf5(output, inputs)
    elif method == "tar":
        merge_tar(output, inputs)
    else:
        raise ValueError(f"Unsupported merge method: {method}")

    # Clean up the configuration dictionary
    config['size'] = os.path.getsize(output)
    config['checksums'] = checksums(output)

    # Write the configuration to a JSON file
    json_name = output + '.json'
    with open(json_name, 'w', encoding="utf-8") as fjson:
        fjson.write(json.dumps(config, indent=2))

def main():
    """Main function for command line execution"""
    with open(sys.argv[1], encoding="utf-8") as f:
        config = json.load(f)
    outdir = sys.argv[2] if len(sys.argv) > 2 else '.'
    merge(config, outdir)

if __name__ == '__main__':
    main()
