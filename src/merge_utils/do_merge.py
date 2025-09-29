"""Actually perform the merging"""

#test?#
import sys
import os
import json
import subprocess
import tarfile
import socket
from datetime import datetime, timezone

def checksums(filename: str) -> dict:
    """Calculate the checksum of a file"""
    proc = subprocess.run(['xrdadler32', filename], capture_output=True, check=False)
    if proc.returncode != 0:
        raise ValueError('xrdadler32 failed', proc.returncode, proc.stderr)
    checksum = proc.stdout.decode('utf-8').split()[0]
    results = {'adler32':checksum}

    return results


def renew_token():
    "" 
    cmd = "htgettoken -i dune --vaultserver htvaultprod.fnal.gov -r interactive --nooidc".split(" ")
    try:
        print ("Renewing token with command: %s"%(" ".join(cmd)))
        rval = subprocess.run(cmd, check=True)
        print ("Token renewed",rval)
    except:
        print ("WARNING: Token renewal failed, skip for now")




def merge_hadd(output: str, inputs: list) -> None:
    """Merge the input files using hadd"""
    cmd = ['hadd', '-v', '0', '-f', output] + inputs
    print(f"Running command:\n{' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def merge_lar(output: str, inputs: list[str], cfg: str) -> None:
    """Merge the input files using lar"""
    cmd = ['lar', '-c', cfg, '-o', output] + inputs
    print(f"Running command:\n{' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def merge_hdf5(output: str, inputs: list[str], cfg: str) -> None:
    """Merge the input files into an HDF5 file"""
    import hdf5_merge # pylint: disable=import-outside-toplevel
    cfg_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config', 'hdf5', cfg)
    hdf5_merge.merge_hdf5(output, inputs, cfg_path)

def merge_tar(output: str, inputs: list[str]) -> None:
    """Merge the input files into a tar.gz archive"""
    with tarfile.open(output,"w:gz") as tar:
        for file in inputs:
            tar.add(file,os.path.basename(file))

def local_copy(inputs: list[str], outdir: str) -> list[str]:
    """Make a local copy of the input files"""
    tmp_files = []
    tmp_dir = os.path.join(outdir, "tmp")
    print(f"Making local copy of input files in {tmp_dir}:")
    for i, path in enumerate(inputs):
        basename = os.path.basename(path)
        if os.path.exists(path):
            print(f"  Skipping {basename} (file already local)")
            continue

        local_path = os.path.join(tmp_dir, basename)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        if os.path.exists(local_path):
            print(f"  Checking {basename} (local copy already exists)")
            try:
                cmd = ['xrdcp', path, local_path, '-C', 'adler32', '--continue']
                subprocess.run(cmd, check=True)
            except subprocess.CalledProcessError:
                print(f"  Replacing {basename} (existing local copy is corrupted)")
                os.remove(local_path)
                cmd = ['xrdcp', path, local_path, '-C', 'adler32']
                subprocess.run(cmd, check=True)
        else:
            print(f"  Copying {basename}")
            cmd = ['xrdcp', path, local_path, '-C', 'adler32']
            subprocess.run(cmd, check=True)

        tmp_files.append(local_path)
        inputs[i] = local_path

    print(f"Copied {len(tmp_files)} files")
    return tmp_files

def merge(config: dict, outdir: str) -> None:
    """Merge the input files into a single output file"""
    method = config['metadata']['merge.method']
    output = os.path.join(outdir, config['name'])
    inputs = config.pop('inputs')
    streaming = config.pop('streaming', False)

    # Renew token if on interactive gpvm at fnal
    if  "dunegpvm" in socket.gethostname():
        renew_token()

    # Make local copies of the input files if not streaming
    tmp_files = []
    if not streaming:
        tmp_files = local_copy(inputs, outdir)

    # Remove output files if they already exist
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    if os.path.exists(output):
        oldname = output+"_"+timestamp+".bak"
        os.rename(output,oldname)
        print(f"WARNING: Output file {output} already exists, renaming to {oldname}")
    json_name = output + '.json'
    if os.path.exists(json_name):
        oldname = json_name+"_"+timestamp+".bak"
        os.rename(json_name,oldname)
        print(f"WARNING: JSON file {json_name} already exists, renaming to {oldname}")

    # Merge the input files based on the specified method
    if method == "hadd":
        merge_hadd(output, inputs)
    elif method == "lar":
        lar_config = config['metadata']['merge.cfg']
        merge_lar(output, inputs, lar_config)
    elif method == "hdf5":
        hdf5_config = config['metadata']['merge.cfg']
        merge_hdf5(output, inputs, hdf5_config)
    elif method == "tar":
        merge_tar(output, inputs)
    else:
        raise ValueError(f"Unsupported merge method: {method}")

    # Write metadata to a JSON file
    config['size'] = os.path.getsize(output)
    config['checksums'] = checksums(output)
    try:
        with open(json_name, 'w', encoding="utf-8") as fjson:
            fjson.write(json.dumps(config, indent=2))
    except Exception as e:
        print(f"WARNING: Could not write JSON file {json_name}: {e}")

    # Clean up temporary files
    if not streaming and len(tmp_files) > 0:
        print("Deleting local input file copies")
        for file in tmp_files:
            os.remove(file)

def main():
    """Main function for command line execution"""
    with open(sys.argv[1], encoding="utf-8") as f:
        config = json.load(f)
    outdir = sys.argv[2] if len(sys.argv) > 2 else '.'
    merge(config, outdir)

if __name__ == '__main__':
    main()
