"""Actually perform the merging"""

import sys
import os
import json
import subprocess
import socket
from datetime import datetime, timezone

def checksums(filename: str) -> dict:
    """Calculate the checksum of a file"""
    ret = subprocess.run(['xrdadler32', filename], capture_output=True, text=True, check=False)
    if ret.returncode != 0:
        print(f"ERROR: xrdadler32 failed with return code {ret.returncode}:")
        print(ret.stderr)
        sys.exit(1)
    checksum = ret.stdout.split()[0]
    results = {'adler32':checksum}
    return results

def renew_token():
    """Try to renew the token if on interactive gpvm at fnal""" 
    cmd = "htgettoken -i dune --vaultserver htvaultprod.fnal.gov -r interactive --nooidc"
    print(f"Renewing token with command: {cmd}")
    ret = subprocess.run(cmd.split(' '), check=False)
    if ret.returncode == 0:
        print ("Token renewed successfully")
    else:
        print ("WARNING: Token renewal failed, skip for now")

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
        cmd = ['xrdcp', path, local_path, '-C', 'adler32']
        exists = os.path.exists(local_path)
        if exists:
            print(f"  Checking {basename} (local copy already exists)")
            ret = subprocess.run(cmd + ['--continue'], check=False)
            if ret.returncode != 0:
                print(f"  Replacing {basename} (existing local copy is corrupted)")
                os.remove(local_path)
                exists = False
        else:
            print(f"  Copying {basename}")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

        if not exists:
            ret = subprocess.run(cmd, check=False)
            if ret.returncode != 0:
                print(f"ERROR: Local copy of {basename} failed with return code {ret.returncode}")
                os.remove(local_path)
                sys.exit(1)

        tmp_files.append(local_path)
        inputs[i] = local_path

    print(f"Copied {len(tmp_files)} files")
    return tmp_files

def clear_outputs(output: str) -> None:
    """Remove existing output files if they exist"""
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

def get_method(metadata: dict, script_dir: str) -> dict:
    """Get the merging method from metadata"""
    method = {'name': metadata['merge.method']}
    if 'merge.cfg' in metadata:
        method['cfg'] = os.path.join(script_dir, metadata['merge.cfg'])
    if 'merge.script' in metadata:
        method['script'] = os.path.join(script_dir, metadata['merge.script'])
    if 'merge.cmd' in metadata:
        method['cmd'] = metadata['merge.cmd'].format(
            script=method.get('script', ''),
            cfg=method.get('cfg', ''),
            output="{output}",
            inputs="{inputs}"
        )
    elif 'script' in method:
        # Default command if script is provided but no cmd
        cmd = method['script']
        if cmd.endswith('.py'):
            cmd = "python3 " + cmd
        if 'cfg' in method:
            cmd += " " + method['cfg']
        cmd += " {output} {inputs}"
        method['cmd'] = cmd
    else:
        print("ERROR: No merging command or script specified!")
        sys.exit(1)
    return method

def merge(config: dict, script_dir: str, out_dir: str) -> None:
    """Merge the input files into a single output file"""
    method = get_method(config['metadata'], script_dir)
    output = os.path.join(out_dir, config['name'])
    inputs = config.pop('inputs')
    settings = config.pop('settings', {})
    streaming = settings.get('streaming', False)

    # Renew token if on interactive gpvm at fnal
    if "dunegpvm" in socket.gethostname():
        renew_token()

    # Make local copies of the input files if not streaming
    tmp_files = []
    if not streaming:
        tmp_files = local_copy(inputs, out_dir)

    clear_outputs(output)

    # Merge the input files based on the specified method
    cmd = method['cmd'].format(output=output, inputs=" ".join(inputs))
    print(f"Merging {len(inputs)} files into {output} using method {method['name']}")
    print(cmd)
    ret = subprocess.run(cmd, shell=True, check=False)
    if ret.returncode != 0:
        print(f"ERROR: Merging failed with return code {ret.returncode}")
        sys.exit(ret.returncode)

    # Write metadata to a JSON file
    config['size'] = os.path.getsize(output)
    config['checksums'] = checksums(output)
    with open(output+'.json', 'w', encoding="utf-8") as fjson:
        fjson.write(json.dumps(config, indent=2))

    # Clean up temporary files
    if not streaming and len(tmp_files) > 0:
        print("Deleting local input file copies")
        for file in tmp_files:
            os.remove(file)

def main():
    """Main function for command line execution"""
    with open(sys.argv[1], encoding="utf-8") as f:
        config = json.load(f)
    script_dir = os.path.dirname(sys.argv[1])
    out_dir = sys.argv[2] if len(sys.argv) > 2 else '.'
    merge(config, script_dir, out_dir)

if __name__ == '__main__':
    main()
