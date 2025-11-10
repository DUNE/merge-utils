"""Actually perform the merging"""

import sys
import os
import json
import copy
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
    if "dunegpvm" not in socket.gethostname():
        return
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

def get_outputs(config: dict) -> list[dict]:
    """Get the output file list from the config, renaming existing files if needed"""
    outputs = config.pop('outputs')
    if len(outputs) == 0:
        print("ERROR: No output files specified!")
        sys.exit(1)
    # Rename existing output files
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    for output in outputs:
        file_name = output.name
        if os.path.exists(file_name):
            oldname = file_name+"_"+timestamp+".bak"
            os.rename(file_name,oldname)
            print(f"WARNING: Output file {file_name} already exists, renaming to {oldname}")
        json_name = output.name + '.json'
        if os.path.exists(json_name):
            oldname = json_name+"_"+timestamp+".bak"
            os.rename(json_name,oldname)
            print(f"WARNING: JSON file {json_name} already exists, renaming to {oldname}")
    return outputs

def get_settings(config: dict, script_dir: str) -> dict:
    """Get the merging settings from the config"""
    settings = config.pop('settings', {})
    settings.setdefault('streaming', False)
    # Merge method settings
    if 'cfg' in settings:
        settings['cfg'] = os.path.join(script_dir, settings['cfg'])
    if 'script' in settings:
        settings['script'] = os.path.join(script_dir, settings['script'])
    if 'cmd' in settings:
        settings['cmd'] = settings['cmd'].format(
            script=settings.get('script', ''),
            cfg=settings.get('cfg', ''),
            inputs="{inputs}",
            outputs="{outputs}",
            output="{output}"
        )
    elif 'script' in settings:
        # Default command if script is provided but no cmd
        cmd = settings['script']
        if cmd.endswith('.py'):
            cmd = "python3 " + cmd
        if 'cfg' in settings:
            cmd += " " + settings['cfg']
        cmd += " {output} {inputs}"
        settings['cmd'] = cmd
    else:
        print("ERROR: No merging command or script specified!")
        sys.exit(1)

    return settings

def write_metadata(outputs: list[dict], out_dir: str, config: dict) -> None:
    """Write file metadata to JSON files"""
    for output in outputs:
        # Apply per-file metadata overrides
        metadata = copy.deepcopy(config)
        metadata['metadata'].update(output.get('metadata', {}))
        metadata['name'] = output['name']
        # Rename default output files if needed
        path = os.path.join(out_dir, output['name'])
        if 'rename' in output:
            old_path = output['rename']
            if os.path.exists(old_path):
                os.rename(old_path, path)
            else:
                print(f"ERROR: Expected output file {output['rename']} not found!")
                sys.exit(1)
        elif not os.path.exists(path):
            print(f"ERROR: Output file {output['name']} not found!")
            sys.exit(1)
        # Check output file attributes
        metadata['size'] = os.path.getsize(path)
        metadata['checksums'] = checksums(path)
        # Write metadata to JSON file
        with open(path+'.json', 'w', encoding="utf-8") as fjson:
            fjson.write(json.dumps(metadata, indent=2))

def merge(config: dict, script_dir: str, out_dir: str) -> None:
    """Merge the input files into a single output file"""
    settings = get_settings(config, script_dir)
    inputs = config.pop('inputs')
    outputs = get_outputs(config)

    renew_token()

    # Make local copies of the input files if not streaming
    tmp_files = []
    if not settings['streaming']:
        tmp_files = local_copy(inputs, out_dir)

    # Merge the input files based on the specified method
    out_paths = [os.path.join(out_dir, output['name']) for output in outputs]
    cmd = settings['cmd'].format(
        inputs=" ".join(inputs),
        outputs=out_paths,
        output=out_paths[0]
    )
    print(f"Merging {len(inputs)} files into {outputs[0]['name']} using method {settings['name']}")
    print(cmd)
    ret = subprocess.run(cmd, shell=True, check=False)
    if ret.returncode != 0:
        print(f"ERROR: Merging failed with return code {ret.returncode}")
        sys.exit(ret.returncode)

    write_metadata(outputs, out_dir, config)

    # Clean up temporary files
    if len(tmp_files) > 0:
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
