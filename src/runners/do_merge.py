"""Actually perform the merging"""

import sys
import os
import json
import copy
import subprocess
import shutil
import socket
from datetime import datetime, timezone
import tarfile
import ROOT
import h5py

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

def list_root(tdir, base="") -> list:
    """
    List all contents of a ROOT file recursively.

    :param dir: ROOT TDirectoryFile or TFile to list
    :param base: Base path for the current directory
    :return: list of object paths
    """
    contents = []
    for key in tdir.GetListOfKeys():
        obj_name = key.GetName()
        full_path = os.path.join(base, obj_name)
        contents.append(full_path)
        if key.IsFolder():
            subdir = tdir.Get(obj_name)
            if isinstance(subdir, ROOT.TDirectoryFile):
                contents.extend(list_root(subdir, full_path))
    return contents

def list_hdf5(group) -> list:
    """
    List all contents of an HDF5 file recursively.

    :param group: HDF5 group to list
    :return: list of dataset paths
    """
    contents = []
    for obj in group.values():
        contents.append(obj.name[1:])  # Remove leading '/'
        if isinstance(obj, h5py.Group):
            contents.extend(list_hdf5(obj))
    return contents

def check_file(path: str, rename: str = None, checklist: list = None) -> bool:
    """
    Make sure a file is readable and not empty

    :param path: Path to the file
    :param rename: If provided, rename this file to the new name
    :param checklist: Optional list of expected contents in the file
    :return: True if the file is valid, False otherwise
    """
    # Rename file if needed
    if rename is not None:
        if os.path.exists(rename):
            shutil.move(rename, path)
        else:
            print(f"ERROR: Expected output file {rename} not found!")
            return False
    elif not os.path.exists(path):
        print(f"ERROR: Output file {os.path.basename(path)} not found!")
        return False

    # Read file contents
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext in ['.root']:
            root_file = ROOT.TFile.Open(path, 'READ')
            if not root_file or root_file.IsZombie():
                print(f"ERROR: Failed to open ROOT file {os.path.basename(path)}")
                return False
            contents = list_root(root_file)
            root_file.Close()
        elif ext in ['.h5', '.hdf5', '.he5']:
            with h5py.File(path, 'r') as hdf5_file:
                contents = list_hdf5(hdf5_file)
        elif ext in ['.tar', '.gz']:
            with tarfile.open(path, 'r') as tar_file:
                contents = tar_file.getnames()
        else:
            print(f"WARNING: Unknown file type {os.path.basename(path)}, skipping content check")
            return True
    except Exception as e:
        print(f"ERROR: Failed to read file {os.path.basename(path)}: {e}")
        return False

    # Check for expected contents
    if checklist is None:
        if len(contents) > 0:
            return True
        print(f"ERROR: File {os.path.basename(path)} is empty")
        return False
    missing = []
    for item in checklist:
        if item not in contents:
            missing.append(item)
    if len(missing) > 0:
        print(f"ERROR: File {os.path.basename(path)} is missing expected contents:")
        for item in missing:
            print(f"  {item}")
        return False
    return True

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
        if os.path.exists(os.path.expanduser(os.path.expandvars(path))):
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

def get_outputs(config: dict, out_dir: str) -> list[dict]:
    """Get the output file list from the config, renaming existing files if needed"""
    outputs = config.pop('outputs')
    if len(outputs) == 0:
        print("ERROR: No output files specified!")
        sys.exit(1)
    # Rename existing output files
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    for output in outputs:
        file_path = os.path.join(out_dir, output['name'])
        if os.path.exists(file_path):
            old_path = file_path+"_"+timestamp+".bak"
            shutil.move(file_path, old_path)
            print(f"WARNING: Output file {file_path} already exists, renaming to {old_path}")
        json_path = file_path + '.json'
        if os.path.exists(json_path):
            old_path = json_path+"_"+timestamp+".bak"
            shutil.move(json_path, old_path)
            print(f"WARNING: JSON file {json_path} already exists, renaming to {old_path}")
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
    if 'script' in settings and 'cmd' not in settings:
        # Default command if script is provided but no cmd
        cmd = settings['script']
        if cmd.endswith('.py'):
            cmd = "python3 " + cmd
        if 'cfg' in settings:
            cmd += " " + settings['cfg']
        cmd += " {output} {inputs}"
        settings['cmd'] = cmd
    if 'cmd' not in settings:
        print("ERROR: No merging command or script specified!")
        sys.exit(1)

    return settings

def write_metadata(outputs: list[dict], out_dir: str, config: dict) -> None:
    """Write file metadata to JSON files"""
    print(f"Processing output files (dir: {out_dir}):")
    valid = True
    for output in outputs:
        name = output['name']
        print(f"Processing {name}")
        # Apply per-file metadata overrides
        metadata = copy.deepcopy(config)
        metadata['metadata'].update(output.get('metadata', {}))
        metadata['name'] = name
        # Check output file existence and validity
        path = os.path.join(out_dir, name)
        if not check_file(path, rename=output.get('rename'), checklist=output.get('checklist')):
            valid = False
            continue
        # Check output file attributes
        metadata['size'] = os.path.getsize(path)
        metadata['checksums'] = checksums(path)
        # Write metadata to JSON file
        with open(path+'.json', 'w', encoding="utf-8") as fjson:
            fjson.write(json.dumps(metadata, indent=2))
    if not valid:
        print("ERROR: One or more output files failed validation!")
        sys.exit(1)
    print("All output files processed successfully")

def merge(config: dict, script_dir: str, out_dir: str) -> None:
    """Merge the input files into a single output file"""
    settings = get_settings(config, script_dir)
    inputs = config.pop('inputs')
    outputs = get_outputs(config, out_dir)

    renew_token()

    # Make local copies of the input files if not streaming
    tmp_files = []
    if not settings['streaming']:
        tmp_files = local_copy(inputs, out_dir)

    # Merge the input files based on the specified method
    out_paths = [os.path.join(out_dir, output['name']) for output in outputs]
    cmd = settings['cmd'].format(
        script=settings.get('script', ''),
        cfg=settings.get('cfg', ''),
        inputs=" ".join(inputs),
        outputs=out_paths,
        output=out_paths[0]
    )
    print(f"Merging {len(inputs)} files into {outputs[0]['name']} using method {settings['method']}")
    print(cmd)
    if settings['streaming']:
        cmd = "LD_PRELOAD=$XROOTD_LIB/libXrdPosixPreload.so " + cmd
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
    if len(sys.argv) > 2:
        out_dir = os.path.expanduser(os.path.expandvars(sys.argv[2]))
    else:
        out_dir = ''
    merge(config, script_dir, out_dir)

if __name__ == '__main__':
    main()
