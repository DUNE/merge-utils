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
import h5py
import ROOT

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

def list_root(folder, base="") -> list:
    """
    List all contents of a ROOT file recursively.

    :param dir: ROOT TDirectoryFile or TFile to list
    :param base: Base path for the current directory
    :return: list of object paths
    """
    contents = []
    for key in folder.GetListOfKeys():
        obj_name = key.GetName()
        full_path = os.path.join(base, obj_name)
        contents.append(full_path)
        #if key.IsFolder():
        if key.GetClassName() in ['TDirectoryFile', 'TDirectory']:
            subdir = folder.Get(obj_name)
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

def check_exists(path: str, rename: str = None) -> bool:
    """
    Check if an output file exists, renaming it from a temporary name if needed.

    :param path: Path to the output file
    :param rename: If provided, rename this file to the new name
    :return: True if the output file exists
    """
    if rename is not None:
        if os.path.isfile(rename):
            shutil.move(rename, path)
        else:
            print(f"ERROR: Expected output file {rename} not found!")
            return False
    if not os.path.isfile(path):
        print(f"ERROR: Output file {os.path.basename(path)} not found!")
        return False
    return True

def check_contents(path: str, checklist: str = None) -> bool:
    """
    Check that an output file has the expected contents.
    The default behavior is to simply check that the file is not empty
    If a checklist file is provided, it will ensure all listed items are present in the output
    Setting checklist to 'skip' will skip the check entirely
    Setting checklist to 'auto' will look for a file named <output>.checklist

    :param path: Path to the file
    :param checklist: Expected contents checklist, or 'skip' or 'auto' for special behavior
    :return: True if the file is valid, False otherwise
    """
    name = os.path.basename(path)
    if checklist == 'skip':
        print(f"WARNING: Skipping content check for {name}")
        return True
    # Read file contents
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext in ['.root']:
            root_file = ROOT.TFile.Open(path, "READ")
            if not root_file or root_file.IsZombie():
                print(f"ERROR: Failed to open ROOT file {name}")
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
            print(f"WARNING: Output file {name} has unknown type {ext}, skipping content check")
            return True
    except Exception as e:
        print(f"ERROR: Failed to read file {name}: {e}")
        return False
    # Check for empty file
    if len(contents) == 0:
        print(f"ERROR: File {name} is empty")
        return False
    if checklist is None:
        return True
    # Check for expected contents
    if checklist == 'auto':
        checklist = path + '.checklist'
        if not os.path.isfile(checklist):
            print(f"ERROR: Expected checklist file {checklist} not found!")
            return False
    missing = []
    with open(checklist, 'r', encoding="utf-8") as f:
        for line in f:
            item = line.strip()
            if item and item not in contents:
                missing.append(item)
    if len(missing) > 0:
        print(f"ERROR: File {name} is missing expected contents:")
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

def get_outputs(config: dict, script_dir: str, out_dir: str) -> list[dict]:
    """Get the output file list from the config, renaming existing files if needed"""
    outputs = config.pop('outputs')
    if len(outputs) == 0:
        print("ERROR: No output files specified!")
        sys.exit(1)
    # Check output file specs
    errors = False
    for i, output in enumerate(outputs):
        checklist = output.get('checklist')
        if checklist and checklist not in ['skip', 'auto']:
            checklist = os.path.join(script_dir, checklist)
            if not os.path.isfile(checklist):
                print(f"ERROR: Output {i} checklist file {checklist} does not exist!")
                errors = True
            else:
                output['checklist'] = checklist
    if errors:
        sys.exit(1)
    # Rename existing output files
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    for i, output in enumerate(outputs):
        file_path = os.path.join(out_dir, output['name'])
        if os.path.exists(file_path):
            old = file_path+"_"+timestamp+".bak"
            shutil.move(file_path, old)
            print(f"WARNING: Output {i} data file {file_path} already exists, renaming to {old}")
        json_path = file_path + '.json'
        if os.path.exists(json_path):
            old = json_path+"_"+timestamp+".bak"
            shutil.move(json_path, old)
            print(f"WARNING: Output {i} JSON file {json_path} already exists, renaming to {old}")
    return outputs

def write_metadata(outputs: list[dict], out_dir: str, config: dict) -> None:
    """Write file metadata to JSON files"""
    valid = True
    for output in outputs:
        name = output['name']
        print(f"Processing output file {name}")
        # Rename output file if needed and make sure it exists
        path = os.path.join(out_dir, name)
        if not check_exists(path, rename=output.get('rename')):
            valid = False
            continue
        # Check file size
        size = os.path.getsize(path)
        if size <= output.get('size', 0):
            print(f"ERROR: Output file {name} is too small! ({size} <= {output.get('size', 0)})")
            valid = False
            continue
        # Make sure the output file is readable and has the expected contents
        if not check_contents(path, output.get('checklist')):
            valid = False
            continue
        # Apply per-file metadata overrides
        metadata = copy.deepcopy(config)
        metadata['metadata'].update(output.get('metadata', {}))
        metadata['name'] = name
        metadata['size'] = size
        metadata['checksums'] = checksums(path)
        # Write metadata to JSON file
        with open(path+'.json', 'w', encoding="utf-8") as fjson:
            fjson.write(json.dumps(metadata, indent=2))
    if not valid:
        print("ERROR: One or more output files failed validation!")
        sys.exit(1)

def merge(config: dict, script_dir: str, out_dir: str) -> None:
    """Merge the input files into a single output file"""
    settings = get_settings(config, script_dir)
    inputs = config.pop('inputs')
    outputs = get_outputs(config, script_dir, out_dir)

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
    print(f"Merging {len(inputs)} files into {outputs[0]['name']} with method {settings['method']}")
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
