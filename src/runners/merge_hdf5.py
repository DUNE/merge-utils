"""Merge multiple HDF5 files into a single HDF5 file."""

import sys
import re
import os
import collections
from datetime import datetime, timezone
import numpy
import yaml
import h5py

cfg = {}
divisions = {}
cleanup = {}
inconsistent = {}

class AttrMin:
    """Merge attributes by taking the minimum value."""
    def __init__(self):
        self.value = float('inf')

    def add(self, value):
        """Add a new value to the metadata."""
        self.value = min(self.value, value)
        return True

    @property
    def valid(self):
        """Check if the value is valid."""
        return self.value != float('inf')

class AttrMax:
    """Merge attributes by taking the maximum value."""
    def __init__(self):
        self.value = -float('inf')

    def add(self, value):
        """Add a new value to the metadata."""
        self.value = max(self.value, value)
        return True

    @property
    def valid(self):
        """Check if the value is valid."""
        return self.value != -float('inf')

class AttrSum:
    """Merge attributes by adding the values."""
    def __init__(self):
        self.value = 0

    def add(self, value):
        """Add a new value to the metadata."""
        self.value += value
        return True

    @property
    def valid(self):
        """Check if the value is valid."""
        return self.value != 0

class AttrUnique:
    """Merge attributes by taking the unique values."""
    def __init__(self, value=None):
        self.value = value
        self._valid = True

    def add(self, value):
        """Add a new value to the metadata."""
        if self.value is None:
            self.value = value
        elif isinstance(value, numpy.ndarray):
            if not numpy.array_equal(self.value, value):
                self._valid = False
        elif self.value != value:
            self._valid = False
        return self._valid

    @property
    def valid(self):
        """Check if the value is valid."""
        return self._valid and self.value is not None

class AttrList:
    """Merge attributes by making a list of values."""
    def __init__(self):
        self.value = []

    def add(self, value):
        """Add a new value to the metadata."""
        if isinstance(value, list):
            self.value.extend(value)
        else:
            self.value.append(value)
        return True

    @property
    def valid(self):
        """Check if the value is valid."""
        return len(self.value) > 0

class AttrOverride:
    """Merge attributes by overriding the value."""
    def __init__(self, value=None):
        self.value = value

    def add(self, value): # pylint:disable=unused-argument
        """Add a new value to the metadata."""
        return True  # Always return True, as this is an override

    @property
    def valid(self):
        """Check if the value is valid."""
        return self.value is not None

ATTR_CLASSES = {
    'unique': AttrUnique,
    'list': AttrList,
    'min': AttrMin,
    'max': AttrMax,
    'sum': AttrSum,
}

def get_cfg(cfg_dict: dict, path: str) -> dict:
    """
    Get configuration keys that match a given path
    
    :param cfg_dict: Dictionary containing configuration settings
    :param path: Path to the object in the HDF5 file
    :return: Dictionary with matching keys
    """
    out = {}
    path_arr = path.split('/')
    # Check if any keys match the path
    for key, value in cfg_dict.items():
        key_arr = key.split('/')
        if key_arr[-1] in out:
            continue
        if len(key_arr) != len(path_arr) + 1:
            continue
        if all(re.fullmatch(k, p) for k, p in zip(key_arr[:-1], path_arr)):
            out[key_arr[-1]] = value
    return out

def merge_attrs(path: str, attrs: list) -> dict:
    """
    Check if the list of attributes is consisistent across all files.
    
    :param path: Path to the object in the HDF5 file
    :param attrs: List of attributes to check
    :return: Dictionary with consistent attributes
    """
    merged = collections.defaultdict(AttrUnique)
    # Check merging mode
    for key, value in get_cfg(cfg['attrs']['mode'], path).items():
        if value in ATTR_CLASSES:
            merged[key] = ATTR_CLASSES[value]()
        else:
            raise ValueError(f"Unknown attribute mode: {value} for key {key}")
    # Check overrides
    for key, value in get_cfg(cfg['attrs']['overrides'], path).items():
        merged[key] = AttrOverride(value)
    # Special keys will be assigned after merging, set to None for now
    for key, value in get_cfg(cfg['attrs']['special'], path).items():
        merged[key] = AttrOverride(None)
        cleanup[f"{path}/{key}"] = value

    # Merge attributes
    errs = {}
    for attr in attrs:
        for key, value in attr.items():
            if not merged[key].add(value):
                if key in errs:
                    errs[key].append(value)
                else:
                    errs[key] = [merged[key].value, value]

    # Add inconsistent attributes to the inconsistent dict
    for key, values in errs.items():
        inconsistent[f"{path}/{key}"] = values

    # Return all valid attributes
    return {k: v.value for k, v in merged.items() if v.valid}

def get_axis(dataset) -> int:
    """
    Get the scaling axis for a given dataset.
    
    :param dataset: Dataset object
    :return: Axis index
    """
    name = dataset.name
    shape = dataset.shape

    if len(shape) == 1: # 1D datasets only have one axis
        return 0

    axis = 0
    axes = get_cfg(cfg['datasets']['axis'], name)
    if len(shape) > 1:
        if name in axes:
            axis = axes[name]
        elif f"{len(shape)}D" in cfg['datasets']['axis']:
            axis = cfg['datasets']['axis'][f"{len(shape)}D"]
        else:
            axis = cfg['datasets']['axis'].get('default', 0)
    if axis >= len(shape):
        print(f"Warning: Axis {axis} is out of bounds for dataset '{name}' with shape {shape}. Using 0 instead.")
        axis = 0
    return axis

def merge_dataset(fout: str, datasets: list) -> None:
    """
    Merge a list of datasets into the output file.
    
    :param fout: Output file handle
    :param datasets: List of datasets to merge
    """
    name = datasets[0].name
    shape = datasets[0].shape
    dtype = datasets[0].dtype
    axis = get_axis(datasets[0])

    name_arr = name.split('/')
    dim_str = f" ({axis})" if axis else ""
    print(f"{'. '*(len(name_arr)-2)}{name_arr[-1]}\t\tF={len(datasets)}, S={shape}{dim_str}, T={dtype}")
    attrs = merge_attrs(name, [d.attrs for d in datasets])

    if len(datasets) == 1:
        # If only one dataset, just copy it
        fout.copy(datasets[0], name, without_attrs=True)
        new_dset = fout[name]
        new_dset.attrs.update(attrs)
        return

    # Check shape and dtype consistency, and find divisions
    divs = [0, shape[axis]]
    for dataset in datasets[1:]:
        if dataset.dtype != dtype:
            raise ValueError(f"Inconsistent dtype for dataset '{name}': {dtype} vs {dataset.dtype}")
        for i, s in enumerate(dataset.shape):
            if i == axis:
                divs.append(divs[-1] + s)
            elif s != shape[i]:
                raise ValueError(f"Inconsistent shape for dataset '{name}': {shape} vs {dataset.shape}")

    # Create new merged dataset
    shape = list(shape)
    shape[axis] = divs[-1]
    shape = tuple(shape)
    if datasets[0].maxshape[axis] is None or datasets[0].maxshape[axis] >= shape[axis]:
        fout.copy(datasets[0], name, without_attrs=True)
        new_dset = fout[name]
        new_dset.resize(shape)
    else:
        new_dset = fout.create_dataset_like(name, datasets[0], shape=shape)
    new_dset.attrs.update(attrs)
    insert_slice = [slice(None)] * len(shape)
    for i, dataset in enumerate(datasets):
        insert_slice[axis] = slice(divs[i], divs[i+1])
        new_dset[tuple(insert_slice)] = dataset[:]

    divisions[name] = divs[1:]

def merge_group(fout: str, groups: list) -> None:
    """
    Merge a list of groups into the output file.
    
    :param fout: Output file handle
    :param groups: List of groups to merge
    """
    name = groups[0].name
    if name == '/':
        fout.attrs.update( merge_attrs('', [g.attrs for g in groups]) )
    else:
        name_arr = name.split('/')
        print(f"{'. '*(len(name_arr)-2)}{name_arr[-1]}\t\tF={len(groups)}")
        fout.create_group(name)
        fout[name].attrs.update( merge_attrs(name, [g.attrs for g in groups]) )
    for i, group in enumerate(groups):
        for val in group.values():
            # Skip values we have already copied
            if val.name in fout:
                continue
            # See if the same value appears in multiple files
            vals = [val] + [f[val.name] for f in groups[i+1:] if val.name in f]
            if isinstance(val, h5py.Group):
                merge_group(fout, vals)
            elif isinstance(val, h5py.Dataset):
                merge_dataset(fout, vals)

def merge_hdf5(output: str, inputs: list, config: str) -> None:
    """Merge the input hdf5 files"""
    creation_time = datetime.now(timezone.utc)
    with open(config, encoding="utf-8") as f:
        cfg.update(yaml.safe_load(f))
    print(f"Oputput file: {output}")
    print(f"Input files: {inputs}")
    print(f"Configuration: {cfg}")
    fout = h5py.File(output, 'w')

    # Merge all the input files
    # copy files locally
    fins = [h5py.File(f, 'r') for f in inputs]
    fout.attrs.update( merge_attrs('', [f.attrs for f in fins]) )
    merge_group(fout, fins)
    for f in fins:
        f.close()
        # remove the local copy

    # Warn about inconsistent attributes
    if inconsistent:
        print(f"Warning: Omitted {len(inconsistent)} inconsistent attributes:")
        for attr, values in inconsistent.items():
            print(f"  {attr}:")
            for value in values:
                print(f"    {value}")

    # Set special attributes
    closing_time = datetime.now(timezone.utc)
    special_vals = {
        'creation_time': str(int(creation_time.timestamp()*1000)),
        'closing_time': str(int(closing_time.timestamp()*1000)),
        'creation_time_str': creation_time.strftime("%Y%m%dT%H%M%S"),
        'closing_time_str': closing_time.strftime("%Y%m%dT%H%M%S"),
        'file_size': os.path.getsize(output),
        'file_index': 0  # Assuming single output file, index is 0
    }
    for attr, value in cleanup.items():
        attr_arr = attr.split('/')
        path = '/'.join(attr_arr[:-1])
        if not path:
            path = '/'
        if value in special_vals:
            fout[path].attrs[attr_arr[-1]] = special_vals[value]
        else:
            raise ValueError(f"Unknown special attribute {value} for path {path}")

    fout.close()

if __name__ == "__main__":
    cfg_file = sys.argv[1]
    output_file = sys.argv[2]
    input_files = sys.argv[3:]
    merge_hdf5(output_file, input_files, cfg_file)
