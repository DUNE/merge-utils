"""Container for a set of files to be merged"""

from __future__ import annotations
import os
import collections
import logging
import subprocess
import hashlib
import math
from typing import Iterable, Generator

from merge_utils import io_utils, config, meta

logger = logging.getLogger(__name__)

class MergeFile:
    """A generic data file with metadata"""
    def __init__(self, data: dict):
        self._did = data['namespace'] + ':' + data['name']
        self.path = data.get('path', None)
        self.fid = data.get('fid', None)
        self.size = data['size']
        self.checksums = data['checksums']
        if len(self.checksums) == 0:
            logger.warning("No checksums for %s", self)
        elif 'adler32' not in self.checksums:
            logger.warning("No adler32 checksum for %s", self)
        self.metadata = data['metadata']
        self.parents = data.get('parents', [])
        self.valid = meta.validate(self._did, self.metadata)

    @property
    def did(self) -> str:
        """The file DID (namespace:name)"""
        return self._did

    @property
    def namespace(self) -> str:
        """The file namespace"""
        return self.did.split(':', 1)[0]

    @property
    def name(self) -> str:
        """The file name"""
        return self.did.split(':', 1)[1]

    @property
    def format(self):
        """The file format (core.file_format)"""
        return self.metadata['core.file_format']

    def __eq__(self, other) -> bool:
        return self.did == str(other)

    def __lt__(self, other) -> bool:
        return self.did < other.did

    def __hash__(self) -> int:
        return hash(self.did)

    def __str__(self) -> str:
        return self.did

    def get_fields(self, fields: list) -> tuple:
        """
        Get the namespace and specified metadata values from the file

        :param fields: list of metadata fields to extract
        :return: tuple of values for each field
        """
        values = [self.namespace] + [self.metadata.get(field, "") for field in fields]
        return tuple(values)

class MergeSet(collections.UserDict):
    """Class to keep track of a set of files for merging"""
    def __init__(self, files: list[MergeFile] = None):
        super().__init__()

        self.invalid = {}
        self.inconsistent = {}
        self.unreachable = {}
        self.missing = collections.defaultdict(int)

        self.consistent_fields = config.validation['consistent']
        self.field_values = None
        self.consistent = True

        self.unique = True

        if files:
            self.add_files(files)

    def set_unreachable(self, dids: Iterable[str]) -> None:
        """
        Mark files as unreachable, e.g. not found in Rucio or not accessible.

        :param dids: list of file DIDs to mark as unreachable
        """
        for did in dids:
            file = self.data.pop(did)
            file.valid = False
            self.unreachable[did] = file

    def add_file(self, file: MergeFile | dict) -> MergeFile:
        """
        Add a file to the set

        :param file: A MergeFile object or a dictionary with file metadata
        :return: the added MergeFile object, or None if it was a duplicate
        """
        if isinstance(file, dict):
            file = MergeFile(file)
        did = file.did

        # Check if the file is valid
        if not file.valid:
            self.invalid[did] = file
            return None

        # Check if the file is already in the set
        if did in self.data:
            self.unique = False
            self.data[did].count += 1
            lvl = logging.ERROR if config.validation['skip']['duplicate'] else logging.CRITICAL
            logger.log(lvl, "Found duplicate input file %s", did)
            return None

        # Actualy add the file
        self.data[did] = file
        self.data[did].count = 1
        logger.debug("Added file %s", did)

        # Check if the file metadata is consistent
        vals = file.get_fields(self.consistent_fields)
        if not self.field_values:
            # First file, so set the field values
            self.field_values = vals
        elif self.field_values != vals:
            self.consistent = False

        return file

    def add_files(self, files: Iterable) -> dict:
        """
        Add a collection of files to the set

        :param files: collection of MergeFile objects or dictionaries with file metadata
        :return: dict of MergeFile objects that were added
        """
        new_files = {}
        for file in files:
            new_file = self.add_file(file)
            if new_file is not None:
                new_files[new_file.did] = new_file
        logger.info("Added %d unique files", len(new_files))
        return new_files

    @property
    def dupes(self) -> dict:
        """Return counts of duplicate file DIDs"""
        return {did:(file.count-1) for did, file in self.data.items() if file.count > 1}

    @property
    def files(self) -> list[MergeFile]:
        """Return the list of files"""
        return sorted(self.data.values())

    def __iter__(self):
        """Iterate over the files"""
        return iter(self.files)

    @property
    def hash(self) -> str:
        """Get a hash from the list of files"""
        concat = '/'.join(sorted(self.data.keys()))
        return hashlib.sha256(concat.encode('utf-8')).hexdigest()

    @property
    def size(self) -> int:
        """Get the total size of the files"""
        return sum(file.size for file in self.data.values())

    def check_validity(self, final: bool = False) -> bool:
        """
        Check if the files in the set are valid and log any invalid files.
        
        :param final: print final summary of invalid files even if bad files are allowed
        :return: True if all files are valid, False otherwise
        """
        skip = config.validation['skip']['invalid']
        if len(self.invalid) == 0 or (skip and not final):
            return True
        lvl = logging.ERROR if skip else logging.CRITICAL
        io_utils.log_list("Found {n} file{s} with invalid metadata:", self.invalid, lvl)
        return skip

    def check_consistency(self, final: bool = False) -> bool:
        """
        Check if the files in the set have consistent namespaces and metadata fields.
        If not, log the inconsistencies and move the inconsistent files out of the set.

        :param final: do final check and log even if bad files are allowed
        :return: True if the files are consistent, False otherwise
        """
        skip = config.validation['skip']['inconsistent']
        if self.consistent or (skip and not final):
            return True
        lvl = logging.ERROR if skip else logging.CRITICAL

        # Figure out the most common values for the checked fields
        counts = collections.defaultdict(int)
        for file in self.files:
            counts[file.get_fields(self.consistent_fields)] += 1
        self.field_values = max(counts, key=counts.get)
        n_errs = len(self.data) - counts[self.field_values]
        s_errs = "s" if n_errs != 1 else ""
        errs = [f"Found {n_errs} file{s_errs} in set with inconsistent metadata:"]

        for file in self.files:
            values = file.get_fields(self.consistent_fields)
            if values != self.field_values:
                file.valid = False
                file_errs = []
                if values[0] != self.field_values[0]:
                    v1 = f"'{values[0]}'" if values[0] else "None"
                    v2 = f"'{self.field_values[0]}'" if self.field_values[0] else "None"
                    file_errs.append(f"  namespace: {v1} (expected {v2})")
                for i, key in enumerate(self.consistent_fields, start=1):
                    if values[i] != self.field_values[i]:
                        v1 = f"'{values[i]}'" if values[i] else "None"
                        v2 = f"'{self.field_values[i]}'" if self.field_values[i] else "None"
                        file_errs.append(f"  {key}: {v1} (expected {v2})")
                n_errs = len(file_errs)
                s_errs = "s" if n_errs != 1 else ""
                errs.append(f"File {file.did} has {n_errs} bad key{s_errs}:")
                errs.extend(file_errs)
        logger.log(lvl, "\n  ".join(errs))

        # Move inconsistent files out of the set
        self.inconsistent = {did: file for did, file in self.data.items() if not file.valid}
        self.data = {did: file for did, file in self.data.items() if file.valid}

        return skip

    def check_reachability(self, final: bool = False) -> bool:
        """
        Check if the files in the set are reachable and log any unreachable files.
        
        :param final: print final summary of unreachable files even if bad files are allowed
        :return: True if all files are reachable, False otherwise
        """
        skip = config.validation['skip']['unreachable']
        if len(self.unreachable) == 0 or (skip and not final):
            return True
        lvl = logging.ERROR if skip else logging.CRITICAL
        io_utils.log_list("Found {n} unreachable file{s}:", self.unreachable, lvl)
        return skip

    def check_missing(self, final: bool = False) -> bool:
        """
        Check if any files were missing metadata.
        
        :param final: print final summary of missing files even if bad files are allowed
        :return: True if all files have metadata, False otherwise
        """
        skip = config.validation['skip']['missing']
        if len(self.missing) == 0 or (skip and not final):
            return True
        lvl = logging.ERROR if skip else logging.CRITICAL
        io_utils.log_dict("Failed to retrieve data for {n} file{s}:", self.missing, lvl)
        return skip

    def check_uniqueness(self, final: bool = False) -> bool:
        """
        Check if the files in the set are unique and log any duplicate files.
        
        :param final: print final summary of duplicate files even if bad files are allowed
        :return: True if all files are unique, False otherwise
        """
        skip = config.validation['skip']['duplicate']
        if self.unique or (skip and not final):
            return True
        lvl = logging.ERROR if skip else logging.CRITICAL
        io_utils.log_dict("Found {n} duplicated file{s}:", self.dupes, lvl)
        return skip

    def check_errors(self, final: bool = False) -> bool:
        """
        Check for errors in the set and log them.
        
        :param final: print final summary of errors even if bad files are allowed
        :return: True if unskipped errors were found, False otherwise
        """
        return not all([
            self.check_missing(final),
            self.check_validity(final),
            self.check_consistency(final),
            self.check_reachability(final),
            self.check_uniqueness(final)
        ])

    def group_count(self) -> list[int]:
        """Group input files by count"""
        target_size = config.merging['target_size']
        total_size = len(self.data)
        if total_size < target_size:
            logger.info("Merging %d inputs into 1 group", len(self.data))
            return [len(self.data)]

        if config.merging['equalize']:
            n_groups = math.ceil(total_size / target_size)
            target_size = total_size / n_groups
            divs = [round(i*target_size) for i in range(1, n_groups)]
        else:
            divs = list(range(target_size, total_size, target_size))
        divs.append(len(self.data))
        logger.info("Merging %d inputs into %d groups of %d files",
                        len(self.data), len(divs), target_size)
        return divs

    def group_size(self) -> list[int]:
        """Group input files by size"""
        target_size = config.merging['target_size'] * 1024**3
        total_size = self.size
        if total_size < target_size:
            logger.info("Merging %d inputs into 1 group", len(self.data))
            return [len(self.data)]

        avg_size = total_size / len(self.data)
        if avg_size > target_size / config.merging['chunk_min']:
            logger.error("%.2f GB input files are too large to merge into %.2f GB groups",
                            avg_size/1024**3, target_size/1024**3)
            return []

        size = 0
        divs = []
        if config.merging['equalize']:
            max_size = target_size
            n_groups = math.ceil(total_size / target_size)
            target_size = total_size / n_groups
            err = 0
            for idx, file in enumerate(self.files):
                # Try to get just above target_size without exceeding max_size
                if size >= target_size - err or size + file.size > max_size:
                    divs.append(idx)
                    err += size - target_size # distribute error across groups
                    size = 0
                size += file.size
        else:
            for idx, file in enumerate(self.files):
                if size + file.size > target_size:
                    divs.append(idx)
                    size = 0
                size += file.size

        divs.append(len(self.data))
        logger.info("Merging %d inputs into %d groups of %.2f GB",
                        len(self.data), len(divs), target_size / 1024**3)
        return divs

    def groups(self) -> Generator[dict, None, None]:
        """Split the files into groups for merging"""
        # Get merged metadata
        merge_meta = meta.merged_keys(self.data, warn = True)
        common = meta.common_keys(self.data, warn = True) # this goes into output dataset metadata in future

        # Figure out the merge method if not already set
        if config.merging['method'] == 'auto':
            fmt = merge_meta['core.file_format']
            for method, cfg in config.merging['methods'].items():
                if fmt in cfg['file_format']:
                    config.merging['method'] = method
                    logger.info("Using merge method '%s' for file format '%s'", method, fmt)
                    break

        merge_name = meta.make_name(merge_meta)
        merge_hash = self.hash

        # Get the group divisions
        if config.merging['target_mode'] == 'count':
            divs = self.group_count()
        elif config.merging['target_mode'] == 'size':
            divs = self.group_size()
        else:
            logger.error("Unknown target mode: %s", config.merging['target_mode'])
            return
        if len(divs) == 0:
            return

        # Actually output the groups
        group_id = 0
        group = MergeChunk(merge_name, merge_hash)
        if len(divs) > 1:
            group.group_id = group_id
        for i, file in enumerate(self.files):
            group.add(file)
            # Check if we need to yield the group
            if i+1 == divs[group_id]:
                logger.debug("Yielding group %d with %d files", group_id, len(group))
                yield group
                group_id += 1
                group = MergeChunk(merge_name, merge_hash, group=group_id)


class MergeChunk(collections.UserDict):
    """Class to keep track of a chunk of files for merging"""
    def __init__(self, name: str, merge_hash: str, group: int = -1):
        super().__init__()
        self._name = name
        self.namespace = config.output['namespace']
        self.merge_hash = merge_hash
        self.site = None
        self.group_id = group
        self.chunk_id = -1
        self.chunks = 0

    @property
    def name(self) -> str:
        """The name of the chunk"""
        the_name, ext = self._name.split('.', 1)
        if self.group_id >= 0:
            the_name = f"{the_name}_f{self.group_id}"
        if self.chunk_id >= 0:
            the_name = f"{the_name}_c{self.chunk_id}"
        return f"{the_name}.{ext}"

    @property
    def tier(self) -> int:
        """Get the pass number for the chunk"""
        if self.chunks == 0:
            return 1
        return 2

    @property
    def inputs(self) -> list[str]:
        """Get the list of input files"""
        if self.chunks == 0:
            return [file.path for file in self.data.values()]
        the_name, ext = self.name.split('.', 1)
        output_dir = config.output['dir']
        return [os.path.join(output_dir, f"{the_name}_c{idx}.{ext}") for idx in range(self.chunks)]

    @property
    def metadata(self) -> dict:
        """Get the metadata for the chunk"""
        md = meta.merged_keys(self.data)
        md['merge.method'] = config.merging['method']
        md['merge.hash'] = self.merge_hash
        if config.merging['method'] == 'lar':
            md['merge.fcl'] = config.merging['methods']['lar']['fcl']
        if self.group_id >= 0:
            md['merge.group'] = self.group_id
        if self.chunk_id >= 0:
            md['merge.chunk'] = self.chunk_id
        return md

    @property
    def parents(self) -> list[str]:
        """Get the list of parent dids"""
        return meta.parents(self.data)

    @property
    def json(self) -> dict:
        """Get the chunk metadata as a JSON-compatible dictionary"""
        data = {
            'name': self.name,
            'namespace': self.namespace,
            'metadata': self.metadata,
            'parents': self.parents,
            'inputs': self.inputs,
        }
        return data

    def add(self, file: MergeFile) -> None:
        """Add a file to the chunk"""
        self.data[file.did] = file
        if self.namespace is None:
            self.namespace = file.namespace

    def chunk(self) -> MergeChunk:
        """Create a subset of the chunk with the same metadata"""
        chunk = MergeChunk(self.name, self.merge_hash, self.group_id)
        chunk.site = self.site
        chunk.chunk_id = self.chunks
        self.chunks += 1
        return chunk


def check_remote_path(path: str, timeout: float = 5) -> bool:
    """Check if a remote path is accessible via xrootd"""
    components = path.split('/', 3)
    url = '/'.join(components[0:3])
    path = '/'+components[3]
    cmd = ['xrdfs', url, 'ls', '-l', path]
    try:
        ret = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.debug("Timeout accessing %s%s", url, path)
        return False
    if ret.returncode == 51:
        logger.debug("Invalid xrootd server %s", url)
        return False
    if ret.returncode == 54:
        logger.debug("No such file %s%s", url, path)
        return False
    if ret.returncode != 0:
        logger.debug("Failed to access %s%s\n  %s", url, path, ret.stderr.strip().split(' ', 1)[1])
        return False
    return True
