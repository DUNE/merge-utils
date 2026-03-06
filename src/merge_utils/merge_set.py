"""Container for a set of files to be merged"""

from __future__ import annotations
import os
import sys
import collections
import logging
import math
import enum
import copy
from typing import Iterable, Generator

from merge_utils import io_utils, config, meta

logger = logging.getLogger(__name__)

class MergeFileError(enum.Flag):
    """Enumeration of possible file error flags"""
    DUPLICATE    = enum.auto()
    NO_METADATA  = enum.auto()
    UNDECLARED   = enum.auto()
    RETIRED      = enum.auto()
    INVALID      = enum.auto()
    NO_REPLICAS  = enum.auto()
    UNREACHABLE  = enum.auto()
    INCONSISTENT = enum.auto()

    @property
    def first(self) -> MergeFileError:
        """Get the first error in the enumeration"""
        err = MergeFileError(self.value & -self.value)
        return err

    @property
    def handling(self) -> str:
        """Get the error handling method from the configuration"""
        if self == MergeFileError(0):
            return 'good'
        err_name = self.first.name
        assert err_name is not None
        return config.validation.handling[err_name.lower()]

    @property
    def group(self) -> bool:
        """Check if the file should count towards grouping"""
        return self.handling in ['good', 'gap']

    @classmethod
    def critical(cls) -> MergeFileError:
        """Get the set of errors that are considered critical"""
        crit = cls(0)
        for err in cls:
            err_name = err.name
            assert err_name is not None
            if config.validation.handling[err_name.lower()] == 'quit':
                crit |= err
        return crit

ERROR_MESSAGES = {
    MergeFileError.DUPLICATE:   "Found {n} duplicated file{s}:",
    MergeFileError.NO_METADATA: "Found {n} file{s} with missing metadata:",
    MergeFileError.UNDECLARED:  "Found {n} file{s} with undeclared metadata:",
    MergeFileError.RETIRED:     "Found {n} retired file{s}:",
    MergeFileError.INVALID:     "Found {n} file{s} with invalid metadata:",
    MergeFileError.NO_REPLICAS: "Found {n} file{s} without replicas:",
    MergeFileError.UNREACHABLE: "Found {n} file{s} without reachable replicas:"
}

class MergeFile:
    """A generic data file with metadata"""

    def __init__(self, data: dict):
        """Initialize the MergeFile with a metadata dictionary"""
        # Set name and check for errors
        self._did = f"{data['namespace']}:{data['name']}"
        self.errors = data.get('errors', MergeFileError(0))
        if self.errors:
            return
        # Set FIDs and check for undeclared files
        self.fid = data.get('fid', None)
        self.parents = set()
        if config.output.grandparents:
            self.set_parents(data.get('parents', []))
        elif self.fid is None:
            self.errors |= MergeFileError.UNDECLARED
        if self.errors:
            return
        # Check if the file is retired
        if data.get('retired', False):
            self.errors |= MergeFileError.RETIRED
            return
        # Set other metadata and validate
        self.replicas = []
        self.size = data.get('size', None)
        self.checksums = data['checksums']
        self.metadata = data['metadata']
        self.validate()

    def set_parents(self, parents: Iterable) -> None:
        """Set the parent FIDs for the file, checking for any missing FIDs"""
        self.parents = set()
        missing = set()
        for parent in parents:
            fid = parent.get('fid')
            if fid:
                self.parents.add(fid)
                continue
            if 'did' in parent:
                missing.add(parent['did'])
            elif 'namespace' in parent and 'name' in parent:
                missing.add(f"{parent['namespace']}:{parent['name']}")
            else:
                missing.add(str(parent))
        if missing:
            self.errors |= MergeFileError.UNDECLARED
            io_utils.log_list("File %s has {n} parent{s} without an FID:" % self.did,
                              list(missing), logging.ERROR)

    def validate(self) -> None:
        """Check for errors or invalid metadata"""
        if not self.size:
            logger.error("No size for %s", self)
            self.errors |= MergeFileError.INVALID
            return
        if len(self.checksums) == 0:
            logger.error("No checksums for %s", self)
            self.errors |= MergeFileError.INVALID
            return
        algos = set(str(algo) for algo in config.validation.checksums)
        self.checksums = {algo: csum for algo, csum in self.checksums.items() if algo in algos}
        if len(self.checksums) == 0:
            logger.warning("No valid checksum for %s", self)
            self.errors |= MergeFileError.INVALID
            return
        if not meta.validate(self.did, self.metadata):
            self.errors |= MergeFileError.INVALID

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
    def file_format(self):
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
        values = [self.namespace]
        for field in fields:
            value = self.metadata.get(field, "")
            try:
                hash(value)
            except TypeError:
                value = str(value)
            values.append(value)
        return tuple(values)

class MergeSet:
    """Class to keep track of a set of files for merging"""

    def __init__(self):
        self._files = []
        self.start_idx = config.input.skip or 0
        self.dids = {}
        self.errors = MergeFileError(0)
        self.consistent_fields = None

    @property
    def end_idx(self) -> int:
        """Get the index of the end of the set (one past the last file)"""
        return self.start_idx + len(self._files)

    def __len__(self) -> int:
        """Get the number of files in the set"""
        return len([f for f in self._files if f is not None])

    def get_by_idx(self, idx: int) -> MergeFile | None:
        """
        Get a file by its index in the set.

        :param idx: index of the file
        :return: MergeFile object or None if not found
        """
        if idx < 0:
            raise IndexError("MergeSet indices must be non-negative")
        idx -= self.start_idx
        if idx < 0 or idx >= len(self._files):
            return None
        return self._files[idx]

    def at(self, idx: int) -> MergeFile:
        """
        Get a file by its index in the set, raising an error if not found.

        :param idx: index of the file
        :return: MergeFile object
        """
        file = self.get_by_idx(idx)
        if file is None:
            raise KeyError(f"File at index {idx} is None")
        return file

    def get_by_did(self, did: str) -> MergeFile:
        """
        Get a file by its DID.

        :param did: DID of the file
        :return: MergeFile object or None if not found
        """
        idx = self.dids.get(did, None)
        if idx is None:
            raise KeyError(f"Unknown file DID: {did}")
        out = self._files[idx - self.start_idx]
        if out is None:
            raise KeyError(f"File DID {did} at index {idx} is None")
        return out

    def get_slice(self, start: int = 0, end: int = 0, step: int = 1) -> list[MergeFile]:
        """
        Get a slice of files by their indices.

        :param start: starting index of the slice
        :param end: ending index of the slice (exclusive)
        :param step: step size for the slice
        :return: list of MergeFile objects
        """
        start = start or self.start_idx
        end = end or self.end_idx
        step = step or 1
        if start < 0 or end < 0:
            raise IndexError("MergeSet indices must be non-negative")
        files = []
        for idx in range(start, end, step):
            file = self.get_by_idx(idx)
            if file is not None:
                files.append(file)
        return files

    def insert(self, idx: int, file: MergeFile) -> None:
        """
        Insert a file at the specified index.

        :param idx: index of the file
        :param file: MergeFile object to set at the index
        """
        # Index must be non-negative
        if idx < 0:
            raise IndexError(f"Index {idx} is out of bounds for setting file")
        # If the index is before the start index, shift the existing files and pad with None
        if idx < self.start_idx:
            pad = self.start_idx - idx - 1
            logger.debug("Shifting MergeSet start by %d to add file at index %d", pad + 1, idx)
            self._files = [file] + [None]*pad + self._files
            self.start_idx = idx
        # If the index is beyond the current list, extend the list and pad with None
        elif idx >= self.end_idx:
            pad = idx - self.end_idx
            logger.debug("Extending MergeSet end by %d to add file at index %d", pad + 1, idx)
            self._files.extend([None]*pad + [file])
        # Otherwise, just set the file at the index
        else:
            old_file = self._files[idx - self.start_idx]
            if old_file is not None:
                raise IndexError(f"MergeSet index {idx} already contains file {old_file.did}")
            logger.debug("Inserting file into MergeSet at index %d", idx)
            self._files[idx - self.start_idx] = file
        # Add to the DID index if the file is not None and not a duplicate
        if file is None:
            return
        did = file.did
        if did in self.dids:
            file.errors |= MergeFileError.DUPLICATE
        else:
            self.dids[did] = idx
        # Check for errors and update good files
        if file.errors:
            self.errors |= file.errors
            return
        # Check for consistency
        if self.consistent_fields is None:
            self.consistent_fields = file.get_fields(config.metadata.consistent)
        elif MergeFileError.INCONSISTENT not in self.errors:
            if file.get_fields(config.metadata.consistent) != self.consistent_fields:
                self.errors |= MergeFileError.INCONSISTENT

    def add(self, skip: int, files: Iterable) -> list:
        """
        Add a batch of files to the set.

        :param files: collection of dictionaries with file metadata
        :return: list of MergeFile objects that were added without errors
        """
        new_files = []
        for idx, file in enumerate(files, start=skip):
            new_file = MergeFile(file)
            self.insert(idx, new_file)
            if not new_file.errors:
                new_files.append(new_file)
        logger.info("Added %d valid files from batch %d", len(new_files), skip)
        return new_files

    @property
    def good_files(self) -> list:
        """List of MergeFile objects for files without errors"""
        return [f for f in self._files if f and not f.errors]

    def set_error(self, dids: Iterable[str], error: MergeFileError) -> None:
        """
        Mark files as having a specific error.

        :param dids: list of file DIDs to mark
        :param error: MergeFileError to set
        """
        if error == MergeFileError(0):
            raise ValueError("Cannot set empty error on files")
        err_count = 0
        for did in dids:
            file = self.get_by_did(did)
            file.errors |= error
            err_count += 1
        if err_count > 0:
            err_name = str(error).rsplit('.', 1)[-1]
            logger.debug("Flagged %d file%s as %s",
                         err_count, "s" if err_count != 1 else "", err_name)
            self.errors |= error

    def check_consistency(self) -> list[str]:
        """
        Pick out the largest consistent subset of files and log any inconsistencies.

        :return: list of log messages about inconsistent files
        """
        # Group good files by their checked field values
        checked_fields = config.metadata.consistent
        groups = collections.defaultdict(list)
        for idx, file in enumerate(self._files, start=self.start_idx):
            if not file or file.errors:
                continue
            groups[file.get_fields(checked_fields)].append((file.did, idx))
        # Find the largest consistent group
        groups = sorted(groups.items(), key=lambda k: len(k[1]), reverse=True)
        self.consistent_fields = groups[0][0]
        good = dict(groups.pop(0)[1])
        # Mark other files as inconsistent and log errors
        if len(groups) == 0:
            logger.info("All good files have consistent metadata, clearing inconsistency flag")
            self.errors &= ~MergeFileError.INCONSISTENT
            return []
        msg = [
            f"Found {len(groups)+1} file groups with inconsistent metadata:",
            f"Group 1 ({len(good)} file{'s' if len(good) != 1 else ''}) metadata:"
        ]
        field_names = ['namespace'] + config.metadata.consistent
        for field, value in zip(field_names, self.consistent_fields):
            msg.append(f"  {field}: '{value}'")
        for gid, (fields, group) in enumerate(groups, start=2):
            msg.append(f"Group {gid} ({len(group)} file{'s' if len(group) > 1 else ''}):")
            for did, idx in group:
                msg.append(f"  {did}")
                self.at(idx).errors |= MergeFileError.INCONSISTENT
            msg.append(f"Group {gid} metadata inconsistencies:")
            for field, good_val, bad_val in zip(field_names, self.consistent_fields, fields):
                if good_val == bad_val:
                    continue
                msg.append(f"  {field}: '{bad_val}' (expected '{good_val}')")
        return msg

    def check_errors(self, final: bool = False) -> None:
        """
        Check and log errors in the set.
        
        :param final: print final summary of errors even if bad files are allowed
        """
        if not self.errors:
            return
        # Check if we need to abort due to critical errors
        fast_fail = config.validation.fast_fail
        critical_errors = MergeFileError.critical()
        abort = bool(self.errors & critical_errors) and (final or fast_fail)
        if not final and not abort:
            return
        # Do final consistency check if needed
        inconsistencies = []
        if MergeFileError.INCONSISTENT in self.errors:
            inconsistencies = self.check_consistency()
            # Double-check if we still need to abort after consistency check
            abort = bool(self.errors & critical_errors) and (final or fast_fail)
            if not final and not abort:
                logger.debug("No critical errors after consistency check, continuing")
                return
        # Log errors
        for err in MergeFileError:
            if err not in self.errors:
                continue
            lvl = logging.CRITICAL if err in critical_errors else logging.ERROR
            if err == MergeFileError.INCONSISTENT:
                logger.log(lvl, '\n  '.join(inconsistencies))
                continue
            err_dids = [file.did for file in self._files if file and file.errors.first == err]
            io_utils.log_list(ERROR_MESSAGES[err], err_dids, lvl)
        # Quit if needed
        if abort:
            io_utils.log_nonzero(
                "Found {n} total file{s} with critical errors!",
                sum(1 for file in self._files if file and file.errors.first in critical_errors),
                logging.CRITICAL
            )
            sys.exit(1)
        # Check for empty set after errors
        if final and len(self.good_files) == 0:
            logger.critical("No valid files remain after error checking!")
            sys.exit(1)

    def group_by_count(self, count: int) -> list[int]:
        """
        Group input files by count
        
        :param count: Number of files to group
        :return: List of group divisions
        """
        # If there are fewer files than the target, just make one group
        target = int(config.grouping.target.value)
        if count < target:
            io_utils.log_print(f"Merging {count} inputs into 1 group")
            return []
        # Otherwise, make groups of the target count
        if config.grouping.equalize:
            n_groups = math.ceil(count / target)
            target = count / n_groups
            divs = [round(i*target) for i in range(1, n_groups)]
        else:
            divs = list(range(target, count, target))
        io_utils.log_print(
            f"Merging {count} inputs into {len(divs)+1} groups of {round(target)} files")
        # Warn about small groups
        if target < config.grouping.chunk_min:
            io_utils.log_print(
                "Target group count is smaller than recommended, did you mean to use size instead?",
                logging.WARNING)
        return divs

    def group_by_size(self, indices: list[int]) -> list[int]:
        """
        Group input files by size
        
        :param indices: Indices of files to group
        :return: List of group divisions
        """
        # Get sizes of input files
        sizes = [self.at(i).size for i in indices]
        count = sum(1 for size in sizes if size)
        total = sum(size for size in sizes if size)
        avg = total / count
        if io_utils.log_nonzero("Found {s} file{s} with no size, using average", len(sizes)-count):
            sizes = [s or avg for s in sizes]
            total += avg * (len(sizes) - count)
            count = len(sizes)
        # If the estimated size is smaller than the target, just make one group
        spec = config.method.outputs[0].size
        fixed = spec.b + avg*spec.a
        estimate = fixed + count*spec.n + total*spec.s
        target = config.grouping.target * 1024**3
        if estimate < target:
            io_utils.log_print(f"Merging {count} inputs into 1 group")
            return []
        # Build list of divisions and group size estimates
        group_sizes = []
        divs = []
        estimate = fixed
        for idx, size in enumerate(sizes):
            delta = spec.n + size*spec.s
            if estimate + delta > target:
                divs.append(idx)
                group_sizes.append(estimate)
                estimate = fixed
            estimate += delta
        group_sizes.append(estimate)
        # If we're not equalizing the groups then we're done
        if not config.grouping.equalize:
            return divs
        # Try to shuffle files between groups if it will improve equality
        while True:
            max_err = 0
            max_idx = -1
            max_delta = 0
            for idx, div in enumerate(divs):
                err = group_sizes[idx+1] - group_sizes[idx]
                if err > 0:
                    delta = spec.n + sizes[div]*spec.s
                    new_err = abs(err - 2*delta)
                    err = max(err - new_err, 0)
                else:
                    delta = -(spec.n + sizes[div-1]*spec.s)
                    new_err = abs(err - 2*delta)
                    err = min(err + new_err, 0)
                if abs(err) > abs(max_err):
                    max_err = err
                    max_idx = idx
                    max_delta = delta
            if max_idx == -1:
                break
            divs[max_idx] += 1 if max_err > 0 else -1
            group_sizes[max_idx] += max_delta
            group_sizes[max_idx+1] -= max_delta
        return divs

    def groups(self) -> Generator[MergeChunk, None, None]:
        """Split the files into groups for merging"""
        # Finish expanding all names before making groups
        meta.make_names(self.good_files)
        # Get indices of files that should count towards grouping
        start = int(config.input.skip or self.start_idx)
        end = int(start + config.input.limit if config.input.limit else self.end_idx)
        indices = []
        for i in range(start, end):
            file = self.get_by_idx(i)
            if file and file.errors.group:
                indices.append(i)
        # Get the group divisions
        if len(indices) == 0:
            logger.critical("No files to group")
            sys.exit(1)
        if config.grouping.mode == 'count':
            divs = self.group_by_count(len(indices))
        elif config.grouping.mode == 'size':
            divs = self.group_by_size(indices)
        else:
            logger.critical("Unknown target mode: %s", config.grouping.mode)
            sys.exit(1)
        # Check if we have a single output group
        if len(divs) == 0:
            group = MergeChunk(config.input.skip.value, config.input.limit.value,
                               self.get_slice(start, end))
            logger.debug("Yielding single group with %d good files", len(group))
            yield group
            return
        # Otherwise, yield groups with appropriate skip and limit
        small_groups = False
        for gid, div in enumerate(divs):
            div = indices[div]
            group = MergeChunk(start, div - start, self.get_slice(start, div))
            start = div
            if len(group) < config.grouping.chunk_min:
                small_groups = True
            if len(group) == 0:
                logger.warning("Skipping group %d with 0 good files", gid)
                continue
            logger.debug("Yielding group %d with %d good files", gid, len(group))
            yield group
        # Yield the final group
        group = MergeChunk(start, end - start, self.get_slice(start, end))
        if len(group) == 0:
            logger.warning("Skipping group %d with 0 good files", len(divs))
        else:
            logger.debug("Yielding group %d with %d good files", len(divs), len(group))
            yield group
        # Warn about small groups
        if len(group) < config.grouping.chunk_min and config.grouping.equalize:
            small_groups = True
        if small_groups:
            io_utils.log_print(
                "Some groups were smaller than the minimum chunk size, "
                "consider adjusting grouping parameters",
                logging.WARNING)
        elif len(group) < config.grouping.chunk_min:
            io_utils.log_print(
                f"Last group has only {len(group)} file{'s' if len(group) != 1 else ''}, "
                "consider adjusting target or using equalize option",
                logging.WARNING)

class MergeChunk:
    """Class to keep track of a chunk of files for merging"""

    def __init__(self, skip: int = None, limit: int = None, files: list = None):
        self.skip = skip
        self.limit = limit
        self.files = []
        self.gaps = set()
        for i, f in enumerate(files or []):
            if not f.errors:
                self.files.append(f)
            elif f.errors.group:
                self.gaps.add(i)
        self.parent = None
        self.children = []
        self.site = None

    @property
    def namespace(self) -> str:
        """Get the namespace for the chunk"""
        if self.parent is None:
            return str(config.output.namespace)
        return str(config.output.scratch.namespace)

    @property
    def tier(self) -> int:
        """Get the tier for the chunk"""
        if not self.children:
            return 0
        return max(child.tier for child in self.children) + 1

    @property
    def chunk_id(self) -> list[int]:
        """Get the chunk indices for the chunk"""
        if self.parent is None:
            return []
        return self.parent.chunk_id + [self.parent.children.index(self)]

    def __len__(self) -> int:
        """Get the number of files in the chunk"""
        return len(self.files)

    def make_name(self, name: str, chunk: list[int]) -> str:
        """Get the name for a chunk output"""
        return name.format(UUID=config.uuid(self.skip, self.limit, chunk))

    def inputs(self, output_id = None) -> list[str]:
        """
        Get the list of input files
        
        :param output_id: individual output stream for pass 2+
        :return: list of input file paths or DIDs
        """
        # If this is the first pass, the inputs are the original input files
        if output_id is None:
            inputs = []
            for file in self.files:
                # Get the path for this site
                if len(file.replicas) != 1:
                    raise RuntimeError(f"MergeChunk input file has {len(file.replicas)} replicas?")
                inputs.append(file.replicas[0].path)
            return inputs
        # Otherwise, the inputs are the outputs from the previous pass
        base_name = str(config.method.outputs[output_id].name)
        cid = self.chunk_id
        inputs = [self.make_name(base_name, cid + [c]) for c in range(len(self.children))]
        # For batch jobs, just list the DIDs and we'll get the paths later
        if not config.output.local:
            namespace = str(config.output.scratch.namespace)
            return [f"{namespace}:{name}" for name in inputs]
        # For local jobs, return the full paths to the output files
        output_dir = str(config.output.out_dir)
        return [os.path.join(output_dir, name) for name in inputs]

    def outputs(self, output_id = None) -> list[dict]:
        """
        Get the list of output file specifications for the chunk
        
        :param output_id: individual output stream for pass 2+
        :return: list of output specifications
        """
        if output_id is None:
            specs = config.method.outputs
        else:
            specs = [config.method.outputs[output_id]]
        # Concretize the output specifications for this chunk
        outputs = []
        chunk = self.chunk_id
        for spec in specs:
            output = {'name': self.make_name(spec.name, chunk)}
            if spec.size_min:
                output['size'] = spec.size_min([f.size for f in self.files])
            if spec.checklist:
                output['checklist'] = spec.checklist.value
            md = {}
            if spec.metadata:
                md.update({k: v.value for k, v in spec.metadata.items()})
            if output_id is not None and spec.pass2:
                pass2 = meta.match_method(name=spec.pass2)
                if pass2.metadata:
                    md.update({k: v.value for k, v in pass2.metadata.items()})
                if pass2.outputs[0].metadata:
                    md.update({k: v.value for k, v in pass2.outputs[0].metadata.items()})
                if pass2.outputs[0].rename:
                    output['rename'] = str(pass2.outputs[0].rename)
            elif spec.rename:
                output['rename'] = str(spec.rename)
            if md:
                output['metadata'] = md
            outputs.append(output)
        return outputs

    @property
    def metadata(self) -> dict:
        """Get the metadata for the chunk"""
        md = meta.merged_keys(self.files)
        if self.skip is not None:
            md['merge.skip'] = self.skip
        if self.limit is not None:
            md['merge.limit'] = self.limit
        chunk_id = self.chunk_id
        if chunk_id:
            md['merge.chunk'] = chunk_id
        return md

    @property
    def parents(self) -> list[str]:
        """Get the list of parent dids"""
        return meta.parents(self.files)

    def settings(self, output_id = None) -> dict:
        """
        Get the merging settings for the chunk
        
        :param output_id: individual output stream for pass 2+
        :return: settings dictionary
        """
        spec = config.method
        # Pass 2 may use a different method per output
        if output_id is not None and spec.outputs[output_id].pass2:
            method = spec.outputs[output_id].pass2
            spec = meta.match_method(name=method)
            if spec is None:
                logger.critical("Unknown merging method: %s", method)
                sys.exit(1)
        # Build the settings dictionary from the spec
        settings = {
            'streaming': config.input.streaming.value,
            'method': spec.method_name.value
        }
        for key in ['cfg', 'script', 'cmd']:
            if spec[key]:
                settings[key] = spec[key].value
        return settings

    def spec(self, output_id = None) -> dict:
        """
        Get the merging specification dictionary for a given output stream

        :param output_id: individual output stream for pass 2+
        :return: merging specification dictionary
        """
        data = {
            'namespace': self.namespace,
            'metadata': self.metadata,
            'parents': self.parents,
            'inputs': self.inputs(output_id),
            'outputs': self.outputs(output_id),
            'settings': self.settings(output_id)
        }
        return data

    @property
    def specs(self) -> list[dict]:
        """
        Get the list of merging specification dictionaries for all output streams

        :return: list of merging specification dictionaries
        """
        # For the first pass we just have one spec with all outputs
        if len(self.children) == 0:
            return [self.spec()]
        # For later passes we have one spec per output stream
        return [self.spec(output_id) for output_id in range(len(config.method.outputs))]

    def make_child(self, files: list) -> MergeChunk:
        """Make a child chunk with the given files"""
        for file in files:
            if file not in self.files:
                logger.critical("Child chunk contains file not in parent chunk: %s", file)
                sys.exit(1)
        child = MergeChunk(self.skip, self.limit, files=files)
        child.site = self.site
        child.parent = self
        self.children.append(child)
        return child
