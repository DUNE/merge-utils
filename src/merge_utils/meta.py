"""Utility functions for merging metadata for multiple files."""

import logging
import collections

from merge_utils import config, io_utils

logger = logging.getLogger(__name__)

def fix(name: str, metadata: dict) -> None:
    """
    Fix the metadata dictionary.

    :param name: name of the file (for logging)
    :param metadata: metadata dictionary
    """
    fixes = []
    # Fix misspelled keys
    for bad_key, good_key in config.validation['fixes']['keys'].items():
        if bad_key in metadata:
            fixes.append(f"Key '{bad_key}' -> '{good_key}'")
            metadata[good_key] = metadata.pop(bad_key)

    # Fix missing keys
    for key, value in config.validation['fixes']['missing'].items():
        if key not in metadata:
            fixes.append(f"Key '{key}' value None -> '{value}'")
            metadata[key] = value

    # Fix misspelled values
    for key in config.validation['fixes']:
        if key in ['keys', 'missing'] or key not in metadata:
            continue
        value = metadata[key]
        if value in config.validation['fixes'][key]:
            new_value = config.validation['fixes'][key][value]
            fixes.append(f"Key '{key}' value '{value}' -> '{new_value}'")
            metadata[key] = new_value

    if fixes:
        io_utils.log_list("Applying {n} metadata fix{es} to file %s:" % name, fixes, logging.INFO)

def check_required(metadata: dict) -> list:
    """
    Check if the metadata dictionary contains all required keys.

    :param metadata: metadata dictionary
    :return: List of any missing required keys
    """
    errs = []
    # Check for required keys
    required = set()
    inserts = MergeMetaNameDict()
    for key in config.validation['required']:
        required.add(key)
        if key not in metadata:
            if key in config.validation['optional']:
                continue
            errs.append(f"Missing required key: {key}")
        else:
            inserts[key] = metadata[key]

    # Check for conditionally required keys
    for condition, keys in config.validation['conditional'].items():
        expr = condition.format_map(inserts)
        try:
            if not eval(expr): #pylint: disable=eval-used
                logger.debug("Skipping condition: %s", expr)
                continue
        except Exception as exc:
            raise ValueError(f"Error evaluating condition ({condition})") from exc
        logger.debug("Matched condition: %s", expr)
        for key in keys:
            if key in required:
                continue
            required.add(key)
            if key not in metadata and key not in config.validation['optional']:
                errs.append(f"Missing conditionally required key: {key} (from {condition})")

    return errs

def validate(name: str, metadata: dict, requirements: bool = True) -> bool:
    """
    Validate the metadata dictionary.

    :param name: name of the file (for logging)
    :param metadata: metadata dictionary
    :param requirements: whether to check for required keys
    :return: True if metadata is valid, False otherwise
    """
    # Fix metadata
    fix(name, metadata)
    errs = []
    # Check for required keys
    if requirements:
        errs.extend(check_required(metadata))

    # Check for restricted keys
    for key, options in config.validation['restricted'].items():
        if key not in metadata:
            continue
        value = metadata[key]
        if value not in options:
            errs.append(f"Invalid value for {key}: {value}")

    # Check value types
    for key, expected_type in config.validation['types'].items():
        if key not in metadata or key in config.validation['restricted']:
            continue
        value = metadata[key]
        type_name = type(value).__name__
        if (type_name == expected_type) or (expected_type == 'float' and type_name == 'int'):
            continue
        errs.append(f"Invalid type for {key}: {value} (expected {expected_type})")

    if errs:
        lvl = logging.ERROR if config.validation['skip']['invalid'] else logging.CRITICAL
        io_utils.log_list("File %s has {n} invalid metadata key{s}:" % name, errs, lvl)
        return False

    return True

class MergeMetaMin:
    """Merge metadata by taking the minimum value."""
    warn = False

    def __init__(self):
        self.value = float('inf')

    def add(self, value):
        """Add a new value to the metadata."""
        self.value = min(self.value, value)

    @property
    def valid(self):
        """Check if the value is valid."""
        return self.value != float('inf')

class MergeMetaMax:
    """Merge metadata by taking the maximum value."""
    warn = False

    def __init__(self):
        self.value = -float('inf')

    def add(self, value):
        """Add a new value to the metadata."""
        self.value = max(self.value, value)

    @property
    def valid(self):
        """Check if the value is valid."""
        return self.value != -float('inf')

class MergeMetaSum:
    """Merge metadata by adding the values."""
    warn = False

    def __init__(self):
        self.value = 0

    def add(self, value):
        """Add a new value to the metadata."""
        self.value += value

    @property
    def valid(self):
        """Check if the value is valid."""
        return self.value != 0

class MergeMetaUnion:
    """Merge metadata by taking the union."""
    warn = False

    def __init__(self):
        self._value = set()

    def add(self, value):
        """Add a new value to the metadata."""
        self._value.update(value)

    @property
    def value(self):
        """Get the merged value."""
        return list(self._value)

    @property
    def valid(self):
        """Check if the value is valid."""
        return len(self._value) > 0

class MergeMetaUnique:
    """Merge metadata by taking the unique values."""
    def __init__(self, value=None):
        self.value = value
        self._valid = True
        self.warn = False

    def add(self, value):
        """Add a new value to the metadata."""
        if self.value is None:
            self.value = value
        elif self.value != value:
            self._valid = False
            self.warn = True

    @property
    def valid(self):
        """Check if the value is valid."""
        return self._valid and self.value is not None

class MergeMetaAll:
    """Merge metadata by taking the set of values."""
    warn = False

    def __init__(self):
        self._value = set()

    def add(self, value):
        """Add a new value to the metadata."""
        self._value.update(value)

    @property
    def value(self):
        """Get the merged value."""
        if len(self._value) == 1:
            return next(iter(self._value))
        return list(self._value)

    @property
    def valid(self):
        """Check if the value is valid."""
        return len(self._value) > 0

class MergeMetaOverride:
    """Merge metadata by overriding the value."""
    warn = False

    def __init__(self, value=None):
        self.value = value

    def add(self, value):
        """Add a new value to the metadata."""

    @property
    def valid(self):
        """Check if the value is valid."""
        return self.value is not None

MERGE_META_CLASSES = {
    'unique': MergeMetaUnique,
    'all': MergeMetaAll,
    'min': MergeMetaMin,
    'max': MergeMetaMax,
    'sum': MergeMetaSum,
    'union': MergeMetaUnion,
    #'skip': MergeMetaOverride,
}

def merged_keys(files: dict, warn: bool = False) -> dict:
    """
    Merge metadata from multiple files into a single dictionary.

    :param files: set of files to merge
    :param warn: whether to warn about inconsistent metadata
    :return: merged metadata
    """
    metadata = collections.defaultdict(
        MERGE_META_CLASSES[config.merging['metadata']['default']]
    )
    for key, mode in config.merging['metadata'].items():
        if key in ['default', 'overrides']:
            continue
        if mode in MERGE_META_CLASSES:
            metadata[key] = MERGE_META_CLASSES[mode]()
        else:
            metadata[key] = MergeMetaOverride()
    for key, value in config.merging['metadata']['overrides'].items():
        metadata[key] = MergeMetaOverride(value)
    for file in files.values():
        for key, value in file.metadata.items():
            metadata[key].add(value)

    if warn:
        io_utils.log_list("Omitting {n} inconsistent metadata key{s}:",
            [k for k, v in metadata.items() if v.warn]
        )
    metadata = {k: v.value for k, v in metadata.items() if v.valid}
    if not validate("output", metadata, requirements=False):
        logger.critical("Merged metadata is invalid, cannot continue!")
        raise ValueError("Merged metadata is invalid")
    return metadata

def common_keys(files: dict, warn: bool = False) -> dict:
    """ find keys that are common to all merged files - useful for dataset metadata """
    common = {}
    different = [] # flag keys that have multiple values
    for file in files.values():    
        for key, value in file.metadata.items():
            if key in different: continue # already looked at this one
            if key not in common: # first instance
                common[key] = value
            else:  
                if common[key] != value: # key has 2 calues, flag it and remove from common
                    different.append(key)
                    common.pop(key)
    return common 

def parents(files: dict) -> list[str]:
    """
    Retrieve all the parents from a set of files.

    :param files: set of files to merge
    :return: set of parents
    """
    if not config.output['grandparents']:
        output = []
        for file in files.values():
            output.append({
                "fid": file.fid,
                "name": file.name,
                "namespace": file.namespace
            })
    grandparents = set()
    for file in files.values():
        for grandparent in file.parents:
            grandparents.add(tuple(sorted(grandparent.items())))
    return [dict(t) for t in grandparents]

class MergeMetaNameDict(collections.UserDict):
    """Class to inject metadata into a name template."""

    def __init__(self, value: str = None):
        """
        Initialize the NameMetaFormatter with a value.

        :param value: value to insert
        """
        super().__init__()
        self._str = value or ""

    def __str__(self):
        return self._str

    def __repr__(self):
        if not self._str:
            return f"{self.data}"
        if not self.data:
            return f"'{self._str}'"
        return f"'{self._str}'{self.data}"

    def __getitem__(self, name):
        name_list = name.split('.', 1)
        if name_list[0] not in self.data:
            self.data[name_list[0]] = MergeMetaNameDict(name_list[0])
        if len(name_list) == 1:
            return self.data[name_list[0]]
        return self.data[name_list[0]][name_list[1]]

    def __setitem__(self, name, value):
        self[name]._str = value

    def __getattr__(self, name):
        return self[name]

def make_name(metadata: dict) -> str:
    """
    Create a name for the merged file based on the metadata.

    :param metadata: metadata dictionary
    :return: merged file name
    """
    inserts = MergeMetaNameDict()
    for key, value in metadata.items():
        if not isinstance(value, str):
            value = str(value)
        value = value.split('.', 1)[0]
        value = config.output['abbreviations'].get(key, {}).get(value, value)
        inserts[key] = value
    inserts['timestamp'] = io_utils.get_timestamp()

    name = config.output['name'].format_map(inserts)
    ext = config.merging['methods'][config.merging['method']]['ext']
    return f"{name}_merged_{inserts['timestamp']}{ext}"
