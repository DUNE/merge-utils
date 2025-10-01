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
    for key in config.validation['required']:
        required.add(key)
        if key not in metadata:
            if key in config.validation['optional']:
                continue
            errs.append(f"Missing required key: {key}")

    # Check for conditionally required keys
    for condition, keys in config.validation['conditional'].items():
        expr = condition.format_map(MetaNameDict(metadata))
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

class MergeMetaSubset:
    """Merge metadata by taking the subset of consistent values."""
    def __init__(self, value=None):
        self.value = value

    def add(self, value):
        """Add a new value to the metadata."""
        if self.value is None:
            self.value = value
        else:
            for k, v in value.items():
                if k in self.value and self.value[k] != v:
                    logger.debug("Removing inconsistent key '%s': %s != %s", k, self.value[k], v)
                    del self.value[k]

    @property
    def valid(self):
        """Check if the value is valid."""
        return self.value is not None and len(self.value) > 0

    @property
    def warn(self):
        """Whether to warn about inconsistent metadata."""
        return self.value is not None and len(self.value) == 0

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
    'subset': MergeMetaSubset,
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
        io_utils.log_list("Omitting {n} inconsistent metadata key{s} from output:",
            [k for k, v in metadata.items() if v.warn]
        )
    metadata = {k: v.value for k, v in metadata.items() if v.valid}
    if not validate("output", metadata, requirements=False):
        logger.critical("Merged metadata is invalid, cannot continue!")
        raise ValueError("Merged metadata is invalid")
    return metadata

def parents(files: dict) -> list[str]:
    """
    Retrieve all the parents from a set of files.

    :param files: set of files to merge
    :return: set of parents
    """
    if not config.output['grandparents']:
        logger.info("Listing direct parents")
        output = []
        for file in files.values():
            output.append({
                "fid": file.fid,
                "name": file.name,
                "namespace": file.namespace
            })
        return output
    logger.info("Listing grandparents instead of direct parents")
    grandparents = set()
    for file in files.values():
        for grandparent in file.parents:
            grandparents.add(tuple(sorted(grandparent.items())))
    return [dict(t) for t in grandparents]

class MetaNameDict:
    """Wrapper class to access metadata dictionary."""

    def __init__(self, meta: dict):
        """
        Initialize the MetaNameDict with a metadata dictionary.
        """
        self._dict = meta

    class MetaNameReader:
        """Class to read metadata values."""

        def __init__(self, meta: dict, key: str):
            self._dict = meta
            self._key = key

        def __getattr__(self, name):
            return MetaNameDict.MetaNameReader(self._dict, self._key + '.' + name)

        def __str__(self):
            val = self._dict.get(self._key)
            if val is None:
                logger.warning("Metadata key '%s' not found", self._key)
                return self._key
            val = str(val)
            return config.output['abbreviations'].get(self._key, {}).get(val, val)

        def __getitem__(self, name):
            val = self._dict.get(self._key)
            if val is None:
                logger.warning("Metadata key '%s' not found", self._key)
                return self._key
            if not hasattr(val, '__getitem__'):
                logger.warning("Metadata key '%s' is not subscriptable", self._key)
                return f"{self._key}[{name}]"
            val2 = val.get(eval(name)) #pylint: disable=eval-used
            if val2 is None:
                logger.warning("Metadata key '%s[%s]' not found", self._key, name)
                return f"{self._key}[{name}]"
            val2 = str(val2)
            return config.output['abbreviations'].get(f"{self._key}[{name}]", {}).get(val2, val2)

    def __getitem__(self, name):
        return MetaNameDict.MetaNameReader(self._dict, name)

def make_name(metadata: dict) -> str:
    """
    Create a name for the merged file based on the metadata.

    :param metadata: metadata dictionary
    :return: merged file name
    """
    name = config.output['name'].format_map(MetaNameDict(metadata))
    ext = config.merging['methods'][config.merging['method']]['ext']
    return f"{name}_merged_{io_utils.get_timestamp()}{ext}"
