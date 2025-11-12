"""Utility functions for merging metadata for multiple files."""

import os
import sys
import logging
import collections

from merge_utils import config, io_utils, __version__

logger = logging.getLogger(__name__)

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
            return config.metadata['abbreviations'].get(self._key, {}).get(val, val)

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
            return config.metadata['abbreviations'].get(f"{self._key}[{name}]", {}).get(val2, val2)

    def __getitem__(self, name):
        if name.startswith('$'):
            return os.getenv(name[1:], name[1:])
        if name == "TIMESTAMP":
            return config.timestamp
        return MetaNameDict.MetaNameReader(self._dict, name)

    def format(self, template: str) -> str:
        """
        Format a string using the metadata dictionary.

        :param template: template string
        :return: formatted string
        """
        return template.format_map(self)

    def eval(self, condition: str) -> bool:
        """
        Evaluate a condition using the metadata dictionary.

        :param condition: condition string to evaluate
        :return: evaluated value
        """
        expr = condition.format_map(self)
        try:
            val = eval(expr) #pylint: disable=eval-used
        except Exception as exc:
            raise ValueError(f"Error evaluating condition ({condition})") from exc
        return val

def fix(name: str, metadata: dict) -> None:
    """
    Fix the metadata dictionary.

    :param name: name of the file (for logging)
    :param metadata: metadata dictionary
    """
    fixes = []
    # Fix misspelled keys
    for bad_key, good_key in config.metadata['fixes']['keys'].items():
        if bad_key in metadata:
            fixes.append(f"Key '{bad_key}' -> '{good_key}'")
            metadata[good_key] = metadata.pop(bad_key)

    # Fix missing keys
    for key, value in config.metadata['fixes']['missing'].items():
        if key not in metadata:
            fixes.append(f"Key '{key}' value None -> '{value}'")
            metadata[key] = value

    # Fix misspelled values
    for key in config.metadata['fixes']:
        if key in ['keys', 'missing'] or key not in metadata:
            continue
        value = metadata[key]
        if value in config.metadata['fixes'][key]:
            new_value = config.metadata['fixes'][key][value]
            fixes.append(f"Key '{key}' value '{value}' -> '{new_value}'")
            metadata[key] = new_value

    if fixes:
        io_utils.log_list("Applying {n} metadata fix{es} to file %s:" % name, fixes, logging.DEBUG)

def check_required(metadata: dict) -> list:
    """
    Check if the metadata dictionary contains all required keys.

    :param metadata: metadata dictionary
    :return: List of any missing required keys
    """
    errs = []
    # Check for required keys
    required = set()
    for key in config.metadata['required']:
        required.add(key)
        if key not in metadata:
            if key in config.metadata['optional']:
                continue
            errs.append(f"Missing required key: {key}")

    # Check for conditionally required keys
    name_dict = MetaNameDict(metadata)
    for condition, keys in config.metadata['conditional'].items():
        if not name_dict.eval(condition):
            logger.debug("Skipping condition: %s", condition)
            continue
        logger.debug("Matched condition: %s", condition)
        for key in keys:
            if key in required:
                continue
            required.add(key)
            if key not in metadata and key not in config.metadata['optional']:
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
    for key, options in config.metadata['restricted'].items():
        if key not in metadata:
            continue
        value = metadata[key]
        if value not in options:
            errs.append(f"Invalid value for {key}: {value}")

    # Check value types
    for key, expected_type in config.metadata['types'].items():
        if key not in metadata or key in config.metadata['restricted']:
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

def merge_cfg_keys() -> dict:
    """
    Get special merging configuration keys from the global config.
    
    :return: dictionary of merging configuration keys
    """
    keys = {
        'version': __version__,
        'method': config.merging['method']['name'],
        'timestamp': config.timestamp
    }
    for key in ['cmd', 'script', 'cfg']:
        val = config.merging['method'].get(key)
        if val is not None:
            if key in ['script', 'cfg']:
                val = os.path.basename(val)
            keys[key] = val
    for key in ['skip', 'limit', 'tag', 'comment']:
        val = config.inputs.get(key)
        if val is not None:
            keys[key] = val
    return keys

def merged_keys(files: dict, warn: bool = False) -> dict:
    """
    Merge metadata from multiple files into a single dictionary.

    :param files: set of files to merge
    :param warn: whether to warn about inconsistent metadata
    :return: merged metadata
    """
    metadata = collections.defaultdict(
        MERGE_META_CLASSES[config.metadata['merging']['default']]
    )
    for key, mode in config.metadata['merging'].items():
        if key in ['default', 'overrides']:
            continue
        if mode in MERGE_META_CLASSES:
            metadata[key] = MERGE_META_CLASSES[mode]()
        else:
            metadata[key] = MergeMetaOverride()
    for key, value in config.metadata['overrides'].items():
        metadata[key] = MergeMetaOverride(value)
    for key, value in merge_cfg_keys().items():
        metadata[f"merge.{key}"] = MergeMetaOverride(value)

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

def set_method_auto(metadata: dict) -> None:
    """
    Auto-select merging method based on metadata conditions.

    :param metadata: metadata dictionary
    """
    # Find the first matching merging method (in reverse order)
    method = {}
    name_dict = MetaNameDict(metadata)
    for mtd in reversed(config.merging['methods']):
        condition = mtd.get('cond', 'True')
        if name_dict.eval(condition):
            if condition == 'True':
                condition = "unconditional"
            logger.info("Auto-selected merging method '%s' (%s)", mtd['name'], condition)
            method = mtd
            break
    if not method:
        logger.critical("Failed to auto-select merging method!")
        sys.exit(1)

    # Set merging method parameters
    config.merging['method']['name'] = method['name']
    explicit = False
    for key in ['script', 'cmd', 'cfg']:
        if key in config.merging['method'] and config.merging['method'][key] is not None:
            logger.warning("Explicit value for merge.%s overrides %s default", key, method['name'])
            explicit = True
        else:
            config.merging['method'][key] = method.get(key, None)
    if config.merging['method']['dependencies']:
        logger.warning("Explicity adding merge.dependencies:\n  %s",
                       "\n  ".join(config.merging['method']['dependencies']))
        explicit = True
    config.merging['method']['dependencies'].extend(method.get('dependencies', []))
    if config.merging['method']['outputs']:
        logger.warning("Explicit list of merge.outputs overrides %s default", method['name'])
        explicit = True
    else:
        config.merging['method']['outputs'] = method.get('outputs', [])
    if explicit:
        logger.warning("Consider specifying an explicity merging method instead of using 'auto'!")

def set_method(method: dict) -> None:
    """
    Set merging method parameters.

    :param method: merging method dictionary
    """
    logger.info("Using built-in merging method '%s'", method['name'])
    for key in ['script', 'cmd', 'cfg']:
        if key in config.merging['method'] and config.merging['method'][key] is not None:
            logger.info("Explicit value for merge.%s overrides %s default", key, method['name'])
        else:
            config.merging['method'][key] = method.get(key, None)
    if config.merging['method']['dependencies']:
        logger.info("Explicity adding merge.dependencies:\n  %s",
                       "\n  ".join(config.merging['method']['dependencies']))
    config.merging['method']['dependencies'].extend(method.get('dependencies', []))
    if config.merging['method']['outputs']:
        logger.info("Explicit list of merge.outputs overrides %s default", method['name'])
    else:
        config.merging['method']['outputs'] = method.get('outputs', [])

def set_method_custom() -> None:
    """
    Set merging method parameters for a custom script.
    """
    name = config.merging['method']['name']
    cmd = config.merging['method'].setdefault('cmd')
    script = config.merging['method'].setdefault('script')
    if not script and (not cmd or '{script}' in cmd):
        # Assume the name is a script
        config.merging['method']['script'] = name
        config.merging['method']['name'] = os.path.basename(name)
    logger.info("Using custom merging method: %s", name)

    config.merging['method'].setdefault('cfg', None)
    config.merging['method'].setdefault('dependencies', [])
    config.merging['method'].setdefault('outputs', [])

def auto_output(files: dict) -> None:
    """
    Auto-generate an output file name with the same extension as the inputs, if needed.

    :param files: set of files to merge
    """
    if config.merging['method']['outputs']:
        return
    extensions = set()
    for file in files:
        extensions.add(os.path.splitext(file.name)[-1])
    if len(extensions) != 1:
        logger.critical("Cannot determine extension for merged files!")
        sys.exit(1)
    ext = extensions.pop()
    config.merging['method']['outputs'] = [{'name': f"{{name}}_merged{ext}"}]
    logger.info("Auto-detected file extension '%s' from input files", ext)

def log_method() -> None:
    """
    Log the final merging method configuration.
    """
    msg = [f"Final settings for merging method '{config.merging['method']['name']}':"]
    for key in ['cmd', 'script', 'cfg']:
        msg.append(f"{key}: {config.merging['method'][key]}")
    msg.append("dependencies:")
    msg.extend([f"  {dep}" for dep in config.merging['method']['dependencies']])
    msg.append("outputs:")
    for output in config.merging['method']['outputs']:
        if 'rename' in output:
            msg.append(f"  {output['name']} (renamed from {output['rename']})")
        else:
            msg.append(f"  {output['name']}")
        if 'metadata' in output:
            msg.extend([f"    {k}: {v}" for k, v in output['metadata'].items()])
        if 'method' in output:
            msg.append(f"    method: {output['method']}")
    logger.info("\n  ".join(msg))

def check_method(files: dict) -> None:
    """
    Check and set the merging method based on the input file metadata.

    :param files: set of files to merge
    """
    # Figure out merging method
    name = config.merging['method']['name']
    if name == 'auto':
        set_method_auto(merged_keys(files, warn=False))
    else:
        # Check if we're using a built-in merging method
        methods = [m for m in config.merging['methods'] if m['name'] == name]
        if methods:
            set_method(methods[-1])
        else:
            set_method_custom()

    # Set the output file name if not provided
    auto_output(files)

    # Convert dependencies to a unique set of full paths
    dependencies = set()
    if config.merging['method']['script']:
        dependencies.add(io_utils.find_runner(config.merging['method']['script']))
    if config.merging['method']['cfg']:
        dependencies.add(io_utils.find_cfg(config.merging['method']['cfg']))
    for dep in config.merging['method']['dependencies']:
        dependencies.add(io_utils.find_file(dep, ["config", "src"], recursive=True))
    config.merging['method']['dependencies'] = list(dependencies)

    # Check for issues with the merging command
    cmd = config.merging['method']['cmd']
    if cmd:
        if config.merging['method']['script'] and '{script}' not in cmd:
            logger.warning("Merging command does not call provided '{script}'")
        if config.merging['method']['cfg'] and '{cfg}' not in cmd:
            logger.warning("Merging command does not use provided '{cfg}'")
        if '{inputs}' not in cmd:
            logger.critical("Merging command does not specify '{inputs}'")
            sys.exit(1)
        n_out = sum(1 for output in config.merging['method']['outputs'] if 'rename' not in output)
        if n_out > 0 and '{output' not in cmd:
            logger.critical("Merging command does not specify '{output}' (or '{outputs[#]}')")
            sys.exit(1)

    # Log final merging method configuration
    log_method()

def make_names(files: dict) -> str:
    """
    Update merging method and create a name for the merged files.

    :param files: set of files to merge
    :return: merged file name
    """
    check_method(files)
    metadata = merged_keys(files, warn=True) # recalculate with correct method settings
    # Set output namespaces if they are not given
    if not config.output.get('namespace'):
        config.output['namespace'] = next(iter(files.values())).namespace
    if not config.output['scratch'].get('namespace'):
        config.output['scratch']['namespace'] = config.output['namespace']
    name_dict = MetaNameDict(metadata)
    name = name_dict.format(config.output['name'])
    config.output['name'] = name
    metadata['name'] = name
    for output in config.merging['method']['outputs']:
        name, ext = os.path.splitext(name_dict.format(output['name']))
        output['name'] = f"{name}_{config.timestamp}{ext}"
    io_utils.log_list(
        "Output file name{s}:",
        [output['name'] for output in config.merging['method']['outputs']],
        logging.INFO
    )
