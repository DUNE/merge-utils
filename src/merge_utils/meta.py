"""Utility functions for merging metadata for multiple files."""

import os
import sys
import logging
import collections

from merge_utils import config, io_utils, __version__

logger = logging.getLogger(__name__)

class MetaNameDict:
    """Wrapper class to access metadata dictionary."""

    def __init__(self, metadata: dict):
        """
        Initialize the MetaNameDict with a metadata dictionary.
        """
        self.data = metadata
        self.errors = []

    def get(self, key, default=None):
        """
        Get a metadata value by key.

        :param key: metadata key
        :param default: default value if key not found
        :return: metadata value
        """
        return self.data.get(key, default)

    class MetaVal:
        """Class to read metadata values."""

        def __init__(self, meta, key, idx=None):
            self._dict = meta
            self._key = key
            self._idx = idx

        @property
        def key(self) -> str:
            """
            Get the metadata key.

            :return: metadata key
            """
            if self._idx is not None:
                return f"{self._key}[{self._idx}]"
            return self._key

        def read_list(self, data) -> any:
            """
            Try to extract a value from a list.
            
            :param data: list of data
            :return: extracted value
            """
            if isinstance(self._idx, int):
                if self._idx >= 0 and self._idx < len(data):
                    return data[self._idx]
                self._dict.errors.append(f"Metadata key '{self.key}' index out of range")
            elif isinstance(self._idx, str):
                try:
                    idx = [int(i) if i else None for i in self._idx.split(':')]
                    if len(idx) == 1:
                        return data[idx[0]]
                    if len(idx) == 2:
                        return data[slice(idx[0], idx[1])]
                    if len(idx) == 3:
                        return data[slice(idx[0], idx[1], idx[2])]
                except ValueError:
                    pass
                self._dict.errors.append(f"Metadata key '{self.key}' has invalid slice")
            else:
                self._dict.errors.append(f"Metadata key '{self.key}' has invalid index")
            return None

        def read_dict(self, data) -> any:
            """
            Try to extract a value from a dictionary.
            
            :param data: dictionary of data
            :return: extracted value
            """
            key = self._idx
            if key.startswith("'") and key.endswith("'"):
                key = key[1:-1]
            elif key.startswith('"') and key.endswith('"'):
                key = key[1:-1]
            val = data.get(key)
            if val is None:
                self._dict.errors.append(f"Metadata key '{self.key}' has invalid index")
            return val

        @property
        def value(self) -> any:
            """
            Get the metadata value.

            :return: metadata value
            """
            if self._key.startswith('$'):
                val = os.getenv(self._key[1:], None)
                if val is None:
                    self._dict.errors.append(f"Environment variable '{self._key[1:]}' not found")
                return val
            val = self._dict.get(self._key)
            if val is None:
                self._dict.errors.append(f"Metadata key '{self._key}' not found")
            elif self._idx is not None:
                if isinstance(val, list):
                    val = self.read_list(val)
                elif isinstance(val, dict):
                    val = self.read_dict(val)
                elif hasattr(val, '__getitem__'):
                    try:
                        val = val[self._idx]
                    except (KeyError, IndexError, TypeError):
                        self._dict.errors.append(f"Metadata key '{self.key}' has invalid index")
                        val = None
                else:
                    self._dict.errors.append(f"Metadata key '{self._key}' is not subscriptable")
                    val = None
            return val

        def __format__(self, format_spec):
            key = self.key
            #logger.debug("Formatting metadata key '%s' with spec '%s'", key, format_spec)
            val = self.value
            if val is None:
                if format_spec:
                    return f"{{{key}:{format_spec}}}"
                return f"{{{key}}}"
            if isinstance(val, str):
                # Apply specific abbreviations
                val = config.metadata['abbreviations'].get(key, {}).get(val, val)
                # Remove known extensions
                for ext in config.metadata['abbreviations'].get('extensions', []):
                    if val.endswith(f".{ext}"):
                        val = val[:-(len(ext)+1)]
                        break
            # Apply formatting
            try:
                output = format(val, format_spec)
            except (ValueError, TypeError):
                self._dict.errors.append(
                    f"Failed to format key '{key}' ({val}) with spec '{format_spec}'"
                )
                return f"{{{key}:{format_spec}}}"
            # Apply general substitutions
            for old, new in config.metadata['abbreviations'].get('substitutions', {}).items():
                output = output.replace(old, new)
            return output

    class MetaKey:
        """Class to parse metadata keys."""

        def __init__(self, meta, key: str):
            self._dict = meta
            self._key = key

        def __getattr__(self, name):
            return MetaNameDict.MetaKey(self._dict, self._key + '.' + name)

        def __format__(self, format_spec):
            return format(MetaNameDict.MetaVal(self._dict, self._key), format_spec)

        def __getitem__(self, name):
            return MetaNameDict.MetaVal(self._dict, self._key, name)

    def __getitem__(self, name):
        if name.startswith('$'):
            return MetaNameDict.MetaVal(self, name)
        if name == "TIMESTAMP":
            return config.timestamp
        if name == "UUID":
            return config.uuid()
        if name == "NAME":
            return config.output['name']
        return MetaNameDict.MetaKey(self, name)

    def format(self, template: str, strict: bool = True) -> str:
        """
        Format a string using the metadata dictionary.

        :param template: template string
        :return: formatted string
        """
        self.errors = []
        output = template.format_map(self)
        if self.errors:
            errors = [output] + self.errors
            if strict:
                io_utils.log_list("Failed to parse name template:", errors, logging.CRITICAL)
                sys.exit(1)
            io_utils.log_list("Failed to parse name template:", errors, logging.ERROR)
            self.errors = []
        return output

    def eval(self, condition: str) -> bool:
        """
        Evaluate a condition using the metadata dictionary.

        :param condition: condition string to evaluate
        :return: evaluated value
        """
        expr = self.format(condition)
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
            #logger.debug("Skipping condition: %s", condition)
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
    for key in ['skip', 'limit', 'tag', 'comment', 'query', 'dataset']:
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

def match_method(name: str = None, metadata: dict = None) -> dict:
    """
    Find a built-in merging method by name or metadata conditions.

    :param name: name of the merging method
    :param metadata: metadata dictionary
    :return: merging method dictionary
    """
    # Match by name
    if name:
        methods = [m for m in config.merging['methods'] if m['name'] == name]
        if not methods:
            return None
        return methods[-1]
    # Match by conditions
    if metadata:
        name_dict = MetaNameDict(metadata)
        for method in reversed(config.merging['methods']):
            condition = method.get('cond', 'False')
            if name_dict.eval(condition):
                if condition == 'True':
                    condition = "unconditional"
                logger.info("Auto-selected merging method '%s' (%s)", method['name'], condition)
                return method
    # No match found
    return None

def set_method_auto(metadata: dict) -> None:
    """
    Auto-select merging method based on metadata conditions.

    :param metadata: metadata dictionary
    """
    method = match_method(metadata=metadata)
    if method is None:
        logger.critical("Failed to auto-select a merging method!")
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
        method = match_method(name=name)
        if method:
            set_method(method)
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

    # Make sure stage-2 merging only produces 1 output
    for idx, output in enumerate(config.merging['method']['outputs']):
        if 'method' in output:
            method2 = match_method(name=output['method'])
            if method2 is None:
                logger.critical("Output %d has unknown merging method '%s'", idx, output['method'])
                sys.exit(1)
            if len(method2['outputs']) != 1:
                logger.critical("Output %d merging method '%s' must produce exactly 1 output!",
                                idx, output['method'])
                sys.exit(1)
        elif len(config.merging['method']['outputs']) != 1:
            logger.critical("Output %d must specify a merging method for stage-2 merges!", idx)
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
    for output in config.merging['method']['outputs']:
        #name, ext = os.path.splitext(name_dict.format(output['name']))
        #output['name'] = f"{name}_{config.timestamp}{ext}"
        output['name'] = name_dict.format(output['name'])
    io_utils.log_list(
        "Output file name{s}:",
        [output['name'] for output in config.merging['method']['outputs']],
        logging.INFO
    )
