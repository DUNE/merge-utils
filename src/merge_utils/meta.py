"""Utility functions for merging metadata for multiple files."""

import os
import sys
import logging
import collections

from merge_utils import config, io_utils, naming

logger = logging.getLogger(__name__)

def fix(name: str, metadata: dict) -> None:
    """
    Fix the metadata dictionary.

    :param name: name of the file (for logging)
    :param metadata: metadata dictionary
    """
    fixes = []
    # Fix misspelled keys
    for key, replacement in config.metadata.fixes.bad_keys.items():
        if key in metadata:
            fixes.append(f"Key '{key}' -> '{replacement}'")
            metadata[str(replacement)] = metadata.pop(key)

    # Fix missing keys
    for key, value in config.metadata.fixes.missing_keys.items():
        if key not in metadata:
            fixes.append(f"Key '{key}' value None -> '{value}'")
            metadata[key] = value._value # pylint: disable=protected-access

    # Fix misspelled values
    for key, replacements in config.metadata.fixes.bad_values.items():
        value = metadata.get(key, None)
        replacement = replacements.get(value, None)
        if replacement is not None:
            fixes.append(f"Key '{key}' value '{value}' -> '{replacement}'")
            metadata[key] = replacement._value # pylint: disable=protected-access

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
    for key in [str(k) for k in config.metadata.required]:
        required.add(key)
        if key not in metadata:
            if key in config.metadata.optional:
                continue
            errs.append(f"Missing required key: {key}")

    # Check for conditionally required keys
    name_dict = naming.Formatter(metadata)
    for spec in config.metadata.conditional:
        condition = spec.cond
        if not name_dict.eval(condition):
            #logger.debug("Skipping condition: %s", condition)
            continue
        logger.debug("Matched condition: %s", condition)
        for key in [str(k) for k in spec.required]:
            if key in required:
                continue
            required.add(key)
            if key not in metadata and key not in config.metadata.optional:
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
    for key, options in config.metadata.restricted.items():
        if key not in metadata:
            continue
        value = metadata[key]
        if value not in options:
            errs.append(f"Invalid value for {key}: {value}")

    # Check value types
    for key, expected_type in config.metadata.types.items():
        if key not in metadata or key in config.metadata.restricted:
            continue
        value = metadata[key]
        type_name = type(value).__name__
        if (type_name == expected_type) or (expected_type == 'float' and type_name == 'int'):
            continue
        errs.append(f"Invalid type for {key}: {value} (expected {expected_type})")

    if errs:
        crit = config.validation.error_handling.invalid == 'quit'
        lvl = logging.CRITICAL if crit else logging.ERROR
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
        'version': config.version,
        'method': config.method.method_name,
        'timestamp': config.job.timestamp
    }
    for key in ['cmd', 'script', 'cfg']:
        val = config.method[key]
        if val:
            if key in ['script', 'cfg']:
                val = os.path.basename(str(val))
            keys[key] = val
    for key in ['skip', 'limit', 'tag', 'comment']:
        val = config.input[key]
        if val:
            keys[key] = val
    if config.input.mode == 'query':
        keys['query'] = config.input.inputs[0]
    elif config.input.mode == 'dataset':
        keys['dataset'] = config.input.inputs[0]
    return keys

def add_origin(metadata: dict, app: str) -> None:
    """
    Add origin information to the metadata dictionary for transform jobs.

    :param metadata: metadata dictionary
    :param app: name of the application
    """
    # Get existing application information
    name = metadata.get('core.application.name')
    ver = metadata.get('core.application.version')
    cfg = metadata.get('dune.config_file') #TODO: make sure this is correct for all applications

    # If missing, should be raw data
    if name is None:
        tier = metadata.get('core.data_tier')
        if tier not in ['raw']:
            logger.critical("Transform job missing core.application.name for %s data!", tier)
            sys.exit(1)
        for key in ['names', 'versions', 'config_files']:
            full_key = f"origin.applications.{key}"
            if full_key in metadata:
                logger.critical("Transform job missing core.application.name but has origin info!")
                sys.exit(1)
        metadata['origin.applications.names'] = []
        metadata['origin.applications.versions'] = {}
        metadata['origin.applications.config_files'] = {}

    # Add current application information
    metadata['core.application'] = app
    metadata['core.application.family'], metadata['core.application.name'] = app.split('.', 1)
    metadata['core.application.version'] = str(config.method.environment.dunesw_version)
    if not config.method.cfg:
        io_utils.log_print("Running a transform job without a config file!", logging.WARNING)
    metadata['dune.config_file'] = str(config.method.cfg)

    # if there is no origin application then we're done
    if name is None:
        return

    # Increment stage until we find a unique name
    names = set(metadata.get('origin.applications.names', []))
    if 'origin.applications.versions' in metadata:
        names.update(metadata['origin.applications.versions'].keys())
    if 'origin.applications.config_files' in metadata:
        names.update(metadata['origin.applications.config_files'].keys())
    if name in names:
        stage = 2
        if '_stage' in name:
            name, stage = name.split('_stage', 1)
            stage = int(stage) + 1
        while f"{name}_stage{stage}" in names:
            stage += 1
        name = f"{name}_stage{stage}"

    # Add origin information
    if 'origin.applications.names' in metadata:
        metadata['origin.applications.names'].append(name)
    else:
        metadata['origin.applications.names'] = [name]
    if 'origin.applications.versions' in metadata:
        metadata['origin.applications.versions'][name] = ver
    else:
        metadata['origin.applications.versions'] = {name: ver}
    if 'origin.applications.config_files' in metadata:
        metadata['origin.applications.config_files'][name] = cfg
    else:
        metadata['origin.applications.config_files'] = {name: cfg}

def merged_keys(files: list, warn: bool = False) -> dict:
    """
    Merge metadata from multiple files into a single dictionary.

    :param files: list of files to merge
    :param warn: whether to warn about inconsistent metadata
    :return: merged metadata
    """
    metadata = collections.defaultdict(
        MERGE_META_CLASSES[str(config.metadata.merging['default'])]
    )
    for key, mode in config.metadata.merging.items():
        if key == 'default':
            continue
        merge_class = MERGE_META_CLASSES.get(str(mode), None)
        if merge_class is not None:
            metadata[key] = merge_class()
        else:
            metadata[key] = MergeMetaOverride()
    for key, value in config.metadata.overrides.items():
        metadata[key] = MergeMetaOverride(value._value)  # pylint: disable=protected-access
    for key, value in merge_cfg_keys().items():
        metadata[f"merge.{key}"] = MergeMetaOverride(str(value))

    for file in files:
        for key, value in file.metadata.items():
            metadata[key].add(value)

    if warn:
        io_utils.log_list("Omitting {n} inconsistent metadata key{s} from output:",
            [k for k, v in metadata.items() if v.warn]
        )
    metadata = {k: v.value for k, v in metadata.items() if v.valid}

    if config.method.transform:
        add_origin(metadata, str(config.method.transform))

    if not validate("output", metadata, requirements=False):
        logger.critical("Merged metadata is invalid, cannot continue!")
        raise ValueError("Merged metadata is invalid")
    return metadata

def parents(files: list) -> list:
    """
    Retrieve all the parents from a set of files.

    :param files: list of files to merge
    :return: list of parent dictionaries of the form {"fid": fid}
    """
    if not config.output.grandparents:
        logger.info("Listing direct parents")
        fids = {file.fid for file in files}
    else:
        logger.info("Listing grandparents instead of direct parents")
        fids = set()
        for file in files:
            fids.update(file.parents)
    return [{"fid": fid} for fid in fids]

def match_method(name: str = None, metadata: dict = None) -> dict:
    """
    Find a built-in merging method by name or metadata conditions.

    :param name: name of the merging method
    :param metadata: metadata dictionary
    :return: merging method dictionary
    """
    # Match by name
    if name:
        methods = [m for m in config.method.defaults if m.method_name == name]
        if not methods:
            return None
        return methods[-1]
    # Match by conditions
    if metadata:
        name_dict = naming.Formatter(metadata)
        for method in reversed(config.method.defaults):
            condition = method.cond
            if name_dict.eval(condition):
                if condition == 'True':
                    condition = "unconditional"
                logger.info("Auto-selected merging method '%s' (%s)", method.method_name, condition)
                return method
    # No match found
    return None

def set_method(method: dict, warn: bool = False) -> None:
    """
    Set merging method parameters.

    :param method: merging method dictionary
    """
    lvl = logging.WARNING if warn else logging.INFO
    explicit = False
    method_name = method.method_name
    for key in ['script', 'cmd', 'cfg', 'transform']:
        if config.method[key]:
            logger.log(lvl, "Explicit value for merge.%s overrides %s default", key, method_name)
            explicit = True
        else:
            config.method[key] = method[key]
    if config.method.dependencies:
        logger.log(lvl, "Explicity adding merge.dependencies:\n  %s",
                       "\n  ".join(config.method.dependencies))
        explicit = True
    config.method.dependencies |= method.dependencies
    if config.method.outputs:
        logger.log(lvl, "Explicit list of merge.outputs overrides %s default", method_name)
        explicit = True
    else:
        config.method.outputs = method.outputs
    config.metadata.overrides.update(method.metadata)
    if warn and explicit:
        logger.warning("Consider explicitly specifying a merging method instead of using 'auto'!")

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
    config.method.method_name = method.method_name
    set_method(method, warn=True)

def set_method_custom() -> None:
    """
    Set merging method parameters for a custom script.
    """
    name = config.method.method_name
    cmd = config.method.cmd
    script = config.method.script
    if not script and (not cmd or '{script}' in cmd):
        # Assume the name is a script
        config.method.script = name
        config.method.method_name = os.path.basename(name)
    logger.info("Using custom merging method: %s", name)

def auto_output(files: list) -> None:
    """
    Auto-generate an output file name with the same extension as the inputs, if needed.

    :param files: list of files to merge
    """
    if config.method.outputs:
        return
    extensions = set()
    for file in files:
        extensions.add(os.path.splitext(file.name)[-1])
    if len(extensions) != 1:
        logger.critical("Cannot determine extension for merged files!")
        sys.exit(1)
    ext = extensions.pop()
    config.method.outputs = [{'name': f"{{NAME}}_merged_{{UUID}}{ext}"}]
    logger.info("Auto-detected file extension '%s' from input files", ext)

def log_method() -> None:
    """
    Log the final merging method configuration.
    """
    msg = [f"Final settings for merging method '{config.method.method_name}':"]
    for key in ['cmd', 'script', 'cfg', 'transform']:
        msg.append(f"{key}: {config.method[key]}")
    msg.append("dependencies:")
    msg.extend([f"  {dep}" for dep in config.method.dependencies])
    msg.append("outputs:")
    for output in config.method.outputs:
        if output.rename:
            msg.append(f"  {output.name} (renamed from {output.rename})")
        else:
            msg.append(f"  {output.name}")
        msg.append(f"    size: {output.size}")
        if output.size_min:
            msg.append(f"    size_min: {output.size_min}")
        if output.checklist:
            msg.append(f"    checklist: {output.checklist}")
        if output.metadata:
            msg.extend([f"    {k}: {v}" for k, v in output.metadata.items()])
        if output.pass2:
            msg.append(f"    pass2 method: {output.pass2}")
    logger.info("\n  ".join(msg))

def check_method(files: list) -> None:
    """
    Check and set the merging method based on the input file metadata.

    :param files: list of files to merge
    """
    # Figure out merging method
    name = config.method.method_name
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

    # Locate full paths for any dependencies
    deps = set()
    if config.method.script:
        deps.add(io_utils.find_runner(config.method.script))
    if config.method.cfg:
        deps.add(io_utils.find_cfg(config.method.cfg))
    for dep in config.method.dependencies:
        deps.add(io_utils.find_file(dep, ["config", "src"], recursive=True))

    # Check for issues with the merging command
    cmd = config.method.cmd
    if cmd:
        if config.method.script and '{script}' not in cmd:
            logger.warning("Merging command does not call provided '{script}'")
        if config.method.cfg and '{cfg}' not in cmd:
            logger.warning("Merging command does not use provided '{cfg}'")
        if '{inputs}' not in cmd:
            logger.critical("Merging command does not specify '{inputs}'")
            sys.exit(1)
        n_out = sum(1 for output in config.method.outputs if not output.rename)
        if n_out > 0 and '{output' not in cmd:
            logger.critical("Merging command does not specify '{output}' (or '{outputs[#]}')")
            sys.exit(1)

    # Check for issues with the merging outputs, and add any additional dependencies
    for idx, output in enumerate(config.method.outputs):
        if output.checklist:
            deps.add(io_utils.find_file(output.checklist, ["config", "src"], recursive=True))
        if output.pass2:
            method2 = match_method(name=output.pass2)
            if method2 is None:
                logger.critical("Output %d has unknown merging method '%s'", idx, output.method)
                sys.exit(1)
            if len(method2.outputs) != 1:
                logger.critical("Output %d merging method '%s' must produce exactly 1 output!",
                                idx, output.method)
                sys.exit(1)
            if method2.script:
                deps.add(io_utils.find_runner(method2.script))
            if method2.cfg:
                deps.add(io_utils.find_cfg(method2.cfg))
        elif len(config.method.outputs) != 1:
            logger.critical("Output %d must specify a merging method for stage-2 merges!", idx)
            sys.exit(1)

    # Convert dependencies set back to a list and store in config
    config.method.dependencies = list(deps)

    # Log final merging method configuration
    log_method()

def make_names(files: list):
    """
    Update merging method and create a name for the merged files.

    :param files: list of files to merge
    """
    check_method(files)
    metadata = merged_keys(files, warn=True) # recalculate with correct method settings
    # Set output namespaces if they are not given
    if not config.output.namespace:
        config.output.namespace = files[0].namespace
    if not config.output.scratch.namespace:
        config.output.scratch.namespace = config.output.namespace
    # Format output file names
    formatter = naming.Formatter(metadata)
    if '{UUID}' in config.output.name:
        logger.critical("File {UUID} should go in merging.method.outputs, not output.name")
        sys.exit(1)
    formatter.format(config.output.name)
    for idx, output in enumerate(config.method.outputs):
        missing_field = False
        for field in ['{NAME}', '{UUID}']:
            if field not in output.name:
                logger.critical("Output %d name must contain '%s'", idx, field)
                missing_field = True
        if missing_field:
            sys.exit(1)
        formatter.format(output.name, defer_uuid=True)
    io_utils.log_list(
        "Output file name{s}:",
        [output.name for output in config.method.outputs],
        logging.INFO
    )
    # Format any other strings in the config that may use metadata keys
    while config.string_keys:
        key_name = config.string_keys.pop()
        skip = False
        for prefix in ['method.cmd', 'method.defaults', 'naming', 'metadata']:
            if key_name.startswith(prefix):
                skip = True
                break
        if skip:
            continue
        key = config.get_key(key_name)
        if not key:
            continue
        formatter.format(key)
