"""Module for configuration settings."""

import logging
import json
import os
import sys
import socket
import fnmatch
from datetime import datetime, timezone
from typing import Any, Optional

from merge_utils import io_utils, __version__
from merge_utils.config_keys import ConfigKey, ConfigDict, ConfigMap, ConfigList, type_defs, key_defs

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = ["defaults/metadata.yaml", "defaults/defaults.yaml"]

OInt = Optional[int]
OStr = Optional[str]
OList = Optional[list]

# Configuration dictionary
cfg_dict = ConfigDict()

def __getattr__(name: str) -> Any:
    return cfg_dict.__getattr__(name)

def get_key(name: str) -> ConfigKey:
    """Get a config key by name"""
    remaining = f".{name}"
    obj = cfg_dict
    while remaining:
        obj_name = obj._name if obj._name else "root" # pylint: disable=protected-access
        if remaining.startswith('.'):
            dot_idx = remaining.find('.', 1)
            if dot_idx == -1:
                dot_idx = len(remaining)
            sub_idx = remaining.find('[', 1)
            if sub_idx == -1:
                sub_idx = len(remaining)
            idx = min(dot_idx, sub_idx)
            attr = remaining[1:idx]
            remaining = remaining[idx:]
            obj = getattr(obj, attr, None)
            if obj is None:
                raise KeyError(f"Config key '{obj_name}' has no member named '{attr}'")
        else:
            idx = remaining.find(']')
            attr = remaining[1:idx]
            remaining = remaining[idx+1:]
            if isinstance(obj, ConfigMap):
                obj = obj[attr]
            elif isinstance(obj, ConfigList):
                obj = obj[int(attr)]
            else:
                raise KeyError(f"Config key '{obj_name}' is not a collection and cannot be indexed")
    return obj

def update(file_name: str) -> None:
    """
    Update the global configuration with values from the provided dictionary.
    
    :param file_name: Name of the configuration file.
    :return: None
    """
    cfg = io_utils.read_config_file(file_name)
    errors = []
    # Check version compatibility
    ver = cfg.pop('version', None)
    if ver and ver != __version__:
        logger.error("Failed to load config file %s: version mismatch (file: %s, package: %s)",
                      file_name, ver, __version__)
        sys.exit(1)
    if 'version' not in cfg_dict._value: # pylint: disable=protected-access
        cfg_dict._update({'version': __version__}) # pylint: disable=protected-access
    # Update schema if provided
    schema = cfg.pop('schema', None)
    if schema:
        if cfg_dict._locked: # pylint: disable=protected-access
            errors.append("User configuration files cannot change the config schema!")
        else:
            type_defs.update(schema.get('type_defs', {}))
            key_defs.update(schema.get('key_defs', {}))
    # Update configuration
    errors.extend(cfg_dict._update(cfg)) # pylint: disable=protected-access
    if errors:
        io_utils.log_list(f"Failed to load config file {file_name}:",
                          errors, level=logging.CRITICAL)
        sys.exit(1)

def uuid(skip: OInt = None, limit: OInt = None, chunk: OList = None) -> str:
    """Generate a unique identifier based on the job tag and timestamp.
    
    :param skip: Number of initial entries to skip.
    :param limit: Maximum number of entries to process.
    :param chunk: Optional chunk id list to include in the UUID.
    :return: Unique identifier string.
    """
    timestamp = cfg_dict.job.timestamp
    tag = cfg_dict.input.tag
    if skip is None:
        skip = cfg_dict.input.skip
    if limit is None:
        limit = cfg_dict.input.limit
    pad = 6

    out = f"{timestamp}"
    if chunk:
        out = f"c{'-'.join(map(str, chunk))}_{out}"
    if limit:
        out = f"l{limit:0{pad}d}_{out}"
    if skip:
        out = f"s{skip:0{pad}d}_{out}"
    if tag:
        out = f"{tag}_{out}"

    return out

def set_host() -> None:
    """
    Set the host name in the configuration.

    :return: None
    """
    # Match hostname against patterns in config, using longest pattern first
    hostname = socket.gethostname()
    hosts = sorted(cfg_dict.local.hosts.items(), key=lambda x: len(x[0]), reverse=True)
    match = None
    for pattern, site in hosts:
        if not fnmatch.fnmatch(hostname, pattern):
            continue
        # We have a match
        if cfg_dict.local.site:
            # If a local site is already configured, check for agreement
            if cfg_dict.local.site != site:
                if match is None:
                    match = site
                continue
            logger.info("Configured local site %s matches host '%s'", site, hostname)
        else:
            logger.info("Selected local site '%s' based on host '%s'", site, hostname)
            cfg_dict.local.site = site
        return
    # See if we have a match that doesn't agree with the configured local site
    if match is not None:
        logger.error("Configured local site %s does not match site %s for host '%s'",
                     cfg_dict.local.site, match, hostname)
    # If we have a configured local site but no match, warn about potential misconfiguration
    elif cfg_dict.local.site:
        logger.warning("Configured local site %s does not match unknown host '%s'",
                       cfg_dict.local.site, hostname)
    # No match found
    else:
        logger.info("No local site available for unknown host '%s'", hostname)

def check_environment() -> None:
    """
    Check environment variables for default key settings

    :return: None
    """
    # Get DUNE SW version
    if cfg_dict.method.environment.dunesw_version:
        logger.info("Using DUNE SW version: %s (from cfg)",
                    cfg_dict.method.environment.dunesw_version)
    else:
        ver = os.getenv('DUNESW_VERSION')
        if ver is None:
            ver = os.getenv('DUNE_VERSION')
        cfg_dict.method.environment.dunesw_version = ver
        logger.info("Using DUNE SW version: %s (from env)", ver)
    # Get DUNE SW qualifier
    if cfg_dict.method.environment.dunesw_qualifier:
        logger.info("Using DUNE qualifier: %s (from cfg)",
                    cfg_dict.method.environment.dunesw_qualifier)
    else:
        qual = os.getenv('DUNE_QUALIFIER')
        cfg_dict.method.environment.dunesw_qualifier = qual
        logger.info("Using DUNE qualifier: %s (from env)", qual)
    # Get any additional environment variables specified in the config
    for var, val in cfg_dict.method.environment.vars.items():
        if val:
            logger.info("Using env var %s=%s (from cfg)", var, val)
        else:
            val = os.getenv(var)
            if val is None:
                logger.critical("Missing required environment variable: %s", var)
                sys.exit(1)
            cfg_dict.method.environment.vars[var] = val
            logger.info("Using env var %s=%s (from env)", var, val)

def set_error_handling() -> None:
    """
    Apply default error handling settings to any errors set to 'default' in the configuration.
    """
    default = cfg_dict.validation.handling.default
    for err, handling in cfg_dict.validation.handling.items():
        # Don't try to apply default error handling to itself
        if err == 'default':
            continue
        # Skip errors that aren't defaultable
        if 'default' not in handling:
            continue
        if handling == 'default':
            cfg_dict.validation.handling[err] = default

def custom_serializer(obj):
    """
    Custom JSON serializer for ConfigKey objects.
    
    :param obj: Object to serialize.
    :return: JSON-serializable representation of the object.
    """
    if isinstance(obj, ConfigKey):
        return obj._json() # pylint: disable=protected-access
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def dump() -> None:
    """
    Dump the current configuration to the tmp directory or stdout.

    :return: None
    """
    dest = cfg_dict.job.dir
    json_dump = json.dumps(cfg_dict, default=custom_serializer, indent=2)
    if dest:
        dest = os.path.join(str(dest), 'config.json')
        logger.info("Config written to:\n  %s", dest)
        with open(dest, 'w', encoding="utf-8") as f:
            f.write(json_dump)
    else:
        logger.info("Config:\n%s", json_dump)

def override(args: dict, arg: str, option: ConfigKey, name: OStr = None) -> OStr:
    """
    Override a configuration option with a new value.

    :param args: Dictionary of command-line arguments.
    :param arg: Name of the command-line argument.
    :param option: Configuration option to override.
    :param name: Optional name for logging purposes.
    :return: Resulting value of the configuration option.
    """
    if name is None:
        name = arg.replace('_', ' ')
    value = args.pop(arg, None)
    if value is not None:
        if option:
            logger.info("Overriding %s: %s", name, repr(value))
        option._set(value) # pylint: disable=protected-access
    if not option:
        return None
    return str(option)

def set_cmd_opts(args: dict) -> None:
    """
    Override configuration settings with command-line arguments.

    :param args: Dictionary of command-line arguments.
    :return: None
    """
    # Override configuration with command line arguments
    # I/O modes
    override(args, "input_mode", cfg_dict.input.mode)
    out_mode = override(args, "output_mode", cfg_dict.output.mode)
    local = override(args, "local", cfg_dict.output.local)
    if local and out_mode in ['validate', 'dids']:
        logger.warning("Option --local has no effect in output mode '%s'", out_mode)

    # Job settings
    override(args, "tag", cfg_dict.input.tag)
    override(args, "comment", cfg_dict.input.comment)
    override(args, "campaign", cfg_dict.input.campaign)
    skip = args.pop("skip", None)
    if skip is not None:
        if skip <= 0:
            skip = None
        if cfg_dict.input.skip:
            if skip is None:
                logger.info("Overriding skip: none")
            else:
                logger.info("Overriding skip: %d", skip)
        cfg_dict.input.skip = skip
    limit = args.pop("limit", None)
    if limit is not None:
        if limit <= 0:
            limit = None
        if cfg_dict.input.limit:
            if limit is None:
                logger.info("Overriding limit: none")
            else:
                logger.info("Overriding limit: %d", limit)
        cfg_dict.input.limit = limit

    # Already-done handling
    if args.pop("retry", False):
        if not cfg_dict.input.tag:
            logger.critical("Cannot use --retry without a job tag specified!")
            sys.exit(1)
        cfg_dict.validation.handling.already_done = 'gap'
    if cfg_dict.validation.handling.already_done != 'include' and not cfg_dict.input.tag:
        logger.warning("Already-done checking is disabled without a job tag specified!")
        cfg_dict.validation.handling.already_done = 'include'

    # Output settings
    override(args, "name", cfg_dict.output.name, "output name")
    override(args, "namespace", cfg_dict.output.namespace, "output namespace")
    override(args, "method", cfg_dict.method.method_name, "merge method")

def load(args: Optional[dict] = None) -> None:
    """
    Load the specified configuration files.
    Missing keys will be filled in with the defaults in DEFAULT_CONFIG.
    
    :param args: List of configuration files.
    :return: None
    """
    io_utils.log_print("Loading configuration...")
    # Load default configuration files first
    defaults_dir = os.path.join(io_utils.pkg_dir(), 'config', 'defaults')
    for cfg_file in os.listdir(defaults_dir):
        path = os.path.join(defaults_dir, cfg_file)
        if os.path.isfile(path):
            update(path)
    cfg_dict._lock()  # pylint: disable=protected-access
    logger.info("Loaded default configuration files.")

    if args is None:
        return

    # Load user configuration files
    user_cfgs = args.pop("config", [])
    for cfg_file in user_cfgs:
        update(cfg_file)
        cfg_dict.job.config_files.append(cfg_file)
    if user_cfgs:
        logger.info("Loaded user configuration files.")

    # Load command line overrides and environment variables
    set_cmd_opts(args)
    set_host()
    set_error_handling()
    check_environment()
    logger.info("Loaded command line overrides and environment variables.")

    # Set unique job identifier and create tmp directory
    cfg_dict.job.timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

def resume(job_dir: str, args: dict) -> None:
    """
    Reload the configuration for an existing job
    
    :param job_dir: Directory of the existing job.
    :param args: Command-line arguments to override configuration settings.
    :return: None
    """
    io_utils.log_print("Reloading old configuration...")
    if os.path.isfile(job_dir):
        job_dir = os.path.dirname(job_dir)
    cfg_file = os.path.join(job_dir, 'config.json')
    if not os.path.isfile(cfg_file):
        logger.error("Could not find config file %s", cfg_file)
        sys.exit(1)
    # Clear existing config and load from file
    cfg_dict._clear() # pylint: disable=protected-access
    update(cfg_file)
    logger.info("Loaded old configuration file.")

    # Override output mode
    out_mode = override(args, "output_mode", cfg_dict.output.mode)
    local = override(args, "local", cfg_dict.output.local)
    if local and out_mode in ['validate', 'dids']:
        logger.warning("Option --local has no effect in output mode '%s'", out_mode)
