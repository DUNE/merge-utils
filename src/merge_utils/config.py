"""Module for configuration settings."""

import logging
import json
import os
from datetime import datetime, timezone

from merge_utils import io_utils

DEFAULT_CONFIG = ["defaults/metadata.yaml", "defaults/defaults.yaml"]

# Configuration dictionaries
metadata: dict = {}
inputs: dict = {}
output: dict = {}
validation: dict = {}
sites: dict = {}
merging: dict = {}
timestamp: str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

logger = logging.getLogger(__name__)

def uuid() -> str:
    """Generate a unique identifier based on the job tag and timestamp."""
    tag = inputs.get('tag')
    if tag:
        return f"{tag}_{timestamp}"
    return timestamp

def update_list(old_list: list, new_list: list) -> None:
    """
    Append values from new_list to old_list.
    Strings beginning with '~' are removed from old_list instead.
    
    :param old_list: List to be updated.
    :param new_list: List with new values.
    :return: None
    """
    # Ensure new_list is a list
    if not isinstance(new_list, list):
        new_list = [new_list]
    for val in new_list:
        if isinstance(val, str) and val.startswith("~"):
            # Remove the value if it starts with '~'
            val = val[1:]  # Remove the '~' prefix
            if val in old_list:
                old_list.remove(val)
        elif val not in old_list:
            # Add the value if it is not already in the old list
            old_list.append(val)

def update_dict(old_dict: dict, new_dict: dict) -> None:
    """
    Add key value pairs from new_dict to old_dict.
    If a key in new_dict does not exist in old_dict, it is added.
    If the value is a dict or list, the values are merged recursively.
    If a key in new_dict starts with '~', it overrides the value in old_dict instead.
    If the value is None, the key is removed from old_dict instead.
    
    :param old_dict: Dictionary to be updated.
    :param new_dict: Dictionary with new values.
    :return: None
    """
    for key, val in new_dict.items():
        if val is None and key in old_dict:
            # If the value is None, remove the key from the old dictionary
            del old_dict[key]
            continue
        if key.startswith("~"):
            # If the key starts with '~', override the value in old_dict
            key = key[1:]  # Remove the '~' prefix
            old_dict[key] = val
            continue
        if key not in old_dict:
            # If the key does not exist in the old dictionary, add it
            old_dict[key] = val
            continue
        old_val = old_dict.get(key, None)
        if isinstance(old_val, dict):
            # If both are dictionaries, recursively update
            if isinstance(val, dict):
                update_dict(old_dict[key], val)
        elif isinstance(old_val, list):
            # If the old value is a list, extend it with the new value
            update_list(old_dict[key], val)
        else:
            old_dict[key] = val

def update(cfg: dict) -> None:
    """
    Update the global configuration with values from the provided dictionary.
    
    :param cfg: Dictionary containing new configuration values.
    :return: None
    """
    update_dict(metadata, cfg.get("metadata", {}))
    update_dict(inputs, cfg.get("inputs", {}))
    update_dict(output, cfg.get("output", {}))
    update_dict(validation, cfg.get("validation", {}))
    update_dict(sites, cfg.get("sites", {}))
    update_dict(merging, cfg.get("merging", {}))

def check_environment() -> None:
    """
    Check environment variables for default key settings

    :return: None
    """
    if 'dune_version' not in merging or merging['dune_version'] is None:
        merging['dune_version'] = os.getenv('DUNE_VERSION')
    if 'dune_qualifier' not in merging or merging['dune_qualifier'] is None:
        merging['dune_qualifier'] = os.getenv('DUNE_QUALIFIER')

def load(files: list = None) -> None:
    """
    Load the specified configuration files.
    Missing keys will be filled in with the defaults in DEFAULT_CONFIG.
    
    :param files: List of configuration files.
    :return: None
    """
    # Add the default configuration file to the beginning of the list
    if not files:
        files = DEFAULT_CONFIG
    elif isinstance(files, str):
        files = DEFAULT_CONFIG + [files]
    else:
        files = DEFAULT_CONFIG + files

    for file in files:
        cfg = io_utils.read_config_file(file)
        logger.info("Loaded configuration file %s", file)
        update(cfg)

    check_environment()

    msg = [
        "Final merged configuration:",
        f"metadata: {json.dumps(metadata, indent=2)}",
        f"inputs: {json.dumps(inputs, indent=2)}",
        f"output: {json.dumps(output, indent=2)}",
        f"validation: {json.dumps(validation, indent=2)}",
        f"sites: {json.dumps(sites, indent=2)}",
        f"merging: {json.dumps(merging, indent=2)}"
    ]
    logger.info("\n".join(msg))
