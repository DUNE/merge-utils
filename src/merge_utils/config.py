"""Module for configuration settings."""

import logging
import json
import os
from datetime import datetime, timezone

from merge_utils import io_utils

DEFAULT_CONFIG = ["defaults/metadata.yaml", "defaults/defaults.yaml"]

class ConfigKey:
    """Base class for configuration keys"""

    def __init__(self, name: str):
        self._name = name
        self._value = None
        self._type = ['none']

    def _lock(self) -> list:
        return self._type

    def _set(self, value) -> list:
        self._value = value
        return []

    def __set__(self, instance, value):
        errors = self._set(value)
        if errors:
            raise ValueError(errors[0])

    def __get__(self, instance, owner=None):
        return self._value

class ConfigValue(ConfigKey):
    """Class to manage a configuration value with type checking"""

    def __init__(self, name: str, value = None, val_type: str = None):
        super().__init__(name)
        if value is None and val_type is None:
            raise ValueError(f"Config key {name} needs a type or default value")
        self._value = value
        if val_type is not None:
            self._type = val_type
        elif isinstance(value, str):
            self._type = 'str'
        elif isinstance(value, bool):
            self._type = 'bool'
        elif isinstance(value, int):
            self._type = 'int'
        elif isinstance(value, float):
            self._type = 'float'
        else:
            raise TypeError(f"Config key '{name}' has unsupported type")

    def _set(self, value) -> list:
        if value is None:
            self._value = None
            return []
        if self._type == 'str' and not isinstance(value, str):
            return [f"Config key '{self._name}' must be a string"]
        if self._type == 'bool' and not isinstance(value, bool):
            return [f"Config key '{self._name}' must be a boolean"]
        if self._type == 'int' and not isinstance(value, int):
            return [f"Config key '{self._name}' must be an integer"]
        if self._type == 'float' and not isinstance(value, (int, float)):
            return [f"Config key '{self._name}' must be a float"]
        self._value = value
        return []

class ConfigOption(ConfigKey):
    """Class to manage a configuration option with predefined choices"""

    def __init__(self, name: str, options: str):
        super().__init__(name)
        if not options.startswith('(') or not options.endswith(')'):
            raise ValueError(f"Config key {name} options must be in parentheses")
        options = [opt.strip() for opt in options[1:-1].split(',')]
        if len(options) <= 1:
            raise ValueError(f"Config key {name} must have more than one option")
        self._value = options[0]
        self._options = set(options)
        self._type = ['opt']

    def _set(self, value: str) -> list:
        if value not in self._options:
            return [f"Config key '{self._name}' must be one of ({', '.join(self._options)})"]
        self._value = value
        return []

class ConfigSet(ConfigKey):
    """Class to manage a configuration set (of strings)"""

    def __init__(self, name: str):
        super().__init__(name)
        self._value = set()
        self._type = ['set']

    def _set(self, value: list) -> list:
        if not isinstance(value, list):
            return [f"Config key '{self._name}' must be a list of strings"]
        errors = []
        removals = set()
        for item in value:
            if not isinstance(item, str):
                errors.append(f"Config key '{self._name}' must be a list of strings")
                continue
            if item.startswith('~'):
                removals.add(item[1:])
            else:
                self._value.add(item)
        self._value = set(value)
        return errors

    def __contains__(self, item):
        return item in self._value

    def __iter__(self):
        return iter(self._value)

class ConfigList(collections.UserList):
    """Class to manage configuration list"""

    def __init__(self, name: str):
        super().__init__()
        self._name = name
        self._locked = False
        self._type = ['list']
    
    def _lock(self) -> list:
        self._locked = True
        if len(self.data) == 0:
            self._type = ['list', 'str']  # default to list of strings
            return self._type
        types = [item._lock() for item in self.data]
        for i in range(len(types[0])):
            next_type = types[0][i]
            for t in types[1:]:
                if len(t) <= i or t[i] != next_type:
                    return self._type
            self._type.append(next_type)
        return self._type
    
    def _set(self, value: list) -> list:
        if not isinstance(value, list):
            return [f"Config key '{self._name}' must be a list"]
        errors = []
        for item in value:
            # Remove strings starting with '~'
            if isinstance(item, str) and item.startswith('~'):
                item = item[1:]
                count = 0
                while item in self.data:
                    self.data.remove(item)
                    count += 1

                # Remove the value if it starts with '~'
                for old_item in self.data:
                    if isinstance(old_item, ConfigValue) and old_item._value == item:
                        self.data.remove(old_item)
                        break
                continue
        return errors


class ConfigDict(collections.UserDict):
    """Class to manage configuration dictionary"""

    def __init__(self):
        super().__init__()
        self._locked = False
        self._extendable = False
        self._required_keys = set()
        self._type = None
    
    def __setitem__(self, key, value):
        override = False
        if key.startswith('~'):
            key = key[1:]
            override = True
        
        if self._locked and key not in self.data:
            raise KeyError(f"Cannot add new key '{key}' to locked ConfigDict")
        if self._type and not isinstance(value, self._type):
            raise TypeError(f"Value for key '{key}' must be of type {self._type.__name__}")
        super().__setitem__(key, value)

# Configuration dictionary
config_dict = ConfigDict()
timestamp: str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

logger = logging.getLogger(__name__)

def uuid() -> str:
    """Generate a unique identifier based on the job tag and timestamp."""
    tag = inputs.get('tag')
    skip = inputs.get('skip')
    limit = inputs.get('limit')
    pad = 6
    out = timestamp
    if limit:
        out = f"l{limit:0{pad}d}_{out}"
    if skip:
        out = f"s{skip:0{pad}d}_{out}"
    if tag:
        out = f"{tag}_{out}"
    return out

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
