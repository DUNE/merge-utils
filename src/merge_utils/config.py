"""Module for configuration settings."""

import logging
import json
import os
from datetime import datetime, timezone

from merge_utils import io_utils

DEFAULT_CONFIG = ["defaults/metadata.yaml", "defaults/defaults.yaml"]

type_defs = dict()
key_defs = dict()

def make_cfg_type(name, type_name):
    """Factory function to create appropriate ConfigKey subclass based on type name."""
    if type_name in ['<bool>', '<int>', '<float>', '<str>']:
        return ConfigValue(name, None, type_name[1:-1]), []
    if type_name.startswith('<opt>'):
        return ConfigOption(name, type_name[5:]), []
    if type_name.startswith('<set(') and type_name.endswith(')>'):
        val_type = type_name[5:-2]
        return ConfigSet(name, val_type), []
    if type_name.startswith('<map(') and type_name.endswith(')>'):
        val_type = type_name[5:-2]
        return ConfigMap(name, val_type), []
    if type_name.startswith('<list(') and type_name.endswith(')>'):
        val_type = type_name[6:-2]
        return ConfigList(name, val_type), []
    if type_name in type_defs:
        return ConfigClass(name, type_name, type_defs[type_name]), []
    return None, [f"Config key '{name}' has unsupported type '{type_name}'"]

def make_cfg_val(name, val):
    """Factory function to create appropriate ConfigKey subclass based on value."""
    # If the key has a predefined type, use that
    if name in key_defs:
        key, errors = make_cfg_type(name, key_defs[name])
        errors.extend(key._update(val)) # pylint: disable=protected-access
        return key, errors
    # If the value is a type definition, use that
    if isinstance(val, str) and val.startswith('<'):
        return make_cfg_type(name, val)
    # If the value is a simple type, create a ConfigValue
    if isinstance(val, (bool, int, float, str)):
        return ConfigValue(name, val), []
    if isinstance(val, list):
        out = ConfigSet(name)
        errors = out._update(val) # pylint: disable=protected-access
        return out, errors
    if isinstance(val, dict):
        out = ConfigDict(name)
        errors = out._update(val) # pylint: disable=protected-access
        return out, errors
    return None, [f"Config key '{name}' has unsupported value '{val}'"]

class ConfigKey:
    """Base class for configuration keys"""

    def __init__(self, name: str):
        self._name = name
        self._value = None
        self._type = ['none']

    def _lock(self) -> list:
        return self._type

    def _clear(self) -> None:
        self._value = None

    def _update(self, value) -> list:
        self._value = value
        return []

    def __str__(self):
        return str(self._value)

    def __repr__(self):
        return f"<{self._type[0]}> {repr(self._value)}"

    def __eq__(self, value):
        if isinstance(value, ConfigKey):
            return self._value == value._value
        return self._value == value

class ConfigValue(ConfigKey): # pylint: disable=too-few-public-methods
    """Class to manage a configuration value with type checking"""

    def __init__(self, name: str, value = None, val_type: str = None):
        super().__init__(name)
        if value is None and val_type is None:
            raise ValueError(f"Config key {name} needs a type or default value")
        self._value = value
        if val_type is not None:
            self._type = val_type
        elif isinstance(value, (bool, int, float, str)):
            self._type = [type(value).__name__]
        else:
            raise TypeError(f"Config key '{name}' has unsupported type")
        self._default = value

    def _lock(self) -> list:
        self._default = self._value
        return self._type
    
    def _clear(self) -> None:
        self._value = self._default

    def _update(self, value) -> list:
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

class ConfigOption(ConfigValue): # pylint: disable=too-few-public-methods
    """Class to manage a configuration option with predefined choices"""

    def __init__(self, name: str, options: str):
        super().__init__(name, val_type='str')
        if not options.startswith('(') or not options.endswith(')'):
            raise ValueError(f"Config key {name} options must be in parentheses")
        options = [opt.strip() for opt in options[1:-1].split(',')]
        self._value = options[0]
        self._options = [options[0]] + sorted(set(options) - set([options[0]]))
        if len(self._options) <= 1:
            raise ValueError(f"Config key {name} must have more than one option")
        self._type = ['opt']

    def _update(self, value: str) -> list:
        if value not in self._options:
            return [f"Config key '{self._name}' must be one of ({', '.join(self._options)})"]
        self._value = value
        return []

class ConfigCollection(ConfigKey):
    """Base class for configuration collections"""

    def __init__(self, name: str):
        super().__init__(name)

    def __contains__(self, item):
        return item in self._value

    def __iter__(self):
        return iter(self._value)

    def __len__(self):
        return len(self._value)

class ConfigSet(ConfigCollection):
    """Class to manage a configuration set (of strings)"""

    def __init__(self, name: str, val_type: str = None):
        super().__init__(name)
        self._value = set()
        self._type = ['set']
        if val_type is None:
            self._type.append('str')
        else:
            self._type.extend(val_type)

    def _clear(self) -> None:
        self._value = set()

    def _update(self, value: list) -> list:
        if not isinstance(value, list):
            return [f"Config key '{self._name}' must be a list of strings"]
        if not all(isinstance(item, str) for item in value):
            return [f"Config key '{self._name}' must be a list of strings"]
        self._value -= set(item[1:] for item in value if item.startswith('~'))
        self._value |= set(item for item in value if not item.startswith('~'))
        return []

class ConfigMap(ConfigCollection):
    """Class to manage a configuration map"""

    def __init__(self, name: str, val_type: list = None):
        super().__init__(name)
        self._value = dict()
        self._required = set()
        self._type = ['map']
        if val_type is None:
            self._type.append('str')
        else:
            self._type.extend(val_type)

    def _clear(self) -> None:
        # Remove all non-required keys
        self._value = {k: v for k, v in self._value.items() if k in self._required}
        # Clear all required keys
        for val in self._value.values():
            val._clear() # pylint: disable=protected-access

    def _add_required(self, key: str, value) -> None:
        """Add a required key to the map"""
        self._value[key] = value
        self._required.add(key)

    def _update(self, value: dict) -> list:
        if not isinstance(value, dict):
            return [f"Config key '{self._name}' must be a dictionary with string keys"]
        errors = []
        for key, val in value.items():
            if not isinstance(key, str):
                return [f"Config key '{self._name}' must be a dictionary with string keys"]
            # Keys starting with '~' override existing keys
            if key.startswith('~'):
                key = key[1:]
                if key in self._value:
                    self._value[key]._clear() # pylint: disable=protected-access
            # Values set to None remove existing keys if permitted
            name = f"{self._name}.{key}" if self._name else key
            if val is None:
                if key in self._required:
                    self._value[key]._clear() # pylint: disable=protected-access
                elif key in self._value:
                    del self._value[key]
                continue
            # If the key exists, update it
            if key in self._value:
                errors.extend(self._value[key]._update(val)) # pylint: disable=protected-access
                continue
            # If the key does not exist, create it
            new_key, new_errors = make_cfg_type(name, self._type[1])
            errors.extend(new_errors)
            if new_key is None:
                continue
            errors.extend(new_key._update(val)) # pylint: disable=protected-access
            self._value[key] = new_key
        return errors
    
    def __getitem__(self, key):
        return self._value[key]

    def __setitem__(self, key, value):
        errors = self._update({key: value})
        if errors:
            raise ValueError(errors[0])

class ConfigList(ConfigCollection):
    """Class to manage a configuration list"""

    def __init__(self, name: str, val_type: str = None):
        super().__init__(name)
        self._value = list()
        self._type = ['list']
        if val_type is None:
            self._type.append('str')
        else:
            self._type.extend(val_type)

    def _clear(self) -> None:
        self._value = list()

    def _update(self, value: list) -> list:
        if not isinstance(value, list):
            return [f"Config key '{self._name}' must be a list"]
        errors = []
        for item in value:
            name = f"{self._name}[{len(self._value)}]"
            new_key, new_errors = make_cfg_type(name, self._type[1])
            errors.extend(new_errors)
            if new_key is None:
                continue
            errors.extend(new_key._update(item)) # pylint: disable=protected-access
            self._value.append(new_key)
        return errors

class ConfigDict(ConfigKey):
    """Class to manage a configuration dictionary"""

    def __init__(self, name: str):
        super().__init__(name)
        self._value = dict()
        self._locked = False
        self._type = ['dict']
    
    def _lock(self) -> list:
        self._locked = True
        return self._type
    
    def _clear(self) -> None:
        for val in self._value.values():
            val._clear() # pylint: disable=protected-access
    
    def _update(self, value: dict) -> list:
        if not isinstance(value, dict):
            return [f"Config key '{self._name}' must be a dictionary"]
        errors = []
        for key, val in value.items():
            if not isinstance(key, str):
                return [f"Config key '{self._name}' must be a dictionary with string keys"]
            # Keys starting with '~' override existing keys
            if key.startswith('~'):
                key = key[1:]
                if key in self._value:
                    self._value[key]._clear() # pylint: disable=protected-access
            # Values set to None clear existing keys but do not remove them
            if val is None:
                self._value[key]._clear() # pylint: disable=protected-access
                continue
            # Update keys that already exist
            if key in self._value:
                errors.extend(self._value[key]._update(val)) # pylint: disable=protected-access
                continue
            # Cannot add new keys if locked
            if self._locked:
                errors.append(f"Config key '{self._name}' has no member named '{key}'")
                continue
            # Create new keys
            new_key, new_errors = make_config_key(f"{self._name}.{key}", val)
            errors.extend(new_errors)
            if new_key is not None:
                self._value[key] = new_key
        return errors
    
    def __getattr__(self, key):
        return self._value[key]

    def __setattr__(self, key, value):
        if key.startswith('_'):
            super().__setattr__(key, value)
        else:
            errors = self._update({key: value})
            if errors:
                raise ValueError(errors[0])

class ConfigClass(ConfigDict):
    """Class to manage a configuration class instance"""

    def __init__(self, name: str, type_name: str, spec: dict):
        super().__init__(name)
        self._type = [type_name]
        errors = self._update(spec)
        if errors:
            raise TypeError(f"Config key '{name}' has invalid class specification:\n  {'\n  '.join(errors)}")
        self._lock()

    

class ConfigDict(ConfigKey):
    """Class to manage a configuration dictionary"""

    def __init__(self, name: str):
        super().__init__(name)
        self._value = dict()
        self._locked = False
        self._extendable = False
        self._required = set()
        self._type = ['dict']

    def _lock(self) -> list:
        self._locked = True
        if len(self._value) == 0:
            self._type = ['dict', 'str']  # default to list of strings
            return self._type
        types = []
        for key, val in self._value.items():
            key_type = val._lock() # pylint: disable=protected-access
            if not self._extendable or key in self._required:
                continue
            if types is []:
                types = key_type
                continue
            raise TypeError(f"Config key '{self._name}' has inconsistent types for extendable keys")
        self._type = ['dict'] + types
        return self._type

    def _clear(self) -> None:
        # If extendable, remove all non-required keys
        if self._extendable:
            self._value = {k: v for k, v in self._value.items() if k in self._required}
        # Clear all keys including required keys
        for val in self._value.values():
            val._clear() # pylint: disable=protected-access

    def _update(self, value: dict) -> list:
        if not isinstance(value, dict):
            return [f"Config key '{self._name}' must be a dictionary"]
        errors = []
        for key, val in value.items():
            # Keys starting with '~' override existing keys
            if key.startswith('~'):
                key = key[1:]
                if key in self._value:
                    self._value[key]._clear() # pylint: disable=protected-access
            # Values set to None remove existing keys if permitted
            name = f"{self._name}.{key}" if self._name else key
            if val is None:
                if not self._extendable:
                    errors.append(f"Config key '{self._name}' does not support removing keys")
                elif key in self._required:
                    errors.append(f"Config key '{name}' is required and cannot be removed")
                elif key in self._value:
                    del self._value[key]
                continue
            # If the key exists, update it
            if key in self._value:
                errors.extend(self._value[key]._update(val)) # pylint: disable=protected-access
                continue
            # If the key does not exist, create it if permitted
            if self._locked and not self._extendable:
                errors.append(f"Config key '{self._name}' does not support adding new keys")
                continue
            new_key, new_errors = make_config_key(name, val, self._type[1:])
            errors.extend(new_errors)
            if new_key is None:
                continue
            if not self._locked:
                self._value[key] = new_key
                continue
            # If locked, check that the type of the new key is correct
            key_type = new_key._lock() # pylint: disable=protected-access
            if key_type != self._type[1:]:
                errors.append(f"Config key '{name}' must be of type {'->'.join(self._type[1:])}'")
                continue
            self._value[key] = new_key
        return errors

    def __contains__(self, item):
        return item in self._value

    def __iter__(self):
        return iter(self._value)

    def __len__(self):
        return len(self._value)

    def __getitem__(self, key):
        return self._value[key]

    def __setitem__(self, key, value):
        errors = self._update({key: value})
        if errors:
            raise ValueError(errors[0])

    def __getattr__(self, key):
        return self._value[key]

    def __setattr__(self, key, value):
        if key.startswith('_'):
            super().__setattr__(key, value)
        else:
            errors = self._update({key: value})
            if errors:
                raise ValueError(errors[0])


class ConfigList(ConfigKey):
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
        types = 
        [item._lock() for item in self.data]
        for i in range(len(types[0])):
            next_type = types[0][i]
            for t in types[1:]:
                if len(t) <= i or t[i] != next_type:
                    return self._type
            self._type.append(next_type)
        return self._type
    
    def _update(self, value: list) -> list:
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
