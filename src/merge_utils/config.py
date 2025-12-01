"""Module for configuration settings."""

import logging
import json
import os
import sys
from datetime import datetime, timezone

from merge_utils import io_utils

DEFAULT_CONFIG = ["defaults/metadata.yaml", "defaults/defaults.yaml"]

logger = logging.getLogger(__name__)

timestamp: str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
uuid: str = timestamp  # Placeholder for unique identifier

type_defs = {}
key_defs = {}

string_keys = set()
path_keys = set()

def parse_type(value: str) -> tuple:
    """
    Parse a type string into a list of type components.
    
    :param value: Type string of the form '<type(subtype)>value'
    :return: Tuple of (type, sub_type, value)
    """
    if not isinstance(value, str):
        return type(value).__name__, None, value
    value = value.replace(' ', '')
    # Types specified with <type>value
    if value.startswith('<') and '>' in value:
        key_type, value = value[1:].rsplit('>', 1)
    else:
        return 'str', None, value
    # Sub-types specified with <type(subtype)>
    sub_type = None
    if '(' in key_type and key_type.endswith(')'):
        key_type, sub_type = key_type[:-1].split('(', 1)
    return key_type, sub_type, value

def check_condition(condition: str) -> tuple:
    """
    Check that a condition expression is valid.

    :param condition: Condition expression string
    :return: Tuple of (normalized condition string, list of errors)
    """
    if condition is None:
        return "False", []
    if isinstance(condition, bool):
        return str(condition), []
    if not isinstance(condition, str):
        return None, ["must be a condition expression"]
    if condition.lower() in ['', 'false', '0', 'no']:
        return "False", []
    if condition.lower() in ['true', '1', 'yes']:
        return "True", []
    # TODO: Parse the condition expression to check validity?
    if config_dict._locked: # pylint: disable=protected-access
        return condition, ["cannot be set by user configuration files"]
    return condition, []

class ConfigKey:
    """Base class for configuration keys"""

    def __init__(self, name: str, type_name: str = None):
        self._name = name
        self._value = None
        self._type = type_name if type_name else 'none'

    def _lock(self) -> None:
        """Prevent further changes to the config schema after loading defaults"""
        pass

    def _clear(self) -> None:
        """Clear the value of the key"""
        self._value = None

    def _update(self, value) -> list:
        """Recursively update the config tree and return any errors"""
        self._value = value
        return []
    
    def _set(self, value) -> None:
        """Set the value of the key (used for assignment)"""
        errors = self._update(value)
        if errors:
            if len(errors) == 1:
                raise TypeError(errors[0])
            raise TypeError(f"Failed to set config key '{self._name}':\n  {'\n  '.join(errors)}")

    def _err(self, msg: str) -> str:
        """Format an error message for this config key"""
        if self._name:
            return f"Config key '{self._name}' {msg}"
        return f"Config root {msg}"
    
    def _json(self):
        """Return a JSON-serializable representation of the config key"""
        return self._value

    def __str__(self):
        return str(self._value)

    def __repr__(self):
        return f"<{self._type}> {repr(self._value)}"

    def __eq__(self, value):
        if isinstance(value, ConfigKey):
            return self._value == value._value and self._type == value._type
        return self._value == value

class ConfigString(ConfigKey):
    """Class to manage a configuration string"""

    def __init__(self, name: str, type_name: str = None):
        if type_name is None:
            type_name = 'str'
        super().__init__(name, type_name)
        if type_name not in ['str', 'path']:
            raise ValueError(self._err("must be of type 'str' or 'path'"))
        self._default = None
        if type_name == 'str':
            string_keys.add(name)
        else:
            path_keys.add(name)

    def _lock(self) -> None:
        self._default = self._value

    def _clear(self) -> None:
        self._value = self._default

    def _update(self, value: str) -> list:
        if value is None:
            self._value = None
            return []
        if not isinstance(value, str):
            return [self._err("must be a string")]
        self._value = value
        return []

    def _set(self, value: str) -> None:
        if value is None:
            self._value = None
            return
        try:
            self._value = str(value)
        except (ValueError, TypeError):
            raise TypeError(self._err("must be a string"))
    
    def __del__(self):
        try:
            if self._type[0] == 'str':
                string_keys.discard(self._name)
            else:
                path_keys.discard(self._name)
        except Exception:
            pass

class ConfigCondition(ConfigKey):
    """Class to manage a configuration condition expression"""

    def __init__(self, name: str):
        super().__init__(name, 'cond')
        self._locked = False
        if config_dict._locked: # pylint: disable=protected-access
            self._locked = True

    def _lock(self) -> None:
        if self._value is None:
            self._value = "False"
        self._locked = True

    def _clear(self) -> None:
        self._value = "False"

    def _update(self, value: str) -> list:
        if self._locked:
            return [self._err("cannot be modified by user configuration files")]
        value, errors = check_condition(value)
        if errors:
            return [self._err(err) for err in errors]
        self._value = value
        return []

class ConfigValue(ConfigKey):
    """Class to manage a configuration value with type checking"""

    def __init__(self, name: str, type_name: str):
        super().__init__(name, type_name)
        if type_name not in ['bool', 'int', 'float']:
            raise TypeError(self._err(f"must be of type 'bool', 'int', or 'float'"))
        self._default = None

    def _lock(self) -> None:
        self._default = self._value

    def _clear(self) -> None:
        self._value = self._default

    def _update(self, value) -> list:
        if value is None:
            self._value = None
            return []
        if self._type == 'bool' and not isinstance(value, bool):
            return [self._err("must be a boolean")]
        if self._type == 'int' and not isinstance(value, int):
            return [self._err("must be an integer")]
        if self._type == 'float' and not isinstance(value, (int, float)):
            return [self._err("must be a float")]
        self._value = value
        return []
    
    def _set(self, value) -> None:
        if value is None:
            self._value = None
            return
        error = ""
        if self._type == 'bool':
            if isinstance(value, str):
                if value.lower() in ['true', '1', 'yes']:
                    self._value = True
                elif value.lower() in ['false', '0', 'no']:
                    self._value = False
                else:
                    error = self._err("must be a boolean")
            elif isinstance(value, bool):
                self._value = value
            else:
                error = self._err("must be a boolean")
        elif self._type == 'int':
            try:
                self._value = int(value)
            except (ValueError, TypeError):
                error = self._err("must be an integer")
        elif self._type == 'float':
            try:
                self._value = float(value)
            except (ValueError, TypeError):
                error = self._err("must be a float")
        else:
            error = self._err(f"has unsupported type {self._type}")
        if error:
            raise TypeError(error)

    def __gt__(self, other):
        if isinstance(other, ConfigValue):
            return self._value > other._value
        return self._value > other
    
    def __lt__(self, other):
        if isinstance(other, ConfigValue):
            return self._value < other._value
        return self._value < other

class ConfigOption(ConfigKey): # pylint: disable=too-few-public-methods
    """Class to manage a configuration option with predefined choices"""

    def __init__(self, name: str, options: str):
        super().__init__(name, 'opt')
        options = [opt.strip() for opt in options.split(',')]
        self._value = options[0]
        self._options = [options[0]] + sorted(set(options) - set([options[0]]))
        if len(self._options) <= 1:
            raise ValueError(self._err("must have more than one option"))

    def _clear(self) -> None:
        self._value = self._options[0]

    def _update(self, value: str) -> list:
        if value not in self._options:
            return [self._err(f"must be one of ({', '.join(self._options)})")]
        self._value = value
        return []
    
    def _set(self, value: str) -> None:
        errors = self._update(value)
        if errors:
            raise ValueError(errors[0])

class ConfigTuple(ConfigKey):
    """Class to manage a configuration tuple"""

    def __init__(self, name: str):
        super().__init__(name, 'tuple')
        self._default = None
    
    def _lock(self) -> None:
        self._default = self._value.copy()

    def _update(self, value) -> list:
        if isinstance(value, str) and value.startswith('(') and value.endswith(')'):
            value = [item.strip() for item in value[1:-1].split(',')]
        elif isinstance(value, (int, float)):
            value = [value]
        elif not isinstance(value, (list, tuple)):
            return [self._err("must be a tuple (got '{value}')")]
        if self._default is None:
            self._value = list(value)
            return []
        if len(value) > len(self._value):
            return [self._err(f"must have at most {len(self._value)} elements")]
        for i in range(len(self._value)):
            self._value[i] = value[i] if i < len(value) else self._default[i]
        return []

class ConfigCollection(ConfigKey):
    """Base class for configuration collections"""

    def _set(self, value) -> None:
        self._clear()
        errors = self._update(value)
        if errors:
            if len(errors) == 1:
                raise TypeError(errors[0])
            raise TypeError(f"Failed to set config key '{self._name}':\n  {'\n  '.join(errors)}")

    def _json(self):
        return [val._json() for val in self._value]
    
    def __contains__(self, item):
        return item in self._value

    def __iter__(self):
        return iter(self._value)

    def __len__(self):
        return len(self._value)

class ConfigSet(ConfigCollection):
    """Class to manage a configuration set (of strings)"""

    def __init__(self, name: str, val_type: str = None):
        super().__init__(name, 'set')
        self._value = set()
        self._val_type = 'str'
        if val_type is not None:
            if val_type not in ['str']:
                raise ValueError(self._err("set only supports 'str' values"))
            self._val_type = val_type

    def _clear(self) -> None:
        self._value = set()

    def _update(self, value: list) -> list:
        if not isinstance(value, list):
            return [self._err("must be a list of strings")]
        if not all(isinstance(item, str) for item in value):
            return [self._err("must be a list of strings")]
        self._value -= set(item[1:] for item in value if item.startswith('~'))
        self._value |= set(item for item in value if not item.startswith('~'))
        return []

class ConfigMap(ConfigCollection):
    """Class to manage a configuration map"""

    def __init__(self, name: str, val_type: str = None):
        super().__init__(name, 'map')
        self._value = {}
        self._required = set()
        self._key_type = 'str'
        self._val_type = '<str>'
        if val_type is not None:
            self._val_type = val_type
            sub_type = ''
            if '(' in val_type:
                self._val_type, sub_type = val_type.split('(', 1)
                sub_type = '(' + sub_type
            if ',' in self._val_type:
                self._key_type, self._val_type = self._val_type.split(',', 1)
            self._val_type = f"<{self._val_type}{sub_type}>"
            logger.debug("ConfigMap '%s' (%s: %s) created from spec '%s'", name, self._key_type, self._val_type, val_type)
            if self._key_type not in ['str', 'cond']:
                raise ValueError(self._err("map only supports 'str' or 'cond' keys"))

    def _lock(self) -> None:
        for val in self._value.values():
            val._lock() # pylint: disable=protected-access

    def _clear(self) -> None:
        # Remove all non-required keys
        self._value = {k: v for k, v in self._value.items() if k in self._required}
        # Clear all required keys
        for val in self._value.values():
            val._clear() # pylint: disable=protected-access

    def _update(self, value: dict) -> list:
        if not isinstance(value, dict):
            return [self._err(f"must be a dictionary with {self._key_type} keys")]
        errors = []
        key_errors = []
        for key, val in value.items():
            if not isinstance(key, str):
                return [self._err(f"must be a dictionary with {self._key_type} keys")]
            if self._key_type == 'cond':
                # Make sure the condition string is valid
                key, key_errors = check_condition(key)
                if key is None:
                    errors.extend([self._err(f"key {err}") for err in key_errors])
                    continue
            else:
                # String keys starting with '~' override existing keys
                if key.startswith('~'):
                    key = key[1:]
                    if key in self._value:
                        self._value[key]._clear() # pylint: disable=protected-access
            # Values set to None remove existing keys if permitted
            name = f"{self._name}[{key}]"
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
            if key_errors:
                errors.extend([self._err(f"key {err}") for err in key_errors])
                continue
            if name in key_defs:
                self._required.add(key)
            new_key, new_errors = make_cfg_key(name, self._val_type)
            errors.extend(new_errors)
            if new_key is None:
                continue
            errors.extend(new_key._update(val)) # pylint: disable=protected-access
            self._value[key] = new_key
        return errors

    def _json(self):
        return {key: val._json() for key, val in self._value.items()}

    def __getitem__(self, key):
        return self._value[key]

    def __setitem__(self, key, value):
        errors = self._update({key: value})
        if errors:
            raise ValueError(errors[0])

class ConfigList(ConfigCollection):
    """Class to manage a configuration list"""

    def __init__(self, name: str, val_type: str = None):
        super().__init__(name, 'list')
        self._value = []
        self._val_type = '<str>'
        if val_type is not None:
            self._val_type = f"<{val_type}>"

    def _lock(self) -> None:
        for val in self._value:
            val._lock() # pylint: disable=protected-access

    def _clear(self) -> None:
        self._value = []

    def _update(self, value: list) -> list:
        if not isinstance(value, list):
            return [self._err("must be a list")]
        errors = []
        for item in value:
            name = f"{self._name}[{len(self._value)}]"
            new_key, new_errors = make_cfg_key(name, self._val_type)
            errors.extend(new_errors)
            if new_key is None:
                continue
            errors.extend(new_key._update(item)) # pylint: disable=protected-access
            self._value.append(new_key)
        return errors

class ConfigDict(ConfigKey):
    """Class to manage a configuration dictionary"""

    def __init__(self, name: str = None, type_name: str = None):
        if name is None:
            name = ""
        if type_name is None:
            type_name = 'dict'
        super().__init__(name, type_name)
        self._value = {}
        self._locked = False
        if type_name != 'dict':
            errors = self._update(type_defs[type_name])
            self._locked = True
            if errors:
                raise TypeError(f"Config class {type_name} has invalid spec:\n  {'\n  '.join(errors)}")

    def _lock(self) -> None:
        self._locked = True
        for val in self._value.values():
            val._lock() # pylint: disable=protected-access

    def _clear(self) -> None:
        for val in self._value.values():
            val._clear() # pylint: disable=protected-access

    def _update(self, value: dict) -> list:
        if not isinstance(value, dict):
            return [self._err("must be a dictionary with string keys")]
        errors = []
        for key, val in value.items():
            if not isinstance(key, str):
                return [self._err("must be a dictionary with string keys")]
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
                errors.append(self._err(f"has no member named '{key}'"))
                continue
            # Create new keys
            name = f"{self._name}.{key}" if self._name else key
            new_key, new_errors = make_cfg_key(name, val)
            errors.extend(new_errors)
            if new_key is not None:
                self._value[key] = new_key
        return errors

    def _json(self):
        return {key: val._json() for key, val in self._value.items()}

    def __getattr__(self, key):
        return self._value[key]

    def __setattr__(self, key, value):
        if key.startswith('_'):
            super().__setattr__(key, value)
        elif key in self._value:
            self._value[key]._set(value)
        elif not self._locked:
            new_key, errors = make_cfg_key(key, value)
            if errors:
                raise ValueError(errors[0])
            self._value[key] = new_key
        else:
            raise AttributeError(self._err(f"has no member named '{key}'"))

# Configuration dictionary
config_dict = ConfigDict()

CONFIG_CLASSES = {
#    'val': ConfigValue,
#    'str': ConfigString,
    'cond': ConfigCondition,
    'opt': ConfigOption,
    'tuple': ConfigTuple,
    'set': ConfigSet,
    'map': ConfigMap,
    'list': ConfigList,
    'dict': ConfigDict
}

def make_cfg_key(name: str, value: any = None) -> tuple:
    """Factory function to create appropriate ConfigKey subclass based on type name."""
    type_name, sub_type, value = parse_type(value)
    if name in key_defs:
        logger.debug("Overriding config key '%s' with spec: %s", name, repr(key_defs[name]))
        spec = key_defs.pop(name)
        if spec is not None:
            type_name, sub_type, default = parse_type(spec)
            if value is None and default is not None:
                value = default
    if type_name in ['bool', 'int', 'float']:
        key = ConfigValue(name, type_name)
    elif type_name in ['str', 'path']:
        key = ConfigString(name, type_name)
    elif type_name in type_defs:
        key = ConfigDict(name, type_name)
    else:
        key_class = CONFIG_CLASSES.get(type_name, None)
        if key_class is None:
            return None, [f"Config key '{name}' has unsupported type '{type_name}'"]
        key = key_class(name, sub_type) if sub_type else key_class(name)
    if value:
        errors = key._update(value)
        if errors:
            return None, errors
    return key, []

def update(file_name: str) -> None:
    """
    Update the global configuration with values from the provided dictionary.
    
    :param file_name: Name of the configuration file.
    :return: None
    """
    cfg = io_utils.read_config_file(file_name)
    errors = []
    schema = cfg.pop('schema', None)
    if schema:
        if config_dict._locked: # pylint: disable=protected-access
            errors.append("User configuration files cannot change the config schema!")
        else:
            type_defs.update(schema.get('type_defs', {}))
            key_defs.update(schema.get('key_defs', {}))
    errors.extend(config_dict._update(cfg)) # pylint: disable=protected-access
    if errors:
        io_utils.log_list(f"Failed to load config file {file_name}:", errors, level=logging.CRITICAL)
        sys.exit(1)

def set_uuid():
    """Generate a unique identifier based on the job tag and timestamp."""
    tag = config_dict.inputs.tag
    skip = config_dict.inputs.skip
    limit = config_dict.inputs.limit
    pad = 6
    global uuid
    if limit:
        uuid = f"l{limit:0{pad}d}_{uuid}"
    if skip:
        uuid = f"s{skip:0{pad}d}_{uuid}"
    if tag:
        uuid = f"{tag}_{uuid}"

def check_environment() -> None:
    """
    Check environment variables for default key settings

    :return: None
    """
    if config_dict.merging.dunesw_version is None:
        ver = os.getenv('DUNESW_VERSION')
        if ver is None:
            ver = os.getenv('DUNE_VERSION')
        config_dict.merging.dunesw_version = ver
    if config_dict.merging.dunesw_qualifier is None:
        config_dict.merging.dunesw_qualifier = os.getenv('DUNE_QUALIFIER')

def custom_serializer(obj):
    if isinstance(obj, ConfigKey):
        return obj._json()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def dump() -> str:
    """
    Dump the current configuration as a JSON string.

    :return: JSON string of the current configuration
    """
    return json.dumps(config_dict, default=custom_serializer, indent=2)

def load(files: list = None) -> None:
    """
    Load the specified configuration files.
    Missing keys will be filled in with the defaults in DEFAULT_CONFIG.
    
    :param files: List of configuration files.
    :return: None
    """
    # Load default configuration files first
    for file in DEFAULT_CONFIG:
        update(file)
    config_dict._lock()  # pylint: disable=protected-access
    # Load user configuration files
    if files is None:
        files = []
    for file in files:
        update(file)

    check_environment()

    logger.info("Final merged configuration:\n%s", dump())
