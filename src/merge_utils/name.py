"""Utilities for expanding name templates using metadata."""

import os
import sys
import logging

from merge_utils import config, io_utils

logger = logging.getLogger(__name__)

CONFIG_KEYS = {
    "CFG": "config_dict",
    "TIMESTAMP": "timestamp",
    "UUID": "uuid",
    "NAME": "output.name",
    "DUNESW_VERSION": "merging.dunesw_version",
    "DUNESW_QUALIFIER": "merging.dunesw_qualifier"
}

KEY_BLACKLIST = {
    "$DUNESW_VERSION": "DUNESW_VERSION",
    "$DUNE_VERSION": "DUNESW_VERSION",
    "$DUNE_QUALIFIER": "DUNESW_QUALIFIER",
    "DUNE_VERSION": "DUNESW_VERSION",
    "DUNE_QUALIFIER": "DUNESW_QUALIFIER"
}

def read_list(data: list, idx: any) -> tuple:
    """
    Try to extract a value from a list.
    
    :param data: list of data
    :param idx: index or slice string
    :return: extracted value and list of errors
    """
    if isinstance(idx, int):
        if idx >= 0 and idx < len(data):
            return data[idx], []
        return None, ["index out of range"]
    elif isinstance(idx, str):
        try:
            idx = [int(i) if i else None for i in idx.split(':')]
            if len(idx) == 1:
                return data[idx[0]]
            if len(idx) == 2:
                return data[slice(idx[0], idx[1])]
            if len(idx) == 3:
                return data[slice(idx[0], idx[1], idx[2])]
        except ValueError:
            pass
        return None, ["invalid slice"]
    return None, ["invalid index"]

def read_dict(data: dict, key: any) -> tuple:
    """
    Try to extract a value from a dictionary.
    
    :param data: dictionary of data
    :param key: dictionary key
    :return: extracted value
    """
    if isinstance(key, str):
        # Remove quotes from string keys
        if key.startswith("'") and key.endswith("'"):
            key = key[1:-1]
        elif key.startswith('"') and key.endswith('"'):
            key = key[1:-1]
    val = data.get(key)
    if val is None:
        return None, ["invalid index"]
    return val, []

class Formatter:
    """Wrapper class to access metadata dictionary."""

    def __init__(self, metadata: dict = None):
        """
        Initialize the Formatter with a metadata dictionary.
        """
        self.metadata = metadata
        self.errors = []
        self.metadata_err = False

    def get(self, key, idx=None):
        """
        Get a metadata value by key.

        :param key: metadata key
        :param idx: optional index or slice
        :return: metadata value
        """
        if self.metadata is None:
            self.metadata_err = True
            self.errors.insert(0, "Metadata keys cannot be inserted into this string")
            return None
        val = self.metadata.get(key)
        if val is None:
            self.errors.append(f"Metadata key '{key}' not found")
        elif idx is not None:
            errors = []
            if isinstance(val, list):
                val, errors = read_list(val, idx)
            elif isinstance(val, dict):
                val, errors = read_dict(val, idx)
            elif hasattr(val, '__getitem__'):
                try:
                    val = val[self._idx]
                except (KeyError, IndexError, TypeError):
                    errors = ["invalid index"]
                    val = None
            else:
                self.errors.append(f"Metadata key '{key}' is not subscriptable")
                val = None
            self.errors.extend([f"Metadata key '{key}[{idx}]' has {err}" for err in errors])
        return val
    
    def format_key(self, key: str, val: any, spec: str) -> str:
        """
        Format a metadata key with a given specification.

        :param key: key name
        :param val: key value
        :param spec: format specification
        :return: formatted string
        """
        #logger.debug("Formatting metadata key '%s' with spec '%s'", key, format_spec)
        val = self.value
        if val is None:
            if spec:
                return f"{{{key}:{spec}}}"
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
            output = format(val, spec)
        except (ValueError, TypeError):
            output = f"{{{key}:{spec}}}"
            self.errors.append(f"Invalid format spec '{output}' for value '{val}'")
            return output
        # Apply general substitutions
        for old, new in config.metadata['abbreviations'].get('substitutions', {}).items():
            output = output.replace(old, new)
        return output

    class MetaReader:
        """Class to read metadata values."""

        def __init__(self, head, key, idx=None, valid=True):
            self._head = head
            self._key = key
            self._idx = idx
            self._valid = valid

        def __getattr__(self, name):
            if self._idx is not None:
                new_name = f"{self._key}[{self._idx}].{name}"
                return Formatter.MetaReader(self._head, new_name, valid=False)
            return Formatter.MetaReader(self._head, f"{self._key}.{name}", valid=self._valid)

        def __getitem__(self, name):
            if self._idx is not None:
                new_name = f"{self._key}[{self._idx}][{name}]"
                return Formatter.MetaReader(self._head, new_name, valid=False)
            return Formatter.MetaReader(self._head, self._key, name, valid=self._valid)
        
        def __format__(self, spec):
            key = self._key if self._idx is None else f"{self._key}[{self._idx}]"
            if not self._valid:
                self._head.errors.append(f"Metadata key '{key}' failed parsing")
                return self._head.format_key(key, None, spec)
            val = self._head.get(self._key, self._idx)
            return self._head.format_key(key, val, spec)

    class EnvReader:
        """Class to read environment variable values."""
    
        def __init__(self, head, key):
            self._head = head
            self._key = key
        
        def __format__(self, format_spec):
            val = os.getenv(self._key[1:], None)
            if val is None:
                self._head.errors.append(f"Environment var '{self._key[1:]}' not found")
            return self._head.format_key(self._key, val, format_spec)
    
    class CfgReader:
        """Class to read configuration values."""
    
        def __init__(self, head, key, val):
            self._head = head
            self._key = key
            self._val = val

        def __getattr__(self, name):
            new_key = f"{self._key}.{name}"
            new_val = None
            if self._val is not None:
                try:
                    new_val = getattr(self._val, name)
                except AttributeError:
                    self._head.errors.append(f"Config key '{new_key}' not found")
            return Formatter.CfgReader(self._head, new_key, new_val)

        def __getitem__(self, name):
            new_key = f"{self._key}[{name}]"
            new_val = None
            if self._val is not None:
                try:
                    new_val = self._val[name]
                except (KeyError, IndexError, TypeError):
                    self._head.errors.append(f"Config key '{new_key}' not found")
            return Formatter.CfgReader(self._head, new_key, new_val)
        
        def __format__(self, spec):
            val = self._val
            if self._val is not None and hasattr(self._val, '_value'):
                val = self._val._value
            if isinstance(val, str) and '{' in val:
                self._head.errors.append(f"Config key '{self._key}' contains unexpanded template")
                val = None
            return self._head.format_key(self._key, self._val, spec)

    def __getitem__(self, name):
        if name in KEY_BLACKLIST:
            new_name = KEY_BLACKLIST[name]
            if new_name:
                logger.warning("Name substitution with '%s' is not allowed, use '%s' instead", name, new_name)
                name = new_name
            else:
                self.errors.append(f"Name substitution with '{name}' is not allowed")
                return Formatter.CfgReader(self, name, None)
        if name in CONFIG_KEYS:
            obj = getattr(config, CONFIG_KEYS[name], None)
            if obj is None:
                self.errors.append(f"Config key '{name}' not found")
            return Formatter.CfgReader(self, name, obj)
        if name.startswith('$'):
            return Formatter.EnvReader(self, name)
        return Formatter.MetaReader(self, name)

    def format(self, template: config.ConfigString):
        """
        Format a string using the metadata dictionary.

        :param template: config string template
        """
        # Validate input
        if not isinstance(template, config.ConfigKey):
            logger.critical(f"The name formatter expects a config key, got '{type(template)}'")
            sys.exit(1)
        if not isinstance(template, config.ConfigString):
            logger.critical(f"Config key '{template._name}' is not a string and cannot be formatted")
            sys.exit(1)
        config.string_keys.discard(template._name) # Remove from unformatted keys
        # Skip keys witout any {...} fields
        if '{' not in template._value or '}' not in template._value:
            return
        # Perform formatting
        self.errors = []
        self.metadata_err = False
        result = template.format_map(self)
        if self.errors:
            io_utils.log_list(
                f"Config key '{template._name}' could not be formatted:\n  (got '{result}')",
                self.errors, logging.CRITICAL)
            sys.exit(1)
        # Expand paths if needed
        if template._type == 'path':
            result = os.path.expanduser(os.path.expandvars(result))
        template._value = result
        logger.debug("Config key '%s' formatted to '%s'", template._name, result)
    
    def format_all(self):
        """
        Format all string configuration keys using the metadata dictionary.
        """
        while config.string_keys:
            key_name = config.string_keys.pop()
            key = getattr(config, key_name, None)
            if key is None:
                continue
            self.format(key)

    def eval(self, condition: str) -> bool:
        """
        Evaluate a condition using the metadata dictionary.

        :param condition: condition string to evaluate
        :return: evaluated value
        """
        self.errors = []
        self.metadata_err = False
        expr = condition.format_map(self)
        if self.errors:
            io_utils.log_list(
                f"Error evaluating condition expression '{condition}':",
                self.errors, logging.CRITICAL)
            sys.exit(1)
        try:
            val = eval(expr) #pylint: disable=eval-used
        except Exception as exc:
            logger.critical("Error evaluating condition expression '%s':\n  %s", expr, exc)
            sys.exit(1)
        return val