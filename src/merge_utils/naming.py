"""Utilities for expanding name templates using metadata."""

import os
import sys
import logging

from merge_utils import config, io_utils

logger = logging.getLogger(__name__)

CONFIG_KEYS = {
    "CFG": "",
    "TIMESTAMP": "job.timestamp",
    "NAME": "output.name",
    "DUNESW_VERSION": "method.environment.dunesw_version",
    "DUNESW_QUALIFIER": "method.environment.dunesw_qualifier",
}

FUNC_KEYS = {
    "UUID": config.uuid,
    "PKG": io_utils.pkg_dir
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
        self.defer_uuid = False

    def reset(self):
        """Reset the formatter state."""
        self.errors = []
        self.metadata_err = False
        self.defer_uuid = False

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
                    val = val[idx]
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
        if val is None:
            if spec:
                return f"{{{key}:{spec}}}"
            return f"{{{key}}}"
        if isinstance(val, str):
            # Apply specific abbreviations
            abbr = str(config.naming.abbreviations.get(key, {}).get(val, ''))
            if abbr:
                logger.debug("Abbreviating key '%s' value '%s' to '%s'", key, val, abbr)
                val = abbr
            # Remove known extensions
            for ext in config.naming.extensions:
                if val.endswith(f".{ext}"):
                    logger.debug("Stripping extension '.%s' from '%s'", ext, val)
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
        for old, new in config.naming.substitutions.items():
            output = output.replace(str(old), str(new))
        return output

    class MetaFormatter:
        """Class to format metadata keys."""

        def __init__(self, head, key, idx=None, valid=True):
            self._head = head
            self._key = key
            self._idx = idx
            self._valid = valid

        def __getattr__(self, name):
            if self._idx is not None:
                new_name = f"{self._key}[{self._idx}].{name}"
                return Formatter.MetaFormatter(self._head, new_name, valid=False)
            return Formatter.MetaFormatter(self._head, f"{self._key}.{name}", valid=self._valid)

        def __getitem__(self, name):
            if self._idx is not None:
                new_name = f"{self._key}[{self._idx}][{name}]"
                return Formatter.MetaFormatter(self._head, new_name, valid=False)
            return Formatter.MetaFormatter(self._head, self._key, name, valid=self._valid)

        def __format__(self, spec):
            key = self._key if self._idx is None else f"{self._key}[{self._idx}]"
            if not self._valid:
                self._head.errors.append(f"Metadata key '{key}' failed parsing")
                return self._head.format_key(key, None, spec)
            val = self._head.get(self._key, self._idx)
            out = self._head.format_key(key, val, spec)
            logger.debug("Key '%s' with value '%s' formatted to '%s'", key, val, out)
            return out

    class CfgFormatter:
        """Class to format configuration keys."""

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
            return Formatter.CfgFormatter(self._head, new_key, new_val)

        def __getitem__(self, name):
            new_key = f"{self._key}[{name}]"
            new_val = None
            if self._val is not None:
                try:
                    new_val = self._val[name]
                except (KeyError, IndexError, TypeError):
                    self._head.errors.append(f"Config key '{new_key}' not found")
            return Formatter.CfgFormatter(self._head, new_key, new_val)

        def __format__(self, spec):
            if isinstance(self._val, config.ConfigString):
                logger.debug("Recursively formatting config key '%s'", self._key)
                Formatter(self._head.metadata).format(self._val, self._head.defer_uuid)
            val = self._val
            if val is not None:
                val = val._value # pylint: disable=protected-access
                if val is None:
                    self._head.errors.append(f"Config key '{self._key}' not set")
            return self._head.format_key(self._key, val, spec)

    class ValFormatter:
        """Class to format generic values."""

        def __init__(self, head, key, value, no_format=False):
            self._head = head
            self._key = key
            self._val = value
            self._no_format = no_format

        def __format__(self, format_spec):
            if self._no_format and format_spec:
                if format_spec:
                    self._head.errors.append(f"Formatting not allowed for key '{self._key}'")
                    return self._head.format_key(self._key, None, format_spec)
                return str(self._val)
            return self._head.format_key(self._key, self._val, format_spec)

    def __getitem__(self, name):
        if name in KEY_BLACKLIST:
            new_name = KEY_BLACKLIST[name]
            if new_name:
                logger.warning("Name substitution '%s' is not allowed, use '%s'", name, new_name)
                name = new_name
            else:
                self.errors.append(f"Name substitution with '{name}' is not allowed")
                return Formatter.CfgFormatter(self, name, None)
        if self.defer_uuid and name == "UUID":
            return Formatter.ValFormatter(self, name, "{UUID}", no_format=True)
        if name in FUNC_KEYS:
            return Formatter.ValFormatter(self, name, FUNC_KEYS[name]())
        if name in CONFIG_KEYS:
            obj = config.get_key(CONFIG_KEYS[name])
            return Formatter.CfgFormatter(self, name, obj)
        if name.startswith('$'):
            val = os.getenv(name[1:], None)
            if val is None:
                self.errors.append(f"Environment var '{name[1:]}' not found")
            return Formatter.ValFormatter(self, name, val)
        return Formatter.MetaFormatter(self, name)

    def format(self, template: config.ConfigString, defer_uuid: bool = False):
        """
        Format a config string using the metadata dictionary.

        :param template: config string template
        """
        name = template._name # pylint: disable=protected-access
        # Validate input
        if not isinstance(template, config.ConfigKey):
            logger.critical("The name formatter expects a config key, got '%s'", type(template))
            sys.exit(1)
        if not isinstance(template, config.ConfigString):
            logger.critical("Config key '%s' is not a string and cannot be formatted", name)
            sys.exit(1)
        config.string_keys.discard(name) # Remove from unformatted keys
        # Skip keys witout any {...} fields
        val = template.value
        if val is None or '{' not in val or '}' not in val:
            return
        logger.debug("Formatting config key '%s'", name)
        # Perform formatting
        self.reset()
        self.defer_uuid = defer_uuid
        result = str(template).format_map(self)
        if self.errors:
            io_utils.log_list(
                f"Config key '{name}' could not be formatted:\n  (got '{result}')",
                self.errors, logging.CRITICAL)
            sys.exit(1)
        # Expand paths if needed
        if template._type == 'path': # pylint: disable=protected-access
            result = io_utils.expand_path(result)
        template._set(result) # pylint: disable=protected-access
        logger.debug("Config key '%s' formatted to '%s'", name, result)

    def eval(self, condition: str) -> bool:
        """
        Evaluate a condition using the metadata dictionary.

        :param condition: condition string to evaluate
        :return: evaluated value
        """
        self.reset()
        logger.debug("Evaluating condition '%s'", condition)
        expr = str(condition).format_map(self)
        if self.errors:
            io_utils.log_list(
                f"Error evaluating condition expression '{condition}':",
                self.errors, logging.ERROR)
            return False
        try:
            val = eval(expr) #pylint: disable=eval-used
        except Exception as exc:
            logger.error("Error evaluating condition expression '%s':\n  %s", expr, exc)
            return False
        logger.debug("Condition expression '%s' evaluated to '%s'", expr, val)
        return val
