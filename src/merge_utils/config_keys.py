"""Module for configuration key classes."""

from __future__ import annotations
import logging
import operator
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)

type_defs = {}
key_defs = {}
string_keys = set()

class ConfigKey(ABC):
    """Base class for configuration keys"""
    _type: str = 'none' # Type of the config key
    _conversions: set = set() # Allowed type conversions

    def __init__(self, name: str):
        self._name = name
        self._value = None

    @property
    def _subtype(self) -> str:
        """Return the subtype of the config key, if any"""
        return ''

    def _lock(self) -> None:
        """Prevent further changes to the config schema after loading defaults"""

    def _clear(self) -> None:
        """Clear the value of the key"""
        self._value = None

    @abstractmethod
    def _do_update(self, value) -> list:
        """Actually update the config value, and return any errors"""
        self._value = value
        return []

    def _update(self, value) -> list:
        """Recursively update the config tree and return any errors"""
        val_type, _, val = parse_type(value)
        if val_type is None:
            self._clear()
            return []
        if val_type != self._type and val_type not in self._conversions:
            return [self._err(f"must be a {self._type} (got '{val_type}')")]
        if val is None:
            return []
        return self._do_update(val)

    def _set(self, value) -> None:
        """Clear and set the value of the key (used for assignment)"""
        if value is self:
            return
        self._clear()
        errors = self._update(value)
        if errors:
            if len(errors) == 1:
                raise TypeError(errors[0])
            err_str = '\n  '.join(errors)
            raise TypeError(f"Failed to set config key '{self._name}':\n  {err_str}")

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

    def __format__(self, format_spec):
        return format(self._value, format_spec)

    def __bool__(self):
        return bool(self._value)

    def __eq__(self, value):
        if isinstance(value, ConfigKey):
            return self._value == value._value and self._type == value._type
        return self._value == value

class ConfigValue(ConfigKey):
    """Base class for individual configuration values"""

    def __init__(self, name: str):
        super().__init__(name)
        self._default = None

    def _lock(self) -> None:
        """Prevent further changes to the config schema after loading defaults"""
        self._default = self._value

    def _clear(self) -> None:
        """Clear the value of the key"""
        self._value = self._default

    @property
    def value(self):
        """Get the raw value of the key"""
        return self._value

    def val(self, default=None):
        """Get the value of the key, or a default if not set"""
        return self._value if self._value is not None else default

    def set(self, value) -> None:
        """Set the value of the key"""
        self._set(value)

class ConfigString(ConfigValue):
    """Class to manage a configuration string"""
    _type: str = 'str'
    _conversions: set = set()

    def __init__(self, name: str):
        super().__init__(name)
        string_keys.add(name)

    def _do_update(self, value) -> list:
        self._value = str(value)
        return []

    def __del__(self):
        try:
            string_keys.discard(self._name)
        except Exception:
            pass

    def __contains__(self, item):
        return item in self._value

    def format(self, *args, **kwargs):
        """Format the string value with the given arguments"""
        if self._value is None:
            return None
        return self._value.format(*args, **kwargs)

class ConfigPath(ConfigString):
    """Class to manage a configuration file path"""
    _type: str = 'path'
    _conversions: set = {'str'}

class ConfigCondition(ConfigKey):
    """Class to manage a configuration condition expression"""
    _type: str = 'cond'
    _conversions: set = {'str', 'bool'}

    def __init__(self, name: str):
        super().__init__(name)
        self._locked = False

    def _lock(self) -> None:
        if self._value is None:
            self._value = "False"
        self._locked = True

    def _clear(self) -> None:
        self._value = "False"

    def _do_update(self, value) -> list:
        if self._locked:
            logger.warning(self._err("modified by user configuration file"))
        value, errors = check_condition(value)
        if errors:
            return [self._err(err) for err in errors]
        self._value = value
        return []

class ConfigBool(ConfigValue):
    """Class to manage a configuration boolean option"""
    _type: str = 'bool'
    _conversions: set = {'str'}
    FALSE_STRINGS = {'false', '0', 'no'}
    TRUE_STRINGS = {'true', '1', 'yes'}

    def _do_update(self, value) -> list:
        if isinstance(value, str):
            val_lower = str(value).lower()
            if val_lower in self.TRUE_STRINGS:
                self._value = True
                return []
            if val_lower in self.FALSE_STRINGS:
                self._value = False
                return []
        else:
            self._value = bool(value)
            return []
        return [self._err(f"must be a boolean value (got '{value}')")]

    def __bool__(self):
        return bool(self._value)

class ConfigNum(ConfigValue):
    """Class to manage a configuration number"""

    def _operate(self, other, op):
        if self._value is None:
            return None
        if isinstance(other, ConfigNum):
            other = other.value
            if other is None:
                return None
        return op(self._value, other)

    def __gt__(self, other):
        return self._operate(other, operator.gt)

    def __ge__(self, other):
        return self._operate(other, operator.ge)

    def __lt__(self, other):
        return self._operate(other, operator.lt)

    def __le__(self, other):
        return self._operate(other, operator.le)

    def __add__(self, other):
        return self._operate(other, operator.add)

    def __radd__(self, other):
        return self.__add__(other)

    def __iadd__(self, other):
        self._set(self.__add__(other))
        return self

    def __sub__(self, other):
        return self._operate(other, operator.sub)

    def __rsub__(self, other):
        return self._operate(other, lambda x, y: operator.sub(y, x))

    def __isub__(self, other):
        self._set(self.__sub__(other))
        return self

    def __mul__(self, other):
        return self._operate(other, operator.mul)

    def __rmul__(self, other):
        return self.__mul__(other)

    def __imul__(self, other):
        self._set(self.__mul__(other))
        return self

    def __truediv__(self, other):
        return self._operate(other, operator.truediv)

    def __rtruediv__(self, other):
        return self._operate(other, lambda x, y: operator.truediv(y, x))

    def __itruediv__(self, other):
        self._set(self.__truediv__(other))
        return self

class ConfigInt(ConfigNum):
    """Class to manage a configuration integer"""
    _type: str = 'int'
    _conversions: set = {'str'}

    def _do_update(self, value) -> list:
        try:
            value = int(value)
        except ValueError as err:
            return [self._err(f"failed to convert value to int: {err}")]
        self._value = value
        return []

    def __int__(self):
        return int(self._value)

class ConfigFloat(ConfigNum):
    """Class to manage a configuration float"""
    _type: str = 'float'
    _conversions: set = {'str', 'int'}

    def _do_update(self, value) -> list:
        try:
            value = float(value)
        except ValueError as err:
            return [self._err(f"failed to convert value to float: {err}")]
        self._value = value
        return []

    def __float__(self):
        return float(self._value)

class ConfigOption(ConfigKey):
    """Class to manage a configuration option with predefined choices"""
    _type: str = 'opt'
    _conversions: set = {'str'}

    def __init__(self, name: str, options: str):
        super().__init__(name)
        opts = [opt.strip() for opt in options.split(',')]
        self._value = opts[0]
        uique_opts = set(opts)
        self._options = []
        dupes = set()
        for opt in opts:
            if opt in uique_opts:
                self._options.append(opt)
                uique_opts.remove(opt)
            else:
                dupes.add(opt)
        if dupes:
            raise ValueError(self._err(f"has duplicate options: {', '.join(dupes)}"))
        if len(self._options) <= 1:
            raise ValueError(self._err("must have more than one option"))

    @property
    def _subtype(self) -> str:
        return ','.join(self._options)

    def _clear(self) -> None:
        self._value = self._options[0]

    def _do_update(self, value) -> list:
        if value not in self._options:
            return [self._err(f"must be one of ({', '.join(self._options)})")]
        self._value = value
        return []

    def __bool__(self):
        return True

    def __contains__(self, option):
        return option in self._options

    def __eq__(self, value):
        if isinstance(value, ConfigKey):
            value = value._value
        if value == self._value:
            return True
        if value in self._options:
            return False
        raise ValueError(self._err(f"must be one of ({', '.join(self._options)})"))

class ConfigSizeSpec(ConfigKey):
    """Class to manage a configuration size prediction specification"""
    _type: str = 'size'
    _conversions: set = {'str', 'int'}

    PARAMS = [
        ['s', 'sum'],
        ['n', 'num', 'number'],
        ['a', 'avg', 'average'],
        ['b', 'kb', 'mb', 'gb', 'tb']
    ]

    def parse_term(self, term: str) -> tuple[float, str]:
        """Parse a term of the form 'number*param' or 'number param'"""
        if '*' in term:
            coeff, param = term.split('*', 1)
        else:
            alpha_idx = -1
            for i, c in enumerate(term):
                if c.isalpha():
                    alpha_idx = i
                    break
            if alpha_idx == 0:
                coeff = 1
                param = term
            elif alpha_idx == -1:
                coeff = term
                param = 'b'
            else:
                coeff = term[:alpha_idx]
                param = term[alpha_idx:]
        try:
            coeff = float(coeff)
        except ValueError as err:
            raise ValueError(self._err(f"failed to parse coefficient of term '{term}'")) from err
        if not param.isalpha():
            raise ValueError(self._err(f"failed to parse parameter of term '{term}'"))
        return coeff, param.lower()

    def parse_spec(self, spec_str: str) -> list:
        """
        Parse size specification consisting of a sum of terms of the form
        'number * param' or 'number param',
        where param is one of PARAMS, or '' for bytes. For example: '2*s + 0.5*n + 10mb'

        :param spec_str: specification string
        :return: tuple of coefficents for (s,n,a,b)
        """
        errors = []
        coeffs: list[float | None] = [None]*len(self.PARAMS)
        spec = spec_str.replace(' ', '').split('+')
        for term in spec:
            try:
                coeff, param = self.parse_term(term)
            except ValueError as err:
                errors.append(str(err))
                continue
            param_idx = -1
            for i, opts in enumerate(self.PARAMS):
                if param in opts:
                    param_idx = i
                    break
            if param_idx == -1:
                errors.append(self._err(f"Unknown parameter '{param}' in term '{term}'"))
                continue
            if param_idx == len(self.PARAMS)-1:
                coeff *= 1024**self.PARAMS[param_idx].index(param)
            if coeffs[param_idx] is not None:
                errors.append(self._err(f"Redundant term '{term}' in size spec"))
            coeffs[param_idx] = coeff
        if errors:
            return errors
        self._value = tuple(c or 0 for c in coeffs)
        return []

    def _do_update(self, value) -> list:
        if isinstance(value, str):
            return self.parse_spec(value)
        if isinstance(value, int):
            self._value = (0, 0, 0, value)
            return []
        if isinstance(value, (list, tuple)) and len(value) == len(self.PARAMS):
            try:
                self._value = tuple(float(v) for v in value)
                return []
            except ValueError as err:
                return [self._err(f"failed to parse numeric value in list: {err}")]
        return [self._err(f"must be a size spec or a list of {len(self.PARAMS)} numbers")]

    @property
    def s(self) -> float:
        """Coefficient for sum of input sizes term"""
        return self._value[0] if self._value else 0

    @property
    def n(self) -> float:
        """Coefficient for number of inputs term"""
        return self._value[1] if self._value else 0

    @property
    def a(self) -> float:
        """Coefficient for average input size term"""
        return self._value[2] if self._value else 0

    @property
    def b(self) -> float:
        """Constant term in bytes"""
        return self._value[3] if self._value else 0

    def __bool__(self):
        return bool(self._value and any(coeff != 0 for coeff in self._value))

    def __call__(self, sizes: list) -> float:
        """Evaluate the size spec for a given list of input sizes"""
        if not self._value:
            return 0
        n = len(sizes)
        s = sum(sizes)
        a = s/n if n > 0 else 0
        return self._value[0]*s + self._value[1]*n + self._value[2]*a + self._value[3]

    def __str__(self):
        out = []
        for coeff, param in zip(self._value, [opts[1] for opts in self.PARAMS[:-1]]):
            if coeff == 0:
                continue
            if coeff == 1:
                out.append(param)
            else:
                out.append(f"{coeff}*{param}")
        if self._value[-1]:
            val = self._value[-1]
            order = 0
            while val >= 1024 and order < len(self.PARAMS[-1]):
                val /= 1024
                order += 1
            out.append(f"{val}{self.PARAMS[-1][order]}")
        return ' + '.join(out) if out else '0'

    def __repr__(self):
        return f"<{self._type}> {str(self)}"

    def __format__(self, format_spec):
        return format(str(self), format_spec)

    def _json(self):
        """Return a JSON-serializable representation of the config key"""
        return str(self)

class ConfigTuple(ConfigValue):
    """Class to manage a configuration tuple"""
    _type: str = 'tuple'
    _conversions: set = {'str', 'int', 'float', 'list'}

    def _lock(self) -> None:
        self._default = self._value.copy()

    def _do_update(self, value) -> list:
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
        for idx, default in enumerate(self._default):
            self._value[idx] = value[idx] if idx < len(value) else default
        return []

class ConfigCollection(ConfigKey):
    """Base class for configuration collections"""

    def __init__(self, name: str, val_type: str):
        super().__init__(name)
        self._val_type = val_type or 'str'

    @property
    def _subtype(self) -> str:
        return self._val_type

    def _lock(self) -> None:
        pass

    def _json(self) -> Any:
        if not self._value:
            return []
        return [val._json() for val in self._value] # pylint: disable=protected-access

    def __contains__(self, item):
        return item in self._value

    def __iter__(self):
        if not self._value:
            return iter([])
        return iter(self._value)

    def __len__(self):
        if not self._value:
            return 0
        return len(self._value)

class ConfigSet(ConfigCollection):
    """Class to manage a configuration set (of strings)"""
    _type: str = 'set'
    _conversions: set = {'list'}

    def __init__(self, name: str, val_type = None):
        super().__init__(name, val_type)
        self._value = set()
        if self._val_type not in BASIC_CLASSES:
            raise ValueError(self._err(f"unsupported value type '{self._val_type}'"))

    def _clear(self) -> None:
        self._value = set()

    def _do_update(self, value) -> list:
        subs = set(item for item in value if isinstance(item, str) and item.startswith('~'))
        adds = set(item for item in value if item not in subs)
        cls = BASIC_CLASSES[self._val_type]
        self._value -= set(cls(item[1:]) for item in subs)
        self._value |= set(cls(item) for item in adds)
        return []

    def _json(self):
        return list(self._value)

    def __getitem__(self, key):
        raise AttributeError("ConfigSet does not support indexing")

    def __setitem__(self, key, value):
        raise AttributeError("ConfigSet does not support indexing")

    def __ior__(self, other):
        if isinstance(other, ConfigSet):
            self._value |= other._value
        elif isinstance(other, (set, list)):
            self._value |= set(other)
        else:
            raise TypeError(self._err("can only merge with another set or list"))
        return self

    def extend(self, other):
        """Extend the set with another set or list"""
        self |= other

class ConfigMap(ConfigCollection):
    """Class to manage a configuration map"""
    _type: str = 'map'
    _conversions: set = {'dict'}

    def __init__(self, name: str, val_type = None):
        self._key_type = 'str'
        if val_type is not None:
            sub_type = ''
            if '(' in val_type:
                val_type, sub_type = val_type.split('(', 1)
                sub_type = '(' + sub_type
            if ',' in val_type:
                self._key_type, val_type = self._val_type.split(',', 1)
            val_type = f"{val_type}{sub_type}"
        if not self._key_type:
            self._key_type = 'str'
        elif self._key_type not in BASIC_CLASSES:
            raise ValueError(self._err(f"unsupported key type '{self._key_type}'"))
        super().__init__(name, val_type)
        self._value = {}
        self._required = set()

    @property
    def _subtype(self) -> str:
        return f"{self._key_type},{self._val_type}"

    def _lock(self) -> None:
        for val in self._value.values():
            val._lock() # pylint: disable=protected-access

    def _clear(self) -> None:
        # Remove all non-required keys
        self._value = {k: v for k, v in self._value.items() if k in self._required}
        # Clear all required keys
        for val in self._value.values():
            val._clear() # pylint: disable=protected-access

    def _do_update(self, value) -> list:
        errors = []
        for key, val in value.items():
            # String keys starting with '~' override existing keys
            if isinstance(key, str) and key.startswith('~'):
                key = BASIC_CLASSES[self._key_type](key[1:])
                if key in self._value:
                    self._value[key]._clear() # pylint: disable=protected-access
            else:
                key = BASIC_CLASSES[self._key_type](key)
            # Values of None remove existing keys if permitted
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
            name = f"{self._name}[{key}]"
            if name in key_defs:
                self._required.add(key)
            new_key, new_errors = make_cfg_key(name, f"<{self._val_type}>")
            errors.extend(new_errors)
            if new_key is None:
                continue
            errors.extend(new_key._update(val)) # pylint: disable=protected-access
            self._value[key] = new_key
        return errors

    def _json(self):
        return {key: val._json() for key, val in self._value.items()} # pylint: disable=protected-access

    def __getitem__(self, key):
        return self._value[key]

    def __setitem__(self, key, value):
        if value is None and key not in self._required:
            if key in self._value:
                del self._value[key]
            return
        self._value[key]._set(value) # pylint: disable=protected-access

    def keys(self):
        """Get the keys in the config map"""
        return self._value.keys()

    def values(self):
        """Get the values in the config map"""
        return self._value.values()

    def items(self):
        """Get the items in the config map"""
        return self._value.items()

    def get(self, key, default=None):
        """Get a value from the config map, returning default if not found"""
        val = self._value.get(key)
        if isinstance(val, ConfigValue):
            val = val.value
        if val is None:
            return default
        return val

    def update(self, other):
        """Update the config map with another dictionary or ConfigMap"""
        errors = self._update(other)
        if errors:
            if len(errors) == 1:
                raise TypeError(errors[0])
            err_str = '\n  '.join(errors)
            raise TypeError(self._err(f"Failed to update config map:\n  {err_str}"))

class ConfigList(ConfigCollection):
    """Class to manage a configuration list"""
    _type: str = 'list'

    def __init__(self, name: str, val_type = None):
        super().__init__(name, val_type)
        self._value = []

    def _lock(self) -> None:
        for val in self._value:
            val._lock() # pylint: disable=protected-access

    def _clear(self) -> None:
        self._value = []

    def _do_update(self, value) -> list:
        errors = []
        for item in value:
            name = f"{self._name}[{len(self._value)}]"
            new_key, new_errors = make_cfg_key(name, f"<{self._val_type}>")
            errors.extend(new_errors)
            if new_key is None:
                continue
            errors.extend(new_key._update(item)) # pylint: disable=protected-access
            self._value.append(new_key)
        return errors

    def __getitem__(self, idx):
        return self._value[idx]

    def __setitem__(self, idx, value):
        self._value[idx]._set(value) # pylint: disable=protected-access

    def append(self, item) -> None:
        """Append a new item to the list"""
        if item is None:
            return
        errs = self._update([item])
        if errs:
            if len(errs) == 1:
                raise TypeError(errs[0])
            err_str = '\n  '.join(errs)
            raise TypeError(self._err(f"Failed to append:\n  {err_str}"))

    def extend(self, items: list) -> None:
        """Extend the list with new items"""
        if items is None:
            return
        errs = self._update(items)
        if errs:
            if len(errs) == 1:
                raise TypeError(errs[0])
            err_str = '\n  '.join(errs)
            raise TypeError(self._err(f"Failed to extend:\n  {err_str}"))

class ConfigDict(ConfigKey):
    """Class to manage a configuration dictionary"""
    _type: str = 'dict'
    _conversions: set = {'dict'}

    def __init__(self, name = None, type_name = None):
        if name is None:
            name = ""
        super().__init__(name)
        self._value = {}
        self._locked = False
        if type_name:
            self._type = type_name
            errors = self._update(type_defs[type_name])
            self._locked = True
            if errors:
                err_str = '\n  '.join(errors)
                raise TypeError(f"Invalid config spec for '{type_name}':\n  {err_str}")

    def _lock(self) -> None:
        self._locked = True
        for val in self._value.values():
            val._lock() # pylint: disable=protected-access

    def _clear(self) -> None:
        for val in self._value.values():
            val._clear() # pylint: disable=protected-access

    def _do_update(self, value) -> list:
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
        return {key: val._json() for key, val in self._value.items()} # pylint: disable=protected-access

    def items(self):
        """Get the items in the config dict"""
        return self._value.items()

    def __getitem__(self, key):
        return self._value[key]

    def __setitem__(self, key, value):
        if key in self._value:
            self._value[key]._set(value)
        elif not self._locked:
            name = f"{self._name}.{key}" if self._name else key
            new_key, errors = make_cfg_key(name, value)
            if errors:
                raise ValueError(errors[0])
            self._value[key] = new_key
        else:
            raise AttributeError(self._err(f"has no member named '{key}'"))

    def __getattr__(self, key):
        return self._value[key]

    def __setattr__(self, key, value):
        if key.startswith('_'):
            super().__setattr__(key, value)
        else:
            self.__setitem__(key, value)

    def get(self, key, default=None):
        """Get a value from the config dict, returning default if not found"""
        val = self._value.get(key)
        if isinstance(val, ConfigValue):
            val = val.value
        if val is None:
            return default
        return val


def parse_type(value) -> tuple:
    """
    Parse a type string into a list of type components.
    
    :param value: Raw value or string of the form '<type(subtype)>value'
    :return: Tuple of (type, subtype, value)
    """
    if value is None:
        return None, None, None
    if isinstance(value, ConfigKey):
        return value._type, value._subtype, value._value # pylint: disable=protected-access
    if not isinstance(value, str):
        return type(value).__name__, None, value
    # Types specified with <type>value
    if value.startswith('<') and '>' in value:
        key_type, value = value[1:].rsplit('>', 1)
        # Remove extra spaces
        key_type = key_type.replace(' ', '')
        value = value.lstrip()
        if value == '':
            value = None
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
    return condition, []

BASIC_CLASSES = {
    'bool': bool,
    'int': int,
    'float': float,
    'str': str
}

CONFIG_CLASSES = {
    'bool': ConfigBool,
    'int': ConfigInt,
    'float': ConfigFloat,
    'str': ConfigString,
    'path': ConfigPath,
    'cond': ConfigCondition,
    'opt': ConfigOption,
    'size_spec': ConfigSizeSpec,
    'tuple': ConfigTuple,
    'set': ConfigSet,
    'map': ConfigMap,
    'list': ConfigList,
    'dict': ConfigDict
}

def make_cfg_key(name: str, value = None) -> tuple:
    """Factory function to create appropriate ConfigKey subclass based on type name."""
    type_name, sub_type, value = parse_type(value)
    # Check for key definition overrides
    if name in key_defs:
        logger.debug("Overriding config key '%s' with spec: %s", name, repr(key_defs[name]))
        spec = key_defs.get(name)
        if spec is not None:
            type_name, sub_type, default = parse_type(spec)
            if value is None and default is not None:
                value = default
    # Create the appropriate config key
    if type_name in type_defs:
        key = ConfigDict(name, type_name)
    elif type_name in CONFIG_CLASSES:
        key_class = CONFIG_CLASSES[type_name]
        key = key_class(name, sub_type) if sub_type else key_class(name)
    else:
        return None, [f"Config key '{name}' has unknown type '{type_name}'"]
    # Set the initial value if provided
    if value:
        errors = key._update(value) # pylint: disable=protected-access
        if errors:
            return None, errors
    return key, []
