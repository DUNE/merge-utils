"""Utilites for reading and writing files and other I/O operations"""

import os
import sys
import logging
import logging.config
import json
import pathlib
import math
from collections.abc import Iterable

# tomllib was added to the standard library in Python 3.10, need tomli for DUNE
try:
    import tomllib # type: ignore
except ImportError:
    import tomli as tomllib

import yaml

logger = logging.getLogger(__name__)

def pkg_dir() -> str:
    """Get the base directory of the package"""
    directory = os.environ.get('MERGE_UTILS_DIR')
    if directory:
        return directory
    return os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

def src_dir() -> str:
    """Get the source directory of the package"""
    return os.path.join(pkg_dir(), 'src', 'merge_utils')

def get_inputs(filelists: list[str] = None) -> list[str]:
    """
    Get a list of inputs from the the file lists and standard input

    :param filelists: full paths to files containing lists of entries
    :return: combined list of entries
    """
    inputs = []

    if filelists is None:
        filelists = []
    for filelist in filelists:
        with open(filelist, encoding="utf-8") as f:
            entries = f.readlines()
        log_nonzero("Found {n} input{s} in file %s" % filelist, len(entries))
        inputs.extend([x.strip() for x in entries])

    if not sys.stdin.isatty():
        entries = sys.stdin.readlines()
        log_nonzero("Found {n} input{s} from standard input", len(entries))
        inputs.extend([x.strip() for x in entries])

    return inputs

def expand_path(path: str, base_dir: str = None) -> str:
    """
    Expand environment variables and user home in a path.
    If the path is relative and a base directory is provided,
    make the path absolute using the base directory.

    :param path: Path to expand
    :param base_dir: Base directory for relative paths
    :return: Expanded path
    """
    path = os.path.expanduser(os.path.expandvars(path))
    if not os.path.isabs(path) and base_dir is not None:
        path = os.path.join(os.path.expanduser(os.path.expandvars(base_dir)), path)
    return os.path.abspath(path)

def find_file(name: str, dirs: list[str] = None, recursive: bool = False) -> str:
    """
    Locate a file by name in a list of directories

    :param name: File name or path to locate
    :param dirs: List of directories to search
    :param recursive: Check sub-directories recursively
    :return: Full path to the located file
    :raises FileNotFoundError: If the file does not exist
    """
    logger.debug("Searching for file %s", name)
    path = os.path.expanduser(os.path.expandvars(name))

    # First, check if the path exists as given
    if os.path.exists(path):
        return os.path.abspath(path)

    # If the path is absolute, check if it exists
    if os.path.isabs(path):
        raise FileNotFoundError(f"Failed to read file {path}")

    # Search the provided directories
    if dirs is None:
        dirs = []
    for directory in dirs:
        test_path = os.path.expanduser(os.path.expandvars(os.path.join(directory, name)))
        if not os.path.isabs(test_path):
            test_path = os.path.join(pkg_dir(), test_path)
        if os.path.exists(test_path):
            return os.path.abspath(test_path)
        if recursive:
            dirs.extend([entry.path for entry in os.scandir(directory) if entry.is_dir()])

    # For FCL files, also check the FHICL_FILE_PATH environment variable
    if name.endswith(".fcl"):
        fcl_dirs = os.getenv("FHICL_FILE_PATH")
        if fcl_dirs is None:
            logger.warning("FHICL_FILE_PATH environment variable is not set")
        else:
            for directory in fcl_dirs.split(':'):
                test_path = os.path.expanduser(os.path.expandvars(os.path.join(directory, name)))
                if os.path.exists(test_path):
                    return os.path.abspath(test_path)

    # If we reach this point, the file was not found
    raise FileNotFoundError(f"Failed to read file {name}")

def find_cfg(name: str) -> str:
    """
    Find the full path to a configuration file

    :param name: Name of the configuration file
    :return: Full path to the configuration file
    :raises FileNotFoundError: If the file does not exist
    """
    return find_file(name, [os.path.join(pkg_dir(), "config")], recursive=True)

def find_runner(name: str) -> str:
    """
    Find the full path to a runner script

    :param name: Name of the runner script
    :return: Full path to the runner script
    :raises FileNotFoundError: If the file does not exist
    """
    return find_file(name, [os.path.join(pkg_dir(), "src", "runners")])

def read_config_file(name: str = None) -> dict:
    """
    Read a configuration file in JSON, TOML, or YAML format

    :param name: Name of the configuration file
    :return: Dictionary containing the configuration settings
    :raises FileNotFoundError: If the file does not exist
    :raises ValueError: If the file type is not supported
    """
    if name is None:
        return None
    path = find_cfg(name)

    suffix = pathlib.Path(path).suffix
    if suffix in [".json"]:
        logger.debug("Reading JSON file %s", path)
        with open(path, encoding="utf-8") as f:
            cfg = json.load(f)
    elif suffix in [".toml"]:
        logger.debug("Reading TOML file %s", path)
        with open(path, mode="rb") as f:
            cfg = tomllib.load(f)
    elif suffix in [".yaml", ".yml"]:
        logger.debug("Reading YAML file %s", path)
        with open(path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    else:
        logger.error("Unknown file type: %s", suffix)
        raise ValueError(f"Unknown file type: {suffix}")
    return cfg

def setup_log(name: str, log_file: str = None, verbosity: int = 0) -> None:
    """Configure logging"""
    logger_config = read_config_file("logging.json")
    if log_file:
        logger_config['handlers']['file']['filename'] = log_file
    else:
        log_file = logger_config['handlers']['file']['filename']
    if not os.path.isabs(log_file):
        log_file = os.path.join(pkg_dir(), "logs", log_file)
        logger_config['handlers']['file']['filename'] = log_file

    # If we're appending to an existing log file, add a newline before the new log
    if os.path.exists(log_file):
        with open(logger_config['handlers']['file']['filename'], 'a', encoding="utf-8") as logfile:
            logfile.write("\n")

    logging.config.dictConfig(logger_config)
    logger.info("Starting script %s", os.path.basename(name))
    set_log_level(verbosity)

def set_log_level(level: int) -> None:
    """Override the logging level for the console"""
    if level == 0:
        level = "ERROR"
    elif level == 1:
        level = "WARNING"
    elif level == 2:
        level = "INFO"
    elif level >= 3:
        level = "DEBUG"

    for handler in logging.getLogger().handlers:
        if handler.get_name() == "console":
            handler.setLevel(level)
            handler.addFilter(lambda record:
                              not hasattr(record, 'block') or record.block != "console")

def log_print(msg: str, level=logging.INFO) -> None:
    """Print a message and save it to the log file"""
    logger.log(level, msg, stacklevel=2, extra={'block': 'console'})
    print(msg)

def log_nonzero(msg: str, value: int, level=logging.DEBUG) -> int:
    """Log a message if the value is non-zero"""
    if value == 0:
        return 0
    if value == 1:
        msg = msg.format(n=1, s="", es="")
    else:
        msg = msg.format(n=value, s="s", es="es")

    logger.log(level, msg, stacklevel=2)
    return value

def log_list(msg: str, items: Iterable, level=logging.WARNING) -> int:
    """Log a message for a list of items"""
    total = len(items)
    if total == 0:
        return 0
    if total == 1:
        msg = [msg.format(n=1, s="", es="")]
    else:
        msg = [msg.format(n=total, s="s", es="es")]

    msg += [f"\n  {item}" for item in sorted(items)]
    logger.log(level, "".join(msg), stacklevel=2)
    return total

def log_dict(msg: str, items: dict, level=logging.WARNING) -> int:
    """Log a message for a dictionary of items with counts"""
    total = sum(items.values())
    if total == 0:
        return 0
    if total == 1:
        msg = [msg.format(n=1, s="", es="")]
    else:
        msg = [msg.format(n=total, s="s", es="es")]

    mult = max(items.values())
    if mult == 1:
        msg += [f"\n  {item}" for item in sorted(items)]
    else:
        pad = int(math.log10(mult)+1)
        msg += [f"\n  ({count:{pad}}) {item}" for item, count in sorted(items.items())]
    logger.log(level, "".join(msg), stacklevel=2)
    return total
