"""Class variants for managing local file retrieval and merging operations."""

import logging
import os
import sys

from merge_utils import io_utils, config
from merge_utils.retriever import MetaRetriever, PathFinder, DidRetriever

logger = logging.getLogger(__name__)

def get_xrootd_path(path: str) -> str:
    """
    Convert a local path to an XRootD path if applicable.
    
    :param path: Local file path
    :return: XRootD file path if conversion is applicable, otherwise original path
    """

    #TODO: generalize this for other sites?
    if path.startswith('/pnfs/'):
        # convert pnfs paths to xroot paths
        fnal_prefix = "root://fndca1.fnal.gov:1094/pnfs/fnal.gov/usr"
        path = path.replace('/pnfs', fnal_prefix)
        return {config.local.site: (path, 0)}
    return {None: (path, 0)}

def search(file: str, dirs: list) -> str:
    """
    Search for a file in a list of directories.
    
    :param file: Name of the file to search for
    :param dirs: List of directories to search in
    :return: Full path to the file if found, otherwise None
    """
    for directory in dirs:
        path = os.path.join(directory, file)
        if os.path.exists(path):
            return path
    return None

def get_local_files(files: list, dirs: list) -> PathFinder:
    """
    Search local filesystem for pairs of data and metadata files.
    If both data and metadata files are found, return a LocalPathFinder using a LocalMetaRetriever.
    If only metadata files are found, return a RucioPathFinder using a LocalMetaRetriever.
    If only data files are found, return a LocalPathFinder using a MetaCatRetriever.
    
    :param files: List of input data or metadata file paths
    :param dirs: List of directories to search for corresponding data or metadata files
    :return: Appropriate PathFinder object
    """
    logger.info("Processing local input files")
    dirs = list(dirs)
    names = []
    data_paths = []
    meta_paths = []
    found = {}
    dupes = set()

    # Sort input files into data and metadata
    for path in files:
        # Determine if file is data or metadata and extract base name
        name = os.path.basename(str(path))
        paths = data_paths
        if os.path.splitext(name)[1] == '.json':
            name = os.path.splitext(name)[0]
            paths = meta_paths
        # Check if we've already seen this base name
        idx = found.get(name, None)
        if idx is None:
            # New file
            idx = len(names)
            names.append(name)
            data_paths.append(None)
            meta_paths.append(None)
            found[name] = idx
        # Assign file path to appropriate slot
        if paths[idx] is not None:
            dupes.add(idx)
        else:
            paths[idx] = str(path)
    if dupes:
        if config.validation.error_handling.duplicate == 'quit':
            io_utils.log_list("Found {n} duplicate input file{s}:", list(dupes), logging.CRITICAL)
            sys.exit(1)
        io_utils.log_list("Ignoring {n} duplicate input file{s}:", list(dupes), logging.ERROR)

    # Search for missing data and/or metadata files
    for idx, name in enumerate(names):
        if not data_paths[idx]:
            data_paths[idx] = search(name, [os.path.dirname(meta_paths[idx])] + dirs)
        elif not meta_paths[idx]:
            meta_paths[idx] = search(name + '.json', [os.path.dirname(data_paths[idx])] + dirs)

    if any(meta_paths):
        logger.info("Reading metadata from local files")
        meta = LocalMetaRetriever(names, meta_paths, dupes = dupes)
    else:
        logger.info("No metadata files found, requesting metadata from MetaCat")
        ns = config.input.namespace
        meta = DidRetriever(dids = [f"{ns}:{name}" for name in names], dupes = dupes)

    if any(data_paths):
        logger.info("Reading data from local files")
        data = LocalPathFinder(meta, files = {n: p for n, p in zip(names, data_paths) if p})
    else:
        logger.info("No data files found, requesting physical file paths from Rucio")
        from merge_utils.rucio_utils import RucioFinder #pylint: disable=import-outside-toplevel
        data = RucioFinder(meta)

    return data

class LocalMetaRetriever(MetaRetriever):
    """MetaRetriever for local files"""

    def __init__(self, names: list, paths: list, dupes: set = None):
        """
        Initialize the LocalMetaRetriever with a list of json files.

        :param names: list of input file names
        :param paths: list of metadata file paths
        """
        super().__init__("local_meta")

        self.names = names
        self.paths = paths
        self.dupes = dupes or set()

    async def get_metadata(self, skip: int, limit: int) -> list:
        """
        Asynchronously retrieve metadata for a specific batch of files.

        :param skip: number of files to skip
        :param limit: maximum number of files to retrieve
        :return: list of file metadata dictionaries
        """
        files = []
        end = min(skip + limit, len(self.names))
        namespace = str(config.input.namespace)
        missing = {}
        for idx in range(skip, end):
            name = self.names[idx]
            if idx in self.dupes:
                logger.debug("Skipping duplicate file: %s", name)
                files.append({'namespace': namespace, 'name': name, 'duplicate': True})
                continue
            path = self.paths[idx]
            metadata = None
            if path and os.path.exists(path):
                try:
                    metadata = io_utils.read_config_file(path)
                except Exception as exc:
                    logger.error(f"Error reading metadata file '{path}': {exc}")
            # If file is missing or unreadable, create a placeholder
            if metadata is None:
                metadata = {'namespace': namespace, 'name': name, 'missing': True}
                missing[name] = len(files)
            files.append(metadata)
        # Make sure files exist in MetaCat, if needed for parent listing
        if not config.output.grandparents:
            await self.check_existence(files)
        # If we were missing any local files, try to find them in MetaCat
        if missing:
            io_utils.log_list("Checking MetaCat for {n} missing metadata file{s}:",
                              missing, logging.INFO)
            res = await self.client.files([{'did': f"{namespace}:{name}"} for name in missing],
                                          metadata = True, provenance = config.output.grandparents)
            for file in res:
                files[missing[file['name']]] = file
        return files

class LocalPathFinder(PathFinder):
    """PathFinder for local files"""

    def __init__(self, source: MetaRetriever, files: dict = None, dirs: list = None):
        """
        Initialize the LocalMetaRetriever with a list of json files.

        :param source: MetaRetriever object to use as the source of file metadata
        :param files: dictionary of metadata file names and paths
        :param dirs: list of directories to search for data files
        """
        super().__init__('local_data', source)

        self.paths = files or {}
        self.dirs = dirs or []

    async def get_paths(self, files: dict) -> list:
        """
        Asynchronously retrieve paths for a specific batch of files.

        :param files: dictionary of files to retrieve paths for
        :return: list of file path dictionaries
        """
        paths = []
        for file in files.values():
            name = file.name
            path = self.paths.get(name, None)
            if path is not None:
                paths.append({name: path})
        return paths

    async def process(self, files: dict, paths: list) -> None:
        """
        Process a batch of files to assign paths.
        
        :param files: dictionary of files to process
        """
        path_dict = {}
        for path in paths:
            path_dict.update(path)

        unreachable = []
        for file in files.values():
            name = file.name
            path = path_dict.get(name, None)
            if path is None:
                # If no path is found, try searching in the provided directories
                path = search(file.name, self.dirs)
                if path is None:
                    # If still not found, mark as unreachable
                    unreachable.append(file.did)
                    continue
            file.paths = get_xrootd_path(path)

        crit = config.validation.error_handling.unreachable == 'quit'
        lvl = logging.CRITICAL if crit else logging.ERROR
        io_utils.log_list("Failed to locate {n} file path{s}:", unreachable, lvl)
        self.files.set_unreachable(unreachable)
