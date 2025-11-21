# Changelog

All notable changes to this project will be documented in this file.

## [0.7.3] - 2025-09-19

### Changed

- Redefined UUID as {tag}_s{skip:06d}_l{limit:06d}_{timestamp}

## [0.7.2] - 2025-09-17

### Changed

- Individual output files now use {NAME} instead of {name} to refer to output.name
- Expanded list of general substitutions for output file names

### Fixed

- Python string formatting should now work correctly for output file names
- Name formatting failures now print an error instead of returning a garbled name
- Logging lists now print in the original order instead of getting sorted

## [0.7.1] - 2025-09-15

### Added

- Per-RSE distance offets in config

### Fixed

- Re-added "-n 1000000" to lar commands to ensure all events actually get processed
- Removed '.fcl' and other common file extensions when formatting output file names

## [0.7.0] - 2025-09-13

### Added

- Support for multiple output files from a single job
- Dataset input mode for merging entire datasets
- Options for skip, limit, tag, and comment
- Defined UUID as "tag_timestamp", can use {UUID} to add to names
- Name substitution can now use environment variables with the syntax {$VAR}
- Separate output namespace and lifetime settings for temporary files

### Changed

- Merging methods now specify a list of outputs instead of a single extension
- Output files have their own name templates, can refer to standard output.name with {name}
- Metadata overrides are now defined for each output file, rather than per method
- Optional "rename" paramter for outputs that need to be renamed after running the script
- Optional "method" parameter for outputs requiring a different merge method in case of a second pass

### Fixed

- Commented out oidc_scope and wlcg.groups entries in rucio config
- Required "dune.output_status" to be "confirmed" for merging input files
- The same timestamp is now used for the whole merge job, instead of generating new ones
- Default for validation.skip.unreachable reverted to False

## [0.6.0] - 2025-09-07

### Added

- Support for arbitrary commands or scripts in addition to standard merging
- 

### Changed

- Merge methods now use individual runner scripts in the src/runners directory

### Fixed

- Improved ability to automatically find files (configs, runners, etc...)
