# Changelog

All notable changes to this project will be documented in this file.

## [0.7.3] - 2025-09-19

### Changed

- Redefined UUID as {tag}_s{skip:06d}_l{limit:06d}_{timestamp}

## [0.7.2] - 2025-09-17

### Fixed

- Python string formatting should now work correctly for output file names
- Name formatting failures now print an error instead of returning a garbled name
- Logging lists now print in the original order instead of getting sorted

### Changed

- Individual output files now use {NAME} instead of {name} to refer to output.name
- Expanded list of general substitutions for output file names

## [0.7.1] - 2025-09-15

### Fixed

- Re-added "-n 1000000" to lar commands to ensure all events actually get processed
- Removed '.fcl' and other common file extensions when formatting output file names

### Changed

- Added per-RSE distance offets to config

## [0.7.0] - 2025-09-13

### Changed

- Jobs producing multiple output files are now supported
