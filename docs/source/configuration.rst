Configuration
=============

In most cases the user is expected to provide one or more config files with the option '-c my_config.yaml' on the command line.  The script will search for the specified files in the merge-utils config directory, or the full file path may be provided.  If multiple user configs are provided the settings will be applied in the order they are given, this may be useful for large production campagins with a master config file plus minor adjustments for individual datasets.  Command line options are applied last and will override the relevant settings from any config files.

Config files may be written in yaml, json, or toml, but have some special behavior to handle multiple config files.  

Default configurations are set in the following files. 

.. toctree::

    defaults
    metadata
    logging
    valid_values
