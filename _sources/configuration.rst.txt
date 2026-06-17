Configuration
=============

Merge-utils is designed to be highly configurable, with a large number of settings that can be adjusted by the user.  This allows it to be used for a wide variety of merging tasks and workflows, but also means that it can be somewhat complex to set up and use.  This page provides an overview of the configuration system, including how settings are applied and the different types of settings available.

Default configuration files are provided in the config/defaults directory, and should have reasonable settings for basic merging tasks.  The default config files are written in yaml format and include comments, so reading through them will give the most up-to-date and comprehensive overview of the available settings and their meanings.  The default config files may also be used as a reference for user config files, but should not be modified directly.  Instead, users should create a copy of either the default config or the various templates available in the config/examples directory, and then modify the copy as needed for their specific use case.  The default config settings are currently split into two files in an attempt to improve readability:

.. toctree::

    defaults
    metadata

In most cases the user is expected to provide one or more additional config files with the option '-c my_config.yaml' on the command line.  While the default configs are written in yaml, user configs may be written in yaml, json, or toml, and more formats may be added on request.  The script will search for the specified files in the merge-utils config directory, or the full file path may be provided.  If multiple user configs are provided the settings will be applied in the order they are given, this may be useful for large production campagins with a master config file plus minor adjustments for individual datasets.  

There are also a number of command line options that can be used to adjust various settings.  These are described in more detail on the command line API page, but in general they are intended to provide an easy way to switch merging modes or to run similar merges with different input files.  All command line options have a corresponding config key, and will override any settings from the default or user config files.  However, these options may still be set in user config files if the command line options are not used, which may be desirable for reproducibility in production campaigns.  

The final configuration for a given merge job is therefore a combination of:
1. The default configuration settings
2. Any user config files provided on the command line, applied in the order they are given
3. Any command line options provided, which override both the default and user config settings
A json representation of the final config is saved to the output directory as config.json.  This is used to resume jobs that fail due to database timeouts or similar issues, and can also be used to manually run a new merge job with the same settings by providing it as a user config file.

Types
=====

Config keys are type-checked, and will give an error if the user provides a value of the wrong type.  However, user configs may set key values to None, which will set the key to its default value.  Some keys have default values of None as well, which means they are optional and will be ignored or have some obvious default behavior if not set by the user.  Some types also have special handling, see below for details.  In the default config and example templates, key types are indicated with angle bracket notation:
.. code-block:: yaml

    key_name: <type(subtype)> value

The subtype is optional, and is generally useed to specify the contents of a container type such as a list or dict.  It is possible to chain subtypes for nested containers, eg "<list(list(int))>" for a list of lists of integers.  The value is also optional, and will be treated as None if not provided.

Numeric types (<int>, <float>, <bool>):
    These types are fairly self-explanatory, with the caveat that the values are optionals and may be set to None as mentioned above.  The user may provide any value that can be cast to the correct type.  For bools, the values ("true", "yes", "1") will be interpreted as True, while ("false", "no", "0") will be interpreted as False.

String types (<str>, <path>):
    Literal strings are self-explanatory, but strings may also include variables using python's f-string syntax, eg "{variable:format}".  Variables may refer to other config keys, environment variables, or metadata keys from the output files.  See the naming page for more details on the string formatting system.  The <path> type is a special case of <str> that will be expanded and converted to an absolute path after formatting.

Option (<opt(option1,option2,...)>):
    This type is used for keys that must be set to one of a specific set of options.  The user may provide any value that matches one of the options in the parentheses, ignoring case and whitespace.  The first option in the list is treated as the default value for the key.

Condition (<cond>):
    This type is used for condition strings that are evaluated at runtime.  They are used to check for additional metadata requirements for certain types of files, and to automatically choose an appropriate merging method based on the file metadata.  The strings are currently evaluated using python's eval() function, so they are potentially dangerous and should not be set in user configs without careful consideration.  If a condition refers to metadata keys that do not exist, the condition will evaluate to False.

Size Estimator (<size_spec>):
    This is a special type used to specify how the size of an output file scales with the size of the input files.  Four modes are currently supported:

    sum ('s', 'sum'):
        The output file size scales as the sum of the input file sizes.  This is the default setting and should be appropriate for most basic merges of generic data.
    
    average ('a', 'avg', 'average'):
        The output file size is set to the average of the input file sizes.  This is suitable for data such as ROOT histograms, where merging is done by adding the bin contents together but the number of bins (and thus the file size) remains the same.
    
    number ('n', 'num', 'number'):
        The output file size scales as the number of input files, independent of the file sizes.  This may be useful for something like diagnostics, where each input file contributes a fixed set of data to the output file.
    
    constant ('', 'b', 'kb', 'mb', 'gb', 'tb'):
        The output file size is set to a constant value, either in bytes or in human-readable units.  This may be useful for files with a fixed amount of overhead due to the structure of the file or embedded metadata.
    
    The user may also provide a formula specifying a linear combination of the individual modes, eg "0.5*sum + 0.5*avg" could be used to estimate the size of ROOT files containing a mixture of TTrees and histograms.

Collection types (<list>, <set>, <map>):
    These types are used for collections of values, which raises the question of how to combine collections from multiple config files.  The default behavior is to add any user-specified values to the existing collection.  If the user instead wishes to ignore the default values and start with an empty collection, they may add a tilde (~) to the key name in their config file.  For example, if the default config has a key "my_list: <list> [1, 2, 3]", if the user sets "my_list: [4, 5]" in their config file the final value will be [1, 2, 3, 4, 5].  If the user instead sets "~my_list: [4, 5]" in their config file, they will get a final value of [4, 5].  Sets and maps also support removing individual values, see below for details.

Lists (<list(subtype)>):
    Lists are ordered collections of values, may be restricted to contain only values of a certain type with the subtype notation.  User-specified values are added to the end of the list by default, although they may also override the default list with the tilde notation as described above.  When searching lists for a matching value, merge-utils will iterate in reverse order so that user-specified values take precedence over default values.

Sets (<set(subtype)>):
    Sets are unordered collections of unique values, and typically contain strings.  Because sets are not natively supported by either json or yaml, they are simply represented as lists in config files but will be converted to sets by the interpreter.  In addition to overriding the entire set as described above, users may also remove individual values from the default set by prefixing them with a tilde.  For example, a user config with "my_set: ['value1', '~value2']" will add "value1" to the set but remove "value2", if it exists.

Maps (<map(key_type, value_type)>):
    Maps are collections of unordered key-value pairs.  The key_type is assumed to be a string unless otherwise specified.  Maps are similar to dictionaries, but merge-utils makes a distinction because the user may add or remove values from a map.  Somewhat similarly to the sets, users may remove individual key-value pairs from the set by setting the value to None (also represented as ~ in yaml).  For example, a user config with "my_map: {'key1': 'value1', 'key2': ~}" will add the key-value pair "key1": "value1" to the map but remove "key2" if it exists.

Custom dictionary types:
    Some keys use custom types such as <merging_method> or <output_file>.  These are are dictionaries with specific required keys and value types, defined in the schema.type_defs section of the default config.  The user may provide any subset of the required keys, and the rest will be filled in with default values.  

Sections
========

The configuration keys are organized into sections based on their purpose. 

See an annotated template at

.. toctree::

    template
 
The sections are somewhat fluid and may change as the code evolves, but currently include:


input
-----

The input section includes keys related to the input files, including the input mode, the inputs themselves, skip and limit values to select a subset of files, and directories to search for local input files.  It also includes keys related to the job, such as the job tag, comment, and campaign.  Finally, it includes some keys defining how the input files should be handled, such as whether to stream them from remote storage or create local copies.

Most of the keys in the input section are overriden by command line options, and will often be set this way for simple merging tasks.  The most typical use case is probably to create a user config file defining the general merging behavior, and then to use this config for a number of individual merges with different input files specified by command line options.  However, for production campaigns it is possible to fully specify the input files and settings in user config files, which may be better for reproducibility.

output
------

The output section includes some keys related to how the merge should be run, such as the mode (merge, validate, etc) and whether to run locally or submit to JustIN.  These may also be set by command line options, but may be included in user config files for the same reasons as the input settings.  The user may also specify a custom directory for the job scripts generated by merge-utils, which are saved to merge-utils/tmp by default.  

The rest of the keys in the output section are related to the final output files created by the merging.  The user must specify the output file name, and may also set the output namespace if they wish to use a different namespace from the parents.  Grandparents mode will use the parents of the input files as the parents of the merged file, and should be used when merging temporary files that are not registered in the metadata catalog.  

The output name specified in this section is just a base name for the job, do not include things like skip and limit values or file extensions.  Merge-utils will automatically append a suffix to the base name to create the final output file names.  This suffix includes the file extension, a unique ID composed of the job tag, timestamp, skip, and limit, and additional suffixes specific to individual output streams if multiple outputs are created.  These suffixes are defined in the method section of the config file, and the user should have little need to adjust them.

The output `name` may simply be a literal string, but it may be a template including variables referring to other config keys, environment variables, or metadata keys from the output files.  This allows the user to create dynamic file names that automatically incorporate relevant information about the merge job and its inputs.  These variables are formatted using python's f-string syntax, see the naming page for more details.  

The output file locations depend on whether the merge is run locally or as a batch job.  For local runs, the output files will be saved to the directory specified by the out_dir key.  For batch runs, the output files will be automatically added to MetaCat and Rucio, using the lifetime specified in the batch subsection.  The user may also force a specific output RSE for the output files.  For merges that require multiple passes, the lifetime and RSE for the intermediate files may be set separately using the scratch subsection.

For large datasets we typically want to create multiple merged files of a reasonable size, rather than merging the entire dataset into a single huge file.  This behavior is controlled by the grouping subsection, which includes a size target for the outputs and whether to group by the number of input files or by the size in GB.  There is also an option to try to equalize the output file sizes, in case the dataset size is not a multiple of the target grouping.  For production jobs it is probably best for reproducibility to stick to a fixed number of input files, with equalization disabled.

metadata
--------

The metadata section has been moved to the config/defaults/metadata.yaml file, to avoid cluttering the main config.  It defines a number of requirements for valid file metadata, including required keys and keys restricted to specific types or values.  Some keys are required based on specific conditions, such as whether the file is real data or Monte Carlo.  There is also a subsection with some fixes for common metadata issues, which should be applied before checking the metadata for validity.  It is possible for user config files to override these settings, but in general it is probably best for users to leave these settings as is.  Also, most of these settings will be deprecated in the future when the MetaCatch package is fully implemented, which will handle metadata validation and fixing in a more robust and flexible way.

The three options likely to be relevant for users are the lists of consistent and optional metadata keys, and the dictionary of overrides.  All input files must have the same namespace and matching values for the consistent keys, but it may be desireable to use a more or less restrictive list of keys depending on the use case.  The list of optional keys negates any required keys, whether they come from the main list or are required based on some condition.  These options allow merges to be performed even when some metadata is missing or inconsistent, but in that case the user should also define overrides to set the metadata appropriately for the output files.  

validation
----------

The validation section sets options for input file validation and error handling.  Large MetaCat and Rucio queries are split into more reasonably sized batches based on the batch_size parameter.  When explicit file locations are provided instead of using Rucio, the paths are checked for validity and accessibility.  This can be I/O bottlenecked, so the concurrency parameter may be used to speed up the process by checking multiple paths in parallel.  The fast_fail option will cause the script to exit immediately if any unhandled errors are found, disabling this will cause it to continue processing more batches to get a full list of problem files but is typically a waste of time.  

The handling subsection provides a set of switches for how various types of errors are handled.  The default behavior is to quit if any errors are encountered, and the user is expected to fix the underlying issue and re-run the script.  However, it is also possible to skip problem files and continue with the merge.  This may be done in two ways: skip mode ignores the file entirely while gap mode still includes the file in the output group size calculations.  The latter essentially leaves space for the missing files in the outputs, and should be used for transient issues where the user expects to merge the missing files and add them to the original outputs at a later time.  For more permanent issues such as corrupted files that cannot be merged, skip mode is probably more appropriate.

The default handling key can be used to change the handling mode for all error types at once, without needing to specify each one individually.  However, if any specific handling mode is set to something other than default, it will override the default handling mode for that error type.  There are also some conditions that are not strictly errors, such as files that have already been merged in a previous job.  For these cases the default behavior is instead to include the file in the merge, but the user may also set any of the other error handling modes if they wish.

The checksums from MetaCat are checked for consistency against Rucio, or against the actual file checksums in the case of explicit file paths.  The default checksum type for DUNE is Adler32, but the user may specify additional checksum types to check.  Only one matching checksum is required for the file to be considered valid, and merge-utils will go through the list and skip any checksums that are missing.  The output file parents must also be valid files in MetaCat, files specified by name are checked for existence while files specified by FID are assumed to come from MetaCat and are not checked unless check_ids is set to True.

method
------

The method section includes settings related to how the merge is performed.  The method_name is a human-readable name for the merging method, and if this matches a method from the standard_methods section then the corresponding default settings will be inherited.  Any explicit changes to the settings in the method section will override the default values from the standard method.  The method name may also be set to "auto", in which case merge-utils will attempt to automatically choose an appropriate merging method based on the metadata of the input files and the conditions specified in the standard_methods section.  If no conditions are met, it will fall back to simply creating a tarball of the inputs which should work for any arbitrary files.

The cmd key allows the user to specify an arbitrary bash command which will be run to actually perform the merge.  The cmd string may use python f-string syntax with the keywords script, cfg, output, or inputs to substitute in the relevant values.  Defining the script and cfg variables with the corresponding keys allows merge-utils to locate the required files and add them to the job submission tarball, and the user may also specify additional dependencies if needed.  In the case of multiple output streams, the user may use the outputs (plural) variable to refer to the list of outputs, the singular keyword output is equivilant to outputs[0] and is provided for convenience when there is only one output stream.

Jobs that perform actual processing are referred to as transform jobs, as opposed to simple merges where the inputs are merely concatenated together.  For transform jobs, the user should use the transform key to specify the application name of the procesing step being performed.  This will be used to set the output files' core.application metadata keys, with the original values being added to origin.applications.  

The outputs key defines the list of output stream specifications.  Each output must have a name, which must include both {NAME} and {UUID} variables which refer to the main output.name and the file's unique ID respectively.  If the merge command produces a default output file name, the user may use the rename key to specify this file and it will be renamed to match the spec.  The pass2 key may be used to specify a different merging method for the second pass of a transform job, since the outputs from pass 1 are no longer the same type of file as the original inputs.  Transform jobs may also specify per-output metadata overrides, as well as temporary metadata values for the intermediate files in multi-stage merges.

The user may also define a size estimator for each output stream, which may be defined as a linear combination of the sum or average sizes of the input files, the number of inputs, or a constant term.  The default is simply the sum of the input sizes, but some files may scale differently with the number and size of inputs.  This size estimator is used when the output grouping mode is set to size, and may be ignored when grouping by number of files.  The user may also set a minimum size for the output files, which will be used when validating the output files after the merge and will throw an error if the output size is too small.  The user may also provide a file with an explicit checklist of expected contents for the output file, in which case an error will be thrown if anything from the checklist is missing.

The chunks subsection allows the user to control how many files are merged together in a single pass.  The chunk_max sets the maximum number of files per merge, and is a hard limit.  The chunk_min is used to avoid inefficiently merging very small chunks, it is merely a warning and may be safely ignored.  The max_size is intended to intelligently limit the number of files per merge to avoid running out of space on a worker node, but currently it is not fully implemented.

Finally, the environment subsection allows the user to specify the DUNE software version to use for batch jobs, as well as other environment variables that should be set before running the merge command.  The user may also specify a custom Apptainer image or their own custom products, but these are experimental features that have not been carefully tested and should be used with caution.

sites
-----

The sites section includes settings related to the JustIN batch system and site selection.  Merge-utils uses the site-storage distance database from JustIN, but the user may specify per-site and per-RSE distance offsets to adjust their priority.  Setting a distance offset above the max_distance will exclude that site or RSE from consideration, while setting a negative distance offset will increase its priority.  The default distance offset for sites is infinity, meaning only whitelisted sites will be considered.  For RSEs the default distance offset is 0, meaning all RSEs will be considered unless explicity blacklisted.  There is a separate default offset of 100 for tape-only RSEs, so they should only be considered if no disk-based RSEs are available.  DCACHE RSEs must be explicity specified, and are given an additional distance penalty for unstaged files.  The user is free to tweak these distance settings, but they are mainly intended for experts.

local
-----

The local section is used to identify the site where the front-end merge-utils script is being run.  This is useful when running local merges, or when merging files that are stored locally instead of in Rucio.  The hosts key is a map of known hostnames to site names, or the user may set the local site directly with the site key.  The xrootd subsection includes mappings between local file paths and their corresponding xrootd paths, which are used to make the files accessible to batch jobs and avoid overusing pnfs for large merges.  The hosts and xrootd mappings are not intended to be modified by most users, please contact the merge-utils team if you want to add a new site.

Logging
=======

    logging

