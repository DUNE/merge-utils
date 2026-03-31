Configuration
=============

In most cases the user is expected to provide one or more config files with the option '-c my_config.yaml' on the command line.  These config files can be written in yaml, json, or toml, and more formats may be added on request.  The script will search for the specified files in the merge-utils config directory, or the full file path may be provided.  If multiple user configs are provided the settings will be applied in the order they are given, this may be useful for large production campagins with a master config file plus minor adjustments for individual datasets.  Command line options are applied last and will override the relevant settings from any config files.

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


Default configurations are set in the following files:

.. toctree::

    defaults
    metadata
    logging

