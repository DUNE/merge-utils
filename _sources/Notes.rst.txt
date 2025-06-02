Notes on sphinx
---------------

This was my second sphinx project.

I started with the https://www.sphinx-doc.org/en/master/tutorial/getting-started.html tutorial so I have a similar structure.

I used the nature theme that Igor Mandrichenko used from https://metacat.readthedocs.io/en/latest/

rst format
**********

https://docutils.sourceforge.io/docs/user/rst/quickstart.html

Not my favorite -

  - the need to keep track of indentation and blank lines with poor error messages is painful

  - indentation differences when adding comments to code others wrote can be interesting

Github actions
**************

I set up github actions to build on my github page hschellman/merge-utils-test

The github actions had some issues:

  - It assumed the code was in main - I had to move to develop

  - I had to add a gh-pages branch by hand

  - I adapted `<https://github.com/ammaraskar/sphinx-action>`_ from Ammar Askar but it insisted on using python 3.8 which broke the 3.9 features in our code.  So I wrote a much simpler one myself. 

  - The action can be found in the sphinx.yml file 

  - I was not able to get the programoutput extension to work as adding a pip install for it did not work. 
  
  - 2025-05-22 I managed to get the programoutput to work by copying the python code for the merge command line from my venv into docs/source and making the command  '.. program-output:: python merge -h'

  - merge contains

    .. literalinclude:: merge

  - I had to add a

    .. code-block:: python

      autodoc_mock_imports = ["metacat","rucio"]

    to the `conf.py`  to avoid errors due to imports of external code.  I just got blanks otherwise.

Google indexing Notes
*********************

# Go to `Google Search Console <https://search.google.com/search-console/welcome?hl=en&utm_source=wmx&utm_medium=deprecation-pane&utm_content=home>`_

# choose  URL prefix and enter your top level URL you will get back a 

# I github added build/html (just the directories) to github to be in the same directory as `index.html` and then placed the key file I got from google in that directory.  

# I then added that file to github, committed and pushed.  

# Once the action to build the docs go to the search console again and go to URL inspection.  You should be able to check that the URL exists and request indexing. 