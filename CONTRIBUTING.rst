.. _bradata-contributing:

Contributing
############

**note**: nothing here is set in stone. if you think something here is misguided, speak to the maintainers.

general guidelines
==================

-  OPEN-SOURCE: this is an open-source project. therefore, everything in
   it should be open-source (scripts, documentation, file formats, etc).

-  LANGUAGE: this project's language is English, even if most of our
   contributors are Brazilian and we're working with Brazilian data. Our
   purpose is to make this project welcoming of international
   contributors and maybe even spread its idea abroad.

-  STANDARDS: whenever possible use (or convert things to) the
   international standard. for most data, this will mean changing the
   encoding from latin1 to UTF-8 and changing the date format from
   DD/MM/YYYY to YYYY-MM-DD. standardizing will make it easier to work
   with several databases together. if you find something that should be
   an exception, open an issue or talk to the coordinators.

-  ATTRIBUTION: please be aware when employing third-party software:
   check if their license is compatible with your use. (if unsure, ask).
   **always** attribute someone else's work to them. similarly, when you
   complete any work, you must attribute it to yourself under an
   open-source license. check `here <https://choosealicense.com/>`__ if
   unsure about a license, or just pick the MIT license which is our
   default. all files contributed must be prefixed by their license and
   author in a comment.

-  DOCUMENTATION: all code must be thoroughly documented. undocumented
   code or incomprehensible code will not be accepted. choose clarity
   over performance unless you absolutely have to pick the latter.
   (hint: `you almost never
   will <http://softwareengineering.stackexchange.com/questions/80084/is-premature-optimization-really-the-root-of-all-evil>`__.)

-  

code guidelines
===============

file structure
--------------

in the bradata package, every smodule is an institution (data provider). at its directory, its ``__init__.py`` should contain the functions and classes that are to be available to the public, and **nothing else**. that's because the preferred way for a user to use the ``bradata`` package is to explore what it has to offer by tab-completion available at ipython and jupyter notebook, as the package is projected to have a number of functions greater than what a user would like to memorize.

importing only the public functions in the ``__init__.py`` file prevents the namespace from being crowded with private objects::

    import bradata.tse as tse
    tse.get_candidatos()

submodules should be divided by similarity or proximity, for instance ``bradata/cgu/_cadastros.py`` has functions to get three different databases, but as the code to get them is mostly the same they reside together. (the three functions are actually only one function and two wrappers, to prevent writing more code than we need to). if the submodule is not meant to be called by the user, it should start with an underscore (\_), so that it doesn't pollute the namespace.

git workflow
============

so you've forked the repo and added some nice functionality, or correct some bug. thank you very much! but before we can accept your work, you must follow a few simple procedures:

- document every function, class, module, etc. you create or change, prerrably using `google-style docstrings <http://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html>`_. if you are implementing some tricky part, we'd appreciate if you wrote a tutorial or some kind of extensive documentation. we autogenerate documentation using sphinx, and you may write in .md or .rst, but please write.

- always ``git pull [source-repo] master`` before making a pull request!

- if you created a new public module or submodule, import it in the ``__init__.py`` of the main package.

- add your name to the :ref:`bradata-authors`;

contributors
============

contributors are listed under :ref:`bradata-authors`. only people
who have had a pull request accepted are listed as contributors.
