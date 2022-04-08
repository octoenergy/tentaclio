Documentation
=====================================

Tentaclio uses `reStructuredText`_ (Restructured Text), `MyST`_ (Markedly Structured Text) and
the `Sphinx`_ documentation system. This allows it to be built into other forms for easier
viewing and browsing.

Building docs
-------------

To create an HTML version of the docs, use::

    $ cd docs

    $ make html

This will generate a static set of HTML files with root `docs/_build/html/index.html`. This can be
viewed in a browser

.. _reStructuredText: https://docutils.sourceforge.io/rst.html
.. _MyST: https://myst-parser.readthedocs.io/en/latest/
.. _Sphinx: https://www.sphinx-doc.org/

