Installation
============

Bootstrap in this order:

.. code-block:: bash

   pip install -r requirements.txt
   pip install -e .
   pytest

Notes
-----

- ``requirements.txt`` is the canonical quick-start dependency entrypoint.
- ``requirements/test.txt`` contains explicit test requirements.
- Project metadata is declared in ``pyproject.toml``.
- For optional grouped installs, see the ``requirements/`` directory.
