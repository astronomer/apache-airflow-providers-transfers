.. universal-transfer-operator documentation master file, created by
   sphinx-quickstart on Tue Jun 21 12:50:10 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root ``toctree`` directive.

Welcome to the Universal Transfer Operator documentation!
=========================================================

.. toctree::
   :maxdepth: 2
   :glob:

   getting-started/*
   universal_transfer_operator/operator.rst

.. toctree::
   :maxdepth: 2
   :caption: Transfer examples:
   :glob:

   universal_transfer_operator/supported_transfers/databases/*
   universal_transfer_operator/supported_transfers/files/*

.. toctree::
   :maxdepth: 3
   :caption: Third-party transfers:
   :glob:

   universal_transfer_operator/supported_integrations/fivetran/*
   universal_transfer_operator/supported_integrations/fivetran/connector/*
   universal_transfer_operator/supported_integrations/fivetran/connector/supported_connectors/*
   universal_transfer_operator/supported_integrations/fivetran/destination/*
   universal_transfer_operator/supported_integrations/fivetran/destination/supported_destinations/*

.. toctree::
   :maxdepth: 2
   :caption: Concepts and guides
   :glob:

   guides/concepts.rst

.. toctree::
   :maxdepth: 1
   :caption: Reference
   :glob:

   autoapi/*
   configurations.rst
   supported_databases.rst
   supported_file.rst
   supported_ingestors.rst
   supported_transfer_mode.rst

.. toctree::
   :maxdepth: 1
   :caption: CHANGELOG
   :glob:

   CHANGELOG.md

.. toctree::
   :maxdepth: 2
   :caption: Developing Universal Transfer Operator
   :glob:

   development/*

Indices and Tables
==================

 * :ref:`genindex`
 * :ref:`modindex`
 * :ref:`search`
