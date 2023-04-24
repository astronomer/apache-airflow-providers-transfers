.. _fivetran_connector:

Fivetran Connector
~~~~~~~~~~~~~~~~~~~
Fivetran connects to all of your supported data sources and loads the data from them into your :ref:`destination <fivetran_snowflake_destination>`. Each data source has one or more connectors that run as independent processes that persist for the duration of one update. A single Fivetran account, made up of multiple connectors, loads data from multiple data sources into one or more destinations.

In the Connectors section of your dashboard, you can view all the connectors that sync to your destinations, add new connectors, and see in-depth information about individual connectors. You can also upload CSV files to your destination. More details: `Fivetran Connector <https://fivetran.com/docs/getting-started/fivetran-dashboard/connectors>`_

:py:mod:`universal_transfer_operator operator <universal_transfer_operator.universal_transfer_operator>` maps the airflow connections to create the Fivetran Connector. Each connector aspects configuration details as per the data source. Following is the example of parameters passed to connector as ``config``.

    .. literalinclude:: ../../../../../example_dags/example_dag_fivetran.py
       :language: python
       :start-after: [START fivetran_transfer_without_setup]
       :end-before: [END fivetran_transfer_without_setup]

.. note::
    More details on parameters which can be pass as part of Fivetran connector config is documented `here <https://fivetran.com/docs/rest-api/connectors#createaconnector>`_
