.. _fivetran_destination:

Fivetran Destination
~~~~~~~~~~~~~~~~~~~~~
Fivetran connects to all of your supported data sources and loads the data from them into your destination. From the each connector, Fivetran copies the file into staging tables in the destination. In the process, Fivetran transmit the ephemeral encryption key for the file to the destination so it can decrypt the data as it arrives. Before Fivetran write the data into the destination, Fivetran update the schema of existing tables to accommodate the newer incoming batch of data. Fivetran then merge the data from the staging tables with the existing data present in the destination. Finally, Fivetran apply the deletes (if any) on the existing tables. Once Fivetran complete the write process, the connector process terminates itself. A system scheduler will later restart the process for the next update.

In the Destination section of your dashboard, you can view all the destinations that sync to your destinations, add new destinations, and see in-depth information about individual destinations. More details: `Fivetran Destinations <https://fivetran.com/docs/destinations>`_

:py:mod:`universal_transfer_operator operator <universal_transfer_operator.universal_transfer_operator>` maps the airflow connections to create the Fivetran Destination. Each destination aspects configuration details as per the destination. Following is the example of parameters passed to destination as ``config``.

    .. literalinclude:: ../../../../../example_dags/example_dag_fivetran.py
       :language: python
       :start-after: [START fivetran_transfer_without_setup]
       :end-before: [END fivetran_transfer_without_setup]

.. note::
    More details on parameters which can be pass as part of Fivetran destination config is documented `here <https://fivetran.com/docs/rest-api/destinations#createadestination>`_
