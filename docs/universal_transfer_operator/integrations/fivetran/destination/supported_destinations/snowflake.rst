.. _fivetran_snowflake_destination:

Snowflake Destination
~~~~~~~~~~~~~~~~~~~~~
:py:obj:`SnowflakeDestination <universal_transfer_operator.integrations.fivetran.destination.snowflake.SnowflakeDestination>` creates the destination on Fivetran for Snowflake. Here is an example of parameters that can be passed:

.. literalinclude:: ../../../../../../example_dags/example_dag_fivetran.py
       :language: python
       :start-after: [START fivetran_transfer_without_setup]
       :end-before: [END fivetran_transfer_without_setup]

.. note::
    Universal Transfer Operator also takes care of mapping the airflow connections and creating the corresponding Fivetran :ref:`destination <fivetran_snowflake_destination>` and Fivetran :ref:`connector <fivetran_connector>`.

Possible parameters that can be passed as config for :py:obj:`SnowflakeDestination <universal_transfer_operator.integrations.fivetran.destination.snowflake.SnowflakeDestination>` are listed `here <https://fivetran.com/docs/rest-api/destinations/config#snowflake>`_
