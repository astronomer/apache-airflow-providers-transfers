.. _fivetran_integrations:

Transfer using Fivetran
~~~~~~~~~~~~~~~~~~~~~~~
The :py:mod:`universal_transfer_operator operator <universal_transfer_operator.universal_transfer_operator>` allows data transfers between any supported source :ref:`dataset` and destination :ref:`dataset` using :ref:`third_party` mode of transfer.


You can transfer using Fivetran, with two scenarios:

1. Fivetran :ref:`connector <fivetran_connector>` setup already done using Fivetran UI.

    You can set the connector on Fivetran UI using the following steps:

    * Create Fivetran Group
    * Create Fivetran Destination
    * Create Fivetran connector
    * Start initial sync
    * Run the transfer job

    .. note::
        Detailed instruction for the above steps can be followed `here <https://fivetran.com/docs/getting-started>`_

    Here is an example of how to use Fivetran for transfers when connector is already setup:

        .. literalinclude:: ../../../../example_dags/example_dag_fivetran.py
           :language: python
           :start-after: [START fivetran_transfer_with_setup]
           :end-before: [END fivetran_transfer_with_setup]


2. Setup Fivetran :ref:`connector <fivetran_connector>` and transfer using universal transfer operator

    When user has Fivetran account without any :ref:`connector <fivetran_connector>` setup, Universal Transfer Operator takes care of creation of Fivetran destination, Fivetran :ref:`connector <fivetran_connector>` and finally doing the transfers. ``transfer_params`` has to be passed optional details about :ref:`connector <fivetran_connector>` and destination. Universal Transfer Operator also takes care of mapping the airflow connections and creating the corresponding Fivetran destination and Fivetran :ref:`connector <fivetran_connector>`.

    Here is an example of how to use Fivetran to setup the :ref:`connector <fivetran_connector>` on Fivetran and do the transfer once the setup is done by Universal Transfer Operator

        .. literalinclude:: ../../../../example_dags/example_dag_fivetran.py
           :language: python
           :start-after: [START fivetran_transfer_without_setup]
           :end-before: [END fivetran_transfer_without_setup]

.. _fivetran_integrations_parameters:

Parameters to be passed for Fivetran Integrations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
There are two ways user can pass the parameters for ``transfer_params``:

1. Users can pass the parameters as a python dictionary with configuration for Fivetran. Here is an example of how to use Fivetran to setup the connector on Fivetran and do the transfer once the setup is done by Universal Transfer Operator

    .. literalinclude:: ../../../../example_dags/example_dag_fivetran.py
       :language: python
       :start-after: [START fivetran_transfer_without_setup]
       :end-before: [END fivetran_transfer_without_setup]

2. User can pass the :py:obj:`FiveTranOptions <universal_transfer_operator.integrations.fivetran.fivetran.FiveTranOptions>` class to ``transfer_params``. Here is an example:

    .. literalinclude:: ../../../../example_dags/example_dag_fivetran.py
       :language: python
       :start-after: [START fivetran_transfer_with_setup]
       :end-before: [END fivetran_transfer_with_setup]

.. note::
    Possible parameters allowed to be passed for Fivetran :ref:`connector <fivetran_connector>` are attributes of :py:obj:`FiveTranOptions <universal_transfer_operator.integrations.fivetran.fivetran.FiveTranOptions>`. Universal Transfer Operator also takes care of mapping the airflow connections and creating the corresponding Fivetran :ref:`destination <fivetran_snowflake_destination>` and Fivetran :ref:`connector <fivetran_connector>`.

1. ``conn_id`` - Connection id of Fivetran
2. ``connector_id`` - The unique identifier for the :ref:`connector <fivetran_connector>` within the Fivetran system if setup is done (optional)
3. :py:obj:`Group <universal_transfer_operator.integrations.fivetran.fivetran.Group>` - Group in FiveTran system (optional)
4. :py:obj:`Connector <universal_transfer_operator.integrations.fivetran.connector.base.FivetranConnector>` - :ref:`connector <fivetran_connector>` in Fivetran system (optional)
5. :py:obj:`Destination <universal_transfer_operator.integrations.fivetran.destination.base.FivetranDestination>` - :ref:`destination <fivetran_snowflake_destination>` in Fivetran system (optional)
