.. _universal_transfer_operator:

When to use the ``universal_transfer_operator`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The :py:mod:`universal_transfer_operator operator <universal_transfer_operator.universal_transfer_operator>` allows data transfers between any supported source :ref:`dataset` and destination :ref:`dataset`. It offers a consistent agnostic interface, simplifying the users' experience, so they do not need to use specific providers or operators for transfers.

This ensures a consistent set of :py:mod:`Data Providers <universal_transfer_operator.data_providers>` that can read from and write to :ref:`dataset`. The Universal Transfer
Operator can use the respective :py:mod:`Data Providers <universal_transfer_operator.data_providers>` to transfer between as a source and a destination. It also takes advantage of any existing fast and
direct high-speed endpoints, such as Snowflake’s built-in ``COPY INTO`` command to load S3 files efficiently into the Snowflake.

Universal transfer operator also supports the transfers using third-party platforms like Fivetran.

.. to edit figure below refer - https://drive.google.com/file/d/1Ih0SRnMvgKTQHLJaW9k21jutjEiyacRz/view?usp=sharing
.. figure:: /images/approach.png

There are three modes to transfer data using of the ``universal_transfer_operator``.

1. :ref:`non_native`
2. :ref:`native`
3. :ref:`third_party`

More details on how transfer works can be found at :ref:`transfer_working`.

Case 1: Transfer using non-native approach
    Following is an example of non-native transfers between Google cloud storage and Sqlite:

    .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
       :language: python
       :start-after: [START transfer_non_native_gs_to_sqlite]
       :end-before: [END transfer_non_native_gs_to_sqlite]

Case 2: Transfer using native approach

Case 3: Transfer using third-party platform
    Here is an example of how to use Fivetran for transfers:

    .. literalinclude:: ../../example_dags/example_dag_fivetran.py
       :language: python
       :start-after: [START fivetran_transfer_with_setup]
       :end-before: [END fivetran_transfer_with_setup]

.. _cross_database_transfers:

Cross database transfers
~~~~~~~~~~~~~~~~~~~~~~~~
Universal transfer operators can be used to transfer data between databases. For examples:

.. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
   :language: python
   :start-after: [START transfer_non_native_bigquery_to_snowflake]
   :end-before: [END transfer_non_native_bigquery_to_snowflake]


Comparison with traditional transfer Operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
1. File to File transfers
    Following example transfers data from S3 to GCS using Universal transfer Operator:

        .. literalinclude:: ../../example_dags/transfer_comparison_with_traditional_transfer_operator.py
           :language: python
           :start-after: [START howto_transfer_file_from_s3_to_gcs_using_universal_transfer_operator]
           :end-before: [END howto_transfer_file_from_s3_to_gcs_using_universal_transfer_operator]


    Following example transfers data from S3 to GCS using traditional S3ToGCSOperator:

        .. literalinclude:: ../../example_dags/transfer_comparison_with_traditional_transfer_operator.py
           :language: python
           :start-after: [START howto_transfer_file_from_s3_to_gcs_using_traditional_S3ToGCSOperator]
           :end-before: [END howto_transfer_file_from_s3_to_gcs_using_traditional_S3ToGCSOperator]

2. File to Table transfers
    Following example transfers data from S3 to Snowflake using Universal transfer Operator:

        .. literalinclude:: ../../example_dags/transfer_comparison_with_traditional_transfer_operator.py
           :language: python
           :start-after: [START howto_transfer_data_from_s3_to_snowflake_using_universal_transfer_operator]
           :end-before: [END howto_transfer_data_from_s3_to_snowflake_using_universal_transfer_operator]

    Following example transfers data from S3 to Snowflake using traditional S3ToSnowflakeOperator. Table and stage needs to be created before the transfer. This is not handled by ``S3ToSnowflakeOperator``.

        .. literalinclude:: ../../example_dags/transfer_comparison_with_traditional_transfer_operator.py
           :language: python
           :start-after: [START howto_transfer_data_from_s3_to_snowflake_using_S3ToSnowflakeOperator]
           :end-before: [END howto_transfer_data_from_s3_to_snowflake_using_S3ToSnowflakeOperator]


.. _load_strategies:

Load Strategies
~~~~~~~~~~~~~~~

Following are the load strategies supported when :ref:`table` is the destination dataset:

.. literalinclude:: ../../src/universal_transfer_operator/constants.py
   :language: python
   :start-after: [START LoadExistStrategy]
   :end-before: [END LoadExistStrategy]


If the table you trying to create already exists, you can specify whether you want to replace the table or append the new data by specifying either if_exists='append' or if_exists='replace'.

1. ``if_exists="replace"``
    By default if ``transfer_params`` is not passed as an argument to :py:mod:`universal_transfer_operator operator <universal_transfer_operator.universal_transfer_operator>`, it sets if_exists='replace' by default.

    .. literalinclude:: ../../example_dags/example_append_and_replace_strategies.py
       :language: python
       :start-after: [START howto_transfer_data_from_s3_to_snowflake_using_replace]
       :end-before: [END howto_transfer_data_from_s3_to_snowflake_using_replace]

    .. note::
        If you use if_exists='replace', existing table will be dropped and the schema of the new data will be used.

2. ``if_exists="append"``
    User can set ``if_exists="append"`` by passing an argument ``transfer_params=TransferIntegrationOptions(if_exists="append")`` to :py:mod:`universal_transfer_operator operator <universal_transfer_operator.universal_transfer_operator>` as the example below:

    .. literalinclude:: ../../example_dags/example_append_and_replace_strategies.py
       :language: python
       :start-after: [START howto_transfer_data_from_s3_to_snowflake_using_append]
       :end-before: [END howto_transfer_data_from_s3_to_snowflake_using_append]
