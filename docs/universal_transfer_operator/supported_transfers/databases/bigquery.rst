***************
Google Bigquery
***************

Transfer to Google Bigquery as destination dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
User can transfer data to Google Bigquery from following sources:

#. :ref:`table`

    .. literalinclude:: ../../../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START database]
       :end-before: [END database]

#. :ref:`file`

    .. literalinclude:: ../../../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START filelocation]
       :end-before: [END filelocation]

Following transfer modes are supported:

1. :ref:`non_native`
    Following is an example of a non-native transfer between GCS to BigQuery using non-native transfer:

    .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
       :language: python
       :start-after: [START transfer_non_native_gs_to_bigquery]
       :end-before: [END transfer_non_native_gs_to_bigquery]

2. :ref:`native`
    Following is an example of a native transfer between S3 to BigQuery using non-native transfer:

    .. literalinclude:: ../../../../tests_integration/test_data_provider/test_databases/test_bigquery.py
           :language: python
           :start-after: [START transfer_from_s3_to_bigquery_natively]
           :end-before: [END transfer_from_s3_to_bigquery_natively]

Supported native transfers parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. list-table::
   :widths: auto

   * - File Location
     - Database
     - transfer_params
     - Permission
     - transfer_params_key
   * - S3
     - Bigquery
     - https://cloud.google.com/bigquery-transfer/docs/s3-transfer#bq
     - https://cloud.google.com/bigquery/docs/s3-transfer#required_permissions and ``bigquery.jobs.create``
     - "s3_transfer_parameters"
   * - GCS
     - Bigquery
     - https://cloud.google.com/bigquery-transfer/docs/cloud-storage-transfer#bq
     - https://cloud.google.com/bigquery/docs/cloud-storage-transfer#required_permissions and ``bigquery.jobs.create``
     - "gs_transfer_parameters"

Examples
########
1. AWS S3 to Google Bigquery transfers
    - :ref:`non_native`
        Following is an example of a non-native transfer between AWS S3 to BigQuery using non-native transfer:

            .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_s3_to_bigquery]
               :end-before: [END transfer_non_native_s3_to_bigquery]

    - :ref:`native`
        Following is an example of a native transfer between S3 to BigQuery using native transfer:

        .. literalinclude:: ../../../../tests_integration/test_data_provider/test_databases/test_bigquery.py
               :language: python
               :start-after: [START transfer_from_s3_to_bigquery_natively]
               :end-before: [END transfer_from_s3_to_bigquery_natively]

2. GCS to Google Bigquery transfers
    - :ref:`non_native`
        Following is an example of a non-native transfer between GCS to BigQuery using non-native transfer:

            .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_gs_to_bigquery]
               :end-before: [END transfer_non_native_gs_to_bigquery]

    - :ref:`native`
        Following is an example of a native transfer between GCS to BigQuery using native transfer:

        .. literalinclude:: ../../../../tests_integration/test_data_provider/test_databases/test_bigquery.py
               :language: python
               :start-after: [START transfer_from_gcs_to_bigquery_natively]
               :end-before: [END transfer_from_gcs_to_bigquery_natively]

Transfer from Google Bigquery as source dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
User can transfer data from Google BigQuery to the following destination dataset:

#. :ref:`table`

    .. literalinclude:: ../../../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START database]
       :end-before: [END database]

#. :ref:`file`

    .. literalinclude:: ../../../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START filelocation]
       :end-before: [END filelocation]

Following transfer modes are supported:

1. Transfer using non-native approach
    Following is an example of non-native transfers between Bigquery to Snowflake using non-native transfer:

            .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_bigquery_to_snowflake]
               :end-before: [END transfer_non_native_bigquery_to_snowflake]

2. Transfer using third-party platform

Examples
########

1. Bigquery to Snowflake transfers
    - :ref:`non_native`
        Following is an example of non-native transfers between Bigquery to Snowflake using non-native transfer:

            .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_bigquery_to_snowflake]
               :end-before: [END transfer_non_native_bigquery_to_snowflake]

2. Bigquery to Sqlite transfers
    - :ref:`non_native`
        Following is an example of non-native transfers between Bigquery to Sqlite using non-native transfer:

            .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_bigquery_to_sqlite]
               :end-before: [END transfer_non_native_bigquery_to_sqlite]
