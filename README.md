> ⚠️ **DEPRECATED**  
> This project is no longer maintained. The GitHub repository has been archived, and no further updates will be made.  
> Please use at your own risk and consider alternatives if available.

<h1 align="center">
  Universal Transfer Operator
</h1>
  <h3 align="center">
transfers made easy<br><br>
</h3>


[![CI](https://github.com/astronomer/apache-airflow-provider-transfers/actions/workflows/ci-uto.yaml/badge.svg)](https://github.com/astronomer/apache-airflow-provider-transfers)

The `UniversalTransferOperator` simplifies how users transfer data from a source to a destination using [Apache Airflow](https://airflow.apache.org/). It offers a consistent agnostic interface, improving the users' experience so they do not need to use explicitly specific providers or operators.

At the moment, it supports transferring data between [file locations](https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/src/universal_transfer_operator/constants.py#L26-L32) and [databases](https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/src/universal_transfer_operator/constants.py#L72-L74) (in both directions) and cross-database transfers.

This project is maintained by [Astronomer](https://astronomer.io).

## Prerequisites

- Apache Airflow >= 2.4.0.

## Install

The apache-airflow-provider-transfers is available at [PyPI](https://pypi.org/project/apache-airflow-provider-transfers/). Use the standard Python
[installation tools](https://packaging.python.org/en/latest/tutorials/installing-packages/).

To install a cloud-agnostic version of the apache-airflow-provider-transfers, run:

```shell
pip install apache-airflow-provider-transfers
```

You can also install dependencies for using the `UniversalTransferOperator` with popular cloud providers:

```shell
pip install apache-airflow-provider-transfers[amazon,google,snowflake]
```

## Quickstart
Users can get started quickly with following two approaches:
1. Spinning up a local Airflow infrastructure using the open-source Astro CLI and Docker
2. Using vanilla Airflow and Python

### Run `UniversalTransferOperator` using [astro](https://docs.astronomer.io/astro/create-first-dag)
* Create an [Astro project](https://docs.astronomer.io/astro/create-first-dag).
  * Open your terminal or IDE
  * Install [docker](https://docs.docker.com/engine/install/) and  [astro-cli](https://docs.astronomer.io/astro/cli/overview) using this [documentation](https://docs.astronomer.io/astro/cli/install-cli).
  * Create a new directory for your Astro project:

    ```shell
    mkdir <your-astro-project-name>
     ```
  * Open the directory:

    ```shell
    cd <your-astro-project-name>
    ```
  * Run the following Astro CLI command to initialize an Astro project in the directory:
    ```shell
    astro dev init
    ```
  * This command generates the following files in the directory:


        .
        ├── .env # Local environment variables
        ├── dags # Where your DAGs go
        │   ├── example-dag-basic.py # Example DAG that showcases a simple ETL data pipeline
        │   └── example-dag-advanced.py # Example DAG that showcases more advanced Airflow features, such as the TaskFlow API
        ├── Dockerfile # For the Astro Runtime Docker image, environment variables, and overrides
        ├── include # For any other files you'd like to include
        ├── plugins # For any custom or community Airflow plugins
        │   └── example-plugin.py
        ├── tests # For any DAG unit test files to be run with pytest
        │   └── test_dag_integrity.py # Test that checks for basic errors in your DAGs
        ├── airflow_settings.yaml # For your Airflow connections, variables and pools (local only)
        ├── packages.txt # For OS-level packages
        └── requirements.txt # For Python packages (add apache-airflow-provider-transfers here)


  * Add the following in requirements.txt

    ```text
    apache-airflow-provider-transfers[all]
    ```

  * Copy file named [example_transfer_and_return_files.py](./example_dags/example_transfer_and_return_files.py) and [example_snowflake_transfers.py](./example_dags/example_snowflake_transfers.py) and add it to the `dags` directory of your Airflow project:

   https://github.com/astronomer/apache-airflow-provider-transfers/blob/04b53d780790eaa3b424458742bc89d6fbec2ccd/example_dags/example_transfer_and_return_files.py#L1-L45

   https://github.com/astronomer/apache-airflow-provider-transfers/blob/a80dc84b7f33bb86ae244f79411b240f4f4c7e22/example_dags/example_snowflake_transfers.py#L1-L46

   Alternatively, you can download `example_transfer_and_return_files.py` and `example_snowflake_transfers.py`.
   ```shell
    curl -O https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/example_dags/example_transfer_and_return_files.py

    curl -O https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/example_dags/example_snowflake_transfers.py
   ```

  * Create the environment variable for AWS bucket, Snowflake and google cloud bucket for the transfer as per the following:

    https://github.com/astronomer/apache-airflow-provider-transfers/blob/e711c41e841559542148c0b52f000e1159a818f7/example_dags/example_snowflake_transfers.py#L11-L12

    https://github.com/astronomer/apache-airflow-provider-transfers/blob/e711c41e841559542148c0b52f000e1159a818f7/example_dags/example_transfer_and_return_files.py#L10-L14

  * To start running the example DAGs in a local Airflow environment, run the following command from your project directory:

    ```shell
    astro dev start
    ```
  * After your project builds successfully, open the Airflow UI in your web browser at https://localhost:8080/. Find your DAGs in the dags directory in the Airflow UI.
  * Create airflow connection for snowflake, google, SFTP and amazon using airflow UI and run the DAGs.
  * Create airflow connection for snowflake, google and amazon using airflow UI as per documentation below.

    | Cloud Provider | Connection name      |  Documentation link                                                                                                          |
    |----------------|----------------------|------------------------------------------------------------------------------------------------------------------------------|
    | Amazon         | aws_default          | [aws connection](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html)                |
    | Google         | google_cloud_default | [google cloud connection](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html)       |
    | Snowflake      | snowflake_conn       | [snowflake connection](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html) |

  * Trigger the DAGs and validate the transfers.


### Run `UniversalTransferOperator` using vanilla [airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html) and python
  * Install airflow and setup project following this [documentation](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html).
  * Ensure that your Airflow environment is set up correctly by running the following commands:

    ```shell
    export AIRFLOW_HOME=`pwd`
    airflow db init
    ```

  * Add the following in requirements.txt

    ```text
    apache-airflow-provider-transfers[all]
    ```

  * Copy file named [example_transfer_and_return_files.py](./example_dags/example_transfer_and_return_files.py) and [example_snowflake_transfers.py](./example_dags/example_snowflake_transfers.py) and add it to the `dags` directory of your Airflow project:

   https://github.com/astronomer/apache-airflow-provider-transfers/blob/04b53d780790eaa3b424458742bc89d6fbec2ccd/example_dags/example_transfer_and_return_files.py#L1-L45

   https://github.com/astronomer/apache-airflow-provider-transfers/blob/a80dc84b7f33bb86ae244f79411b240f4f4c7e22/example_dags/example_snowflake_transfers.py#L1-L46

   Alternatively, you can download `example_transfer_and_return_files.py` and `example_snowflake_transfers.py`.
   ```shell
    curl -O https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/example_dags/example_transfer_and_return_files.py

    curl -O https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/example_dags/example_snowflake_transfers.py
   ```

  * Create the environment variable for AWS bucket, Snowflake and google cloud bucket for the transfer as per the following:

    https://github.com/astronomer/apache-airflow-provider-transfers/blob/e711c41e841559542148c0b52f000e1159a818f7/example_dags/example_snowflake_transfers.py#L11-L12

    https://github.com/astronomer/apache-airflow-provider-transfers/blob/e711c41e841559542148c0b52f000e1159a818f7/example_dags/example_transfer_and_return_files.py#L10-L14

  * Run your project in a local Airflow environment.
  * After your project builds successfully, open the Airflow UI in your web browser at https://localhost:8080/. Find your DAGs in the dags directory in the Airflow UI.
  * Create airflow connection for snowflake, google and amazon using airflow UI as per documentation below.

    | Cloud Provider | Connection name      |  Documentation link                                                                                                          |
    |----------------|----------------------|------------------------------------------------------------------------------------------------------------------------------|
    | Amazon         | aws_default          | [aws connection](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/connections/aws.html)                |
    | Google         | google_cloud_default | [google cloud connection](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html)       |
    | Snowflake      | snowflake_conn       | [snowflake connection](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/connections/snowflake.html) |

  * Trigger the DAGs and validate the transfers.

## Example DAGs

Checkout the [example_dags](./example_dags) folder for examples of how the `UniversalTransferOperator` can be used.


## How Universal Transfer Operator Works

![Approach](./docs/images/approach.png)

With `UniversalTransferOperator`, users can perform data transfers using the following transfer modes:

1. Non-native
2. Native
3. Third-party


### Non-native transfer

Non-native transfers rely on transferring the data through the Airflow worker node. Chunking is applied where possible. This method can be suitable for datasets smaller than 2GB, depending on the source and target. The performance of this method is highly dependent upon the worker's memory, disk, processor and network configuration.

Internally, the steps involved are:
- Retrieve the dataset data in chunks from dataset storage to the worker node.
- Send data to the cloud dataset from the worker node.

Following is an example of non-native transfers between Google cloud storage and Sqlite:

https://github.com/astronomer/apache-airflow-provider-transfers/blob/a80dc84b7f33bb86ae244f79411b240f4f4c7e22/example_dags/example_universal_transfer_operator.py#L68-L74

### Improving bottlenecks by using native transfer

An alternative to using the Non-native transfer method is the native method. The native transfers rely on mechanisms and tools offered by the data source or data target providers. In the case of moving from object storage to a Snowflake database, for instance, a native transfer consists in using the built-in ``COPY INTO`` command. When loading data from S3 to BigQuery, the Universal Transfer Operator uses the GCP  Storage Transfer Service.

The benefit of native transfers is that they will likely perform better for larger datasets (2 GB) and do not rely on the Airflow worker node hardware configuration. With this approach, the Airflow worker nodes are used as orchestrators and do not perform the transfer. The speed depends exclusively on the service being used and the bandwidth between the source and destination.

Steps:
- Request destination dataset to ingest data from the source dataset.
- Destination dataset requests source dataset for data.

> **_NOTE:_**
 The Native method implementation is in progress and will be available in future releases.


### Transfer using a third-party tool
The `UniversalTransferOperator` can also offer an interface to generic third-party services that transfer data, similar to Fivetran.

Here is an example of how to use Fivetran for transfers:

https://github.com/astronomer/apache-airflow-provider-transfers/blob/7d5188c4af214d5cdaeb714654e9bdf5b48cb3fb/example_dags/example_dag_fivetran.py#L35-L56

## Supported technologies

- Databases supported:

    https://github.com/astronomer/apache-airflow-provider-transfers/blob/513591afb967694062097d6b36170265883b77e3/src/universal_transfer_operator/constants.py#L72-L74

- File store supported:

    https://github.com/astronomer/apache-airflow-provider-transfers/blob/513591afb967694062097d6b36170265883b77e3/src/universal_transfer_operator/constants.py#L28-L31


## Documentation

The documentation is a work in progress -- we aim to follow the [Diátaxis](https://diataxis.fr/) system.

- **[Reference guide](https://apache-airflow-provider-transfers.readthedocs.io/)**: Commands, modules, classes and methods

- **[Getting Started Tutorial](https://apache-airflow-provider-transfers.readthedocs.io/en/latest/getting-started/GETTING_STARTED.html)**: A hands-on introduction to the Universal Transfer Operator


## Changelog

The **Universal Transfer Operator** follows semantic versioning for releases. Check the [changelog](/docs/CHANGELOG.md) for the latest changes.


## Release management

See [Managing Releases](/docs/development/RELEASE.md) to learn more about our release philosophy and steps.


## Contribution guidelines

All contributions, bug reports, bug fixes, documentation improvements, enhancements, and ideas are welcome.

Read the [Contribution Guideline](/docs/development/CONTRIBUTING.md) for a detailed overview of how to contribute.

Contributors and maintainers should abide by the [Contributor Code of Conduct](CODE_OF_CONDUCT.md).


## License

[Apache Licence 2.0](LICENSE)
