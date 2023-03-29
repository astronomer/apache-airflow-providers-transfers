<h1 align="center">
  Universal Transfer Operator
</h1>
  <h3 align="center">
transfers made easy<br><br>
</h3>


[![CI](https://github.com/astronomer/apache-airflow-provider-transfers/actions/workflows/ci-uto.yaml/badge.svg)](https://github.com/astronomer/apache-airflow-provider-transfers)

The **UniversalTransferOperator** simplifies how you transfer data from a source to a destination using [Apache Airflow](https://airflow.apache.org/). Its agnostic interface eliminates the need to use specific providers or operators.

At the moment, it supports transferring data between [file locations](https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/src/universal_transfer_operator/constants.py#L26-L32) and [databases](https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/src/universal_transfer_operator/constants.py#L72-L74), as well as cross-database transfers.

This project is maintained by [Astronomer](https://astronomer.io).

## Installation

```sh
pip install apache-airflow-provider-transfers
```

## Example DAGs

See the [example_dags](./example_dags) folder for examples of how you can use the UniversalTransferOperator.

## How the UniversalTransferOperator works

![Approach](./docs/images/approach.png)

The purpose of the UniversalTransferOperator is to move data from a source dataset to a destination dataset. Your datasets can be defined as `Files` or as `Tables`. 

Instead of using different operators for each of your transfers, the UniversalTransferOperator supports three universal transfer types:

- Non-native transfers
- Native transfers
- Third-party transfers

### Non-native transfers

In a non-native transfer, you transfer data from a source to a destination through Airflow workers. Chunking is applied where possible. This method can be suitable for datasets smaller than 2GB in size. However, the performance of this method is dependent upon the worker's memory, disk, processor, and network configuration.

To use this type of transfer, you provide the UniversalTransferOperator with:

- A `task_id`.
- A `source_dataset`, defined as a `File` or `Table`.
- A `destination_dataset`, defined as a `File` or `Table`.

When you initiate the transfer, the following happens in Airflow:

- The worker retrieves the dataset in chunks from the data source.
- The worker sends data to the destination dataset.

Following is an example of non-native transfers between Google cloud storage and Sqlite:

https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/example_dags/example_universal_transfer_operator.py#L37-L41

### Native transfers

In a native transfer, Airflow relies on the mechanisms and tools offered by your data source and destination to facilitate the transfer. For example, when you use a native transfer to transfer data from object storage to a Snowflake database, Airflow calls on Snowflake to run the ``COPY INTO`` command. Another example is that when loading data from S3 to BigQuery, the UniversalTransferOperator calls on the GCP Storage Transfer Service to facilitate the data transfer.

The benefit of native transfers is that they can perform better for larger datasets (2 GB) and don't rely on the Airflow worker node hardware configuration. Airflow worker nodes are used only as orchestrators and don't perform any data operations. The speed depends exclusively on the service being used and the bandwidth between the source and destination.

When you initiate the transfer, the following happens in Airflow:

- The worker calls on the destination dataset to ingest data from the source dataset.
- The destination dataset runs the necessary steps to request and ingest data from the source dataset.

> **Note**
> The Native method implementation is in progress and will be available in future releases.

### Third-party transfers

In a third-party transfer, the UniversalTransferOperator calls on a third-party service to facilitate your data transfer, such as Fivetran.

To complete a third-party transfer, you provide the UniversalTransferOperator with:

- A source dataset, defined as a `Table` or `File`.
- A destination dataset, defined as a `Table` or `File`.
- The parameter `transfer_mode=TransferMode.THIRDPARTY`.
- `transfer_params` for the third-party tool.

When you initiate the transfer, the following happens in Airflow:

- The worker calls on the third-party tool to facilitate the data transfer.

Currently, Fivetran is the only suppported third-party tool. See [`fivetran.py`](https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/src/universal_transfer_operator/integrations/fivetran.py) for a complete list of parameters that you can set to determine how Fivetran completes the transfer.

Here is an example of how to use Fivetran for transfers:

https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/example_dags/example_dag_fivetran.py#L52-L58

## Supported technologies

- Databases supported:

    https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/src/universal_transfer_operator/constants.py#L72-L74

- File store supported:

    https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/src/universal_transfer_operator/constants.py#L26-L32

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
