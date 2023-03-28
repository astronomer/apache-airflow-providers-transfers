<h1 align="center">
  Universal Transfer Operator
</h1>
  <h3 align="center">
transfers made easy<br><br>
</h3>


[![CI](https://github.com/astronomer/apache-airflow-provider-transfers/actions/workflows/ci-uto.yaml/badge.svg)](https://github.com/astronomer/apache-airflow-provider-transfers)

The **Universal Transfer Operator** simplifies how users transfer data from a source to a destination using [Apache Airflow](https://airflow.apache.org/). It offers a consistent agnostic interface, improving the users' experience so they do not need to use explicitly specific providers or operators.

At the moment, it supports transferring data between [file locations](https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/src/universal_transfer_operator/constants.py#L26-L32) and [databases](https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/src/universal_transfer_operator/constants.py#L72-L74) (in both directions) and cross-database transfers.

This project is maintained by [Astronomer](https://astronomer.io).

## Installation

```
pip install apache-airflow-provider-transfers
```


## Example DAGs

Checkout the [example_dags](./example_dags) folder for examples of how the UniversalTransfeOperator can be used.


## How Universal Transfer Operator Works

![Approach](./docs/images/approach.png)

With Universal Transfer Operator, users can perform data transfers using the following transfer modes:

1. Non-native
2. Native
3. Third-party


### Non-native transfer

Non-native transfers rely on transferring the data through the Airflow worker node. Chunking is applied where possible. This method can be suitable for datasets smaller than 2GB, depending on the source and target. The performance of this method is highly dependent upon the worker's memory, disk, processor and network configuration.

Internally, the steps involved are:
- Retrieve the dataset data in chunks from dataset storage to the worker node.
- Send data to the cloud dataset from the worker node.

Following is an example of non-native transfers between Google cloud storage and Sqlite:

https://github.com/astronomer/apache-airflow-provider-transfers/blob/main/example_dags/example_universal_transfer_operator.py#L37-L41


### Improving bottlenecks by using native transfer

An alternative to using the Non-native transfer method is the native method. The native transfers rely on mechanisms and tools offered by the data source or data target providers. In the case of moving from object storage to a Snowflake database, for instance, a native transfer consists in using the built-in ``COPY INTO`` command. When loading data from S3 to BigQuery, the Universal Transfer Operator uses the GCP  Storage Transfer Service.

The benefit of native transfers is that they will likely perform better for larger datasets (2 GB) and do not rely on the Airflow worker node hardware configuration. With this approach, the Airflow worker nodes are used as orchestrators and do not perform the transfer. The speed depends exclusively on the service being used and the bandwidth between the source and destination.

Steps:
- Request destination dataset to ingest data from the source dataset.
- Destination dataset requests source dataset for data.

> **_NOTE:_**
 The Native method implementation is in progress and will be available in future releases.


### Transfer using a third-party tool
The Universal Transfer Operator can also offer an interface to generic third-party services that transfer data, similar to Fivetran.

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
