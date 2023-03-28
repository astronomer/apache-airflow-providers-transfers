# Changelog


## 0.1.0

### Features

* Add `UniversalTransferOperator` that allows data transfers between supported source and target Datasets
  in [Apache Airflow](https://airflow.apache.org/). The supported datasets are: `File` and `Table`. It offers a
  consistent agnostic interface, simplifying the users' experience, so they do not need to use specific providers or
  operators for
  transfers. [#1492](https://github.com/astronomer/apache-airflow-provider-transfers/commit/62c895a93c746a1536557701af90195c09653948)
  , [#1619](https://github.com/astronomer/apache-airflow-provider-transfers/commit/01a8fc6356937debd0d702fdf689338a70b0f0d9)
* Add transfer support for the following databases:

| Databases       | Transfer Mode supported  | Pull Request                                                                                                             |
|-----------------|--------------------------|--------------------------------------------------------------------------------------------------------------------------|
| SQLite          | non-native, third-party  | [#1731](https://github.com/astronomer/apache-airflow-provider-transfers/commit/9a311e7cff7adef37210d1721c3c4e3f115e054b) |
| Google BigQuery | non-native, third-party  | [#1732](https://github.com/astronomer/apache-airflow-provider-transfers/commit/32afcef05e034d1ddffbf16804b2c81e0308901f) |
| Snowflake       | non-native, third-party  | [#1735](https://github.com/astronomer/apache-airflow-provider-transfers/commit/35376b3f8d4f61a7a6b169499d4ba9cdd19e8cb5) |

* Add transfer support for the following filesystems:

| File stores | Transfer Mode supported | Pull Request                                                                                                                                                                                                                                       |
|-------------|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Amazon S3   | non-native, third-party | [#1492](https://github.com/astronomer/apache-airflow-provider-transfers/commit/62c895a93c746a1536557701af90195c09653948), [#1621](https://github.com/astronomer/apache-airflow-provider-transfers/commit/01a8fc6356937debd0d702fdf689338a70b0f0d9) |
| Local       | non-native, third-party | [#1735](https://github.com/astronomer/apache-airflow-provider-transfers/commit/35376b3f8d4f61a7a6b169499d4ba9cdd19e8cb5)                                                                                                                               |
| Google GCS  | non-native, third-party | [#1492](https://github.com/astronomer/apache-airflow-provider-transfers/commit/62c895a93c746a1536557701af90195c09653948), [#1621](https://github.com/astronomer/apache-airflow-provider-transfers/commit/01a8fc6356937debd0d702fdf689338a70b0f0d9) |
| SFTP        | non-native              | [#1725](https://github.com/astronomer/apache-airflow-provider-transfers/commit/8554233807623eb17c6fde9020ed96c154c7821e), [#1866](https://github.com/astronomer/apache-airflow-provider-transfers/commit/251ed96f72f039a5f71ff907521f06084d7fa3ad) |

* Add support for cross-database transfers.  [#1732](https://github.com/astronomer/apache-airflow-provider-transfers/commit/32afcef05e034d1ddffbf16804b2c81e0308901f), [#1731](https://github.com/astronomer/apache-airflow-provider-transfers/commit/9a311e7cff7adef37210d1721c3c4e3f115e054b), [#1735](https://github.com/astronomer/apache-airflow-provider-transfers/commit/35376b3f8d4f61a7a6b169499d4ba9cdd19e8cb5)

### Docs

* Add Sphinx documentation and update the
  Readme.md [#1844](https://github.com/astronomer/apache-airflow-provider-transfers/commit/513591afb967694062097d6b36170265883b77e3)
  , [#29](https://github.com/astronomer/apache-airflow-provider-transfers/pull/29)

### Misc

* Add the GitHub workflow to build docs, run pre-commit checks and run all tests as part of
  CI. [#2](https://github.com/astronomer/apache-airflow-provider-transfers/pull/2)
  , [#30](https://github.com/astronomer/apache-airflow-provider-transfers/pull/30)
  , [#1844](https://github.com/astronomer/apache-airflow-provider-transfers/commit/513591afb967694062097d6b36170265883b77e3)
* Add Makefile to build and test
  locally. [#1492](https://github.com/astronomer/apache-airflow-provider-transfers/commit/62c895a93c746a1536557701af90195c09653948)
  , [#5](https://github.com/astronomer/apache-airflow-provider-transfers/pull/5)
