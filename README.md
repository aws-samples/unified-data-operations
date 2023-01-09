# data product processor

The data product processor is a library for dynamically creating and executing Apache Spark Jobs based on a declarative description of a data product. A data product describes 1-to-many data sets that are to be created.

The declaration is based on YAML and covers input and output data stores as well as data structures. It can be augmented with custom, PySpark-based transformation logic.

## Installation
**Prerequisites**  
- Python 3.x
- Apache Spark 3.x

**Install with pip**
```commandline
pip install data-product-processor
```

## Getting started
### Declare a basic data product
Please see [Data product specification](docs/data-product-specification.md) for an overview on the files required to declare a data product.

### Process the data product
From folder in which the previously created file are stored, run the data-product-processor as follows:

```commandline
data-product-processor \
  --default_data_lake_bucket some-datalake-bucket \
  --aws_profile some-profile \
  --aws_region eu-central-1 \
  --local
```
This command will run Apache Spark locally (due to the --local switch) and store the output on an S3 bucket (authenticated with the AWS profile used in the parameter).

If you want to run the library from a different folder than the data product decleration, reference the latter through the additional argument `--product_path`.
```commandline
data-product-processor \
  --product_path ../path-to-some-data-product \
  --default_data_lake_bucket some-datalake-bucket \
  --aws_profile some-profile \
  --aws_region eu-central-1 \
  --local
```

## CLI Arguments
```commandline
data-product-processor --help

  --JOB_ID - the unique id of this Glue/EMR job
  --JOB_RUN_ID - the unique id of this Glue job run
  --JOB_NAME - the name of this Glue job
  --job-bookmark-option - job-bookmark-disable if you don't want bookmarking
  --TempDir - tempoarary results directory
  --product_path - the data product definition folder
  --aws_profile - the AWS profile to be used for connection
  --aws_region - the AWS region to be used
  --local - local development
  --jars - extra jars to be added to the Spark context
  --additional-python-modules - this parameter is injected by Glue, currently it is not in use
  --default_data_lake_bucket - a default bucket location (with s3a:// prefix)
```
## References
- [Data product specification](docs/data-product-specification.md)
- [Access management](docs/access-management.md)

## Tutorials
- [How to write and test custom transformation logic?](docs/how-to/transformation-logic.md)
- [How to reference custom Spark dependencies?](docs/how-to/custom-dependencies.md)
- [How to set up local development?](docs/how-to/local-development.md)