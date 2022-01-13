# Data Product Processor

Library for dynamically creating and executing ETL Jobs based on a declarative description of a data product.

The processor will is executed as Spark job based on a data product definitions (Yaml files and custom spark code),
generating one or more output data assets.  

## Why is this an interesting project?


Command line parameters:

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
    --output_bucket - default data lake S3 bucket name (default output for data product datasets)

### Integration into Glue:

- Script location - the location of the main.py
- Python lib path - the location of the wheel package of this project (eg. data-product-processor-version-py3.whl)
- Other lib path - the reference to the zipped data product definition (eg. s3://data-products/product_a_customers-1.0.0.zip)
- Temporary directory - an arbitrary S3 bucket for temporary files
- Job parameters - Glue job parameters, including the dependencies of this libraryr (eg. --additional-python-modules pydantic,quinn,pyyaml,mypy-boto3-glue)


## Architecture

![architectural diagram](./docs/data-product-processor-arch.png)

#Prioritised Todos:

- cleanup the models
- cover the current feature set with unit tests
- cover the current feature set with integration test
- model location should be taken from the data catalog
- figure out what happens when you have no access

