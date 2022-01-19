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
    --default_data_lake_bucket - a default bucket location (with s3a:// prefix)


### Integration into Glue:

- Script location - the location of the main.py
- Python lib path - the location of the wheel package of this project (eg. data-product-processor-version-py3.whl)
- Other lib path - the reference to the zipped data product definition (eg. s3://data-products/product_a_customers-1.0.0.zip)
- Temporary directory - an arbitrary S3 bucket for temporary files
- Job parameters - Glue job parameters, including the dependencies of this libraryr (eg. --additional-python-modules pydantic,quinn,pyyaml,mypy-boto3-glue)


## Access Management
The access management concept is based on two separate mechanisms:
1. Tagging all produced data to control which groups should have access to data
    - This is controlled by the data producers, via the model YAML files
    - The data producers know their data best and can control which groups should have access (does it contain PII? Is it intended to be public or private, etc.)
    - the platform takes over this process and tags all produced data files based on the configuration in the YAML files
2. Managing groups of people (or services) who are allows to join those groups to gain access to the data.
    - IAM policies, which provide access to S3 data files which have been tagged as mentioned before have to be created manually (as of now)
      - please see `access/policie.json` as an example for providing access to files which have specific tags defined.
    - those policies can be attached to IAM groups to provide access to one or multiple combinations of access control tags
    - IAM users then can join and leave groups to gain access to the data, matching the policies assigned to those groups

### Technical Implementation
The S3 writer automatically applies the following tags to all data files written out to S3:
- tags defined in the `model.yml` under `models.<model>.tags` are added to all output data files in the dataset's S3 folder as is, using the tag's name and value without modification.
- tags defined in the `model.yml` under `models.<model>.access` are added to all output data files in the dataset's S3 folder as well, but the tag names are prefixed with `access_`, to have a clear distinction between access control tags and custom tags, every data producer can define without limitation.
  - Example: the access tag `confidentiality` with value `private` will be assigned as S3 tag `access_confidentiality` with value `private`. 


### Limitations
Based on the metadata defined in the model's YAML files, the interpreter will set tags to all files written out to 
Amazon S3. Currently, only files written to S3 are supported to be tagged automatically. Access policies and group have to be created by the user manually and IAM users have to be assigned to IAM groups manually to actually manage access to the data.  

## Architecture

![architectural diagram](./docs/data-product-processor-arch.png)

#Prioritised Todos:

- cleanup the models
- cover the current feature set with unit tests
- cover the current feature set with integration test
- model location should be taken from the data catalog
- figure out what happens when you have no access

