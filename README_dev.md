[![pipeline status](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/badges/master/pipeline.svg)](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/-/commits/master)
[![coverage report](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/badges/master/coverage.svg)](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/-/commits/master)

# Data Mesh Task Interpreter

Interprets YAML based task definition of
the [data mesh](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-solution) as AWS Glue job.

## Format

See [model.yml](deprecated_ts/interpreters/model.yml) and [product.yml](deprecated_ts/interpreters/product.yml)
test examples.

# Setup real-local development environment

## Install development environment on OSX

Everything will be installed in virtual environment in your local project folder.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-test.txt
```

Don't forget to switch the new virtual environment in your IDE too.

Building the wheel package:

```commandline
pip install -U pip wheel setuptools
python3 setup.py bdist_wheel
```
As a result you should see 

Also: make sure Java is installed. On OSX:

```bash
brew tap homebrew/cask-versions
brew update
brew tap  homebrew/cask
brew tap adoptopenjdk/openjdk
brew install --cask adoptopenjdk11
brew install maven
```

Install spark dependencies:

```bash
mkdir spark_deps
cd spark_deps
wget https://jdbc.postgresql.org/download/postgresql-42.2.23.jar
```

Install the AWS dependencies for hadoop:

1. check the current version of hadoop: ```ll -al .venv/lib/python3.9/site-packages/pyspark/jars |grep hadoop```
2. create a POM file in the spark_deps folder (make sure the version field matches the current hadoop version):

```xml
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.mycompany.app</groupId>
  <artifactId>my-app</artifactId>
  <version>1</version>
    <dependencies>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-aws</artifactId>
            <version>3.3.1</version>
        </dependency>
    </dependencies>
</project>
```

Download the dependencies:

```bash
mvn --batch-mode -f ./pom.xml -DoutputDirectory=./jars dependency:copy-dependencies
mv jars/* .
```

Set the following parameters onto the execution context in your IDE:

```commandline
--JOB_NAME "TEST" --product_path /tests/assets/integration --default_data_lake_bucket <SOME_DATA_LAKE_BUCKEY> --aws_profile <your-aws-account-profile> --aws_region <your-region> --local --jars "aws-java-sdk-bundle-1.11.375.jar,hadoop-aws-3.2.0.jar"
```

Alternatively you can run the whole solution from the command line:
```commandline
python main.py --JOB_NAME "TEST" --product_path /tests/assets/integration --default_data_lake_bucket <SOME_DATA_LAKE_BUCKEY> --aws_profile <your-aws-account-profile> --aws_region <your-region> --local --jars "aws-java-sdk-bundle-1.11.375.jar,hadoop-aws-3.2.0.jar"
```

Optionally you might need to export Spark Home if the Spark environment is not found in your installation.

```commandline
export SPARK_HOME="$(pwd)/.venv/lib/python3.9/site-packages/pyspark"
```

Run the tests from command line (while the virtual environment is activated):

```commandline
pytest
```

## Troubleshooting

On error:
```
py4j.protocol.Py4JError: org.apache.spark.api.python.PythonUtils.getPythonAuthSocketTimeout does not exist in the JVM
```

Type this:
```commandline
export PYTHONPATH="${SPARK_HOME}/python;${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip;${PYTHONPATH}"
```



## CI/CD

The Gitlab based CI/CD pipeline can be dound at: [gitlab-ci.yml](.gitlab-ci.yml).

## Setup local Spark playground

This is a description of an optional and somewhat unrelated step, for setting up an interactive development environment that helps to experiment with Spark concepts in a local environment.

Make sure that you execute these commands in a virtual environment (see the top of this document for instructions):

```commandline
pip install ptpython
ptpython
```

Type the following in the ptpython console:

[optional] only if you encounter errors with the larger snippet bellow:
```python
import findspark
findspark.init()
```
Interactive development:
```python
import sys
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
    LongType,
    DoubleType
)

os.environ["AWS_PROFILE"] = 'finn'
conf = SparkConf()
conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
conf.set("spark.jars", './spark_deps/postgresql-42.2.23.jar')

spark = SparkSession.builder.appName('repl') \
        .config(conf=conf) \
        .getOrCreate()

movie_schema = StructType([
    StructField('movieId', IntegerType(), True),
    StructField('title', StringType(), True),
    StructField('genres', StringType(), True)
])

df = spark.createDataFrame([(1, 'Jumanji(1995)', 'Adventure | Children | Fantasy'),
                                          (2, 'Heat (1995)', 'Action|Crime|Thriller')],
                                         movie_schema)
```
Get catalog information:
```python
import boto3, json
session = boto3.Session(profile_name='finn', region_name='eu-central-1')
glue = session.client('glue')
s = json.dumps(glue.get_table(DatabaseName='test_db', Name='person'), indent=4, default=str)
print(s)
```