[![pipeline status](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/badges/master/pipeline.svg)](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/-/commits/master)
[![coverage report](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/badges/master/coverage.svg)](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/-/commits/master)

# Data Mesh Task Interpreter

Interprets YAML based task definition of
the [data mesh](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-solution) as AWS Glue job.

## Format

See [model.yml](deprecated_ts/interpreters/model.yml) and [product.yml](deprecated_ts/interpreters/product.yml)
test examples.

# Setup real-local development environment

## Install environment on OSX

Everything will be installed in virtual environment in your local project folder.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U -e .
pip install -r requirements-test.txt
```

Don't forget to switch the new virtual environment in your IDE too.

Also: make sure Java is installed. On OSX:

```
brew tap homebrew/cask-versions
brew update
brew tap  homebrew/cask
brew tap adoptopenjdk/openjdk
brew install --cask adoptopenjdk11
brew install maven
```

Install spark dependencies:

```
mkdir spark_deps
cd spark_deps
wget https://jdbc.postgresql.org/download/postgresql-42.2.23.jar
```

Install the AWS dependencies for hadoop:

1. check the current version of hadoop: ```ll -al .venv/lib/python3.9/site-packages/pyspark/jars |grep hadoop```
2. create a POM file in the spark_deps folder (make sure the version field matches the current hadoop version):

```
<project>
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.mycompany.app</groupId>
  <artifactId>my-app</artifactId>
  <version>1</version>
    <dependencies>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-aws</artifactId>
            <version>3.2.0</version>
        </dependency>
    </dependencies>
</project>
```

Download the dependencies:

```
mkdir spark_deps
mvn --batch-mode -f ./pom.xml -DoutputDirectory=./jars dependency:copy-dependencies
mv jars/* .
```

Set the following parameters onto the exection context:

```commandline
--JOB_NAME "TEST" --product_path /tests/assets/integration --aws_profile finn --aws_region eu-central-1 --local --jars "aws-java-sdk-bundle-1.11.375.jar,hadoop-aws-3.2.0.jar"
```

Alternatively you can run the whole solution from the command line:

```commandline
SPARK_HOME="/Users/csatam/Code/data-mesh-task-interpreter/.venv/lib/python3.9/site-packages/pyspark"
export SPARK_HOME
python main.py --JOB_NAME "TEST" --product_path /tests/assets/integration --aws_profile finn --aws_region eu-central-1 --local --jars "aws-java-sdk-bundle-1.11.375.jar,hadoop-aws-3.2.0.jar"
```

Run the tests from command line (while the virtual environment is activated):

```commandline
pytest
```

## CI/CD

See [gitlab-ci.yml](.gitlab-ci.yml).

## Setup local Spark playground

Make sure that you execute these commands in a virtual environment (see the top of this document for instructions):

```commandline
pip install ipython
iptyhon
```

Type the following in the system:

```python
import findspark
findspark.init()
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

sys.path.insert(0, f"{os.environ['SPARK_HOME']}/python/lib/pyspark.zip")
sys.path.insert(0, f"{os.environ['SPARK_HOME']}/python/lib/py4j-0.10.9-src.zip")

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