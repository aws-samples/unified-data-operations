# Setup of local development environment

> **Note**: The subsequent steps assume an installation on ___MacOS/OSX___

## 1) Installation of tools and dependencies

### Python
Everything will be installed in virtual environment in your local project folder.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-test.txt
```

### Java

Install openjdk and maven.

```bash
brew tap homebrew/cask-versions
brew update
brew tap  homebrew/cask
brew tap adoptopenjdk/openjdk
brew install --cask adoptopenjdk11
brew install maven
```

### Apache Spark

Install spark dependencies:

```bash
mkdir spark_deps
cd spark_deps
wget https://jdbc.postgresql.org/download/postgresql-42.2.23.jar
```

Install the AWS dependencies for Apache Hadoop:

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

Then, run:

```bash
mvn --batch-mode -f ./pom.xml -DoutputDirectory=./jars dependency:copy-dependencies
mv jars/* .
```

## 2) Test the installation

> **Note:** Don't forget to switch the new virtual environment in your IDE too.

### Package creation

Test whether the python package (wheel) can be build through which the data-product-processor is distributed.

```commandline
pip install -U pip wheel setuptools
python3 setup.py bdist_wheel
```

### Local invocation of data-product-processor

To test if the data-product-processor can be executed correctly, follow the subsequent steps.

Alternatively you can run the whole solution from the command line:

```commandline
data-product-processor \
    --JOB_NAME "TEST" \
    --product_path /tests/assets/integration \
    --default_data_lake_bucket <SOME_DATA_LAKE_BUCKEY> \
    --aws_profile <your-aws-account-profile> \
    --aws_region <your-region>
```

Optionally you might need to export Spark Home if the Spark environment is not found in your installation.

```commandline
export SPARK_HOME="$(pwd)/.venv/lib/python3.9/site-packages/pyspark"
```

Run the tests from command line (while the virtual environment is activated):

```commandline
pytest
```

# Troubleshooting / common errors

## py4j

```
py4j.protocol.Py4JError: org.apache.spark.api.python.PythonUtils.getPythonAuthSocketTimeout does not exist in the JVM
```
Resolve through:
```commandline
export PYTHONPATH="${SPARK_HOME}/python;${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip;${PYTHONPATH}"
```

## Sfl4j not found

```commandline
[NOT FOUND  ] org.slf4j#slf4j-api;1.7.5!slf4j-api.jar
```
**Solution**
Remove dir in .ivy2/cache, ivy2/jars and .m2/repository