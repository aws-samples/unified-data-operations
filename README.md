[![pipeline status](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/badges/master/pipeline.svg)](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/-/commits/master)
[![coverage report](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/badges/master/coverage.svg)](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/-/commits/master)

# Data Mesh Task Interpreter

Interprets YAML based task definition of the [data mesh](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-solution) as AWS Glue job.

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
2. create a POM file in the spark_deps folder:
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



## CI/CD

See [gitlab-ci.yml](.gitlab-ci.yml).