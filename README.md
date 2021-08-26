[![pipeline status](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/badges/master/pipeline.svg)](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/-/commits/master)
[![coverage report](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/badges/master/coverage.svg)](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-task-interpreter/-/commits/master)

# Data Mesh Task Interpreter

Interprets YAML based task definition of the [data mesh](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-solution) as AWS Glue job.

## Format

See [model.yml](tests/interpreters/model.yml) and [product.yml](tests/interpreters/product.yml)
test examples.

## Setup development environment

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
```

## Build

    docker-compose build

## Test

    docker-compose run test

## CI/CD

See [gitlab-ci.yml](.gitlab-ci.yml).