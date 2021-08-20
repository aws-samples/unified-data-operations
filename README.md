[![pipeline status](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/job-interpreter/badges/master/pipeline.svg)](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/job-interpreter/-/commits/master)
[![coverage report](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/job-interpreter/badges/master/coverage.svg)](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/job-interpreter/-/commits/master)

# Job Interpreter

Interprets YAML based task definition of the [data mesh](https://gitlab.aws.dev/aws-sa-dach/teams/dnb/data-mesh-solution) as AWS Glue job.

## Format

See [model.yml](job_interpreter/tests/interpreters/model.yml) and [product.yml](job_interpreter/tests/interpreters/product.yml)
test examples.

## Build

    docker-compose build

## Test

    docker-compose run test

## CI/CD

See [gitlab-ci.yml](.gitlab-ci.yml).