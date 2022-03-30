# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

FROM registry.gitlab.aws.dev/aws-sa-dach/teams/dnb/docker-glue-pyspark:1.2.1

COPY . /app

RUN pip install -U -e  . \
  && pip install -r requirements-test.txt