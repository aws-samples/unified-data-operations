# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from driver.aws.datalake_api import Partition
from driver.aws.resolvers import reshuffle_partitions


def test_partitions():
    ps = ['gender=Female/age=20-39', 'gender=Male/age=0-19', 'gender=Female/age=40+', 'gender=Female/age=0-19',
          'gender=Male/age=20-39', 'gender=Male/age=40+']
    partitions = list()
    for p in ps:
        po = Partition(p)
        partitions.append(po)
    for print_p in partitions:
        print(str(print_p))
    part_dict = reshuffle_partitions(prefix='s3a://glue-job-test-destination-bucket/', partitions=partitions)
    print(json.dumps(part_dict, indent=4))

    if not(len(partitions) > 0): raise AssertionError