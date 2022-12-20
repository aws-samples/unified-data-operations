# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from typing import List, Dict

from driver.aws import providers

logger = logging.getLogger(__name__)


class Partition:
    def __init__(self, path_key):
        path_iterator = iter(os.path.split(path_key))
        segments = next(pe for pe in path_iterator if pe).split("=")
        self.name = segments[0]
        self.value = segments[1]
        sub_partition = next(path_iterator, None)
        self.subpartitions = list()
        if sub_partition:
            o = Partition(sub_partition)
            self.subpartitions.append(o)

    def get_partition_chain(
        self, prefix: str, parent_key: str = None, parent_value: str = None
    ) -> List[Dict[str, str]]:
        pchain = list()
        prepped_prefix = os.path.join(prefix, f"{self.name}={self.value}")
        pkeys = list()
        pkey_values = list()
        if parent_key and parent_value:
            pkeys.append(parent_key)
            pkey_values.append(parent_value)
        if len(self.subpartitions) > 0:
            for sp in self.subpartitions:
                pchain.extend(
                    sp.get_partition_chain(
                        prepped_prefix, parent_key=self.name, parent_value=self.value
                    )
                )
        else:
            pkeys.append(self.name)
            pkey_values.append(self.value)
            pchain.append(
                {"keys": pkeys, "values": pkey_values, "location": prepped_prefix}
            )
        return pchain


def read_partitions(bucket: str, container_folder: str = None):
    s3 = providers.get_s3()
    rsp = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=os.path.join(container_folder, ""),
        ExpectedBucketOwner=providers.get_aws_account_id(),
    )
    keys = set(os.path.dirname(k.get("Key")) for k in rsp.get("Contents"))
    prefix = rsp.get("Prefix")
    partition_keys = [p[len(prefix) :] for p in keys if p != prefix.rstrip("/")]
    partitions = list()
    for p in partition_keys:
        partitions.append(Partition(p))
    return partitions


def tag_files(bucket: str, prefix: str, tags: dict):
    s3 = providers.get_s3()

    tags_s3 = []
    for tag_name in tags.keys():
        tags_s3.append({"Key": tag_name, "Value": str(tags[tag_name])})

    for key in find_files(bucket, prefix):
        s3.put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging={"TagSet": tags_s3},
            ExpectedBucketOwner=providers.get_aws_account_id(),
        )


def find_files(bucket: str, prefix: str) -> List[str]:
    s3 = providers.get_s3()
    files = s3.list_objects_v2(
        Bucket=bucket, Prefix=prefix, ExpectedBucketOwner=providers.get_aws_account_id()
    )
    return [f["Key"] for f in files["Contents"]]
