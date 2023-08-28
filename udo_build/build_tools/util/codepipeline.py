# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import tempfile
from typing import Dict
import zipfile
import boto3


TEMP_PREFIX = ''

s3 = boto3.client('s3')
sts = boto3.client('sts')
code_pipeline = boto3.client('codepipeline')


def download_sources(artifact_bucket: str, artifact_key: str, source_path: str):
    tmp_file = tempfile.NamedTemporaryFile(
        prefix=TEMP_PREFIX, suffix='.zip', delete=True)

    with open(tmp_file.name, 'wb') as f:
        print(f'Downloading {artifact_key} from {artifact_bucket}')
        s3.download_fileobj(Bucket=artifact_bucket, Key=artifact_key, Fileobj=f, ExtraArgs={
                            'ExpectedBucketOwner': sts.get_caller_identity().get('Account')})

    with zipfile.ZipFile(tmp_file.name, 'r') as zip_ref:
        zip_ref.extractall(source_path)


def extract_job_metadata(event):
    job_id = event['CodePipeline.job']['id']
    job_data = event['CodePipeline.job']['data']
    artifacts = job_data['inputArtifacts'][0]
    bucket = artifacts['location']['s3Location']['bucketName']
    key = artifacts['location']['s3Location']['objectKey']

    return job_id, bucket, key


def put_job_success(job_id, message: str, payload: Dict[str, str] = None):
    print('Putting job success: ' + message)

    if payload is not None:
        code_pipeline.put_job_success_result(
            jobId=job_id, executionDetails={'summary': message}, outputVariables=payload)
    else:
        code_pipeline.put_job_success_result(
            jobId=job_id, executionDetails={'summary': message})


def put_job_failure(job_id, message):
    print('Putting job failure: ' + message)
    code_pipeline.put_job_failure_result(jobId=job_id, failureDetails={
                                         'message': message, 'type': 'JobFailed'})
