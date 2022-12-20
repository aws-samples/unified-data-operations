# Access management

The access management concept is based on two separate mechanisms:

1. Tagging all produced data to control which groups should have access to data
    - This is controlled by the data producers, via the model YAML files
    - The data producers know their data best and can control which groups should have access (does it contain PII? Is
      it intended to be public or private, etc.)
    - the platform takes over this process and tags all produced data files based on the configuration in the YAML files
2. Managing groups of people (or services) who are allows to join those groups to gain access to the data.
    - IAM policies, which provide access to S3 data files which have been tagged as mentioned before have to be created
      manually (as of now)
        - please see `access/policy_template.json` as an example for providing access to files which have specific tags
          defined.
    - those policies can be attached to IAM groups to provide access to one or multiple combinations of access control
      tags
    - IAM users then can join and leave groups to gain access to the data, matching the policies assigned to those
      groups

## Technical implementation

The S3 writer automatically applies the following tags to all data files written out to S3:

- tags defined in the `model.yml` under `models.<model>.tags` are added to all output data files in the dataset's S3
  folder as is, using the tag's name and value without modification.
- tags defined in the `model.yml` under `models.<model>.access` are added to all output data files in the dataset's S3
  folder as well, but the tag names are prefixed with `access_`, to have a clear distinction between access control tags
  and custom tags, every data producer can define without limitation.
    - Example: the access tag `confidentiality` with value `private` will be assigned as S3 tag `access_confidentiality`
      with value `private`.

## Limitations

Based on the metadata defined in the model's YAML files, the processor will set S3 tags to all files written out to
Amazon S3, found in the data dataset's "folder" (meaning all files, with the
prefix `<data product id>/<output dataset id>/`)

Currently, only files written to S3 are supported to be tagged automatically.

Access policies and group have to be created by the user manually and IAM users have to be assigned to IAM groups
manually to actually manage access to the data.