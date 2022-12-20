# Data product specification

Each data product consists of 
- [product.yml](#product.yml)
- [model.yml](#model.yml)

Optionally, there can be ```tasks``` folder which can contain custom transformation logic in form of python-files.

```
.
├── product.yaml      --> Describes overall data product and the aggregation pipeline
├── model.yaml        --> Describes the datasets that are being consumed and produced within the data product
└── tasks             --> Optionally stores custom transformation logic
    ├── __init__.py
    ├── task1.py
    └── task2.py
```

## product.yaml

The ```product.yml``` defines the transformation pipeline, consisting one or more tasks, each having one or more inputs
and outputs.

```yaml
---
schema_version: 1.rc-1
product:
  id: customers_personal_data
  description: All customer data
  version: "1.0.0"
  owner: jane.doe@acme.com
  engine: glue | emr | dbt
  pipeline:
    schedule: "0 */1 * * *"
    tasks:
      - id: extract customer data
        logic:
          module: tasks.custom_business_logic
          parameters:
            create_timestamp: true
        inputs:
          - connection: test_db_connection
            table: sportstickets.person_relevant
        outputs:
          - model: person_pii
          - model: person_pub
...
```
### Schema
| Attribute | Type | Optional | Description |
| - | - | - | - |
|schema_version|str||the schema version against this yaml file will be validated|
|product|dict||overall data product|
|product.id|str||the identification of this product, that is unique across the whole organisation (or at least across one data platform)|
|product.name|str|x|Human-readable name|
|product.description|str||The detailed description of the data-product.|
|product.version|str||The version of this data-product.|
|product.owner|str||the e-mail address of the data product owner who will get notified upon failures|
|product.pipeline|dict||Contains scheduling information and the list of transformation tasks|
|pipeline.schedule|str||The cron expression for scheduling the trigger of this job|
|pipeline.tasks|list||the list of tasks to be executed to produce the final version of this data-product|
|task.id|str||the identification of this task, that is unique within the same data product|
|task.logic|dict|x|the custom transformation logic of this task. If not specified, the processor will fall back to built-in ingest task with pass-through logic (that brings no additional transformation to the ones defined in the model.|
|task.logic.module|str||defines the module that contains the custom aggregation logic and has an execute method|
|task.logic.parameters|dict||custom parameters used by the aggregation logic|
|task.inputs|dict||list of IO handlers (be it connection to a database or a dataset in the data-lake represented by a model)|
|task.outputs|dict||list of IO handlers (be it connection to a database or a dataset in the data-lake represented by a model)|

**Default ingestion logic**  
The ``logic`` keyword and all of its parameters can be omitted. In that case the ```builtin.ingest``` logic is being
used by default. This is useful when you want to ingest tables, but you don't need to make any custom transformation
beyond the once provided on the model object.

The builtin ```ingest``` module can also take parameters, such as create_timestamp (false by default). If this is
specified a new column is added with the ingestion timestamp.

```yaml
tasks:
  - id: process_some_files
    logic:
      module: builtin.ingest
      parameters:
        create_timestamp: true
```
### Inputs
**1) Connection**  
Connection to an external resources, such as RDBMS (eg. PostgreSQL) of which connection details are stored in Glue as a managed connection.
```yaml
inputs:
  - connection: sportstickets
      table: sportstickets.person
      model: person_pii
```
| Attribute | Type | Optional | Description |
| - | - | - | - |
|inputs.connection|str||ID of connection stored in Glue Data Catalog.|
|inputs.connection.table|str||Table qualified with schema from which to read: \<schema\>.\<table\>|
|inputs.connection.model|str|x|Logical name under which to make the raw input addressable. For instance, in custom transformation logic. Defaults to table name.|
**2) Model**
```yaml
inputs:
  - model: facilities.locations
```
| Attribute | Type | Optional | Description |
| - | - | - | - |
|inputs.model|str||ID of a model provided through a further data product: \<data-product-id\>.\<model-id\>|
**3) File**
```yaml
inputs:
  - file: s3://my-bucket/crm-customers.parquet
    model: customers
```
| Attribute | Type | Optional | Description |
| - | - | - | - |
|inputs.file|str||Absolute URI to a S3-based file|
|inputs.model|str|x|Logical name under which to make the raw input addressable. For instance, in custom transformation logic. Defaults to model file name without extension.|
### Outputs
**1) Model**
```yaml
outputs:
  - model: person_pii
  - model: person_pub
```

### Defaults

Some default configuration options can be added to the data product. These can be used, when there's no other value
overriding them on the Models (read later). The following defaults are supported:

```yaml
  defaults:
    storage:
      location: some_bucket/some_folder
      options:
        compression: uncompressed | snappy | gzip | lzo | brotli | lz4
        coalesce: 2
```

Check the meaning of the various parameters on the ```model.storage``` property down bellow;

## model.yml

The ```model.yaml``` enlists the details of all input (optional) and output (mandatory) models. The model contains
information about the 1.) schema types and validation, 2.) storage location and access controls and 3.) directives for
transformation.

The core structure is the following:

```yaml
schema_version: 1.rc-1
models:
  - id: person
    version: "1.0"
    description: "Person Model"
    extends: other_model
    validation: strict | lazy
    xtra_columns: raze | ignore
    columns:
      - ...
      - ...
      - ...
    meta:
      contains_pii: true
      steward: jane.doe@acme.com
    storage:
      location: '/some_data_lake/some_folder'
      type: lake
      format: parquet
    tags:
      cost_center: 123455
      use_case: Customer 360
    access:
      domain: customer_support
      confidentiality: private
```

### Schema
| Attribute | Type | Optional | Description |
| - | - | - | - |
|schema_version|str||the schema version against this yaml file will be validated|
|models|dict||contains a list of input and output models|
|model.id|str||an identification string that is unique within this dataproduct. In most places the model (dataset) will be referenced as ```product_id.model_id```; This documentation used the ```model``` and ```dataset```  keywords somewhat interchangebly. Model refers mostly to the metadata that describes a dataset.|
|model.version|str||defines the version of this model. Once the model was published, the version should be changed at each schema change|
|model.description|str||human readable description of a model that gets registered in the data catalog as well|
|model.name|str|x|human readable name of the model, similar in use to the id|
|model.extends|str|x| it is a directive that helps to inherit column definitions from another model, therefore it makes it easier to define derived models, that only override or extend one or more columns|
|model.validation|str|x|the default value is lazy validation (even if the keyword "validation" is omitted). Lazy validation will only check the type of the columns that are stated in the model, but will accept extra columns (that are available on the Data Frame but not defined in the model). A strict validation will raise an exception if the Data Frame has more columns than stated in the Model|
|model.xtra_columns|str|x| if omitted, it will ignore the existence of extra columns on the Data Frame. If defined with the value **raze**, it will remove any columns from the Data Frame that are not specified on the Model|
|model.columns|dict||a list of columns, stating data types, constraint validators and column transformers|
|model.meta|dict|x|a list of key-value pairs that are added to the data catalog as meta data|
|model.storage|dict|x|a definition about the location (and compression, file format, etc.) of the output dataset|
|model.tags|dict|x|a set of key-value pairs that are assigned to the output data-set and can help later with the governance (e.g cost control)|
|model.access|dict||provides a list of key-value tags that will govern access to the output data-sets|

#### Storage

The storage attribute defines technical properties of the output data set on the target data store.
```yaml
schema_version: 1.rc-1
models:
  - id: person
    ...
    storage:
      type: lake | file
      location: 'some_bucket/some_file'
      format: parquet | csv
      options:
        skip_first_row: true | false
        partition_by:
         - gender
         - age
      compression: uncompressed | snappy | gzip | lzo | brotli | lz4
      coalesce: 2
      bucketed_at: 512M <-- not yet supported
```
| Attribute | Type | Optional | Description |
| - | - | - | - |
|type|enum|x|Options: `lake`,`file` <br>The default type is `lake` as in Data Lake. This will write the dataset onto S3 and register the dataset with the Glue Data Catalog|
|location|str|x|location on which to write out the model. <br> the output file location is first looked up on the model's ```storage.location``` property. If not found, it will look on the product's ```defaults.storage``` options, and ultimately will check the ```--default_data_lake_bucket``` command line parameter.|
|format|enum|x|Options: `parquet`, `csv`<br>the file format used to writing out data|
|options|dict|x|a set of options specified details for the write-out|
|options.skip_first_row|bool|x|will not write out the first row|
|options.partition_by|list|x| list of columns used for partitioning the parquet file|
|compression|enum|x|Options: `uncompressed`,`snappy`,`gzip`,`lzo`,`brotli`,`lz4`<br>Default: `snappy`<br>compression algorithm for writing out model|
|coalesce|int|x|Default: 2<br> the number of file for each partition (the more you have, the more parallel reads are possible but not recommended for small files)|
|bucketed_at|str|x| defines the file-chunk size to be written |

#### Columns

Every model defines one or more columns that are used for schema and constraint validation, and for built-in
transformations.

```yaml
columns:
  - id: id
    type: integer
    constraints:
      - type: unique
  - id: full_name
    type: string
    transform:
      - type: encrypt
  - id: gender
    type: string
    constraints:
      - type: not_null
      - type: regexp
        options:
          value: '^Male|Female$'
```
| Attribute | Type | Optional | Description |
| - | - | - | - |
|id|str||the id of the column which is unique within the model (and coincides with the column name)|
|type|enum||Options: `str`, `int`, `bool` ..<br>the data type of the column (e.g. string, integer, boolean, etc.)|
|constraints|dict|x|he list of constraints that apply for this column|
|constraints.type|enum||Options: see contraint types further below<br>the constraint with which the column is validated|
|transform|dict|x|the list of transforms that apply to this column|
|transform.type|enum||Options: see transform types further below<br>defines the transformation applied on the column|

#### Constraint Validators

##### Unique Constraint Validator

This constraint takes no parameters and assures that the columns has distinct values, that are unique within the column.

Example:

```yaml
- id: id
  type: integer
  constraints:
    - type: unique
```

##### Regexp Constraint Validator

This constraint assures that all values in a column match a particular regular expression, provided within
the ```value``` keyword. It can be used for e-mail validation or strict option list validation and alike.

Example:

```yaml
- id: gender
  type: string
  constraints:
    - type: not_null
    - type: regexp
      options:
        value: '^Male|Female$'
```

##### Past Constraint Validator

It applies to date/timestamp columns, an assures that all values are in the past. If the optional ```threshold``` value
is not defined, it will compare every row against the ```datetime.now()```. When the ```threshold``` option is defined,
column values are compared against ```now()+timedelta(threshold)```, so values that are in the near future (defined by
the threshold) are accepted too.

Example:

```yaml
- id: transaction_date
  type: timestamp
  constraints:
    - type: past
      options:
        threshold: 10
        time_unit: minutes
```

Accepted time units: seconds, minutes, hours, days, weeks;

##### Future Constraint Validator

Similarly to the Pas validator, it checks that all the values in a date/timestamp column are in the future. If the
optional ```threshold``` value is not defined, it will compare every row against the ```datetime.now()```. When
the ```threshold``` option is defined, column values are compared against ```now()-timedelta(threshold)```, so values
that are in the near past (defined by the threshold) are accepted too.

```yaml
- id: transaction_date
  type: timestamp
  constraints:
    - type: future
      options:
        threshold: 60
        time_unit: seconds
```

##### Freshness Constraint Validator

It checks that the values in a date/timestamp column are not older than the specified threshold. Additionally, it can run
the same checks on groups defined by another column.

Example:

```yaml
- id: transaction_date
  type: timestamp
  constraints:
    - type: freshness
      options:
        threshold: 1
        time_unit: days
        group_by: geo
```

The example above checks that the latest value in the ```transaction_date``` column for each distinct ```geo``` is not
older than 1 day. This can be useful when fresh data is missing only from one or more geographical areas (geos).

#### Built-in transformers

Transformers can run as pre- or post-processors, before or after the custom aggregation logic. Transformers are attached
to columns in the model schema definition and are running before the schema validation. Multiple transformers can be
combined for one column.

##### Anonymize Transformer

Will hash the column values, so they can be used for uniqueness validation, or for grouping/partitioning purposes, but
will not identify a person anymore (therefore it is mostly applied for PII data for compliance with data privacy
regulations).

Example:

```yaml
- id: ip_address
  transform:
    - type: anonymize
```

##### Encrypt Transformer

Similar to the ```anonymize```, with the difference of using an sha256 encryption algorithm for envelope encryption.

Example:

```yaml
- id: full_name
    type: string
    transform:
      - type: encrypt
```

##### Skip Transformer

Will simply cause the removal of the column where it is applied. Example:

```yaml
- id: last_name
  type: string
  transform:
    - type: skip
```

##### Bucketize Transformer

It is part of the anonymization toolkit, and it is used to restructure values in buckets.

Example:

```yaml
- id: age
  type: integer
  transform:
    - type: bucketize
      options:
        buckets:
          0: 0-19
          20: 20-39
          40: 40+
```

Here the exact age of a person (which can be considered as PII data) is replaced with age ranges (age buckets), which
will allow the further segmentation of customers, but will remove from the PII nature of the data.