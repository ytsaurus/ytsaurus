### Tool for importing table.

This tool downloads all the chosen data from external storage and converts it into a YTsaurus table.

Supported data formats:
- [parquet](https://parquet.apache.org/)

Supported external storages:
- S3 storage
- [huggingface](https://huggingface.co/)

{% note info "Note" %}

Huggingface Datasets Server automatically converts every dataset to the Parquet format, so it is possible to import any dataset from huggingface.

{% endnote %}

How to use:

{% list tabs %}
- S3

   ```bash
   ./import_table s3 --proxy <cluster-name> --url https://s3.yandexcloud.net --region ru-central1 --bucket bucket_name --prefix common_parquet_files_prefix --output //tmp/result_parquet_table
   ```

- Huggingface

   ```bash
    ./import_table huggingface --proxy <cluster-name> --dataset Deysi/spanish-chinese --split train --output //tmp/result_parquet_table
   ```
{% endlist %}

**Common Parameters**

- **output** — path to output table
- **proxy** — cluster name
- **format** — format name, `parquet` by default.

**S3 Parameters**

- **url** — endpoint url of s3 storage, for example: https://s3.yandexcloud.net or https://s3-us-west-2.amazonaws.com
- **region** — region of S3, optional parameter
- **bucket** — bucket name
- **prefix** — common prefix of files that represent a single table, if this option is not set, all files from bucket will be imported

S3 access keys should be placed in environment variables 'ACCESS_KEY_ID' and 'SECRET_ACCESS_KEY'

**Huggingface Parameters**

- **dataset** — name of huggingface [dataset](https://huggingface.co/docs/datasets-server/en/index)
- **config** — [configuration](https://huggingface.co/docs/datasets-server/en/configs_and_splits#configurations) of dataset, `default` by default
- **split** — specific subsets of data reserved for specific needs ([doc](https://huggingface.co/docs/datasets-server/en/configs_and_splits#splits))
    - `train` — data used to train a model
    - `test` — data reserved for evaluation only
    - `validation` — data reserved for evaluation and improving model hyperparameters

Huggingface token should be placed in the environment variable 'HUGGINGFACE_TOKEN'