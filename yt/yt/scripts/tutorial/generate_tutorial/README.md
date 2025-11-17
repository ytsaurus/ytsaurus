# YTsaurus Table Generator Documentation

This script is used to generate and populate tables in a YTsaurus cluster for tutorial or testing purposes. It performs two main steps:

1. Extracts tables from an attached archive.
2. Generates three new tables (`nomenclature`, `prices`, and `orders`) and adds associated scripts to the **Query Tracker** for tutorials.

---

## Parameters

All parameters are passed via the command line using the `--` prefix.

| Parameter | Description | Default / Required |
|----------|-------------|---------------------|
| `--proxy` | Path to the YTsaurus cluster (e.g., `ytsaurus.my_company.domain`). | **Required** |
| `--yt-directory` | Base directory in YTsaurus where tables will be created. | **Required** |
| `--create-directory` | Create the directory if it doesn't exist. | `True` |
| `--max-job-count` | Maximum number of jobs in a YTsaurus operation. | `100` |
| `--nomenclature-count` | Number of nomenclatures to generate (defines the size of the dynamic table). | `10000` |
| `--days-to-generate` | Number of days to generate data for `prices` and `orders` tables. | `7` |
| `--max-order-size` | Maximum number of items in a single order. | `200` |
| `--desired-order-size` | Desired number of rows per day in the `orders` table. | `2000` |
| `-f`, `--force` | Force execution even if the `yt-directory` is not empty. | `False` |
| `--scripts-folder` | Folder containing query template scripts. | `"scripts"` |
| `--full-wipe-annotations` | Remove all annotations from the user's Query Tracker. | `False` |

---

## Generated Tables

### 1. `nomenclature`

- **Description**: Contains product nomenclature data.
- **Schema**:
  ```yaml
  - name: id
    type: int64
    sort_order: ascending
  - name: name
    type: string
  - name: is_rx
    type: boolean
  - name: first_appeared
    type: timestamp
  - name: meta_data
    type: any
  ```
- **Table Attributes**:
  - `unique_keys: True`
  - `strict: True`
- **Size**: Determined by `--nomenclature-count`.

---

### 2. `prices`

- **Description**: Contains price history for each nomenclature item.
- **Schema**:
  ```yaml
  - name: nomenclature_id
    type: int64
  - name: date
    type: date
  - name: price
    type: double
  - name: min_price
    type: double
  ```
- **Size**: `nomenclature_count * days_to_generate`

---

### 3. `orders`

- **Description**: Contains order data partitioned by date.
- **Schema**:
  ```yaml
  - name: date
    type: date
  - name: nomenclature_id
    type: int64
  - name: order_uuid
    type: string
  - name: quantity
    type: int64
  - name: order_meta
    type: any
  ```
- **Partitioning**: Partitioned by `date` column.
- **Size**: Approximately `desired_order_size * days_to_generate`

---

## Query Tracker Integration

When the script `generate_tutorial.py` is executed, it:

- Saves the query IDs of the created tutorials.
- On subsequent runs, it tries to find and remove the previously created query IDs.
- If the old query IDs are missing (e.g., due to manual deletion), the `--full-wipe-annotations` flag can be used to fully clean up existing tutorials before adding new ones.

---

## Usage Example

```bash
python generate_tutorial.py \
  --proxy ytsaurus.my_company.domain \
  --yt-directory //home/tutorial_tables \
  --nomenclature-count 5000 \
  --days-to-generate 14 \
  --desired-order-size 3000 \
  --force
```

---

## Notes

- If the `yt-directory` is not empty and `--force` is not set, the script will exit to prevent accidental overwriting.
- The `--full-wipe-annotations` option is useful for cleaning up the Query Tracker before adding new tutorials.

---

## Testing

To test the script, you can run it with small values for `--nomenclature-count` and `--days-to-generate` to ensure correctness before scaling up.

---
