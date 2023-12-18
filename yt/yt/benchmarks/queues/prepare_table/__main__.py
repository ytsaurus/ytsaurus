import argparse
import numpy.random as random
import string

from math import floor, ceil

import yt.wrapper as yt

KB = 1024
MB = 1024 * KB
GB = 1024 * MB


class RandomStringGenerator:
    def __init__(self):
        self.data = ""
        self.ptr = 0

    def generate(self, n):
        if self.ptr + n > len(self.data):
            self._refill()
        res = self.data[self.ptr:self.ptr + n]
        self.ptr += n
        return res

    def _refill(self):
        self.data = self.data[self.ptr:] + "".join(random.choice(tuple(string.ascii_lowercase), 100000))
        self.ptr = 0


class StaticTableWriteMapper:
    rsg = RandomStringGenerator()
    rng = random.default_rng()

    def __init__(self, desired_data_weight_per_job, desired_row_size, column_value_size_jitter, columns):
        self.desired_data_weight_per_job = desired_data_weight_per_job
        self.desired_row_size = desired_row_size
        self.column_value_size_jitter = column_value_size_jitter
        self.columns = columns

        desired_column_value_size = self.desired_row_size / len(self.columns)

        self.value_size_min = max(floor((1 - self.column_value_size_jitter) * desired_column_value_size), 1)
        self.value_size_max = max(ceil((1 + self.column_value_size_jitter) * desired_column_value_size), 1)

    def __call__(self, row):
        generated_data_weight = 0
        while generated_data_weight < self.desired_data_weight_per_job:
            row = {column: self.rsg.generate(self.rng.integers(self.value_size_min, self.value_size_max, endpoint=True))
                   for column in self.columns}
            generated_data_weight += sum(map(len, row.values()))
            yield row


def main():
    parser = argparse.ArgumentParser(description="Queue benchmark data preparation")
    parser.add_argument("--dst", type=str, required=True, help="Destination table")
    parser.add_argument("--job-count", type=int, default=1000, help="Numbers of jobs to run")
    parser.add_argument("--desired-size", type=int, default=1 * GB, help="Desired total data weight in bytes")
    parser.add_argument("--desired-row-size", type=int, default=KB, help="Desired approx. size of a row in bytes")
    parser.add_argument("--column-value-size-jitter", type=float, default=0.2,
                        help="Determines the variability of column value sizes")
    parser.add_argument("--column-count", type=int, default=10, help="Column count, should be in the range [1, 100)")
    parser.add_argument("--tablet-cell-bundle", type=str, default="default", help="Bundle to create result table in")
    parser.add_argument("--block-size", type=int, default=256 * KB, help="Output table block size")

    args = parser.parse_args()

    desired_data_weight_per_job = args.desired_size / args.job_count

    columns = [f"c{i:02}" for i in range(args.column_count)]
    schema = [{"name": column, "type": "string"} for column in columns]

    yt.create("table", args.dst, attributes={"schema": schema}, force=True)

    with yt.TempTable() as fake_input:
        yt.write_table(fake_input, [{"a": "b"} for i in range(args.job_count)])

        op_spec = {
            "title": "Queue benchmark data preparation",
            "job_count": args.job_count,
            "job_io": {
                "table_writer": {
                    "block_size": args.block_size,
                }
            }
        }

        yt.run_map(
            StaticTableWriteMapper(
                desired_data_weight_per_job=desired_data_weight_per_job,
                desired_row_size=args.desired_row_size,
                column_value_size_jitter=args.column_value_size_jitter,
                columns=columns),
            fake_input,
            destination_table=args.dst,
            spec=op_spec,
            sync=True)

    yt.alter_table(args.dst, dynamic=True)
    yt.set(args.dst + "/@tablet_cell_bundle", args.tablet_cell_bundle)
    yt.mount_table(args.dst, sync=True)

    print("Created new ordered dynamic table", args.dst)


if __name__ == "__main__":
    main()
