import yt.wrapper as yt

import yt.yson as yson

from collections import defaultdict
import argparse
import os


BUCKETS = [2 ** i for i in range(24)]
BUCKET_DESCRIPTORS = ["<{}".format(bucket) for bucket in BUCKETS + ["inf"]]

MAX_BATCH_SIZE = 100

GB = (2 ** 30)


@yt.aggregator
class Mapper():
    def __init__(self, schema):
        self.columns = []
        for column in schema:
            if "sort_order" in column:
                assert column["sort_order"] == "ascending"
                continue
            if column["type"] != "string":
                continue
            self.columns.append(column["name"])
        sorted(self.columns)

    def __call__(self, rows):
        column_bucket_sizes = defaultdict(lambda: [0] * (len(BUCKETS) + 1))
        column_bucket_data_sizes = defaultdict(lambda: [0] * (len(BUCKETS) + 1))

        for row in rows:
            for column in self.columns:
                if column not in row or not row[column]:
                    index = 0
                    size = 0
                else:
                    index = self.compute_bucket_index(len(row[column]))
                    size = len(row[column])
                column_bucket_sizes[column][index] += 1
                column_bucket_data_sizes[column][index] += size

        for column in self.columns:
            column_result = {"column": column}
            bucket_sizes = column_bucket_sizes[column]
            bucket_data_sizes = column_bucket_data_sizes[column]
            column_result.update(dict(list(zip(
                BUCKET_DESCRIPTORS,
                bucket_sizes))))
            column_result.update(dict(list(zip(
                ["#{}".format(bucket_desc) for bucket_desc in BUCKET_DESCRIPTORS],
                bucket_data_sizes))))

            yield column_result

    def compute_bucket_index(self, length):
        index = 0
        while index < len(BUCKETS):
            if BUCKETS[index] > length:
                break
            index += 1
        return index


def reducer(key, column_results):
    result = next(column_results)
    for column_result in column_results:
        for key, value in column_result.items():
            if key == "column":
                assert value == result["column"]
                continue
            result[key] += value
    yield result


class Visualizer(object):
    def __init__(self, args, client, column_results):
        self.args = args
        self.client = client
        self.column_results = column_results

        self.column_data_sizes = defaultdict(int)
        for column_result in self.column_results:
            for bucket_desc in BUCKET_DESCRIPTORS:
                self.column_data_sizes[column_result["column"]] += column_result["#{}".format(bucket_desc)]
        self.column_data_sizes = {
            column: float(data_size) / self.args.sampling_rate
            for column, data_size in self.column_data_sizes.items()
        }

        self.table_data_weight = float(self.client.get("{}/@data_weight".format(self.args.table_path)))
        self.table_data_weight_gb = self.table_data_weight / GB

        self.static_data_weight = float(self.client.get("{}/@data_weight".format(self.args.static_path))) / self.args.sampling_rate
        self.static_data_weight_gb = self.static_data_weight / GB

        self.meta_size = self.static_data_weight - sum(self.column_data_sizes.values())
        self.column_data_sizes["_meta"] = self.meta_size

        self.column_data_sizes = {
            column: data_size / GB
            for column, data_size in self.column_data_sizes.items()
        }

    def visualize_column_weight_distribution(self, ax):
        percent_sum = 0.
        print()
        for column, data_size in sorted(list(self.column_data_sizes.items()), key=lambda item: item[1], reverse=True):
            percent = 100. * data_size / self.static_data_weight_gb
            percent_sum += percent
            print("{}:\t{:.2f}G ({:.2f}%)".format(column, data_size, percent))
        print("\nTotal static:\t{:.2f}G ({:.2f}%)".format(
            self.static_data_weight_gb, percent_sum))
        print("Total dynamic:\t{:.2f}G ({:.2f}%)".format(
            self.table_data_weight_gb, 100. * self.table_data_weight / self.static_data_weight))

        ax.set_title("Column data weight distribution")
        ax.set_ylabel("Weight in GB")

        indexes = list(range(len(self.column_data_sizes) + 1))
        labels = list(self.column_data_sizes.keys()) + ["_static"]  # , "_dynamic"]
        ax.bar(
            indexes,
            list(self.column_data_sizes.values()) + [self.static_data_weight_gb])  # , table_data_weight_gb])
        ax.set_xticks(indexes)
        ax.set_xticklabels(labels, rotation=90)
        ax.grid()

    def visualize_inline_weight_ratio(self, ax):
        inline_data_weight = []
        inline_data_weight_ratio = []
        current_weight = 0.
        for bucket_desc in BUCKET_DESCRIPTORS:
            for column_result in self.column_results:
                current_weight += column_result["#{}".format(bucket_desc)]
            inline_data_weight.append(current_weight / self.args.sampling_rate + self.meta_size)
            inline_data_weight_ratio.append(100 * inline_data_weight[-1] / self.static_data_weight)

        thresholds = BUCKETS[6:-10]
        inline_data_weight = inline_data_weight[6:-11]
        inline_data_weight_ratio = inline_data_weight_ratio[6:-11]

        print()
        for threshold, data_weight, data_weight_ratio in zip(thresholds, inline_data_weight, inline_data_weight_ratio):
            print("Threshold: {},\tData weight: {:.2f}G ({:.2f}%)".format(
                threshold,
                data_weight / GB,
                data_weight_ratio))

        ax.set_title("Inline data weight")
        ax.set_xlabel("Inline threshold in bytes")
        ax.set_ylabel("Inline weight ratio")

        ax.plot(thresholds, inline_data_weight_ratio)

        ax.set_xscale('symlog', base=2)
        ax.set_xticks(thresholds)
        ax.set_xticklabels(thresholds)
        ax.grid()


def visualize(args, client):
    column_results = list(client.read_table(args.columns_path))

    fig, axs = plt.subplots(2, 1, figsize=(15, 12), constrained_layout=True)

    visualizer = Visualizer(args, client, column_results)
    visualizer.visualize_column_weight_distribution(axs[0])
    visualizer.visualize_inline_weight_ratio(axs[1])

    plt.savefig(args.result_file)


def do_compute_weight(args, client):
    basename = os.path.basename(args.table_path)
    args.static_path = "{}/{}_static_table".format(args.computation_path, basename)
    args.columns_path = "{}/{}_columns_result".format(args.computation_path, basename)

    if not client.exists(args.computation_path):
        client.create("map_node", args.computation_path)

    table_schema = client.get("{}/@schema".format(args.table_path))

    client.run_merge(
        args.table_path,
        "<schema={}>{}".format(yson.dumps(table_schema), args.static_path),
        spec={
            "force_transform": True,
            "sampling": {
                "sampling_rate": args.sampling_rate,
            },
        })

    result_schema = [{"name": "column", "type": "string"}]
    for bucket_desc in BUCKET_DESCRIPTORS:
        result_schema.append({"name": bucket_desc, "type": "int64"})
        result_schema.append({"name": "#{}".format(bucket_desc), "type": "int64"})

    client.run_map_reduce(
        Mapper(table_schema),
        reducer,
        args.static_path,
        "<schema={}>{}".format(yson.dumps(result_schema), args.columns_path),
        reduce_by=["column"])

    visualize(args, client)


def do_compute_saturation(args, client):
    chunk_ids = client.get("{}/@chunk_ids".format(args.table_path))
    print(len(chunk_ids))

    batch_client = client.create_batch_client()

    chunk_infos = []
    batch_count = (len(chunk_ids) - 1) / MAX_BATCH_SIZE + 1
    print("Will read chunk info in {} batches".format(batch_count))
    for batch_index in range(batch_count):
        if batch_index % 100 == 0:
            print("Batch {}/{}".format(batch_index, batch_count))

        for chunk_id in chunk_ids[batch_index * MAX_BATCH_SIZE:(batch_index + 1) * MAX_BATCH_SIZE]:
            chunk_infos.append(
                batch_client.get("#{}".format(chunk_id), attributes=["chunk_type", "hunk_chunk_refs", "data_weight"]))
        batch_client.commit_batch()

    total_data_weight = client.get("{}/@data_weight".format(args.table_path))
    chunk_data_weight = 0
    hunk_chunk_data_weight = 0

    chunk_to_hunk_chunk_refs = {}
    hunk_chunk_to_weight = {}
    for chunk_id, chunk_info in zip(chunk_ids, chunk_infos):
        if not chunk_info.is_ok():
            error = yt.YtResponseError(chunk_info.get_error())
            assert error.is_resolve_error()
            continue

        chunk_attributes = chunk_info.get_result().attributes
        chunk_type = chunk_attributes.get("chunk_type")
        data_weight = chunk_attributes.get("data_weight")
        if chunk_type == "table":
            chunk_data_weight += data_weight
            chunk_to_hunk_chunk_refs[chunk_id] = chunk_attributes.get("hunk_chunk_refs")
        else:
            assert chunk_type == "hunk"
            hunk_chunk_data_weight += data_weight
            hunk_chunk_to_weight[chunk_id] = data_weight

    chunk_to_hunk_ref_count = defaultdict(int)
    chunk_to_hunk_ref_length = defaultdict(int)

    hunk_chunk_to_reffing_chunk_count = defaultdict(int)
    hunk_chunk_to_ref_length = defaultdict(int)

    for chunk_id, hunk_chunk_refs in chunk_to_hunk_chunk_refs.items():
        for ref in hunk_chunk_refs:
            chunk_to_hunk_ref_count[chunk_id] += ref["hunk_count"]
            chunk_to_hunk_ref_length[chunk_id] += ref["total_hunk_length"]

            hunk_chunk_to_reffing_chunk_count[ref["chunk_id"]] += 1
            hunk_chunk_to_ref_length[ref["chunk_id"]] += ref["total_hunk_length"]

    hunk_chunk_to_saturation = {}
    for chunk_id, ref_length in hunk_chunk_to_ref_length.items():
        weight = hunk_chunk_to_weight[chunk_id]
        hunk_chunk_to_saturation[chunk_id] = 100 * float(ref_length) / weight

    fig, axs = plt.subplots(2, 3, figsize=(20, 10), constrained_layout=True)

    axs[0, 0].set_title("Chunk dist by number of referenced hunk chunks")
    axs[0, 0].hist([len(refs) for refs in list(chunk_to_hunk_chunk_refs.values())], bins=30)
    axs[0, 0].grid()

    axs[0, 1].set_title("Chunk dist by number of referenced hunks")
    axs[0, 1].hist(list(chunk_to_hunk_ref_count.values()), bins=30)
    axs[0, 1].grid()

    axs[0, 2].set_title("Chunk dist by total ref length")
    axs[0, 2].hist(list(chunk_to_hunk_ref_length.values()), bins=30)
    axs[0, 2].grid()

    axs[1, 0].set_title("Hunk chunk dist by reffing chunk count")
    axs[1, 0].hist(list(hunk_chunk_to_reffing_chunk_count.values()), bins=30)
    axs[1, 0].grid()

    axs[1, 1].set_title("Hunk chunk dist by total ref length")
    axs[1, 1].hist(list(hunk_chunk_to_ref_length.values()), bins=30)
    axs[1, 1].grid()

    axs[1, 2].set_title("Hunk chunk dist by saturation")
    axs[1, 2].hist(list(hunk_chunk_to_saturation.values()), bins=30)
    axs[1, 2].grid()

    plt.savefig(args.result_file)

    print()
    print("Table chunk count: {},\tdata weight: {:.2f}G ({:.2f}%)".format(
        len(chunk_to_hunk_chunk_refs),
        float(total_data_weight - hunk_chunk_data_weight) / GB,
        100 * float(total_data_weight - hunk_chunk_data_weight) / total_data_weight))
    print("Hunk chunk count: {},\tdata weight: {:.2f}G ({:.2f}%),\ttotal ref length: {:.2f}G ({:.2f}%)".format(
        len(hunk_chunk_to_weight),
        float(hunk_chunk_data_weight) / GB,
        100 * float(hunk_chunk_data_weight) / total_data_weight,
        float(sum(hunk_chunk_to_ref_length.values())) / GB,
        100 * float(sum(hunk_chunk_to_ref_length.values())) / total_data_weight))


if __name__ == "__main__":
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    parser = argparse.ArgumentParser(description="Hunk chunk advisor")

    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser_group = common_parser.add_argument_group("Common")

    common_parser_group.add_argument("--proxy", type=yt.config.set_proxy, help="YT proxy", required=True)
    common_parser_group.add_argument("--table-path", type=str, help="Table path", required=True)
    common_parser_group.add_argument("--sampling-rate", type=float, default=1., help="Analyzer sampling rate")
    common_parser_group.add_argument("--result-file", type=str, help="Result file name", required=True)

    subparsers = parser.add_subparsers(dest="mode", help="Mode specific arguments")

    compute_weight_parser = subparsers.add_parser(
        "compute_weight",
        parents=[common_parser],
        help="Computes inline weight ratio")
    compute_weight_parser_group = compute_weight_parser.add_argument_group("Compute weight params")
    compute_weight_parser_group.add_argument("--computation-path", type=str, help="Path for intermediate computation results", required=True)

    compute_saturation_parser = subparsers.add_parser(
        "compute_saturation",
        parents=[common_parser],
        help="Computes effective hunk chunk weight")

    args = parser.parse_args()

    client = yt.YtClient(config=yt.config.config)
    client.config["pickling"]["module_filter"] = (lambda module: "hashlib" not in getattr(module, "__name__", "") and (getattr(module, "__name__", "") != "hmac"))

    if args.mode == "compute_weight":
        do_compute_weight(args, client)
    else:
        do_compute_saturation(args, client)
