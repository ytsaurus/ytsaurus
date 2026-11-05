from yt.yt.flow.tools.reshard_flow_tables.lib import get_args, reshard_tables

if __name__ == "__main__":
    args = get_args()
    reshard_tables(args)
