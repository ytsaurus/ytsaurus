from . import foo

from yt.wrapper.cli_helpers import run_main
import yt.wrapper as yt

def func(row):
    row["constant"] = foo.CONSTANT
    yield row

def main():
    yt.run_map(func, "//tmp/input_table", "//tmp/output_table")

if __name__ == "__main__":
    run_main(main)
