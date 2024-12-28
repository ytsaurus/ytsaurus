from yt.yt.orm.library.snapshot.codegen.config import SnapshotConfig
from yt.yt.orm.library.snapshot.codegen.model import init_snapshot
from yt.yt.orm.library.snapshot.codegen.render import render_snapshot

from pathlib import Path
import click


@click.command()
@click.option("-i", "--input", required=True, type=Path, help="Input snapshot spec file path")
@click.option("-o", "--output", required=True, type=Path, help="Output dir path")
def main(input, output):
    config = SnapshotConfig.from_file(input)
    snapshot = init_snapshot(config)
    output.mkdir(mode=0o777, parents=True, exist_ok=True)
    render_snapshot(snapshot, output)


if __name__ == "__main__":
    main()
