import click
from pathlib import Path

from .lib import render  # type: ignore


@click.command()
@click.argument("template-path", type=click.Path(exists=True, dir_okay=False))
@click.option("--output-path", "-o", type=click.Path(),
              help=("Resulting rendered YAML path. "
                    "Prints to stdout if unspecified."))
@click.option("--version", "-v", type=int, help="Template version variable.")
def cli(template_path, output_path, version):
    """Render versioned YAML templates.

    TEMPLATE_PATH is a source YAML template.
    """
    with open(template_path) as f:
        rendered = render(f.read(), version)

    if output_path is None:
        click.echo(rendered)
    else:
        with open(output_path, 'w') as f:
            f.write(rendered)

        click.echo(f'Written file "{Path(output_path).name}"')


if __name__ == "__main__":
    cli()
