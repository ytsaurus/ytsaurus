import json
import os

import click
import yaml

from library.python import resource
from yt.admin.ytsaurus_ci import cloudfunction_client
from yt.admin.ytsaurus_ci import compatibility_graph
from yt.admin.ytsaurus_ci import component_registry
from yt.admin.ytsaurus_ci import consts
from yt.admin.ytsaurus_ci import ghcr
from yt.admin.ytsaurus_ci import scenario_processor


@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)


@cli.command()
@click.option("--job-id", type=str, required=True, help="job_id of interested test")
@click.option("--cloud-function-token", type=str, required=True)
def reproduce(job_id, cloud_function_token):
    client = cloudfunction_client.CloudFunctionClient(
        cloudfunction_client.YCFunctionAuth(
            cloud_function_token=cloud_function_token,
        )
    )

    content = client.run_task(job_id)
    if content["status"]:
        color = "green"
    else:
        color = "red"

    click.secho(content, fg=color)


def version_filter_option(f):
    return click.option(
        "--version-filter", type=str, required=False, default="{}", help="--version-filter '{\"operator\": \"main\"}'"
    )(f)


@cli.group()
def matrix():
    pass


@matrix.command()
@version_filter_option
@click.option("--json", "with_json", is_flag=True)
def run(version_filter, with_json):
    registry = component_registry.VersionComponentRegistry(yaml.safe_load(resource.resfs_read(consts.COMPONENTS_PATH)))
    graph = compatibility_graph.CompatibilityGraph(registry)

    suites = graph.find_all_test_suites(json.loads(version_filter))
    if with_json:
        print(suites)
    else:
        compatibility_graph.print_suites(suites)

    if suites:
        click.secho(f"\nTotal: {len(suites)} compatible suite(s)", fg="green")
    else:
        click.secho("\nNo compatible suites found for given components", fg="red")


@matrix.command()
@click.option("--output", "output_dir", type=click.Path(), required=True)
@click.option(
    "--components",
    multiple=True,
    required=True,
    type=click.Choice(
        [
            "ytsaurus",
            "operator",
            "chyt",
            "spyt",
            "query_tracker",
            "strawberry",
        ]
    ),
)
def docs(output_dir, components):
    registry = component_registry.VersionComponentRegistry(yaml.safe_load(resource.resfs_read(consts.COMPONENTS_PATH)))
    os.makedirs(output_dir, exist_ok=True)

    for component in components:
        md = compatibility_graph.format_compat_table(registry, component)
        path = os.path.join(output_dir, f"{component}.md")
        with open(path, "w", encoding="utf-8") as f:
            f.write(md)

    click.secho(f"Written {len(components)} file(s) to {output_dir}", fg="green")


@cli.command()
@click.option("--scenario", required=True, help="Scenario name")
@click.option("--git-token", type=str, required=True)
@click.option("--git-api-url", type=str, default="https://api.github.com")
@click.option("--cloud-function-token", type=str, required=True)
@click.option("--version-filter", type=str, required=False, default="{}")
@click.option("--apply", is_flag=True, help="Make new task with generated spec")
@click.option("--force", is_flag=True, help="Overwrite job")
@click.option("--verbose", is_flag=True, help="Detailed output of request")
def run_scenario(
    scenario,
    git_token,
    git_api_url,
    cloud_function_token,
    version_filter,
    apply,
    force,
    verbose,
):
    auth = ghcr.GitHubAuth(token=git_token, base_url=git_api_url)
    processed_scenarios = scenario_processor.ProcessScenario(scenario, auth, json.loads(version_filter))
    client = cloudfunction_client.CloudFunctionClient(
        cloudfunction_client.YCFunctionAuth(
            cloud_function_token=cloud_function_token,
        )
    )

    for scenario in processed_scenarios:
        json_payload = scenario.to_dict()
        if force:
            json_payload["force"] = True
        content = client.submit_task(json_payload, apply)

        if not apply:
            click.secho("You are using cli without `--apply`, and nothing will be applied\n", fg="yellow")
            if not verbose:
                click.secho(scenario.preview())
            else:
                click.secho("\n\nYou can reproduce this command with curl", fg="yellow")
                click.secho(content)

            continue

        if apply:
            click.secho("Your changes will be applied\n", fg="green")
            if verbose:
                click.echo(json.dumps(json_payload, indent=2, ensure_ascii=False))
            else:
                click.echo(scenario.preview())

            if content["status"]:
                color = "green"
            else:
                color = "red"

            click.secho(content, fg=color)


def main():
    cli(obj={})
