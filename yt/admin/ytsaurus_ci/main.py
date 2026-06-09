import json
import os

import click
import yaml
import hashlib

from library.python import resource
from yt.admin.ytsaurus_ci import cloudfunction_client
from yt.admin.ytsaurus_ci import compatibility_graph
from yt.admin.ytsaurus_ci import component_registry
from yt.admin.ytsaurus_ci import consts
from yt.admin.ytsaurus_ci import enums
from yt.admin.ytsaurus_ci import ghcr
from yt.admin.ytsaurus_ci import pretty
from yt.admin.ytsaurus_ci import scenario_processor


@click.group(context_settings=dict(help_option_names=["-h", "--help"]))
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)


def _resolve_cloud_function_token(ctx, param, value):
    if value:
        return value

    try:
        with open(consts.CLOUD_FUNCTION_TOKEN_PATH) as f:
            value = f.read().strip()
    except OSError:
        value = ""

    if not value:
        raise click.BadParameter(f"provide --cloud-function-token or put it in {consts.CLOUD_FUNCTION_TOKEN_PATH}")

    return value


def cloud_function_token_option(f):
    return click.option(
        "--cloud-function-token",
        type=str,
        default="",
        callback=_resolve_cloud_function_token,
        help=f"Cloud Function token; if omitted, taken from {consts.CLOUD_FUNCTION_TOKEN_PATH}",
    )(f)


@cli.command()
@click.option("--job-id", type=str, required=True, help="job_id of interested test")
@cloud_function_token_option
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
@click.option(
    "--output",
    "output_dir",
    type=click.Path(),
    default="yt/docs/en/_includes/compatibility",
    required=True,
)
def docs(output_dir):
    registry = component_registry.VersionComponentRegistry(yaml.safe_load(resource.resfs_read(consts.COMPONENTS_PATH)))
    os.makedirs(output_dir, exist_ok=True)

    component = compatibility_graph.PIVOT_COMPONENT
    md = compatibility_graph.format_compat_table(registry)
    path = os.path.join(output_dir, f"{component}.md")

    snapshots_file = os.path.join(consts.BASE_DIR, consts.SNAPSHOTS_PATH, component)
    with open(snapshots_file, "w", encoding="utf-8") as f:
        snapshot = hashlib.sha512(md.encode())
        f.write(snapshot.hexdigest())

    with open(path, "w", encoding="utf-8") as f:
        f.write(md)

    if "/en/" in path:
        path = path.replace("/en/", "/ru/")
    else:
        path = path.replace("/ru/", "/en/")

    with open(path, "w", encoding="utf-8") as f:
        f.write(md)

    click.secho(f"Written compatibility table to {output_dir}", fg="green")


@cli.command()
@click.option("--scenario", required=True, help="Scenario name")
@click.option("--git-token", type=str, required=True)
@click.option("--git-api-url", type=str, default="https://api.github.com")
@cloud_function_token_option
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


@cli.command()
@click.option("--job-id", type=str, required=True, help="Id of the job to get info for")
@cloud_function_token_option
def job_info(job_id, cloud_function_token):
    client = cloudfunction_client.CloudFunctionClient(
        cloudfunction_client.YCFunctionAuth(
            cloud_function_token=cloud_function_token,
        )
    )

    content = client.get_task_info(job_id)
    pretty.print_job_info(content, job_id)


@cli.command()
@click.option(
    "--status",
    type=click.Choice([s.name for s in enums.JobStatus]),
    required=False,
    default=None,
    help="Filter tasks by status",
)
@click.option("--components-key-filter", type=str, required=False, default=None, help="Filter tasks by components key")
@click.option("--passed", type=bool, required=False, default=None, help="Filter by passed checks")
@cloud_function_token_option
@click.option("--json", "with_json", is_flag=True, help="Print raw response")
def list_tasks(status, components_key_filter, passed, cloud_function_token, with_json):
    client = cloudfunction_client.CloudFunctionClient(
        cloudfunction_client.YCFunctionAuth(
            cloud_function_token=cloud_function_token,
        )
    )

    content = client.list_tasks(
        status=enums.JobStatus[status].value if status else None,
        components_key_filter=components_key_filter,
        passed=passed,
    )

    if with_json:
        click.echo(json.dumps(content, indent=2, ensure_ascii=False))
    else:
        pretty.print_tasks_list(content)


def main():
    cli(obj={})
