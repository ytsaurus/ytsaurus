import click
import json
import requests
import curlify

import cfg_loader
import ghcr


@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(dict)
    ctx.obj["config"] = "global_config"


@cli.command()
@click.option("--scenario", type=click.Choice(["nightly-dev"]), required=True, help="Scenario name")
@click.option("--git-token", type=str, required=True)
@click.option("--git-api-url", type=str, default="https://api.github.com")
@click.option("--cloud-functions-token", type=str, required=True)
@click.option("--cloud-function-id", type=str, default="d4ee6v3cr3udu6bpnova")
@click.option("--apply", is_flag=True, help="Make new task with generated spec")
def run_scenario(scenario, git_token, git_api_url, cloud_functions_token, cloud_function_id, apply):
    auth = ghcr.GitHubAuth(token=git_token, base_url=git_api_url)
    result = cfg_loader.ProcessScenario(scenario, auth)
    session = requests.Session()
    session.headers.update({"Authorization": f"Api-Key {cloud_functions_token}"})
    json_payload = result.to_json()
    req = session.prepare_request(
        requests.Request(
            'POST',
            f"https://functions.yandexcloud.net/{cloud_function_id}?integration=raw",
            json=json_payload,
        )
    )

    if not apply:
        click.secho("You are using cli without `--apply`, and nothing not will be applied\n", fg="yellow")

        click.secho("\n\nYou can reproduce this command with curl", fg="yellow")
        click.secho(curlify.to_curl(req))

        return

    if apply:
        click.secho("Your changes will be applied\n", fg="green")
        click.echo(json.dumps(json_payload, indent=2, ensure_ascii=False))

        response = session.send(req)
        response.raise_for_status()
        content = response.json()
        if content["status"]:
            color = "green"
        else:
            color = "red"

        click.secho(response.text, fg=color)

    session.close()


if __name__ == "__main__":
    cli(obj={})
