from datetime import datetime

import click

from yt.admin.ytsaurus_ci import enums


def print_tasks_list(data):
    tasks = data.get("tasks", [])
    total = data.get("total", len(tasks))

    click.echo()
    click.secho(f"  Tasks ({total}):", bold=True)
    click.secho("  " + "─" * 50, fg="bright_black")

    if not tasks:
        click.secho("  No tasks found", fg="yellow")
        click.echo()
        return

    for job_id in tasks:
        click.secho(f"    • {job_id}", fg="blue")

    click.echo()


def print_job_info(data, job_id):
    status, status_color = _format_job_status(data.get("status", ""))

    click.echo()
    click.secho(f"  Job: {job_id}", bold=True)
    click.secho("  " + "─" * 50, fg="bright_black")

    click.secho("  Status:    ", nl=False, bold=True)
    click.secho(status, fg=status_color, bold=True)

    click.secho("  Namespace: ", nl=False, bold=True)
    click.echo(data.get("namespace", "—"))

    click.secho("  Duration:  ", nl=False, bold=True)
    click.echo(_format_duration(data))

    logs = [url for url in data.get("logs_urls", []) if url]
    if logs:
        click.echo()
        click.secho("  Logs:", bold=True)
        for url in logs:
            click.secho(f"    • {url}", fg="blue")

    components = data.get("components", [])
    if components:
        click.echo()
        click.secho("  Components:", bold=True)
        for c in components:
            _print_component(c)

    operator = data.get("operator", {})
    if operator:
        click.echo()
        click.secho("  Operator:", bold=True)
        _print_component(operator.get("operator", {}))
        click.secho(f"    {'Helm:':<15}", nl=False)
        click.secho(operator.get("helm_url", "—"), fg="bright_black")

    upgrade = data.get("upgrade", {})
    if upgrade:
        click.echo()
        scenario = upgrade.get("scenario_name", "")
        click.secho("  Upgrade target", nl=False, bold=True)
        if scenario:
            click.secho(f" ({scenario}):", bold=True)
        else:
            click.secho(":", bold=True)

        for c in upgrade.get("upgrade_components", []):
            _print_component(c)

        target_operator = upgrade.get("upgrade_operator", {})
        if target_operator:
            _print_component(target_operator.get("operator", {}))
            click.secho(f"    {'Helm:':<15}", nl=False)
            click.secho(target_operator.get("helm_url", "—"), fg="bright_black")

    failed = data.get("failed_checks", [])
    click.echo()
    if failed:
        click.secho(f"  Failed Checks ({len(failed)}):", bold=True, fg="red")
        for check in failed:
            desc = check.get("description", {})
            click.secho(f"    [{check['type']}] ", nl=False, fg="red", bold=True)
            click.secho(f"{desc.get('release_name', '—')} v{desc.get('version', '—')}")
            click.secho(f"           {desc.get('helm_url', '—')}", fg="bright_black")
    else:
        click.secho("  No failed checks", fg="green")
    click.echo()


def _format_duration(data):
    from_str = data.get("created_at", "")
    to_str = data.get("finished_at", "")
    if not from_str or not to_str:
        return "—"

    t0 = datetime.fromisoformat(from_str.replace("Z", "+00:00")).replace(tzinfo=None)
    t1 = datetime.fromisoformat(to_str.replace("Z", "+00:00")).replace(tzinfo=None)
    minutes = int((t1 - t0).total_seconds() // 60)
    return f"{t0.strftime('%Y-%m-%d %H:%M')} → {t1.strftime('%H:%M')} ({minutes}m)"


def _print_component(c):
    click.secho(f"    {c.get('name', '—'):<15}", nl=False, bold=True)
    click.secho(f"{c.get('branch', '—'):<8}", nl=False)
    click.secho(f"v{c.get('version', '—'):<10}", nl=False, fg="cyan")
    click.secho(f"({c.get('revision', '—')[:7]}, {c.get('commit_date', '—')})", fg="bright_black")


def _format_job_status(status):
    match status:
        case enums.JobStatus.FINISHED:
            color = "green"
        case enums.JobStatus.NEW:
            color = "yellow"
        case enums.JobStatus.PENDING:
            color = "blue"
        case enums.JobStatus.FAULT:
            color = "red"
        case enums.JobStatus.ABORTED:
            color = "red"
        case enums.JobStatus.UNSPECIFIED:
            color = "magenta"
        case _:
            color = "white"

    status = status.replace("TASK_STATUS_", "")
    return status, color
