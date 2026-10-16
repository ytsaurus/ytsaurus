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

    passed = data.get("passed")
    if passed is not None:
        click.secho("  Passed:    ", nl=False, bold=True)
        click.secho("yes" if passed else "no", fg="green" if passed else "red", bold=True)

    reason = data.get("reason")
    if reason:
        click.secho("  Reason:    ", nl=False, bold=True)
        click.secho(reason, fg="red")

    click.secho("  Namespace: ", nl=False, bold=True)
    click.echo(data.get("namespace", "—"))

    click.secho("  Scenario:  ", nl=False, bold=True)
    click.echo(data.get("scenario", "—"))

    priority, priority_color = _format_priority(data.get("priority", ""))
    click.secho("  Priority:  ", nl=False, bold=True)
    click.secho(priority, fg=priority_color, bold=True)

    click.secho("  Duration:  ", nl=False, bold=True)
    click.echo(_format_duration(data))

    artifacts = [url for url in data.get("logs_urls", []) if url]
    click.echo()
    if artifacts:
        click.secho("  Artifacts:", bold=True)
        for url in artifacts:
            click.secho(f"    • {url}", fg="blue")
    else:
        click.secho("  No artifacts", fg="yellow")

    components = data.get("components", [])
    operator = data.get("operator", {})
    upgrade = data.get("upgrade", {})

    if upgrade:
        _print_upgrade(components, operator, upgrade)
    else:
        if components:
            click.echo()
            click.secho("  Components:", bold=True)
            for c in components:
                _print_component(c)

        if operator:
            click.echo()
            click.secho("  Operator:", bold=True)
            _print_component(operator.get("operator", {}))
            click.secho(f"    {'Helm:':<15}", nl=False)
            click.secho(operator.get("helm_url", "—"), fg="bright_black")

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


def _print_upgrade(components, operator, upgrade):
    click.echo()
    scenario = upgrade.get("scenario_name", "")
    click.secho("  Upgrade", nl=False, bold=True)
    if scenario:
        click.secho(f" ({scenario}):", bold=True)
    else:
        click.secho(":", bold=True)

    from_versions = {c.get("name"): c.get("version") for c in components}
    operator_from = operator.get("operator", {}) if operator else {}
    from_versions[operator_from.get("name")] = operator_from.get("version")

    targets = list(upgrade.get("upgrade_components", []))
    target_operator = upgrade.get("upgrade_operator", {})
    if target_operator:
        targets.append(target_operator.get("operator", {}))

    for c in targets:
        _print_upgrade_transition(c, from_versions.get(c.get("name")))


def _print_upgrade_transition(c, from_version):
    to_version = c.get("version", "—")
    click.secho(f"    {c.get('name', '—'):<15}", nl=False, bold=True)
    if from_version is None or from_version == to_version:
        click.secho(f"v{to_version}", fg="cyan")
    else:
        click.secho(f"v{from_version}", nl=False, fg="bright_black")
        click.secho(" → ", nl=False)
        click.secho(f"v{to_version}", fg="cyan")


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


def _format_priority(priority):
    match priority:
        case enums.TaskPriority.CRITICAL:
            color = "red"
        case enums.TaskPriority.HIGH:
            color = "yellow"
        case enums.TaskPriority.NORMAL:
            color = "green"
        case _:
            color = "white"

    if not priority:
        return "—", color

    priority = priority.replace("TASK_PRIORITY_", "")
    return priority, color
