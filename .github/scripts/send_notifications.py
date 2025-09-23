import argparse
import typing
import requests

from datetime import datetime
from urllib.parse import quote

from github import Github
from github import Workflow
from github import WorkflowRun

CONCLUSION_SUCCESS = "success"
CONCLUSION_FAILURE = "failure"
CONCLUSION_CANCELLED = "cancelled"
STATUS_COMPLETED = "completed"

DISTANCE_BETWEEN_FAILED_JOBS = 10


def _parse_args():
    parser = argparse.ArgumentParser(
        prog="Get status of the last completed action",
    )

    parser.add_argument(
        "--git-token",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--repo",
        default="ytsaurus/ytsaurus",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--workflow",
        help="For example `C++ CI`",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--ref",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--tg-token",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--tg-chat-id",
        type=int,
        required=True,
    )

    parser.add_argument(
        "--current-job-conclusion",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--current-job-id",
        type=int,
        required=True,
    )

    parser.add_argument(
        "--git-server-url",
        default="https://api.github.com",
        type=str,
        required=True,
    )

    return parser.parse_args()


def _get_workflow(repo, workflow_name: str) -> typing.Optional[Workflow.Workflow]:
    current_workflow = None
    for workflow in repo.get_workflows():
        if workflow.name == workflow_name and workflow.state == "active":
            current_workflow = workflow

    if not current_workflow:
        print("Not found workflow with name", workflow_name)

    return current_workflow


def _get_prev_conclusion(workflow: Workflow.Workflow, ref: str, time: str) -> typing.Optional[str]:
    runs = workflow.get_runs(branch=ref, status=STATUS_COMPLETED, created=f"<{time}")
    if not runs or runs.totalCount == 0:
        print("No one runs with such filter")
        return

    return runs[0].conclusion


def _get_last_success_run(
    workflow: Workflow.Workflow, ref: str, time: str
) -> typing.Tuple[typing.Optional[WorkflowRun.WorkflowRun], int]:
    runs = workflow.get_runs(branch=ref, status=STATUS_COMPLETED, created=f"<{time}")
    if not runs or runs.totalCount == 0:
        print("No one runs with such filter")
        return None, 0

    counter = 0
    for run in runs:
        counter += 1
        if run.conclusion == CONCLUSION_SUCCESS:
            return run, counter

    return None, counter


def _get_new_workflow_state(prev_conclusion, current_conclusion):
    state_to_alert = {
        (CONCLUSION_SUCCESS, CONCLUSION_CANCELLED): "cancelled ❗️",
        (CONCLUSION_SUCCESS, CONCLUSION_FAILURE): "failed ❌",
        (CONCLUSION_FAILURE, CONCLUSION_SUCCESS): "fixed ✅",
        (CONCLUSION_CANCELLED, CONCLUSION_SUCCESS): "fixed ✅",
    }

    return state_to_alert.get((prev_conclusion, current_conclusion))


def _make_new_fail_message(
    new_workflow_state, workflow_name: str, ref: str, server_url: str, repo: str,
    current_job_id: int, commit_message: str,
) -> str:
    human_readable_url = server_url.replace("api.", "")

    return "\n".join((
        new_workflow_state,
        f"Workflow *{workflow_name}*: {human_readable_url}/{repo}/actions/runs/{current_job_id}",
        f"Git ref: *{ref}*.",
        "Commit: ```",
        commit_message,
        "```"
    ))


def _make_still_broken_message(repo: str, git_url: str, ref: str, wf_name: str, wf_file: str) -> str:
    human_readable_url = git_url.replace("api.", "")
    query_param = quote(f"branch:{ref}")

    return "\n".join((
        "⚠️",
        f"The workflow *{wf_name}* still broken in branch *{ref}* more than 24 hours.",
        f"Link: {human_readable_url}/{repo}/actions/workflows/{wf_file}?query={query_param}",
    ))


def _send_message(tg_token: str, tg_chat_id: int, message: str, parse_mode="Markdown"):
    url = "https://api.telegram.org/bot{}/sendMessage".format(tg_token)
    data = {
        "chat_id": tg_chat_id,
        "disable_web_page_preview": True,
        "parse_mode": parse_mode,
        "text": message,
    }
    response = requests.post(url, data=data)
    try:
        response.raise_for_status()
    except requests.HTTPError:
        print(response.json())


def main():
    args = _parse_args()

    gh = Github(login_or_token=args.git_token, base_url=args.git_server_url)
    repo = gh.get_repo(args.repo)

    workflow = _get_workflow(repo, args.workflow)
    if not workflow:
        return

    created_time_current_job = repo.get_workflow_run(args.current_job_id).created_at
    created_time_current_job = created_time_current_job.strftime("%Y-%m-%dT%H:%M:%SZ")
    prev_conclusion = _get_prev_conclusion(workflow, args.ref, created_time_current_job)
    if CONCLUSION_SUCCESS not in (prev_conclusion, args.current_job_conclusion):
        last_run, dist_to_job = _get_last_success_run(workflow, args.ref, created_time_current_job)
        if last_run and dist_to_job % DISTANCE_BETWEEN_FAILED_JOBS == 0:
            current_datetime = datetime.now(last_run.created_at.tzinfo)
            if (current_datetime - last_run.created_at).days >= 1:
                wf_file = workflow.path.split('/')[-1]
                msg = _make_still_broken_message(
                    repo=args.repo,
                    git_url=args.git_server_url,
                    ref=args.ref,
                    wf_name=workflow.name,
                    wf_file=wf_file,
                )
                _send_message(
                    tg_token=args.tg_token,
                    tg_chat_id=args.tg_chat_id,
                    message=msg,
                )

    new_workflow_state = _get_new_workflow_state(prev_conclusion, args.current_job_conclusion)
    if new_workflow_state:
        current_job = repo.get_workflow_run(args.current_job_id)
        msg = _make_new_fail_message(
            new_workflow_state,
            workflow_name=workflow.name,
            ref=args.ref,
            server_url=args.git_server_url,
            repo=args.repo,
            current_job_id=args.current_job_id,
            commit_message=current_job.head_commit.message,
        )
        _send_message(tg_token=args.tg_token, tg_chat_id=args.tg_chat_id, message=msg)
        print("Workflow's was changed: ", new_workflow_state)
    else:
        print("The workflow state hasn't changed.")


if __name__ == "__main__":
    main()
