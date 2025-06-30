from github import Github

import argparse
import requests

CONCLUSION_SUCCESS = "success"
CONCLUSION_FAILURE = "failure"


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
        "--commit-message",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--git-server-url",
        default="https://api.github.com",
        type=str,
        required=True,
    )

    return parser.parse_args()


def _get_prev_conclusion(repo, workflow_name, ref):
    current_workflow = None
    for workflow in repo.get_workflows():
        if workflow.name == workflow_name:
            current_workflow = workflow

    if not current_workflow:
        print("Not found workflow with name", workflow_name)
        return

    runs = current_workflow.get_runs(branch=ref, status="completed")
    if not runs or runs.totalCount == 0:
        print("No one runs with such filter")
        return

    return runs[0].conclusion


def _get_workflow_state(prev_conclusion, current_conclusion):
    state_to_alert = {
        (CONCLUSION_SUCCESS, CONCLUSION_FAILURE): "failed ❌",
        (CONCLUSION_FAILURE, CONCLUSION_SUCCESS): "fixed ✅",
    }

    return state_to_alert.get((prev_conclusion, current_conclusion))


def _send_notify(args, workflow_state):
    message = "\n".join((
        workflow_state,
        f"Workflow *{args.workflow}*: {args.git_server_url}/{args.repo}/actions/runs/{args.current_job_id}",
        f"Git ref: *{args.ref}*.",
        f"Commit: ```{args.commit_message}```",
    ))

    url = "https://api.telegram.org/bot{}/sendMessage".format(args.tg_token)
    data = {
        "chat_id": args.tg_chat_id,
        "disable_web_page_preview": True,
        "parse_mode": "Markdown",
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
    prev_conclusion = _get_prev_conclusion(repo, args.workflow, args.ref)

    workflow_state = _get_workflow_state(prev_conclusion, args.current_job_conclusion)
    if workflow_state:
        _send_notify(args, workflow_state)
        print("Workflow's was changed: ", workflow_state)
    else:
        print("The workflow state hasn't changed.")


if __name__ == "__main__":
    main()
