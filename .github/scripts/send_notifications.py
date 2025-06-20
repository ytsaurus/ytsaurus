import argparse
import requests

CONCLUSION_SUCCESS = 'success'
CONCLUSION_FAILURE = 'failure'


def send_notify(args, wf_state):
    message = '\n'.join((
        wf_state,
        f'Workflow *{args.workflow}*: {args.git_server_url}/{args.repo}/actions/runs/{args.current_job_id}',
        f'Git {args.ref_type}: *{args.ref}*.',
        f'Commit: ```{args.commit_message}```',
    ))

    url = 'https://api.telegram.org/bot{}/sendMessage'.format(args.tg_token)
    data = {'chat_id': args.tg_chat_id, 'text': message, 'parse_mode': 'Markdown'}
    response = requests.post(url, data=data)
    try:
        response.raise_for_status()
    except requests.HTTPError:
        print(response.json())


def main():
    parser = argparse.ArgumentParser(
        prog='Get status of the last completed action to stdout',
    )

    parser.add_argument(
        '--git-token',
        type=str,
        required=True,
    )

    parser.add_argument(
        '--repo',
        default='ytsaurus/ytsaurus',
        type=str,
        required=True,
    )

    parser.add_argument(
        '--workflow',
        help='For example `C++ CI`',
        type=str,
        required=True,
    )

    parser.add_argument(
        '--ref',
        type=str,
        required=True,
    )

    parser.add_argument(
        '--ref-type',
        type=str,
        required=True,
    )

    parser.add_argument(
        '--tg-token',
        type=str,
        required=True,
    )

    parser.add_argument(
        '--tg-chat-id',
        type=int,
        required=True,
    )

    parser.add_argument(
        '--current-job-conclusion',
        type=str,
        required=True,
    )

    parser.add_argument(
        '--current-job-id',
        type=int,
        required=True,
    )

    parser.add_argument(
        '--commit-message',
        type=str,
        required=True,
    )

    parser.add_argument(
        '--git-server-url',
        type=str,
        required=True,
    )

    args = parser.parse_args()

    headers = {'Authorization': f'Bearer {args.git_token}'}

    r = requests.get(f'https://api.github.com/repos/{args.repo}/actions/workflows', headers=headers)
    workflow_url = None
    for wf in r.json()['workflows']:
        if wf['name'] == args.workflow:
            workflow_url = wf['url']
            break

    if not workflow_url:
        print('Not found workflow with name', args.workflow)
        return

    r = requests.get(f'{workflow_url}/runs?status=completed&{args.ref_type}={args.ref}',
                     headers=headers)
    r.raise_for_status()

    runs = r.json()['workflow_runs']
    if runs:
        last = runs[0]
        prev_conclusion = last['conclusion']
        state_to_alert = {
            (CONCLUSION_SUCCESS, CONCLUSION_FAILURE): "failed ❌",
            (CONCLUSION_FAILURE, CONCLUSION_SUCCESS): "fixed ✅",
        }

        wf_state = state_to_alert.get((prev_conclusion, args.current_job_conclusion))
        if wf_state:
            send_notify(args, wf_state)
        else:
            print('State is not changed')
    else:
        print('No runs for', r.params)


if __name__ == '__main__':
    main()
