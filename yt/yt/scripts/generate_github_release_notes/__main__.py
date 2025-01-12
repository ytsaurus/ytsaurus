import argparse
import subprocess
import re
from collections import defaultdict

DELIMITER = "-#-#-#-#"


def get_commits(since_commit, until_commit):
    result = subprocess.run(
        ['git', 'log', f'{since_commit}..{until_commit}', f'--pretty=format:%H%n%B%n{DELIMITER}'],
        capture_output=True,
        text=True
    )
    commits = result.stdout.strip().split(f'\n{DELIMITER}\n')
    return [commit.strip() for commit in commits if commit.strip()]


def parse_changelog_entry(commit_description):
    changelog_pattern = re.compile(
        r'Changelog entry.*\n'
        r'.*Type: (.+)[\n]+'
        r'.*Component: (.+)[\n]+'
        r'(.+)[\n]+',
        re.DOTALL | re.IGNORECASE
    )

    match = changelog_pattern.search(commit_description)

    if match:
        return match.group(1).strip(), match.group(2).strip(), match.group(3).strip()
    else:
        if "Changelog entry" in commit_description:
            return "unparsed", "unparsed", commit_description
    return None


def generate_release_notes(since_commit, until_commit):
    commits = get_commits(since_commit, until_commit)
    release_notes = defaultdict(lambda: defaultdict(list))

    for commit in commits:
        commit_lines = commit.split('\n', 1)
        commit_hash = commit_lines[0].strip()
        commit_description = commit_lines[1] if len(commit_lines) > 1 else ''

        commit_description = commit_description.strip().strip(DELIMITER).strip()
        commit_description = re.sub(r"commit_hash:[\w\d]+", "", commit_description)
        commit_description, _, _ = commit_description.partition("---")
        commit_description, _, _ = commit_description.partition("Pull Request")
        changelog_entry = parse_changelog_entry(commit_description)
        if changelog_entry:
            commit_type, component, message = changelog_entry
            release_notes[component][commit_type].append((commit_hash, message))

    return release_notes


def format_release_notes(release_notes, components, ignore_unparsed):
    formatted_notes = []
    for component, types in release_notes.items():
        if component == "unparsed":
            continue
        if components and component not in components:
            continue
        formatted_notes.append(f'### {component}')
        if 'feature' in types:
            formatted_notes.append('Features:')
            for commit_hash, message in types.get('feature', []) + types.get('change', []):
                formatted_notes.append(f'- {message} (Commit: {commit_hash})')
        if 'fix' in types:
            formatted_notes.append('Fixes:')
            for commit_hash, message in types['fix']:
                formatted_notes.append(f'- {message} (Commit: {commit_hash})')
        for t in types:
            if t != "fix" and t != "feature":
                formatted_notes.append('Other:')
                for commit_hash, message in types[t]:
                    formatted_notes.append(f'- {message} (Commit: {commit_hash})')
        formatted_notes.append('')

    if "unparsed" in release_notes and not ignore_unparsed:
        formatted_notes.append("### UNPARSED")
        for commit_hash, message in release_notes["unparsed"]["unparsed"]:
            formatted_notes.append(f'- {message} (Commit: {commit_hash})')
            formatted_notes.append('')

    return '\n'.join(formatted_notes)


def main():
    parser = argparse.ArgumentParser(
        description="Generate github release notes"
    )

    parser.add_argument(
        "-s", "--since-commit",
        required=True,
        help="First commit"
    )

    parser.add_argument(
        "-u", "--until-commit",
        required=False,
        default="HEAD",
        help="Last commit"
    )

    parser.add_argument(
        "-c", "--component",
        action="append",
        help="Filter to components to display"
    )

    parser.add_argument(
        "-i", "--ignore-unparsed",
        action="store_true",
        default=False,
        help="Ignore unparsed commit with changelog entry"
    )

    args = parser.parse_args()

    release_notes = generate_release_notes(args.since_commit, args.until_commit)
    print(format_release_notes(release_notes, args.component, args.ignore_unparsed))


if __name__ == "__main__":
    main()
