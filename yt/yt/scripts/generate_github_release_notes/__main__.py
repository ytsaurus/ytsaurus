import argparse
import re
import subprocess
import sys

from collections import defaultdict

DELIMITER = "-#-#-#-#"


class ChangelogEntry:
    def __init__(self, commit_type, component, message):
        self.commit_type = commit_type
        self.component = component
        self.message = message[:-1] if message.endswith(".") else message


def parse_changelog_entry(commit_description):
    changelog_pattern = re.compile(
        r'Changelog entry.*\n'
        r'.*Type: (.+)[\n]+'
        r'.*Component: ([^\n]+)\n+'
        r'(.+)[\n]+',
        re.DOTALL | re.IGNORECASE
    )

    match = changelog_pattern.search(commit_description)

    if match:
        return ChangelogEntry(match.group(1).strip(), match.group(2).strip(), match.group(3).strip())
    else:
        if "Changelog entry" in commit_description:
            return ChangelogEntry("unparsed", "unparsed", commit_description)
    return None


class Commit:
    def __init__(self, commit_log, repo):
        commit_lines = commit_log.split('\n', 1)
        self.hash = commit_lines[0].strip()
        self.name = commit_lines[1].strip().split('\n')[0]
        commit_description = commit_lines[1] if len(commit_lines) > 1 else ''

        commit_description = commit_description.strip().strip(DELIMITER).strip()
        commit_description = re.sub(r"commit_hash:[\w\d]+", "", commit_description)
        commit_description, _, _ = commit_description.partition("---")
        commit_description, _, _ = commit_description.partition("Pull Request")

        self.description = commit_description

        self.changelog_entry = parse_changelog_entry(self.description)
        self.repo = repo

    def get_link(self):
        return f"[{self.hash[:7]}](https://github.com/ytsaurus/{self.repo}/commit/{self.hash})"

    def get_formatted(self, add_name=False):
        result = ""
        if self.changelog_entry:
            result += f"- {self.changelog_entry.message}"

        if add_name:
            result += f"; {self.name}"

        result += f", {self.get_link()}."
        return result


def get_commits(repo, since_commit, until_commit):
    result = subprocess.run(
        ['git', 'log', f'{since_commit}..{until_commit}', f'--pretty=format:%H%n%B%n{DELIMITER}'],
        capture_output=True,
        text=True
    )
    commits = result.stdout.strip().split(f'\n{DELIMITER}\n')
    return [Commit(commit.strip(), repo) for commit in commits if commit.strip()]


def filter_commits(commits, commits_prev_release):
    result_commits = []
    removed_commits = []

    for commit in commits:
        remove = False
        for prev_commit in commits_prev_release:
            if commit.name in prev_commit.name:
                removed_commits += [(commit, prev_commit)]
                remove = False
                break
        if not remove:
            result_commits += [commit]
    return result_commits, removed_commits


class ReleaseNotes:
    def __init__(self, args):
        self.args = args
        self.by_component = defaultdict(lambda: defaultdict(list))
        self.removed_commits = []

    def get_formatted(self):
        formatted_notes = []
        for component, type_to_commits in self.by_component.items():
            if component == "unparsed":
                continue
            if self.args.components and component not in self.args.components:
                continue
            if self.args.ignore_components and component in self.args.ignore_components:
                continue
            formatted_notes.append(f"### {component}")

            if "feature" in type_to_commits or "change" in type_to_commits:
                formatted_notes.append("Features:")
                for commit in type_to_commits.get("feature", []) + type_to_commits.get("change", []):
                    formatted_notes.append(commit.get_formatted())

            if "fix" in type_to_commits:
                formatted_notes.append("Fixes:")
                for commit in type_to_commits["fix"]:
                    formatted_notes.append(commit.get_formatted())

            for t in type_to_commits:
                if t != "fix" and t != "feature" and t != "change":
                    formatted_notes.append("Other:")
                    for commit in type_to_commits[t]:
                        formatted_notes.append(commit.get_formatted())
            formatted_notes.append("")

        if "unparsed" in self.by_component and not self.args.ignore_unparsed:
            formatted_notes.append("### UNPARSED")
            for commit in self.by_component["unparsed"]["unparsed"]:
                formatted_notes.append(commit.get_formatted())
                formatted_notes.append("")

        if self.removed_commits and self.args.print_removed:
            formatted_notes.append("### REMOVED")
            for commit, prev_commit in self.removed_commits:
                formatted_notes.append(commit.get_formatted(add_name=True))
                formatted_notes.append(prev_commit.get_formatted(add_name=True))
                formatted_notes.append("")

        return '\n'.join(formatted_notes)


def generate_release_notes(args):
    release_notes = ReleaseNotes(args)

    commits = get_commits(args.repo, args.since_commit, args.until_commit)
    if args.since_commit_prev_release and args.until_commit_prev_release:
        commits_prev_release = get_commits(args.repo, args.since_commit_prev_release, args.until_commit_prev_release)
        commits, release_notes.removed_commits = filter_commits(commits, commits_prev_release)

    for commit in commits:
        if commit.changelog_entry:
            release_notes.by_component[commit.changelog_entry.component][commit.changelog_entry.commit_type].append(commit)

    return release_notes


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
        "-sp", "--since-commit-prev-release",
        required=False,
        help="First commit of the previous release"
    )

    parser.add_argument(
        "-up", "--until-commit-prev-release",
        required=False,
        help="Last commit of the previous release"
    )

    parser.add_argument(
        "-c", "--component",
        action="append",
        dest="components",
        help="Filter to components to display"
    )

    parser.add_argument(
        "-ic", "--ignore-component",
        action="append",
        dest="ignore_components",
        help="Components to ignore"
    )

    parser.add_argument(
        "-r", "--repo",
        required=False,
        default="ytsaurus",
        help="Github repository in ytsaurus organization"
    )

    parser.add_argument(
        "-i", "--ignore-unparsed",
        action="store_true",
        default=False,
        help="Ignore unparsed commit with changelog entry"
    )

    parser.add_argument(
        "-pr", "--print-removed",
        action="store_true",
        default=False,
        help="Print removed commits"
    )

    args = parser.parse_args()

    release_notes = generate_release_notes(args)
    print("Parsed components: ", file=sys.stderr)
    for component in release_notes.by_component:
        print(f"* '{component}'", file=sys.stderr)
    print(release_notes.get_formatted())


if __name__ == "__main__":
    main()
