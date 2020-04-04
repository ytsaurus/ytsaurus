import argparse
import subprocess

from collections import Counter, namedtuple


Function = namedtuple("Function", ["need_authors", "name", "first_line"])


def get_author_counts(file_path, first, last):
    blame = subprocess.check_output([
        "git",
        "blame",
        file_path,
        "-L", "{},{}".format(first + 1, last + 1),
        "-e",
    ])
    c = Counter()
    for x in blame.split():
        if x.endswith("@yandex-team.ru>"):
            c[x[2:-len("@yandex-team.ru>")]] += 1
    return c


def add_authors_marks(file_path, threshold):
    with open(file_path) as f:
        orig_lines = list(f.readlines())
        lines = list(map(str.strip, orig_lines))
    functions = []
    for li, l in enumerate(lines):
        if l.startswith("def test"):
            name = l[4:].split('(')[0]
            start = li - 1
            need_authors = True
            while start >= 0 and lines[start].startswith("@"):
                if lines[start].startswith("@authors"):
                    need_authors = False
                    break
                start -= 1
            functions.append(Function(name=name, first_line=start + 1, need_authors=need_authors))

    authors = []
    for i, function in enumerate(functions):
        if not function.need_authors:
            authors.append(None)
            continue
        if i + 1 < len(functions):
            last = functions[i+1].first_line - 1
        else:
            last = len(lines)-1
        counts = get_author_counts(file_path, function.first_line, last)
        common = list(filter(
            lambda (_, lines): float(lines) / (last - function.first_line + 1) >= threshold,
            counts.iteritems()
        ))
        if not common:
            common = c.most_common(n=3)
        authors.append([a for a, _ in common])

    res_lines = []
    fun_i = 0
    for i, l in enumerate(orig_lines):
        if fun_i < len(functions) and functions[fun_i].first_line == i and functions[fun_i].need_authors:
            spaces = len(l) - len(l.lstrip())
            authors_str = ", ".join('"{}"'.format(author) for author in authors[fun_i])
            res_lines.append("{}@authors({})\n".format(" " * spaces, authors_str))
            fun_i += 1
        res_lines.append(l)

    with open(file_path, "w") as f:
        for l in res_lines:
            f.write(l)
    print("Processed {}, + {} lines".format(fname, len(res_lines) - len(orig_lines)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "files",
        nargs="+",
        help="Files to patch",
    )
    parser.add_argument(
        "--threshold",
        help="Ratio of lines in function blame needed to add a commiter to @authors",
        default=0.3,
    )
    args = parser.parse_args()

    for fname in args.files:
        add_authors_marks(fname, args.threshold)

