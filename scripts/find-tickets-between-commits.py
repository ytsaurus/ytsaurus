#!/usr/bin/env python

import argparse
import requests
import logging
import sys
import re

def trunc(s):
    return s if len(s) < 80 else s[:80] + "..."

def retrieve(url):
    logging.info("Retrieving url `" + url + "`...")
    response = requests.get(url)
    data = response.json()
    return data

def get_commit_info(data):
    return (
        data["sha"], 
        data["commit"]["committer"]["date"], 
        data["commit"]["committer"]["name"],
        data["commit"]["message"])

def main():
    parser = argparse.ArgumentParser(description="Find tickets between two commits in the history")
    parser.add_argument("--silent", action="store_true", help="Do not log anything")
    parser.add_argument("BASE", type=str, action="store")
    parser.add_argument("HEAD", type=str, action="store")
    
    options = parser.parse_args()
    
    if options.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
    

    base = options.BASE
    head = options.HEAD

    logging.info("Base = `" + base + "`")
    logging.info("Head = `" + head + "`")

    commits_info = []

    logging.info("Fetching information about base commit...")
    url = "https://github.yandex-team.ru/api/v3/repos/yt/yt/commits/" + base
    data = retrieve(url)
    base_commit_info = get_commit_info(data)
    base_commit_date = base_commit_info[1]
    logging.info("Base commit date = " + base_commit_date)

    logging.info("Fetching information about head commit...")
    url = "https://github.yandex-team.ru/api/v3/repos/yt/yt/commits/" + head
    data = retrieve(url)
    head_commit_date = data["commit"]["committer"]["date"]
    logging.info("Head commit date = " + head_commit_date)
    
    if base_commit_date >= head_commit_date:
        logging.error("Base commit was made after the head commit. Shouldn't you swap the arguments?")
        sys.exit(1)

    while head != base:
        url = "https://github.yandex-team.ru/api/v3/repos/yt/yt/compare/{0}...{1}".format(base, head)
        data = retrieve(url)

        for commit in reversed(data["commits"]):
            sha = commit["sha"]
            if commits_info and commits_info[-1][0] == sha:
                # We don"t want duplicates in our list.
                continue
            commits_info.append(get_commit_info(commit))
        if not (data["commits"]):
            break
        else:
            logging.info("Retrieved commits, earliest commit: ")
            logging.info(" ".join(commits_info[-1]))
            if head == commits_info[-1][0]:
                break
            head = commits_info[-1][0]
    # After such procedure the base commit isn"t presented in the list,
    # so we manually add it.
    commits_info.append(base_commit_info)

    
    tickets_list = []

    for sha, date, author, message in commits_info:
        tickets = re.findall(r"\b(YT-\d*|YTADMIN-\d*)\b", message, flags=re.DOTALL)
        tickets_list += tickets
    print " ".join(tickets_list)

if __name__ == "__main__":
    main()
