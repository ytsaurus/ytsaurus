import argparse
import hashlib
import json
import sys
import tarfile
import time
from pathlib import Path
from urllib.request import urlopen

LOCK_PATH = Path(__file__).with_name("server.lock.json")
DOWNLOAD_BLOCK_SIZE = 1024 * 1024


def file_sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def acquire_archive(url: str, destination, deadline_seconds: int = 600) -> str:
    destination = Path(destination)
    deadline = time.monotonic() + deadline_seconds
    with urlopen(url, timeout=60) as source, destination.open("wb") as output:
        while block := source.read(DOWNLOAD_BLOCK_SIZE):
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Public YDB archive download exceeded {deadline_seconds} seconds")
            output.write(block)
    return file_sha256(destination)


def extract_binary(archive, destination, member_name="ydbd") -> str:
    destination = Path(destination)
    with tarfile.open(archive) as package:
        candidates = [
            member
            for member in package.getmembers()
            if member.isfile() and Path(member.name).parent.name == "bin" and Path(member.name).name == member_name
        ]
        if len(candidates) != 1:
            raise RuntimeError(f"Expected exactly one regular bin/{member_name} archive member")
        with package.extractfile(candidates[0]) as source, destination.open("wb") as output:
            while block := source.read(DOWNLOAD_BLOCK_SIZE):
                output.write(block)
    destination.chmod(0o700)
    return file_sha256(destination)


def acquire_locked_binary(lock, directory, deadline_seconds=600):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    archive = directory / "ydbd.tar.gz"
    archive_sha256 = acquire_archive(lock["archiveUrl"], archive, deadline_seconds)
    if archive_sha256 != lock["archiveSha256"]:
        raise RuntimeError("Downloaded YDB archive does not match server.lock.json")
    binary = directory / "ydbd"
    if extract_binary(archive, binary) != lock["binarySha256"]:
        raise RuntimeError("Extracted ydbd does not match server.lock.json")
    cli = directory / "ydb"
    if extract_binary(archive, cli, member_name="ydb") != lock["cliSha256"]:
        raise RuntimeError("Extracted ydb CLI does not match server.lock.json")
    return binary, cli


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url")
    parser.add_argument("--lock", default=str(LOCK_PATH))
    parser.add_argument("--deadline-seconds", type=int, default=600)
    parser.add_argument("directory")
    arguments = parser.parse_args()
    if arguments.url:
        directory = Path(arguments.directory)
        directory.mkdir(parents=True, exist_ok=True)
        archive = directory / "ydbd.tar.gz"
        archive_sha256 = acquire_archive(arguments.url, archive, arguments.deadline_seconds)
        identities = {
            "archiveUrl": arguments.url,
            "archiveSha256": archive_sha256,
            "binarySha256": extract_binary(archive, directory / "ydbd"),
            "cliSha256": extract_binary(archive, directory / "ydb", member_name="ydb"),
        }
        json.dump(identities, sys.stdout)
        sys.stdout.write("\n")
        return
    lock = json.loads(Path(arguments.lock).read_text())
    for path in acquire_locked_binary(lock, arguments.directory, arguments.deadline_seconds):
        print(path)


if __name__ == "__main__":
    main()
