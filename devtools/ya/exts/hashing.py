import os
import hashlib
from hashlib import md5  # hashlib.md5 deprecated
import six

import exts.os2


def git_like_hash_with_size(filepath, follow_links=False):
    """
    Calculate git like hash for path
    """

    if not follow_links and os.path.islink(filepath):
        return git_like_hash_str_with_size(os.readlink(filepath))

    sha = hashlib.sha1()

    file_size = 0

    with open(filepath, 'rb') as f:
        while True:
            block = f.read(2**16)

            if not block:
                break

            file_size += len(block)
            sha.update(block)

    sha.update(six.ensure_binary('\0'))
    sha.update(six.ensure_binary(str(file_size)))

    return sha.hexdigest(), file_size


def git_like_hash_str_with_size(s):
    """
    Calculate git like hash for string
    """
    s_bytes = six.ensure_binary(s, errors='backslashreplace')
    sha = hashlib.sha1()
    sha.update(s_bytes)
    sha.update(b'\0')
    sha.update(six.ensure_binary(str(len(s))))

    return sha.hexdigest(), len(s)


def git_like_hash(filepath):
    """
    Calculate git like hash for path
    """
    file_hash, file_size = git_like_hash_with_size(filepath)

    return file_hash


def sum_hashes(hashes):
    """
    Calculate sum of hashes
    """
    current_hash = md5()

    for hash_ in hashes:
        current_hash.update(six.ensure_binary(hash_) + b'#')

    return six.ensure_str(current_hash.hexdigest())


def md5_value(string):
    """
    Calculate md5 for string value
    """
    return six.ensure_str(md5(six.ensure_binary(string)).hexdigest())


def file_hash(filename, hash_obj, chunk_size=1 << 16):
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            hash_obj.update(chunk)

    return six.ensure_str(hash_obj.hexdigest())


def md5_file(filename, hash_md5=None):
    return file_hash(filename, hash_md5 or hashlib.md5())


def md5_path(path, include_dir_layout=False):
    """
    Calculate md5 for path
    """
    res = md5()

    if os.path.isfile(path):
        md5_file(path, res)
    else:
        for root, dirs, files in exts.os2.fastwalk(path):
            dirs.sort()
            files.sort()
            for file_name in files:
                abs_file = os.path.join(root, file_name)
                md5_file(abs_file, res)
                if include_dir_layout:
                    res.update(six.ensure_binary(os.path.relpath(abs_file, path)))

    return six.ensure_str(res.hexdigest())


try:
    import cityhash

    def fast_hash(x):
        return str(cityhash.hash64(six.ensure_binary(x)))

    def fast_filehash(x):
        return str(cityhash.filehash64(six.ensure_binary(x)))

except (ImportError, AttributeError):
    fast_hash = md5_value
    fast_filehash = git_like_hash
