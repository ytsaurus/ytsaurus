import yt.library.ytprof.proto.profile_pb2 as profile_pb2

from yt_commands import wait

import yt.packages.requests as requests

import os


def save_ytprof_profile(port, profile, target_path_gz):
    try:
        responce = requests.get(f"http://localhost:{port}/ytprof/{profile}", stream=True)

        if responce.status_code == 200:
            with open(target_path_gz, 'wb') as f:
                for chunk in responce.iter_content(chunk_size=1024):
                    f.write(chunk)
            return True
        else:
            return False
    except Exception:
        return False


def unzip_profile(target_path_gz):
    assert os.system(f"gzip -dfk {target_path_gz}") == 0


def process_ytprof_heap_profile(path_to_run, port, allocation_tags):
    target_path_gz = os.path.join(path_to_run, f"{port}-heap.pb.gz")
    target_path_pb = os.path.join(path_to_run, f"{port}-heap.pb")

    wait(lambda: save_ytprof_profile(port, "heap", target_path_gz))
    unzip_profile(target_path_gz)
    assert os.path.exists(target_path_pb), "Profile not found"

    profile_message = profile_pb2.Profile()

    with open(target_path_pb, "rb") as file:
        profile_message.ParseFromString(file.read())

    memory_usage = {}
    string_table = profile_message.string_table

    for sample in profile_message.sample:
        memory = sample.value[1]
        for label in sample.label:
            key = string_table[label.key]
            value = string_table[label.str] or label.num
            if key in allocation_tags:
                memory_usage.setdefault(key, {})
                memory_usage[key][value] = memory + memory_usage[key].setdefault(value, 0)

    return memory_usage
