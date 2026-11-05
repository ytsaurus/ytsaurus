from .run_operation_commands import run_operation
from .operation_commands import TimeWatcher
from .spec_builders import VanillaSpecBuilder
from .config import get_config
from .cli_helpers import print_to_output, populate_argument_help

import datetime
import json
import os
import re
import subprocess
import sys

from typing import Optional, Tuple, TypedDict, List, Dict, Any


class _ClusterEnvironmentType(TypedDict):
    cluster: str
    runtime_python_version: Optional[Tuple[int, int, int]]
    runtime_ubuntu_codename: Optional[str]
    runtime_glibc: Optional[Tuple[int, int]]
    docker_layers_support: Optional[bool]


def _run_docker(params: List[str]) -> str:
    docker_result = subprocess.run(
        params,
        capture_output=True,
        text=True,
    )
    exit_code = docker_result.returncode

    if exit_code:
        print(f"params: {params}", file=sys.stderr)
        print(docker_result.stderr, file=sys.stderr)
        return None

    return docker_result.stdout


def get_cluster_environment(client) -> _ClusterEnvironmentType:
    runtime_python_version: Optional[Tuple[int, int, int]] = None
    runtime_ubuntu_codename: Optional[str] = None
    runtime_glibc: Optional[Tuple[int, int]] = None
    docker_layers_support: Optional[bool] = None

    op_python = run_operation(
        VanillaSpecBuilder().max_failed_job_count(1).task("get_env", {
            "max_failed_job_count": 1,
            "command": "python3 --version >&2; lsb_release -a >&2; python3 -c \"import platform; print(platform.libc_ver())\" >&2",
            "job_count": 1,
        }),
        sync=False,
        client=client,
    )
    op_docker = run_operation(
        VanillaSpecBuilder().max_failed_job_count(1).task("check_docker", {
            "max_failed_job_count": 1,
            "command": "sleep 1",
            "job_count": 1,
            "docker_image": "docker.io/ubuntu:24.04",
        }),
        sync=False,
        client=client,
    )
    op_python.wait(print_progress=False, check_result=True)
    op_time_watcher = TimeWatcher(min_interval=1, max_interval=10, slowdown_coef=0.2)
    for _ in op_docker.get_state_monitor(op_time_watcher):
        pass

    op_python_stderr = op_python.get_jobs_with_error_or_stderr()[0].get("stderr")
    if op_python_stderr is None:
        raise RuntimeError("Can't get cluster info")

    re_match = re.search(r"Python (\d+)\.(\d+)\.(\d+)", op_python_stderr)
    if re_match:
        runtime_python_version = (int(re_match.group(1)), int(re_match.group(2)), int(re_match.group(3)))

    re_match = re.search(r"Codename:\t(\S+)", op_python_stderr)
    if re_match:
        runtime_ubuntu_codename = re_match.group(1)

    re_match = re.search(r"'glibc', '(\d+)\.(\d+)'", op_python_stderr)
    if re_match:
        runtime_glibc = (int(re_match.group(1)), int(re_match.group(1)))

    docker_layers_support = op_docker.get_state().is_finished()

    return {
        "cluster": get_config(client)["proxy"]["url"],
        "runtime_python_version": runtime_python_version,
        "runtime_ubuntu_codename": runtime_ubuntu_codename,
        "runtime_glibc": runtime_glibc,
        "docker_layers_support": docker_layers_support,
    }


def _add_parser_get_cluster_environment(top_parser, client):
    def _action(**kwargs):
        data = get_cluster_environment(client)
        result = {
            "cluster": data["cluster"],
            "python": data["runtime_python_version"],
            "ubuntu_codename": data["runtime_ubuntu_codename"],
            "glibc": data["runtime_glibc"],
            "is_docker_supported": bool(data["docker_layers_support"]),
        }
        result = "\n".join(
            k.capitalize().replace("_", " ") + ": " + ('.'.join(map(str, v)) if isinstance(v, Tuple) else str(v))
            for k, v in result.items()
        )
        print_to_output(result)
    parser = top_parser.add_parser("get-cluster-env", help="Get cluster OS/Python version")
    parser.set_defaults(func=_action)


def _get_cluster_alias(proxy) -> str:
    cluster_filtered = re.sub(r"\W+", "_", proxy.lower())
    cluster_filtered = re.sub(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "", cluster_filtered)
    return cluster_filtered


def _get_default_docker_image_name(client) -> str:
    cluster_filtered = _get_cluster_alias(proxy=get_config(client)["proxy"]["url"])
    return f"cluster_env_{os.getlogin().lower()}_{cluster_filtered}:latest"


def get_dockerfile_for_cluster(
    cluster_env: Dict[str, Any],
    image_name,
    user_base_image: str = None,
    python_version: str = None,
    with_modules: List[str] = None,
    with_local_modules: bool = False,
    client=None,
) -> str:
    cluster_base_image = f"ubuntu:{cluster_env['runtime_ubuntu_codename']}"

    if not with_modules:
        with_modules = []

    if with_local_modules:
        pip_result = subprocess.run(["pip", "freeze", "--exclude-editable"], capture_output=True)
        pip_result.stdout
        with_modules.extend(pip_result.stdout.decode().strip().split("\n"))

    print_to_output(f"\tusing ubuntu: {user_base_image or cluster_base_image!r}, python: {python_version or '.'.join(map(str, cluster_env['runtime_python_version']))!r}")

    return f"""# autogenerated for cluster {cluster_env["cluster"].upper()}
# {datetime.datetime.now().isoformat()}
# buid:
# $ docker build --platform linux/amd64 -t {image_name} .
# test:
# $ docker run --platform linux/amd64 --rm -e YT_TOKEN="$YT_TOKEN" -e YT_PROXY="$YT_PROXY" -v {os.path.expanduser('~')}:{os.path.expanduser('~')} -it {image_name} bash
# run:
# $ docker run --platform linux/amd64 --rm -e USER=$USER -e YT_TOKEN_PATH="${{YT_TOKEN_PATH:-$HOME/.yt/token}}" -e YT_TOKEN="$YT_TOKEN" -e YT_PROXY="$YT_PROXY" -v $HOME:$HOME -it {image_name} bash -ic "python $PWD/my_script.py"

FROM --platform=linux/amd64 {user_base_image or cluster_base_image}
LABEL image-type=yt-client-image target-yt-cluster={_get_cluster_alias(proxy=cluster_env["cluster"])} use-base-image={user_base_image or ''}
ENV CLUSTER_PYTHON="{python_version or ".".join(map(str, cluster_env["runtime_python_version"]))}"

RUN apt-get update && apt-get install -y lsb-release python3-venv libffi-dev python3-dev git curl python-is-python3 vim gcc make \\
libssl-dev liblzma-dev libsqlite3-dev libbz2-dev libncurses5-dev libreadline-dev
RUN curl https://pyenv.run | bash && test -d $HOME/.pyenv/bin

RUN echo 'export PYENV_ROOT="$HOME/.pyenv"\\n \\
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"\\n \\
eval "$(pyenv init - bash)"\\n \\
pyenv activate  my_yt_app_'$CLUSTER_PYTHON'\\n \\
export YT_BASE_LAYER="{user_base_image or ''}"\\n' \\
>> /root/.bashrc

RUN echo '{{"user_base_image": "{user_base_image or ''}"}}' > /_yt_image_meta.json

RUN export PYENV_ROOT="$HOME/.pyenv"; \\
test -d $PYENV_ROOT/bin && export PATH="$PYENV_ROOT/bin:$PATH"; \\
eval "$(pyenv init - bash)"; \\
pyenv install $CLUSTER_PYTHON && pyenv virtualenv  $CLUSTER_PYTHON  my_yt_app_$CLUSTER_PYTHON && \\
mkdir ~/.yt && \\
pyenv activate  my_yt_app_$CLUSTER_PYTHON && pip install ipython;
{("RUN export PYENV_ROOT=$HOME/.pyenv; export PATH=$PYENV_ROOT/bin:$PATH; eval "'"'"$(pyenv init - bash)"'"'"; pyenv activate  my_yt_app_$CLUSTER_PYTHON && pip install " + " ".join(with_modules)) if with_modules else ""}
"""  # noqa


def _check_docker(image_name: str):
    out = _run_docker([
        "docker",
        "run",
        "--platform=linux/amd64",
        "--rm",
        image_name,
        "bash", "-c", "apt-get update >/dev/null 2>&1 && apt-get install -y curl >/dev/null 2>&1 && arch && curl -s https://github.com/ -o /dev/null && echo 'github - ok'",
    ])
    assert out, f"Can't check docker image {image_name!r}"
    out = out.splitlines()
    assert out[0] == "x86_64", "Configure docker for x86  architecture."
    assert len(out) == 2 and out[1] == "github - ok", "No internet connection from docker container. Configure docker."


def _build_image_for_cluster(dockerfile: str, image_name: str):
    params = [
        "docker",
        "build",
        "--platform=linux/amd64",
        "-t",
        f"{image_name}",
        "-",
    ]

    process = subprocess.Popen(
        params,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, stderr = process.communicate(input=dockerfile)
    exit_code = process.wait()
    if exit_code:
        print(stdout, file=sys.stdout)
        print(stderr, file=sys.stderr)
        sys.exit(exit_code)

    return image_name


def _add_parser_get_dockerfile_for_cluster(top_parser, client):
    def _action(**kwargs):
        image_name = kwargs.get("image_name") or _get_default_docker_image_name(client=client)

        print_to_output("- Run operations to check cluster environment...")
        cluster_env = get_cluster_environment(client)

        _check_docker(kwargs.get("base_image") or f"ubuntu:{cluster_env['runtime_ubuntu_codename']}")

        print_to_output("- Create dockerfile...")
        data = get_dockerfile_for_cluster(
            cluster_env=cluster_env,
            image_name=image_name,
            user_base_image=kwargs.get("base_image"),
            python_version=kwargs.get("python_version"),
            with_modules=kwargs.get("with_modules"),
            with_local_modules=kwargs.get("with_local_modules"),
            client=client,
        )
        if kwargs.get("print_dockerfile_only"):
            print_to_output(data)
            return

        print_to_output("- Build docker image (may take up to 10 minutes)...")
        builded_image = _build_image_for_cluster(
            dockerfile=data,
            image_name=image_name,
        )

        print_to_output(f"- All done\nBuilded docker image:\nimage={builded_image}")

    parser = top_parser.add_parser("prepare", help="Create dockerfile for cluster OS/Python version and build image")
    parser.add_argument("--image-name", help="Target image name")
    parser.add_argument("--base-image", help="Base image. For example - \"docker.io/library/ubuntu:trusty\". By default - as on target cluster")
    parser.add_argument("--python-version", help="Python version. For example \"3.13\". By default - as on target cluster")
    parser.add_argument("--print-dockerfile-only", action="store_true", help="Only create Dockerfile")
    parser.add_argument("--with-modules", nargs="*", dest="with_modules", help="Python module to install into container")
    parser.add_argument("--with-local-modules", action="store_true", help="Collect installed modules from current environment")
    parser.set_defaults(func=_action)


def docker_run_script(image_name, script_path, client):
    params = [
        "docker",
        "run",
        "--platform=linux/amd64",
        "--rm",
        "-e", f"USER={os.environ.get('USER', '')}",
        "-e", f"YT_TOKEN_PATH={os.environ.get('YT_TOKEN_PATH', '') or (os.environ.get('HOME', '') + '/.yt/token')}",
        "-e", f"YT_TOKEN={os.environ.get('YT_TOKEN', '')}",
        "-e", f"YT_PROXY={get_config(client)['proxy']['url']}",
        "-e", f"YT_LOG_LEVEL={os.environ.get('YT_LOG_LEVEL', '')}",
        "-e", f"YT_CONFIG_PATCHES={os.environ.get('YT_CONFIG_PATCHES', '')}",
        "-e", f"YT_CONFIG_PATH={os.environ.get('YT_CONFIG_PATH', '')}",
        "-e", f"YT_CONFIG_PROFILE={os.environ.get('YT_CONFIG_PROFILE', '')}",
        "-v", f"{os.environ.get('HOME', '')}:{os.environ.get('HOME', '')}",
        "-it", image_name, "bash", "-ic", f"python {os.path.abspath(script_path)}",
    ]

    process = subprocess.Popen(
        params,
        stdin=sys.stdin,
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
    exit_code = process.wait()
    if exit_code:
        sys.exit(exit_code)


def _add_parser_docker_run_script(top_parser, client):
    if os.environ.get("YT_PROXY") and client and client.config["proxy"]["url"]:
        os.environ["YT_PROXY"] = client.config["proxy"]["url"]

    def _action(**kwargs):
        docker_run_script(
            image_name=kwargs.get("image_name") or _get_default_docker_image_name(client=client),
            script_path=kwargs.get("script_path"),
            client=client,
        )
    parser = top_parser.add_parser("run", help="Run script in specific docker contaner")
    parser.add_argument("--image-name", help="Docker image")
    parser.add_argument("script_path", help="Path to script")
    parser.set_defaults(func=_action)


def docker_list_images(all_images: bool = False, client=None):
    params = [
        "docker",
        "images",
        "--format",
        "{{json .}}",
        "--filter",
        "label=image-type=yt-client-image",
    ]

    cluster_for_filter = f"{_get_cluster_alias(proxy=get_config(client)['proxy']['url'])}" if get_config(client)["proxy"]["url"] and not all_images else None

    if cluster_for_filter:
        params.extend([
            "--filter",
            f"label=target-yt-cluster={cluster_for_filter}",
        ])

    docker_result = _run_docker(params)

    if docker_result is None:
        sys.exit(1)

    if cluster_for_filter:
        print_to_output(f"# for cluster {cluster_for_filter!r}")

    if not docker_result:
        print_to_output("# ...no images\n")
        return

    result = []
    for line in docker_result.strip().split("\n"):
        image = json.loads(line)
        if image["Repository"] and image["Repository"] != "<none>":
            result.append(f"image=\"{image['Repository']}:{image['Tag']}\"\tsize=\"{image['Size']}\"\tcreated=\"{image['CreatedAt']}\"")
    print_to_output("# images:\n" + "\n".join(result))


def _add_parser_docker_list_images(top_parser, client):
    def _action(**kwargs):
        docker_list_images(
            all_images=kwargs.get("all_images"),
            client=client,
        )
    parser = top_parser.add_parser("list", help="List images for scrupt run (by default - for current cluster)")
    parser.add_argument("--all", action="store_true", dest="all_images", help="Show images for all clusters")
    parser.set_defaults(func=_action)


def docker_add_package(image_name, command, client):
    tmp_container = "0_tmp_container"

    params = [
        "docker",
        "run",
        "--platform=linux/amd64",
        "--name",
        tmp_container,
        "-e", f"USER={os.environ.get('USER', '')}",
        "-e", f"YT_TOKEN_PATH={os.environ.get('YT_TOKEN_PATH', '') or (os.environ.get('HOME', '') + '/.yt/token')}",
        "-e", f"YT_TOKEN={os.environ.get('YT_TOKEN', '')}",
        "-e", f"YT_PROXY={get_config(client)['proxy']['url']}",
        "-v", f"{os.environ.get('HOME', '')}:{os.environ.get('HOME', '')}",
        image_name,
        "bash", "-ic", command,
    ]
    run_stdout = _run_docker(params)
    if run_stdout is None:
        _run_docker(["docker", "rm", tmp_container])
        return

    params = [
        "docker",
        "commit",
        tmp_container,
        image_name,
    ]
    if _run_docker(params) is None:
        _run_docker(["docker", "rm", tmp_container])
        return

    if _run_docker(["docker", "rm", tmp_container]) is None:
        return

    return run_stdout


def _add_parser_docker_add_package(top_parser, client):
    def _action(**kwargs):
        result = docker_add_package(
            image_name=kwargs.get("image-name") or _get_default_docker_image_name(client=client),
            command=f"pip install {' '.join(kwargs.get('packages'))}" if kwargs.get("packages") else kwargs.get("bash"),
            client=client,
        )
        print_to_output(result)
    parser = top_parser.add_parser("install", help="List images for scrupt run")
    parser.add_argument("--image-name", help="Docker image name to modify")
    parser.add_argument("--pip", nargs="*", dest="packages", help="Python package to intsall")
    parser.add_argument("--bash", help="Bash command to execute")
    parser.set_defaults(func=_action)


def add_devtools_parser(root_subparsers):
    parser = populate_argument_help(root_subparsers.add_parser("devtools", description="Devtools commands"))
    devtools_subparser = parser.add_subparsers()
    image_parser = populate_argument_help(devtools_subparser.add_parser("image", description="Run operations in docker"))
    image_subparsers = image_parser.add_subparsers()

    _add_parser_get_cluster_environment(image_subparsers, client=None)
    _add_parser_get_dockerfile_for_cluster(image_subparsers, client=None)
    _add_parser_docker_run_script(image_subparsers, client=None)
    _add_parser_docker_list_images(image_subparsers, client=None)
    _add_parser_docker_add_package(image_subparsers, client=None)
