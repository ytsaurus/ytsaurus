import argparse
from dataclasses import dataclass
from typing import List


PIPE_FROM_RECIPE_TO_RUNNER = "from_recipe_to_yt_env.pipe"
PIPE_FROM_RUNNER_TO_RECIPE = "from_yt_env_to_recipe.pipe"


def get_config_path_from_args(args: List[str]) -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--yt-env-config",
        help="YSON file containing options for YTEnvSetup")
    return parser.parse_args(args).yt_env_config


@dataclass
class Pipe:
    input_fifo: str
    output_fifo: str

    def wait(self, expected: str) -> None:
        # TODO(kvk1920): it's better to open pipe once on program start. It can
        # be a bit complicated to avoid deadlocks, but it should be done.
        # TODO(kvk1920): consider to use O_NONBLOCK.
        with open(self.input_fifo, "r") as f:
            # TODO(kvk1920): read with timeout.
            message = f.readline().strip()
            if message != expected:
                # YT env runner can report its exception via sending message.
                # Since message is a single line every "\n" in exception
                # traceback is replaced with "\n". Replace it back to make
                # tracebacks more readable.
                message = message.replace("\\n", "\n")
                raise RuntimeError(f"Unexpected message: {message}")

    def send(self, message: str) -> None:
        with open(self.output_fifo, "a") as f:
            f.write(message)
            f.write("\n")
            f.flush()
