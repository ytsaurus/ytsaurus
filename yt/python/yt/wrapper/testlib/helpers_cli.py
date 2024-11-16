import subprocess
import sys
from typing import IO


class YtCli:
    def __init__(self, env: dict[str, str], cwd: str, replace: dict[str, list[str]]):
        self.env = env
        self.cwd = cwd
        self.replace = replace
        self.processes_to_kill: list[subprocess.Popen] = []

    def check_output(self, args: list[str], stdin: str | int = subprocess.PIPE, timeout: float | None = 90.0) -> bytes:
        completed_process = self.run(args, stdin, timeout=timeout)
        if completed_process.returncode != 0:
            self._write_stderr(args, stdin, completed_process.stdout, completed_process.stderr)
            raise subprocess.CalledProcessError(completed_process.returncode, completed_process.args, completed_process.stdout, completed_process.stderr)
        return completed_process.stdout

    def run(self, args: list[str], stdin: str | int = subprocess.PIPE, timeout: float | None = 90.0) -> subprocess.CompletedProcess:
        args = self._patch_first_arg(args)
        completed_process = self._execute_command(args, stdin, timeout=timeout)
        return completed_process

    def run_in_background(self, args: list[str], stdin: int | IO | None = subprocess.PIPE) -> subprocess.Popen:
        args = self._patch_first_arg(args)
        process = subprocess.Popen(
            args,
            stdin=stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=self.env,
            cwd=self.cwd
        )
        self.processes_to_kill.append(process)
        return process

    def _patch_first_arg(self, args: list[str]) -> list[str]:
        if args[0] in self.replace:
            args = self.replace[args[0]] + args[1:]
        return args

    def _execute_command(self, args: list[str], stdin: str | int = subprocess.PIPE, timeout: float | None = 90.0) -> subprocess.CompletedProcess:
        if isinstance(stdin, str):
            popen_stdin = subprocess.PIPE
        else:
            popen_stdin = stdin
        process = subprocess.Popen(
            args,
            stdin=popen_stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=self.env,
            cwd=self.cwd
        )
        try:
            if isinstance(stdin, str):
                stdout, stderr = process.communicate(input=stdin.encode("utf-8"), timeout=timeout)
            else:
                stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.CalledProcessError:
            return subprocess.CompletedProcess(process.args, process.returncode, process.stdout, process.stderr)
        except Exception as ex:
            raise ex
        return subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)

    def _write_stderr(self, args: list[str], stdin: str | int | None, stdout: bytes, stderr: bytes) -> None:
        tail_len = 2048
        print("Executed command", args, file=sys.stderr)
        if stdin:
            if isinstance(stdin, str):
                print("STDIN:", stdin[-tail_len:], file=sys.stderr)
            else:
                print("STDIN:", stdin, file=sys.stderr)
        print("Process stdout", stdout[-tail_len:], file=sys.stderr)
        print("Process stderr", stderr[-tail_len:], file=sys.stderr)

    def __del__(self):
        for process in self.processes_to_kill:
            process.kill()
