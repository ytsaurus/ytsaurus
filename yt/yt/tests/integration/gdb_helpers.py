from yt_commands import create_tmpdir, print_debug, wait
import os

PTRACE_SCOPE_CHECK_RESULT_CACHED = None


def _check_ptrace_scope():
    global PTRACE_SCOPE_CHECK_RESULT_CACHED
    if PTRACE_SCOPE_CHECK_RESULT_CACHED is not None:
        return PTRACE_SCOPE_CHECK_RESULT_CACHED
    ptrace_scope_file = open("/proc/sys/kernel/yama/ptrace_scope", 'r')
    try:
        content = ptrace_scope_file.read()
        result = content.strip() == "0"
    except IOError:
        print_debug("Cannot read /proc/sys/kernel/yama/ptrace_scope, assuming old-fashioned Linux value of 0.")
        result = True
    if not result:
        raise ValueError("Invalid YAMA ptrace_scope. In order for gdb to attach properly, please set "
                         "/proc/sys/kernel/yama/ptrace_scope to 0 (current value is {}). Refer to man 2 ptrace for "
                         "further details.".format(content))
    PTRACE_SCOPE_CHECK_RESULT_CACHED = result


def attach_gdb(pid, ex=None, autoresume=True):
    """
    Attach GDB optionally running some commands on startup. GDB will be started in newly created
    tmux window (seems convenient to max42@, but feel free to introduce other ways of starting GDB process :)
    Note that while GDB attaches to the process, execution of test is suspended.
    When GDB is attached and setup (e.g. breakpoints or watch expressions) is finished,
    execution of traced process and test should be simultaneously resumed using special "resume" command in GDB.

    Typical code snippet:

        from gdb_helpers import attach_gdb
        attach_gdb(self.Env.get_service_pids("controller_agent")[0], ex=["b sorted_chunk_pool.cpp:4242"])

    NB: Arcadia large tests impose some timeout on test execution (30min or so), so keep that in mind.

    :param pid: process pid to attach
    :type pid int
    :param ex: list of commands to run on startup
    :type ex list
    :param autoresume: if True, provided commands will be executed, test execution will be resumed and process will be
    continued. If False, you must manually type "resume" in GDB in order to continue execution; this allows you
    to spend some time observing state of the process.
    """

    _check_ptrace_scope()

    ex = ex or []

    tmpdir = create_tmpdir("gdb")
    barrier_path = os.path.join(tmpdir, "init_barrier")
    print_debug("Starting gdb; barrier file {}".format(barrier_path))
    GDB_PROLOGUE = """
define resume
echo Resuming execution in the test and continuing program execution\\n
echo Touching barrier file {filename}\\n
echo \\n
shell touch {filename}
continue
end

echo \\n
echo -e "You may set up all necessary stuff for your debugging session."
echo -e "After doing so, type 'resume' to continue trace execution and resume tests.\\n"
echo Barrier path: {filename}\\n
echo \\n
""".format(filename=barrier_path)

    prologue_path = os.path.join(tmpdir, "prologue.gdb")
    with open(prologue_path, "w") as prologue_file:
        prologue_file.write(GDB_PROLOGUE)

    ex.append("source {}".format(prologue_path))
    if autoresume:
        ex.append("resume")

    cmd = "ya tool gdb -p {} {}".format(int(pid), "".join(" -ex '{}'".format(e) for e in ex))
    cmd = 'tmux new-window "{}"'.format(cmd)

    os.system(cmd)

    print_debug("Waiting for appearance of {}".format(barrier_path))

    wait(lambda: os.path.exists(barrier_path), iter=10**5)

    print_debug("Resuming test execution")
