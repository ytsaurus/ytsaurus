from .driver import make_request

def run_job_shell(job_id, timeout=None, command=None, client=None):
    """Runs interactive shell in the job sandbox.

    :param str job_id: job id.
    """
    from .job_shell import JobShell

    JobShell(job_id, interactive=True, timeout=timeout, client=client).run(command=command)

def get_job_stderr(operation_id, job_id, client=None):
    """Gets stderr of the specified job.

    :param str operation_id: operation id.
    :param str job_id: job id.
    """
    return make_request(
        "get_job_stderr",
        {"operation_id": operation_id, "job_id": job_id},
        return_content=False,
        client=client)

def abort_job(job_id, interrupt_timeout, client=None):
    """Interrupts running job with preserved result.

    :param str job_id: job id.
    :param int interrupt_timeout: wait for interrupt before abort (in ms).
    """
    make_request(
        "abort_job",
        {"job_id": job_id, "interrupt_timeout": interrupt_timeout},
        client=client)

def dump_job_context(job_id, path, client=None):
    """Dumps job input context to specified path."""
    return make_request("dump_job_context", {"job_id": job_id, "path": path}, client=client)
