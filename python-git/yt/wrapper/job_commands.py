from .driver import make_request

def run_job_shell(job_id, timeout=None, client=None):
    """ Run interactive shell in the job sandbox

    :param job_id: (string) job id
    """
    from .job_shell import JobShell

    JobShell(job_id, interactive=True, timeout=timeout, client=client).run()

def get_job_stderr(operation_id, job_id, client=None):
    """ Get stderr of the specified job

    :param operation_id: (string) operation id
    :param job_id: (string) job_id
    """
    return make_request(
        "get_job_stderr",
        {"operation_id": operation_id, "job_id": job_id},
        return_content=False,
        client=client)

def abort_job(job_id, interrupt_timeout, client=None):
    """ Interrupt running job with preserved result

    :param job_id: (string) job_id
    :param interrupt_timeout: (milliseconds) wait for interrupt before abort
    """
    make_request(
        "abort_job",
        {"job_id": job_id, "interrupt_timeout": interrupt_timeout},
        client=client)
