from job_shell import JobShell

def run_job_shell(job_id, client=None):
    """ Run interactive shell in the job sandbox

    :param job_id: (string) job id
    """
    JobShell(job_id, client=client).run()
