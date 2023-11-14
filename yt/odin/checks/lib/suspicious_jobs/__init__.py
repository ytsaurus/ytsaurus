from yt.common import date_string_to_timestamp

from collections import defaultdict
import time


def run_check_impl(yt_client, logger, options, states, include_nodes_with_tag="internal"):
    # `critical_suspicious_job_inactivity_timeout` is a timeout in minutes enough to make this check become red.
    critical_suspicious_job_inactivity_timeout = options["critical_suspicious_job_inactivity_timeout"]

    OUTPUT_LIMIT = 10  # Maximum number of jobs to be printed in this diagnostic.

    suspicious_jobs_map = yt_client.get("//sys/scheduler/orchid/scheduler/suspicious_jobs")
    suspicious_jobs_by_type = defaultdict(list)

    nodes_with_tag = [str(i) for i in yt_client.list('//sys/cluster_nodes', attributes=['tags'], read_from="cache")
                      if include_nodes_with_tag in i.attributes['tags']]

    for key, value in suspicious_jobs_map.items():
        if "node" in value and value["node"] not in nodes_with_tag:
            continue
        value["job_id"] = key
        suspicious_jobs_by_type[value["type"]].append(value)
    suspicious_jobs_total = sum([len(suspicious_jobs) for suspicious_jobs in suspicious_jobs_by_type.values()])

    current_time = time.time()

    def get_inactivity_time(suspicious_job):
        return current_time - date_string_to_timestamp(suspicious_job["last_activity_time"])

    logger.info("Number of suspicious jobs grouped by job type (total {}):".format(suspicious_jobs_total))
    max_inactivity_time_except_remote_copy = 0
    for job_type, suspicious_jobs in suspicious_jobs_by_type.items():
        logger.info("  %s: %d", job_type, len(suspicious_jobs))
        suspicious_jobs.sort(key=get_inactivity_time, reverse=True)
        if job_type != "remote_copy":
            max_inactivity_time_except_remote_copy = max(
                max_inactivity_time_except_remote_copy,
                get_inactivity_time(suspicious_jobs[0]))

    if not suspicious_jobs_by_type:
        logger.info("  (no suspicious jobs)")
        return states.FULLY_AVAILABLE_STATE

    logger.info("Maximum inactivity time (except remote copy) is %d seconds. Top-%d suspicious jobs for each job type:",
                max_inactivity_time_except_remote_copy, OUTPUT_LIMIT)

    for job_type, suspicious_jobs in suspicious_jobs_by_type.items():
        logger.info("Job type: %s", job_type)
        for job in suspicious_jobs[:OUTPUT_LIMIT]:
            logger.info("  Inactivity time: %d seconds. Job: %s", get_inactivity_time(job), job)

    if max_inactivity_time_except_remote_copy < critical_suspicious_job_inactivity_timeout:
        return states.PARTIALLY_AVAILABLE_STATE

    return states.UNAVAILABLE_STATE
