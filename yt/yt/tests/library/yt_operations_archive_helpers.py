from yt_commands import (
    lookup_rows, wait_no_assert, delete_rows, insert_rows, wait)

from yt.common import uuid_to_parts, parts_to_uuid

JOB_ARCHIVE_TABLE = "//sys/operations_archive/jobs"
OPERATION_IDS_TABLE = "//sys/operations_archive/operation_ids"


def get_job_from_archive(op_id, job_id):
    op_id_hi, op_id_lo = uuid_to_parts(op_id)
    job_id_hi, job_id_lo = uuid_to_parts(job_id)
    rows = lookup_rows(
        JOB_ARCHIVE_TABLE,
        [
            {
                "operation_id_hi": op_id_hi,
                "operation_id_lo": op_id_lo,
                "job_id_hi": job_id_hi,
                "job_id_lo": job_id_lo,
            }
        ],
    )
    return rows[0] if rows else None


def get_allocation_id_from_archive(op_id, job_id):
    allocation_id_hi = None
    allocation_id_lo = None

    @wait_no_assert
    def get_allocation_id_parts():
        nonlocal allocation_id_hi, allocation_id_lo
        job_from_archive = get_job_from_archive(op_id, job_id)
        assert "allocation_id_hi" in job_from_archive
        assert "allocation_id_lo" in job_from_archive
        allocation_id_hi = job_from_archive["allocation_id_hi"]
        allocation_id_lo = job_from_archive["allocation_id_lo"]

    assert allocation_id_hi is not None and allocation_id_lo is not None
    return parts_to_uuid(allocation_id_hi, allocation_id_lo)


def delete_job_from_archive(op_id, job_id):
    op_id_hi, op_id_lo = uuid_to_parts(op_id)
    job_id_hi, job_id_lo = uuid_to_parts(job_id)
    delete_rows(
        JOB_ARCHIVE_TABLE,
        [
            {
                "operation_id_hi": op_id_hi,
                "operation_id_lo": op_id_lo,
                "job_id_hi": job_id_hi,
                "job_id_lo": job_id_lo,
            }
        ],
        atomicity="none",
    )


def update_job_in_archive(op_id, job_id, attributes):
    op_id_hi, op_id_lo = uuid_to_parts(op_id)
    job_id_hi, job_id_lo = uuid_to_parts(job_id)
    attributes.update(
        {
            "operation_id_hi": op_id_hi,
            "operation_id_lo": op_id_lo,
            "job_id_hi": job_id_hi,
            "job_id_lo": job_id_lo,
        }
    )

    def do_update_job_in_archive():
        insert_rows(JOB_ARCHIVE_TABLE, [attributes], update=True, atomicity="none")
        return True

    wait(do_update_job_in_archive, ignore_exceptions=True)


def get_controller_state_from_archive(op_id, job_id):
    wait(lambda: get_job_from_archive(op_id, job_id) is not None)
    job_from_archive = get_job_from_archive(op_id, job_id)
    return job_from_archive.get("controller_state")
