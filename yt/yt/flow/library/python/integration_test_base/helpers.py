from yt.wrapper import yson

from os import PathLike


def get_yson_config(path: str | PathLike) -> dict:
    with open(path, "rb") as f:
        return yson.load(f)


def dump_yson_config(config: dict, path: str | PathLike):
    with open(path, "wb") as f:
        yson.dump(config, f, yson_format="pretty")


def dump_operation_stderr(client, operation_id: str, path: str | PathLike):
    with open(path, "w") as f:
        for job in client.list_jobs(operation_id)["jobs"]:
            f.write(f"Job: {job['id']} state={job.get('state')}\n")

            if "error" in job:
                f.write(yson.dumps(job["error"], yson_format="pretty").decode("utf-8"))
                f.write("\n")

            try:
                f.write(client.get_job_stderr(operation_id, job["id"]).read().decode("utf-8"))
            except Exception as ex:
                f.write(f"[get_job_stderr failed: {ex}]\n")

            f.write("\n\n")


def nested_setdefault(d, *keys):
    for key in keys:
        d = d.setdefault(key, {})
    return d
