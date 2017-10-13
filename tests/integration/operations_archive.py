import yt.operations_archive

from datetime import timedelta

def clean_operations(client):
    yt.operations_archive.clear_operations(soft_limit=0, hard_limit=0, grace_timeout=timedelta(seconds=0),
                                           archive_timeout=timedelta(days=0), execution_timeout=timedelta(seconds=280),
                                           max_operations_per_user=200, robots=[], archive=True, archive_jobs=True,
                                           push_metrics=False, thread_count=0, stderr_thread_count=0,
                                           remove_threshold=20000, client=client)
