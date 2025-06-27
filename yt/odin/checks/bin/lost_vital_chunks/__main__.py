from yt_odin_checks.lib.check_runner import main


def run_check_impl(yt_client, logger, options,  path, status_name, sample_path=None, count_path=None):
    yt_client.config["proxy"]["request_timeout"] = 40000

    # COMPAT(koloshmet): quorum missing chunks map has no sample map, but lost vital chunks map has one.
    if sample_path is not None and yt_client.exists(sample_path):
        chunk_ids = yt_client.list(sample_path, max_size=options["max_size"])
        chunk_count = yt_client.get(count_path)
    else:
        chunk_ids = yt_client.list(path, max_size=options["max_size"])
        chunk_count = yt_client.get(f"{path}/@count")

    logger.info(f"{status_name.capitalize()} chunks number is %s", chunk_count)
    if len(chunk_ids) > 0:
        logger.info(f"Sample of {status_name} chunks: %s", " ".join(chunk_ids[:options["max_size"]]))
        for chunk_id in chunk_ids:
            try:
                logger.info("Last seen replicas for chunk {}: {}".format(chunk_id, yt_client.get("#" + chunk_id + "/@last_seen_replicas")))
            except Exception as error:
                logger.exception("Failed to get last seen replicas for chunk {}: '{}'".format(chunk_id, error))

    return int(chunk_count)


def run_check(yt_client, logger, options, states):
    lvc = run_check_impl(
        yt_client,
        logger,
        options,
        path="//sys/lost_vital_chunks",
        status_name="lost vital",
        sample_path="//sys/lost_vital_chunks_sample",
        count_path="//sys/@lost_vital_chunk_count",
    )

    if lvc == 0:
        return states.FULLY_AVAILABLE_STATE
    else:
        return states.UNAVAILABLE_STATE, lvc


if __name__ == "__main__":
    main(run_check)
