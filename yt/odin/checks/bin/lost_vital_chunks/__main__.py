from yt_odin_checks.lib.check_runner import main


def run_check(yt_client, logger, options, states):
    lost_vital_chunks = yt_client.list("//sys/lost_vital_chunks", max_size=options["max_size"])
    lost_vital_chunks_count = max(yt_client.get("//sys/lost_vital_chunks/@count"), len(lost_vital_chunks))
    logger.info("Lost vital chunks number is %s", lost_vital_chunks_count)
    if lost_vital_chunks:
        logger.info("Sample of lost vital chunks: %s", " ".join(lost_vital_chunks[:options["max_size"]]))
        for chunk_id in lost_vital_chunks:
            try:
                logger.info("Last seen replicas for chunk {}: {}".format(chunk_id, yt_client.get("#" + chunk_id + "/@last_seen_replicas")))
            except Exception as error:
                logger.exception("Failed to get last seen replicas for chunk {}: '{}'".format(chunk_id, error))

    if lost_vital_chunks_count == 0:
        return states.FULLY_AVAILABLE_STATE
    else:
        return states.UNAVAILABLE_STATE, int(lost_vital_chunks_count)


if __name__ == "__main__":
    main(run_check)
