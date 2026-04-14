from yt_odin_checks.lib.check_runner import main


DEFAULT_TEMP_PATH = "//sys/admin/odin/cypress_commands"


def _ensure_temp_path_exists(yt_client, logger, temp_path):
    if yt_client.exists(temp_path):
        return

    logger.info('Creating "%s"', temp_path)
    yt_client.mkdir(temp_path, recursive=True)


def _get_temp_document_path(yt_client, temp_path):
    if not temp_path.endswith("/"):
        temp_path += "/"

    return yt_client.find_free_subpath(temp_path + "document_")


def run_check(yt_client, logger, options, states):
    temp_path = options.get("temp_path", DEFAULT_TEMP_PATH).rstrip("/")
    document_path = None
    actual_payload = None
    payload = {
        "check": "cypress_commands",
        "value": "ok",
    }

    try:
        _ensure_temp_path_exists(yt_client, logger, temp_path)

        logger.info("Getting %s", temp_path)
        yt_client.get(temp_path)

        logger.info("Listing %s", temp_path)
        yt_client.list(temp_path)

        logger.info("Getting attributes of %s", temp_path)
        yt_client.get(temp_path + "/@")

        document_path = _get_temp_document_path(yt_client, temp_path)
        logger.info("Creating document %s", document_path)
        yt_client.create("document", document_path)

        logger.info("Setting %s", document_path)
        yt_client.set(document_path, payload)

        logger.info("Getting %s", document_path)
        actual_payload = yt_client.get(document_path)
    finally:
        if document_path is not None:
            yt_client.remove(document_path, force=True)

    if actual_payload != payload:
        message = "Unexpected document payload for {}: expected {}, got {}".format(
            document_path,
            payload,
            actual_payload,
        )
        logger.error(message)
        return states.UNAVAILABLE_STATE, message

    return states.FULLY_AVAILABLE_STATE, "Cypress commands succeeded for {}".format(temp_path)


if __name__ == "__main__":
    main(run_check)
