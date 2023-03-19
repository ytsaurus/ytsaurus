from lib.run_application import GunicornApp
import logging
import os


def main():
    logger = logging.getLogger("werkzeug")
    logger.setLevel(logging.CRITICAL)

    if not os.path.exists("logs"):
        os.makedirs("logs")

    logging.basicConfig(
        filename="logs/comment_service.log", level=logging.DEBUG,
    )

    handler = logging.handlers.WatchedFileHandler("logs/comment_service.log")
    handler.setFormatter(logging.Formatter("%(levelname)-8s [%(asctime)s] %(message)s"))
    logging.getLogger().addHandler(handler)

    app = GunicornApp(
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", 5000)),
        workers=int(os.environ.get('WORKERS', 10)),
    )

    app.run()


if __name__ == "__main__":
    main()
