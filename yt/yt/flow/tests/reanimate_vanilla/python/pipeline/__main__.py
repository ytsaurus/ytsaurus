"""Entry point for the Python reanimate-test companion process."""

from yt.yt.flow.library.python.companion import Pipeline

from .secret_sink import SecretSink


def main():
    pipeline = Pipeline()
    pipeline.add("mapper", SecretSink())
    pipeline.run()


if __name__ == "__main__":
    main()
