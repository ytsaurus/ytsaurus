import os
import sys
import signal


def main():
    if os.environ.get("USE_CUSTOM_SIGNAL_HANDLER") == "1":
        def handler(*args, **kwargs):
            print("SIGSEGV handled", file=sys.stderr)
        signal.signal(signal.SIGSEGV, handler)

    import yt_yson_bindings

    signal.raise_signal(signal.SIGSEGV)


if __name__ == "__main__":
    main()
