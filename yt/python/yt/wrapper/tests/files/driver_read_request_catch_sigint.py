from yt_driver_bindings import BufferedStream

import signal

def main():
    signal.signal(signal.SIGINT, signal.default_int_handler)
    s = BufferedStream(10)
    s.read(5)

if __name__ == "__main__":
    main()
