import zstandard
import argparse
import re
import codecs
import tqdm


def process(logfile):
    dctx = zstandard.ZstdDecompressor()
    reader = codecs.getreader("utf-8")(dctx.stream_reader(logfile))
    restarts = 0
    threads = {}
    thread_id_map = {}
    regex = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}\\s+D\\s+Stress\\s+Debug event \\(Seq: ([0-9]+), Thread: ([0-9]+)\\)\\s+MyThread:([0-9]+)\\s+[0-9a-f]{16}\\s*$")
    start_regex = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}\\s+I\\s+Logging\\s+Logging started \\(.*\\)\\s*$")
    thread_regex = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}\\s+[DI]\\s+Concurrency\\s.*$")
    for ln in tqdm.tqdm(reader.readlines()):
        if ln == "\n":
            continue
        ln = ln[:-1]
        mt = regex.match(ln)
        if mt:
            seq = int(mt[1])
            thread_id = int(mt[2])
            real_thread_id = int(mt[3])
            if seq == 0:
                assert thread_id not in threads
                threads[thread_id] = 1
                thread_id_map[thread_id] = real_thread_id
            else:
                assert threads[thread_id] == seq
                assert thread_id_map[thread_id] == real_thread_id
                threads[thread_id] += 1
            continue
        mt = start_regex.match(ln)
        if mt:
            restarts += 1
            threads = {}
            continue
        mt = thread_regex.match(ln)
        if mt:
            continue
        raise RuntimeError("Broken line: " + repr(ln))
    print("OK, restarts = {}".format(restarts))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--logfile",
        action="store",
        type=argparse.FileType('rb'),
        required=True,
        help="log file to read")
    args = parser.parse_args()
    process(args.logfile)
    return 0
