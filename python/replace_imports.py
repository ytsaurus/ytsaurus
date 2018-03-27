import sys

def main():
    for line in sys.stdin:
        if line.strip().startswith("import"):
            line = line.replace("\"yt/", "\"proto/yt/")
            line = line.replace("\"yp/", "\"proto/yp/")
        sys.stdout.write(line)

if __name__ == "__main__":
    main()

