import subprocess
import json
import sys
import os


def load_database():
    result = dict()
    with open("compile_commands.json") as f:
        db = json.load(f)
    for item in db:
        result[item['file']] = item['command']
    return result


def commentout(filename, skip=0):
    index = 0
    commented_line = None

    oldfilename = filename + ".bak"
    os.rename(filename, oldfilename)
    newf = open(filename, "w")
    with open(oldfilename) as f:
        for line in f:
            if commented_line is None and line.startswith("#include <"):
                index += 1
                if index > skip:
                    commented_line = line
                    newf.write("///")
                    # comment out
            newf.write(line)
    newf.close()

    return commented_line


def revert(filename):
    oldfilename = filename + ".bak"
    os.rename(oldfilename, filename)


def check_compile(command):
    exitcode = subprocess.call(command.split())
    return exitcode == 0


def main(prefix):
    db = load_database()

    for filename, command in db.iteritems():
        if filename.startswith(prefix):
            print >>sys.stderr, "Filename: " + filename
            print "Filename: " + filename

            index = 0
            while True:
                commented_line = commentout(filename, skip=index)
                if not commented_line:
                    break
                print >>sys.stderr, "Comment out and compile {0} include ({1}) ...".format(index, commented_line.strip())

                result = check_compile(command)
                if result:
                    print "One can comment out " + commented_line.strip() + " from " + filename
                revert(filename)

                index += 1


if __name__ == "__main__":
    main(sys.argv[1])