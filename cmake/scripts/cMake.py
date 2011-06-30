#!/usr/bin/env python
#!/usr/local/bin/python
#!/home/denplusplus/bin/bin/python

import sys
import os
import subprocess
from optparse import OptionParser

def getCPUCount():
    try:
        import multiprocessing
        return multiprocessing.cpu_count()
    except (ImportError,NotImplementedError):
        print >>sys.stderr, "warn: can't determine cpu count"
        pass
    return 8

def handleOptions():
    parser = OptionParser()
    parser.add_option("-d", "--debug", action="store_true", dest="debug", help="make debug", default=False)
    parser.add_option("-V", "--valgrind", action="store_true", dest="valgrind", help="make valgrind", default=False)
    parser.add_option("-k", "--kedevelop", action="store_true", dest="kdevelop", help="kdevelop", default=False)
    parser.add_option("-p", "--profile", action="store_true", dest="profile", help="make profile", default=False)
    parser.add_option("-m", "--cmake", action="store_true", dest="cmake", help="cmake", default=False)
    parser.add_option("-a", "--all", action="store_true", dest="all", help="make all", default=False)
    parser.add_option("-g", "--go", action="store_true", dest="cd", help="print cd command", default=False)
    parser.add_option("-c", "--clean", action="store_true", dest="clean", help="remove CMakeCache.txt", default=False)
    parser.add_option("-t", "--threads", dest="thread", help="number of threads", default=str(getCPUCount()))
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet", help="quiet mode", default=False)
    parser.add_option("-T", "--tests", action="store_true", dest="tests", help="test", default=False)
    parser.add_option("-u", "--unittests", action="store_true", dest="unittests", help="local unittests", default=False)
    parser.add_option("-l", "--local", action="store_true", dest="local", help="no peerdirs", default=False)
    (options, args) = parser.parse_args()
    return options

ARCADIA = "/arcadia/"
MAKE_OPTIONS = "NOSTRIP=yes"

def mkdir(dir):
    if not os.access(dir, os.F_OK):
        os.mkdir(dir)

def getOS():
    pOS = subprocess.Popen("uname -s", shell=True, stdout=subprocess.PIPE)
    pOS.wait()
    return pOS.stdout.readline().strip()

def splitCwd(cwd, buildDir):
    i = cwd.find(ARCADIA)
    if i < 0:
        if cwd.endswith(ARCADIA[:len(ARCADIA) - 1]):
            return cwd, ""
        raise Exception("No %s in path '%s'" % (arcadia, cwd))
    return cwd[:i + 1] + buildDir, cwd[i + len(ARCADIA):]

QUIET = False

def run(cmd):
    print cmd
    if QUIET: return
    proc = subprocess.Popen(cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr, stdin=sys.stdin)
    proc.wait()

def make(prefix, path, threads, makeOnly, tests, buildHome):
    os = getOS()
    if "Linux" != os and "Darwin" != os:
        makeBin = "gmake"
    else:
        makeBin = "make"
    if tests:
        makeBin = "~/work/arcadia/check/run_test.py -s; ~/work/arcadia/check/run_test.py -f -b"
    if makeOnly != "":
        prefix += " MAKE_ONLY=" + makeOnly
    if tests:
        sPath = buildHome
    else:
        sPath = path
    cmd = "cd %s; %s %s -j%d" % (sPath, prefix, makeBin, threads)
    run(cmd)

def mainCMake():
    options = handleOptions()
    if options.kdevelop:
        mode = "KDevelop"
    elif options.debug:
        mode = "Debug"
    elif options.profile:
        mode = "Profile"
    elif options.valgrind:
        mode = "Valgrind"
    else: 
        mode = "Release"
    
    buildHome, projectParts = splitCwd(os.getcwd(), "arcadia-" + mode.lower())
    newCwd = buildHome + "/" + projectParts
    mkdir(buildHome)
    print >> sys.stderr, "buildHome=%s\nprojectParts=%s\n" % (buildHome, projectParts)
    
    global QUIET
    QUIET = options.quiet
    if options.clean:
        run("rm %s/CMakeCache.txt" % buildHome)
    
    if options.cmake:
        makeOnly = ""
        if projectParts != "":
            makeOnly = "-DMAKE_ONLY=%s" % projectParts
        run("cd %s; cmake %s -DNOSTRIP=yes -DCMAKE_BUILD_TYPE=%s %s ../arcadia" % (buildHome, makeOnly, mode if not options.kdevelop else "Debug", "-G \"KDevelop3 - Unix Makefiles\"" if options.kdevelop else ""))

    threadCount = int(options.thread)
    makeOnly = ""
    if not options.all:
        makeOnly = projectParts
    if not options.kdevelop:
        make(MAKE_OPTIONS, newCwd, threadCount, makeOnly, options.tests, buildHome)
    else:
        run("kdevelop --project %s/Makefile" % buildHome)
    
    if options.cd: 
        print "cd %s" % newCwd

def mainMake():
    options = handleOptions()
    run("make -j%d" % int(options.thread))

def main():
    if os.path.exists("CMakeLists.txt"):
        mainCMake()
    else:
        mainMake()

if __name__ == "__main__": 
    main()
