#!/usr/bin/python

import argparse
import paramiko
import hostlist
import time
import threading
import subprocess

class CopySessionBase(object):
    def __init__(self, src_node, src_file, dst_node, dst_file):
        self.src_node = src_node
        self.dst_node = dst_node
        self.src_file = src_file
        self.dst_file = dst_file

    def descr(self):
        return "%s:%s to %s:%s" %(self.src_node, self.src_file, self.dst_node, self.dst_file)

    def report_start(self):
        print "Copy %s" % (self.descr())

    def report_finish(self):
        if self.status() == 0:
            print "Succesfully copied %s" % self.descr()
        else:
            print "Copy %s exited with status %s:\n%s" %( self.descr(), self.status(), self._stderr())

class RemoteCopySession(CopySessionBase):
    def __init__(self, src_node, src_file, dst_node, dst_file):
        super(RemoteCopySession, self).__init__(src_node, src_file, dst_node, dst_file)
        self._start_remote_copy()

    def _start_remote_copy(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(hostname=self.src_node)
        self.session = self.client.get_transport().open_session()
        paramiko.agent.AgentRequestHandler(self.session)
        command = "scp -r -o StrictHostKeyChecking=no '%s' '%s':'%s'" % (self.src_file, self.dst_node, self.dst_file)
        self.session.exec_command(command)

    def ready(self):
        return self.session.exit_status_ready()

    def status(self):
        return self.session.recv_exit_status()

    def _stdout(self):
        result = ""
        while True:
            r = self.session.recv(1024)
            if len(r) == 0:
                break
            else:
                result += r
        return result

    def _stderr(self):
        result = ""
        while True:
            r = self.session.recv_stderr(1024)
            if len(r) == 0:
                break
            else:
                result += r
        return result

    def close(self):
        self.session.close()
        self.client.close()

class LocalCopySession(CopySessionBase):
    def __init__(self, src_node, src_file, dst_node, dst_file):
        super(LocalCopySession, self).__init__(src_node, src_file, dst_node, dst_file)
        self._start_local_copy()

    def _start_local_copy(self):
        class Worker(threading.Thread):
            def __init__(self, src_file, dst_node, dst_file):
                super(Worker, self).__init__()
                self.dst_node = dst_node
                self.src_file = src_file
                self.dst_file = dst_file
            def run(self):
                command = ["scp", "-r", "-o", "StrictHostKeyChecking=no", self.src_file, "%s:%s" % (self.dst_node, self.dst_file)]
                self.sp = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.stdout, self.stderr = self.sp.communicate()
                self.status = self.sp.poll()

        self.worker = Worker(self.src_file, self.dst_node, self.dst_file)
        self.worker.start()

    def _stderr(self):
        return self.worker.stderr

    def _stdout(self):
        return self.worker.stdout

    def ready(self):
        return not self.worker.is_alive()

    def status(self):
        return self.worker.status

    def close(self):
        self.worker.join()
        pass

def remote_put(src_file, dst_node, dst_file):
    print "Succesfully copied %s" % descr

def create_session(src_node, src_file, dst_node, dst_file):
    if src_node == "localhost":
        return LocalCopySession(src_node, src_file, dst_node, dst_file)
    else:
        return RemoteCopySession(src_node, dst_file, dst_node, dst_file)

def distribute(src_file, recipients, dst_file):
    donors = ["localhost"]
    sessions = []
    failures = {r:0 for r in recipients}
    attempt_threshold = 1
    while len(sessions) > 0 or len(recipients) > 0:
        if len(donors) > 0 and len(recipients) > 0:
            donor = donors.pop()
            recepient = recipients.pop()
            sessions.append(create_session(donor, src_file, recepient, dst_file))
            sessions[-1].report_start()
            continue
        for i in xrange(len(sessions)):
            session = sessions[i]
            if session.ready():
                del sessions[i]
                session.report_finish()
                donors.append(session.src_node)
                if session.status() == 0:
                    donors.append(session.dst_node)
                else:
                    failures[session.dst_node] += 1
                    if failures[session.dst_node] >= attempt_threshold:
                        print "Too many failed attempts for host %s, disabled" % session.dst_node
                    else:
                        recipients.append(session.dst_node)
                session.close()
                break

    failed_nodes = [r for r in failures if failures[r] >= attempt_threshold]
    print "Ditribution finished. Failed at %s hosts: %s" % (len(failed_nodes), failed_nodes)

def expand_hostlist(nodes):
    if nodes.startswith("^"):
        return [node.strip() for node in file(nodes[1:])]
    else:
        return hostlist.expand_hostlist(nodes)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("src", type=str, help="Source file")
    parser.add_argument("--dst", type=str, metavar="path", help="Destination file")
    parser.add_argument("nodes", type=str, metavar="hostlist", nargs="+", help="Destination hosts")
    args = parser.parse_args()

    nodelist = []
    for nodes in args.nodes:
        nodelist += expand_hostlist(nodes)
    if len(nodes) == 0:
        return

    distribute(args.src, nodelist, args.dst if args.dst else args.src)

if __name__ == "__main__":
    main() 
