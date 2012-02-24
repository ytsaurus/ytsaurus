import subprocess


def launch_yt(**kw):
    args = dict(
        bufsize=1,
        stdin=subprocess.PIPE, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE,
        )
    args.update(kw)
    return subprocess.Popen(['ytdriver'], **args)

def exec_cmd(cmd):
    yt = launch_yt()
    stdout, stderr = yt.communicate(cmd + '\n')
    return (stdout, stderr, yt.returncode)


###########################################################################

#TODO: think about necessity of this functions
def do_get(path, yt = None):
    return execute_cmd('{{do = get; path="{path}"}}'.format(**vars()), yt)
    
def execute_cmd(cmd, yt = None):
    if yt:
        yt.stdin.write(cmd + '\n')
        yt.stdin.flush()
        print 'executed', cmd
        return yt.stdout.readline().strip('\n')
    else:
        yt = launch_yt()
        return yt.communicate(cmd + '\n')[0]

def execute_error_cmd(cmd, yt = None):
    if yt:
        yt.stdin.write(cmd + '\n')
        yt.stdin.flush()
        return yt.stderr.readline().strip('\n')
    else:
        yt = launch_yt()
        return yt.communicate(cmd + '\n')[1]
