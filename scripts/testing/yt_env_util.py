import subprocess

def do_get(path, yt = None):
    return execute_cmd('{{do = get; path="{path}"}}'.format(**vars()), yt)
    
def execute_cmd(cmd, yt = None):
    if yt:
        yt.stdin.write(cmd + '\n')
        return yt.stdout.readline().strip('\n')
    else:
        yt = launch_yt()
        return yt.communicate(cmd + '\n')[0]

def execute_error_cmd(cmd, yt = None):
    if yt:
        yt.stdin.write(cmd + '\n')
        return yt.stderr.readline().strip('\n')
    else:
        yt = launch_yt()
        return yt.communicate(cmd + '\n')[1]

def launch_yt():
    return subprocess.Popen(['ytdriver'], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE)