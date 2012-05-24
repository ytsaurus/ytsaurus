import subprocess

YT = "yt"

###########################################################################

class YTError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

###########################################################################


def command(name, *args, **kw):
    process = run_command(name, *args, **kw)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        print 'XXX:', stderr
        raise YTError(stderr)
    print stdout
    return stdout.strip('\n')

def convert_to_yt_args(*args, **kw):
    all_args = list(args)
    for k, v in kw.items():
        # dirty hack for --tx
        if k == 'tx':
            v = '"%s"' % v
        all_args.extend(['--' + k, v])
    return all_args

def run_command(name, *args, **kw):
    all_args = [name] + convert_to_yt_args(*args, **kw)
    print all_args

    process = subprocess.Popen([YT] + all_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return process    


###########################################################################

