import subprocess

YT = "yt"

def command(name, *args, **kw):
    process = run_command(name, *args, **kw)
    stdout, stderr = process.communicate()
    return stdout, stderr, process.returncode

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

