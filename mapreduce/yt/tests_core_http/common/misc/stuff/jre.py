import os
import tarfile
import tempfile


JDK_TGZ = 'jdk1.8.0_60.tar'


def get_java_executable():
    if not hasattr(get_java_executable, 'java_dir'):
        get_java_executable.java_dir = tempfile.mkdtemp(prefix='java_')
        tgz = tarfile.open(JDK_TGZ)
        tgz.extractall(path=get_java_executable.java_dir)

    return os.path.join(get_java_executable.java_dir, 'bin', 'java')


def find_jars(path):
    jars = []
    for root, _, files in os.walk(path):
        for f in files:
            if f.endswith('.jar'):
                jars.append(os.path.abspath(os.path.normpath(os.path.join(root, f))))
    return jars
