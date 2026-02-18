import ssl
import os


def builtin_ca():
    return None, None, ssl.builtin_cadata()


# Normally certifi.where() returns a path to a certificate file;
# here it returns a callable.
def where():
    return builtin_ca


# Patch ssl module to accept a callable cafile.
load_verify_locations = ssl.SSLContext.load_verify_locations


def load_verify_locations__callable(self, cafile=None, capath=None, cadata=None):
    if callable(cafile):
        envfile = os.environ.get("SSL_CERT_FILE")
        if envfile is None:
            cafile, capath, cadata = cafile()
        else:
            cafile = envfile
        capath = os.environ.get("SSL_CERT_DIR", capath)

    return load_verify_locations(self, cafile, capath, cadata)


ssl.SSLContext.load_verify_locations = load_verify_locations__callable
