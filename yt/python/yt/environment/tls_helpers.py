from itertools import chain
import subprocess
import tempfile


def openssl_binary():
    try:
        from yt.environment.arcadia_interop import search_binary_path
        return search_binary_path("openssl")
    except ImportError:
        return "openssl"


def create_ca(ca_cert, ca_cert_key, subj="/CN=Fake CA", key_type="rsa:2048"):
    with tempfile.NamedTemporaryFile("w", suffix='.cnf') as cfg:

        # kludge for openssl 1.x
        cfg.write("[req]\ndistinguished_name = req\n")
        cfg.flush()

        subprocess.check_call([
            openssl_binary(), "req", "-batch", "-x509", "-config", cfg.name,
            "-sha512", "-nodes", "-newkey", key_type,
            "-days", "30", "-subj", subj,
            "-keyout", ca_cert_key, "-out", ca_cert,
            "-addext", "basicConstraints=critical,CA:TRUE,pathlen:0",
            "-addext", "keyUsage=critical,keyCertSign",
        ], stderr=subprocess.DEVNULL)


def create_certificate(cert, cert_key, ca_cert, ca_cert_key, names, subj=None, extended_key_usage="serverAuth", key_type="rsa:2048"):
    if subj is None:
        subj = "/CN=" + names[0]
    addext = [
        "subjectAltName = " + ",".join(["DNS:" + n for n in names]),
        "basicConstraints = critical,CA:FALSE",
        "keyUsage = critical,digitalSignature,keyEncipherment",
        "extendedKeyUsage = critical," + extended_key_usage,
    ]
    addext_args = list(chain(*[["-addext", ext] for ext in addext]))

    # works for openssl 3.x
    # run([openssl_binary(), "req",  "-batch", "-x509", "-nodes", "-sha512",
    #      "-CA", ca_cert, "-CAkey", ca_cert_key
    #      "-days", "30", "-subj", subj, "-addext", addext,
    #      "-newkey", key_type, "-keyout", cert_key, "-out", cert])

    # kludge for openssl 1.x
    with tempfile.NamedTemporaryFile("r", suffix='.csr') as cert_req, \
         tempfile.NamedTemporaryFile("w", suffix='.cnf') as cfg, \
         tempfile.NamedTemporaryFile("w", suffix='.cnf') as ext:

        cfg.write("[req]\ndistinguished_name = req\n")
        cfg.flush()

        ext.write("[ext]\n{}\n".format("\n".join(addext)))
        ext.flush()

        subprocess.check_call([
            openssl_binary(), "req", "-new", "-batch", "-config", cfg.name,
            "-nodes", "-newkey", key_type, "-subj", subj,
            "-keyout", cert_key, "-out", cert_req.name,
        ] + addext_args, stderr=subprocess.DEVNULL)

        subprocess.check_call([
            openssl_binary(), "x509", "-req", "-sha512", "-days", "30",
            "-CAcreateserial", "-CA", ca_cert, "-CAkey", ca_cert_key,
            "-in", cert_req.name, "-extfile", ext.name, "-extensions", "ext",
            "-out", cert,
        ], stderr=subprocess.DEVNULL)


def verify_certificate(cert, ca_cert, name):
    proc = subprocess.run(
        [openssl_binary(), "verify", "-trusted", ca_cert, "-verify_hostname", name, cert],
        check=False)
    return proc.returncode == 0


def get_server_certificate(address):
    proc = subprocess.run(
        [openssl_binary(), "s_client", "-connect", address],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    header, footer = b"-----BEGIN CERTIFICATE-----\n", b"-----END CERTIFICATE-----\n"
    return proc.stdout[proc.stdout.index(header):proc.stdout.index(footer)+len(footer)]


def get_certificate_fingerprint(cert=None, cert_content=None):
    if cert is not None:
        with open(cert, "rb") as f:
            cert_content = f.read()
    else:
        assert cert_content is not None
    proc = subprocess.run(
        [openssl_binary(), "x509", "-noout", "-fingerprint"],
        input=cert_content,
        stdout=subprocess.PIPE,
    )
    return proc.stdout.decode().strip()
