from yt.environment.tls_helpers import openssl_binary, create_certificate, create_ca, verify_certificate

import subprocess


def print_certificate(cert):
    subprocess.check_call([openssl_binary(), "x509", "-noout", "-text", "-in", cert])


def test_tls_helpers():
    create_ca(ca_cert="ca.crt", ca_cert_key="ca.key")
    print_certificate("ca.crt")

    create_certificate(ca_cert="ca.crt", ca_cert_key="ca.key", cert="out.crt", cert_key="out.key", names=["correct", "alternative"])
    print_certificate("out.crt")

    assert verify_certificate(ca_cert="ca.crt", cert="out.crt", name="correct")
    assert not verify_certificate(ca_cert="ca.crt", cert="out.crt", name="wrong")
    assert verify_certificate(ca_cert="ca.crt", cert="out.crt", name="alternative")
